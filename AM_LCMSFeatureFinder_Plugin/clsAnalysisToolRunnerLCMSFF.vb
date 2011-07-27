Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
'Imports PRISM.Files
'Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerLCMSFF
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    ' Class for running the LCMS Feature Finder
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_FEATURE_FINDER_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_FEATURE_FINDER_DONE As Single = 95

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs LCMS Feature Finder tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType
        Dim blnSuccess As Boolean

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running LCMSFeatureFinder")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerLCMSFF.OperateAnalysisTool(): Enter")
        End If

        ' Determine the path to the LCMSFeatureFinder folder
        Dim progLoc As String
        progLoc = DetermineProgramLocation()
        If String.IsNullOrWhiteSpace(progLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Set up and execute a program runner to run the LCMS Feature Finder
        CmdStr = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("LCMSFeatureFinderIniFile"))
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = False
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "LCMSFeatureFinder", True) Then
            m_message = "Error running LCMSFeatureFinder"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
            blnSuccess = False
        Else
            blnSuccess = True
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now
        m_progress = PROGRESS_PCT_FEATURE_FINDER_DONE

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        If Not blnSuccess Then
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging FeatureFinder problems
            CopyFailedResultsToArchiveFolder()
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the .UIMF file first, plus also the Decon2LS .csv files)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & ".UIMF"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "*.csv"))
        Catch ex As Exception
            ' Ignore errors here
        End Try

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_FEATURE_FINDER_RUNNING, 0, "", "", "", False)
        End If

    End Sub

    ''' <summary>
    ''' Determine the path to the correct version of LCMSFeatureFinder.exe
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function DetermineProgramLocation() As String

        Const EXE_NAME As String = "LCMSFeatureFinder.exe"

        ' Lookup the path to the folder that contains the LCMSFeaturefinder
        Dim progLoc As String = m_mgrParams.GetParam("LCMSFeatureFinderProgLoc")

        If String.IsNullOrWhiteSpace(progLoc) Then
            m_message = "Manager parameter LCMSFeatureFinderProgLoc is not defined in the Manager Control DB"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return String.Empty
        End If

        ' Check whether the settings file specifies that a specific version of LCMSFeatureFinder.exe be used
        Dim strLCMSFeatureFinderVersion As String = m_jobParams.GetParam("LCMSFeatureFinder_Version")

        If Not String.IsNullOrWhiteSpace(strLCMSFeatureFinderVersion) Then

            ' Specific version is defined; verify that the folder exists
            progLoc = System.IO.Path.Combine(progLoc, strLCMSFeatureFinderVersion)

            If Not System.IO.Directory.Exists(progLoc) Then
                m_message = "Version-specific LCMSFeatureFinder folder not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
                Return String.Empty
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Using specific version of the LCMSFeatureFinder: " & progLoc)
            End If
        End If

        ' Define the path to the .Exe, then verify that it exists
        progLoc = System.IO.Path.Combine(progLoc, EXE_NAME)

        If Not System.IO.File.Exists(progLoc) Then
            m_message = "Cannot find LCMSFeatureFinder program file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
            Return String.Empty
        End If

        ' Store the FeatureFinder version info in the database
        StoreToolVersionInfo(progLoc)

        Return progLoc

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal strFeatureFinderProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)
        Dim ioFeatureFinderInfo As System.IO.FileInfo

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioFeatureFinderInfo = New System.IO.FileInfo(strFeatureFinderProgLoc)
        If Not ioFeatureFinderInfo.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

            Return False
        End If

        ' Lookup the version of the Feature Finder
        StoreToolVersionInfoOneFile(strToolVersionInfo, ioFeatureFinderInfo.FullName)

        ' Lookup the version of the FeatureFinder Library (in the feature finder folder)
        Dim strFeatureFinderDllLoc As String = System.IO.Path.Combine(ioFeatureFinderInfo.DirectoryName, "FeatureFinder.dll")
        StoreToolVersionInfoOneFile(strToolVersionInfo, strFeatureFinderDllLoc)

        ' Lookup the version of the UIMF Library (in the feature finder folder)
        StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioFeatureFinderInfo.DirectoryName, "UIMFLibrary.dll"))

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(strFeatureFinderProgLoc))
        ioToolFiles.Add(New System.IO.FileInfo(strFeatureFinderDllLoc))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try


    End Function

    Private Sub StoreToolVersionInfoOneFile(ByRef strToolVersionInfo As String, ByVal strFilePath As String)

        ' If folder 32BitDLLs exists below the folder defined in strFeatureFinderProgLoc, then 
        ' preferably use the files in that folder to determine the version number.  However, only
        ' use the files if their Last-Modified date is the same as the date of the primary files
        '
        ' We're doing this because the 32-bit Analysis Manager cannot use Reflection to determine version info in 64-bit compiled DLLs

        Dim ioFileInfo As System.IO.FileInfo
        Dim ioFileInfo32Bit As System.IO.FileInfo

        Dim str32BitEquivalentFilePath As String

        Try
            ioFileInfo = New System.IO.FileInfo(strFilePath)

            If Not ioFileInfo.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "File not found by StoreToolVersionInfoOneFile: " & strFilePath)
            Else
                ' Look for a subfolder named 32BitDLLs
                str32BitEquivalentFilePath = System.IO.Path.Combine(System.IO.Path.Combine(ioFileInfo.DirectoryName, "32BitDLLs"), ioFileInfo.Name)
                ioFileInfo32Bit = New System.IO.FileInfo(str32BitEquivalentFilePath)

                If Not ioFileInfo32Bit.Exists Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "32-bit version of the file not at: " & str32BitEquivalentFilePath & "; 64-bit version info will be incomplete")
                Else

                    ' Confirm that the 64-bit and 32-bit version of the files have modification dates less than 1 minute apart

                    If Math.Abs(ioFileInfo32Bit.LastWriteTime.Subtract(ioFileInfo.LastWriteTime).TotalMinutes) <= 1 Then
                        ioFileInfo = ioFileInfo32Bit
                    Else
                        Dim strMessage As String
                        strMessage = "32-bit version of " & ioFileInfo.Name & " has a modification time more than 1 minute away from the 64-bit version; " & _
                                     "32-bit=" & ioFileInfo32Bit.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) & " vs. " & _
                                     "64-bit=" & ioFileInfo.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT)

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
                    End If
                End If

                Dim oAssemblyName As System.Reflection.AssemblyName
                oAssemblyName = System.Reflection.Assembly.LoadFrom(ioFileInfo.FullName).GetName

                Dim strNameAndVersion As String
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for " & System.IO.Path.GetFileName(strFilePath) & ": " & ex.Message)
        End Try


    End Sub
#End Region

End Class
