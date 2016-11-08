Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.Collections.Generic
Imports System.IO

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

        ' Determine the path to the LCMSFeatureFinder folder
        Dim progLoc As String
        progLoc = MyBase.DetermineProgramLocation("LCMSFeatureFinder", "LCMSFeatureFinderProgLoc", "LCMSFeatureFinder.exe")

        If String.IsNullOrWhiteSpace(progLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the FeatureFinder version info in the database
        blnSuccess = StoreToolVersionInfo(progLoc)
        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining LCMS FeatureFinder version"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Set up and execute a program runner to run the LCMS Feature Finder
        CmdStr = Path.Combine(m_WorkDir, m_jobParams.GetParam("LCMSFeatureFinderIniFile"))
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
        m_StopTime = System.DateTime.UtcNow
        m_progress = PROGRESS_PCT_FEATURE_FINDER_DONE

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(500)         ' 1 second delay
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

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

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'ZipResult

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
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & ".UIMF"))
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & "*.csv"))
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
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
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

        UpdateStatusFile(PROGRESS_PCT_FEATURE_FINDER_RUNNING)
        
        LogProgress("LCMSFeatureFinder")

    End Sub

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal strFeatureFinderProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioFeatureFinderInfo As FileInfo
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioFeatureFinderInfo = New FileInfo(strFeatureFinderProgLoc)
        If Not ioFeatureFinderInfo.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), blnSaveToolVersionTextFile:=False)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

            Return False
        End If

        ' Lookup the version of the Feature Finder
        blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, ioFeatureFinderInfo.FullName)
        If Not blnSuccess Then Return False

        ' Lookup the version of the FeatureFinder Library (in the feature finder folder)
        Dim strFeatureFinderDllLoc As String = Path.Combine(ioFeatureFinderInfo.DirectoryName, "FeatureFinder.dll")
        blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, strFeatureFinderDllLoc)
        If Not blnSuccess Then Return False

        ' Lookup the version of the UIMF Library (in the feature finder folder)
        blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, Path.Combine(ioFeatureFinderInfo.DirectoryName, "UIMFLibrary.dll"))
        If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(strFeatureFinderProgLoc))
        ioToolFiles.Add(New FileInfo(strFeatureFinderDllLoc))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

End Class
