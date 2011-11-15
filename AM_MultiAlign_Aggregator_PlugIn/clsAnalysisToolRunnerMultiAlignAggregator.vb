Option Strict On

'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2010, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerMultiAlignAggregator
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    ' Class for running MultiAlignAggregator analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MULTIALIGN_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_MULTI_ALIGN_DONE As Single = 95

    Protected WithEvents CmdRunner As clsRunDosProgram  

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MultiAlign Aggregator tool
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

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MultiAlign")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMultiAlignAggregator.RunTool(): Enter")
        End If

        ' Determine the path to the MultiAlign folder
        Dim progLoc As String
        progLoc = DetermineProgramLocation("MultiAlign", "MultiAlignProgLoc", "MultiAlignConsole.exe")

        If String.IsNullOrWhiteSpace(progLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the MultiAlign version info in the database
		If Not StoreToolVersionInfo(progLoc) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining MultiAlign version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Dim MultiAlignResultFilename As String = m_jobParams.GetParam("ResultFilename")

        If String.IsNullOrWhiteSpace(MultiAlignResultFilename) Then
            MultiAlignResultFilename = m_Dataset
        End If

        ' Set up and execute a program runner to run MultiAlign
        CmdStr = " -files " & clsAnalysisResourcesMultiAlignAggregator.MULTIALIGN_INPUT_FILE & " -params " & System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName")) & " -path " & m_WorkDir & " -name " & MultiAlignResultFilename & " -plots"
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = False
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "MultiAlign", True) Then
            m_message = "Error running MultiAlign"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
            blnSuccess = False
        Else
            blnSuccess = True
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow
        m_progress = PROGRESS_PCT_MULTI_ALIGN_DONE

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
            ' Useful for debugging MultiAlign problems
            CopyFailedResultsToArchiveFolder()
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        'Rename the log file so it is consistent with other log files. MultiAlign will add ability to specify log file name
        RenameLogFile(MultiAlignResultFilename)

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        ' Move the Plots folder to the result files folder
        Dim diPlotsFolder As System.IO.DirectoryInfo
        diPlotsFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"))

        Dim strTargetFolderPath As String
        strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots")
        diPlotsFolder.MoveTo(strTargetFolderPath)

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Function RenameLogFile(ByVal LogFilename As String) As IJobParams.CloseOutType

        Dim TmpFile As String = String.Empty
        Dim Files As String()
        Dim LogExtension As String = "-log.txt"
        Dim NewFilename As String = LogFilename & LogExtension
        'This is what MultiAlign is currently naming the log file
        Dim LogNameFilter As String = LogFilename & ".db3-log*.txt"
        Try
            'Get the log file name.  There should only be one log file
            Files = Directory.GetFiles(m_WorkDir, LogNameFilter)
            'go through each log file found.  Again, there should only be one log file
            For Each TmpFile In Files
                'Check to see if the log file exists.  If so, only rename one of them
                If Not File.Exists(NewFilename) Then
                    My.Computer.FileSystem.RenameFile(TmpFile, NewFilename)
                End If
            Next

        Catch ex As Exception
            'Even if the rename failed, go ahead and continue

        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

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
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal strMultiAlignProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioMultiAlignProg As System.IO.FileInfo
		Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioMultiAlignProg = New System.IO.FileInfo(strMultiAlignProgLoc)

        ' Lookup the version of MultiAlign 
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioMultiAlignProg.FullName)
		If Not blnSuccess Then Return False

        ' Lookup the version of additional DLLs
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLProteomics.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLControls.dll"))
		If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll")))
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_MULTIALIGN_RUNNING, 0, "", "", "", False)
        End If

    End Sub

   
#End Region

End Class
