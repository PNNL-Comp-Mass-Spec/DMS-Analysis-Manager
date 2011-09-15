'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/17/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerSeqCluster
	Inherits clsAnalysisToolRunnerSeqBase

	'*********************************************************************************************************
	'Overrides Sequest tool runner to provide cluster-specific methods
	'*********************************************************************************************************

    Private Const DTA_DELETE_INTERVAL_SECONDS As Integer = 15
    Private Const DTA_DELETE_HOLDOFF_SECONDS As Integer = 30

#Region "Module Variables"

    Dim WithEvents mOutFileWatcher As New System.IO.FileSystemWatcher
    Dim WithEvents mDtaFileDeleteTimer As System.Timers.Timer

    ' The following holds the file names of out files that have been created
    ' Every DTA_DELETE_INTERVAL_SECONDS, will look for candidates older than DTA_DELETE_HOLDOFF_SECONDS 
    ' For each, will delete the corresponding DTA file, then remove from mOutFileCandidates
    Dim mOutFileCandidates As New System.Collections.Queue
    Dim mDtaFilesDeleted As New System.Collections.Generic.SortedList(Of String, System.DateTime)

    Dim mSequestVersionInfoStored As Boolean

    Dim WithEvents m_CmdRunner As clsRunDosProgram
    Dim m_ErrMsg As String = ""
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Does nothing at present</remarks>
    Public Sub New()
    End Sub

    ''' <summary>
    ''' Modifies base class Setup method to provide log message appropriate for this class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object providing tools for status file updates</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.Setup()")
        End If

    End Sub

    ''' <summary>
    ''' Modifies MakeOUTFiles to remove multiple processes used on non-clustered machines
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Protected Overrides Function MakeOUTFiles() As IJobParams.CloseOutType

        'Creates Sequest .out files from DTA files
        Dim CmdStr As String
        Dim ResCode As Boolean
        Dim OutFiles() As String

        mOutFileCandidates.Clear()
        mDtaFilesDeleted.Clear()
        mSequestVersionInfoStored = False

        ' Initialize the out file watcher
        With mOutFileWatcher
            .BeginInit()
            .Path = m_WorkDir
            .IncludeSubdirectories = False
            .Filter = "*.out"
            .NotifyFilter = System.IO.NotifyFilters.FileName
            .EndInit()
            .EnableRaisingEvents = True
        End With

        ' Initialize the DTA deletion timer
        mDtaFileDeleteTimer = New System.Timers.Timer(DTA_DELETE_INTERVAL_SECONDS * 1000)
        mDtaFileDeleteTimer.Start()

        m_CmdRunner = New clsRunDosProgram(m_WorkDir)

        'Run the OUT file generation program
        CmdStr = " -P" & m_jobParams.GetParam("parmFileName") & " *.dta"
        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), making files")
        End If

        ResCode = m_CmdRunner.RunProgram(m_mgrParams.GetParam("seqprogloc"), CmdStr, "Seq", True)
        If Not ResCode Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unknown error making OUT files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Disable the Out File Watcher and the Dta File delete timer
        mOutFileWatcher.EnableRaisingEvents = False
        mDtaFileDeleteTimer.Stop()

        'Make sure objects are released
        System.Threading.Thread.Sleep(10000)         '10 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        'Verify out file creation
        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), verifying out file creation")
        End If
        OutFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.out")
        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), outfile count: " & OutFiles.GetLength(0).ToString)
        End If

        If Not mSequestVersionInfoStored Then
            ' Tool version not yet recorded; record it now
            If OutFiles.Length > 0 Then
                ' Pass the path to the first out file created
                mSequestVersionInfoStored = StoreToolVersionInfo(OutFiles(0))
            Else
                mSequestVersionInfoStored = StoreToolVersionInfo(String.Empty)
            End If
        End If

        If OutFiles.Length < 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "No OUT files created, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "No OUT files created")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        End If

        'Package out files into concatenated text files 
        If Not ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Zip concatenated .out files
        If Not ZipConcatOutFile(m_WorkDir, m_JobNum) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add .out extension to list of file extensions to delete
        clsGlobal.m_FilesToDeleteExt.Add(".out")

        'Add cluster statistics to summary file
        AddClusterStatsToSummaryFile()

        'If we got here, everything worked
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Provides a wait loop while Sequest is running
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub m_CmdRunner_LoopWaiting() Handles m_CmdRunner.LoopWaiting
        Static dtLastOutFileCountTime As System.DateTime = System.DateTime.UtcNow
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        ' Compute the progress by comparing the number of .Out files to the number of .Dta files 
        ' (only count the files every 10 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastOutFileCountTime).TotalSeconds >= 10 Then
            dtLastOutFileCountTime = System.DateTime.UtcNow
            CalculateNewStatus(False)
        End If

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)
        End If

    End Sub

    ''' <summary>
    ''' Reads sequest.log file after Sequest finishes and adds cluster statistics info to summary file
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub AddClusterStatsToSummaryFile()

        Dim SeqLogFilePath As String = System.IO.Path.Combine(m_WorkDir, "sequest.log")
        Dim NumNodeMachines As Integer
        Dim NumSlaveProcesses As Integer
        Dim TotalSearchTime As Integer
        Dim SearchedFileCount As Integer
        Dim AvgSearchTime As Single

        'Verify sequest.log file exists
        If Not System.IO.File.Exists(SeqLogFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Sequest log file not found for job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            Exit Sub
        End If

        'Read the sequest.log file
        Dim fileContents As String
        Try
            fileContents = My.Computer.FileSystem.ReadAllText(SeqLogFilePath)
        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception reading sequest log file: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        End Try

        'Node machine count
        NumNodeMachines = GetIntegerFromSeqLogFileString(fileContents, "starting the sequest task on\s+\d+\s+node")
        If NumNodeMachines = 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), node machine count line not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        ElseIf NumNodeMachines < 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving node machine count: " & m_ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        End If

        'Sequest process count
        NumSlaveProcesses = GetIntegerFromSeqLogFileString(fileContents, "Spawned\s+\d+\s+slave processes")
        If NumSlaveProcesses = 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), slave process count line not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        ElseIf NumSlaveProcesses < 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving slave process count: " & m_ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        End If

        'Total search time
        TotalSearchTime = GetIntegerFromSeqLogFileString(fileContents, "Total search time:\s+\d+")
        If TotalSearchTime = 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), total search time line not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        ElseIf TotalSearchTime < 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving total search time: " & m_ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        End If

        'Searched file count
        SearchedFileCount = GetIntegerFromSeqLogFileString(fileContents, "secs for\s+\d+\s+files")
        If SearchedFileCount = 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), searched file count line not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        ElseIf SearchedFileCount < 0 Then
            Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving searched file count: " & m_ErrMsg
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, Msg)
            Exit Sub
        End If

        'Average search time
        AvgSearchTime = CSng(TotalSearchTime) / CSng(SearchedFileCount)
        'AvgSearchTime = GetSingleFromSeqLogFileString(fileContents, "Average search time:\s+\d+\.\d+")
        'If AvgSearchTime = 0 Then
        '	Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), ave search time line not found"
        '	m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
        '	Exit Sub
        'ElseIf AvgSearchTime < 0 Then
        '	Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving ave search time: " & m_ErrMsg
        '	m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
        '	Exit Sub
        'End If

        'Write the statistics to the summary file
        clsSummaryFile.Add(vbCrLf & "Cluster node machine count: " & NumNodeMachines.ToString)
        clsSummaryFile.Add("Sequest process count: " & NumSlaveProcesses.ToString)
        clsSummaryFile.Add("Searched file count: " & SearchedFileCount.ToString)
        clsSummaryFile.Add("Total search time: " & TotalSearchTime.ToString & " secs")
        clsSummaryFile.Add("Ave search time: " & AvgSearchTime.ToString("##0.000") & " secs" & vbCrLf)

    End Sub

    ''' <summary>
    ''' Finds specified integer value in a sequest.log file
    ''' </summary>
    ''' <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
    ''' <param name="RegexStr">Regular expresion match string to uniquely identify the line containing the count of interest</param>
    ''' <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
    ''' <remarks>If -1 returned, error message is in module variable m_ErrMsg</remarks>
    Private Function GetIntegerFromSeqLogFileString(ByVal InpFileStr As String, ByVal RegexStr As String) As Integer

        Dim RetVal As Integer = 0
        Dim TmpStr As String

        Try
            'Find the specified substring in the input file string
            TmpStr = System.Text.RegularExpressions.Regex.Match(InpFileStr, RegexStr, System.Text.RegularExpressions.RegexOptions.IgnoreCase Or _
                                                                                      System.Text.RegularExpressions.RegexOptions.Multiline).Value
            If TmpStr = "" Then Return 0

            'Find the item count in the substring
            RetVal = CInt(System.Text.RegularExpressions.Regex.Match(TmpStr, "\d+").Value)
            Return RetVal
        Catch ex As Exception
            m_ErrMsg = ex.Message
            Return -1
        End Try

    End Function

    ''' <summary>
    ''' Finds specified integer value in a sequest.log file
    ''' </summary>
    ''' <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
    ''' <param name="RegexStr">Regular expresion match string to uniquely identify the line containing the count of interest</param>
    ''' <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
    ''' <remarks>If -1 returned, error message is in module variable m_ErrMsg</remarks>
    Private Function GetSingleFromSeqLogFileString(ByVal InpFileStr As String, ByVal RegexStr As String) As Single

        Dim RetVal As Single = 0.0
        Dim TmpStr As String

        Try
            'Find the specified substring in the input file string
            TmpStr = System.Text.RegularExpressions.Regex.Match(InpFileStr, RegexStr, System.Text.RegularExpressions.RegexOptions.IgnoreCase Or _
                                                                                      System.Text.RegularExpressions.RegexOptions.Multiline).Value
            If TmpStr = "" Then Return 0.0

            'Find the item count in the substring
            RetVal = CSng(System.Text.RegularExpressions.Regex.Match(TmpStr, "\d+\.\d+").Value)
            Return RetVal
        Catch ex As Exception
            m_ErrMsg = ex.Message
            Return -1.0
        End Try

    End Function

    ''' <summary>
    ''' Adds newly created .Out file to Queue mOutFileCandidates
    ''' This queue is used to delete corresponding .DTA files as the .Out files are created
    ''' </summary>
    ''' <param name="OutFileName"></param>
    ''' <remarks></remarks>
    Private Sub HandleOutFileChange(ByVal OutFileName As String)

        Try

            If String.IsNullOrEmpty(OutFileName) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "OutFileName is empty; this is unexpected")
                End If
            Else
                Dim objEntry As New System.Collections.Generic.KeyValuePair(Of String, System.DateTime)(OutFileName, System.DateTime.UtcNow)

                If m_DebugLevel >= 5 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching new out file: " & objEntry.Key)
                End If

                mOutFileCandidates.Enqueue(objEntry)
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error adding new candidate to mOutFileCandidates (" & OutFileName & "): " & ex.Message)
            End If
        End Try

    End Sub

    Private Sub ProcessCandidateDtaFiles()

        Dim strDtaFilePath As String = String.Empty

        Dim objEntry As System.Collections.Generic.KeyValuePair(Of String, System.DateTime)
        Dim blnContinue As Boolean

        Dim intItemsDequeued As Integer = 0
        Dim intDtaCountDeleted As Integer = 0

        Try
            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Examining out file creation dates (Candidate Count = " & mOutFileCandidates.Count & ")")
            End If

            If mOutFileCandidates.Count > 0 And Not mSequestVersionInfoStored Then
                ' Determine tool version

                ' Pass the path to the first out file created
                objEntry = CType(mOutFileCandidates.Peek, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))
                If StoreToolVersionInfo(System.IO.Path.Combine(m_WorkDir, objEntry.Key)) Then
                    mSequestVersionInfoStored = True
                End If
            End If

            blnContinue = True
            Do While mOutFileCandidates.Count > 0 AndAlso blnContinue

                ' Examine the time associated with the next item that would be dequeued
                objEntry = CType(mOutFileCandidates.Peek, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))

                If System.DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= DTA_DELETE_HOLDOFF_SECONDS Then

                    ' Entry is old enough; pop it off the queue
                    objEntry = CType(mOutFileCandidates.Dequeue, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))
                    intItemsDequeued += 1

                    strDtaFilePath = System.IO.Path.GetFileNameWithoutExtension(objEntry.Key) & ".dta"
                    If Not mDtaFilesDeleted.ContainsKey(strDtaFilePath) Then
                        ' DTA file not yet deleted
                        mDtaFilesDeleted.Add(strDtaFilePath, System.DateTime.UtcNow)

                        Try
                            strDtaFilePath = System.IO.Path.Combine(m_WorkDir, strDtaFilePath)

                            If m_DebugLevel >= 5 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting DTA file: " & strDtaFilePath)
                            End If

                            System.IO.File.Delete(strDtaFilePath)
                            intDtaCountDeleted += 1

                        Catch ex As Exception
                            ' Log the error, but continue
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting DTA file: " & ex.Message)
                        End Try

                    End If

                Else
                    blnContinue = False
                End If

            Loop

            If intItemsDequeued > 0 AndAlso m_DebugLevel >= 3 Then
                If intItemsDequeued > intDtaCountDeleted Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dequeued " & intItemsDequeued & " entries and deleted " & intDtaCountDeleted & " processed DTA files; " & mOutFileCandidates.Count & " remain in queue")
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleted " & intDtaCountDeleted & " processed DTA files; " & mOutFileCandidates.Count & " remain in queue")
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessCandidateDtaFiles: " & ex.Message)
        End Try

    End Sub

    Private Sub mOutFileWatcher_Created(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Created
        HandleOutFileChange(e.Name)
    End Sub

    Private Sub mOutFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Changed
        HandleOutFileChange(e.Name)
    End Sub

    Private Sub mDtaFileDeleteTimer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles mDtaFileDeleteTimer.Elapsed
        ProcessCandidateDtaFiles()
    End Sub

#End Region

End Class
