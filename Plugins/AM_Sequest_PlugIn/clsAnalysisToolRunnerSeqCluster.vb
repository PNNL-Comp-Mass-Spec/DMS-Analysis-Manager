'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/17/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerSeqCluster
	Inherits clsAnalysisToolRunnerSeqBase

	'*********************************************************************************************************
	'Overrides Sequest tool runner to provide cluster-specific methods
	'*********************************************************************************************************

	Private Const TEMP_FILE_COPY_INTERVAL_SECONDS As Integer = 300
	Private Const OUT_FILE_APPEND_INTERVAL_SECONDS As Integer = 30
	Private Const OUT_FILE_APPEND_HOLDOFF_SECONDS As Integer = 30

#Region "Module Variables"

	Protected WithEvents mOutFileWatcher As New System.IO.FileSystemWatcher
	Protected WithEvents mOutFileAppenderTimer As System.Timers.Timer

	' The following holds the file names of out files that have been created
	' Every OUT_FILE_APPEND_INTERVAL_SECONDS, will look for candidates older than OUT_FILE_APPEND_HOLDOFF_SECONDS 
	' For each, will append the data to the _out.txt.tmp file, delete the corresponding DTA file, and remove from mOutFileCandidates
	Protected mOutFileCandidates As New System.Collections.Queue()
	Protected mOutFileCandidateInfo As New System.Collections.Generic.Dictionary(Of String, System.DateTime)

	Protected mOutFileHandlerInUse As Long

	Protected mSequestVersionInfoStored As Boolean

	Protected mTempJobParamsCopied As Boolean
	Protected mLastTempFileCopyTime As System.DateTime
	Protected mTransferFolderPath As String

	Protected WithEvents m_CmdRunner As clsRunDosProgram
	Protected m_ErrMsg As String = ""
#End Region

#Region "Methods"

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
		Dim ProgLoc As String

		mOutFileCandidates.Clear()
		mOutFileCandidateInfo.Clear()
		mOutFileNamesAppended.Clear()
		mOutFileHandlerInUse = 0

		mSequestVersionInfoStored = False
		mTempJobParamsCopied = False
		mLastTempFileCopyTime = System.DateTime.UtcNow

		mTransferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath")
		mTransferFolderPath = System.IO.Path.Combine(mTransferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"))
		mTransferFolderPath = System.IO.Path.Combine(mTransferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"))

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

		' Initialize the Out File Appender timer
		mOutFileAppenderTimer = New System.Timers.Timer(OUT_FILE_APPEND_INTERVAL_SECONDS * 1000)
		mOutFileAppenderTimer.Start()

		m_CmdRunner = New clsRunDosProgram(m_WorkDir)

		ProgLoc = m_mgrParams.GetParam("seqprogloc")
		If Not System.IO.File.Exists(ProgLoc) Then
			m_message = "Sequest .Exe not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & ProgLoc)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Run the OUT file generation program
		CmdStr = " -P" & m_jobParams.GetParam("parmFileName") & " *.dta"
		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), making files")
		End If

		ResCode = m_CmdRunner.RunProgram(ProgLoc, CmdStr, "Seq", True)
		If Not ResCode Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unknown error making OUT files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Disable the Out File Watcher and the Out File Appender timers
		mOutFileWatcher.EnableRaisingEvents = False
		mOutFileAppenderTimer.Stop()

		'Make sure objects are released
		System.Threading.Thread.Sleep(5000)		 ' 5 second delay
		GC.Collect()
		GC.WaitForPendingFinalizers()

		'Verify out file creation
		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), verifying out file creation")
		End If
		OutFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.out")
		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), outfile count: " & (OutFiles.Length + mTotalOutFileCount))
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

		If (mTotalOutFileCount + OutFiles.Length) < 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "No OUT files created, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			m_message = clsGlobal.AppendToComment(m_message, "No OUT files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		End If

		' Append any remaining .out files to the _out.txt.tmp file, then rename it to _out.txt
		If Not ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Add .out extension to list of file extensions to delete
		m_JobParams.AddResultFileExtensionToSkip(".out")

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
		m_SummaryFile.Add(vbCrLf & "Cluster node machine count: " & NumNodeMachines.ToString)
		m_SummaryFile.Add("Sequest process count: " & NumSlaveProcesses.ToString)
		m_SummaryFile.Add("Searched file count: " & SearchedFileCount.ToString)
		m_SummaryFile.Add("Total search time: " & TotalSearchTime.ToString & " secs")
		m_SummaryFile.Add("Ave search time: " & AvgSearchTime.ToString("##0.000") & " secs" & vbCrLf)

	End Sub

	Private Function CopyFileToTransferFolder(ByVal strSourceFileName As String, ByVal strTargetFileName As String) As Boolean

		Dim strSourceFilePath As String
		Dim strTargetFilePath As String

		Try
			strSourceFilePath = System.IO.Path.Combine(m_WorkDir, strSourceFileName)
			strTargetFilePath = System.IO.Path.Combine(mTransferFolderPath, strTargetFileName)

			If System.IO.File.Exists(strSourceFilePath) Then

				If Not System.IO.Directory.Exists(mTransferFolderPath) Then
					System.IO.Directory.CreateDirectory(mTransferFolderPath)
				End If

				System.IO.File.Copy(strSourceFilePath, strTargetFilePath, True)
			End If

		Catch ex As Exception
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error copying file " & strSourceFileName & " to " & mTransferFolderPath & ": " & ex.Message)
			End If
			Return False
		End Try

		Return True

	End Function

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
	''' Adds newly created .Out file to mOutFileCandidates and mOutFileCandidateInfo
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
				If m_DebugLevel >= 5 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching new out file: " & OutFileName)
				End If

				If Not mOutFileCandidateInfo.ContainsKey(OutFileName) Then
					Dim dtQueueTime As System.DateTime = System.DateTime.UtcNow
					Dim objEntry As New System.Collections.Generic.KeyValuePair(Of String, System.DateTime)(OutFileName, dtQueueTime)

					mOutFileCandidates.Enqueue(objEntry)
					mOutFileCandidateInfo.Add(OutFileName, dtQueueTime)
				End If

			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error adding new candidate to mOutFileCandidates (" & OutFileName & "): " & ex.Message)
			End If
		End Try

	End Sub

	Private Sub ProcessCandidateOutFiles()

		Dim strDtaFilePath As String = String.Empty
		Dim strSourceFileName As String
		Dim blnSuccess As Boolean
		Dim blnContinue As Boolean

		Dim objEntry As System.Collections.Generic.KeyValuePair(Of String, System.DateTime)

		Dim intItemsProcessed As Integer = 0

		' Examine mOutFileHandlerInUse; if greater then zero, then exit the sub
		If System.Threading.Interlocked.Read(mOutFileHandlerInUse) > 0 Then
			Exit Sub
		End If

		Try
			System.Threading.Interlocked.Increment(mOutFileHandlerInUse)

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

			If String.IsNullOrEmpty(mTempConcatenatedOutFilePath) Then
				mTempConcatenatedOutFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_out.txt.tmp")
			End If

			If mOutFileCandidates.Count > 0 Then
				' Examine the time associated with the next item that would be dequeued
				objEntry = CType(mOutFileCandidates.Peek, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))
				If System.DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS Then

					' Open the _out.txt.tmp file
					Using swTargetFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(mTempConcatenatedOutFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))

						blnContinue = True
						intItemsProcessed = 0
						Do While mOutFileCandidates.Count > 0 AndAlso blnContinue AndAlso System.DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS

							' Entry is old enough; pop it off the queue
							objEntry = CType(mOutFileCandidates.Dequeue, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))
							intItemsProcessed += 1

							Try
								Dim fiOutFile As System.IO.FileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, objEntry.Key))

								If Not mOutFileNamesAppended.Contains(fiOutFile.Name) Then
									AppendOutFile(fiOutFile, swTargetFile)
									mTotalOutFileCount += 1
								End If

								If mOutFileCandidates.Count > 0 Then
									objEntry = CType(mOutFileCandidates.Peek, System.Collections.Generic.KeyValuePair(Of String, System.DateTime))
								End If

							Catch ex As Exception
								Console.WriteLine("Warning, exception appending out file: " & ex.Message)
								blnContinue = False
							End Try

						Loop


					End Using
				End If
			End If

			If intItemsProcessed > 0 Then

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Appended " & intItemsProcessed & " .out files to the _out.txt.tmp file; " & mOutFileCandidates.Count & " out files remain in the queue")
				End If

				If System.DateTime.UtcNow.Subtract(mLastTempFileCopyTime).TotalSeconds >= TEMP_FILE_COPY_INTERVAL_SECONDS Then
					If Not mTempJobParamsCopied Then
						strSourceFileName = "JobParameters_" & m_JobNum & ".xml"
						blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName & ".tmp")

						If blnSuccess Then
							strSourceFileName = m_jobParams.GetParam("ParmFileName")
							blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName & ".tmp")
						End If

						If blnSuccess Then
							mTempJobParamsCopied = True
						End If

					End If

					' Copy the _out.txt.tmp file
					strSourceFileName = System.IO.Path.GetFileName(mTempConcatenatedOutFilePath)
					blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName)

					' Copy the sequest.log file
					strSourceFileName = "sequest.log"
					blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName)

					mLastTempFileCopyTime = System.DateTime.UtcNow

				End If

			End If

		Catch ex As Exception
			Console.WriteLine("Warning, error in ProcessCandidateOutFiles: " & ex.Message)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessCandidateOutFiles: " & ex.Message)
		Finally
			' Make sure mOutFileHandlerInUse is now zero
			Dim lngZero As Long = 0
			System.Threading.Interlocked.Exchange(mOutFileHandlerInUse, lngZero)
		End Try

	End Sub

	Private Sub mOutFileWatcher_Created(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Created
		HandleOutFileChange(e.Name)
	End Sub

	Private Sub mOutFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Changed
		HandleOutFileChange(e.Name)
	End Sub

	Private Sub mOutFileAppenderTime_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles mOutFileAppenderTimer.Elapsed
		ProcessCandidateOutFiles()
	End Sub

#End Region

End Class
