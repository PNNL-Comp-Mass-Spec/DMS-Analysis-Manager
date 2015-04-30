'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

''' <summary>
''' Overrides Sequest tool runner to provide cluster-specific methods
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerSeqCluster
	Inherits clsAnalysisToolRunnerSeqBase

#Region "Constants"
	Protected Const TEMP_FILE_COPY_INTERVAL_SECONDS As Integer = 300
	Protected Const OUT_FILE_APPEND_INTERVAL_SECONDS As Integer = 30
	Protected Const OUT_FILE_APPEND_HOLDOFF_SECONDS As Integer = 30
	Protected Const STALE_NODE_THRESHOLD_MINUTES As Integer = 5
	Protected Const MAX_NODE_RESPAWN_ATTEMPTS As Integer = 6
#End Region

#Region "Structures"
	Protected Structure udtSequestNodeProcessingStats
		Public NumNodeMachines As Integer
		Public NumSlaveProcesses As Integer
		Public TotalSearchTimeSeconds As Double
		Public SearchedFileCount As Integer
		Public AvgSearchTime As Single
		Public Sub Clear()
			NumNodeMachines = 0
			NumSlaveProcesses = 0
			TotalSearchTimeSeconds = 0
			SearchedFileCount = 0
			AvgSearchTime = 0
		End Sub
	End Structure
#End Region
#Region "Module Variables"

	Protected WithEvents mOutFileWatcher As New System.IO.FileSystemWatcher
	Protected WithEvents mOutFileAppenderTimer As System.Timers.Timer

	' The following holds the file names of out files that have been created
	' Every OUT_FILE_APPEND_INTERVAL_SECONDS, will look for candidates older than OUT_FILE_APPEND_HOLDOFF_SECONDS 
	' For each, will append the data to the _out.txt.tmp file, delete the corresponding DTA file, and remove from mOutFileCandidates
	Protected mOutFileCandidates As Queue(Of KeyValuePair(Of String, System.DateTime)) = New Queue(Of KeyValuePair(Of String, System.DateTime))
	Protected mOutFileCandidateInfo As Dictionary(Of String, System.DateTime) = New Dictionary(Of String, System.DateTime)

	Protected mLastOutFileStoreTime As System.DateTime
	Protected mSequestAppearsStalled As Boolean
	Protected mAbortSinceSequestIsStalled As Boolean

	Protected mSequestVersionInfoStored As Boolean

	Protected mTempJobParamsCopied As Boolean
	Protected mLastTempFileCopyTime As System.DateTime
	Protected mTransferFolderPath As String

	Protected mLastOutFileCountTime As System.DateTime = System.DateTime.UtcNow
	Protected mLastActiveNodeQueryTime As System.DateTime = System.DateTime.UtcNow

	Protected mLastActiveNodeLogTime As System.DateTime

	Protected mResetPVM As Boolean
	Protected mNodeCountSpawnErrorOccurences As Integer
	Protected mNodeCountActiveErrorOccurences As Integer
	Protected mLastSequestStartTime As System.DateTime

	Protected WithEvents m_CmdRunner As clsRunDosProgram
	Protected WithEvents m_UtilityRunner As clsRunDosProgram

	Protected mUtilityRunnerTaskName As String = String.Empty

	Protected m_ErrMsg As String = ""

	' This dictionary tracks the most recent time each node was observed via PVM command "ps -a"
	Protected mSequestNodes As New Dictionary(Of String, System.DateTime)

	Protected mSequestLogNodesFound As Boolean
	Protected mSequestNodesSpawned As Integer
	Protected mIgnoreNodeCountActiveErrors As Boolean

	Protected mSequestNodeProcessingStats As udtSequestNodeProcessingStats

	Protected mSequestSearchStartTime As System.DateTime
	Protected mSequestSearchEndTime As System.DateTime

	Protected m_ActiveNodeRegEx As System.Text.RegularExpressions.Regex = _
	  New System.Text.RegularExpressions.Regex("\s+(?<node>[a-z0-9-.]+\s+[a-z0-9]+)\s+.+sequest.+slave.*", _
	  RegexOptions.IgnoreCase Or RegexOptions.CultureInvariant Or RegexOptions.Compiled)

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
		Dim OutFiles() As String
		Dim ProgLoc As String

		Dim diWorkDir As System.IO.DirectoryInfo

		Dim intDTACountRemaining As Integer
		Dim blnSuccess As Boolean
		Dim blnProcessingError As Boolean

		mOutFileCandidates.Clear()
		mOutFileCandidateInfo.Clear()
		mOutFileNamesAppended.Clear()
		mOutFileHandlerInUse = 0

		mSequestNodeProcessingStats.Clear()

		mSequestVersionInfoStored = False
		mTempJobParamsCopied = False

		mLastTempFileCopyTime = System.DateTime.UtcNow
		mLastActiveNodeLogTime = System.DateTime.UtcNow
		mLastOutFileStoreTime = System.DateTime.UtcNow
		mSequestAppearsStalled = False
		mAbortSinceSequestIsStalled = False

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

		ProgLoc = m_mgrParams.GetParam("seqprogloc")
		If Not System.IO.File.Exists(ProgLoc) Then
			m_message = "Sequest .Exe not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & ProgLoc)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Initialize the Out File Appender timer
		mOutFileAppenderTimer = New System.Timers.Timer(OUT_FILE_APPEND_INTERVAL_SECONDS * 1000)
		mOutFileAppenderTimer.Start()

		diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

		If diWorkDir.GetFiles("sequest*.log*").Length > 0 Then
			' Parse any sequest.log files present in the work directory to determine the number of spectra already processed
			UpdateSequestNodeProcessingStats(True)
		End If

		mNodeCountSpawnErrorOccurences = 0
		mNodeCountActiveErrorOccurences = 0
		mLastSequestStartTime = System.DateTime.UtcNow

		mIgnoreNodeCountActiveErrors = m_jobParams.GetJobParameter("IgnoreSequestNodeCountActiveErrors", False)

		Do
			' Reset several pieces of information on each iteration of this Do Loop
			mSequestNodes.Clear()
			mSequestLogNodesFound = False
			mSequestNodesSpawned = 0
			mResetPVM = False
			mSequestSearchStartTime = System.DateTime.UtcNow
			mSequestSearchEndTime = System.DateTime.UtcNow

			mLastOutFileCountTime = System.DateTime.UtcNow
			mLastActiveNodeQueryTime = System.DateTime.UtcNow

			m_CmdRunner = New clsRunDosProgram(m_WorkDir)

			' Define the arguments to pass to the Sequest .Exe
			CmdStr = " -P" & m_jobParams.GetParam("parmFileName") & " *.dta"
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  " & ProgLoc & " " & CmdStr)
			End If

			' Run Sequest to generate OUT files
			mLastSequestStartTime = System.DateTime.UtcNow
			blnSuccess = m_CmdRunner.RunProgram(ProgLoc, CmdStr, "Seq", True)

			mSequestSearchEndTime = System.DateTime.UtcNow

			If blnSuccess And Not mResetPVM And Not mAbortSinceSequestIsStalled Then
				intDTACountRemaining = 0
			Else

				If Not mResetPVM And Not mAbortSinceSequestIsStalled Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... CmdRunner returned false; ExitCode = " & m_CmdRunner.ExitCode)
				End If

				' Check whether any .DTA files remain for this dataset
				intDTACountRemaining = GetDTAFileCountRemaining()

				If intDTACountRemaining > 0 Then

					blnSuccess = False
					If mNodeCountSpawnErrorOccurences < MAX_NODE_RESPAWN_ATTEMPTS And mNodeCountActiveErrorOccurences < MAX_NODE_RESPAWN_ATTEMPTS Then
						Dim intMaxPVMResetAttempts As Integer = 4

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Resetting PVM in MakeOUTFiles")
						blnSuccess = ResetPVMWithRetry(intMaxPVMResetAttempts)

					End If

					If Not blnSuccess Then
						' Log message "Error resetting PVM; disabling manager locally"
						m_message = PVM_RESET_ERROR_MESSAGE
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, PVM_RESET_ERROR_MESSAGE & "; disabling manager locally")
						m_NeedToAbortProcessing = True
						blnProcessingError = True
						Exit Do
					End If

				Else
					' No .DTAs remain; if we have as many .out files as the original source .dta files, then treat this as success, otherwise as a failure
					Dim intOutFileCount As Integer
					intOutFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount

					If intOutFileCount = m_DtaCount Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... The number of OUT files (" & intOutFileCount & ") is equivalent to the original DTA count (" & m_DtaCount & "); we'll consider this a successful job despite the Sequest CmdRunner error")
					ElseIf intOutFileCount > m_DtaCount Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... The number of OUT files (" & intOutFileCount & ") is greater than the original DTA count (" & m_DtaCount & "); we'll consider this a successful job despite the Sequest CmdRunner error")
					ElseIf intOutFileCount >= CInt(m_DtaCount * 0.999) Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... The number of OUT files (" & intOutFileCount & ") is within 0.1% of the original DTA count (" & m_DtaCount & "); we'll consider this a successful job despite the Sequest CmdRunner error")
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No DTA files remain and the number of OUT files (" & intOutFileCount & ") is less than the original DTA count (" & m_DtaCount & "); treating this as a job failure")
						blnProcessingError = True
					End If

				End If
			End If

		Loop While intDTACountRemaining > 0

		' Disable the Out File Watcher and the Out File Appender timers
		mOutFileWatcher.EnableRaisingEvents = False
		mOutFileAppenderTimer.Stop()

		' Make sure objects are released
		System.Threading.Thread.Sleep(5000)		 ' 5 second delay
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		UpdateSequestNodeProcessingStats(False)

		' Verify out file creation
		If m_DebugLevel >= 2 And Not blnProcessingError Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... Verifying out file creation")
		End If

		OutFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.out")
		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... Outfile count: " & (OutFiles.Length + mTotalOutFileCount).ToString("#,##0") & " files")
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
			blnProcessingError = True
		End If

		Dim intIterationsRemaining As Integer = 3
		Do
			' Process the remaining .Out files in mOutFileCandidates
			If Not ProcessCandidateOutFiles(True) Then
				' Wait 5 seconds, then try again (up to 3 times)
				System.Threading.Thread.Sleep(5000)
			End If

			intIterationsRemaining -= 1
		Loop While mOutFileCandidates.Count > 0 AndAlso intIterationsRemaining >= 0

		' Append any remaining .out files to the _out.txt.tmp file, then rename it to _out.txt
		If ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum) Then
			' Add .out extension to list of file extensions to delete
			m_jobParams.AddResultFileExtensionToSkip(".out")
		Else
			blnProcessingError = True
		End If

		If blnProcessingError Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE
		End If

		' Add cluster statistics to summary file
		AddClusterStatsToSummaryFile()

		' If we got here, everything worked
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Provides a wait loop while Sequest is running
	''' </summary>
	''' <remarks></remarks>
	Protected Sub m_CmdRunner_LoopWaiting() Handles m_CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		' Compute the progress by comparing the number of .Out files to the number of .Dta files 
		' (only count the files every 15 seconds)
		If System.DateTime.UtcNow.Subtract(mLastOutFileCountTime).TotalSeconds >= 15 Then
			mLastOutFileCountTime = System.DateTime.UtcNow
			CalculateNewStatus()
		End If

		If System.DateTime.UtcNow.Subtract(mLastActiveNodeQueryTime).TotalSeconds >= 120 Then
			mLastActiveNodeQueryTime = System.DateTime.UtcNow

			' Verify that nodes are still analyzing .dta files
			' This procedure will set mResetPVM to True if less than 50% of the nodes are creating .Out files
			ValidateProcessorsAreActive()

			' Look for .Out files that aren't yet tracked by mOutFileCandidateInfo
			CacheNewOutFiles()
		End If

		' Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)
		End If

		If mResetPVM Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... calling m_CmdRunner.AbortProgramNow in LoopWaiting since mResetPVM = True")
			m_CmdRunner.AbortProgramNow(False)
		ElseIf mAbortSinceSequestIsStalled Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... calling m_CmdRunner.AbortProgramNow in LoopWaiting since mAbortSinceSequestIsStalled = True")
			m_CmdRunner.AbortProgramNow(False)
		End If

	End Sub

	''' <summary>
	''' Reads sequest.log file after Sequest finishes and adds cluster statistics info to summary file
	''' </summary>
	''' <remarks></remarks>
	Protected Sub AddClusterStatsToSummaryFile()

		' Write the statistics to the summary file
		m_SummaryFile.Add(Environment.NewLine & "Cluster node count: ".PadRight(24) & mSequestNodeProcessingStats.NumNodeMachines.ToString)
		m_SummaryFile.Add("Sequest process count: ".PadRight(24) & mSequestNodeProcessingStats.NumSlaveProcesses.ToString)
		m_SummaryFile.Add("Searched file count: ".PadRight(24) & mSequestNodeProcessingStats.SearchedFileCount.ToString("#,##0"))
		m_SummaryFile.Add("Total search time: ".PadRight(24) & mSequestNodeProcessingStats.TotalSearchTimeSeconds.ToString("#,##0") & " secs")
		m_SummaryFile.Add("Average search time: ".PadRight(24) & mSequestNodeProcessingStats.AvgSearchTime.ToString("##0.000") & " secs/spectrum")

	End Sub

	Protected Sub CacheNewOutFiles()
		Dim diWorkDir As System.IO.DirectoryInfo

		Try
			diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

			For Each fiFile As System.IO.FileInfo In diWorkDir.GetFiles("*.out", IO.SearchOption.TopDirectoryOnly)
				HandleOutFileChange(fiFile.Name)
			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error in CacheNewOutFiles: " & ex.Message)
		End Try
	End Sub

	Protected Sub CheckForStalledSequest()

		Const SEQUEST_STALLED_WAIT_TIME_MINUTES As Integer = 30

		Try
			Dim dblMinutesSinceLastOutFileStored As Double = System.DateTime.UtcNow.Subtract(mLastOutFileStoreTime).TotalMinutes

			If dblMinutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES Then

				Dim blnResetPVM As Boolean = False

				If mSequestAppearsStalled Then
					If dblMinutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES * 2 Then
						' We already reset SEQUEST once, and another 30 minutes has elapsed
						' Examine the number of .dta files that remain
						Dim intDTAsRemaining As Integer = GetDTAFileCountRemaining()

						If intDTAsRemaining <= CInt(m_DtaCount * 0.999) Then

							' Just a handful of DTA files remain; assume they're corrupt
							Dim diWorkDir As System.IO.DirectoryInfo
							diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest is stalled, and " & intDTAsRemaining & " .DTA file" & CheckForPlurality(intDTAsRemaining) & " remain; assuming they are corrupt and deleting them")
							m_EvalMessage = "Sequest is stalled, but only " & intDTAsRemaining & " .DTA file" & CheckForPlurality(intDTAsRemaining) & " remain"

							For Each fiFile As IO.FileInfo In diWorkDir.GetFiles("*.dta", System.IO.SearchOption.TopDirectoryOnly).ToList()
								fiFile.Delete()
							Next

						Else
							' Too many DTAs remain unprocessed and Sequest is stalled
							' Abort the job

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Sequest is stalled, and " & intDTAsRemaining & " .DTA files remain; aborting processing")
							m_message = "Sequest is stalled and too many .DTA files are un-processed"
							mAbortSinceSequestIsStalled = True

						End If

						blnResetPVM = True

					End If
				Else
					' Sequest appears stalled
					' Reset PVM, then wait another 30 minutes

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest has not created a new .out file in the last " & SEQUEST_STALLED_WAIT_TIME_MINUTES & " minutes; will Reset PVM then wait another " & SEQUEST_STALLED_WAIT_TIME_MINUTES & " minutes")

					blnResetPVM = True

					mSequestAppearsStalled = True

				End If

				If blnResetPVM Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Setting mResetPVM to True in CheckForStalledSequest")
					mResetPVM = True
				End If

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error in CheckForStalledSequest: " & ex.Message)
		End Try
	End Sub

	Protected Function CopyFileToTransferFolder(ByVal strSourceFileName As String, ByVal strTargetFileName As String, ByVal blnAddToListOfServerFilesToDelete As Boolean) As Boolean

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

				If blnAddToListOfServerFilesToDelete Then
					m_jobParams.AddServerFileToDelete(strTargetFilePath)
				End If
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
	Protected Function GetIntegerFromSeqLogFileString(ByVal InpFileStr As String, ByVal RegexStr As String) As Integer

		Dim RetVal As Integer = 0
		Dim TmpStr As String

		Try
			'Find the specified substring in the input file string
			TmpStr = Regex.Match(InpFileStr, RegexStr, RegexOptions.IgnoreCase Or RegexOptions.Multiline).Value

			If String.IsNullOrWhiteSpace(TmpStr) Then
				Return 0
			End If

			'Find the item count in the substring
			If Integer.TryParse(Regex.Match(TmpStr, "\d+").Value, RetVal) Then
				Return RetVal
			Else
				m_ErrMsg = "Numeric value not found in the matched text"
				Return -1
			End If

		Catch ex As Exception
			m_ErrMsg = ex.Message
			Return -1
		End Try

	End Function

	Protected Function GetNodeNamesFromSequestLog(ByVal strLogFilePath As String) As Boolean

		Dim reReceivedReadyMsg As System.Text.RegularExpressions.Regex
		Dim reSpawnedSlaveProcesses As System.Text.RegularExpressions.Regex

		Dim reMatch As System.Text.RegularExpressions.Match

		Dim strLineIn As String
		Dim strHostName As String

		Dim blnFoundSpawned As Boolean

		Try

			If Not System.IO.File.Exists(strLogFilePath) Then
				Return False
			End If

			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... extracting node names from sequest.log")
			End If

			' Initialize the RegEx objects
			reReceivedReadyMsg = New System.Text.RegularExpressions.Regex("received ready messsage from (.+)\(", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reSpawnedSlaveProcesses = New System.Text.RegularExpressions.Regex("Spawned (\d+) slave processes", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)

			mSequestNodesSpawned = 0
			Using srLogFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				' Read each line from the input file
                Do While Not srLogFile.EndOfStream
                    strLineIn = srLogFile.ReadLine

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        ' Check whether line looks like:
                        '    9.  received ready messsage from p6(c0002)

                        reMatch = reReceivedReadyMsg.Match(strLineIn)
                        If Not reMatch Is Nothing AndAlso reMatch.Success Then
                            strHostName = reMatch.Groups(1).Value

                            mSequestNodesSpawned += 1

                        Else
                            reMatch = reSpawnedSlaveProcesses.Match(strLineIn)
                            If Not reMatch Is Nothing AndAlso reMatch.Success Then
                                blnFoundSpawned = True
                            End If
                        End If
                    End If
                Loop

			End Using

			If blnFoundSpawned Then

				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... found " & mSequestNodesSpawned & " nodes in the sequest.log file")
				End If

				Dim intNodeCountMinimum As Integer
				Dim intNodeCountExpected As Integer

				intNodeCountExpected = m_mgrParams.GetParam("SequestNodeCountExpected", 0)
				intNodeCountMinimum = CInt(Math.Floor(0.85 * intNodeCountExpected))

				If mSequestNodesSpawned < intNodeCountMinimum Then

					' If fewer than intNodeCountMinimum .DTA files are present in the work directory, then the node count spawned will be small
					' Thus, need to count the number of DTAs					
					Dim intDTACountRemaining As Integer = GetDTAFileCountRemaining()

					If intDTACountRemaining > mSequestNodesSpawned Then
						mNodeCountSpawnErrorOccurences += 1

						Dim strMessage As String
						strMessage = "Not enough nodes were spawned (Threshold = " & intNodeCountMinimum & " nodes): " & mSequestNodesSpawned & " spawned vs. " & intNodeCountExpected & " expected; mNodeCountSpawnErrorOccurences=" & mNodeCountSpawnErrorOccurences
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)

						mResetPVM = True
					End If
				ElseIf mNodeCountSpawnErrorOccurences > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Resetting mNodeCountSpawnErrorOccurences from " & mNodeCountSpawnErrorOccurences & " to 0")
					mNodeCountSpawnErrorOccurences = 0
				End If

				Return True
			Else
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... Did not find 'Spawned xx slave processes' in the sequest.log file; node names not yet determined")
				End If

				If System.DateTime.UtcNow.Subtract(mLastSequestStartTime).TotalMinutes > 15 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... Over 15 minutes have elapsed since sequest.exe was called; aborting since node names could not be determined")

					mNodeCountSpawnErrorOccurences += 1
					mResetPVM = True
				End If
			End If

		Catch ex As Exception
			' Error occurred
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing Sequest.log file in ValidateSequestNodeCount: " & ex.Message)
			Return False
		End Try

		Return False

	End Function

	''' <summary>
	''' Finds specified integer value in a sequest.log file
	''' </summary>
	''' <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
	''' <param name="RegexStr">Regular expresion match string to uniquely identify the line containing the count of interest</param>
	''' <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
	''' <remarks>If -1 returned, error message is in module variable m_ErrMsg</remarks>
	Protected Function GetSingleFromSeqLogFileString(ByVal InpFileStr As String, ByVal RegexStr As String) As Single

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

	Protected Function ComputeMedianProcessingTime() As Single

		Dim sngOutFileProcessingTimes() As Single
		Dim intMidPoint As Integer

		If mRecentOutFileSearchTimes.Count < 1 Then Return 0

		' Determine the median out file processing time
		' Note that search times in mRecentOutFileSearchTimes are in seconds 
		ReDim sngOutFileProcessingTimes(mRecentOutFileSearchTimes.Count - 1)

		mRecentOutFileSearchTimes.CopyTo(sngOutFileProcessingTimes, 0)

		Array.Sort(sngOutFileProcessingTimes)
		If sngOutFileProcessingTimes.Length <= 2 Then
			intMidPoint = 0
		Else
			intMidPoint = CInt(Math.Floor(sngOutFileProcessingTimes.Length / 2))
		End If

		Return sngOutFileProcessingTimes(intMidPoint)

	End Function

	''' <summary>
	''' Adds newly created .Out file to mOutFileCandidates and mOutFileCandidateInfo
	''' </summary>
	''' <param name="OutFileName"></param>
	''' <remarks></remarks>
	Protected Sub HandleOutFileChange(ByVal OutFileName As String)

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
					Dim objEntry As New KeyValuePair(Of String, System.DateTime)(OutFileName, dtQueueTime)

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

	Protected Function InitializeUtilityRunner(ByVal strTaskName As String, ByVal strWorkDir As String) As Boolean
		Return InitializeUtilityRunner(strTaskName, strWorkDir, intMonitoringIntervalMsec:=1000)
	End Function

	Protected Function InitializeUtilityRunner(ByVal strTaskName As String, ByVal strWorkDir As String, ByVal intMonitoringIntervalMsec As Integer) As Boolean

		Try
			If m_UtilityRunner Is Nothing Then
				m_UtilityRunner = New clsRunDosProgram(strWorkDir)
			Else
				If m_UtilityRunner.State <> PRISM.Processes.clsProgRunner.States.NotMonitoring Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot re-initialize the UtilityRunner to perform task " & strTaskName & " since already running task " & mUtilityRunnerTaskName)
					Return False
				End If
			End If

			If intMonitoringIntervalMsec < 250 Then intMonitoringIntervalMsec = 250
			m_UtilityRunner.MonitorInterval = intMonitoringIntervalMsec

			mUtilityRunnerTaskName = strTaskName

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in InitializeUtilityRunner for task " & strTaskName & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function ProcessCandidateOutFiles(ByVal blnProcessAllRemainingFiles As Boolean) As Boolean

		Dim strDtaFilePath As String = String.Empty
		Dim strSourceFileName As String
		Dim blnSuccess As Boolean
		Dim blnAppendSuccess As Boolean

		Dim objEntry As KeyValuePair(Of String, System.DateTime)

		Dim intItemsProcessed As Integer = 0

		' Examine mOutFileHandlerInUse; if greater then zero, then exit the sub
		If System.Threading.Interlocked.Read(mOutFileHandlerInUse) > 0 Then
			Return False
		End If

		Try
			System.Threading.Interlocked.Increment(mOutFileHandlerInUse)

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Examining out file creation dates (Candidate Count = " & mOutFileCandidates.Count & ")")
			End If

			blnAppendSuccess = True

			If mOutFileCandidates.Count > 0 And Not mSequestVersionInfoStored Then
				' Determine tool version

				' Pass the path to the first out file created
				objEntry = mOutFileCandidates.Peek()
				If StoreToolVersionInfo(System.IO.Path.Combine(m_WorkDir, objEntry.Key)) Then
					mSequestVersionInfoStored = True
				End If
			End If

			If String.IsNullOrEmpty(mTempConcatenatedOutFilePath) Then
				mTempConcatenatedOutFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_out.txt.tmp")
			End If

			If mOutFileCandidates.Count > 0 Then
				' Examine the time associated with the next item that would be dequeued
				objEntry = mOutFileCandidates.Peek()
				If blnProcessAllRemainingFiles OrElse System.DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS Then

					' Open the _out.txt.tmp file
					Using swTargetFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(mTempConcatenatedOutFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))

						intItemsProcessed = 0
						Do While mOutFileCandidates.Count > 0 AndAlso blnAppendSuccess AndAlso (blnProcessAllRemainingFiles OrElse System.DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS)

							' Entry is old enough (or blnProcessAllRemainingFiles=True); pop it off the queue
							objEntry = mOutFileCandidates.Dequeue()
							intItemsProcessed += 1

							Try
								Dim fiOutFile As System.IO.FileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, objEntry.Key))
								AppendOutFile(fiOutFile, swTargetFile)
								mLastOutFileStoreTime = System.DateTime.UtcNow()
								mSequestAppearsStalled = False
							Catch ex As Exception
								Console.WriteLine("Warning, exception appending out file: " & ex.Message)
								blnAppendSuccess = False
							End Try

							If mOutFileCandidates.Count > 0 Then
								objEntry = mOutFileCandidates.Peek()
							End If

						Loop

					End Using

				End If
			End If

			If intItemsProcessed > 0 OrElse blnProcessAllRemainingFiles Then

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Appended " & intItemsProcessed & " .out file" & CheckForPlurality(intItemsProcessed) & " to the _out.txt.tmp file; " & mOutFileCandidates.Count & " out file" & CheckForPlurality(mOutFileCandidates.Count) & " remain in the queue")
				End If

				If blnProcessAllRemainingFiles OrElse System.DateTime.UtcNow.Subtract(mLastTempFileCopyTime).TotalSeconds >= TEMP_FILE_COPY_INTERVAL_SECONDS Then
					If Not mTempJobParamsCopied Then
						strSourceFileName = "JobParameters_" & m_JobNum & ".xml"
						blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName & ".tmp", True)

						If blnSuccess Then
							strSourceFileName = m_jobParams.GetParam("ParmFileName")
							blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName & ".tmp", True)
						End If

						If blnSuccess Then
							mTempJobParamsCopied = True
						End If

					End If

					If intItemsProcessed > 0 Then
						' Copy the _out.txt.tmp file
						strSourceFileName = System.IO.Path.GetFileName(mTempConcatenatedOutFilePath)
						blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName, True)
					End If

					' Copy the sequest.log file (rename to sequest.log.tmp when copying)
					strSourceFileName = "sequest.log"
					blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName & ".tmp", True)

					mLastTempFileCopyTime = System.DateTime.UtcNow

				End If

			End If

		Catch ex As Exception
			Console.WriteLine("Warning, error in ProcessCandidateOutFiles: " & ex.Message)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessCandidateOutFiles: " & ex.Message)
			blnAppendSuccess = False
		Finally
			' Make sure mOutFileHandlerInUse is now zero
			Dim lngZero As Long = 0
			System.Threading.Interlocked.Exchange(mOutFileHandlerInUse, lngZero)
		End Try

		Return blnAppendSuccess

	End Function

	Protected Sub RenameSequestLogFile()
		Dim fiFileInfo As System.IO.FileInfo
		Dim strNewName As String = "??"

		Try
			fiFileInfo = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, "sequest.log"))

			If fiFileInfo.Exists Then
				strNewName = System.IO.Path.GetFileNameWithoutExtension(fiFileInfo.Name) & "_" & fiFileInfo.LastWriteTime.ToString("yyyyMMdd_HHmm") & ".log"
				fiFileInfo.MoveTo(System.IO.Path.Combine(m_WorkDir, strNewName))

				' Copy the renamed sequest.log file to the transfer directory
				CopyFileToTransferFolder(strNewName, strNewName, False)
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming sequest.log file to " & strNewName & ": " & ex.Message)
		End Try

	End Sub

	Protected Function ResetPVMWithRetry(ByVal intMaxPVMResetAttempts As Integer) As Boolean

		Dim blnSuccess As Boolean

		If intMaxPVMResetAttempts < 1 Then intMaxPVMResetAttempts = 1

		Do While intMaxPVMResetAttempts > 0
			blnSuccess = ResetPVM()
			If blnSuccess Then
				Exit Do
			Else
				intMaxPVMResetAttempts -= 1
				If intMaxPVMResetAttempts > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, " ... Error resetting PVM; will try " & intMaxPVMResetAttempts & " more time" & CheckForPlurality(intMaxPVMResetAttempts))
				End If
			End If
		Loop

		If blnSuccess Then
			UpdateSequestNodeProcessingStats(False)
			RenameSequestLogFile()
		End If

		Return blnSuccess

	End Function

	Protected Function ResetPVM() As Boolean

		Dim PVMProgFolder As String		' Folder with PVM
		Dim ExePath As String			' Full path to PVM exe

		Dim blnSuccess As Boolean

		Try
			PVMProgFolder = m_mgrParams.GetParam("PVMProgLoc")
			If String.IsNullOrWhiteSpace(PVMProgFolder) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PVMProgLoc parameter not defined for this manager")
				Return False
			End If

			ExePath = System.IO.Path.Combine(PVMProgFolder, "pvm.exe")
			If Not System.IO.File.Exists(ExePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PVM not found: " & ExePath)
				Return False
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, " ... Resetting PVM")

			blnSuccess = ResetPVMHalt(PVMProgFolder)
			If Not blnSuccess Then
				Return False
			End If

			blnSuccess = ResetPVMWipeTemp(PVMProgFolder)
			If Not blnSuccess Then
				Return False
			End If

			blnSuccess = ResetPVMStartPVM(PVMProgFolder)
			If Not blnSuccess Then
				Return False
			End If

			blnSuccess = ResetPVMAddNodes(PVMProgFolder)
			If Not blnSuccess Then
				Return False
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, " ... PVM restarted")
			mLastActiveNodeQueryTime = System.DateTime.UtcNow

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ResetPVM: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function ResetPVMHalt(ByVal PVMProgFolder As String) As Boolean

		Dim strBatchFilePath As String
		Dim blnSuccess As Boolean

		Try

			strBatchFilePath = System.IO.Path.Combine(PVMProgFolder, "HaltPVM.bat")
			If Not System.IO.File.Exists(strBatchFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Batch file not found: " & strBatchFilePath)
				Return False
			End If

			' Run the batch file
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "     " & strBatchFilePath)
			End If

			Dim strTaskName As String = "HaltPVM"
			If Not InitializeUtilityRunner(strTaskName, PVMProgFolder) Then
				Return False
			End If

			Dim intMaxRuntimeSeconds As Integer = 90
			blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, False, intMaxRuntimeSeconds)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner returned False for " & strBatchFilePath)
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ResetPVMHalt: " & ex.Message)
			Return False
		End Try

		' Wait 5 seconds
		System.Threading.Thread.Sleep(5000)

		Return True

	End Function

	Protected Function ResetPVMWipeTemp(ByVal PVMProgFolder As String) As Boolean

		Dim strBatchFilePath As String
		Dim blnSuccess As Boolean

		Try

			strBatchFilePath = System.IO.Path.Combine(PVMProgFolder, "wipe_temp.bat")
			If Not System.IO.File.Exists(strBatchFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Batch file not found: " & strBatchFilePath)
				Return False
			End If

			' Run the batch file
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "     " & strBatchFilePath)
			End If

			Dim strTaskName As String = "WipeTemp"
			If Not InitializeUtilityRunner(strTaskName, PVMProgFolder) Then
				Return False
			End If

			Dim intMaxRuntimeSeconds As Integer = 120
			blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, True, intMaxRuntimeSeconds)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner returned False for " & strBatchFilePath)
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ResetPVMWipeTemp: " & ex.Message)
			Return False
		End Try

		' Wait 5 seconds
		System.Threading.Thread.Sleep(5000)

		Return True

	End Function

	Protected Function ResetPVMStartPVM(ByVal PVMProgFolder As String) As Boolean

		Dim strBatchFilePath As String
		Dim blnSuccess As Boolean

		Try

			' StartPVM.bat should have a line like this:
			'  pvm.exe -n192.168.1.102 c:\cluster\pvmhosts.txt < QuitNow.txt
			' or like this:
			'  pvm.exe c:\cluster\pvmhosts.txt < QuitNow.txt

			' QuitNow.txt should have this line:
			' quit

			strBatchFilePath = System.IO.Path.Combine(PVMProgFolder, "StartPVM.bat")
			If Not System.IO.File.Exists(strBatchFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Batch file not found: " & strBatchFilePath)
				Return False
			End If

			' Run the batch file
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "     " & strBatchFilePath)
			End If

			Dim strTaskName As String = "StartPVM"
			If Not InitializeUtilityRunner(strTaskName, PVMProgFolder) Then
				Return False
			End If

			Dim intMaxRuntimeSeconds As Integer = 120
			blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, True, intMaxRuntimeSeconds)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner returned False for " & strBatchFilePath)
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ResetPVMStartPVM: " & ex.Message)
			Return False
		End Try

		' Wait 5 seconds
		System.Threading.Thread.Sleep(5000)

		Return True

	End Function

	Protected Function ResetPVMAddNodes(ByVal PVMProgFolder As String) As Boolean

		Dim strBatchFilePath As String
		Dim blnSuccess As Boolean

		Try

			strBatchFilePath = System.IO.Path.Combine(PVMProgFolder, "AddHosts.bat")
			If Not System.IO.File.Exists(strBatchFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Batch file not found: " & strBatchFilePath)
				Return False
			End If

			' Run the batch file
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "     " & strBatchFilePath)
			End If

			Dim strTaskName As String = "AddHosts"
			If Not InitializeUtilityRunner(strTaskName, PVMProgFolder) Then
				Return False
			End If

			Dim intMaxRuntimeSeconds As Integer = 120
			blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, True, intMaxRuntimeSeconds)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner returned False for " & strBatchFilePath)
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ResetPVMAddNodes: " & ex.Message)
			Return False
		End Try

		' Wait 5 seconds
		System.Threading.Thread.Sleep(5000)

		Return True

	End Function

	Protected Sub UpdateSequestNodeProcessingStats(ByVal blnProcessAllSequestLogFiles As Boolean)

		If blnProcessAllSequestLogFiles Then
			Dim diWorkDir As System.IO.DirectoryInfo
			diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

			For Each fiFile As System.IO.FileInfo In diWorkDir.GetFiles("sequest*.log*")
				UpdateSequestNodeProcessingStatsOneFile(fiFile.FullName)
			Next

		Else
			UpdateSequestNodeProcessingStatsOneFile(System.IO.Path.Combine(m_WorkDir, "sequest.log"))
		End If
	End Sub

	Protected Sub UpdateSequestNodeProcessingStatsOneFile(ByVal SeqLogFilePath As String)


		Dim NumNodeMachines As Integer
		Dim NumSlaveProcesses As Integer
		Dim TotalSearchTimeSeconds As Double
		Dim SearchedFileCount As Integer

		Dim intDTAsSearched As Integer

		' Verify sequest.log file exists
		If Not System.IO.File.Exists(SeqLogFilePath) Then
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Sequest log file not found, cannot update Node Processing Stats")
				Exit Sub
			End If
		End If

		' Read the sequest.log file
		Dim sbContents As System.Text.StringBuilder = New System.Text.StringBuilder
		Dim strLineIn As String
		intDTAsSearched = 0

		Try

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(SeqLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
                While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    If strLineIn.StartsWith("Searched dta file") Then
                        intDTAsSearched += 1
                    End If

                    sbContents.AppendLine(strLineIn)
                End While
			End Using

		Catch ex As Exception
			Dim Msg As String = "UpdateNodeStats: Exception reading sequest log file: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
			Exit Sub
		End Try

		Dim strFileContents As String = sbContents.ToString()

		' Node machine count
		NumNodeMachines = GetIntegerFromSeqLogFileString(strFileContents, "starting the sequest task on\s+\d+\s+node")
		If NumNodeMachines = 0 Then
			Dim Msg As String = "UpdateNodeStats: node machine count line not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
		ElseIf NumNodeMachines < 0 Then
			Dim Msg As String = "UpdateNodeStats: Exception retrieving node machine count: " & m_ErrMsg
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
		End If

		If NumNodeMachines > mSequestNodeProcessingStats.NumNodeMachines Then
			mSequestNodeProcessingStats.NumNodeMachines = NumNodeMachines
		End If

		' Sequest process count
		NumSlaveProcesses = GetIntegerFromSeqLogFileString(strFileContents, "Spawned\s+\d+\s+slave processes")
		If NumSlaveProcesses = 0 Then
			Dim Msg As String = "UpdateNodeStats: slave process count line not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
		ElseIf NumSlaveProcesses < 0 Then
			Dim Msg As String = "UpdateNodeStats: Exception retrieving slave process count: " & m_ErrMsg
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
		End If

		If NumSlaveProcesses > mSequestNodeProcessingStats.NumSlaveProcesses Then
			mSequestNodeProcessingStats.NumSlaveProcesses = NumSlaveProcesses
		End If

		' Total search time
		TotalSearchTimeSeconds = GetIntegerFromSeqLogFileString(strFileContents, "Total search time:\s+\d+")
		If TotalSearchTimeSeconds <= 0 Then
			' Total search time line not found (or error)
			' Use internal tracking variables instead
			TotalSearchTimeSeconds = mSequestSearchEndTime.Subtract(mSequestSearchStartTime).TotalSeconds
		End If

		mSequestNodeProcessingStats.TotalSearchTimeSeconds += TotalSearchTimeSeconds

		' Searched file count
		SearchedFileCount = GetIntegerFromSeqLogFileString(strFileContents, "secs for\s+\d+\s+files")
		If SearchedFileCount <= 0 Then
			' Searched file count line not found (or error)
			' Use intDTAsSearched instead
			SearchedFileCount = intDTAsSearched
		End If

		mSequestNodeProcessingStats.SearchedFileCount += intDTAsSearched

		If mSequestNodeProcessingStats.SearchedFileCount > 0 Then
			' Compute average search time
			mSequestNodeProcessingStats.AvgSearchTime = CSng(mSequestNodeProcessingStats.TotalSearchTimeSeconds) / CSng(mSequestNodeProcessingStats.SearchedFileCount)
		End If

	End Sub

	''' <summary>
	''' Uses PVM command ps -a to determine the number of active nodes
	''' Sets mResetPVM to True if fewer than 50% of the nodes are creating .Out files
	''' </summary>
	''' <remarks></remarks>
	Protected Sub ValidateProcessorsAreActive()

		Dim PVMProgFolder As String		' Folder with PVM
		Dim strBatchFilePath As String

		Dim strActiveNodesFilePath As String
		Dim strLineIn As String

		Dim strNodeName As String
		Dim dtLastFinishTime As System.DateTime

		Dim reMatch As System.Text.RegularExpressions.Match

		Dim intNodeCountCurrent As Integer
		Dim intNodeCountActive As Integer
		Dim blnSuccess As Boolean

		Try

			If Not mSequestLogNodesFound Then
				' Parse the Sequest.Log file to determine the names of the spawned nodes

				Dim strLogFilePath As String
				strLogFilePath = System.IO.Path.Combine(m_WorkDir, "sequest.log")

				mSequestLogNodesFound = GetNodeNamesFromSequestLog(strLogFilePath)

				If Not mSequestLogNodesFound OrElse mResetPVM Then
					Exit Sub
				End If
			End If


			' Determine the number of Active Nodes using PVM
			PVMProgFolder = m_mgrParams.GetParam("PVMProgLoc")
			If String.IsNullOrWhiteSpace(PVMProgFolder) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PVMProgLoc parameter not defined for this manager")
				Exit Sub
			End If

			strBatchFilePath = System.IO.Path.Combine(PVMProgFolder, "CheckActiveNodes.bat")
			If Not System.IO.File.Exists(strBatchFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Batch file not found: " & strBatchFilePath)
				Exit Sub
			End If

			strActiveNodesFilePath = System.IO.Path.Combine(m_WorkDir, "ActiveNodesOutput.tmp")

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "     " & strBatchFilePath)
			End If

			Dim strTaskName As String = "CheckActiveNodes"
			If Not InitializeUtilityRunner(strTaskName, PVMProgFolder) Then
				Exit Sub
			End If

			Dim intMaxRuntimeSeconds As Integer = 60
			blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, True, intMaxRuntimeSeconds)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner returned False for " & strBatchFilePath)
			End If

			If Not System.IO.File.Exists(strActiveNodesFilePath) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Warning, ActiveNodes files not found: " & strActiveNodesFilePath)
				End If

				Exit Sub
			End If

			' Parse the ActiveNodesOutput.tmp file
			intNodeCountCurrent = 0
			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strActiveNodesFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    ' Check whether line looks like:
                    '    p6    c0007     6/c,f sequest27_slave

                    reMatch = m_ActiveNodeRegEx.Match(strLineIn)
                    If reMatch.Success Then
                        strNodeName = reMatch.Groups("node").Value

                        If mSequestNodes.TryGetValue(strNodeName, dtLastFinishTime) Then
                            mSequestNodes(strNodeName) = System.DateTime.UtcNow
                        Else
                            mSequestNodes.Add(strNodeName, System.DateTime.UtcNow)
                        End If

                        intNodeCountCurrent += 1
                    End If

                End While
			End Using

			' Log the number of active nodes every 10 minutes
			If m_DebugLevel >= 4 OrElse System.DateTime.UtcNow.Subtract(mLastActiveNodeLogTime).TotalSeconds >= 600 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & intNodeCountCurrent & " / " & mSequestNodesSpawned & " Sequest nodes are active; median processing time = " & ComputeMedianProcessingTime().ToString("0.0") & " seconds/spectrum; " & m_progress.ToString("0.0") & "% complete")
				mLastActiveNodeLogTime = System.DateTime.UtcNow
			End If


			' Look for nodes that have been missing for at least 5 minutes
			intNodeCountActive = 0
			For Each objItem As KeyValuePair(Of String, DateTime) In mSequestNodes
				If System.DateTime.UtcNow.Subtract(objItem.Value).TotalMinutes <= STALE_NODE_THRESHOLD_MINUTES Then
					intNodeCountActive += 1
				End If
			Next

			' Define the minimum node count as 50% of the number of nodes spawned
			Dim intActiveNodeCountMinimum As Integer
			intActiveNodeCountMinimum = CInt(Math.Floor(0.5 * mSequestNodesSpawned))

			If intNodeCountActive < intActiveNodeCountMinimum AndAlso Not mIgnoreNodeCountActiveErrors Then
				mNodeCountActiveErrorOccurences += 1
				Dim strMessage As String
				strMessage = "Too many nodes are inactive (Threshold = " & intActiveNodeCountMinimum & " nodes): " & intNodeCountActive & " active vs. " & mSequestNodesSpawned & " total nodes at start; mNodeCountActiveErrorOccurences=" & mNodeCountActiveErrorOccurences
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
				mResetPVM = True
			ElseIf mNodeCountActiveErrorOccurences > 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Resetting mNodeCountActiveErrorOccurences from " & mNodeCountActiveErrorOccurences & " to 0")
				mNodeCountActiveErrorOccurences = 0
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateProcessorsAreActive: " & ex.Message)
		End Try

	End Sub

	Protected Sub mOutFileWatcher_Created(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Created
		HandleOutFileChange(e.Name)
	End Sub

	Protected Sub mOutFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mOutFileWatcher.Changed
		HandleOutFileChange(e.Name)
	End Sub

	Protected Sub mOutFileAppenderTime_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles mOutFileAppenderTimer.Elapsed
		ProcessCandidateOutFiles(False)
		CheckForStalledSequest()
	End Sub

	Private Sub m_UtilityRunner_Timeout() Handles m_UtilityRunner.Timeout

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "UtilityRunner task " & mUtilityRunnerTaskName & " has timed out; " & m_UtilityRunner.MaxRuntimeSeconds & " seconds has elapsed")

	End Sub
#End Region

End Class
