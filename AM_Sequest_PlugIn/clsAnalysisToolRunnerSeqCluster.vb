'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/25/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerSeqCluster
	Inherits clsAnalysisToolRunnerSeqBase

	'*********************************************************************************************************
	'Overrides Sequest tool runner to provide cluster-specific methods
	'*********************************************************************************************************

#Region "Module Variables"
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
	''' <param name="logger">Logging object</param>
	''' <param name="StatusTools">Object providing tools for status file updates</param>
	''' <remarks></remarks>
	Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As PRISM.Logging.ILogger, ByVal StatusTools As IStatusFile)

		MyBase.Setup(mgrParams, jobParams, logger, StatusTools)

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.Setup()", ILogger.logMsgType.logDebug, True)
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

		m_CmdRunner = New clsRunDosProgram(m_logger, m_WorkDir)

		'Run the OUT file generation program
		CmdStr = " -P" & m_jobParams.GetParam("parmFileName") & " *.dta"
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), making files", _
			 ILogger.logMsgType.logDebug, True)
		End If
		ResCode = m_CmdRunner.RunProgram(m_mgrParams.GetParam("seqprogloc"), CmdStr, "Seq", True)
		If Not ResCode Then
			m_logger.PostEntry("Unknown error making OUT files", ILogger.logMsgType.logError, True)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Make sure objects are released
		System.Threading.Thread.Sleep(20000)		 '20 second delay
		GC.Collect()
		GC.WaitForPendingFinalizers()

		'Verify out file creation
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), verifying out file creation", _
			 ILogger.logMsgType.logDebug, True)
		End If
		OutFiles = Directory.GetFiles(m_WorkDir, "*.out")
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.MakeOutFiles(), outfile count: " & OutFiles.GetLength(0).ToString, _
			 ILogger.logMsgType.logDebug, True)
		End If
		If OutFiles.GetLength(0) < 1 Then
			m_logger.PostEntry("No OUT files created, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = AppendToComment(m_message, "No OUT files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		End If

		'Package out files into concatenated text files 
		If Not ConcatOutFiles(m_WorkDir, m_jobParams.GetParam("datasetNum"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_mgrParams.GetParam("zipprogram"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

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

		'Update the status file
		CalculateNewStatus()
		m_StatusTools.UpdateAndWrite(m_progress)

	End Sub

	''' <summary>
	''' Reads sequest.log file after Sequest finishes and adds cluster statistics info to summary file
	''' </summary>
	''' <remarks></remarks>
	Private Sub AddClusterStatsToSummaryFile()

		Dim SeqLogFilePath As String = Path.Combine(m_mgrParams.GetParam("workdir"), "sequest.log")
		Dim NumNodeMachines As Integer
		Dim NumSlaveProcesses As Integer
		Dim TotalSearchTime As Integer
		Dim SearchedFileCount As Integer
		Dim AvgSearchTime As Single

		'Verify sequest.log file exists
		If Not File.Exists(SeqLogFilePath) Then
			m_logger.PostEntry("Sequest log file not found for job " & m_JobNum, ILogger.logMsgType.logWarning, False)
			Exit Sub
		End If

		'Read the sequest.log file
		Dim fileContents As String
		Try
			fileContents = My.Computer.FileSystem.ReadAllText(SeqLogFilePath)
		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception reading sequest log file: " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		End Try

		'Node machine count
		NumNodeMachines = GetIntegerFromSeqLogFileString(fileContents, "starting the sequest task on\s+\d+\s+node")
		If NumNodeMachines = 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), node machine count line not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		ElseIf NumNodeMachines < 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving node machine count: " & m_ErrMsg
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		End If

		'Sequest process count
		NumSlaveProcesses = GetIntegerFromSeqLogFileString(fileContents, "Spawned\s+\d+\s+slave processes")
		If NumSlaveProcesses = 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), slave process count line not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		ElseIf NumSlaveProcesses < 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving slave process count: " & m_ErrMsg
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		End If

		'Total search time
		TotalSearchTime = GetIntegerFromSeqLogFileString(fileContents, "Total search time:\s+\d+")
		If TotalSearchTime = 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), total search time line not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		ElseIf TotalSearchTime < 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving total search time: " & m_ErrMsg
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		End If

		'Searched file count
		SearchedFileCount = GetIntegerFromSeqLogFileString(fileContents, "secs for\s+\d+\s+files")
		If SearchedFileCount = 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), searched file count line not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
			Exit Sub
		ElseIf SearchedFileCount < 0 Then
			Dim Msg As String = "clsAnalysisToolRunnerSeqCluster.AddClusterStatsToSummaryFile(), Exception retrieving searched file count: " & m_ErrMsg
			m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, False)
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
			TmpStr = Regex.Match(InpFileStr, RegexStr, RegexOptions.IgnoreCase Or RegexOptions.Multiline).Value
			If TmpStr = "" Then Return 0

			'Find the item count in the substring
			RetVal = CInt(Regex.Match(TmpStr, "\d+").Value)
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
			TmpStr = Regex.Match(InpFileStr, RegexStr, RegexOptions.IgnoreCase Or RegexOptions.Multiline).Value
			If TmpStr = "" Then Return 0.0

			'Find the item count in the substring
			RetVal = CSng(Regex.Match(TmpStr, "\d+\.\d+").Value)
			Return RetVal
		Catch ex As Exception
			m_ErrMsg = ex.Message
			Return -1.0
		End Try

	End Function
#End Region

End Class
