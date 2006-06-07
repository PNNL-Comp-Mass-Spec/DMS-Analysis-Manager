Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerSeqCluster
	Inherits clsAnalysisToolRunnerSeqBase

#Region "Module Variables"
	Dim WithEvents m_CmdRunner As clsRunDosProgram
#End Region

	Public Sub New()
	End Sub

	Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As PRISM.Logging.ILogger, ByVal StatusTools As IStatusFile)

		MyBase.Setup(mgrParams, jobParams, logger, StatusTools)

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.Setup()", ILogger.logMsgType.logDebug, True)
		End If

	End Sub

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
		ResCode = m_CmdRunner.RunProgram(m_mgrParams.GetParam("sequest", "seqprogloc"), CmdStr, "Seq", True)
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
		OutFiles = Directory.GetFiles(m_workdir, "*.out")
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
		If Not ConcatOutFiles(m_workdir, m_jobParams.GetParam("datasetNum"), m_jobnum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_workdir, m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"), m_jobnum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'If we got here, everything worked
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Sub m_CmdRunner_LoopWaiting() Handles m_CmdRunner.LoopWaiting

		'Update the status file
		CalculateNewStatus()
		m_StatusTools.UpdateAndWrite(m_progress)

	End Sub

End Class
