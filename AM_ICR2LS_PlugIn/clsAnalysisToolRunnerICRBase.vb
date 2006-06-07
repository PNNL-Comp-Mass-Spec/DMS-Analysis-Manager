Imports PRISM.Logging
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public MustInherit Class clsAnalysisToolRunnerICRBase
	Inherits clsAnalysisToolRunnerBase

	'ICR2LS object for use in analysis
	Protected m_ICR2LSObj As New clsICR2LSWrapper(m_logger)

	'Enumerated constants
	Protected Enum ICR_STATUS As Short
		'TODO: This list must be kept current with ICR2LS
		STATE_IDLE = 1
		STATE_PROCESSING = 2
		STATE_KILLED = 3
		STATE_FINISHED = 4
		STATE_GENERATING = 5
		STATE_TICGENERATION = 6
		STATE_LCQTICGENERATION = 7
		STATE_QTOFPEKGENERATION = 8
		STATE_MMTOFPEKGENERATION = 9
		STATE_LTQFTPEKGENERATION = 10
	End Enum

	'Job running status variable
    Protected m_JobRunning As Boolean

    Public Sub New()

    End Sub

	Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As PRISM.Logging.ILogger, ByVal StatusTools As IStatusFile)

		MyBase.Setup(mgrParams, jobParams, logger, StatusTools)
		m_ICR2LSObj.DebugLevel = m_DebugLevel

	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType

		'Get the settings file info via the base class
		If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'Remainder of tasks are in subclass

	End Function

	Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

	Protected Overridable Function PerfPostAnalysisTasks(ByVal ResType As String) As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType

		'Stop the job timer
		m_StopTime = Now()

		'Get rid of raw data file
		StepResult = DeleteDataFile()
		If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return StepResult
		End If

		'make results folder
		StepResult = MakeResultsFolder(ResType)
		If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return StepResult
		End If

		If Not UpdateSummaryFile() Then
			m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
			 ILogger.logMsgType.logWarning, LOG_DATABASE)
		End If

	End Function

	Protected Function WaitForJobToFinish() As Boolean

		'Waits for ICR2LS job to finish after being started by subclass call
		Dim StatusResult As ICR_STATUS
		'		Dim Progress As Single

		'Monitor status
		While m_JobRunning
			System.Threading.Thread.Sleep(2000)			'Delay for 2 seconds
			StatusResult = CType(m_ICR2LSObj.Status, ICR_STATUS)
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); " & "StatusResult=" & StatusResult.ToString, _
				 ILogger.logMsgType.logDebug, True)
			End If
			Select Case StatusResult
				'TODO: Update this statement when new ICR2LS states are added
			Case ICR_STATUS.STATE_PROCESSING, ICR_STATUS.STATE_GENERATING, ICR_STATUS.STATE_QTOFPEKGENERATION, _
			 ICR_STATUS.STATE_LCQTICGENERATION, ICR_STATUS.STATE_MMTOFPEKGENERATION, _
			 ICR_STATUS.STATE_LTQFTPEKGENERATION, ICR_STATUS.STATE_TICGENERATION
					'Report progress
					m_Progress = m_ICR2LSObj.Progress
					'Update the status report
					m_StatusTools.UpdateAndWrite(m_progress)
					If m_DebugLevel > 0 Then
						m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); " & "Continuing loop", _
						 ILogger.logMsgType.logDebug, True)
					End If
				Case Else
					'Analysis is no longer running, exit loop
					m_JobRunning = False
					If m_DebugLevel > 0 Then
						m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); " & "Ending loop", _
						 ILogger.logMsgType.logDebug, True)
					End If
			End Select
		End While

		'Close the ICR2LS object
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Closing ICR2LS object", _
			 ILogger.logMsgType.logDebug, True)
		End If
		m_ICR2LSObj.CloseICR2LS()
		m_ICR2LSObj = Nothing
		'Fire off the garbage collector to make sure ICR2LS dies
		GC.Collect()
		GC.WaitForPendingFinalizers()
		'Delay to allow ICR2LS to close everything
		System.Threading.Thread.Sleep(3000)

		'Verify ICR2LS exited due to job completion
		If StatusResult <> ICR_STATUS.STATE_FINISHED Then
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); State<>FINISHED", _
				 ILogger.logMsgType.logDebug, True)
			End If
			Return False
		Else
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); State=FINISHED", _
				 ILogger.logMsgType.logDebug, True)
			End If
			Return True
		End If

	End Function

End Class
