Imports PRISM.Logging
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerSeq
	Inherits clsAnalysisToolRunnerSeqBase

	Public Sub New()
	End Sub

	Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
		 ByVal logger As ILogger, ByVal StatusTools As IStatusFile)

		MyBase.Setup(mgrParams, jobParams, logger, StatusTools)
		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeq.Setup()", ILogger.logMsgType.logDebug, True)
		End If

	End Sub

End Class




