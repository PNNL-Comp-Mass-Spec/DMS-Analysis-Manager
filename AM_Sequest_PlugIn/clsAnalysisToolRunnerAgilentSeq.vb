Imports PRISM.Logging
Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerAgilentSeq
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

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the data files (.mgf and .cdf) from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		Try
			'Delete the .mgf file
			FoundFiles = Directory.GetFiles(m_workdir, "*.mgf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
			'Delete the .cdf file, if present
			FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error deleting raw data file(s), job " & m_JobNum, Err, False)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class
