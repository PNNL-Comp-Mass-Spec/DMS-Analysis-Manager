Option Strict On

Imports Prism.Logging
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMASICAgilent
	Inherits clsAnalysisToolRunnerMASICBase

	Public Sub New()
	End Sub

	Protected Overrides Function RunMASIC() As IJobParams.CloseOutType

		Dim strParameterFileName As String
		Dim strParameterFilePath As String
		Dim strInputFilePath As String
		Dim blnSuccess As Boolean

		strParameterFileName = m_JobParams.GetParam("parmFileName")

		If Not strParameterFileName Is Nothing AndAlso strParameterFileName.Trim.ToLower <> "na" Then
			strParameterFilePath = Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName"))
		Else
			strParameterFilePath = String.Empty
		End If

		strInputFilePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".mgf")

		Return MyBase.StartMASICAndWait(strInputFilePath, m_workdir, strParameterFilePath)

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .cdf and .mgf files from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		'Delete the .cdf file
		Try
			FoundFiles = Directory.GetFiles(m_workdir, "*.cdf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error deleting .cdf file, job " & m_JobNum, Err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Delete the .mgf file
		Try
			FoundFiles = Directory.GetFiles(m_workdir, "*.mgf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error deleting .mgf file, job " & m_JobNum, Err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
