Option Strict On
Imports PRISM.Logging
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMASICFinnigan
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

		strInputFilePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".raw")

		Return MyBase.StartMASICAndWait(strInputFilePath, m_workdir, strParameterFilePath)

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .raw file from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		'Delete the .raw file
		Try
			FoundFiles = Directory.GetFiles(m_workdir, "*.raw")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next MyFile
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
			m_logger.PostError("Error finding .raw files to delete, job " & m_JobNum, Err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class
