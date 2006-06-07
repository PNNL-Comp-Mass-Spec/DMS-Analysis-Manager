Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal


Public Class clsAnalysisResults

	Public Sub New(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger)
		m_mgrParams = mgrParams
		m_logger = logger
		m_jobParams = jobParams
	End Sub


	' access to the job parameters
	Private m_jobParams As IJobParams

	' access to the logger
	Private m_logger As ILogger

	' access to mgr parameters
	Private m_mgrParams As IMgrParams

	' for posting a general explanation for external consumption
	Protected m_message As String

	' explanation of what happened to last operation this class performed
	Public ReadOnly Property Message() As String
		Get
			Return m_message
		End Get
	End Property

	Public Function DeliverResults(ByVal ResultsFolderName As String) As IJobParams.CloseOutType

		'Copies the results folder to the transfer directory
		Dim XferDir As String = CheckTerminator(m_jobParams.GetParam("transferFolderPath"))
		Dim WorkDir As String = m_mgrParams.GetParam("commonfileandfolderlocations", "workdir")

		'Verify xfer directory exists
		If Not Directory.Exists(XferDir) Then
			m_logger.PostEntry("Transfer folder not found, job " & m_jobParams.GetParam("jobNum"), _
			ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = "Transfer folder not found"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Verify results folder exists
		If Not Directory.Exists(Path.Combine(WorkDir, ResultsFolderName)) Then
			m_logger.PostEntry("Results folder not found, job " & m_jobParams.GetParam("jobNum"), _
			ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = "Results folder not found"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Append machine name to xfer directory, create directory if it doesn't exist
		XferDir = XferDir & m_mgrParams.GetParam("programcontrol", "machname")
		If Not Directory.Exists(XferDir) Then
			Try
				Directory.CreateDirectory(XferDir)
			Catch err As Exception
				m_logger.PostError("Unable to create server results folder, job " & m_jobParams.GetParam("jobNum"), _
					err, LOG_DATABASE)
				m_message = "Unable to create server results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		End If

		'Copy results folder to xfer directory
		Try
			CopyDirectory(Path.Combine(WorkDir, ResultsFolderName), Path.Combine(XferDir, ResultsFolderName))
		Catch err As Exception
			m_logger.PostError("Error copying results folder, job " & m_jobParams.GetParam("jobNum"), _
				err, LOG_DATABASE)
			m_message = "Unable to create server results folder"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Everything must be OK if we got to here
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
