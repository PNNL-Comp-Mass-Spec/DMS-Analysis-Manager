Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerICR
	Inherits clsAnalysisToolRunnerICRBase

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim ResCode As IJobParams.CloseOutType
		Dim DSNamePath As String
		Dim PekRes As Boolean

		'Start with base class function to get settings information
		ResCode = MyBase.RunTool()
		If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

		'Verify a parm file has been specified
		If Not File.Exists(Path.Combine(m_workdir, m_jobParams.GetParam("parmFileName"))) Then
			'Parm file wasn't specified, but is required for ICR2LS analysis
			CleanupFailedJob("Parm file not found")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Add handling of settings file info here if it becomes necessary in the future

		'Assemble the dataset name
		DSNamePath = CheckTerminator(Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum")))
		If Not Directory.Exists(DSNamePath) Then
			CleanupFailedJob("Unable to find data files in working directory")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Make the PEK file
		m_JobRunning = True
		PekRes = m_ICR2LSObj.MakeICRPEKFile(DSNamePath, Path.Combine(m_workdir, m_jobParams.GetParam("parmFileName")), _
			CheckTerminator(m_workdir))

		If Not PekRes Then
			CleanupFailedJob("Error creating PEK file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Wait for the job to complete
		If Not WaitForJobToFinish() Then
			CleanupFailedJob("Error waiting for PEK job to finish")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Run the cleanup routine from the base class
		If PerfPostAnalysisTasks("ICR") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			m_message = AppendToComment(m_message, "Error performing post analysis tasks")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the dataset folder containing s-folders from the working directory
		Dim RetryCount As Integer = 0
		Dim ErrMsg As String

		While RetryCount < 3
			Try
				System.Threading.Thread.Sleep(5000)				'Allow extra time for ICR2LS to release file locks
				Directory.Delete(Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum")), True)
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Catch Err As IOException
				'If problem is locked file, retry
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("Error deleting data file, attempt #" & RetryCount.ToString, ILogger.logMsgType.logError, True)
				End If
				ErrMsg = err.Message
				RetryCount += 1
			Catch Err As Exception
				m_logger.PostError("Error deleting raw data files, job " & m_JobNum, Err, LOG_DATABASE)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		End While

		'If we got to here, then we've exceeded the max retry limit
		m_logger.PostEntry("Unable to delete raw data file after multiple tries, job " & m_jobnum & _
		 ", Error " & ErrMsg, ILogger.logMsgType.logError, LOG_DATABASE)
		Return IJobParams.CloseOutType.CLOSEOUT_FAILED

	End Function

End Class
