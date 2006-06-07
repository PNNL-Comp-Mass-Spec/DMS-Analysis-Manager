Imports PRISM.Logging
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerQTOFPek
	Inherits clsAnalysisToolRunnerICRBase

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim ResCode As IJobParams.CloseOutType
		Dim DSNamePath As String
		Dim PekRes As Boolean
		Dim OutFileName As String
		Dim UseSGFilter As Boolean

		'Start with base class function to get settings information
		ResCode = MyBase.RunTool()
		If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

		'Get data from settings file
		If m_JobParams.GetParam("settingsFileName") <> "na" Then
            UseSGFilter = m_settingsFileParams.GetParam("QTOFPek", "UseSGFilter", False)
		Else
			UseSGFilter = False
		End If

		'Assemble the dataset name
		DSNamePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".wiff")
		If Not File.Exists(DSNamePath) Then
			CleanupFailedJob("Unable to find data file in working directory")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Assemble the output file name
		OutFileName = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".pek")

		'Make the PEK file
		m_JobRunning = True
		PekRes = m_ICR2LSObj.MakeQTOFPEKFile(DSNamePath, Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName")), _
			OutFileName, UseSGFilter)

		If Not PekRes Then
			CleanupFailedJob("Error creating PEK file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Wait for the job to complete
		If Not WaitForJobToFinish() Then
			CleanupFailedJob("Error waiting for PEK job to finish")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Allow time for ICR2LS to close and clear all file locks (cheezy, but seems to work)
		System.Threading.Thread.Sleep(5000)

		'If specified, run Eric's calibration tool
		If CBool(m_mgrParams.GetParam("qtof", "performcal")) Then
			If Not PerformEricCal() Then
				CleanupFailedJob("Error performing post-analysis calibration process")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		'Run the cleanup routine from the base class
		If PerfPostAnalysisTasks("ICR") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			m_message = AppendToComment(m_message, "Error performing post analysis tasks")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .wiff file from the working directory

		Try
			System.Threading.Thread.Sleep(5000)			 'Allow extra time for ICR2LS to release file locks
			DeleteFileWithRetries(Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".wiff"))
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
			m_logger.PostError("Error deleting raw data file, job " & m_JobNum, Err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function PerformEricCal() As Boolean

		'Performs Eric Strittmatter post-pek cal process
		Dim CalTools As New clsEricQTOFCal
		Dim InputFile As String
		Dim PekFiles() As String = Directory.GetFiles(m_WorkDir, "*.pek")


		'Verify at least one pek file was found
		If PekFiles.Length = 0 Then Return False

		'Initialize cal process timeout
		CalTools.TimeoutSetting = 600		'10 minutes

		'Process all pek files
		For Each InputFile In PekFiles
			If Not CalTools.PerformCal(InputFile) Then Return False
		Next

		'If we got to here, then the cal process worked
		Return True

	End Function

End Class
