Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerAgilentTOFPek
	Inherits clsAnalysisToolRunnerICRBase

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim ResCode As IJobParams.CloseOutType
		Dim PekRes As Boolean

		'Start with base class function to get settings information
		ResCode = MyBase.RunTool()
		If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

		'Verify a parm file has been specified
		If Not File.Exists(Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName"))) Then
			'Parm file wasn't specified, but is required for ICR2LS analysis
			CleanupFailedJob("Parm file not found")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Add handling of settings file info here if it becomes necessary in the future
		'(Settings file is handled by base class)

		'Assemble the dataset folder name for input to ICR2LS
		Dim DSNamePath As String = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".wiff")

		'Assemble other input parameters
		Dim ParmFileNamePath As String = Path.Combine(m_WorkDir, m_JobParams.GetParam("parmFileName"))
		Dim OutFileNamePath As String = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".pek")
        Dim SGFilter As Boolean = m_settingsFileParams.GetParam("mmtofsettings", "sdfilter", True)

		'Make the PEK file
		m_JobRunning = True
		PekRes = m_ICR2LSObj.MakeAgilentTOFPEKFile(DSNamePath, ParmFileNamePath, OutFileNamePath, SGFilter)
		If Not PekRes Then
			CleanupFailedJob("Error creating PEK file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Wait for the job to complete
		If Not WaitForJobToFinish() Then
			CleanupFailedJob("Error waiting for PEK job to finish")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'If specified, run Eric's calibration tool
		If CBool(m_mgrParams.GetParam("agilenttofpek", "performcal")) Then
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

		'Deletes the .wiff dataset file
		Try
			System.Threading.Thread.Sleep(5000)			 'Delay to ensure ICR2LS has completely released the data file
			Dim FoundFiles() As String = Directory.GetFiles(m_workdir, "*.wiff")
			If FoundFiles.GetLength(0) < 1 Then Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			For Each WorkFile As String In FoundFiles
				DeleteFileWithRetries(WorkFile)
			Next
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch err As Exception
			m_logger.PostError("Error deleting raw data file, job " & m_JobNum, err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Function PerformEricCal() As Boolean

		'Performs Eric Strittmatter post-pek cal process
		Dim CalTools As New clsEricAgilTOFCal
		Dim InputFile As String
		Dim PekFiles() As String = Directory.GetFiles(m_WorkDir, "*.pek")

		'Verify at least one pek file was found
		If PekFiles.Length = 0 Then Return False

		'Initialize cal process timeout
		CalTools.TimeoutSetting = 600		'10 minutes

		'Process all pek files
		For Each InputFile In PekFiles
			If (InputFile.IndexOf("_ic.pek") > 0) Or (InputFile.IndexOf("_s.pek") > 0) Then
				'Do nothing with this file
			Else
				'Perform the cal
				If CalTools.PerformCal(InputFile) Then
					Return True
				Else
					Return False
				End If
			End If
		Next

		'If we got to here, then the cal process worked
		Return True

	End Function

End Class
