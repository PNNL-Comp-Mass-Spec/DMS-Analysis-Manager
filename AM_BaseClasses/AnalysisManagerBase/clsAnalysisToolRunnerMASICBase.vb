Option Strict On
Imports PRISM.Logging
Imports System.IO
Imports System.Threading
Imports AnalysisManagerBase.clsGlobal

Public MustInherit Class clsAnalysisToolRunnerMASICBase
	Inherits clsAnalysisToolRunnerBase

	'Job running status variable
	Protected m_JobRunning As Boolean

	Protected m_ErrorMessage As String = String.Empty
	Protected m_ProcessStep As String = String.Empty

	'Failed job cleanup status variable
	Protected m_FailedJobCleanedUp As Boolean

	Public Sub New()
	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType

		' Reset this variable
		m_FailedJobCleanedUp = False

		'Get the settings file info via the base class
		If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'Make the SIC's 
		m_logger.PostEntry("Calling MASIC to create the SIC files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
		Try
			' Note that RunMASIC will populate the File Path variables, then will call 
			'  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
			StepResult = RunMASIC()
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				If Not m_FailedJobCleanedUp Then CleanupFailedJob("Error")
				Return StepResult
			End If
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " & _
				Err.Message, ILogger.logMsgType.logError, True)
			If Not m_FailedJobCleanedUp Then CleanupFailedJob("Exception calling MASIC, " & Err.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Update progress to 100%
		m_progress = 100

		m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, m_progress, 0)

		'Run the cleanup routine from the base class
		If PerfPostAnalysisTasks("SIC") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			If Not m_FailedJobCleanedUp Then
				CleanupFailedJob("Error performing post analysis tasks")
			Else
				m_message = AppendToComment(m_message, "Error performing post analysis tasks")
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function StartMASICAndWait(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String, ByVal strParameterFilePath As String) As IJobParams.CloseOutType
		' Note that this function is normally called by RunMasic() in the subclass

		Dim objMasicProgRunner As PRISM.Processes.clsProgRunner
		Dim CmdStr As String

		m_ErrorMessage = String.Empty
		m_ProcessStep = "NewTask"

		' Call MASIC using the Program Runner class

		' Define the parameters to send to Masic.exe
		CmdStr = "/I:" & strInputFilePath & " /O:" & strOutputFolderPath & " /P:" & strParameterFilePath & " /Q"
		If m_DebugLevel > 0 Then CmdStr &= " /L"

		objMasicProgRunner = New PRISM.Processes.clsProgRunner
		With objMasicProgRunner
			.Name = "MASIC"
			.CreateNoWindow = True
			.Program = m_mgrParams.GetParam("masic", "masicprogloc")
			.Arguments = CmdStr
			.WorkDir = m_WorkDir
			'            .DebugLevel = m_DebugLevel
		End With

		objMasicProgRunner.StartAndMonitorProgram()

		'Wait for the job to complete
		If Not WaitForJobToFinish(objMasicProgRunner) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		objMasicProgRunner = Nothing
		System.Threading.Thread.Sleep(3000)				'Delay for 3 seconds to make sure program exits


		'Verify MASIC exited due to job completion
		If Not m_ErrorMessage Is Nothing AndAlso m_ErrorMessage.Length > 0 Then
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); m_ProcessStep<>COMPLETE", _
				 ILogger.logMsgType.logDebug, True)
			End If
			CleanupFailedJob(m_ErrorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); m_ProcessStep=COMPLETE", _
				 ILogger.logMsgType.logDebug, True)
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Protected MustOverride Function RunMASIC() As IJobParams.CloseOutType

	Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

	Protected Overridable Sub CalculateNewStatus(ByVal strMasicProgLoc As String)

		'Calculates status information for progress file
		'Does this by reading the MasicStatus.xml file

		Const StatusFileName As String = "MasicStatus.xml"

		Dim strPath As String

		Dim fsInFile As System.IO.FileStream
		Dim objXmlReader As System.Xml.XmlTextReader

		Dim strProgress As String = String.Empty

		Try
			strPath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(strMasicProgLoc), StatusFileName)

			If System.IO.File.Exists(strPath) Then

				fsInFile = New System.IO.FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
				objXmlReader = New System.Xml.XmlTextReader(fsInFile)
				objXmlReader.WhitespaceHandling = Xml.WhitespaceHandling.None

				While objXmlReader.Read()

					If objXmlReader.NodeType = System.Xml.XmlNodeType.Element Then
						Select Case objXmlReader.Name
							Case "ProcessingStep"
								If Not objXmlReader.IsEmptyElement Then
									If objXmlReader.Read() Then m_ProcessStep = objXmlReader.Value
								End If
							Case "Progress"
								If Not objXmlReader.IsEmptyElement Then
									If objXmlReader.Read() Then strProgress = objXmlReader.Value
								End If
							Case "Error"
								If Not objXmlReader.IsEmptyElement Then
									If objXmlReader.Read() Then m_ErrorMessage = objXmlReader.Value
								End If
						End Select
					End If
				End While

				If strProgress.Length > 0 Then
					Try
						m_progress = Single.Parse(strProgress)
					Catch ex As Exception
						' Ignore errors
					End Try
				End If

			End If

		Catch ex As Exception
			' Ignore errors
		Finally
			If Not objXmlReader Is Nothing Then
				objXmlReader.Close()
				objXmlReader = Nothing
			End If

			If Not fsInFile Is Nothing Then
				fsInFile.Close()
				fsInFile = Nothing
			End If
		End Try

	End Sub

	Protected Overridable Function PerfPostAnalysisTasks(ByVal ResType As String) As IJobParams.CloseOutType

		Dim StepResult As IJobParams.CloseOutType
		Dim Zipper As PRISM.Files.ZipTools
		Dim ZippedXMLFileName As String
		Dim FoundFiles() As String
		Dim TempFile As String

		'Stop the job timer
		m_StopTime = Now()

		'Get rid of raw data file
		StepResult = DeleteDataFile()
		If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return StepResult
		End If

		'Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
		FoundFiles = Directory.GetFiles(m_workdir, "*_SICs.xml")

		If FoundFiles.Length > 0 Then
			'Setup zipper
			Zipper = New PRISM.Files.ZipTools(m_WorkDir, m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"))

			ZippedXMLFileName = m_jobParams.GetParam("datasetNum") & "_SICs.zip"

			If Not Zipper.MakeZipFile("-normal", Path.Combine(m_workdir, ZippedXMLFileName), Path.Combine(m_workdir, "*_SICs.xml")) Then
				m_logger.PostEntry("Error zipping *_SICs.xml files, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error zipping .ann files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Delete the unneeded files
			Try
				'Delete _SICs.XML files
				FoundFiles = Directory.GetFiles(m_workdir, "*_SICs.xml")
				For Each TempFile In FoundFiles
					DeleteFileWithRetries(TempFile)
				Next
			Catch Err As Exception
				m_logger.PostEntry("Error deleting files, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error deleting files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

		End If

		'Update the job summary file
		If Not UpdateSummaryFile() Then
			m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
			 ILogger.logMsgType.logWarning, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		StepResult = MakeResultsFolder(ResType)
		If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return StepResult
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function WaitForJobToFinish(ByRef objMasicProgRunner As PRISM.Processes.clsProgRunner) As Boolean

		Dim strErrorMessage As String

		'Wait for completion
		m_JobRunning = True

		While m_JobRunning
			System.Threading.Thread.Sleep(3000)				'Delay for 3 seconds

			If objMasicProgRunner.State = 0 Or objMasicProgRunner.State = 10 Then
				m_JobRunning = False
			Else
				CalculateNewStatus(objMasicProgRunner.Program)						 'Update the status
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " & _
					 "Continuing loop: " & m_ProcessStep & " (" & Math.Round(m_progress, 2).ToString & ")", _
						ILogger.logMsgType.logDebug, True)
				End If
			End If

		End While

		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); State=COMPLETE", _
				 ILogger.logMsgType.logDebug, True)
		End If

		If objMasicProgRunner.State = 10 Or objMasicProgRunner.ExitCode <> 0 Then
			Return False
		Else
			Return True
		End If

	End Function

	Protected Overrides Sub CleanupFailedJob(ByVal OopsMessage As String)
		MyBase.CleanupFailedJob(OopsMessage)
		m_FailedJobCleanedUp = True
	End Sub

End Class
