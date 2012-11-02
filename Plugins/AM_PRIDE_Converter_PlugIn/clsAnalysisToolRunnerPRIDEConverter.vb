Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerPRIDEConverter
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running PRIDEConverter
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const PRIDEConverter_CONSOLE_OUTPUT As String = "PRIDEConverter_ConsoleOutput.txt"
	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_CREATING_MISSING_MZXML_FILES As Single = 5
	Protected Const PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES As Single = 15
	Protected Const PROGRESS_PCT_CREATING_PRIDE_XML_FILES As Single = 25
	Protected Const PROGRESS_PCT_SAVING_RESULTS As Single = 95
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const FILE_EXTENSION_PSEUDO_MSGF As String = ".msgf"
	Protected Const FILE_EXTENSION_MSGF_REPORT_XML As String = ".msgf-report.xml"
	Protected Const FILE_EXTENSION_MSGF_PRIDE_XML As String = ".msgf-pride.xml"


	Protected mConsoleOutputErrorMsg As String
	Protected mJobToDatasetMap As Generic.Dictionary(Of Integer, String)

	Protected mToolVersionWritten As Boolean
	Protected mPrideConverterVersion As String = String.Empty
	Protected mPrideConverterProgLoc As String = String.Empty

	Protected mJavaProgLoc As String = String.Empty
	Protected mMSXmlGeneratorAppPath As String = String.Empty

	Private WithEvents mMSXmlCreator As AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"

#End Region

#Region "Methods"
	''' <summary>
	''' Runs PRIDEConverter tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType
		Dim dctPrideReportFiles As Generic.Dictionary(Of Integer, String)

		Dim blnSuccess As Boolean

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPRIDEConverter.RunTool(): Enter")
			End If

			' Verify that program files exist
			If Not DefineProgramPaths() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Note: we will store the Pride Converter version info in the database after we process the first job with Pride Converter
			mToolVersionWritten = False
			mPrideConverterVersion = String.Empty
			mConsoleOutputErrorMsg = String.Empty

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PRIDEConverter")

			' Create .mzXML files for any jobs in lstDataPackagePeptideHitJobs for which the .mzXML file wasn't retrieved
			If Not CreateMissingMzXMLFiles() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			dctPrideReportFiles = New Generic.Dictionary(Of Integer, String)

			' Create the .msgf-report.xml file for each job
			' This function will populate dctPrideReportFiles and mJobToDatasetMap
			blnSuccess = CreatePrideReportXMLFiles(dctPrideReportFiles)

			If blnSuccess Then
				' Create the .msgf-Pride.xml file for each job
				blnSuccess = CreatePrideXMLFiles(dctPrideReportFiles)
			End If

			m_progress = PROGRESS_PCT_COMPLETE
			m_StatusTools.UpdateAndWrite(m_progress)

			If blnSuccess Then
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PRIDEConverter Complete")
				End If
			End If

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		   '2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If Not blnSuccess Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				CopyFailedResultsToArchiveFolder()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MakeResultsFolder()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MoveResultFiles()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				m_message = "Error moving files into results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = CopyResultsFolderToServer()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return result
			End If

		Catch ex As Exception
			m_message = "Exception in PRIDEConverterPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		m_jobParams.RemoveResultFileToSkip(PRIDEConverter_CONSOLE_OUTPUT)

		' Try to save whatever files are in the work directory
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		' Make the results folder
		result = MakeResultsFolder()
		If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Move the result files into the result folder
			result = MoveResultFiles()
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Move was a success; update strFolderPathToArchive
				strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Protected Function CreateMissingMzXMLFiles() As Boolean
		Dim blnSuccess As Boolean
		Dim lstDatasets As Generic.List(Of String)
		Dim intDatasetsProcessed As Integer = 0

		Try
			m_progress = PROGRESS_PCT_CREATING_MISSING_MZXML_FILES
			m_StatusTools.UpdateAndWrite(m_progress)

			lstDatasets = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATASETS_MISSING_MZXML_FILES)
			If lstDatasets.Count = 0 Then
				' Nothing to do
				Return True
			End If

			mMSXmlCreator = New AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams)

			For Each strDataset As String In lstDatasets
				mMSXmlCreator.UpdateDatasetName(m_Dataset)

				blnSuccess = mMSXmlCreator.CreateMZXMLFile()

				If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
					m_message = mMSXmlCreator.ErrorMessage
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error creating the mzXML file"
					End If
				End If

				If Not blnSuccess Then Exit For

				intDatasetsProcessed += 1

				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_MISSING_MZXML_FILES, PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES, intDatasetsProcessed, lstDatasets.Count)
				m_StatusTools.UpdateAndWrite(m_progress)
			Next

		Catch ex As Exception
			m_message = "Exception in CreateMissingMzXMLFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function CreateMSGFFileUsingPHRPReader(ByVal intJob As Integer, ByVal strDataset As String) As String
		Dim strPseudoMsgfFilePath As String = String.Empty

		' ToDo:

		Return strPseudoMsgfFilePath

	End Function

	Protected Function CreatePrideReportXMLFile(ByVal intJob As Integer, ByVal strDataset As String, ByVal strPseudoMsgfFilePath As String) As String

		Dim strPrideReportXMLFilePath As String = String.Empty

		' ToDo:

		Return strPrideReportXMLFilePath

	End Function

	Protected Function CreatePrideReportXMLFiles(ByRef dctPrideReportFiles As Generic.Dictionary(Of Integer, String)) As Boolean

		Dim blnSuccess As Boolean
		Dim lstJobsAndDatasets As Generic.List(Of String)
		Dim intDividerIndex As Integer

		Dim intJob As Integer = 0
		Dim strDataset As String = "??"

		Dim strPseudoMsgfFilePath As String
		Dim strPrideReportXMLFilePath As String

		mJobToDatasetMap = New Generic.Dictionary(Of Integer, String)
		dctPrideReportFiles = New Generic.Dictionary(Of Integer, String)

		Try
			m_progress = PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES
			m_StatusTools.UpdateAndWrite(m_progress)

			lstJobsAndDatasets = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATASET_PACKAGE_JOBS_AND_DATASETS)
			If lstJobsAndDatasets.Count = 0 Then
				m_message = "Job parameter " & clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATASET_PACKAGE_JOBS_AND_DATASETS & " is empty; no jobs to process"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			For Each strJobDataCombo As String In lstJobsAndDatasets

				' Parse out the Job and Dataset
				intDividerIndex = strJobDataCombo.IndexOf("|")
				If intDividerIndex > 0 Then
					If Not Integer.TryParse(strJobDataCombo.Substring(0, intDividerIndex), intJob) Then
						m_message = "Job number not numeric in " & strJobDataCombo & "; unable to continue"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return False
					End If

					strDataset = strJobDataCombo.Substring(intDividerIndex + 1)

					If Not mJobToDatasetMap.ContainsKey(intJob) Then
						mJobToDatasetMap.Add(intJob, strDataset)
					End If

					strPseudoMsgfFilePath = CreateMSGFFileUsingPHRPReader(intJob, strDataset)

					If String.IsNullOrEmpty(strPseudoMsgfFilePath) Then
						If String.IsNullOrEmpty(m_message) Then
							m_message = "Pseudo Msgf file not created for job " & intJob & ", dataset " & strDataset
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						End If
						Return False
					End If

					strPrideReportXMLFilePath = CreatePrideReportXMLFile(intJob, strDataset, strPseudoMsgfFilePath)

					If String.IsNullOrEmpty(strPrideReportXMLFilePath) Then
						If String.IsNullOrEmpty(m_message) Then
							m_message = "Pride report XML file not created for job " & intJob & ", dataset " & strDataset
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						End If
						Return False
					End If

					dctPrideReportFiles.Add(intJob, strPrideReportXMLFilePath)

					m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES, PROGRESS_PCT_CREATING_PRIDE_XML_FILES, dctPrideReportFiles.Count, lstJobsAndDatasets.Count)
					m_StatusTools.UpdateAndWrite(m_progress)

				End If

			Next

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in CreatePrideReportXMLFiles for job " & intJob & ", dataset " & strDataset
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function CreatePrideXMLFiles(ByVal dctPrideReportFiles As Generic.Dictionary(Of Integer, String)) As Boolean

		Dim blnSuccess As Boolean
		Dim intJob As Integer
		Dim strDataset As String = String.Empty
		Dim strMsgfResultsFilePath As String
		Dim strMzXMLFilePath As String
		Dim strPrideReportFilePath As String
		Dim strPrideXmlFilePath As String

		Dim intJobsProcessed As Integer = 0

		Try
			m_progress = PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES
			m_StatusTools.UpdateAndWrite(m_progress)

			For Each kvItem As Generic.KeyValuePair(Of Integer, String) In dctPrideReportFiles

				' ToDo:
				intJob = kvItem.Key
				strDataset = LookupDatasetByJob(intJob)

				strMsgfResultsFilePath = IO.Path.Combine(m_WorkDir, strDataset & FILE_EXTENSION_PSEUDO_MSGF)
				strMzXMLFilePath = IO.Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZML_EXTENSION)
				strPrideReportFilePath = IO.Path.Combine(m_WorkDir, strDataset & FILE_EXTENSION_MSGF_REPORT_XML)

				blnSuccess = RunPrideConverter(intJob, strDataset, strMsgfResultsFilePath, strMzXMLFilePath, strPrideReportFilePath)

				If Not blnSuccess Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error calling RunPrideConverter"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					End If
					Return False
				Else
					' Make sure the result file was created
					strPrideXmlFilePath = IO.Path.Combine(m_WorkDir, strDataset & FILE_EXTENSION_MSGF_PRIDE_XML)
					If Not IO.File.Exists(strPrideXmlFilePath) Then
						m_message = "Pride XML file not created for job " & intJob & ": " & strPrideXmlFilePath
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return False
					End If
				End If

				intJobsProcessed += 1

				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_PRIDE_REPORT_XML_FILES, PROGRESS_PCT_CREATING_PRIDE_XML_FILES, intJobsProcessed, dctPrideReportFiles.Count)
				m_StatusTools.UpdateAndWrite(m_progress)
			Next

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in CreatePrideXMLFiles for job " & intJob & ", dataset " & strDataset
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function DefineProgramPaths() As Boolean

		' mJavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
		' Note that we need to run MSGF with a 64-bit version of Java since it prefers to use 2 or more GB of ram
		mJavaProgLoc = m_mgrParams.GetParam("JavaLoc")
		If Not System.IO.File.Exists(mJavaProgLoc) Then
			If mJavaProgLoc.Length = 0 Then mJavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
			m_message = "Cannot find Java: " & mJavaProgLoc
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		' Determine the path to the PRIDEConverter program
		mPrideConverterProgLoc = DetermineProgramLocation("PRIDEConverter", "PRIDEConverterProgLoc", "pride-converter-2.0-SNAPSHOT.jar")

		If String.IsNullOrEmpty(mPrideConverterProgLoc) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error determining PrideConverter program location"
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		mMSXmlGeneratorAppPath = MyBase.GetMSXmlGeneratorAppPath()

		Return True

	End Function

	Protected Function ExtractPackedJobParameterList(ByVal strParameterName As String) As Generic.List(Of String)

		Dim strList As String

		strList = m_jobParams.GetJobParameter(strParameterName, String.Empty)

		If String.IsNullOrEmpty(strList) Then
			Return New Generic.List(Of String)
		Else
			Return strList.Split(ControlChars.Tab).ToList()
		End If

	End Function

	Protected Function LookupDatasetByJob(ByVal intJob As Integer) As String
		Dim strDataset As String = String.Empty

		If Not mJobToDatasetMap Is Nothing Then
			If mJobToDatasetMap.TryGetValue(intJob, strDataset) Then
				Return strDataset
			End If
		End If

		Return strDataset
	End Function
	''' <summary>
	''' Parse the PRIDEConverter console output file to determine the PRIDE Version
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output:
		'
		' ????

		Try

			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If


			Dim strLineIn As String
			Dim intLinesRead As Integer

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				intLinesRead = 0
				Do While srInFile.Peek() > -1
					strLineIn = srInFile.ReadLine()
					intLinesRead += 1

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						If intLinesRead = 1 Then

							''''''''''''''''''''''''''
							''''      TO FIX      ''''
							''''''''''''''''''''''''''
							'
							' The first line is the Pride Converter version

							If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mPrideConverterVersion) Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PrideConverter version: " & strLineIn)
							End If

							mPrideConverterVersion = String.Copy(strLineIn)

						Else
							If strLineIn.ToLower.Contains("error") Then
								If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
									mConsoleOutputErrorMsg = "Error running Pride Converter:"
								End If
								mConsoleOutputErrorMsg &= "; " & strLineIn
							End If
						End If
					End If
				Loop

			End Using

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function RunPrideConverter(ByVal intJob As Integer, ByVal strDataset As String, ByVal strMsgfResultsFilePath As String, ByVal strMzXMLFilePath As String, ByVal strPrideReportFilePath As String) As Boolean

		Dim CmdStr As String

		If String.IsNullOrEmpty(strMsgfResultsFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "strMsgfResultsFilePath has not been defined; unable to continue")
			Return False
		End If

		If String.IsNullOrEmpty(strMzXMLFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "strMzXMLFilePath has not been defined; unable to continue")
			Return False
		End If

		If String.IsNullOrEmpty(strPrideReportFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "strPrideReportFilePath has not been defined; unable to continue")
			Return False
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PrideConverter on " & System.IO.Path.GetFileName(strMsgfResultsFilePath))
		End If

		m_StatusTools.CurrentOperation = "Running PrideConverter"
		m_StatusTools.UpdateAndWrite(m_progress)

		CmdStr = "-jar " & PossiblyQuotePath(mPrideConverterProgLoc)

		CmdStr &= " -converter -mode convert -engine msgf -sourcefile " & PossiblyQuotePath(strMsgfResultsFilePath)		' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf
		CmdStr &= " -spectrafile " & PossiblyQuotePath(strMzXMLFilePath)												' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.mzXML
		CmdStr &= " -reportfile " & PossiblyQuotePath(strPrideReportFilePath)											' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf-report.xml
		CmdStr &= " -reportOnlyIdentifiedSpectra"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc & " " & CmdStr)

		With CmdRunner
			.CreateNoWindow = False
			.CacheStandardOutput = False
			.EchoOutputToConsole = False

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT)
		End With

		Dim blnSuccess As Boolean
		blnSuccess = CmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", True)

		If Not mToolVersionWritten Then
			If String.IsNullOrWhiteSpace(mPrideConverterVersion) Then
				Dim fiConsoleOutputfile As New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT))
				If fiConsoleOutputfile.Length = 0 Then
					' File is 0-bytes; delete it
					DeleteTemporaryfile(fiConsoleOutputfile.FullName)
				Else
					ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT))
				End If
			End If
			mToolVersionWritten = StoreToolVersionInfo()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			m_message = "Error running PrideConverter, dataset " & strDataset & ", job " & intJob.ToString()
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mPrideConverterVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mPrideConverterProgLoc))

		ioToolFiles.Add(New System.IO.FileInfo(mMSXmlGeneratorAppPath))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex)
			Return False
		End Try

	End Function

	Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
		m_progress = sngPercentComplete
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
	End Sub

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow
		Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT))
			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mPrideConverterVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub

	Private Sub mMSXmlCreator_DebugEvent(Message As String) Handles mMSXmlCreator.DebugEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Message)
	End Sub

	Private Sub mMSXmlCreator_ErrorEvent(Message As String) Handles mMSXmlCreator.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message)
	End Sub

	Private Sub mMSXmlCreator_WarningEvent(Message As String) Handles mMSXmlCreator.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Message)
	End Sub

	Private Sub mMSXmlCreator_LoopWaiting() Handles mMSXmlCreator.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(m_progress)
		End If
	End Sub

#End Region
End Class
