'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerMSGFDB
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSGFDB or MSGF+ analysis
	'*********************************************************************************************************

#Region "Module Variables"
	Protected mToolVersionWritten As Boolean

	' Path to MSGFDB.jar or MSGFPlus.jar
	Protected mMSGFDbProgLoc As String

	Protected mMSGFPlus As Boolean

	Protected mResultsIncludeAutoAddedDecoyPeptides As Boolean = False

	Protected WithEvents mMSGFDBUtils As clsMSGFDBUtils

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSGFDB tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim CmdStr As String
		Dim intJavaMemorySize As Integer
		Dim strMSGFDbCmdLineOptions As String = String.Empty

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False
		Dim blnTooManySkippedSpectra As Boolean = False
		Dim blnSuccess As Boolean

		Dim FastaFilePath As String = String.Empty
		Dim FastaFileSizeKB As Single
		Dim FastaFileIsDecoy As Boolean

		Dim blnUsingMzXML As Boolean
		Dim strScanTypeFilePath As String = String.Empty
		Dim strAssumedScanType As String = String.Empty
		Dim ResultsFileName As String

		Dim Msg As String

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
			' Note that we need to run MSGFDB with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc As String = m_mgrParams.GetParam("JavaLoc")
			If Not File.Exists(JavaProgLoc) Then
				If JavaProgLoc.Length = 0 Then JavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
				m_message = "Cannot find Java: " & JavaProgLoc
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim blnUseLegacyMSGFDB As Boolean
			Dim strMSGFJarfile As String
			Dim strSearchEngineName As String

			blnUseLegacyMSGFDB = clsMSGFDBUtils.UseLegacyMSGFDB(m_jobParams)

			If blnUseLegacyMSGFDB Then
				mMSGFPlus = False
				strMSGFJarfile = clsMSGFDBUtils.MSGFDB_JAR_NAME
				strSearchEngineName = "MS-GFDB"
			Else
				mMSGFPlus = True
				strMSGFJarfile = clsMSGFDBUtils.MSGFPLUS_JAR_NAME
				strSearchEngineName = "MSGF+"
			End If

			' Determine the path to MSGFDB (or MSGF+)
			' It is important that you pass "MSGFDB" to this function, even if mMSGFPlus = True
			' The reason?  The PeptideHitResultsProcessor (and possibly other software) expects the Tool Version file to be named Tool_Version_Info_MSGFDB.txt
			mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", strMSGFJarfile)

			If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Note: we will store the MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
			mToolVersionWritten = False

			result = DetermineAssumedScanType(strAssumedScanType, blnUsingMzXML, strScanTypeFilePath)
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			End If

			' Initialize mMSGFDBUtils
			mMSGFDBUtils = New clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, mMSGFPlus)

			' Get the FASTA file and index it if necessary
			' Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
			Dim strParameterFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
			result = mMSGFDBUtils.InitializeFastaFile(JavaProgLoc, mMSGFDbProgLoc, FastaFileSizeKB, FastaFileIsDecoy, FastaFilePath, strParameterFilePath)
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			End If

			Dim strInstrumentGroup As String = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

			' Read the MSGFDB Parameter File
			result = mMSGFDBUtils.ParseMSGFDBParameterFile(FastaFileSizeKB, FastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, strMSGFDbCmdLineOptions)
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return result
			ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Problem parsing " & strSearchEngineName & " parameter file"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
			mResultsIncludeAutoAddedDecoyPeptides = mMSGFDBUtils.ResultsIncludeAutoAddedDecoyPeptides

			If mMSGFPlus Then
				ResultsFileName = m_Dataset & "_msgfplus.mzid"
			Else
				ResultsFileName = m_Dataset & "_msgfdb.txt"
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running " & strSearchEngineName)

			' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
			' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
			' (job 611216 succeeded with a value of 5000)
			intJavaMemorySize = m_jobParams.GetJobParameter("MSGFDBJavaMemorySize", 2000)
			If intJavaMemorySize < 512 Then intJavaMemorySize = 512

			'Set up and execute a program runner to run MSGFDB
			CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & mMSGFDbProgLoc

			' Define the input file, output file, and fasta file
			If blnUsingMzXML Then
				CmdStr &= " -s " & m_Dataset & ".mzXML"
			Else
				CmdStr &= " -s " & m_Dataset & "_dta.txt"
			End If

			CmdStr &= " -o " & ResultsFileName
			CmdStr &= " -d " & PossiblyQuotePath(FastaFilePath)

			' Append the remaining options loaded from the parameter file
			CmdStr &= " " & strMSGFDbCmdLineOptions

			' Make sure the machine has enough free memory to run MSGFDB
			Dim blnLogFreeMemoryOnSuccess = Not m_DebugLevel < 1

			If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, strSearchEngineName, blnLogFreeMemoryOnSuccess) Then
				m_message = "Not enough free memory to run " & strSearchEngineName
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)
			End If

			CmdRunner = New clsRunDosProgram(m_WorkDir)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = Path.Combine(m_WorkDir, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
			End With

			m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_STARTING

			blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, strSearchEngineName, True)

			If Not blnSuccess And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
				' Parse the console output file one more time in hopes of finding an error message
				ParseConsoleOutputFile()
			End If

			If Not mToolVersionWritten Then
				If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
					ParseConsoleOutputFile()
				End If
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
			End If


			If Not blnSuccess Then
				Msg = "Error running " & strSearchEngineName
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strSearchEngineName & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strSearchEngineName & " failed (but exit code is 0)")
				End If

				blnProcessingError = True

			Else
				m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB Search Complete")
				End If

				If mMSGFDBUtils.ContinuumSpectraSkipped > 0 Then
					' See if any spectra were processed
					If Not File.Exists(Path.Combine(m_WorkDir, ResultsFileName)) Then
						m_message = "None of the spectra are centroided; unable to process with " & strSearchEngineName
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						blnProcessingError = True
					Else
						' Compute the fraction of all potential spectra that were skipped
						Dim dblFractionSkipped As Double
						Dim strPercentSkipped As String

						dblFractionSkipped = mMSGFDBUtils.ContinuumSpectraSkipped / (mMSGFDBUtils.ContinuumSpectraSkipped + mMSGFDBUtils.SpectraSearched)
						strPercentSkipped = (dblFractionSkipped * 100).ToString("0.0") & "%"

						If dblFractionSkipped > 0.2 Then
							m_message = strSearchEngineName & " skipped " & strPercentSkipped & " of the spectra because they did not appear centroided"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							blnTooManySkippedSpectra = True
						Else
							m_EvalMessage = strSearchEngineName & " processed some of the spectra, but it skipped " & mMSGFDBUtils.ContinuumSpectraSkipped & " spectra that were not centroided (" & strPercentSkipped & " skipped)"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
						End If

					End If

				End If

			End If

			If Not blnProcessingError Then
				result = PostProcessMSGFDBResults(ResultsFileName, JavaProgLoc)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error post-processing the " & strSearchEngineName & "  results"
					End If
					blnProcessingError = True
				End If
			End If

			m_progress = clsMSGFDBUtils.PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		   '2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
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
			m_message = "Error in MSGFDbPlugin->RunTool: " & ex.Message
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If blnTooManySkippedSpectra Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If


	End Function

	''' <summary>
	''' Convert the .mzid file created by MSGF+ to a .tsv file
	''' </summary>
	''' <param name="strMZIDFileName"></param>
	''' <param name="JavaProgLoc"></param>
	''' <returns>The path to the .tsv file if successful; empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function ConvertMZIDToTSV(ByVal strMZIDFileName As String, ByVal JavaProgLoc As String) As String

		Dim strTSVFilePath As String

		strTSVFilePath = mMSGFDBUtils.ConvertMZIDToTSV(JavaProgLoc, mMSGFDbProgLoc, m_Dataset, strMZIDFileName)

		If String.IsNullOrEmpty(strTSVFilePath) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned"
			End If
			Return String.Empty
		Else
			Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

			If splitFastaEnabled Then
				strTSVFilePath = ParallelMSGFPlusRenameFile(strTSVFilePath)
			End If

			Return strTSVFilePath
		End If

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.txt")
		mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.zip")

		' Make the results folder
		result = MakeResultsFolder()
		If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Move the result files into the result folder
			result = MoveResultFiles()
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Move was a success; update strFolderPathToArchive
				strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Protected Function CreateScanTypeFile(ByRef strScanTypeFilePath As String) As Boolean

		Dim objScanTypeFileCreator As clsScanTypeFileCreator
		objScanTypeFileCreator = New clsScanTypeFileCreator(m_WorkDir, m_Dataset)

		strScanTypeFilePath = String.Empty

		If objScanTypeFileCreator.CreateScanTypeFile() Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Created ScanType file: " & Path.GetFileName(objScanTypeFileCreator.ScanTypeFilePath))
			End If
			strScanTypeFilePath = objScanTypeFileCreator.ScanTypeFilePath
			Return True
		Else
			Dim strErrorMessage = "Error creating scan type file: " & objScanTypeFileCreator.ErrorMessage
			m_message = String.Copy(strErrorMessage)

			If Not String.IsNullOrEmpty(objScanTypeFileCreator.ExceptionDetails) Then
				strErrorMessage &= "; " & objScanTypeFileCreator.ExceptionDetails
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage)
			Return False
		End If

	End Function

	Protected Function DetermineAssumedScanType(ByRef strAssumedScanType As String, ByRef blnUsingMzXML As Boolean, ByRef strScanTypeFilePath As String) As IJobParams.CloseOutType
		Dim strScriptNameLCase As String
		strAssumedScanType = String.Empty

		strScriptNameLCase = m_jobParams.GetParam("ToolName").ToLower()
		strScanTypeFilePath = String.Empty

		If strScriptNameLCase.Contains("mzxml") OrElse strScriptNameLCase.Contains("msgfplus_bruker") Then
			blnUsingMzXML = True
		Else
			blnUsingMzXML = False

			' Make sure the _DTA.txt file is valid
			If Not ValidateCDTAFile() Then
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			End If

			strAssumedScanType = m_jobParams.GetParam("AssumedScanType")

			If String.IsNullOrWhiteSpace(strAssumedScanType) Then
				' Create the ScanType file (lists scan type for each scan number)
				If Not CreateScanTypeFile(strScanTypeFilePath) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function ParallelMSGFPlusRenameFile(ByVal resultsFileName As String) As String

		Dim filePathNew = "??"

		Try
			Dim fiFile = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))

			Dim iteration = clsAnalysisResources.GetSplitFastaIteration(m_jobParams, m_message)

			filePathNew = Path.GetFileNameWithoutExtension(fiFile.Name) & "_Part" & iteration & fiFile.Extension
			filePathNew = Path.Combine(m_WorkDir, filePathNew)
			fiFile.MoveTo(filePathNew)

			Return filePathNew

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming file " & resultsFileName & " to " & filePathNew, ex)
			Return (resultsFileName)
		End Try

	End Function

	''' <summary>
	''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile()

		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow
		Dim sngMSGFBProgress As Single = 0

		Try
			If Not mMSGFDBUtils Is Nothing Then
				sngMSGFBProgress = mMSGFDBUtils.ParseMSGFDBConsoleOutputFile()
			End If

			If m_progress < sngMSGFBProgress Then
				m_progress = sngMSGFBProgress

				If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = DateTime.UtcNow
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file: " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function PostProcessMSGFDBResults(ByVal ResultsFileName As String, ByVal JavaProgLoc As String) As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType
		Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

		If splitFastaEnabled Then
			ResultsFileName = ParallelMSGFPlusRenameFile(ResultsFileName)
			ParallelMSGFPlusRenameFile("MSGFDB_ConsoleOutput.txt")
		End If

		' Zip the output file
		result = mMSGFDBUtils.ZipOutputFile(Me, ResultsFileName)
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		If Not mMSGFPlus Then
			m_jobParams.AddResultFileToSkip(ResultsFileName & ".temp.tsv")
		End If

		Dim strMSGFDBResultsFileName As String
		If Path.GetExtension(ResultsFileName).ToLower() = ".mzid" Then
			' Convert the .mzid file to a .tsv file

			UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_CONVERT_MZID_TO_TSV)
			strMSGFDBResultsFileName = ConvertMZIDToTSV(ResultsFileName, JavaProgLoc)

			If String.IsNullOrEmpty(strMSGFDBResultsFileName) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Else
			strMSGFDBResultsFileName = String.Copy(ResultsFileName)
		End If

		' Create the Peptide to Protein map file
		UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

		result = mMSGFDBUtils.CreatePeptideToProteinMapping(strMSGFDBResultsFileName, mResultsIncludeAutoAddedDecoyPeptides)
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
			Return result
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSGFDBUtils.MSGFDbVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(mMSGFDbProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
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
		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			ParseConsoleOutputFile()
			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub

	Private Sub mMSGFDBUtils_ErrorEvent(ByVal errorMessage As String, ByVal detailedMessage As String) Handles mMSGFDBUtils.ErrorEvent
		m_message = String.Copy(errorMessage)
		If String.IsNullOrEmpty(detailedMessage) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
		End If

	End Sub

	Private Sub mMSGFDBUtils_IgnorePreviousErrorEvent() Handles mMSGFDBUtils.IgnorePreviousErrorEvent
		m_message = String.Empty
	End Sub

	Private Sub mMSGFDBUtils_MessageEvent(messageText As String) Handles mMSGFDBUtils.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, messageText)
	End Sub

	Private Sub mMSGFDBUtils_WarningEvent(warningMessage As String) Handles mMSGFDBUtils.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
	End Sub

#End Region

End Class
