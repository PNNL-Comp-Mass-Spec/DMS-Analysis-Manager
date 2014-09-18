'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 09/05/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices
Imports AnalysisManagerMSGFDBPlugIn

''' <summary>
''' Class for running Mz Refinery to recalibrate m/z values in a .mzML file
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerMzRefinery
	Inherits clsAnalysisToolRunnerBase

#Region "Constants and Enums"
	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_MZREFINERY_COMPLETE As Single = 97
	Protected Const PROGRESS_PCT_PLOTS_GENERATED As Single = 98
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const MZ_REFINERY_CONSOLE_OUTPUT As String = "MzRefinery_ConsoleOutput.txt"
	Protected Const ERROR_CHARTER_CONSOLE_OUTPUT_FILE As String = "PPMErrorCharter_ConsoleOutput.txt"

#End Region

#Region "Module Variables"

	Protected mToolVersionWritten As Boolean

	Protected mConsoleOutputErrorMsg As String

	Protected mMSGFDbProgLoc As String
	Protected mMzRefineryProgLoc As String
	Protected mPpmErrorCharterProgLoc As String

	Protected mMSGFPlusResultsFilePath As String

	Protected mRunningMSGFPlus As Boolean
	Protected mRunningMzRefinery As Boolean

	Protected mMSGFPlusComplete As Boolean
	Protected mMSGFPlusCompletionTime As DateTime

	Protected mMzRefineryCorrectionMode As String

	Protected WithEvents mMSGFDBUtils As clsMSGFDBUtils

	Protected mMSXmlCacheFolder As DirectoryInfo

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"

	''' <summary>
	''' Runs MSGF+ then runs MzRefinery
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType

		Try
			' Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMzRefinery.RunTool(): Enter")
			End If

			' Verify that program files exist

			' Determine the path to the MzRefinery program
			mMzRefineryProgLoc = DetermineProgramLocation("MzRefinery", "MzRefineryProgLoc", "mzML_Refinery.exe")

			If String.IsNullOrWhiteSpace(mMzRefineryProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the PPM Error Charter program
			mPpmErrorCharterProgLoc = DetermineProgramLocation("MzRefinery", "MzRefineryProgLoc", "PPMErrorCharter.exe")

			If String.IsNullOrWhiteSpace(mPpmErrorCharterProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
			Dim javaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(javaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
			mMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

			If Not mMSXmlCacheFolder.Exists Then
				LogError("MSXmlCache folder not found: " & msXMLCacheFolderPath)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If


			' Run MSGF+ (includes indexing the fasta file)
			Dim fiMSGFPlusResults As FileInfo = Nothing

			result = RunMSGFPlus(javaProgLoc, fiMSGFPlusResults)

			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error running MSGF+ prior to running MzRefinery"
				End If
				Return result
			End If

			CmdRunner = Nothing

			' Run MzRefinery
			Dim blnSuccess = StartMzRefinery(fiMSGFPlusResults)

			' Look for the results file
			Dim fiMzRefResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_FIXED.mzML"))
			m_jobParams.AddResultFileExtensionToSkip(fiMzRefResultsFile.Extension)

			If fiMzRefResultsFile.Exists Then
				blnSuccess = PostProcessMzRefineryResults(fiMSGFPlusResults, fiMzRefResultsFile)
			Else
				If String.IsNullOrEmpty(m_message) Then
					m_message = "MzRefinery results file not found: " & fiMzRefResultsFile.Name
					blnSuccess = False
				End If
			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		'2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If blnSuccess Then
				m_jobParams.AddResultFileExtensionToSkip(fiMSGFPlusResults.Extension)
			Else
				' Move the source files and any results to the Failed Job folder
				' Useful for debugging problems
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
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			m_message = "Error in MzRefineryPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Index the Fasta file (if needed) then run MSGF+
	''' </summary>
	''' <param name="javaProgLoc"></param>
	''' <param name="fiMSGFPlusResults"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function RunMSGFPlus(
	  ByVal javaProgLoc As String,
	  <Out()> ByRef fiMSGFPlusResults As FileInfo) As IJobParams.CloseOutType

		Const strMSGFJarfile As String = clsMSGFDBUtils.MSGFPLUS_JAR_NAME
		Const strSearchEngineName = "MSGF+"

		fiMSGFPlusResults = Nothing

		' Determine the path to MSGF+
		' It is important that you pass "MSGFDB" to this function because the 
		' PeptideHitResultsProcessor (and possibly other software) expects the Tool Version file to be named Tool_Version_Info_MSGFDB.txt
		mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", strMSGFJarfile)

		If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Note: we will store the MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
		mToolVersionWritten = False

		mMSGFPlusComplete = False

		' These two variables are required for the call to ParseMSGFDBParameterFile
		' They are blank because the source file is a mzML file, and that file includes scan type information
		Dim strScanTypeFilePath As String = String.Empty
		Dim strAssumedScanType As String = String.Empty

		' Initialize mMSGFDBUtils
		mMSGFDBUtils = New clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, blnMSGFPlus:=True)

		' Get the FASTA file and index it if necessary
		' Note: if the fasta file is over 50 MB in size, then only use the first 50 MB

		' Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
		Dim strParameterFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetJobParameter("MzRefParamFile", String.Empty))
		Dim javaExePath = String.Copy(javaProgLoc)
		Dim msgfdbJarFilePath = String.Copy(mMSGFDbProgLoc)

		Dim fastaFilePath As String = String.Empty
		Dim fastaFileSizeKB As Single
		Dim fastaFileIsDecoy As Boolean

		Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

		Const maxFastaFileSizeMB As Integer = 50

		' Initialize the fasta file; truncating it if it is over 50 MB in size
		Dim result = mMSGFDBUtils.InitializeFastaFile(
		  javaExePath, msgfdbJarFilePath,
		  fastaFileSizeKB, fastaFileIsDecoy, fastaFilePath,
		  strParameterFilePath, udtHPCOptions, maxFastaFileSizeMB)

		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		Dim strInstrumentGroup As String = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

		' Read the MSGFDB Parameter File
		Dim strMSGFDbCmdLineOptions = String.Empty

		result = mMSGFDBUtils.ParseMSGFDBParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, strParameterFilePath, udtHPCOptions, strMSGFDbCmdLineOptions)
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Problem parsing MzRef parameter file to extract MGSF+ options"
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim resultsFileName = m_Dataset & "_msgfplus.mzid"
		fiMSGFPlusResults = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running " & strSearchEngineName)

		' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
		' The amount of memory required depends on both the fasta file size and the size of the input .mzML file, since data from all spectra are cached in memory
		' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
		Dim intJavaMemorySize = m_jobParams.GetJobParameter("MzRefMSGFPlusJavaMemorySize", 1500)
		If intJavaMemorySize < 512 Then intJavaMemorySize = 512

		'Set up and execute a program runner to run MSGFDB
		Dim CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & msgfdbJarFilePath

		' Define the input file, output file, and fasta file
		CmdStr &= " -s " & m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION

		CmdStr &= " -o " & fiMSGFPlusResults.Name
		CmdStr &= " -d " & PossiblyQuotePath(fastaFilePath)

		' Append the remaining options loaded from the parameter file
		CmdStr &= " " & strMSGFDbCmdLineOptions

		' Make sure the machine has enough free memory to run MSGF+
		Dim blnLogFreeMemoryOnSuccess = Not m_DebugLevel < 1

		If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, strSearchEngineName, blnLogFreeMemoryOnSuccess) Then
			m_message = "Not enough free memory to run " & strSearchEngineName
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim blnSuccess = StartMSGFPlus(javaExePath, strSearchEngineName, CmdStr)

		If Not blnSuccess And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
			' Parse the console output file one more time in hopes of finding an error message
			ParseMSGFPlusConsoleOutputFile(m_WorkDir)
		End If

		If Not mToolVersionWritten Then
			If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
				ParseMSGFPlusConsoleOutputFile(m_WorkDir)
			End If
			mToolVersionWritten = StoreToolVersionInfo()
		End If

		If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
		End If

		Dim blnProcessingError As Boolean = False

		If blnSuccess Then
			If Not mMSGFPlusComplete Then
				mMSGFPlusComplete = True
				mMSGFPlusCompletionTime = DateTime.UtcNow
			End If
		Else
			Dim msg As String
			If mMSGFPlusComplete Then
				msg = strSearchEngineName & " log file reported it was complete, but aborted the ProgRunner since Java was frozen"
			Else
				msg = "Error running " & strSearchEngineName
			End If
			m_message = clsGlobal.AppendToComment(m_message, msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

			If mMSGFPlusComplete Then
				' Don't treat this as a fatal error
				blnProcessingError = False
				m_EvalMessage = String.Copy(m_message)
				m_message = String.Empty
			Else
				blnProcessingError = True
			End If

			If Not mMSGFPlusComplete Then
				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strSearchEngineName & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strSearchEngineName & " failed (but exit code is 0)")
				End If
			End If

		End If

		If mMSGFPlusComplete Then
			m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE
			m_StatusTools.UpdateAndWrite(m_progress)
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB Search Complete")
			End If
		End If

		' Look for the .mzid file
		fiMSGFPlusResults.Refresh()

		If Not fiMSGFPlusResults.Exists Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "MSGF+ results file not found: " & resultsFileName
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		m_jobParams.AddResultFileToSkip(clsMSGFDBUtils.MOD_FILE_NAME)

		If blnProcessingError Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Protected Function StartMSGFPlus(ByVal javaExePath As String, ByVal strSearchEngineName As String, ByVal CmdStr As String) As Boolean

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaExePath & " " & CmdStr)
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

		mRunningMSGFPlus = True

		' Start MSGF+ and wait for it to exit
		Dim blnSuccess = CmdRunner.RunProgram(javaExePath, CmdStr, strSearchEngineName, True)

		mRunningMSGFPlus = False

		Return blnSuccess

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory (however, delete any .mzML files first)
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		Try
			Dim fiFiles = New DirectoryInfo(m_WorkDir).GetFiles("*" & clsAnalysisResources.DOT_MZML_EXTENSION)
			For Each fiFileToDelete In fiFiles
				fiFileToDelete.Delete()
			Next
		Catch ex As Exception
			' Ignore errors here
		End Try

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

	Private Sub MonitorProgress()

		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 15 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 15 Then
			dtLastStatusUpdate = DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 30 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			If mRunningMSGFPlus Then

				ParseMSGFPlusConsoleOutputFile(m_WorkDir)
				If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
					mToolVersionWritten = StoreToolVersionInfo()
				End If

				If m_progress >= clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE Then
					If Not mMSGFPlusComplete Then
						mMSGFPlusComplete = True
						mMSGFPlusCompletionTime = DateTime.UtcNow
					Else
						If DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes >= 5 Then
							' MSGF+ is stuck at 96% complete and has been that way for 5 minutes
							' Java is likely frozen and thus the process should be aborted
							Dim warningMessage = "MSGF+ has been stuck at " & clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE.ToString("0") & "% complete for 5 minutes; aborting since Java appears frozen"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)

							' Bump up mMSGFPlusCompletionTime by one hour
							' This will prevent this function from logging the above message every 30 seconds if the .abort command fails
							mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1)

							CmdRunner.AbortProgramNow()

						End If
					End If
				End If

			ElseIf mRunningMzRefinery Then
				ParseMzRefineryConsoleOutputFile(Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT))
			End If

		End If
	End Sub


	''' <summary>
	''' Parse the MSGF+ console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <remarks></remarks>
	Private Sub ParseMSGFPlusConsoleOutputFile(ByVal workingDirectory As String)

		Try
			If Not mMSGFDBUtils Is Nothing Then
				Dim msgfPlusProgress = mMSGFDBUtils.ParseMSGFDBConsoleOutputFile(workingDirectory)
				UpdateProgress(msgfPlusProgress)
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MSGF+ console output file: " & ex.Message)
			End If
		End Try

	End Sub

	''' <summary>
	''' Parse the MzRefinery console output file to look for errors
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseMzRefineryConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'

		'         Filtered out 10430 identifications.
		'         Good data points:                                 4645
		'         Average: global ppm Errors:                       3.22947
		'         Systematic Drift (mode):                          3
		'         Systematic Drift (median):                        3
		'         Measurement Precision (stdev ppm):                2.86259
		'         Measurement Precision (stdev(mode) ppm):          2.87108
		'         Measurement Precision (stdev(median) ppm):        2.87108
		'         Average BinWise stdev (scan):                     2.60397
		'         Expected % Improvement (scan):                    9.03471
		'         Expected % Improvement (scan)(mode):              9.30347
		'         Expected % Improvement (scan)(median):            9.03471
		'         Average BinWise stdev (smoothed scan):            2.67439
		'         Expected % Improvement (smoothed scan):           6.57469
		'         Expected % Improvement (smoothed scan)(mode):     6.85072
		'         Expected % Improvement (smoothed scan)(median):   6.57469
		'         Average BinWise stdev (mz):                       2.49953
		'         Expected % Improvement (mz):                      12.683
		'         Expected % Improvement (mz)(mode):                12.941
		'         Expected % Improvement (mz)(median):              12.683
		'         Average BinWise stdev (smoothed mz):              2.60274
		'         Expected % Improvement (smoothed mz):             9.07751
		'         Expected % Improvement (smoothed mz)(mode):       9.34615
		'         Expected % Improvement (smoothed mz)(median):     9.07751
		' Chose mass to charge shift...
		' Reading mzML file    "E:\DMS_WorkDir\QC_Shew_13_07_500ng_CID_A_15Jul14_Lynx_14-02-24.mzML"
		' Outputting fixed to: "E:\DMS_WorkDir\QC_Shew_13_07_500ng_CID_A_15Jul14_Lynx_14-02-24_FIXED.mzML"
		' mzML
		'     m/z: Compression-None, 32-bit
		'     intensity: Compression-None, 32-bit
		'     rt: Compression-None, 32-bit
		' ByteOrder_LittleEndian
		'  indexed="true"

		Try
			If Not File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srInFile.Peek() >= 0
					Dim strLineIn = srInFile.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						Dim strLineInLCase = strLineIn.ToLower()

						If strLineInLCase.StartsWith("error:") OrElse strLineInLCase.Contains("unhandled exception") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MzRefinery:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
							Continue Do
						ElseIf strLineInLCase.StartsWith("chose ") Then
							mMzRefineryCorrectionMode = String.Copy(strLineIn)
						End If

					End If
				Loop

			End Using

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MzRefinery console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Private Function PostProcessMzRefineryResults(ByVal fiMSGFPlusResults As FileInfo, ByVal fiMzRefResultsFile As FileInfo) As Boolean

		Dim mzMLFilePathForDataset = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

		Try
			' Create the plots
			Dim blnSuccess = StartPpmErrorCharter(fiMSGFPlusResults)

			If Not blnSuccess Then
				Return False
			End If

			' Store the PPM Mass Errors in the database
			blnSuccess = StorePPMErrorStatsInDB()
			If Not blnSuccess Then
				Return False
			End If

		Catch ex As Exception
			m_message = "Error creating PPM Error charters"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Try
			' Delete the original .mzML file
			DeleteFileWithRetries(mzMLFilePathForDataset, m_DebugLevel, 2)

		Catch ex As Exception
			m_message = "Error replacing the original .mzML file with the updated version; cannot delete original"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Try

			' Rename the fixed mzML file
			fiMzRefResultsFile.MoveTo(mzMLFilePathForDataset)

		Catch ex As Exception
			m_message = "Error replacing the original .mzML file with the updated version; cannot rename the fixed file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Try

			' Compress the .mzML file
			Dim blnSuccess = m_IonicZipTools.GZipFile(fiMzRefResultsFile.FullName, True)

			If Not blnSuccess Then
				m_message = m_IonicZipTools.Message
				Return False
			End If

		Catch ex As Exception
			m_message = "Error compressing the fixed .mzML file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Try

			Dim fiMzRefFileGzipped = New FileInfo(fiMzRefResultsFile.FullName & clsAnalysisResources.DOT_GZ_EXTENSION)

			' Copy the .mzML.gz file to the cache
			Dim remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiMzRefFileGzipped.FullName, purgeOldFilesIfNeeded:=True)

			If String.IsNullOrEmpty(remoteCachefilePath) Then
				If String.IsNullOrEmpty(m_message) Then
					LogError("CopyFileToServerCache returned false for " & fiMzRefFileGzipped.Name)
				End If
				Return False
			End If

			' Create the _CacheInfo.txt file
			Dim cacheInfoFilePath = fiMzRefFileGzipped.FullName & "_CacheInfo.txt"
			Using swOutFile = New StreamWriter(New FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
				swOutFile.WriteLine(remoteCachefilePath)
			End Using

			m_jobParams.AddResultFileToSkip(fiMzRefFileGzipped.Name)

			Return True

		Catch ex As Exception
			m_message = "Error copying the .mzML.gz file to the remote cache folder"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

	End Function

	Protected Function StartMzRefinery(ByVal fiMSGFPlusResults As FileInfo) As Boolean

		mConsoleOutputErrorMsg = String.Empty

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MzRefinery")

		'Set up and execute a program runner to run MzRefinery
		Dim CmdStr = " " & fiMSGFPlusResults.FullName

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mMzRefineryProgLoc & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT)
		End With

		m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE

		mRunningMzRefinery = True

		' Start MzRefinery and wait for it to exit
		Dim blnSuccess = CmdRunner.RunProgram(mMzRefineryProgLoc, CmdStr, "MzRefinery", True)

		mRunningMzRefinery = False

		If Not CmdRunner.WriteConsoleOutputToFile Then
			' Write the console output to a text file
			System.Threading.Thread.Sleep(250)

			Dim swConsoleOutputfile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
			swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
			swConsoleOutputfile.Close()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		' Parse the console output file one more time to check for errors and to make sure mMzRefineryCorrectionMode is up-to-date
		System.Threading.Thread.Sleep(250)
		ParseMzRefineryConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

		If Not String.IsNullOrEmpty(mMzRefineryCorrectionMode) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MzRefinery " & mMzRefineryCorrectionMode)
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running MzRefinery"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MzRefinery returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MzRefinery failed (but exit code is 0)")
			End If

			Return False

		End If

		m_progress = PROGRESS_PCT_MZREFINERY_COMPLETE
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MzRefinery Complete")
		End If

		Return True

	End Function

	Protected Function StartPpmErrorCharter(ByVal fiMSGFPlusResults As FileInfo) As Boolean

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PPMErrorCharter")

		' Set up and execute a program runner to run the PPMErrorCharter
		Dim CmdStr = " " & fiMSGFPlusResults.FullName

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mPpmErrorCharterProgLoc & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE)
		End With

		' Start the PPM Error Chararter and wait for it to exit
		Dim blnSuccess = CmdRunner.RunProgram(mPpmErrorCharterProgLoc, CmdStr, "PPMErrorCharter", True)

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running PPMErrorCharter"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PPMErrorCharter returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to PPMErrorCharter failed (but exit code is 0)")
			End If

			Return False

		End If

		' Make sure the plots were created
		Dim lstCharts = New List(Of FileInfo)

		lstCharts.Add(New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_MZRefinery_MassErrors.png")))
		lstCharts.Add(New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_MZRefinery_Histograms.png")))

		For Each fiChart In lstCharts
			If Not fiChart.Exists Then
				m_message = "PPMError chart not found: " & fiChart.Name
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If
		Next

		m_progress = PROGRESS_PCT_PLOTS_GENERATED
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PPMErrorCharter Complete")
		End If

		Return True

	End Function

	Private Function StorePPMErrorStatsInDB() As Boolean

		Dim oMassErrorExtractor = New clsMzRefineryMassErrorStatsExtractor(m_mgrParams, m_WorkDir, m_DebugLevel, blnPostResultsToDB:=True)
		Dim blnSuccess As Boolean

		Dim intDatasetID As Integer = m_jobParams.GetJobParameter("DatasetID", 0)
		Dim intJob As Integer
		Integer.TryParse(m_JobNum, intJob)

		Dim consoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE)
		blnSuccess = oMassErrorExtractor.ParsePPMErrorCharterOutput(m_Dataset, intDatasetID, intJob, consoleOutputFilePath)

		If Not blnSuccess Then
			If String.IsNullOrEmpty(oMassErrorExtractor.ErrorMessage) Then
				m_message = "Error parsing PMM Error Charter output to extract mass error stats"
			Else
				m_message = oMassErrorExtractor.ErrorMessage
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
		End If

		m_jobParams.AddResultFileToSkip(ERROR_CHARTER_CONSOLE_OUTPUT_FILE)

		Return blnSuccess

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
		ioToolFiles.Add(New FileInfo(mMzRefineryProgLoc))
		ioToolFiles.Add(New FileInfo(mPpmErrorCharterProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Private Sub UpdateProgress(
	  ByVal currentTaskProgressAtStart As Single,
	  ByVal currentTaskProgressAtEnd As Single,
	  ByVal subTaskProgress As Single)

		Dim progressCompleteOverall = ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, subTaskProgress)

		UpdateProgress(progressCompleteOverall)

	End Sub

	Private Sub UpdateProgress(ByVal progressCompleteOverall As Single)

		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

		If m_progress < progressCompleteOverall Then
			m_progress = progressCompleteOverall

			If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
				dtLastProgressWriteTime = DateTime.UtcNow
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
			End If
		End If

	End Sub

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

		MonitorProgress()

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
