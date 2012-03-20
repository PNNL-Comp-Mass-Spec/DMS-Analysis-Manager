'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
'Imports PRISM.Files
'Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMSGFDB
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSGFDB analysis
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const MSGFDB_CONSOLE_OUTPUT As String = "MSGFDB_ConsoleOutput.txt"
	Protected Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

	Protected Const PROGRESS_PCT_MSGFDB_STARTING As Single = 1
	Protected Const PROGRESS_PCT_MSGFDB_LOADING_DATABASE As Single = 2
	Protected Const PROGRESS_PCT_MSGFDB_READING_SPECTRA As Single = 3
	Protected Const PROGRESS_PCT_MSGFDB_THREADS_SPAWNED As Single = 4
	Protected Const PROGRESS_PCT_MSGFDB_COMPUTING_FDRS As Single = 95
	Protected Const PROGRESS_PCT_MSGFDB_COMPLETE As Single = 96
	Protected Const PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS As Single = 97
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const MSGFDB_OPTION_TDA As String = "TDA"
	Protected Const MSGFDB_OPTION_SHOWDECOY As String = "showDecoy"
	Protected Const MSGFDB_OPTION_FRAGMENTATION_METHOD As String = "FragmentationMethodID"

	Protected Enum eThreadProgressSteps
		PreprocessingSpectra = 0
		DatabaseSearch = 1
		ComputingSpectralProbabilities = 2
		Complete = 3
	End Enum

	Protected Const THREAD_PROGRESS_PCT_PREPROCESSING_SPECTRA As Single = 0
	Protected Const THREAD_PROGRESS_PCT_DATABASE_SEARCH As Single = 5
	Protected Const THREAD_PROGRESS_PCT_COMPUTING_SPECTRAL_PROBABILITIES As Single = 50
	Protected Const THREAD_PROGRESS_PCT_COMPLETE As Single = 100

	Protected mToolVersionWritten As Boolean
	Protected mMSGFDbVersion As String
	Protected mMSGFDbProgLoc As String
	Protected mConsoleOutputErrorMsg As String

	Protected mResultsIncludeDecoyPeptides As Boolean = False

	Protected WithEvents CmdRunner As clsRunDosProgram

	Private WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

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
		Dim strMSGFDbCmdLineOptions As String

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False

		Dim blnSuccess As Boolean

		Dim OrgDbDir As String
		Dim strFASTAFilePath As String
		Dim FastaFileSizeKB As Single

		Dim strParameterFilePath As String
		Dim ResultsFileName As String

		Dim objIndexedDBCreator As New clsCreateMSGFDBSuffixArrayFiles

		Dim strScriptName As String
		Dim blnUsingMzXML As Boolean
		Dim strAssumedScanType As String = String.Empty

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
			' Note that we need to run MSGFDB with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc As String = m_mgrParams.GetParam("JavaLoc")
			If Not System.IO.File.Exists(JavaProgLoc) Then
				If JavaProgLoc.Length = 0 Then JavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Java: " & JavaProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MSGFDB program
			mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", MSGFDB_JAR_NAME)

			If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Note: we will store the MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
			mToolVersionWritten = False
			mMSGFDbVersion = String.Empty
			mConsoleOutputErrorMsg = String.Empty

			strScriptName = m_jobParams.GetParam("ToolName")

			If strScriptName.ToLower().Contains("mzxml") OrElse strScriptName.ToLower().Contains("msgfdb_bruker") Then
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
					If Not CreateScanTypeFile() Then
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					Else
						' Keep the _ScanType.txt file; don't delete it
						' clsGlobal.m_FilesToDeleteExt.Add("_ScanType.txt")
					End If
				End If

			End If

			' Define the path to the fasta file
			OrgDbDir = m_mgrParams.GetParam("orgdbdir")
			strFASTAFilePath = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

			Dim fiFastaFile As System.IO.FileInfo
			fiFastaFile = New System.IO.FileInfo(strFASTAFilePath)

			If Not fiFastaFile.Exists Then
				' Fasta file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			FastaFileSizeKB = CSng(fiFastaFile.Length / 1024.0)

			Dim blnFastaFileIsDecoy As Boolean = False
			Dim strProteinOptions As String
			strProteinOptions = m_jobParams.GetParam("ProteinOptions")
			If Not String.IsNullOrEmpty(strProteinOptions) Then
				If strProteinOptions.ToLower.Contains("seq_direction=decoy") Then
					blnFastaFileIsDecoy = True
				End If
			End If

			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Indexing Fasta file to create Suffix Array files")
			End If

			' Index the fasta file to create the Suffix Array files
			Dim intIteration As Integer = 1

			Do While intIteration <= 2

				result = objIndexedDBCreator.CreateSuffixArrayFiles(m_WorkDir, m_DebugLevel, m_JobNum, JavaProgLoc, mMSGFDbProgLoc, fiFastaFile.FullName, blnFastaFileIsDecoy)
				If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					Exit Do
				ElseIf result = IJobParams.CloseOutType.CLOSEOUT_FAILED OrElse (result <> IJobParams.CloseOutType.CLOSEOUT_FAILED And intIteration >= 2) Then

					' Error message has already been logged
					If Not String.IsNullOrEmpty(objIndexedDBCreator.ErrorMessage) Then
						MyBase.m_message = objIndexedDBCreator.ErrorMessage
					Else
						MyBase.m_message = "Error creating Suffix Array files"
					End If
					Return result
				End If

				intIteration += 1
			Loop

			strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

			If Not System.IO.File.Exists(strParameterFilePath) Then
				m_message = "Parameter file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			' Read the MSGFDB Parameter File
			strMSGFDbCmdLineOptions = ParseMSGFDBParameterFile(strParameterFilePath, FastaFileSizeKB, strAssumedScanType)
			If String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Problem parsing MSGFDB parameter file"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			ResultsFileName = m_Dataset & "_msgfdb.txt"

			If strMSGFDbCmdLineOptions.Contains("-tda 1") Then
				' Make sure the .Fasta file is not a Decoy fasta
				If blnFastaFileIsDecoy Then
					m_message = "Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSGFDB")

			' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
			' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
			' (job 611216 succeeded with a value of 5000)
			intJavaMemorySize = clsGlobal.GetJobParameter(m_jobParams, "MSGFDBJavaMemorySize", 2000)
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
			CmdStr &= " -d " & fiFastaFile.FullName

			' Append the remaining options loaded from the parameter file
			CmdStr &= " " & strMSGFDbCmdLineOptions

			' Make sure the machine has enough free memory to run MSGFDB
			Dim blnLogFreeMemoryOnSuccess As Boolean = True
			If m_DebugLevel < 1 Then blnLogFreeMemoryOnSuccess = False

			If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, "MSGFDB", blnLogFreeMemoryOnSuccess) Then
				m_message = "Not enough free memory to run MSGFDB"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			CmdRunner = New clsRunDosProgram(m_WorkDir)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT)
			End With

			m_progress = PROGRESS_PCT_MSGFDB_STARTING

			blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSGFDB", True)

			If Not mToolVersionWritten Then
				If String.IsNullOrWhiteSpace(mMSGFDbVersion) Then
					ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT))
				End If
				mToolVersionWritten = StoreToolVersionInfo()
			End If

			If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			End If


			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running MSGFDB"
				m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGFDB returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSGFDB failed (but exit code is 0)")
				End If

				blnProcessingError = True

			Else
				m_progress = PROGRESS_PCT_MSGFDB_COMPLETE
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB Search Complete")
				End If
			End If

			If Not blnProcessingError Then
				'Zip the output file
				result = ZipMSGFDBResults(ResultsFileName)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					blnProcessingError = True
				End If
			End If

			If Not blnProcessingError Then
				' Create the Peptide to Protein map file
				result = CreatePeptideToProteinMapping(ResultsFileName)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
					blnProcessingError = True
				End If
			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		   '2 second delay
			GC.Collect()
			GC.WaitForPendingFinalizers()

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

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

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

		Try
			System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
			System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
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
				strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Private Function CreatePeptideToProteinMapping(ByVal ResultsFileName As String) As IJobParams.CloseOutType

		Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")

		' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
		Dim dbFilename As String = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
		Dim strInputFilePath As String
		Dim strFastaFilePath As String

		Dim blnIgnorePeptideToProteinMapperErrors As Boolean
		Dim blnSuccess As Boolean

		UpdateStatusRunning(PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

		strInputFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)
		strFastaFilePath = System.IO.Path.Combine(OrgDbDir, dbFilename)

		Try
			' Validate that the input file has at least one entry; if not, then no point in continuing
			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

			intLinesRead = 0
			Do While srInFile.Peek >= 0 AndAlso intLinesRead < 10
				strLineIn = srInFile.ReadLine()
				If Not String.IsNullOrEmpty(strLineIn) Then
					intLinesRead += 1
				End If
			Loop

			srInFile.Close()

			If intLinesRead <= 1 Then
				' File is empty or only contains a header line
				m_message = "No results above threshold"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No results above threshold; MSGF-DB results file is empty")
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
			End If

		Catch ex As Exception

			m_message = "Error validating MSGF-DB results file contents in CreatePeptideToProteinMapping"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & _
			 m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		End Try

		If mResultsIncludeDecoyPeptides Then
			' Read the original fasta file to create a decoy fasta file
			strFastaFilePath = GenerateDecoyFastaFile(strFastaFilePath, m_WorkDir)

			If String.IsNullOrEmpty(strFastaFilePath) Then
				' Problem creating the decoy fasta file
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error creating a decoy version of the fasta file"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			clsGlobal.FilesToDelete.Add(System.IO.Path.GetFileName(strFastaFilePath))
		End If

		Try
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating peptide to protein map file")
			End If

			blnIgnorePeptideToProteinMapperErrors = AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("IgnorePeptideToProteinMapError"))

			mPeptideToProteinMapper = New PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

			With mPeptideToProteinMapper
				.DeleteTempFiles = True
				.IgnoreILDifferences = False
				.InspectParameterFilePath = String.Empty

				If m_DebugLevel > 2 Then
					.LogMessagesToFile = True
					.LogFolderPath = m_WorkDir
				Else
					.LogMessagesToFile = False
				End If

				.MatchPeptidePrefixAndSuffixToProtein = False
				.OutputProteinSequence = False
				.PeptideInputFileFormat = PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.MSGFDBResultsFile
				.PeptideFileSkipFirstLine = False
				.ProteinDataRemoveSymbolCharacters = True
				.ProteinInputFilePath = strFastaFilePath
				.SaveProteinToPeptideMappingFile = True
				.SearchAllProteinsForPeptideSequence = True
				.SearchAllProteinsSkipCoverageComputationSteps = True
				.ShowMessages = False
			End With

			blnSuccess = mPeptideToProteinMapper.ProcessFile(strInputFilePath, m_WorkDir, String.Empty, True)

			mPeptideToProteinMapper.CloseLogFileNow()

			Dim strResultsFilePath As String
			strResultsFilePath = System.IO.Path.GetFileNameWithoutExtension(strInputFilePath) & PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.FILENAME_SUFFIX_PEP_TO_PROTEIN_MAPPING
			strResultsFilePath = System.IO.Path.Combine(m_WorkDir, strResultsFilePath)

			If blnSuccess Then
				If Not System.IO.File.Exists(strResultsFilePath) Then
					m_message = "Peptide to protein mapping file was not created"
					blnSuccess = False
				Else
					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide to protein mapping complete")
					End If

					blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors)
				End If
			Else
				If mPeptideToProteinMapper.GetErrorMessage.Length = 0 AndAlso mPeptideToProteinMapper.StatusMessage.ToLower().Contains("error") Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running clsPeptideToProteinMapEngine: " & mPeptideToProteinMapper.StatusMessage)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running clsPeptideToProteinMapEngine: " & mPeptideToProteinMapper.GetErrorMessage())
					If mPeptideToProteinMapper.StatusMessage.Length > 0 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsPeptideToProteinMapEngine status: " & mPeptideToProteinMapper.StatusMessage)
					End If
				End If

				If blnIgnorePeptideToProteinMapperErrors Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")

					If System.IO.File.Exists(strResultsFilePath) Then
						blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors)
					Else
						blnSuccess = True
					End If

				Else
					m_message = "Error in CreatePeptideToProteinMapping"
					blnSuccess = False
				End If
			End If

		Catch ex As Exception

			m_message = "Exception in CreatePeptideToProteinMapping"

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreatePeptideToProteinMapping, Error running clsPeptideToProteinMapEngine, job " & _
			 m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))

			If blnIgnorePeptideToProteinMapperErrors Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End Try

		If blnSuccess Then
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

	End Function

	Protected Function CreateScanTypeFile() As Boolean

		Dim objScanTypeFileCreator As clsScanTypeFileCreator
		objScanTypeFileCreator = New clsScanTypeFileCreator(m_WorkDir, m_Dataset)

		If objScanTypeFileCreator.CreateScanTypeFile() Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Created ScanType file: " & System.IO.Path.GetFileName(objScanTypeFileCreator.ScanTypeFilePath))
			End If
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

	' Read the original fasta file to create a decoy fasta file
	''' <summary>
	''' Creates a decoy version of the fasta file specified by strInputFilePath
	''' This new file will include the original proteins plus reversed versions of the original proteins
	''' Protein names will be prepended with REV_
	''' </summary>
	''' <param name="strInputFilePath">Fasta file to process</param>
	''' <param name="strOutputDirectoryPath">Output folder to create decoy file in</param>
	''' <returns>Full path to the decoy fasta file</returns>
	''' <remarks></remarks>
	Protected Function GenerateDecoyFastaFile(ByVal strInputFilePath As String, ByVal strOutputDirectoryPath As String) As String

		Const PROTEIN_LINE_START_CHAR As Char = ">"c
		Const PROTEIN_LINE_ACCESSION_END_CHAR As Char = " "c

		Dim strDecoyFastaFilePath As String = String.Empty
		Dim ioSourceFile As System.IO.FileInfo

		Dim objFastaFileReader As ProteinFileReader.FastaFileReader
		Dim swProteinOutputFile As System.IO.StreamWriter

		Dim blnInputProteinFound As Boolean


		Try
			ioSourceFile = New System.IO.FileInfo(strInputFilePath)
			If Not ioSourceFile.Exists Then
				m_message = "Fasta file not found: " & ioSourceFile.FullName
				Return String.Empty
			End If

			strDecoyFastaFilePath = System.IO.Path.Combine(strOutputDirectoryPath, System.IO.Path.GetFileNameWithoutExtension(ioSourceFile.Name) & "_decoy.fasta")

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating decoy fasta file at " & strDecoyFastaFilePath)
			End If

			objFastaFileReader = New ProteinFileReader.FastaFileReader
			With objFastaFileReader
				.ProteinLineStartChar = PROTEIN_LINE_START_CHAR
				.ProteinLineAccessionEndChar = PROTEIN_LINE_ACCESSION_END_CHAR
			End With

			If Not objFastaFileReader.OpenFile(strInputFilePath) Then
				m_message = "Error reading fasta file with ProteinFileReader to create decoy file"
				Return String.Empty
			End If

			swProteinOutputFile = New System.IO.StreamWriter(New System.IO.FileStream(strDecoyFastaFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

			Do
				blnInputProteinFound = objFastaFileReader.ReadNextProteinEntry()

				If blnInputProteinFound Then
					' Write the forward protein
					swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR & objFastaFileReader.ProteinName & " " & objFastaFileReader.ProteinDescription)
					WriteProteinSequence(swProteinOutputFile, objFastaFileReader.ProteinSequence)

					' Write the decoy protein
					swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR & "REV_" & objFastaFileReader.ProteinName & " " & objFastaFileReader.ProteinDescription)
					WriteProteinSequence(swProteinOutputFile, ReverseString(objFastaFileReader.ProteinSequence))
				End If

			Loop While blnInputProteinFound

			swProteinOutputFile.Close()
			objFastaFileReader.CloseFile()

		Catch ex As Exception
			m_message = "Exception creating decoy fasta file"

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "GenerateDecoyFastaFile, " & m_message, ex)
			Return String.Empty
		End Try

		Return strDecoyFastaFilePath

	End Function

	Protected Function GetMSFGDBParameterNames() As System.Collections.Generic.Dictionary(Of String, String)
		Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)
		dctParamNames = New System.Collections.Generic.Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

		dctParamNames.Add("PMTolerance", "t")
		dctParamNames.Add(MSGFDB_OPTION_TDA, "tda")
		dctParamNames.Add(MSGFDB_OPTION_SHOWDECOY, "showDecoy")
		dctParamNames.Add("FragmentationMethodID", "m")
		dctParamNames.Add("InstrumentID", "inst")
		dctParamNames.Add("EnzymeID", "e")
		dctParamNames.Add("C13", "c13")
		dctParamNames.Add("NNET", "nnet")
		dctParamNames.Add("minLength", "minLength")
		dctParamNames.Add("maxLength", "maxLength")
		dctParamNames.Add("minCharge", "minCharge")
		dctParamNames.Add("maxCharge", "maxCharge")
		dctParamNames.Add("NumMatchesPerSpec", "n")

		' The following are special cases; 
		' do not add to dctParamNames
		'   uniformAAProb
		'   NumThreads
		'   NumMods
		'   StaticMod
		'   DynamicMod

		Return dctParamNames
	End Function

	''' <summary>
	''' Compare to strings (not case sensitive)
	''' </summary>
	''' <param name="strText1"></param>
	''' <param name="strText2"></param>
	''' <returns>True if they match; false if not</returns>
	''' <remarks></remarks>
	Protected Function IsMatch(ByVal strText1 As String, ByVal strText2 As String) As Boolean
		If String.Compare(strText1, strText2, True) = 0 Then
			Return True
		Else
			Return False
		End If
	End Function



	''' <summary>
	''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output:
		'
		' MS-GFDB v6299 (08/22/2011)
		' Loading database files...
		' Loading database finished (elapsed time: 0.23 sec)
		' Reading spectra...
		' Read spectra finished (elapsed time: 9.19 sec)
		' Using 4 threads.
		' Spectrum 0-12074 (total: 12075)
		' pool-1-thread-2: Preprocessing spectra...
		' pool-1-thread-1: Preprocessing spectra...
		' pool-1-thread-1: Preprocessing spectra finished (elapsed time: 33.00 sec)
		' pool-1-thread-1: Database search...
		' pool-1-thread-1: Database search progress... 0.0% complete
		' pool-1-thread-2: Preprocessing spectra finished (elapsed time: 35.00 sec)
		' pool-1-thread-2: Database search...
		' pool-1-thread-2: Database search progress... 0.0% complete
		' pool-1-thread-1: Database search finished (elapsed time: 36.00 sec)
		' pool-1-thread-1: Computing spectral probabilities...
		' pool-1-thread-2: Database search finished (elapsed time: 44.00 sec)
		' pool-1-thread-2: Computing spectral probabilities...
		' pool-1-thread-1: Computing spectral probabilities... 33.1% complete
		' pool-1-thread-2: Computing spectral probabilities... 33.1% complete
		' pool-1-thread-1: Computing spectral probabilities... 66.2% complete
		' Computing FDRs...
		' Computing EFDRs finished(elapsed time: 0.78 sec)
		' MS-GFDB complete (total elapsed time: 699.69 sec)

		Static reExtractThreadCount As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("Using (\d+) thread", _
																										  Text.RegularExpressions.RegexOptions.Compiled Or _
																										  Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow

		Dim eThreadProgressBase() As eThreadProgressSteps
		Dim sngThreadProgressAddon() As Single

		Try
			' Initially reserve space for 32 threads
			' We'll expand these arrays later if needed
			ReDim eThreadProgressBase(32)
			ReDim sngThreadProgressAddon(32)

			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If


			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer

			Dim oMatch As System.Text.RegularExpressions.Match
			Dim intThreadCount As Short = 0

			Dim sngEffectiveProgress As Single
			sngEffectiveProgress = PROGRESS_PCT_MSGFDB_STARTING

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					If intLinesRead = 1 Then
						' The first line is the MSGFDB version
						If strLineIn.ToLower.Contains("gfdb") Then
							If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSGFDbVersion) Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB version: " & strLineIn)
							End If

							mMSGFDbVersion = String.Copy(strLineIn)
						Else
							If strLineIn.ToLower.Contains("error") Then
								If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
									mConsoleOutputErrorMsg = "Error running MSGFDB:"
								End If
								mConsoleOutputErrorMsg &= "; " & strLineIn
							End If
						End If
					End If

					' Update progress if the line starts with one of the expected phrases
					If strLineIn.StartsWith("Loading database files") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_LOADING_DATABASE Then
							sngEffectiveProgress = PROGRESS_PCT_MSGFDB_LOADING_DATABASE
						End If

					ElseIf strLineIn.StartsWith("Reading spectra") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_READING_SPECTRA Then
							sngEffectiveProgress = PROGRESS_PCT_MSGFDB_READING_SPECTRA
						End If
					ElseIf strLineIn.StartsWith("Using") Then

						' Extract out the thread count
						oMatch = reExtractThreadCount.Match(strLineIn)

						If oMatch.Success Then
							Short.TryParse(oMatch.Groups(1).Value, intThreadCount)
						End If

						' Now that we know the thread count, initialize the array that will keep track of the progress % complete for each thread
						If eThreadProgressBase.Length < intThreadCount Then
							ReDim eThreadProgressBase(intThreadCount)
							ReDim sngThreadProgressAddon(intThreadCount)
						End If

						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_THREADS_SPAWNED Then
							sngEffectiveProgress = PROGRESS_PCT_MSGFDB_THREADS_SPAWNED
						End If

					ElseIf strLineIn.StartsWith("Computing EFDRs") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
							sngEffectiveProgress = PROGRESS_PCT_MSGFDB_COMPUTING_FDRS
						End If

					ElseIf strLineIn.StartsWith("MS-GFDB complete") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPLETE Then
							sngEffectiveProgress = PROGRESS_PCT_MSGFDB_COMPLETE
						End If

					ElseIf strLineIn.Contains("Preprocessing spectra") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
							ParseConsoleOutputThreadMessage(strLineIn, eThreadProgressSteps.PreprocessingSpectra, eThreadProgressBase, sngThreadProgressAddon)
						End If

					ElseIf strLineIn.Contains("Database search") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
							ParseConsoleOutputThreadMessage(strLineIn, eThreadProgressSteps.DatabaseSearch, eThreadProgressBase, sngThreadProgressAddon)
						End If

					ElseIf strLineIn.Contains("Computing spectral probabilities finished") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
							ParseConsoleOutputThreadMessage(strLineIn, eThreadProgressSteps.Complete, eThreadProgressBase, sngThreadProgressAddon)
						End If

					ElseIf strLineIn.Contains("Computing spectral probabilities") Then
						If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
							ParseConsoleOutputThreadMessage(strLineIn, eThreadProgressSteps.ComputingSpectralProbabilities, eThreadProgressBase, sngThreadProgressAddon)
						End If

					ElseIf Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
						If strLineIn.ToLower.Contains("error") Then
							mConsoleOutputErrorMsg &= "; " & strLineIn
						End If
					End If
				End If
			Loop

			srInFile.Close()

			If sngEffectiveProgress >= PROGRESS_PCT_MSGFDB_THREADS_SPAWNED AndAlso sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then

				' Increment sngEffectiveProgress based on the data in sngThreadProgressBase() and sngThreadProgressAddon()
				Dim sngProgressAddonAllThreads As Single
				Dim sngProgressOneThread As Single

				sngProgressAddonAllThreads = 0

				For intThread As Integer = 1 To intThreadCount
					sngProgressOneThread = 0

					Select Case eThreadProgressBase(intThread)
						Case eThreadProgressSteps.PreprocessingSpectra
							sngProgressOneThread = THREAD_PROGRESS_PCT_PREPROCESSING_SPECTRA
							sngProgressOneThread += sngThreadProgressAddon(intThread) * (THREAD_PROGRESS_PCT_DATABASE_SEARCH - THREAD_PROGRESS_PCT_PREPROCESSING_SPECTRA) / 100.0!

						Case eThreadProgressSteps.DatabaseSearch
							sngProgressOneThread = THREAD_PROGRESS_PCT_DATABASE_SEARCH
							sngProgressOneThread += sngThreadProgressAddon(intThread) * (THREAD_PROGRESS_PCT_COMPUTING_SPECTRAL_PROBABILITIES - THREAD_PROGRESS_PCT_DATABASE_SEARCH) / 100.0!

						Case eThreadProgressSteps.ComputingSpectralProbabilities
							sngProgressOneThread = THREAD_PROGRESS_PCT_COMPUTING_SPECTRAL_PROBABILITIES
							sngProgressOneThread += sngThreadProgressAddon(intThread) * (THREAD_PROGRESS_PCT_COMPLETE - THREAD_PROGRESS_PCT_COMPUTING_SPECTRAL_PROBABILITIES) / 100.0!

						Case eThreadProgressSteps.Complete
							sngProgressOneThread = THREAD_PROGRESS_PCT_COMPLETE

						Case Else
							' Unrecognized step
					End Select

					sngProgressAddonAllThreads += sngProgressOneThread / intThreadCount
				Next

				sngEffectiveProgress += sngProgressAddonAllThreads * (PROGRESS_PCT_MSGFDB_COMPUTING_FDRS - PROGRESS_PCT_MSGFDB_THREADS_SPAWNED) / 100.0!
			End If

			If m_progress < sngEffectiveProgress Then
				m_progress = sngEffectiveProgress

				If m_DebugLevel >= 3 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = System.DateTime.UtcNow
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Sub ParseConsoleOutputThreadMessage(ByVal strLineIn As String, _
												  ByVal eThreadProgressStep As eThreadProgressSteps, _
												  ByRef eThreadProgressBase() As eThreadProgressSteps, _
												  ByRef sngThreadProgressAddon() As Single)

		Dim oMatch As System.Text.RegularExpressions.Match

		Static reExtractThreadNum As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("thread-(\d+)", _
																										  Text.RegularExpressions.RegexOptions.Compiled Or _
																										  Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static reExtractPctComplete As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("([0-9.]+)% complete", _
																										  Text.RegularExpressions.RegexOptions.Compiled Or _
																										  Text.RegularExpressions.RegexOptions.IgnoreCase)

		' Extract out the thread number
		' Line should look like one of these lines:
		'   pool-1-thread-2: Database search...
		'   pool-1-thread-2: Database search progress... 0.0% complete
		'   pool-1-thread-3: Preprocessing spectra finished (elapsed time: 40.00 sec)
		'   pool-1-thread-1: Computing spectral probabilities...
		'   pool-1-thread-1: Computing spectral probabilities... 66.2% complete
		'   pool-1-thread-1: Computing spectral probabilities finished (elapsed time: 138.00 sec)

		oMatch = reExtractThreadNum.Match(strLineIn)

		If oMatch.Success Then
			Dim intThread As Short
			If Short.TryParse(oMatch.Groups(1).Value, intThread) Then

				If eThreadProgressBase Is Nothing OrElse intThread > eThreadProgressBase.Length Then
					' Array not initialized properly; can't update it
				Else
					If eThreadProgressBase(intThread) < eThreadProgressStep Then
						eThreadProgressBase(intThread) = eThreadProgressStep
					End If

					' Parse out the % complete (if present)
					oMatch = reExtractPctComplete.Match(strLineIn)
					If oMatch.Success Then
						Dim sngProgressPctInLogFile As Single = 0
						If Single.TryParse(oMatch.Groups(1).Value, sngProgressPctInLogFile) Then
							If sngThreadProgressAddon(intThread) < sngProgressPctInLogFile Then
								sngThreadProgressAddon(intThread) = sngProgressPctInLogFile
							End If
						End If
					End If

				End If

			End If
		End If
	End Sub


	''' <summary>
	''' Parses the static and dynamic modification information to create the MSGFDB Mods file
	''' </summary>
	''' <param name="strParameterFilePath">Full path to the MSGF parameter file; will create file MSGFDB_Mods.txt in the same folder</param>
	''' <param name="sbOptions">String builder of command line arguments to pass to MSGFDB</param>
	''' <param name="intNumMods">Max Number of Modifications per peptide</param>
	''' <param name="lstStaticMods">List of Static Mods</param>
	''' <param name="lstDynamicMods">List of Dynamic Mods</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function ParseMSGFDBModifications(ByVal strParameterFilePath As String, _
												ByRef sbOptions As System.Text.StringBuilder, _
												ByVal intNumMods As Integer, _
												ByRef lstStaticMods As System.Collections.Generic.List(Of String), _
												ByRef lstDynamicMods As System.Collections.Generic.List(Of String)) As Boolean

		Const MOD_FILE_NAME As String = "MSGFDB_Mods.txt"
		Dim blnSuccess As Boolean
		Dim strModFilePath As String
		Dim swModFile As System.IO.StreamWriter = Nothing

		Try
			Dim fiParameterFile As System.IO.FileInfo
			fiParameterFile = New System.IO.FileInfo(strParameterFilePath)

			strModFilePath = System.IO.Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME)

			sbOptions.Append(" -mod " & MOD_FILE_NAME)

			swModFile = New System.IO.StreamWriter(New System.IO.FileStream(strModFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

			swModFile.WriteLine("# This file is used to specify modifications for MSGFDB")
			swModFile.WriteLine("")
			swModFile.WriteLine("# Max Number of Modifications per peptide")
			swModFile.WriteLine("# If this value is large, the search will be slow")
			swModFile.WriteLine("NumMods=" & intNumMods)

			swModFile.WriteLine("")
			swModFile.WriteLine("# Static mods")
			If lstStaticMods.Count = 0 Then
				swModFile.WriteLine("# None")
			Else
				For Each strStaticMod As String In lstStaticMods
					Dim strModClean As String = String.Empty

					If ParseMSGFDbValidateMod(strStaticMod, strModClean) Then
						If strModClean.Contains(",opt,") Then
							' Static (fixed) mod is listed as dynamic
							' Abort the analysis since the parameter file is misleading and needs to be fixed							
							m_message = "Static mod definition contains ',opt,'; update the param file to have ',fix,' or change to 'DynamicMod='"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; " & strStaticMod)
							Return False
						End If
						swModFile.WriteLine(strModClean)
					Else
						Return False
					End If
				Next
			End If

			swModFile.WriteLine("")
			swModFile.WriteLine("# Dynamic mods")
			If lstDynamicMods.Count = 0 Then
				swModFile.WriteLine("# None")
			Else
				For Each strDynamicMod As String In lstDynamicMods
					Dim strModClean As String = String.Empty

					If ParseMSGFDbValidateMod(strDynamicMod, strModClean) Then
						If strModClean.Contains(",fix,") Then
							' Dynamic (optional) mod is listed as static
							' Abort the analysis since the parameter file is misleading and needs to be fixed							
							m_message = "Dynamic mod definition contains ',fix,'; update the param file to have ',opt,' or change to 'StaticMod='"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; " & strDynamicMod)
							Return False
						End If
						swModFile.WriteLine(strModClean)
					Else
						Return False
					End If
				Next
			End If

			blnSuccess = True

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MSGFDB Mods file: " & ex.Message)
			blnSuccess = False
		Finally
			If Not swModFile Is Nothing Then
				swModFile.Close()
			End If
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Read the MSGFDB options file and convert the options to command line switches
	''' </summary>
	''' <param name="strParameterFilePath">Path to the MSGFDB Parameter File</param>
	''' <param name="FastaFileSizeKB">Size of the .Fasta file, in KB</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function ParseMSGFDBParameterFile(ByVal strParameterFilePath As String, ByVal FastaFileSizeKB As Single, ByVal strAssumedScanType As String) As String
		Const DEFINE_MAX_THREADS As Boolean = True
		Const SMALL_FASTA_FILE_THRESHOLD_KB As Integer = 20

		Dim sbOptions As System.Text.StringBuilder
		Dim srParamFile As System.IO.StreamReader
		Dim strLineIn As String

		Dim strKey As String
		Dim strValue As String
		Dim intValue As Integer

		Dim intParamFileThreadCount As Integer = 0
		Dim intDMSDefinedThreadCount As Integer = 0

		Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)

		Dim intNumMods As Integer = 0
		Dim lstStaticMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
		Dim lstDynamicMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)

		Dim blnShowDecoyParamPresent As Boolean = False
		Dim blnShowDecoy As Boolean = False
		Dim blnTDA As Boolean = False

		sbOptions = New System.Text.StringBuilder(500)

		' This is set to True if the parameter file contains both TDA=1 and showDecoy=1
		mResultsIncludeDecoyPeptides = False

		Try

			' Initialize the Param Name dictionary
			dctParamNames = GetMSFGDBParameterNames()

			srParamFile = New System.IO.StreamReader(New System.IO.FileStream(strParameterFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

			Do While srParamFile.Peek >= 0
				strLineIn = srParamFile.ReadLine()
				strKey = String.Empty
				strValue = String.Empty

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					strLineIn = strLineIn.Trim()

					If Not strLineIn.StartsWith("#") AndAlso strLineIn.Contains("="c) Then

						Dim intCharIndex As Integer
						intCharIndex = strLineIn.IndexOf("=")
						If intCharIndex > 0 Then
							strKey = strLineIn.Substring(0, intCharIndex).Trim()
							If intCharIndex < strLineIn.Length - 1 Then
								strValue = strLineIn.Substring(intCharIndex + 1).Trim()
							Else
								strValue = String.Empty
							End If
						End If
					End If

				End If

				If Not String.IsNullOrWhiteSpace(strKey) Then

					Dim strArgumentSwitch As String = String.Empty

					' Check whether strKey is one of the standard keys defined in dctParamNames
					If dctParamNames.TryGetValue(strKey, strArgumentSwitch) Then

						If Not String.IsNullOrWhiteSpace(strAssumedScanType) AndAlso IsMatch(strKey, MSGFDB_OPTION_FRAGMENTATION_METHOD) Then
							' Override FragmentationMethodID using strAssumedScanType
							Select Case strAssumedScanType.ToUpper()
								Case "CID"
									strValue = "1"
								Case "ETD"
									strValue = "2"
								Case "HCD"
									strValue = "3"
								Case Else
									' Invalid string
									m_message = "Invalid assumed scan type '" & strAssumedScanType & "'; must be CID, ETD, or HCD"
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
									Return String.Empty
							End Select
						End If

						sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)

						If IsMatch(strKey, MSGFDB_OPTION_SHOWDECOY) Then
							blnShowDecoyParamPresent = True
							If Integer.TryParse(strValue, intValue) Then
								If intValue > 0 Then
									blnShowDecoy = True
								End If
							End If
						ElseIf IsMatch(strKey, MSGFDB_OPTION_TDA) Then
							If Integer.TryParse(strValue, intValue) Then
								If intValue > 0 Then
									blnTDA = True
								End If
							End If
						End If

					ElseIf IsMatch(strKey, "uniformAAProb") Then
						If String.IsNullOrWhiteSpace(strValue) OrElse IsMatch(strValue, "auto") Then
							If FastaFileSizeKB < SMALL_FASTA_FILE_THRESHOLD_KB Then
								sbOptions.Append(" -uniformAAProb 1")
							Else
								sbOptions.Append(" -uniformAAProb 0")
							End If
						Else
							If Integer.TryParse(strValue, intValue) Then
								sbOptions.Append(" -uniformAAProb " & intValue)
							Else
								m_message = "Invalid value for uniformAAProb in MSGFDB parameter file"
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message & ": " & strLineIn)
								srParamFile.Close()
								Return String.Empty
							End If
						End If

					ElseIf DEFINE_MAX_THREADS AndAlso IsMatch(strKey, "NumThreads") Then
						If String.IsNullOrWhiteSpace(strValue) OrElse IsMatch(strValue, "all") Then
							' Do not append -thread to the command line; MSGFDB will use all available cores by default
						Else
							If Integer.TryParse(strValue, intParamFileThreadCount) Then
								' intParamFileThreadCount now has the thread count
							Else
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid value for NumThreads in MSGFDB parameter file: " & strLineIn)
							End If
						End If


					ElseIf IsMatch(strKey, "NumMods") Then
						If Integer.TryParse(strValue, intValue) Then
							intNumMods = intValue
						Else
							m_message = "Invalid value for NumMods in MSGFDB parameter file"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message & ": " & strLineIn)
							srParamFile.Close()
							Return String.Empty
						End If

					ElseIf IsMatch(strKey, "StaticMod") Then
						If Not IsMatch(strValue, "none") Then
							lstStaticMods.Add(strValue)
						End If

					ElseIf IsMatch(strKey, "DynamicMod") Then
						If Not IsMatch(strValue, "none") Then
							lstDynamicMods.Add(strValue)
						End If
					End If

				End If
			Loop

			srParamFile.Close()

			If blnShowDecoy And blnTDA Then
				' Parameter file contains both TDA=1 and showDecoy=1
				mResultsIncludeDecoyPeptides = True
			End If

			If Not blnShowDecoyParamPresent Then
				' Add showDecoy to sbOptions
				sbOptions.Append(" -showDecoy 0")
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception reading MSGFDB parameter file: " & ex.Message)
			Return String.Empty
		End Try

		' Define the thread count
		intDMSDefinedThreadCount = clsGlobal.GetJobParameter(m_jobParams, "MSGFDBThreads", 0)

		If intDMSDefinedThreadCount > 0 Then
			intParamFileThreadCount = intDMSDefinedThreadCount
		End If

		If intParamFileThreadCount > 0 Then
			sbOptions.Append(" -thread " & intParamFileThreadCount)
		End If

		' Create the modification file and append the -mod switch
		If ParseMSGFDBModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods) Then
			Return sbOptions.ToString()
		Else
			Return String.Empty
		End If


	End Function

	''' <summary>
	''' Validates that the modification definition text
	''' </summary>
	''' <param name="strMod">Modification definition</param>
	''' <param name="strModClean">Cleaned-up modification definition (output param)</param>
	''' <returns>True if valid; false if invalid</returns>
	''' <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
	Protected Function ParseMSGFDbValidateMod(ByVal strMod As String, ByRef strModClean As String) As Boolean

		Dim intPoundIndex As Integer
		Dim strSplitMod() As String

		Dim strComment As String = String.Empty

		strModClean = String.Empty

		intPoundIndex = strMod.IndexOf("#"c)
		If intPoundIndex > 0 Then
			strComment = strMod.Substring(intPoundIndex)
			strMod = strMod.Substring(0, intPoundIndex - 1).Trim
		End If

		strSplitMod = strMod.Split(","c)

		If strSplitMod.Length < 5 Then
			' Invalid mod definition; must have 5 sections
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid modification string; must have 5 sections: " & strMod)
			Return False
		End If

		' Reconstruct the mod definition, making sure there is no whitespace
		strModClean = strSplitMod(0).Trim()
		For intIndex As Integer = 1 To strSplitMod.Length - 1
			strModClean &= "," & strSplitMod(intIndex).Trim()
		Next

		If Not String.IsNullOrWhiteSpace(strComment) Then
			' As of August 12, 2011, the comment cannot contain a comma
			' Sangtae Kim has promised to fix this, but for now, we'll replace commas with semicolons
			strComment = strComment.Replace(",", ";")
			strModClean &= "     " & strComment
		End If

		Return True

	End Function

	''' <summary>
	''' 
	''' </summary>
	''' <param name="sbOptions"></param>
	''' <param name="strKeyName"></param>
	''' <param name="strValue"></param>
	''' <param name="strParameterName"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ParseMSFDBParamLine(ByRef sbOptions As System.Text.StringBuilder, _
										   ByVal strKeyName As String, _
										   ByVal strValue As String, _
										   ByVal strParameterName As String) As Boolean

		Dim strCommandLineSwitchName As String = strParameterName

		Return ParseMSFDBParamLine(sbOptions, strKeyName, strValue, strParameterName, strCommandLineSwitchName)

	End Function

	Protected Function ParseMSFDBParamLine(ByRef sbOptions As System.Text.StringBuilder, _
										   ByVal strKeyName As String, _
										   ByVal strValue As String, _
										   ByVal strParameterName As String, _
										   ByVal strCommandLineSwitchName As String) As Boolean

		If IsMatch(strKeyName, strParameterName) Then
			sbOptions.Append(" -" & strCommandLineSwitchName & " " & strValue)
			Return True
		Else
			Return False
		End If


	End Function

	Private Function ReverseString(ByVal strText As String) As String

		Dim chReversed() As Char = strText.ToCharArray()
		Array.Reverse(chReversed)
		Return New String(chReversed)

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSGFDbVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(mMSGFDbProgLoc))

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

	''' <summary>
	''' Make sure the _DTA.txt file exists and has at least one spectrum in it
	''' </summary>
	''' <returns>True if success; false if failure</returns>
	''' <remarks></remarks>
	Protected Function ValidateCDTAFile() As Boolean
		Dim strInputFilePath As String
		Dim srReader As System.IO.StreamReader

		Dim blnDataFound As Boolean = False

		Try
			strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

			If Not System.IO.File.Exists(strInputFilePath) Then
				m_message = "_DTA.txt file not found: " & strInputFilePath
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			srReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

			Do While srReader.Peek >= 0
				If srReader.ReadLine.Trim.Length > 0 Then
					blnDataFound = True
					Exit Do
				End If
			Loop

			srReader.Close()

			If Not blnDataFound Then
				m_message = "The _DTA.txt file is empty"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

		Catch ex As Exception
			m_message = "Exception in ValidateCDTAFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return blnDataFound

	End Function

	Private Function ValidatePeptideToProteinMapResults(ByVal strPeptideToProteinMapFilePath As String, ByVal blnIgnorePeptideToProteinMapperErrors As Boolean) As Boolean

		Const PROTEIN_NAME_NO_MATCH As String = "__NoMatch__"

		Dim blnSuccess As Boolean = False

		Dim intPeptideCount As Integer = 0
		Dim intPeptideCountNoMatch As Integer = 0
		Dim intLinesRead As Integer = 0

		Try
			' Validate that none of the results in strPeptideToProteinMapFilePath has protein name PROTEIN_NAME_NO_MATCH

			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Validating peptide to protein mapping, file " & System.IO.Path.GetFileName(strPeptideToProteinMapFilePath))
			End If

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strPeptideToProteinMapFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

			Do While srInFile.Peek > -1
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If intLinesRead > 1 AndAlso Not String.IsNullOrEmpty(strLineIn) Then
					intPeptideCount += 1
					If strLineIn.Contains(PROTEIN_NAME_NO_MATCH) Then
						intPeptideCountNoMatch += 1
					End If
				End If
			Loop

			srInFile.Close()

			If intPeptideCount = 0 Then
				m_message = "Peptide to protein mapping file is empty"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message & ", file " & System.IO.Path.GetFileName(strPeptideToProteinMapFilePath))
				blnSuccess = False

			ElseIf intPeptideCountNoMatch = 0 Then
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide to protein mapping validation complete; processed " & intPeptideCount & " peptides")
				End If

				blnSuccess = True

			Else
				Dim dblErrorPercent As Double	' Value between 0 and 100
				dblErrorPercent = intPeptideCountNoMatch / intPeptideCount * 100.0

				Dim strErrorMsg As String
				strErrorMsg = dblErrorPercent.ToString("0.0") & "% of the entries in the peptide to protein map file did not match to a protein in the FASTA file"

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMsg)

				If blnIgnorePeptideToProteinMapperErrors Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
					blnSuccess = True

				Else
					m_message = strErrorMsg
					blnSuccess = False
				End If
			End If

		Catch ex As Exception

			m_message = "Error validating peptide to protein map file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & _
			 m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))
			blnSuccess = False

		End Try

		Return blnSuccess

	End Function

	Private Sub WriteProteinSequence(ByRef swOutFile As System.IO.StreamWriter, ByVal strSequence As String)
		Dim intIndex As Integer = 0
		Dim intLength As Integer

		Do While intIndex < strSequence.Length
			intLength = Math.Min(60, strSequence.Length - intIndex)
			swOutFile.WriteLine(strSequence.Substring(intIndex, intLength))
			intIndex += 60
		Loop

	End Sub
	''' <summary>
	''' Zips MSGFDB Output File
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function ZipMSGFDBResults(ResultsFileName As String) As IJobParams.CloseOutType
		Dim TmpFilePath As String

		Try

			TmpFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)
			If Not MyBase.ZipFile(TmpFilePath, False) Then
				Dim Msg As String = "Error zipping output files, job " & m_JobNum
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Add the _msgfdb.txt file to .FilesToDelete since we only want to keep the Zipped version
			clsGlobal.FilesToDelete.Add(ResultsFileName)

		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerMSGFDB.ZipMSGFDBResults, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

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

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT))
			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDbVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub

	Private Sub mPeptideToProteinMapper_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mPeptideToProteinMapper.ProgressChanged

		' Note that percentComplete is a value between 0 and 100

		Const STATUS_UPDATE_INTERVAL_SECONDS As Integer = 5
		Const MAPPER_PROGRESS_LOG_INTERVAL_SECONDS As Integer = 120

		Static dtLastStatusUpdate As System.DateTime
		Static dtLastLogTime As System.DateTime

		Dim sngStartPercent As Single = PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS
		Dim sngEndPercent As Single = PROGRESS_PCT_COMPLETE
		Dim sngPercentCompleteEffective As Single

		sngPercentCompleteEffective = sngStartPercent + CSng(percentComplete / 100.0 * (sngEndPercent - sngStartPercent))

		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= STATUS_UPDATE_INTERVAL_SECONDS Then
			dtLastStatusUpdate = System.DateTime.UtcNow

			' Synchronize the stored Debug level with the value stored in the database
			Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
			AnalysisManagerBase.clsAnalysisToolRunnerBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS, m_mgrParams, m_DebugLevel)

			UpdateStatusRunning(sngPercentCompleteEffective)
		End If

		If m_DebugLevel >= 3 Then
			If System.DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS Then
				dtLastLogTime = System.DateTime.UtcNow
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Mapping peptides to proteins: " & percentComplete.ToString("0.0") & "% complete")
			End If
		End If

	End Sub

#End Region

End Class
