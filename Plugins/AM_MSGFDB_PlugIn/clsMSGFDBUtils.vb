Option Strict On
Imports AnalysisManagerBase

Public Class clsMSGFDBUtils

#Region "Constants"
	Public Const PROGRESS_PCT_MSGFDB_STARTING As Single = 1
	Public Const PROGRESS_PCT_MSGFDB_LOADING_DATABASE As Single = 2
	Public Const PROGRESS_PCT_MSGFDB_READING_SPECTRA As Single = 3
	Public Const PROGRESS_PCT_MSGFDB_THREADS_SPAWNED As Single = 4
	Public Const PROGRESS_PCT_MSGFDB_COMPUTING_FDRS As Single = 95
	Public Const PROGRESS_PCT_MSGFDB_COMPLETE As Single = 96
	Public Const PROGRESS_PCT_MSGFDB_CONVERT_MZID_TO_TSV As Single = 97
	Public Const PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS As Single = 98
	Public Const PROGRESS_PCT_COMPLETE As Single = 99

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

	Protected Const MSGFDB_OPTION_TDA As String = "TDA"
	Protected Const MSGFDB_OPTION_SHOWDECOY As String = "showDecoy"
	Protected Const MSGFDB_OPTION_FRAGMENTATION_METHOD As String = "FragmentationMethodID"
	Protected Const MSGFDB_OPTION_INSTRUMENT_ID As String = "InstrumentID"

	Public Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"
	Public Const MSGFPLUS_JAR_NAME As String = "MSGFPlus.jar"
	Public Const MSGFDB_CONSOLE_OUTPUT_FILE As String = "MSGFDB_ConsoleOutput.txt"

#End Region

#Region "Events"
	Public Event ErrorEvent(Message As String, DetailedMessage As String)
	Public Event IgnorePreviousErrorEvent()
	Public Event MessageEvent(Message As String)
	Public Event WarningEvent(Message As String)
#End Region

#Region "Module Variables"
	Protected m_mgrParams As AnalysisManagerBase.IMgrParams
	Protected m_jobParams As AnalysisManagerBase.IJobParams

	Protected m_WorkDir As String
	Protected m_JobNum As String
	Protected m_DebugLevel As Short

	Protected mMSGFPlus As Boolean
	Protected mMSGFDbVersion As String = String.Empty
	Protected mErrorMessage As String = String.Empty
	Protected mConsoleOutputErrorMsg As String = String.Empty

	Protected mPhosphorylationSearch As Boolean
	Protected mResultsIncludeAutoAddedDecoyPeptides As Boolean

	Protected WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine
#End Region

	Public ReadOnly Property ConsoleOutputErrorMsg As String
		Get
			Return mConsoleOutputErrorMsg
		End Get
	End Property

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public ReadOnly Property MSGFDbVersion As String
		Get
			Return mMSGFDbVersion
		End Get
	End Property

	Public ReadOnly Property PhosphorylationSearch As Boolean
		Get
			Return mPhosphorylationSearch
		End Get
	End Property

	Public ReadOnly Property ResultsIncludeAutoAddedDecoyPeptides As Boolean
		Get
			Return mResultsIncludeAutoAddedDecoyPeptides
		End Get
	End Property

#Region "Methods"
	Public Sub New(oMgrParams As AnalysisManagerBase.IMgrParams, oJobParams As AnalysisManagerBase.IJobParams, ByVal JobNum As String, ByVal strWorkDir As String, ByVal intDebugLevel As Short, ByVal blnMSGFPlus As Boolean)
		m_mgrParams = oMgrParams
		m_jobParams = oJobParams
		m_WorkDir = strWorkDir

		m_JobNum = JobNum
		m_DebugLevel = intDebugLevel

		mMSGFPlus = blnMSGFPlus
		mMSGFDbVersion = String.Empty
		mConsoleOutputErrorMsg = String.Empty

	End Sub

	Protected Sub AdjustSwitchesForMSGFPlus(ByVal blnMSGFPlus As Boolean, ByRef strArgumentSwitch As String, ByRef strValue As String)

		Dim intValue As Integer
		Dim intCharIndex As Integer

		If blnMSGFPlus Then
			' MSGF+

			If IsMatch(strArgumentSwitch, "nnet") Then
				' Auto-switch to ntt
				strArgumentSwitch = "ntt"
				If Integer.TryParse(strValue, intValue) Then
					Select Case intValue
						Case 0 : strValue = "2"			' Fully-tryptic
						Case 1 : strValue = "1"			' Partially tryptic
						Case 2 : strValue = "0"			' No-enzyme search
						Case Else
							' Assume partially tryptic
							strValue = "1"
					End Select
				End If

			ElseIf IsMatch(strArgumentSwitch, "c13") Then
				' Auto-switch to ti
				strArgumentSwitch = "ti"
				If Integer.TryParse(strValue, intValue) Then
					If intValue = 0 Then
						strValue = "0,0"
					ElseIf intValue = 1 Then
						strValue = "-1,1"
					ElseIf intValue = 2 Then
						strValue = "-1,2"
					Else
						strValue = "0,1"
					End If
				Else
					strValue = "0,1"
				End If

			ElseIf IsMatch(strArgumentSwitch, "showDecoy") Then
				' Not valid for MSGF+; skip it
				strArgumentSwitch = String.Empty
			End If

		Else
			' MS-GFDB

			If IsMatch(strArgumentSwitch, "ntt") Then
				' Auto-switch to nnet
				strArgumentSwitch = "nnet"
				If Integer.TryParse(strValue, intValue) Then
					Select Case intValue
						Case 2 : strValue = "0"			' Fully-tryptic
						Case 1 : strValue = "1"			' Partially tryptic
						Case 0 : strValue = "2"			' No-enzyme search
						Case Else
							' Assume partially tryptic
							strValue = "1"
					End Select
				End If

			ElseIf IsMatch(strArgumentSwitch, "ti") Then
				' Auto-switch to c13
				' Use the digit after the comma in the "ti" specification
				strArgumentSwitch = "c13"
				intCharIndex = strValue.IndexOf(",")
				If intCharIndex >= 0 Then
					strValue = strValue.Substring(intCharIndex + 1)
				Else
					' Comma not found
					If Integer.TryParse(strValue, intValue) Then
						strValue = intValue.ToString()
					Else
						strValue = "1"
					End If

				End If

			ElseIf IsMatch(strArgumentSwitch, "addFeatures") Then
				' Not valid for MS-GFDB; skip it
				strArgumentSwitch = String.Empty

			End If

		End If

	End Sub

	Public Function ConvertMZIDToTSV(ByVal JavaProgLoc As String, ByVal MSGFDbProgLoc As String, ByVal strDatasetName As String, ByVal strMZIDFileName As String) As String

		Dim strTSVFilePath As String = String.Empty
		Dim intJavaMemorySize As Integer

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		Try
			' Note that this file needs to be _msgfdb.tsv, not _msgfplus.tsv
			' The reason is that we want the PeptideToProtein Map file to be named Dataset_msgfdb_PepToProtMap.txt for compatibility with PHRPReader
			strTSVFilePath = System.IO.Path.Combine(m_WorkDir, strDatasetName & "_msgfdb.tsv")

			'Set up and execute a program runner to run the MzIDToTsv module of MSGFPlus

			intJavaMemorySize = 2000
			CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -cp " & MSGFDbProgLoc
			CmdStr &= " edu.ucsd.msjava.ui.MzIDToTsv"

			CmdStr &= " -i " & clsAnalysisToolRunnerBase.PossiblyQuotePath(IO.Path.Combine(m_WorkDir, strMZIDFileName))
			CmdStr &= " -o " & clsAnalysisToolRunnerBase.PossiblyQuotePath(strTSVFilePath)
			CmdStr &= " -showQValue 1"
			CmdStr &= " -showDecoy 1"
			CmdStr &= " -unroll 1"

			' Make sure the machine has enough free memory to run MSGFPlus
			Dim blnLogFreeMemoryOnSuccess As Boolean = False

			If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, "MzIDToTsv", blnLogFreeMemoryOnSuccess) Then
				ReportError("Not enough free memory to run the MzIDToTsv module in MSGFPlus")
				Return String.Empty
			End If

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)
			End If

			Dim objCreateTSV As clsRunDosProgram
			objCreateTSV = New clsRunDosProgram(m_WorkDir)

			With objCreateTSV
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = IO.Path.Combine(m_WorkDir, "MzIDToTsv_ConsoleOutput.txt")
			End With

			blnSuccess = objCreateTSV.RunProgram(JavaProgLoc, CmdStr, "MzIDToTsv", True)

			If Not blnSuccess Then
				ReportError("MSGFPlus returned an error code converting the .mzid file to a .tsv file: " & objCreateTSV.ExitCode)
				Return String.Empty
			Else
				Try
					' Delete the console output file
					IO.File.Delete(objCreateTSV.ConsoleOutputFilePath)
				Catch ex As Exception
					' Ignore errors here
				End Try

			End If

		Catch ex As Exception
			ReportError("Error in MSGFDbPlugin->ConvertMZIDToTSV", "Error in MSGFDbPlugin->ConvertMZIDToTSV: & " & ex.Message)
			Return String.Empty
		End Try

		Return strTSVFilePath

	End Function

	Public Function CreatePeptideToProteinMapping(ByVal ResultsFileName As String, blnResultsIncludeAutoAddedDecoyPeptides As Boolean) As IJobParams.CloseOutType

		Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")

		' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
		Dim dbFilename As String = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
		Dim strInputFilePath As String
		Dim strFastaFilePath As String

		Dim msg As String

		Dim blnIgnorePeptideToProteinMapperErrors As Boolean
		Dim blnSuccess As Boolean

		strInputFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)
		strFastaFilePath = System.IO.Path.Combine(OrgDbDir, dbFilename)

		Try
			' Validate that the input file has at least one entry; if not, then no point in continuing
			Dim strLineIn As String
			Dim intLinesRead As Integer

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				intLinesRead = 0
				Do While srInFile.Peek > -1 AndAlso intLinesRead < 10
					strLineIn = srInFile.ReadLine()
					If Not String.IsNullOrEmpty(strLineIn) Then
						intLinesRead += 1
					End If
				Loop

			End Using


			If intLinesRead <= 1 Then
				' File is empty or only contains a header line
				msg = "No results above threshold"
				ReportError(msg, msg & "; " & GetSearchEngineName() & " results file is empty")

				Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
			End If

		Catch ex As Exception

			msg = "Error validating MSGF-DB results file contents in CreatePeptideToProteinMapping"
			ReportError(msg, msg & ", job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		End Try

		If blnResultsIncludeAutoAddedDecoyPeptides Then
			' Read the original fasta file to create a decoy fasta file
			strFastaFilePath = GenerateDecoyFastaFile(strFastaFilePath, m_WorkDir)

			If String.IsNullOrEmpty(strFastaFilePath) Then
				' Problem creating the decoy fasta file
				If String.IsNullOrEmpty(mErrorMessage) Then
					mErrorMessage = "Error creating a decoy version of the fasta file"
				End If
				ReportError(mErrorMessage)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileToSkip(System.IO.Path.GetFileName(strFastaFilePath))
		End If

		Try
			If m_DebugLevel >= 1 Then
				ReportMessage("Creating peptide to protein map file")
			End If

			blnIgnorePeptideToProteinMapperErrors = m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", False)

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
					ReportError("Peptide to protein mapping file was not created", "Peptide to protein mapping file was not created: " & strResultsFilePath)
					blnSuccess = False
				Else
					If m_DebugLevel >= 2 Then
						ReportMessage("Peptide to protein mapping complete")
					End If

					blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors)
				End If
			Else
				If mPeptideToProteinMapper.GetErrorMessage.Length = 0 AndAlso mPeptideToProteinMapper.StatusMessage.ToLower().Contains("error") Then
					ReportError("Error running clsPeptideToProteinMapEngine: " & mPeptideToProteinMapper.StatusMessage)
				Else
					ReportError("Error running clsPeptideToProteinMapEngine: " & mPeptideToProteinMapper.GetErrorMessage())
					If mPeptideToProteinMapper.StatusMessage.Length > 0 Then
						ReportError("clsPeptideToProteinMapEngine status: " & mPeptideToProteinMapper.StatusMessage)
					End If
				End If

				If blnIgnorePeptideToProteinMapperErrors Then
					ReportWarning("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")

					If System.IO.File.Exists(strResultsFilePath) Then
						blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors)
					Else
						blnSuccess = True
					End If

				Else
					ReportError("Error in CreatePeptideToProteinMapping")
					blnSuccess = False
				End If
			End If

		Catch ex As Exception
			msg = "Exception in CreatePeptideToProteinMapping"
			ReportError(msg, "CreatePeptideToProteinMapping, Error running clsPeptideToProteinMapEngine, job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))

			If blnIgnorePeptideToProteinMapperErrors Then
				ReportWarning("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
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

	Public Sub DeleteFileInWorkDir(ByVal strFilename As String)

		Dim fiFile As System.IO.FileInfo

		Try
			fiFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, strFilename))

			If fiFile.Exists Then
				fiFile.Delete()
			End If

		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

	''' Read the original fasta file to create a decoy fasta file
	''' <summary>
	''' Creates a decoy version of the fasta file specified by strInputFilePath
	''' This new file will include the original proteins plus reversed versions of the original proteins
	''' Protein names will be prepended with REV_ or XXX_
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

		Dim blnInputProteinFound As Boolean
		Dim strPrefix As String

		Try
			ioSourceFile = New System.IO.FileInfo(strInputFilePath)
			If Not ioSourceFile.Exists Then
				mErrorMessage = "Fasta file not found: " & ioSourceFile.FullName
				Return String.Empty
			End If

			strDecoyFastaFilePath = System.IO.Path.Combine(strOutputDirectoryPath, System.IO.Path.GetFileNameWithoutExtension(ioSourceFile.Name) & "_decoy.fasta")

			If m_DebugLevel >= 2 Then
				ReportMessage("Creating decoy fasta file at " & strDecoyFastaFilePath)
			End If

			objFastaFileReader = New ProteinFileReader.FastaFileReader
			With objFastaFileReader
				.ProteinLineStartChar = PROTEIN_LINE_START_CHAR
				.ProteinLineAccessionEndChar = PROTEIN_LINE_ACCESSION_END_CHAR
			End With

			If Not objFastaFileReader.OpenFile(strInputFilePath) Then
				ReportError("Error reading fasta file with ProteinFileReader to create decoy file")
				Return String.Empty
			End If

			If mMSGFPlus Then
				strPrefix = "XXX_"
			Else
				strPrefix = "REV_"
			End If

			Using swProteinOutputFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strDecoyFastaFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

				Do
					blnInputProteinFound = objFastaFileReader.ReadNextProteinEntry()

					If blnInputProteinFound Then
						' Write the forward protein
						swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR & objFastaFileReader.ProteinName & " " & objFastaFileReader.ProteinDescription)
						WriteProteinSequence(swProteinOutputFile, objFastaFileReader.ProteinSequence)

						' Write the decoy protein
						swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR & strPrefix & objFastaFileReader.ProteinName & " " & objFastaFileReader.ProteinDescription)
						WriteProteinSequence(swProteinOutputFile, ReverseString(objFastaFileReader.ProteinSequence))
					End If

				Loop While blnInputProteinFound

			End Using

			objFastaFileReader.CloseFile()

		Catch ex As Exception
			Dim msg As String
			msg = "Exception creating decoy fasta file"
			ReportError(msg, "GenerateDecoyFastaFile, " & msg & ": " & ex.Message)
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
		dctParamNames.Add(MSGFDB_OPTION_FRAGMENTATION_METHOD, "m")
		dctParamNames.Add(MSGFDB_OPTION_INSTRUMENT_ID, "inst")
		dctParamNames.Add("EnzymeID", "e")
		dctParamNames.Add("C13", "c13")
		dctParamNames.Add("NNET", "nnet")				' Used by MS-GFDB
		dctParamNames.Add("NTT", "ntt")					' Used by MSGF+
		dctParamNames.Add("minLength", "minLength")
		dctParamNames.Add("maxLength", "maxLength")
		dctParamNames.Add("minCharge", "minCharge")		' Only used if the spectrum file doesn't have charge information
		dctParamNames.Add("maxCharge", "maxCharge")		' Only used if the spectrum file doesn't have charge information
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

	Protected Function GetSearchEngineName() As String
		Return GetSearchEngineName(mMSGFPlus)
	End Function

	Public Shared Function GetSearchEngineName(ByVal blnMSGFPlus As Boolean) As String
		If blnMSGFPlus Then
			Return "MSGF+"
		Else
			Return "MS-GFDB"
		End If
	End Function

	Public Function InitializeFastaFile(ByVal JavaProgLoc As String, ByVal MSGFDbProgLoc As String, ByRef FastaFileSizeKB As Single, ByRef FastaFileIsDecoy As Boolean, ByRef FastaFilePath As String) As IJobParams.CloseOutType

		Dim OrgDbDir As String
		Dim result As IJobParams.CloseOutType

		Dim objIndexedDBCreator As New clsCreateMSGFDBSuffixArrayFiles

		' Define the path to the fasta file
		OrgDbDir = m_mgrParams.GetParam("orgdbdir")
		FastaFilePath = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

		Dim fiFastaFile As System.IO.FileInfo
		fiFastaFile = New System.IO.FileInfo(FastaFilePath)

		If Not fiFastaFile.Exists Then
			' Fasta file not found
			ReportError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		FastaFileSizeKB = CSng(fiFastaFile.Length / 1024.0)
		FastaFileIsDecoy = False

		Dim strProteinOptions As String
		strProteinOptions = m_jobParams.GetParam("ProteinOptions")
		If Not String.IsNullOrEmpty(strProteinOptions) Then
			If strProteinOptions.ToLower.Contains("seq_direction=decoy") Then
				FastaFileIsDecoy = True
			End If
		End If

		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Indexing Fasta file to create Suffix Array files")
		End If

		' Index the fasta file to create the Suffix Array files
		Dim intIteration As Integer = 1

		Do While intIteration <= 2

			' Note that FastaFilePath will get updated by the IndexedDBCreator if we're running Legacy MSGFDB
			result = objIndexedDBCreator.CreateSuffixArrayFiles(m_WorkDir, m_DebugLevel, m_JobNum, JavaProgLoc, MSGFDbProgLoc, FastaFilePath, FastaFileIsDecoy)
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Exit Do
			ElseIf result = IJobParams.CloseOutType.CLOSEOUT_FAILED OrElse (result <> IJobParams.CloseOutType.CLOSEOUT_FAILED And intIteration >= 2) Then

				If Not String.IsNullOrEmpty(objIndexedDBCreator.ErrorMessage) Then
					ReportError(objIndexedDBCreator.ErrorMessage)
				Else
					ReportError("Error creating Suffix Array files")
				End If
				Return result
			End If

			intIteration += 1

		Loop

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

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
	''' <returns>Percent Complete (value between 0 and 100)</returns>
	''' <remarks>MSGFDb version is available via the MSGFDbVersion property</remarks>
	Public Function ParseMSGFDBConsoleOutputFile() As Single

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

		Dim strConsoleOutputFilePath As String = "??"

		Dim eThreadProgressBase() As eThreadProgressSteps
		Dim sngThreadProgressAddon() As Single
		Dim sngEffectiveProgress As Single

		Try
			' Initially reserve space for 32 threads
			' We'll expand these arrays later if needed
			ReDim eThreadProgressBase(32)
			ReDim sngThreadProgressAddon(32)

			strConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT_FILE)
			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					ReportMessage("Console output file not found: " & strConsoleOutputFilePath)
				End If

				Return 0
			End If

			If m_DebugLevel >= 4 Then
				ReportMessage("Parsing file " & strConsoleOutputFilePath)
			End If

			Dim strLineIn As String
			Dim intLinesRead As Integer

			Dim oMatch As System.Text.RegularExpressions.Match
			Dim intThreadCount As Short = 0

			sngEffectiveProgress = PROGRESS_PCT_MSGFDB_STARTING

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				intLinesRead = 0
				Do While srInFile.Peek() >= 0
					strLineIn = srInFile.ReadLine()
					intLinesRead += 1

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						If intLinesRead = 1 Then
							' The first line is the MSGFDB version
							If strLineIn.ToLower.Contains("gfdb") OrElse strLineIn.ToLower.Contains("ms-gf+") Then
								If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSGFDbVersion) Then
									ReportMessage("MSGFDB version: " & strLineIn)
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

						ElseIf strLineIn.StartsWith("Computing EFDRs") OrElse strLineIn.StartsWith("Computing q-values") Then
							If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
								sngEffectiveProgress = PROGRESS_PCT_MSGFDB_COMPUTING_FDRS
							End If

						ElseIf strLineIn.StartsWith("MS-GFDB complete") OrElse strLineIn.StartsWith("MS-GF+ complete") Then
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

						ElseIf strLineIn.Contains("Computing spectral probabilities finished") OrElse strLineIn.Contains("Computing spectral E-values finished") Then
							If sngEffectiveProgress < PROGRESS_PCT_MSGFDB_COMPUTING_FDRS Then
								ParseConsoleOutputThreadMessage(strLineIn, eThreadProgressSteps.Complete, eThreadProgressBase, sngThreadProgressAddon)
							End If

						ElseIf strLineIn.Contains("Computing spectral probabilities") OrElse strLineIn.Contains("Computing spectral E-values") Then
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

			End Using


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

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				ReportWarning("Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

		Return sngEffectiveProgress

	End Function

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

		Try
			Dim fiParameterFile As System.IO.FileInfo
			fiParameterFile = New System.IO.FileInfo(strParameterFilePath)

			strModFilePath = System.IO.Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME)

			' Note that ParseMSGFDbValidateMod will set this to True if a dynamic or static mod is STY phosphorylation 
			mPhosphorylationSearch = False

			sbOptions.Append(" -mod " & MOD_FILE_NAME)

			Using swModFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strModFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

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
								mErrorMessage = "Static mod definition contains ',opt,'; update the param file to have ',fix,' or change to 'DynamicMod='"
								ReportError(mErrorMessage, mErrorMessage & "; " & strStaticMod)
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
								mErrorMessage = "Dynamic mod definition contains ',fix,'; update the param file to have ',opt,' or change to 'StaticMod='"
								ReportError(mErrorMessage, mErrorMessage & "; " & strDynamicMod)
								Return False
							End If
							swModFile.WriteLine(strModClean)
						Else
							Return False
						End If
					Next
				End If

			End Using

			blnSuccess = True

		Catch ex As Exception
			mErrorMessage = "Exception creating MSGFDB Mods file"
			ReportError(mErrorMessage, mErrorMessage & ": " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Read the MSGFDB options file and convert the options to command line switches
	''' </summary>
	''' <param name="FastaFileSizeKB">Size of the .Fasta file, in KB</param>
	''' <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
	''' <param name="strMSGFDbCmdLineOptions">Output: MSGFDb command line arguments</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
	Public Function ParseMSGFDBParameterFile(ByVal FastaFileSizeKB As Single, ByVal FastaFileIsDecoy As Boolean, ByVal strAssumedScanType As String, ByRef strMSGFDbCmdLineOptions As String) As IJobParams.CloseOutType
		Const DEFINE_MAX_THREADS As Boolean = True
		Const SMALL_FASTA_FILE_THRESHOLD_KB As Integer = 20

		Dim strParameterFilePath As String
		Dim strLineIn As String
		Dim sbOptions As System.Text.StringBuilder

		Dim strKey As String
		Dim strValue As String
		Dim intValue As Integer

		Dim intParamFileThreadCount As Integer = 0
		Dim strDMSDefinedThreadCount As String
		Dim intDMSDefinedThreadCount As Integer = 0

		Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)

		Dim intNumMods As Integer = 0
		Dim lstStaticMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
		Dim lstDynamicMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)

		Dim blnShowDecoyParamPresent As Boolean = False
		Dim blnShowDecoy As Boolean = False
		Dim blnTDA As Boolean = False

		Dim strDatasetType As String
		Dim blnHCD As Boolean = False
		Dim blnHighResMSn As Boolean = False

		Dim strSearchEngineName As String

		strMSGFDbCmdLineOptions = String.Empty
		strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

		If Not System.IO.File.Exists(strParameterFilePath) Then
			ReportError("Parameter file not found", "Parameter file not found: " & strParameterFilePath)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		strDatasetType = m_jobParams.GetParam("JobParameters", "DatasetType")
		If strDatasetType.ToUpper().Contains("HCD") Then
			blnHCD = True
		End If

		strSearchEngineName = GetSearchEngineName()

		sbOptions = New System.Text.StringBuilder(500)

		' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
		' Alternatively, if running MSGF+, this is set to true if TDA=1
		mResultsIncludeAutoAddedDecoyPeptides = False

		Try

			' Initialize the Param Name dictionary
			dctParamNames = GetMSFGDBParameterNames()

			Using srParamFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strParameterFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				Do While srParamFile.Peek > -1
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
						Dim strArgumentSwitchOriginal As String

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
										mErrorMessage = "Invalid assumed scan type '" & strAssumedScanType & "'; must be CID, ETD, or HCD"
										ReportError(mErrorMessage)
										Return IJobParams.CloseOutType.CLOSEOUT_FAILED
								End Select
							End If

							strArgumentSwitchOriginal = String.Copy(strArgumentSwitch)

							AdjustSwitchesForMSGFPlus(mMSGFPlus, strArgumentSwitch, strValue)

							If String.IsNullOrEmpty(strArgumentSwitch) Then
								If m_DebugLevel >= 1 And Not IsMatch(strArgumentSwitchOriginal, MSGFDB_OPTION_SHOWDECOY) Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Skipping switch " & strArgumentSwitchOriginal & " since it is not valid for this version of " & strSearchEngineName)
								End If
							ElseIf String.IsNullOrEmpty(strValue) Then
								If m_DebugLevel >= 1 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Skipping switch " & strArgumentSwitch & " since the value is empty")
								End If
							Else
								sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)
							End If


							If IsMatch(strArgumentSwitch, "showDecoy") Then
								blnShowDecoyParamPresent = True
								If Integer.TryParse(strValue, intValue) Then
									If intValue > 0 Then
										blnShowDecoy = True
									End If
								End If
							ElseIf IsMatch(strArgumentSwitch, "tda") Then
								If Integer.TryParse(strValue, intValue) Then
									If intValue > 0 Then
										blnTDA = True
									End If
								End If
							End If

						ElseIf IsMatch(strKey, "uniformAAProb") Then

							If mMSGFPlus Then
								' Not valid for MSGF+; skip it
							Else

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
										mErrorMessage = "Invalid value for uniformAAProb in MSGFDB parameter file"
										ReportError(mErrorMessage, mErrorMessage & ": " & strLineIn)
										srParamFile.Close()
										Return IJobParams.CloseOutType.CLOSEOUT_FAILED
									End If
								End If
							End If

						ElseIf DEFINE_MAX_THREADS AndAlso IsMatch(strKey, "NumThreads") Then
							If String.IsNullOrWhiteSpace(strValue) OrElse IsMatch(strValue, "all") Then
								' Do not append -thread to the command line; MSGFDB will use all available cores by default
							Else
								If Integer.TryParse(strValue, intParamFileThreadCount) Then
									' intParamFileThreadCount now has the thread count
								Else
									ReportWarning("Invalid value for NumThreads in MSGFDB parameter file: " & strLineIn)
								End If
							End If


						ElseIf IsMatch(strKey, "NumMods") Then
							If Integer.TryParse(strValue, intValue) Then
								intNumMods = intValue
							Else
								mErrorMessage = "Invalid value for NumMods in MSGFDB parameter file"
								ReportError(mErrorMessage, mErrorMessage & ": " & strLineIn)
								srParamFile.Close()
								Return IJobParams.CloseOutType.CLOSEOUT_FAILED
							End If

						ElseIf IsMatch(strKey, "StaticMod") Then
							If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not IsMatch(strValue, "none") Then
								lstStaticMods.Add(strValue)
							End If

						ElseIf IsMatch(strKey, "DynamicMod") Then
							If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not IsMatch(strValue, "none") Then
								lstDynamicMods.Add(strValue)
							End If
						End If

						If IsMatch(strKey, MSGFDB_OPTION_INSTRUMENT_ID) Then
							If Integer.TryParse(strValue, intValue) Then
								' 0 means Low-res LCQ/LTQ (Default for CID and ETD); use InstrumentID=0 if analyzing a dataset with low-res CID and high-res HCD spectra
								' 1 means High-res LTQ (Default for HCD).  Do not merge spectra (FragMethod=4) when InstrumentID is 1; scores will degrade
								' 2 means TOF
								If intValue = 1 Then
									blnHighResMSn = True
								End If
							End If
						End If

						If IsMatch(strKey, MSGFDB_OPTION_FRAGMENTATION_METHOD) Then
							If Integer.TryParse(strValue, intValue) Then
								If intValue = 3 Then
									blnHCD = True
								End If
							End If
						End If

					End If
				Loop

			End Using

			If blnTDA Then
				If mMSGFPlus Then
					' Parameter file contains TDA=1 and we're running MSGF+
					mResultsIncludeAutoAddedDecoyPeptides = True
				ElseIf blnShowDecoy Then
					' Parameter file contains both TDA=1 and showDecoy=1
					mResultsIncludeAutoAddedDecoyPeptides = True
				End If
			End If

			If Not blnShowDecoyParamPresent And Not mMSGFPlus Then
				' Add showDecoy to sbOptions
				sbOptions.Append(" -showDecoy 0")
			End If

		Catch ex As Exception
			mErrorMessage = "Exception reading MSGFDB parameter file"
			ReportError(mErrorMessage, mErrorMessage & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Define the thread count; note that MSGFDBThreads could be "all"
		strDMSDefinedThreadCount = m_jobParams.GetJobParameter("MSGFDBThreads", String.Empty)
		If Not Integer.TryParse(strDMSDefinedThreadCount, intDMSDefinedThreadCount) Then
			intDMSDefinedThreadCount = 0
		End If

		If intDMSDefinedThreadCount > 0 Then
			intParamFileThreadCount = intDMSDefinedThreadCount
		End If

		If intParamFileThreadCount <= 0 Then
			' Set intParamFileThreadCount to the number of cores on this computer, minus 1
			' Note that Environment.ProcessorCount tells us the number of logical processors, not the number of cores
			' Thus, we need to use a WMI query (see http://stackoverflow.com/questions/1542213/how-to-find-the-number-of-cpu-cores-via-net-c )

			Dim coreCount As Integer = 0
			For Each item As System.Management.ManagementBaseObject In New System.Management.ManagementObjectSearcher("Select * from Win32_Processor").Get()
				coreCount += Integer.Parse(item("NumberOfCores").ToString())
			Next

			intParamFileThreadCount = coreCount - 1
		End If

		If intParamFileThreadCount > 0 Then
			sbOptions.Append(" -thread " & intParamFileThreadCount)
		End If

		' Create the modification file and append the -mod switch
		' We'll also set mPhosphorylationSearch to True if a dynamic or static mod is STY phosphorylation 
		If Not ParseMSGFDBModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If


		' Check whether we are performing an HCD-based phosphorylation search
		If mPhosphorylationSearch AndAlso blnHCD Then
			If Not sbOptions.ToString().Contains("-protocol ") Then
				' Specify that "Protocol 1" is being used
				' This instructs MSGFDB to use a scoring model specially trained for HCD Phospho data
				sbOptions.Append(" -protocol 1")
			End If
		End If

		strMSGFDbCmdLineOptions = sbOptions.ToString()

		If strMSGFDbCmdLineOptions.Contains("-tda 1") Then
			' Make sure the .Fasta file is not a Decoy fasta
			If FastaFileIsDecoy Then
				ReportError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS


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
			mErrorMessage = "Invalid modification string; must have 5 sections: " & strMod
			ReportError(mErrorMessage)
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

		' Check whether this is a phosphorylation mod
		If strSplitMod(4).Trim().ToUpper().StartsWith("PHOSPH") OrElse strSplitMod(0).ToUpper() = "HO3P" Then
			If strSplitMod(1).ToUpper().IndexOfAny(New Char() {"S"c, "T"c, "Y"c}) >= 0 Then
				mPhosphorylationSearch = True
			End If
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

	Public Shared Function UseLegacyMSGFDB(jobParams As AnalysisManagerBase.IJobParams) As Boolean
		Dim strValue As String

		' Default to using MSGF+
		Dim blnUseLegacyMSGFDB As Boolean = False

		strValue = jobParams.GetJobParameter("UseLegacyMSGFDB", String.Empty)
		If Not String.IsNullOrEmpty(strValue) Then
			If Not Boolean.TryParse(strValue, blnUseLegacyMSGFDB) Then
				' Error parsing strValue; not boolean
				strValue = String.Empty
			End If
		End If

		If String.IsNullOrEmpty(strValue) Then
			strValue = jobParams.GetJobParameter("UseMSGFPlus", String.Empty)

			If Not String.IsNullOrEmpty(strValue) Then
				Dim blnUseMSGFPlus As Boolean
				If Boolean.TryParse(strValue, blnUseMSGFPlus) Then
					strValue = "False"
					blnUseLegacyMSGFDB = False
				Else
					strValue = String.Empty
				End If
			End If

			If String.IsNullOrEmpty(strValue) Then
				' Default to using MSGF+
				blnUseLegacyMSGFDB = False
			End If
		End If

		Return blnUseLegacyMSGFDB

	End Function

	Private Function ValidatePeptideToProteinMapResults(ByVal strPeptideToProteinMapFilePath As String, ByVal blnIgnorePeptideToProteinMapperErrors As Boolean) As Boolean

		Const PROTEIN_NAME_NO_MATCH As String = "__NoMatch__"

		Dim blnSuccess As Boolean = False

		Dim intPeptideCount As Integer = 0
		Dim intPeptideCountNoMatch As Integer = 0
		Dim intLinesRead As Integer = 0

		Try
			' Validate that none of the results in strPeptideToProteinMapFilePath has protein name PROTEIN_NAME_NO_MATCH

			Dim strLineIn As String

			If m_DebugLevel >= 2 Then
				ReportMessage("Validating peptide to protein mapping, file " & System.IO.Path.GetFileName(strPeptideToProteinMapFilePath))
			End If

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strPeptideToProteinMapFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

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

			End Using


			If intPeptideCount = 0 Then
				mErrorMessage = "Peptide to protein mapping file is empty"
				ReportError(mErrorMessage, mErrorMessage & ", file " & System.IO.Path.GetFileName(strPeptideToProteinMapFilePath))
				blnSuccess = False

			ElseIf intPeptideCountNoMatch = 0 Then
				If m_DebugLevel >= 2 Then
					ReportMessage("Peptide to protein mapping validation complete; processed " & intPeptideCount & " peptides")
				End If

				blnSuccess = True

			Else
				Dim dblErrorPercent As Double	' Value between 0 and 100
				dblErrorPercent = intPeptideCountNoMatch / intPeptideCount * 100.0


				mErrorMessage = dblErrorPercent.ToString("0.0") & "% of the entries in the peptide to protein map file did not match to a protein in the FASTA file"
				ReportError(mErrorMessage)

				If blnIgnorePeptideToProteinMapperErrors Then
					ReportWarning("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
					blnSuccess = True
				Else
					RaiseEvent IgnorePreviousErrorEvent()
					blnSuccess = False
				End If
			End If

		Catch ex As Exception

			mErrorMessage = "Error validating peptide to protein map file"
			ReportError(mErrorMessage, mErrorMessage & ", job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))
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
	Public Function ZipOutputFile(ByRef oToolRunner As clsAnalysisToolRunnerBase, ByVal FileName As String) As IJobParams.CloseOutType
		Dim TmpFilePath As String

		Try

			TmpFilePath = System.IO.Path.Combine(m_WorkDir, FileName)
			If Not System.IO.File.Exists(TmpFilePath) Then
				ReportError("MSGFDB results file not found: " & FileName)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
			End If

			If Not oToolRunner.ZipFile(TmpFilePath, False) Then
				Dim Msg As String = "Error zipping output files"
				ReportError(Msg, Msg & ": oToolRunner.ZipFile returned false, job " & m_JobNum)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
			m_jobParams.AddResultFileToSkip(FileName)

		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerMSGFDB.ZipOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
			ReportError("Error zipping output files", Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

#Region "Event Methods"
	Protected Sub ReportError(Message As String)
		ReportError(Message, String.Empty)
	End Sub

	Protected Sub ReportError(Message As String, DetailedMessage As String)
		RaiseEvent ErrorEvent(Message, DetailedMessage)
	End Sub

	Protected Sub ReportMessage(Message As String)
		RaiseEvent MessageEvent(Message)
	End Sub

	Protected Sub ReportWarning(Message As String)
		RaiseEvent WarningEvent(Message)
	End Sub

	Private Sub mPeptideToProteinMapper_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mPeptideToProteinMapper.ProgressChanged

		Const MAPPER_PROGRESS_LOG_INTERVAL_SECONDS As Integer = 120
		Static dtLastLogTime As System.DateTime = System.DateTime.UtcNow

		If m_DebugLevel >= 1 Then
			If System.DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS Then
				dtLastLogTime = System.DateTime.UtcNow
				ReportMessage("Mapping peptides to proteins: " & percentComplete.ToString("0.0") & "% complete")
			End If
		End If

	End Sub
#End Region

End Class
