Option Strict On
Imports AnalysisManagerBase
Imports System.Runtime.InteropServices
Imports System.IO
Imports System.Net

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

	Public Const MSGFDB_TSV_SUFFIX As String = "_msgfdb.tsv"

	' Obsolete setting: Old MS-GFDB program
	'Public Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

	Public Const MSGFPLUS_JAR_NAME As String = "MSGFPlus.jar"
	Public Const MSGFDB_CONSOLE_OUTPUT_FILE As String = "MSGFDB_ConsoleOutput.txt"

	Public Const MOD_FILE_NAME As String = "MSGFDB_Mods.txt"

#End Region

#Region "Events"
	Public Event ErrorEvent(Message As String, DetailedMessage As String)
	Public Event IgnorePreviousErrorEvent()
	Public Event MessageEvent(Message As String)
	Public Event WarningEvent(Message As String)
#End Region

#Region "Module Variables"
	Protected m_mgrParams As IMgrParams
	Protected m_jobParams As IJobParams

	Protected m_WorkDir As String
	Protected m_JobNum As String
	Protected m_DebugLevel As Short

	Protected mMSGFPlus As Boolean
	Protected mMSGFDbVersion As String = String.Empty
	Protected mErrorMessage As String = String.Empty
	Protected mConsoleOutputErrorMsg As String = String.Empty

	Protected mContinuumSpectraSkipped As Integer
	Protected mSpectraSearched As Integer

	Protected mPhosphorylationSearch As Boolean
	Protected mResultsIncludeAutoAddedDecoyPeptides As Boolean

	' Note that clsPeptideToProteinMapEngine utilizes System.Data.SQLite.dll
	Protected WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine
#End Region

	Public ReadOnly Property ContinuumSpectraSkipped() As Integer
		Get
			Return mContinuumSpectraSkipped
		End Get
	End Property

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

	Public ReadOnly Property SpectraSearched As Integer
		Get
			Return mSpectraSearched
		End Get
	End Property

#Region "Methods"

	Public Sub New(oMgrParams As IMgrParams, oJobParams As IJobParams, ByVal JobNum As String, ByVal strWorkDir As String, ByVal intDebugLevel As Short, ByVal blnMSGFPlus As Boolean)
		m_mgrParams = oMgrParams
		m_jobParams = oJobParams
		m_WorkDir = strWorkDir

		m_JobNum = JobNum
		m_DebugLevel = intDebugLevel

		mMSGFPlus = blnMSGFPlus
		mMSGFDbVersion = String.Empty
		mConsoleOutputErrorMsg = String.Empty
		mContinuumSpectraSkipped = 0
		mSpectraSearched = 0

	End Sub

	Protected Sub AdjustSwitchesForMSGFPlus(ByVal blnMSGFPlus As Boolean, ByRef strArgumentSwitch As String, ByRef strValue As String)

		Dim intValue As Integer
		Dim intCharIndex As Integer

		If blnMSGFPlus Then
			' MSGF+

			If clsGlobal.IsMatch(strArgumentSwitch, "nnet") Then
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

			ElseIf clsGlobal.IsMatch(strArgumentSwitch, "c13") Then
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

			ElseIf clsGlobal.IsMatch(strArgumentSwitch, "showDecoy") Then
				' Not valid for MSGF+; skip it
				strArgumentSwitch = String.Empty
			End If

		Else
			' MS-GFDB

			If clsGlobal.IsMatch(strArgumentSwitch, "ntt") Then
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

			ElseIf clsGlobal.IsMatch(strArgumentSwitch, "ti") Then
				' Auto-switch to c13
				' Use the digit after the comma in the "ti" specification
				strArgumentSwitch = "c13"
				intCharIndex = strValue.IndexOf(",", StringComparison.Ordinal)
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

			ElseIf clsGlobal.IsMatch(strArgumentSwitch, "addFeatures") Then
				' Not valid for MS-GFDB; skip it
				strArgumentSwitch = String.Empty

			End If

		End If

	End Sub

    Private Function CanDetermineInstIdFromInstGroup(ByVal instrumentGroup As String, <Out> ByRef instrumentIDNew As String, <Out> ByRef autoSwitchReason As String) As Boolean

        ' Thermo Instruments
        If clsGlobal.IsMatch(instrumentGroup, "QExactive") Then
            instrumentIDNew = "3"
            autoSwitchReason = "based on instrument group " & instrumentGroup
            Return True
        ElseIf clsGlobal.IsMatch(instrumentGroup, "Bruker_Amazon_Ion_Trap") Then    ' Non-Thermo Instrument, low res MS/MS
            instrumentIDNew = "0"
            autoSwitchReason = "based on instrument group " & instrumentGroup
            Return True
        ElseIf clsGlobal.IsMatch(instrumentGroup, "IMS") Then                       ' Non-Thermo Instrument, high res MS/MS
            instrumentIDNew = "1"
            autoSwitchReason = "based on instrument group " & instrumentGroup
            Return True
        ElseIf clsGlobal.IsMatch(instrumentGroup, "Sciex_TripleTOF") Then           ' Non-Thermo Instrument, high res MS/MS
            instrumentIDNew = "1"
            autoSwitchReason = "based on instrument group " & instrumentGroup
            Return True
        Else
            instrumentIDNew = String.Empty
            autoSwitchReason = String.Empty
            Return False
        End If

    End Function

    Private Sub AutoUpdateInstrumentIDIfChanged(ByRef instrumentIDCurrent As String, ByVal instrumentIDNew As String, ByVal autoSwitchReason As String)

        If Not String.IsNullOrEmpty(instrumentIDNew) AndAlso instrumentIDNew <> instrumentIDCurrent Then

            If m_DebugLevel >= 1 Then
                Dim strInstIDDescription As String = "??"
                Select Case instrumentIDNew
                    Case "0"
                        strInstIDDescription = "Low-res MSn"
                    Case "1"
                        strInstIDDescription = "High-res MSn"
                    Case "2"
                        strInstIDDescription = "TOF"
                    Case "3"
                        strInstIDDescription = "Q-Exactive"
                End Select

                ReportMessage("Auto-updating instrument ID from " & instrumentIDCurrent & " to " & instrumentIDNew & " (" & strInstIDDescription & ") " & autoSwitchReason)
            End If

            instrumentIDCurrent = instrumentIDNew
        End If

    End Sub

	Public Function ConvertMZIDToTSV(ByVal javaProgLoc As String, ByVal msgfDbProgLoc As String, ByVal strDatasetName As String, ByVal strMZIDFileName As String) As String

		Dim strTSVFilePath As String
		Dim javaMemorySizeMB As Integer

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		Try
			' Note that this file needs to be _msgfdb.tsv, not _msgfplus.tsv
			' The reason is that we want the PeptideToProtein Map file to be named Dataset_msgfdb_PepToProtMap.txt for compatibility with PHRPReader
			Dim tsvFileName = strDatasetName & MSGFDB_TSV_SUFFIX
			strTSVFilePath = Path.Combine(m_WorkDir, tsvFileName)

			'Set up and execute a program runner to run the MzIDToTsv module of MSGFPlus

			javaMemorySizeMB = 2000
			CmdStr = GetMZIDtoTSVCommandLine(strMZIDFileName, tsvFileName, m_WorkDir, msgfDbProgLoc, javaMemorySizeMB)

			' Make sure the machine has enough free memory to run MSGFPlus
			Const LOG_FREE_MEMORY_ON_SUCCESS As Boolean = False

			If Not clsAnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "MzIDToTsv", LOG_FREE_MEMORY_ON_SUCCESS) Then
				ReportError("Not enough free memory to run the MzIDToTsv module in MSGFPlus")
				Return String.Empty
			End If

			If m_DebugLevel >= 1 Then
				ReportMessage(javaProgLoc & " " & CmdStr)
			End If

			Dim objCreateTSV As clsRunDosProgram
			objCreateTSV = New clsRunDosProgram(m_WorkDir)

			With objCreateTSV
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = Path.Combine(m_WorkDir, "MzIDToTsv_ConsoleOutput.txt")
			End With

			blnSuccess = objCreateTSV.RunProgram(javaProgLoc, CmdStr, "MzIDToTsv", True)

			If Not blnSuccess Then
				ReportError("MSGFPlus returned an error code converting the .mzid file to a .tsv file: " & objCreateTSV.ExitCode)
				Return String.Empty
			Else
				Try
					' Delete the console output file
					File.Delete(objCreateTSV.ConsoleOutputFilePath)
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

	Public Shared Function GetMZIDtoTSVCommandLine(
	  ByVal mzidFileName As String,
	  ByVal tsvFileName As String,
	  ByVal workingDirectory As String,
	  ByVal msgfDbProgLoc As String,
	  ByVal javaMemorySizeMB As Integer) As String

		Dim CmdStr As String

		CmdStr = " -Xmx" & javaMemorySizeMB & "M -cp " & msgfDbProgLoc
		CmdStr &= " edu.ucsd.msjava.ui.MzIDToTsv"

		CmdStr &= " -i " & clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName))
		CmdStr &= " -o " & clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName))
		CmdStr &= " -showQValue 1"
		CmdStr &= " -showDecoy 1"
		CmdStr &= " -unroll 1"

		Return CmdStr

	End Function

	Public Function CreatePeptideToProteinMapping(
	  ByVal ResultsFileName As String,
	  ByVal ePeptideInputFileFormat As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants) As IJobParams.CloseOutType

		Const blnResultsIncludeAutoAddedDecoyPeptides = False
		Dim localOrgDbFolder As String = m_mgrParams.GetParam("orgdbdir")
		Return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder, ePeptideInputFileFormat)

	End Function

	Public Function CreatePeptideToProteinMapping(
	  ByVal ResultsFileName As String,
	  ByVal blnResultsIncludeAutoAddedDecoyPeptides As Boolean) As IJobParams.CloseOutType

		Dim localOrgDbFolder As String = m_mgrParams.GetParam("orgdbdir")
		Return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder)

	End Function

	Public Function CreatePeptideToProteinMapping(
	  ByVal ResultsFileName As String,
	  ByVal blnResultsIncludeAutoAddedDecoyPeptides As Boolean,
	  ByVal localOrgDbFolder As String) As IJobParams.CloseOutType

		Return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder, PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.MSGFDBResultsFile)

	End Function

	Public Function CreatePeptideToProteinMapping(
	  ByVal ResultsFileName As String,
	  ByVal blnResultsIncludeAutoAddedDecoyPeptides As Boolean,
	  ByVal localOrgDbFolder As String,
	  ByVal ePeptideInputFileFormat As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants) As IJobParams.CloseOutType

		' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
		Dim dbFilename As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
		Dim strInputFilePath As String
		Dim strFastaFilePath As String

		Dim msg As String

		Dim blnIgnorePeptideToProteinMapperErrors As Boolean
		Dim blnSuccess As Boolean

		strInputFilePath = Path.Combine(m_WorkDir, ResultsFileName)
		strFastaFilePath = Path.Combine(localOrgDbFolder, dbFilename)

		Try
			' Validate that the input file has at least one entry; if not, then no point in continuing
			Dim strLineIn As String
			Dim intLinesRead As Integer

			Using srInFile As StreamReader = New StreamReader(New FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

				intLinesRead = 0
                Do While Not srInFile.EndOfStream AndAlso intLinesRead < 10
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

			m_jobParams.AddResultFileToSkip(Path.GetFileName(strFastaFilePath))
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
				.PeptideInputFileFormat = ePeptideInputFileFormat
				.PeptideFileSkipFirstLine = False
				.ProteinDataRemoveSymbolCharacters = True
				.ProteinInputFilePath = strFastaFilePath
				.SaveProteinToPeptideMappingFile = True
				.SearchAllProteinsForPeptideSequence = True
				.SearchAllProteinsSkipCoverageComputationSteps = True
				.ShowMessages = False
			End With

			' Note that clsPeptideToProteinMapEngine utilizes Data.SQLite.dll
			blnSuccess = mPeptideToProteinMapper.ProcessFile(strInputFilePath, m_WorkDir, String.Empty, True)

			mPeptideToProteinMapper.CloseLogFileNow()

			Dim strResultsFilePath As String
			strResultsFilePath = Path.GetFileNameWithoutExtension(strInputFilePath) & PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.FILENAME_SUFFIX_PEP_TO_PROTEIN_MAPPING
			strResultsFilePath = Path.Combine(m_WorkDir, strResultsFilePath)

			If blnSuccess Then
				If Not File.Exists(strResultsFilePath) Then
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

					If File.Exists(strResultsFilePath) Then
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

	''' <summary>
	''' Create a trimmed version of fastaFilePath, with max size maxFastaFileSizeMB
	''' </summary>
	''' <param name="fastaFilePath">Fasta file to trim</param>
	''' <param name="maxFastaFileSizeMB">Maximum file size</param>
	''' <returns>Full path to the trimmed fasta; empty string if a problem</returns>
	''' <remarks></remarks>
	Protected Function CreateTrimmedFasta(ByVal fastaFilePath As String, ByVal maxFastaFileSizeMB As Integer) As String
		Dim trimmedFastaFilePath = String.Empty

		Try
			Dim fiFastaFile = New FileInfo(fastaFilePath)

			Dim fiTrimmedFasta = New FileInfo(Path.Combine(fiFastaFile.DirectoryName, Path.GetFileNameWithoutExtension(fiFastaFile.Name) & "_Trim" & maxFastaFileSizeMB & "MB.fasta"))

			If fiTrimmedFasta.Exists Then
				' Verify that the file matches the .hashcheck value
				Dim hashcheckFilePath = fiTrimmedFasta.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX

				Dim hashCheckError = String.Empty
				If clsGlobal.ValidateFileVsHashcheck(fiTrimmedFasta.FullName, hashcheckFilePath, hashCheckError) Then
					' The trimmed fasta file is valid
					ReportMessage("Using existing trimmed fasta: " & fiTrimmedFasta.Name)
					Return fiTrimmedFasta.FullName
				End If

			End If

			ReportMessage("Creating trimmed fasta: " & fiTrimmedFasta.Name)

			Dim contaminantUtility = New clsFastaContaminantUtility()

			Dim dctRequiredContaminants = New Dictionary(Of String, Boolean)
			For Each proteinName In contaminantUtility.ProteinNames()
				dctRequiredContaminants.Add(proteinName, False)
			Next

			Dim maxSizeBytes As Int64 = maxFastaFileSizeMB * 1024 * 1024
			Dim bytesWritten As Int64 = 0
			Dim proteinCount As Integer = 0

			Using srSourceFasta = New StreamReader(New FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
				Using swTrimmedFasta = New StreamWriter(New FileStream(fiTrimmedFasta.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                    While Not srSourceFasta.EndOfStream
                        Dim dataLine = srSourceFasta.ReadLine()

                        If String.IsNullOrWhiteSpace(dataLine) Then Continue While

                        If dataLine.StartsWith(">") Then
                            ' Protein header
                            If bytesWritten > maxSizeBytes Then
                                ' Do not write out any more proteins
                                Exit While
                            End If

                            Dim spaceIndex = dataLine.IndexOf(" "c, 1)
                            If spaceIndex < 0 Then spaceIndex = dataLine.Length - 1
                            Dim proteinName = dataLine.Substring(1, spaceIndex - 1)

                            If dctRequiredContaminants.ContainsKey(proteinName) Then
                                dctRequiredContaminants(proteinName) = True
                            End If

                            proteinCount += 1
                        End If

                        swTrimmedFasta.WriteLine(dataLine)
                        bytesWritten += dataLine.Length + 2
                    End While

					' Add any missing contaminants
					For Each protein In dctRequiredContaminants
						If Not protein.Value Then
							contaminantUtility.WriteProteinToFasta(swTrimmedFasta, protein.Key)
						End If
					Next

				End Using

			End Using

			ReportMessage("Trimmed fasta created using " & proteinCount & " proteins; creating the hashcheck file")

			clsGlobal.CreateHashcheckFile(fiTrimmedFasta.FullName, True)
			trimmedFastaFilePath = fiTrimmedFasta.FullName

		Catch ex As Exception
			mErrorMessage = "Exception trimming fasta file to " & maxFastaFileSizeMb & " MB"
			ReportError(mErrorMessage, "CreateTrimmedFasta, " & mErrorMessage & ": " & ex.Message)
			Return String.Empty
		End Try

		Return trimmedFastaFilePath

	End Function

	Public Sub DeleteFileInWorkDir(ByVal strFilename As String)

		Dim fiFile As FileInfo

		Try
			fiFile = New FileInfo(Path.Combine(m_WorkDir, strFilename))

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

		Dim strDecoyFastaFilePath As String
		Dim ioSourceFile As FileInfo

		Dim objFastaFileReader As ProteinFileReader.FastaFileReader

		Dim blnInputProteinFound As Boolean
		Dim strPrefix As String

		Try
			ioSourceFile = New FileInfo(strInputFilePath)
			If Not ioSourceFile.Exists Then
				mErrorMessage = "Fasta file not found: " & ioSourceFile.FullName
				Return String.Empty
			End If

			strDecoyFastaFilePath = Path.Combine(strOutputDirectoryPath, Path.GetFileNameWithoutExtension(ioSourceFile.Name) & "_decoy.fasta")

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

			Using swProteinOutputFile As StreamWriter = New StreamWriter(New FileStream(strDecoyFastaFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

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

	Protected Function GetMSFGDBParameterNames() As Dictionary(Of String, String)
		Dim dctParamNames As Dictionary(Of String, String)
		dctParamNames = New Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

		dctParamNames.Add("PMTolerance", "t")
		dctParamNames.Add(MSGFDB_OPTION_TDA, "tda")
		dctParamNames.Add(MSGFDB_OPTION_SHOWDECOY, "showDecoy")
		dctParamNames.Add(MSGFDB_OPTION_FRAGMENTATION_METHOD, "m")		' This setting is always set to 0 since we create a _ScanType.txt file that specifies the type of each scan (thus, the value in the parameter file is ignored)
		dctParamNames.Add(MSGFDB_OPTION_INSTRUMENT_ID, "inst")			' This setting is auto-updated based on the instrument class for this dataset, plus also the scan types listed in the _ScanType.txt file (thus, the value in the parameter file is typically ignored)
		dctParamNames.Add("EnzymeID", "e")
		dctParamNames.Add("C13", "c13")					' Used by MS-GFDB
		dctParamNames.Add("IsotopeError", "ti")			' Used by MSGF+
		dctParamNames.Add("NNET", "nnet")				' Used by MS-GFDB
		dctParamNames.Add("NTT", "ntt")					' Used by MSGF+
		dctParamNames.Add("minLength", "minLength")
		dctParamNames.Add("maxLength", "maxLength")
		dctParamNames.Add("minCharge", "minCharge")		' Only used if the spectrum file doesn't have charge information
		dctParamNames.Add("maxCharge", "maxCharge")		' Only used if the spectrum file doesn't have charge information
		dctParamNames.Add("NumMatchesPerSpec", "n")
        dctParamNames.Add("minNumPeaks", "minNumPeaks") ' Auto-added by this code if not defined
        dctParamNames.Add("Protocol", "protocol")

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

	Public Function GetSettingFromMSGFDbParamFile(ByVal strParameterFilePath As String, ByVal strSettingToFind As String) As String
		Return GetSettingFromMSGFDbParamFile(strParameterFilePath, strSettingToFind, String.Empty)
	End Function

	Public Function GetSettingFromMSGFDbParamFile(ByVal strParameterFilePath As String, ByVal strSettingToFind As String, ByVal strValueIfNotFound As String) As String

		Dim strLineIn As String
		Dim kvSetting As KeyValuePair(Of String, String)

		If Not File.Exists(strParameterFilePath) Then
			ReportError("Parameter file not found", "Parameter file not found: " & strParameterFilePath)
			Return strValueIfNotFound
		End If

		Try

			Using srParamFile As StreamReader = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srParamFile.EndOfStream
                    strLineIn = srParamFile.ReadLine()

                    kvSetting = clsGlobal.GetKeyValueSetting(strLineIn)

                    If Not String.IsNullOrWhiteSpace(kvSetting.Key) AndAlso clsGlobal.IsMatch(kvSetting.Key, strSettingToFind) Then
                        Return kvSetting.Value
                    End If
                Loop

			End Using

		Catch ex As Exception
			mErrorMessage = "Exception reading MSGFDB parameter file"
			ReportError(mErrorMessage, mErrorMessage & ": " & ex.Message)
		End Try

		Return strValueIfNotFound

	End Function

	Public Function InitializeFastaFile(
	  ByVal javaProgLoc As String,
	  ByVal msgfDbProgLoc As String,
	  <Out()> ByRef fastaFileSizeKB As Single,
	  <Out()> ByRef fastaFileIsDecoy As Boolean,
	  <Out()> ByRef fastaFilePath As String) As IJobParams.CloseOutType

		Dim udtHPCOptions = New clsAnalysisResources.udtHPCOptionsType

		Return InitializeFastaFile(javaProgLoc, msgfDbProgLoc, fastaFileSizeKB, fastaFileIsDecoy, fastaFilePath, String.Empty, udtHPCOptions)

	End Function

	Public Function InitializeFastaFile(
	 ByVal javaProgLoc As String,
	 ByVal msgfDbProgLoc As String,
	 <Out()> ByRef fastaFileSizeKB As Single,
	 <Out()> ByRef fastaFileIsDecoy As Boolean,
	 <Out()> ByRef fastaFilePath As String,
	 ByVal strMSGFDBParameterFilePath As String,
	 ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As IJobParams.CloseOutType

		Return InitializeFastaFile(javaProgLoc, msgfDbProgLoc, fastaFileSizeKB, fastaFileIsDecoy, fastaFilePath, strMSGFDBParameterFilePath, udtHPCOptions, 0)

	End Function

	Public Function InitializeFastaFile(
	  ByVal javaProgLoc As String,
	  ByVal msgfDbProgLoc As String,
	  <Out()> ByRef fastaFileSizeKB As Single,
	  <Out()> ByRef fastaFileIsDecoy As Boolean,
	  <Out()> ByRef fastaFilePath As String,
	  ByVal strMSGFDBParameterFilePath As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
	  ByVal maxFastaFileSizeMB As Integer) As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType
		Dim oRand = New Random()

		Dim objIndexedDBCreator As clsCreateMSGFDBSuffixArrayFiles
		Dim strMgrName As String = m_mgrParams.GetParam("MgrName", "Undefined-Manager")
		Dim sPICHPCUsername = m_mgrParams.GetParam("PICHPCUser", "")
		Dim sPICHPCPassword = m_mgrParams.GetParam("PICHPCPassword", "")

		objIndexedDBCreator = New clsCreateMSGFDBSuffixArrayFiles(strMgrName, sPICHPCUsername, sPICHPCPassword)

		' Define the path to the fasta file
		Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")
		If udtHPCOptions.UsingHPC Then
			' Override the OrgDB folder to point to Picfs, specifically \\winhpcfs\projects\DMS\DMS_Temp_Org
			localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org")
		End If
		fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

		fastaFileSizeKB = 0
		fastaFileIsDecoy = False

		Dim fiFastaFile As FileInfo
		fiFastaFile = New FileInfo(fastaFilePath)

		If Not fiFastaFile.Exists Then
			' Fasta file not found
			ReportError("Fasta file not found: " & fiFastaFile.Name, "Fasta file not found: " & fiFastaFile.FullName)
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		fastaFileSizeKB = CSng(fiFastaFile.Length / 1024.0)

		Dim strProteinOptions As String
		strProteinOptions = m_jobParams.GetParam("ProteinOptions")
		If Not String.IsNullOrEmpty(strProteinOptions) Then
			If strProteinOptions.ToLower.Contains("seq_direction=decoy") Then
				fastaFileIsDecoy = True
			End If
		End If

        If Not String.IsNullOrEmpty(strMSGFDBParameterFilePath) Then
            Dim strTDASetting As String
            strTDASetting = GetSettingFromMSGFDbParamFile(strMSGFDBParameterFilePath, "TDA")

            Dim tdaValue As Integer
            If Not Integer.TryParse(strTDASetting, tdaValue) Then
                ReportError("TDA value is not numeric: " & strTDASetting)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If tdaValue = 0 Then
                If Not fastaFileIsDecoy AndAlso fastaFileSizeKB / 1024.0 / 1024.0 > 1 Then
                    ' Large Fasta file (over 1 GB in size)
                    ' TDA is 0, so we're performing a forward-only search
                    ' Auto-change fastaFileIsDecoy to True to prevent the reverse indices from being created

                    fastaFileIsDecoy = True
                    If m_DebugLevel >= 1 Then
                        ReportMessage("Processing large FASTA file with forward-only search; auto switching to -tda 0")
                    End If

                ElseIf strMSGFDBParameterFilePath.ToLower().EndsWith("_NoDecoy.txt".ToLower()) Then
                    ' Parameter file ends in _NoDecoy.txt and TDA = 0, thus we're performing a forward-only search
                    ' Auto-change fastaFileIsDecoy to True to prevent the reverse indices from being created

                    fastaFileIsDecoy = True
                    If m_DebugLevel >= 1 Then
                        ReportMessage("Using NoDecoy parameter file with TDA=0; auto switching to -tda 0")
                    End If

                End If
            End If
            
        End If

        If maxFastaFileSizeMB > 0 AndAlso fastaFileSizeKB / 1024.0 > maxFastaFileSizeMB Then
            ' Create a trimmed version of the fasta file
            ReportMessage("Fasta file is over " & maxFastaFileSizeMB & " MB; creating a trimmed version of the fasta file")

            Dim fastaFilePathTrimmed = String.Empty

            ' Allow for up to 3 attempts since multiple processes might potentially try to do this at the same time
            Dim trimIteration As Integer = 0

            While trimIteration <= 2
                trimIteration += 1
                fastaFilePathTrimmed = CreateTrimmedFasta(fastaFilePath, maxFastaFileSizeMB)

                If Not String.IsNullOrEmpty(fastaFilePathTrimmed) Then
                    Exit While
                End If

                If trimIteration <= 2 Then
                    Dim sleepTimeSec = oRand.Next(10, 19)

                    ReportMessage("Fasta file trimming failed; waiting " & sleepTimeSec & " seconds then trying again")
                    Threading.Thread.Sleep(sleepTimeSec * 1000)
                End If

            End While

            If String.IsNullOrEmpty(fastaFilePathTrimmed) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Update fastaFilePath to use the path to the trimmed version
            fastaFilePath = fastaFilePathTrimmed

            fiFastaFile.Refresh()
            fastaFileSizeKB = CSng(fiFastaFile.Length / 1024.0)

        End If

        If m_DebugLevel >= 3 OrElse (m_DebugLevel >= 1 And fastaFileSizeKB > 500) Then
            ReportMessage("Indexing Fasta file to create Suffix Array files")
        End If

        ' Look for the suffix array files that should exist for the fasta file
        ' Either copy them from Gigasax (or Proto-7) or re-create them
        ' 
        Dim indexIteration = 0
        Dim strMSGFPlusIndexFilesFolderPath As String = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPath", "\\gigasax\MSGFPlus_Index_Files")
        Dim strMSGFPlusIndexFilesFolderPathLegacyDB As String = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", "\\proto-7\MSGFPlus_Index_Files")

        While indexIteration <= 2

            indexIteration += 1

            ' Note that fastaFilePath will get updated by the IndexedDBCreator if we're running Legacy MSGFDB
            result = objIndexedDBCreator.CreateSuffixArrayFiles(
              m_WorkDir, m_DebugLevel, m_JobNum,
              javaProgLoc, msgfDbProgLoc,
              fastaFilePath, fastaFileIsDecoy,
              strMSGFPlusIndexFilesFolderPath,
              strMSGFPlusIndexFilesFolderPathLegacyDB,
              udtHPCOptions)

            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Exit While
            ElseIf result = IJobParams.CloseOutType.CLOSEOUT_FAILED OrElse (result <> IJobParams.CloseOutType.CLOSEOUT_FAILED And indexIteration > 2) Then

                If Not String.IsNullOrEmpty(objIndexedDBCreator.ErrorMessage) Then
                    ReportError(objIndexedDBCreator.ErrorMessage)
                Else
                    ReportError("Error creating Suffix Array files")
                End If
                Return result
            Else
                Dim sleepTimeSec = oRand.Next(10, 19)

                ReportMessage("Fasta file indexing failed; waiting " & sleepTimeSec & " seconds then trying again")
                Threading.Thread.Sleep(sleepTimeSec * 1000)
            End If

        End While

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Reads the contents of a _ScanType.txt file, returning the scan info using three generic dictionary objects
	''' </summary>
	''' <param name="strScanTypeFilePath"></param>
	''' <param name="lstLowResMSn">Low Res MSn spectra</param>
	''' <param name="lstHighResMSn">High Res MSn spectra (but not HCD)</param>
	''' <param name="lstHCDMSn">HCD Spectra</param>
	''' <param name="lstOther">Spectra that are not MSn</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function LoadScanTypeFile(ByVal strScanTypeFilePath As String,
	  ByRef lstLowResMSn As Dictionary(Of Integer, String),
	  ByRef lstHighResMSn As Dictionary(Of Integer, String),
	  ByRef lstHCDMSn As Dictionary(Of Integer, String),
	  ByRef lstOther As Dictionary(Of Integer, String)) As Boolean

		Dim strLineIn As String
		Dim intScanNumberColIndex As Integer = -1
		Dim intScanTypeNameColIndex As Integer = -1

		Try
			If Not File.Exists(strScanTypeFilePath) Then
				mErrorMessage = "ScanType file not found: " & strScanTypeFilePath
				Return False
			End If

			lstLowResMSn = New Dictionary(Of Integer, String)
			lstHighResMSn = New Dictionary(Of Integer, String)
			lstHCDMSn = New Dictionary(Of Integer, String)
			lstOther = New Dictionary(Of Integer, String)

			Using srScanTypeFile As StreamReader = New StreamReader(New FileStream(strScanTypeFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                While Not srScanTypeFile.EndOfStream
                    strLineIn = srScanTypeFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim lstColumns As List(Of String)
                        lstColumns = strLineIn.Split(ControlChars.Tab).ToList()

                        If intScanNumberColIndex < 0 Then
                            ' Parse the header line to define the mapping
                            ' Expected headers are ScanNumber   ScanTypeName   ScanType
                            intScanNumberColIndex = lstColumns.IndexOf("ScanNumber")
                            intScanTypeNameColIndex = lstColumns.IndexOf("ScanTypeName")

                        ElseIf intScanNumberColIndex >= 0 Then
                            Dim intScanNumber As Integer
                            Dim strScanType As String
                            Dim strScanTypeLCase As String

                            If Integer.TryParse(lstColumns(intScanNumberColIndex), intScanNumber) Then
                                If intScanTypeNameColIndex >= 0 Then
                                    strScanType = lstColumns(intScanTypeNameColIndex)
                                    strScanTypeLCase = strScanType.ToLower()

                                    If strScanTypeLCase.Contains("hcd") Then
                                        lstHCDMSn.Add(intScanNumber, strScanType)

                                    ElseIf strScanTypeLCase.Contains("hmsn") Then
                                        lstHighResMSn.Add(intScanNumber, strScanType)

                                    ElseIf strScanTypeLCase.Contains("msn") Then
                                        ' Not HCD and doesn't contain HMSn; assume low-res
                                        lstLowResMSn.Add(intScanNumber, strScanType)

                                    ElseIf strScanTypeLCase.Contains("cid") OrElse strScanTypeLCase.Contains("etd") Then
                                        ' The ScanTypeName likely came from the "Collision Mode" column of a MASIC ScanStatsEx file; we don't know if it is high res MSn or low res MSn
                                        ' This will be the case for MASIC results from prior to February 1, 2010, since those results did not have the ScanTypeName column in the _ScanStats.txt file
                                        ' We'll assume low res
                                        lstLowResMSn.Add(intScanNumber, strScanType)

                                    Else
                                        ' Does not contain MSn or HCD
                                        ' Likely SRM or MS1
                                        lstOther.Add(intScanNumber, strScanType)
                                    End If

                                End If
                            End If
                        End If

                    End If

                End While
			End Using

		Catch ex As Exception
			mErrorMessage = "Exception in LoadScanTypeFile"
			ReportError(mErrorMessage, mErrorMessage & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <returns>Percent Complete (value between 0 and 100)</returns>
	''' <remarks>MSGFDb version is available via the MSGFDbVersion property</remarks>
	Public Function ParseMSGFDBConsoleOutputFile() As Single
		Return ParseMSGFDBConsoleOutputFile(m_WorkDir)
	End Function

	''' <summary>
	''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
	''' </summary>
	''' <returns>Percent Complete (value between 0 and 100)</returns>
	''' <remarks>MSGFDb version is available via the MSGFDbVersion property</remarks>
	Public Function ParseMSGFDBConsoleOutputFile(ByVal workingDirectory As String) As Single

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

		Static reExtractThreadCount As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("Using (\d+) thread",
		  Text.RegularExpressions.RegexOptions.Compiled Or
		  Text.RegularExpressions.RegexOptions.IgnoreCase)

		Static reSpectraSearched As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("Spectrum.+\(total: *(\d+)\)",
		  Text.RegularExpressions.RegexOptions.Compiled Or
		  Text.RegularExpressions.RegexOptions.IgnoreCase)

		Dim strConsoleOutputFilePath As String = "??"

		Dim eThreadProgressBase() As eThreadProgressSteps
		Dim sngThreadProgressAddon() As Single
		Dim sngEffectiveProgress As Single

		Try
			' Initially reserve space for 32 threads
			' We'll expand these arrays later if needed
			ReDim eThreadProgressBase(32)
			ReDim sngThreadProgressAddon(32)

			strConsoleOutputFilePath = Path.Combine(workingDirectory, MSGFDB_CONSOLE_OUTPUT_FILE)
			If Not File.Exists(strConsoleOutputFilePath) Then
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

			Dim oMatch As Text.RegularExpressions.Match
			Dim intThreadCount As Short = 0

			sngEffectiveProgress = PROGRESS_PCT_MSGFDB_STARTING
			mContinuumSpectraSkipped = 0
			mSpectraSearched = 0

			Using srInFile As StreamReader = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				intLinesRead = 0
                Do While Not srInFile.EndOfStream
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
                                        mConsoleOutputErrorMsg = "Error running MSGFDB: "
                                    End If
                                    If Not mConsoleOutputErrorMsg.Contains(strLineIn) Then
                                        mConsoleOutputErrorMsg &= "; " & strLineIn
                                    End If
                                End If
                            End If
                        End If

                        ' Look for warning messages  
                        ' Additionally, update progress if the line starts with one of the expected phrases
                        If strLineIn.StartsWith("Ignoring spectrum") Then
                            ' Spectra are typically ignored either because they have too few ions, or because the data is not centroided
                            If strLineIn.IndexOf("spectrum is not centroided", StringComparison.CurrentCultureIgnoreCase) > 0 Then
                                mContinuumSpectraSkipped += 1
                            End If
                        ElseIf strLineIn.StartsWith("Loading database files") Then
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

                        ElseIf strLineIn.StartsWith("Spectrum") Then
                            ' Extract out the number of spectra that MSGF+ will actually search

                            oMatch = reSpectraSearched.Match(strLineIn)

                            If oMatch.Success Then
                                Integer.TryParse(oMatch.Groups(1).Value, mSpectraSearched)
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
							sngProgressOneThread = sngProgressOneThread
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

		Dim oMatch As Text.RegularExpressions.Match

		Static reExtractThreadNum As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("thread-(\d+)", _
		  Text.RegularExpressions.RegexOptions.Compiled Or _
		  Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static reExtractPctComplete As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("([0-9.]+)% complete", _
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
	  ByRef sbOptions As Text.StringBuilder, _
	  ByVal intNumMods As Integer, _
	  ByRef lstStaticMods As List(Of String), _
	  ByRef lstDynamicMods As List(Of String)) As Boolean

		Dim blnSuccess As Boolean
		Dim strModFilePath As String

		Try
			Dim fiParameterFile As FileInfo
			fiParameterFile = New FileInfo(strParameterFilePath)

			strModFilePath = Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME)

			' Note that ParseMSGFDbValidateMod will set this to True if a dynamic or static mod is STY phosphorylation 
			mPhosphorylationSearch = False

			sbOptions.Append(" -mod " & MOD_FILE_NAME)

			Using swModFile As StreamWriter = New StreamWriter(New FileStream(strModFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

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
	''' <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
	''' <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
	''' <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
	''' <param name="strScanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
	''' <param name="strInstrumentGroup">DMS Instrument Group name</param>
	''' <param name="strMSGFDbCmdLineOptions">Output: MSGFDb command line arguments</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
	Public Function ParseMSGFDBParameterFile(
	  ByVal fastaFileSizeKB As Single,
	  ByVal fastaFileIsDecoy As Boolean,
	  ByVal strAssumedScanType As String,
	  ByVal strScanTypeFilePath As String,
	  ByVal strInstrumentGroup As String,
	  ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
	  <Out()> ByRef strMSGFDbCmdLineOptions As String) As IJobParams.CloseOutType

		Dim strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

		Return ParseMSGFDBParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, strParameterFilePath, udtHPCOptions, strMSGFDbCmdLineOptions)
	End Function

	''' <summary>
	''' Read the MSGFDB options file and convert the options to command line switches
	''' </summary>
	''' <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
	''' <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
	''' <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
	''' <param name="strScanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
    ''' <param name="instrumentGroup">DMS Instrument Group name</param>
	''' <param name="strParameterFilePath">Full path to the MSGF+ parameter file to use</param>
	''' <param name="strMSGFDbCmdLineOptions">Output: MSGFDb command line arguments</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
    Public Function ParseMSGFDBParameterFile(
      ByVal fastaFileSizeKB As Single,
      ByVal fastaFileIsDecoy As Boolean,
      ByVal strAssumedScanType As String,
      ByVal strScanTypeFilePath As String,
      ByVal instrumentGroup As String,
      ByVal strParameterFilePath As String,
      ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
      <Out()> ByRef strMSGFDbCmdLineOptions As String) As IJobParams.CloseOutType

        Const SMALL_FASTA_FILE_THRESHOLD_KB As Integer = 20

        Dim strLineIn As String
        Dim sbOptions As Text.StringBuilder

        Dim kvSetting As KeyValuePair(Of String, String)
        Dim intValue As Integer

        Dim intParamFileThreadCount = 0
        Dim strDMSDefinedThreadCount As String
        Dim intDMSDefinedThreadCount = 0

        Dim intNumMods = 0
        Dim lstStaticMods = New List(Of String)
        Dim lstDynamicMods = New List(Of String)

        Dim blnShowDecoyParamPresent = False
        Dim blnShowDecoy = False
        Dim blnTDA = False

        Dim strSearchEngineName As String

        strMSGFDbCmdLineOptions = String.Empty

        If Not File.Exists(strParameterFilePath) Then
            ReportError("Parameter file not found", "Parameter file not found: " & strParameterFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        'Dim strDatasetType As String
        'Dim blnHCD As Boolean = False
        'strDatasetType = m_jobParams.GetParam("JobParameters", "DatasetType")
        'If strDatasetType.ToUpper().Contains("HCD") Then
        '	blnHCD = True
        'End If

        strSearchEngineName = GetSearchEngineName()

        sbOptions = New Text.StringBuilder(500)

        ' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
        ' Alternatively, if running MSGF+, this is set to true if TDA=1
        mResultsIncludeAutoAddedDecoyPeptides = False

        Try

            ' Initialize the Param Name dictionary
            Dim dctParamNames = GetMSFGDBParameterNames()

            Using srParamFile = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srParamFile.EndOfStream
                    strLineIn = srParamFile.ReadLine()

                    kvSetting = clsGlobal.GetKeyValueSetting(strLineIn)

                    If Not String.IsNullOrWhiteSpace(kvSetting.Key) Then

                        Dim strValue As String = kvSetting.Value

                        Dim strArgumentSwitch As String = String.Empty
                        Dim strArgumentSwitchOriginal As String

                        ' Check whether kvSetting.key is one of the standard keys defined in dctParamNames
                        If dctParamNames.TryGetValue(kvSetting.Key, strArgumentSwitch) Then

                            If clsGlobal.IsMatch(kvSetting.Key, MSGFDB_OPTION_FRAGMENTATION_METHOD) Then

                                If Not String.IsNullOrWhiteSpace(strScanTypeFilePath) Then
                                    ' Override FragmentationMethodID to always be 0
                                    strValue = "0"

                                ElseIf Not String.IsNullOrWhiteSpace(strAssumedScanType) Then
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

                            ElseIf clsGlobal.IsMatch(kvSetting.Key, MSGFDB_OPTION_INSTRUMENT_ID) Then

                                If Not String.IsNullOrWhiteSpace(strScanTypeFilePath) Then

                                    Dim eResult = DetermineInstrumentID(strValue, strScanTypeFilePath, instrumentGroup)
                                    If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                                        Return eResult
                                    End If

                                ElseIf Not String.IsNullOrWhiteSpace(instrumentGroup) Then
                                    Dim instrumentIDNew As String = String.Empty
                                    Dim autoSwitchReason As String = String.Empty

                                    If Not CanDetermineInstIdFromInstGroup(instrumentGroup, instrumentIDNew, autoSwitchReason) Then
                                        Dim datasetName = m_jobParams.GetParam("JobParameters", "DatasetNum")
                                        Dim countLowResMSn As Integer
                                        Dim countHighResMSn As Integer
                                        Dim countHCDMSn As Integer

                                        If LookupScanTypesForDataset(datasetName, countLowResMSn, countHighResMSn, countHCDMSn) Then
                                            ExamineScanTypes(countLowResMSn, countHighResMSn, countHCDMSn, instrumentIDNew, autoSwitchReason)
                                        End If

                                    End If

                                    AutoUpdateInstrumentIDIfChanged(strValue, instrumentIDNew, autoSwitchReason)

                                End If

                            End If

                            strArgumentSwitchOriginal = String.Copy(strArgumentSwitch)

                            AdjustSwitchesForMSGFPlus(mMSGFPlus, strArgumentSwitch, strValue)

                            If String.IsNullOrEmpty(strArgumentSwitch) Then
                                If m_DebugLevel >= 1 And Not clsGlobal.IsMatch(strArgumentSwitchOriginal, MSGFDB_OPTION_SHOWDECOY) Then
                                    ReportWarning("Skipping switch " & strArgumentSwitchOriginal & " since it is not valid for this version of " & strSearchEngineName)
                                End If
                            ElseIf String.IsNullOrEmpty(strValue) Then
                                If m_DebugLevel >= 1 Then
                                    ReportWarning("Skipping switch " & strArgumentSwitch & " since the value is empty")
                                End If
                            Else
                                sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)
                            End If


                            If clsGlobal.IsMatch(strArgumentSwitch, "showDecoy") Then
                                blnShowDecoyParamPresent = True
                                If Integer.TryParse(strValue, intValue) Then
                                    If intValue > 0 Then
                                        blnShowDecoy = True
                                    End If
                                End If
                            ElseIf clsGlobal.IsMatch(strArgumentSwitch, "tda") Then
                                If Integer.TryParse(strValue, intValue) Then
                                    If intValue > 0 Then
                                        blnTDA = True
                                    End If
                                End If
                            End If

                        ElseIf clsGlobal.IsMatch(kvSetting.Key, "uniformAAProb") Then

                            If mMSGFPlus Then
                                ' Not valid for MSGF+; skip it
                            Else

                                If String.IsNullOrWhiteSpace(strValue) OrElse clsGlobal.IsMatch(strValue, "auto") Then
                                    If fastaFileSizeKB < SMALL_FASTA_FILE_THRESHOLD_KB Then
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

                        ElseIf clsGlobal.IsMatch(kvSetting.Key, "NumThreads") Then
                            If String.IsNullOrWhiteSpace(strValue) OrElse clsGlobal.IsMatch(strValue, "all") Then
                                ' Do not append -thread to the command line; MSGFDB will use all available cores by default
                            Else
                                If Integer.TryParse(strValue, intParamFileThreadCount) Then
                                    ' intParamFileThreadCount now has the thread count
                                Else
                                    ReportWarning("Invalid value for NumThreads in MSGFDB parameter file: " & strLineIn)
                                End If
                            End If


                        ElseIf clsGlobal.IsMatch(kvSetting.Key, "NumMods") Then
                            If Integer.TryParse(strValue, intValue) Then
                                intNumMods = intValue
                            Else
                                mErrorMessage = "Invalid value for NumMods in MSGFDB parameter file"
                                ReportError(mErrorMessage, mErrorMessage & ": " & strLineIn)
                                srParamFile.Close()
                                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                            End If

                        ElseIf clsGlobal.IsMatch(kvSetting.Key, "StaticMod") Then
                            If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not clsGlobal.IsMatch(strValue, "none") Then
                                lstStaticMods.Add(strValue)
                            End If

                        ElseIf clsGlobal.IsMatch(kvSetting.Key, "DynamicMod") Then
                            If Not String.IsNullOrWhiteSpace(strValue) AndAlso Not clsGlobal.IsMatch(strValue, "none") Then
                                lstDynamicMods.Add(strValue)
                            End If
                        End If

                        'If clsGlobal.IsMatch(kvSetting.Key, MSGFDB_OPTION_FRAGMENTATION_METHOD) Then
                        '	If Integer.TryParse(strValue, intValue) Then
                        '		If intValue = 3 Then
                        '			blnHCD = True
                        '		End If
                        '	End If
                        'End If

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
        If String.IsNullOrWhiteSpace(strDMSDefinedThreadCount) OrElse
           strDMSDefinedThreadCount.ToLower() = "all" OrElse
           Not Integer.TryParse(strDMSDefinedThreadCount, intDMSDefinedThreadCount) Then
            intDMSDefinedThreadCount = 0
        End If

        If intDMSDefinedThreadCount > 0 Then
            intParamFileThreadCount = intDMSDefinedThreadCount
        End If

        Dim limitCoreUsage = False

        If Dns.GetHostName.ToLower().StartsWith("proto-") Then
            ' Running on a Proto storage server (e.g. Proto-4, Proto-5, or Proto-11)
            ' Limit the number of cores used to 75% of the total core count
            limitCoreUsage = True
        End If

        If udtHPCOptions.UsingHPC Then
            ' Do not define the thread count when running on HPC; MSGF+ should use all 16 cores (or all 32 cores)
            If intParamFileThreadCount > 0 Then intParamFileThreadCount = 0

        ElseIf intParamFileThreadCount <= 0 OrElse limitCoreUsage Then
            ' Set intParamFileThreadCount to the number of cores on this computer
            ' Note that Environment.ProcessorCount tells us the number of logical processors, not the number of cores
            ' Thus, we need to use a WMI query (see http://stackoverflow.com/questions/1542213/how-to-find-the-number-of-cpu-cores-via-net-c )

            Dim coreCount = 0
            For Each item As Management.ManagementBaseObject In New Management.ManagementObjectSearcher("Select * from Win32_Processor").Get()
                coreCount += Integer.Parse(item("NumberOfCores").ToString())
            Next

            If limitCoreUsage Then
                Dim maxAllowedCores = CInt(Math.Floor(coreCount * 0.75))
                If intParamFileThreadCount > 0 AndAlso intParamFileThreadCount < maxAllowedCores Then
                    ' Leave intParamFileThreadCount unchanged
                Else
                    intParamFileThreadCount = maxAllowedCores
                End If
            Else
                ' Prior to July 2014 we would use "coreCount - 1" when the computer had more than 4 cores because MSGF+ would actually use intParamFileThreadCount+1 cores
                ' Starting with version v10072 it now uses intParamFileThreadCount cores as instructed
                intParamFileThreadCount = coreCount
            End If

        End If

        If intParamFileThreadCount > 0 Then
            sbOptions.Append(" -thread " & intParamFileThreadCount)
        End If

        ' Create the modification file and append the -mod switch
        ' We'll also set mPhosphorylationSearch to True if a dynamic or static mod is STY phosphorylation 
        If Not ParseMSGFDBModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Prior to MSGF+ version v9284 we used " -protocol 1" at the command line when performing an HCD-based phosphorylation search
        ' However, v9284 now auto-selects the correct protocol based on the spectrum type and the dynamic modifications
        ' Options for -protocol are 0=NoProtocol (Default), 1=Phosphorylation, 2=iTRAQ, 3=iTRAQPhospho
        '
        ' As of March 23, 2015, if the user is searching for Phospho mods with TMT labeling enabled, 
        ' then MSGF+ will use a model trained for TMT peptides (without phospho)
        ' In this case, the user should probably use a parameter file with Protocol=1 defined (which leads to sbOptions having "-protocol 1")
       

        strMSGFDbCmdLineOptions = sbOptions.ToString()

        ' By default, MSGF+ filters out spectra with fewer than 20 data points
        ' Override this threshold to 5 data points
        If strMSGFDbCmdLineOptions.IndexOf("-minNumPeaks", StringComparison.CurrentCultureIgnoreCase) < 0 Then
            strMSGFDbCmdLineOptions &= " -minNumPeaks 5"
        End If

        If strMSGFDbCmdLineOptions.Contains("-tda 1") Then
            ' Make sure the .Fasta file is not a Decoy fasta
            If fastaFileIsDecoy Then
                ReportError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS


    End Function

	Private Function DetermineInstrumentID(ByRef instrumentIDCurrent As String, ByVal scanTypeFilePath As String, ByVal instrumentGroup As String) As IJobParams.CloseOutType

		' Override Instrument ID based on the instrument class and scan types in the _ScanType file

		' #  0 means Low-res LCQ/LTQ (Default for CID and ETD); use InstrumentID=0 if analyzing a dataset with low-res CID and high-res HCD spectra
		' #  1 means High-res LTQ (Default for HCD; also appropriate for high res CID).  Do not merge spectra (FragMethod=4) when InstrumentID is 1; scores will degrade
		' #  2 means TOF
		' #  3 means Q-Exactive

		If String.IsNullOrEmpty(instrumentGroup) Then instrumentGroup = "#Undefined#"

		Dim instrumentIDNew As String = String.Empty
		Dim autoSwitchReason As String = String.Empty

		If Not CanDetermineInstIdFromInstGroup(instrumentGroup, instrumentIDNew, autoSwitchReason) Then

			' Instrument ID is not obvious from the instrument group
			' Examine the scan types in scanTypeFilePath

			' If low res MS1,  then Instrument Group is typically LCQ, LTQ, LTQ-ETD, LTQ-Prep, VelosPro

			' If high res MS2, then Instrument Group is typically VelosOrbi, or LTQ_FT

			' Count the number of High res CID or ETD spectra
			' Count HCD spectra separately since MSGF+ has a special scoring model for HCD spectra

			Dim lstLowResMSn As Dictionary(Of Integer, String) = Nothing
			Dim lstHighResMSn As Dictionary(Of Integer, String) = Nothing
			Dim lstHCDMSn As Dictionary(Of Integer, String) = Nothing
			Dim lstOther As Dictionary(Of Integer, String) = Nothing
			Dim blnSuccess As Boolean

			blnSuccess = LoadScanTypeFile(scanTypeFilePath, lstLowResMSn, lstHighResMSn, lstHCDMSn, lstOther)

			If Not blnSuccess Then
				If String.IsNullOrEmpty(mErrorMessage) Then
					mErrorMessage = "LoadScanTypeFile returned false for " & Path.GetFileName(scanTypeFilePath)
					ReportError(mErrorMessage)
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			ElseIf lstLowResMSn.Count + lstHighResMSn.Count + lstHCDMSn.Count = 0 Then
				mErrorMessage = "LoadScanTypeFile could not find any MSn spectra " & Path.GetFileName(scanTypeFilePath)
				ReportError(mErrorMessage)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				ExamineScanTypes(lstLowResMSn.Count, lstHighResMSn.Count, lstHCDMSn.Count, instrumentIDNew, autoSwitchReason)
			End If

		End If

		AutoUpdateInstrumentIDIfChanged(instrumentIDCurrent, instrumentIDNew, autoSwitchReason)

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

    Private Sub ExamineScanTypes(
      ByVal countLowResMSn As Integer,
      ByVal countHighResMSn As Integer,
      ByVal countHCDMSn As Integer,
      <Out> ByRef instrumentIDNew As String,
      <Out> ByRef autoSwitchReason As String)

        instrumentIDNew = String.Empty
        autoSwitchReason = String.Empty

        If countLowResMSn + countHighResMSn + countHCDMSn = 0 Then
            ' Scan counts are all 0; leave instrumentIDNew untouched
            ReportMessage("Scan counts provided to ExamineScanTypes are all 0; cannot auto-update InstrumentID")
        Else
            Dim dblFractionHiRes As Double = 0

            If countHighResMSn > 0 Then
                dblFractionHiRes = countHighResMSn / (countLowResMSn + countHighResMSn)
            End If

            If dblFractionHiRes > 0.1 Then
                ' At least 10% of the spectra are HMSn
                instrumentIDNew = "1"
                autoSwitchReason = "since " & (dblFractionHiRes * 100).ToString("0") & "% of the spectra are HMSn"

            Else
                If countLowResMSn = 0 And countHCDMSn > 0 Then
                    ' All of the spectra are HCD
                    instrumentIDNew = "1"
                    autoSwitchReason = "since all of the spectra are HCD"
                Else
                    instrumentIDNew = "0"
                    If countHCDMSn = 0 And countHighResMSn = 0 Then
                        autoSwitchReason = "since all of the spectra are low res MSn"
                    Else
                        autoSwitchReason = "since there is a mix of low res and high res spectra"
                    End If
                End If

            End If

        End If

    End Sub

    Protected Function LookupScanTypesForDataset(
      ByVal datasetName As String,
      <Out> ByRef countLowResMSn As Integer,
      <Out> ByRef countHighResMSn As Integer,
      <Out> ByRef countHCDMSn As Integer) As Boolean

        countLowResMSn = 0
        countHighResMSn = 0
        countHCDMSn = 0

        Try

            If String.IsNullOrEmpty(datasetName) Then
                Return False
            End If

            Dim connectionString As String = m_mgrParams.GetParam("connectionstring")

            Dim sqlStr As Text.StringBuilder = New Text.StringBuilder

            sqlStr.Append(" SELECT HMS, MS, [CID-HMSn], [CID-MSn], ")
            sqlStr.Append("   [HCD-HMSn], [ETD-HMSn], [ETD-MSn], ")
            sqlStr.Append("   [SA_ETD-HMSn], [SA_ETD-MSn], ")
            sqlStr.Append("   HMSn, MSn, ")
            sqlStr.Append("   [PQD-HMSn], [PQD-MSn]")
            sqlStr.Append(" FROM V_Dataset_ScanType_CrossTab")
            sqlStr.Append(" WHERE Dataset = '" & datasetName & "'")

            Dim dtResults As DataTable = Nothing
            Const retryCount = 2

            'Get a table to hold the results of the query
            Dim blnSuccess = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LookupScanTypesForDataset", retryCount, dtResults)

            If Not blnSuccess Then
                ReportError("Excessive failures attempting to retrieve dataset scan types in LookupScanTypesForDataset")
                dtResults.Dispose()
                Return False
            End If

            'Verify at least one row returned
            If dtResults.Rows.Count < 1 Then
                ' No data was returned
                ReportMessage("No rows were returned for dataset " & datasetName & " from V_Dataset_ScanType_CrossTab in DMS")
                Return False
            Else
                For Each curRow As DataRow In dtResults.Rows

                    countLowResMSn += clsGlobal.DbCInt(curRow("CID-MSn"))
                    countHighResMSn += clsGlobal.DbCInt(curRow("CID-HMSn"))
                    countHCDMSn += clsGlobal.DbCInt(curRow("HCD-HMSn"))

                    countHighResMSn += clsGlobal.DbCInt(curRow("ETD-HMSn"))
                    countLowResMSn += clsGlobal.DbCInt(curRow("ETD-MSn"))

                    countHighResMSn += clsGlobal.DbCInt(curRow("SA_ETD-HMSn"))
                    countLowResMSn += clsGlobal.DbCInt(curRow("SA_ETD-MSn"))

                    countHighResMSn += clsGlobal.DbCInt(curRow("HMSn"))
                    countLowResMSn += clsGlobal.DbCInt(curRow("MSn"))

                    countHighResMSn += clsGlobal.DbCInt(curRow("PQD-HMSn"))
                    countLowResMSn += clsGlobal.DbCInt(curRow("PQD-MSn"))

                Next

                dtResults.Dispose()
                Return True
            End If

        Catch ex As Exception
            Const msg = "Exception in LookupScanTypersForDataset"
            ReportError(msg, msg & ": " & ex.Message)
            Return False
        End Try

    End Function

	''' <summary>
	''' Validates that the modification definition text
	''' </summary>
	''' <param name="strMod">Modification definition</param>
	''' <param name="strModClean">Cleaned-up modification definition (output param)</param>
	''' <returns>True if valid; false if invalid</returns>
	''' <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
	Protected Function ParseMSGFDbValidateMod(ByVal strMod As String, <Out()> ByRef strModClean As String) As Boolean

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
	Protected Function ParseMSFDBParamLine(ByRef sbOptions As Text.StringBuilder, _
	   ByVal strKeyName As String, _
	   ByVal strValue As String, _
	   ByVal strParameterName As String) As Boolean

		Dim strCommandLineSwitchName As String = strParameterName

		Return ParseMSFDBParamLine(sbOptions, strKeyName, strValue, strParameterName, strCommandLineSwitchName)

	End Function

	Protected Function ParseMSFDBParamLine(ByRef sbOptions As Text.StringBuilder, _
	   ByVal strKeyName As String, _
	   ByVal strValue As String, _
	   ByVal strParameterName As String, _
	   ByVal strCommandLineSwitchName As String) As Boolean

		If clsGlobal.IsMatch(strKeyName, strParameterName) Then
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

	Public Shared Function UseLegacyMSGFDB(jobParams As IJobParams) As Boolean

		Return False

		'Dim strValue As String

		'' Default to using MSGF+
		'Dim blnUseLegacyMSGFDB As Boolean = False

		'strValue = jobParams.GetJobParameter("UseLegacyMSGFDB", String.Empty)
		'If Not String.IsNullOrEmpty(strValue) Then
		'	If Not Boolean.TryParse(strValue, blnUseLegacyMSGFDB) Then
		'		' Error parsing strValue; not boolean
		'		strValue = String.Empty
		'	End If
		'End If

		'If String.IsNullOrEmpty(strValue) Then
		'	strValue = jobParams.GetJobParameter("UseMSGFPlus", String.Empty)

		'	If Not String.IsNullOrEmpty(strValue) Then
		'		Dim blnUseMSGFPlus As Boolean
		'		If Boolean.TryParse(strValue, blnUseMSGFPlus) Then
		'			strValue = "False"
		'			blnUseLegacyMSGFDB = False
		'		Else
		'			strValue = String.Empty
		'		End If
		'	End If

		'	If String.IsNullOrEmpty(strValue) Then
		'		' Default to using MSGF+
		'		blnUseLegacyMSGFDB = False
		'	End If
		'End If

		'Return blnUseLegacyMSGFDB

	End Function

	Private Function ValidatePeptideToProteinMapResults(ByVal strPeptideToProteinMapFilePath As String, ByVal blnIgnorePeptideToProteinMapperErrors As Boolean) As Boolean

		Const PROTEIN_NAME_NO_MATCH As String = "__NoMatch__"

		Dim blnSuccess As Boolean

		Dim intPeptideCount As Integer = 0
		Dim intPeptideCountNoMatch As Integer = 0
		Dim intLinesRead As Integer = 0

		Try
			' Validate that none of the results in strPeptideToProteinMapFilePath has protein name PROTEIN_NAME_NO_MATCH

			Dim strLineIn As String

			If m_DebugLevel >= 2 Then
				ReportMessage("Validating peptide to protein mapping, file " & Path.GetFileName(strPeptideToProteinMapFilePath))
			End If

			Using srInFile As StreamReader = New StreamReader(New FileStream(strPeptideToProteinMapFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srInFile.EndOfStream
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
				ReportError(mErrorMessage, mErrorMessage & ", file " & Path.GetFileName(strPeptideToProteinMapFilePath))
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

	Private Sub WriteProteinSequence(ByRef swOutFile As StreamWriter, ByVal strSequence As String)
		Dim intIndex As Integer = 0
		Dim intLength As Integer

		Do While intIndex < strSequence.Length
			intLength = Math.Min(60, strSequence.Length - intIndex)
			swOutFile.WriteLine(strSequence.Substring(intIndex, intLength))
			intIndex += 60
		Loop

	End Sub


	''' <summary>
	''' Zips MSGFDB Output File (creating a .gz file)
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function ZipOutputFile(ByRef oToolRunner As clsAnalysisToolRunnerBase, ByVal FileName As String) As IJobParams.CloseOutType
		Dim TmpFilePath As String

		Try

			TmpFilePath = Path.Combine(m_WorkDir, FileName)
			If Not File.Exists(TmpFilePath) Then
				ReportError("MSGF+ results file not found: " & FileName)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
			End If

			If Not oToolRunner.GZipFile(TmpFilePath, False) Then
				Const Msg As String = "Error zipping output files"
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
		Static dtLastLogTime As DateTime = DateTime.UtcNow

		If m_DebugLevel >= 1 Then
			If DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS Then
				dtLastLogTime = DateTime.UtcNow
				ReportMessage("Mapping peptides to proteins: " & percentComplete.ToString("0.0") & "% complete")
			End If
		End If

	End Sub
#End Region

End Class
