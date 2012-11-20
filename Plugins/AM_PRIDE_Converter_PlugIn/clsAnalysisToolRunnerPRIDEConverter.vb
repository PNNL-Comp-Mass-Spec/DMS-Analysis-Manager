Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader

Public Class clsAnalysisToolRunnerPRIDEConverter
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running PRIDEConverter
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const PRIDEConverter_CONSOLE_OUTPUT As String = "PRIDEConverter_ConsoleOutput.txt"
	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_CREATING_MISSING_MZXML_FILES As Single = 5
	Protected Const PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES As Single = 15
	Protected Const PROGRESS_PCT_CREATING_PRIDE_XML_FILES As Single = 25
	Protected Const PROGRESS_PCT_SAVING_RESULTS As Single = 95
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const FILE_EXTENSION_PSEUDO_MSGF As String = ".msgf"
	Protected Const FILE_EXTENSION_MSGF_REPORT_XML As String = ".msgf-report.xml"
	Protected Const FILE_EXTENSION_MSGF_PRIDE_XML As String = ".msgf-pride.xml"

	Protected mConsoleOutputErrorMsg As String

	' This dictionary tracks the peptide hit jobs defined for this data package
	' The keys are job numbers and the values contains job info
	Protected mDataPackagePeptideHitJobs As Generic.Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)

	Protected mToolVersionWritten As Boolean
	Protected mPrideConverterVersion As String = String.Empty
	Protected mPrideConverterProgLoc As String = String.Empty

	Protected mJavaProgLoc As String = String.Empty
	Protected mMSXmlGeneratorAppPath As String = String.Empty

	Private WithEvents mMSXmlCreator As AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures and Enums"
	Protected Structure udtFilterThresholdsType
		Public FDRThreshold As Single
		Public PepFDRThreshold As Single
		Public MSGFSpecProbThresold As Single
		Public UseFDRThreshold As Boolean
		Public UsePepFDRThreshold As Boolean
		Public UseMSGFSpecProb As Boolean
		Public Sub Clear()
			UseFDRThreshold = False
			UsePepFDRThreshold = False
			UseMSGFSpecProb = True
			FDRThreshold = 0.01
			PepFDRThreshold = 0.01
			MSGFSpecProbThresold = 0.000000001
		End Sub
	End Structure

	Protected Structure udtPseudoMSGFDataType
		Public ResultID As Integer
		Public Peptide As String
		Public CleanSequence As String
		Public PrefixResidue As String
		Public SuffixResidue As String
		Public ScanNumber As Integer
		Public ChargeState As Short
		Public PValue As String
		Public MQScore As String
		Public TotalPRMScore As String
		Public NTT As Short
		Public MSGFSpecProb As String
		Public DeltaScore As String
		Public DeltaScoreOther As String
	End Structure

	Protected Enum eMSGFReportXMLFileLocation
		Header = 0
		SearchResultIdentifier = 1
		Metadata = 2
		Protocol = 3
		MzDataAdmin = 4
		MzDataInstrument = 5
		MzDataDataProcessing = 6
		ExperimentAdditional = 7
		Identifications = 8
		PTMs = 9
		DatabaseMappings = 10
		ConfigurationOptions = 11
	End Enum

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

			' Initialize mDataPackagePeptideHitJobs			
			If Not LookupDataPackagePeptideHitJobs() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Check whether we are only creating the .msgf files
			Dim blnCreateMSGFReportFilesOnly As Boolean = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", False)

			' Create .mzXML files for any jobs in lstDataPackagePeptideHitJobs for which the .mzXML file wasn't retrieved
			If Not blnCreateMSGFReportFilesOnly AndAlso Not CreateMissingMzXMLFiles() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			dctPrideReportFiles = New Generic.Dictionary(Of Integer, String)

			' Create the .msgf-report.xml file for each job
			' This function will populate dctPrideReportFiles and mJobToDatasetMap
			blnSuccess = CreateMSGFReportFiles(dctPrideReportFiles, blnCreateMSGFReportFilesOnly)

			If blnSuccess AndAlso Not blnCreateMSGFReportFilesOnly Then
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

			m_jobParams.AddResultFileExtensionToSkip(".msgf")

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

			m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt")

			mMSXmlCreator = New AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams)

			For Each strDataset As String In lstDatasets

				mMSXmlCreator.UpdateDatasetName(strDataset)

				blnSuccess = mMSXmlCreator.CreateMZXMLFile()

				If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
					m_message = mMSXmlCreator.ErrorMessage
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error creating the mzXML file"
					End If
				End If

				If Not blnSuccess Then Exit For

				intDatasetsProcessed += 1

				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_MISSING_MZXML_FILES, PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES, intDatasetsProcessed, lstDatasets.Count)
				m_StatusTools.UpdateAndWrite(m_progress)
			Next

		Catch ex As Exception
			m_message = "Exception in CreateMissingMzXMLFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function CreatePseudoMSGFFileUsingPHRPReader( _
	  ByVal intJob As Integer, _
	  ByVal strDataset As String, _
	  ByVal udtFilterThresholds As udtFilterThresholdsType, _
	  ByRef lstPseudoMSGFData As Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType)), _
	  ByRef dctProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String)), _
	  ByRef dctProteinPSMCounts As Generic.Dictionary(Of Integer, Integer)) As String

		Const MSGF_SPECPROB_NOTDEFINED As Integer = 10

		Dim lstScansWritten As Generic.SortedSet(Of Integer)
		Dim udtJobInfo As clsAnalysisResources.udtDataPackageJobInfoType = New clsAnalysisResources.udtDataPackageJobInfoType

		Dim strMzXMLFilename As String

		Dim strSynopsisFileName As String
		Dim strSynopsisFilePath As String
		Dim strSynopsisFilePathAlt As String

		Dim strPseudoMsgfFilePath As String = String.Empty

		Dim strTotalPRMScore As String
		Dim strPValue As String
		Dim strDeltaScore As String
		Dim strDeltaScoreOther As String

		Dim dblMSGFSpecProb As Double
		Dim dblFDR As Double
		Dim dblPepFDR As Double

		Dim blnValidPSM As Boolean
		Dim blnThresholdChecked As Boolean

		Dim blnFDRValuesArePresent As Boolean = False
		Dim blnPepFDRValuesArePresent As Boolean = False

		Try

			If Not mDataPackagePeptideHitJobs.TryGetValue(intJob, udtJobInfo) Then
				m_message = "Job " & intJob & " not found in mDataPackagePeptideHitJobs; this is unexpected"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return String.Empty
			End If

			If lstPseudoMSGFData.Count > 0 Then
				lstPseudoMSGFData.Clear()
			End If

			lstScansWritten = New Generic.SortedSet(Of Integer)

			strMzXMLFilename = strDataset & ".mzXML"

			' Determine the correct capitalization for the mzXML file
			Dim diWorkdir As System.IO.DirectoryInfo = New System.IO.DirectoryInfo(m_WorkDir)
			Dim fiFiles() As System.IO.FileInfo = diWorkdir.GetFiles(strMzXMLFilename)

			If fiFiles.Length > 0 Then
				strMzXMLFilename = fiFiles(0).Name
			Else
				' mzXML file not found; don't worry about this right now (it's possible that CreateMSGFReportFilesOnly = True)
			End If

			strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset)

			strSynopsisFilePath = IO.Path.Combine(m_WorkDir, strSynopsisFileName)

			' Check whether PHRP files with a prefix of "Job12345_" exist
			' This prefix is added by RetrieveDataPackagePeptideHitJobPHRPFiles if multiple peptide_hit jobs are included for the same dataset
			strSynopsisFilePathAlt = IO.Path.Combine(m_WorkDir, "Job" & udtJobInfo.Job & "_" & strSynopsisFileName)

			If System.IO.File.Exists(strSynopsisFilePathAlt) Then
				strSynopsisFilePath = String.Copy(strSynopsisFilePathAlt)
			End If

			Using objReader As clsPHRPReader = New clsPHRPReader(strSynopsisFilePath, udtJobInfo.PeptideHitResultType, True, True)

				objReader.SkipDuplicatePSMs = False

				strPseudoMsgfFilePath = IO.Path.Combine(m_WorkDir, udtJobInfo.Dataset & "_Job" & udtJobInfo.Job & FILE_EXTENSION_PSEUDO_MSGF)
				Using swMSGFFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strPseudoMsgfFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					' Write the header line
					swMSGFFile.WriteLine( _
					 "#SpectrumFile" & ControlChars.Tab & _
					 "Scan#" & ControlChars.Tab & _
					 "Annotation" & ControlChars.Tab & _
					 "Protein" & ControlChars.Tab & _
					 "Charge" & ControlChars.Tab & _
					 "MQScore" & ControlChars.Tab & _
					 "Length" & ControlChars.Tab & _
					 "TotalPRMScore" & ControlChars.Tab & _
					 "MedianPRMScore" & ControlChars.Tab & _
					 "FractionY" & ControlChars.Tab & _
					 "FractionB" & ControlChars.Tab & _
					 "Intensity" & ControlChars.Tab & _
					 "NTT" & ControlChars.Tab & _
					 "p-value" & ControlChars.Tab & _
					 "F-Score" & ControlChars.Tab & _
					 "DeltaScore" & ControlChars.Tab & _
					 "DeltaScoreOther" & ControlChars.Tab & _
					 "RecordNumber" & ControlChars.Tab & _
					 "DBFilePos" & ControlChars.Tab & _
					 "SpecFilePos" & ControlChars.Tab & _
					 "SpecProb"
					 )

					' Write each data line, filtering on either PepFDR or FDR if defined, or MSGF_SpecProb if PepFDR and/or FDR are not available
					While objReader.MoveNext()

						blnValidPSM = True
						blnThresholdChecked = False

						' Determine MSGFSpecProb; store 10 if we don't find a valid number
						If Not Double.TryParse(objReader.CurrentPSM.MSGFSpecProb, dblMSGFSpecProb) Then
							dblMSGFSpecProb = MSGF_SPECPROB_NOTDEFINED
						End If

						dblFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1)
						If dblFDR > -1 Then
							blnFDRValuesArePresent = True
						End If

						dblPepFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PepFDR, -1)
						If dblPepFDR > -1 Then
							blnPepFDRValuesArePresent = True
						End If

						If udtFilterThresholds.UseMSGFSpecProb Then
							If dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThresold Then
								blnValidPSM = False
							End If
							blnThresholdChecked = True
						End If

						If blnPepFDRValuesArePresent AndAlso udtFilterThresholds.UsePepFDRThreshold Then
							' Typically only MSGFDB results will have PepFDR values
							If dblPepFDR > udtFilterThresholds.PepFDRThreshold Then
								blnValidPSM = False
							End If
							blnThresholdChecked = True
						End If

						If blnFDRValuesArePresent AndAlso udtFilterThresholds.UseFDRThreshold Then
							' Typically only MSGFDB results will have FDR values
							If dblFDR > udtFilterThresholds.FDRThreshold Then
								blnValidPSM = False
							End If
							blnThresholdChecked = True
						End If

						If blnValidPSM And Not blnThresholdChecked Then
							' Switch to filtering on MSGFSpecProbThresold instead of on FDR or PepFDR
							If dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED AndAlso udtFilterThresholds.MSGFSpecProbThresold < 0.0001 Then
								If dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThresold Then
									blnValidPSM = False
								End If
							End If
						End If

						If blnValidPSM Then

							Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String) = Nothing

							If Not dctProteins.TryGetValue(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence) Then

								' Protein not found in dctProteinPSMCounts
								' If the search engine is MSGFDB and the protein name starts with REV_ or XXX_ then skip this protein since it's a decoy result
								' Otherwise, add the protein to dctProteins and dctProteinPSMCounts, though we won't know its sequence

								Dim strProteinUCase As String = objReader.CurrentPSM.ProteinFirst.ToUpper()

								If udtJobInfo.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
									If strProteinUCase.StartsWith("REV_") OrElse strProteinUCase.StartsWith("XXX_") Then
										blnValidPSM = False
									End If
								Else
									If strProteinUCase.StartsWith("REVERSED_") OrElse strProteinUCase.StartsWith("SCRAMBLED_") OrElse strProteinUCase.StartsWith("XXX.") Then
										blnValidPSM = False
									End If
								End If

								If blnValidPSM Then
									kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(dctProteins.Count, String.Empty)
									dctProteinPSMCounts.Add(kvIndexAndSequence.Key, 0)
									dctProteins.Add(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence)
								End If

							End If

						End If

						If blnValidPSM Then

							' These fields are used to hold different scores depending on the search engine
							strTotalPRMScore = "0"
							strPValue = "0"
							strDeltaScore = "0"
							strDeltaScoreOther = "0"

							Select Case udtJobInfo.PeptideHitResultType
								Case clsPHRPReader.ePeptideHitResultType.Sequest
									strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_Sp)
									strPValue = objReader.CurrentPSM.MSGFSpecProb
									strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn)
									strDeltaScoreOther = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn2)

								Case clsPHRPReader.ePeptideHitResultType.MSGFDB
									strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_DeNovoScore)
									strPValue = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_PValue)
							End Select

							' The .MSGF file can only contain one match for each scan number
							' If it includes multiple matches, then PRIDE Converter crashes when reading the .mzXML file
							' In contrast, the .msgf-report.xml file _can_ contain multiple matches for the same scan

							If lstScansWritten.Contains(objReader.CurrentPSM.ScanNumber) Then
								Console.WriteLine("Skipping ResultID " & objReader.CurrentPSM.ResultID & " since previous result already stored for scan " & objReader.CurrentPSM.ScanNumber)
							Else


								swMSGFFile.WriteLine( _
								 strMzXMLFilename & ControlChars.Tab & _
								 objReader.CurrentPSM.ScanNumber & ControlChars.Tab & _
								 objReader.CurrentPSM.Peptide & ControlChars.Tab & _
								 objReader.CurrentPSM.ProteinFirst & ControlChars.Tab & _
								 objReader.CurrentPSM.Charge & ControlChars.Tab & _
								 objReader.CurrentPSM.MSGFSpecProb & ControlChars.Tab & _
								 objReader.CurrentPSM.PeptideCleanSequence.Length() & ControlChars.Tab & _
								 strTotalPRMScore & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 objReader.CurrentPSM.NumTrypticTerminii & ControlChars.Tab & _
								 strPValue & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 strDeltaScore & ControlChars.Tab & _
								 strDeltaScoreOther & ControlChars.Tab & _
								 objReader.CurrentPSM.ResultID & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 "0" & ControlChars.Tab & _
								 objReader.CurrentPSM.MSGFSpecProb
								 )


								Dim strPrefix As String = String.Empty
								Dim strSuffix As String = String.Empty
								Dim strPrimarySequence As String = String.Empty

								If Not clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(objReader.CurrentPSM.Peptide, strPrimarySequence, strPrefix, strSuffix) Then
									strPrefix = String.Empty
									strSuffix = String.Empty
								End If

								Dim udtPseudoMSGFData As udtPseudoMSGFDataType = New udtPseudoMSGFDataType
								With udtPseudoMSGFData
									.ResultID = objReader.CurrentPSM.ResultID
									.Peptide = String.Copy(objReader.CurrentPSM.Peptide)
									.CleanSequence = String.Copy(objReader.CurrentPSM.PeptideCleanSequence)
									.PrefixResidue = String.Copy(strPrefix)
									.SuffixResidue = String.Copy(strSuffix)
									.ScanNumber = objReader.CurrentPSM.ScanNumber
									.ChargeState = objReader.CurrentPSM.Charge
									.PValue = String.Copy(strPValue)
									.MQScore = String.Copy(objReader.CurrentPSM.MSGFSpecProb)
									.TotalPRMScore = String.Copy(strTotalPRMScore)
									.NTT = objReader.CurrentPSM.NumTrypticTerminii
									.MSGFSpecProb = String.Copy(objReader.CurrentPSM.MSGFSpecProb)
									.DeltaScore = String.Copy(strDeltaScore)
									.DeltaScoreOther = String.Copy(strDeltaScoreOther)
								End With

								Dim lstMatchesForProtein As Generic.List(Of udtPseudoMSGFDataType) = Nothing
								If lstPseudoMSGFData.TryGetValue(objReader.CurrentPSM.ProteinFirst, lstMatchesForProtein) Then
									lstMatchesForProtein.Add(udtPseudoMSGFData)
								Else
									lstMatchesForProtein = New Generic.List(Of udtPseudoMSGFDataType)
									lstMatchesForProtein.Add(udtPseudoMSGFData)
									lstPseudoMSGFData.Add(objReader.CurrentPSM.ProteinFirst, lstMatchesForProtein)
								End If


								lstScansWritten.Add(objReader.CurrentPSM.ScanNumber)
							End If

						End If

					End While
				End Using
			End Using

		Catch ex As Exception
			m_message = "Exception in CreatePseudoMSGFFileUsingPHRPReader"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return String.Empty
		End Try


		Return strPseudoMsgfFilePath

	End Function

	Protected Function CreateMSGFReportFiles(ByRef dctPrideReportFiles As Generic.Dictionary(Of Integer, String), ByVal blnCreateMSGFReportFilesOnly As Boolean) As Boolean

		Dim blnSuccess As Boolean

		Dim intJob As Integer = 0
		Dim strDataset As String = "??"

		Dim strTemplateFileName As String
		Dim udtFilterThresholds As udtFilterThresholdsType

		Dim strPseudoMsgfFilePath As String
		Dim strPrideReportXMLFilePath As String

		Dim strLocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
		Dim strCachedOrgDBName As String = String.Empty
		Dim strOrgDBNameGenerated As String
		Dim strProteinCollectionListOrFasta As String

		' This dictionary holds protein name in the key 
		' The value is a key-value pair where the key is the Protein Index and the value is the protein sequence
		Dim dctProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String))

		' This dictionary holds the protein index as the key and tracks the number of filter-passing PSMs for each protein as the value
		Dim dctProteinPSMCounts As Generic.Dictionary(Of Integer, Integer)

		Dim lstPseudoMSGFData As Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType))
		lstPseudoMSGFData = New Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType))

		Try
			m_progress = PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES
			m_StatusTools.UpdateAndWrite(m_progress)

			' Initialize the dictionaries
			dctPrideReportFiles = New Generic.Dictionary(Of Integer, String)
			dctProteins = New Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String))
			dctProteinPSMCounts = New Generic.Dictionary(Of Integer, Integer)

			' Determine the filter thresholds
			udtFilterThresholds.Clear()

			With udtFilterThresholds
				.FDRThreshold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.FDRThreshold)
				.PepFDRThreshold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.PepFDRThreshold)
				.MSGFSpecProbThresold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.MSGFSpecProbThresold)

				.UseFDRThreshold = m_jobParams.GetJobParameter("UseFDRThreshold", udtFilterThresholds.UseFDRThreshold)
				.UsePepFDRThreshold = m_jobParams.GetJobParameter("UsePepFDRThreshold", udtFilterThresholds.UsePepFDRThreshold)
				.UseMSGFSpecProb = m_jobParams.GetJobParameter("UseMSGFSpecProb", udtFilterThresholds.UseMSGFSpecProb)
			End With

			strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing:=False)

			For Each kvEntry As Generic.KeyValuePair(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType) In mDataPackagePeptideHitJobs
				intJob = kvEntry.Value.Job
				strDataset = kvEntry.Value.Dataset

				strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob), String.Empty)
				If String.IsNullOrEmpty(strOrgDBNameGenerated) Then
					m_message = "Job parameter " & clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob) & " was not found in CreateMSGFReportFiles; unable to continue"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If

				If Not String.IsNullOrEmpty(kvEntry.Value.ProteinCollectionList) AndAlso kvEntry.Value.ProteinCollectionList <> "na" Then
					strProteinCollectionListOrFasta = kvEntry.Value.ProteinCollectionList
				Else
					strProteinCollectionListOrFasta = kvEntry.Value.LegacyFastaFileName
				End If

				If strCachedOrgDBName <> strOrgDBNameGenerated Then
					' Need to read the proteins from the fasta file

					dctProteins.Clear()
					dctProteinPSMCounts.Clear()

					Dim objFastaFileReader As ProteinFileReader.FastaFileReader
					Dim strFastaFilePath As String = IO.Path.Combine(strLocalOrgDBFolder, strOrgDBNameGenerated)
					objFastaFileReader = New ProteinFileReader.FastaFileReader()

					If Not objFastaFileReader.OpenFile(strFastaFilePath) Then
						m_message = "Error opening fasta file " & strOrgDBNameGenerated & "; objFastaFileReader.OpenFile() returned false"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; see " & strLocalOrgDBFolder)
						Return False
					Else
						Console.WriteLine()
						Console.WriteLine("Reading proteins from " & strFastaFilePath)

						Do While objFastaFileReader.ReadNextProteinEntry()
							If Not dctProteins.ContainsKey(objFastaFileReader.ProteinName) Then
								Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String)
								kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(dctProteins.Count, objFastaFileReader.ProteinSequence)

								Try
									dctProteins.Add(objFastaFileReader.ProteinName, kvIndexAndSequence)
								Catch ex As Exception
									Throw New Exception("Dictionary error for dctProteins", ex)
								End Try

								Try
									dctProteinPSMCounts.Add(kvIndexAndSequence.Key, 0)
								Catch ex As Exception
									Throw New Exception("Dictionary error for dctProteinPSMCounts", ex)
								End Try

							End If
						Loop
						objFastaFileReader.CloseFile()
					End If

					strCachedOrgDBName = String.Copy(strOrgDBNameGenerated)
				End If

				Console.WriteLine((dctPrideReportFiles.Count + 1).ToString & ": Creating .msgf file for job " & intJob & ", " & strDataset)

				lstPseudoMSGFData.Clear()

				strPseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(intJob, strDataset, udtFilterThresholds, lstPseudoMSGFData, dctProteins, dctProteinPSMCounts)

				'If intJob = 861784 Then
				'	' Temp Debug Hack

				'	Dim lstMatchesForHack As Generic.List(Of udtPseudoMSGFDataType)
				'	Dim udtItemForHack As udtPseudoMSGFDataType
				'	lstMatchesForHack = lstPseudoMSGFData("SO_1931")
				'	udtItemForHack = lstMatchesForHack(1)

				'	With udtItemForHack
				'		.Peptide = "K.LTVADMTGGNFTVTNGGVFGSLMSTPILNL.P"
				'		.PrefixResidue = "K"
				'		.SuffixResidue = "P"
				'		.ChargeState = 2
				'		.CleanSequence = "LTVADMTGGNFTVTNGGVFGSLMSTPILNL"
				'		.TotalPRMScore = "50"
				'		.PValue = "1E-10"
				'	End With
				'	lstMatchesForHack.Add(udtItemForHack)

				'End If

				If String.IsNullOrEmpty(strPseudoMsgfFilePath) Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Pseudo Msgf file not created for job " & intJob & ", dataset " & strDataset
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					End If
					Return False
				End If

				If Not blnCreateMSGFReportFilesOnly Then

					strPrideReportXMLFilePath = CreateMSGFReportXMLFile(strTemplateFileName, kvEntry.Value, strPseudoMsgfFilePath, lstPseudoMSGFData, dctProteins, dctProteinPSMCounts, strOrgDBNameGenerated, strProteinCollectionListOrFasta)

					If String.IsNullOrEmpty(strPrideReportXMLFilePath) Then
						If String.IsNullOrEmpty(m_message) Then
							m_message = "Pride report XML file not created for job " & intJob & ", dataset " & strDataset
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						End If
						Return False
					End If

					Try
						dctPrideReportFiles.Add(intJob, strPrideReportXMLFilePath)
					Catch ex As Exception
						Throw New Exception("Dictionary error for dctPrideReportFiles", ex)
					End Try

				End If

				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES, PROGRESS_PCT_CREATING_PRIDE_XML_FILES, dctPrideReportFiles.Count, mDataPackagePeptideHitJobs.Count)
				m_StatusTools.UpdateAndWrite(m_progress)

			Next

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportFiles for job " & intJob & ", dataset " & strDataset
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function CreateMSGFReportXMLFile( _
	  ByVal strTemplateFileName As String, _
	  ByVal udtJobInfo As clsAnalysisResources.udtDataPackageJobInfoType, _
	  ByVal strPseudoMsgfFilePath As String, _
	  ByRef lstPseudoMSGFData As Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType)), _
	  ByRef dctProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String)), _
	  ByRef dctProteinPSMCounts As Generic.Dictionary(Of Integer, Integer), _
	  ByVal strOrgDBNameGenerated As String, _
	  ByVal strProteinCollectionListOrFasta As String) As String


		Dim strPrideReportXMLFilePath As String = String.Empty

		Dim blnInsideMzDataDescription As Boolean
		Dim blnSkipNode As Boolean

		Dim lstElementCloseDepths As Generic.Stack(Of Integer)

		Dim eFileLocation As eMSGFReportXMLFileLocation = eMSGFReportXMLFileLocation.Header

		Try
			lstElementCloseDepths = New Generic.Stack(Of Integer)

			' Open strTemplateFileName and parse it to create a new XML file
			' Use a forward-only XML reader, copying some elements verbatim and customizing others
			' When we reach <Identifications>, we write out the data that was cached from strPseudoMsgfFilePath
			'    Must write out data by protein

			' Next, append the protein sequences in dctProteins to the <Fasta></Fasta> section

			' Finally, write the remaining sections
			' <PTMs>
			' <DatabaseMappings>
			' <ConfigurationOptions>


			strPrideReportXMLFilePath = strPseudoMsgfFilePath & "-report.xml"

			Using objXmlWriter As Xml.XmlTextWriter = New Xml.XmlTextWriter(New IO.FileStream(strPrideReportXMLFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read), Text.Encoding.UTF8)
				objXmlWriter.Formatting = Xml.Formatting.Indented
				objXmlWriter.Indentation = 4

				objXmlWriter.WriteStartDocument()

				Using objXmlReader As Xml.XmlTextReader = New Xml.XmlTextReader(New IO.FileStream(IO.Path.Combine(m_WorkDir, strTemplateFileName), IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

					Do While objXmlReader.Read()

						Select Case objXmlReader.NodeType
							Case Xml.XmlNodeType.Whitespace
								' Skip whitespace since the writer should be auto-formatting things
								' objXmlWriter.WriteWhitespace(objXmlReader.Value)

							Case Xml.XmlNodeType.Comment
								objXmlWriter.WriteComment(objXmlReader.Value)

							Case Xml.XmlNodeType.Element
								' Start element

								Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
									Dim intPoppedVal As Integer
									intPoppedVal = lstElementCloseDepths.Pop()

									objXmlWriter.WriteEndElement()
								Loop

								eFileLocation = UpdateMSGFReportXMLFileLocation(eFileLocation, objXmlReader.Name, blnInsideMzDataDescription)

								blnSkipNode = False
								Select Case objXmlReader.Name
									Case "sourceFilePath"
										' Update this element's value to contain strPseudoMsgfFilePath
										objXmlWriter.WriteElementString("sourceFilePath", strPseudoMsgfFilePath)
										blnSkipNode = True

									Case "timeCreated"
										' Write out the current date/time in this format: 2012-11-06T16:04:44Z
										objXmlWriter.WriteElementString("timeCreated", System.DateTime.Now.ToUniversalTime().ToString("s") & "Z")
										blnSkipNode = True

									Case "MzDataDescription"
										blnInsideMzDataDescription = True

									Case "sampleName"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
											' Write out the current job's Experiment Name
											objXmlWriter.WriteElementString("sampleName", udtJobInfo.Experiment)
											blnSkipNode = True
										End If

									Case "sourceFile"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
											objXmlWriter.WriteStartElement("sourceFile")

											objXmlWriter.WriteElementString("nameOfFile", IO.Path.GetFileName(strPseudoMsgfFilePath))
											objXmlWriter.WriteElementString("pathToFile", strPseudoMsgfFilePath)
											objXmlWriter.WriteElementString("fileType", "MSGF file")

											objXmlWriter.WriteEndElement()	' sourceFile
											blnSkipNode = True

										End If

									Case "software"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataDataProcessing Then
											CreateMSGFReportXmlFileWriteSoftwareVersion(objXmlReader, objXmlWriter, udtJobInfo.PeptideHitResultType)
											blnSkipNode = True
										End If

									Case "cvParam"
										If eFileLocation = eMSGFReportXMLFileLocation.ExperimentAdditional Then
											' Override the cvParam if it has Accession PRIDE:0000175

											objXmlWriter.WriteStartElement("cvParam")

											If objXmlReader.HasAttributes() Then
												Dim strValueOverride As String = String.Empty
												objXmlReader.MoveToFirstAttribute()
												Do
													If objXmlReader.Name = "accession" AndAlso objXmlReader.Value = "PRIDE:0000175" Then
														strValueOverride = "DMS PRIDE_Converter " & System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
													End If

													If objXmlReader.Name = "value" AndAlso strValueOverride.Length > 0 Then
														objXmlWriter.WriteAttributeString(objXmlReader.Name, strValueOverride)
													Else
														objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)
													End If

												Loop While objXmlReader.MoveToNextAttribute()
											End If

											objXmlWriter.WriteEndElement()	' cvParam
											blnSkipNode = True

										End If

									Case "Identifications"
										If Not CreateMSGFReportXMLFileWriteIDs(objXmlWriter, lstPseudoMSGFData, dctProteins, dctProteinPSMCounts, strOrgDBNameGenerated, udtJobInfo.PeptideHitResultType) Then
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreateMSGFReportXMLFileWriteIDs returned false; aborting")
											Return String.Empty
										End If

										If Not CreateMSGFReportXMLFileWriteProteins(objXmlWriter, lstPseudoMSGFData, dctProteins, dctProteinPSMCounts, strOrgDBNameGenerated) Then
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreateMSGFReportXMLFileWriteProteins returned false; aborting")
											Return String.Empty
										End If

										blnSkipNode = True

									Case "Fasta"
										' This section is written out by CreateMSGFReportXMLFileWriteIDs
										blnSkipNode = True

									Case "PTMs"
										' In the future, we might write out customized PTMs in CreateMSGFReportXMLFileWriteProteins
										' For now, just copy over whatever is in the template msgf-report.xml file
										'
										blnSkipNode = False

									Case "DatabaseMappings"

										objXmlWriter.WriteStartElement("DatabaseMappings")
										objXmlWriter.WriteStartElement("DatabaseMapping")

										objXmlWriter.WriteElementString("SearchEngineDatabaseName", strOrgDBNameGenerated)
										objXmlWriter.WriteElementString("SearchEngineDatabaseVersion", "Unknown")

										objXmlWriter.WriteElementString("CuratedDatabaseName", strProteinCollectionListOrFasta)
										objXmlWriter.WriteElementString("CuratedDatabaseVersion", "1")

										objXmlWriter.WriteEndElement()		' DatabaseMapping
										objXmlWriter.WriteEndElement()		' DatabaseMappings

										blnSkipNode = True

									Case "ConfigurationOptions"
										objXmlWriter.WriteStartElement("ConfigurationOptions")

										WriteConfigurationOption(objXmlWriter, "search_engine", "MSGF")
										WriteConfigurationOption(objXmlWriter, "peptide_threshold", "0.05")
										WriteConfigurationOption(objXmlWriter, "add_carbamidomethylation", "false")

										objXmlWriter.WriteEndElement()		' ConfigurationOptions

										blnSkipNode = True

								End Select


								If blnSkipNode Then
									If objXmlReader.NodeType <> Xml.XmlNodeType.EndElement Then
										' Skip this element (and any children nodes enclosed in this elemnt)
										' Likely should not do this when objXmlReader.NodeType is Xml.XmlNodeType.EndElement
										objXmlReader.Skip()
									End If

								Else
									' Copy this element from the source file to the target file

									objXmlWriter.WriteStartElement(objXmlReader.Name)

									If objXmlReader.HasAttributes() Then
										objXmlReader.MoveToFirstAttribute()
										Do
											objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)
										Loop While objXmlReader.MoveToNextAttribute()

										lstElementCloseDepths.Push(objXmlReader.Depth)

									ElseIf objXmlReader.IsEmptyElement Then
										objXmlWriter.WriteEndElement()
									End If

								End If

							Case Xml.XmlNodeType.EndElement

								Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth + 1
									Dim intPoppedVal As Integer
									intPoppedVal = lstElementCloseDepths.Pop()
									objXmlWriter.WriteEndElement()
								Loop

								objXmlWriter.WriteEndElement()

								If objXmlReader.Name = "MzDataDescription" Then
									blnInsideMzDataDescription = False
								End If

								Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
									Dim intPoppedVal As Integer
									intPoppedVal = lstElementCloseDepths.Pop()
								Loop

							Case Xml.XmlNodeType.Text
								objXmlWriter.WriteString(objXmlReader.Value)

						End Select

					Loop

				End Using

				objXmlWriter.WriteEndDocument()
			End Using


		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportXMLFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return String.Empty
		End Try

		Return strPrideReportXMLFilePath

	End Function

	Protected Function CreateMSGFReportXMLFileWriteIDs( _
	  ByRef objXmlWriter As Xml.XmlTextWriter,
	  ByRef lstPseudoMSGFData As Dictionary(Of String, List(Of udtPseudoMSGFDataType)),
	  ByRef dctProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String)), _
	  ByRef dctProteinPSMCounts As Generic.Dictionary(Of Integer, Integer), _
	  ByVal strOrgDBNameGenerated As String, _
	  ByVal ePeptideHitResultType As clsPHRPReader.ePeptideHitResultType) As Boolean

		Try

			objXmlWriter.WriteStartElement("Identifications")

			For Each kvProteinEntry As Generic.KeyValuePair(Of String, Generic.List(Of udtPseudoMSGFDataType)) In lstPseudoMSGFData

				Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String) = Nothing

				If Not dctProteins.TryGetValue(kvProteinEntry.Key, kvIndexAndSequence) Then
					' Protein not found in dctProteins; this is unexpected (should have already been added by CreatePseudoMSGFFileUsingPHRPReader()
					' Add the protein to dctProteins and dctProteinPSMCounts, though we won't know its sequence

					kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(dctProteins.Count, String.Empty)
					dctProteinPSMCounts.Add(kvIndexAndSequence.Key, kvProteinEntry.Value.Count)
					dctProteins.Add(kvProteinEntry.Key, kvIndexAndSequence)

				Else
					dctProteinPSMCounts(kvIndexAndSequence.Key) = kvProteinEntry.Value.Count
				End If

				objXmlWriter.WriteStartElement("Identification")

				objXmlWriter.WriteElementString("Accession", kvProteinEntry.Key)			' Protein name
				' objXmlWriter.WriteElementString("CuratedAccession", kvProteinEntry.Key)		' Cleaned-up version of the Protein name; for example, for ref|NP_035862.2 we would put "NP_035862" here
				objXmlWriter.WriteElementString("UniqueIdentifier", kvProteinEntry.Key)		' Protein name
				' objXmlWriter.WriteElementString("AccessionVersion", "1")						' Accession version would be determined when curating the "Accession" name.  For example, for ref|NP_035862.2 we would put "2" here
				objXmlWriter.WriteElementString("Database", strOrgDBNameGenerated)
				objXmlWriter.WriteElementString("DatabaseVersion", "Unknown")

				' Write out each PSM for this protein
				For Each udtPeptide As udtPseudoMSGFDataType In kvProteinEntry.Value
					objXmlWriter.WriteStartElement("Peptide")

					objXmlWriter.WriteElementString("Sequence", udtPeptide.CleanSequence)
					objXmlWriter.WriteElementString("CuratedSequence", String.Empty)
					objXmlWriter.WriteElementString("Start", "0")
					objXmlWriter.WriteElementString("End", "0")
					objXmlWriter.WriteElementString("SpectrumReference", udtPeptide.ScanNumber.ToString())
					objXmlWriter.WriteElementString("isSpecific", "false")

					objXmlWriter.WriteElementString("UniqueIdentifier", udtPeptide.ScanNumber.ToString())		' I wanted to record ResultID here, but we instead have to record Scan Number; otherwise PRIDE Converter Crashes

					objXmlWriter.WriteStartElement("additional")

					WriteCVParam(objXmlWriter, "PRIDE", "PRIDE:0000065", "Upstream flanking sequence", udtPeptide.PrefixResidue)
					WriteCVParam(objXmlWriter, "PRIDE", "PRIDE:0000066", "Downstream flanking sequence", udtPeptide.SuffixResidue)

					WriteCVParam(objXmlWriter, "MS", "MS:1000041", "charge state", udtPeptide.ChargeState.ToString())
					WriteCVParam(objXmlWriter, "MS", "MS:1000042", "peak intensity", "0.0")
					WriteCVParam(objXmlWriter, "MS", "MS:1001870", "p-value for peptides", udtPeptide.PValue)

					WriteUserParam(objXmlWriter, "MQScore", udtPeptide.MQScore)
					WriteUserParam(objXmlWriter, "TotalPRMScore", udtPeptide.TotalPRMScore)

					' WriteUserParam(objXmlWriter, "MedianPRMScore", "0.0")
					' WriteUserParam(objXmlWriter, "FractionY", "0.0")
					' WriteUserParam(objXmlWriter, "FractionB", "0.0")

					WriteUserParam(objXmlWriter, "NTT", udtPeptide.NTT.ToString())

					' WriteUserParam(objXmlWriter, "F-Score", "0.0")

					WriteUserParam(objXmlWriter, "DeltaScore", udtPeptide.DeltaScore)
					WriteUserParam(objXmlWriter, "DeltaScoreOther", udtPeptide.DeltaScoreOther)
					WriteUserParam(objXmlWriter, "SpecProb", udtPeptide.MSGFSpecProb)


					objXmlWriter.WriteEndElement()		' additional

					objXmlWriter.WriteEndElement()		' Peptide
				Next

				' Protein level-scores
				objXmlWriter.WriteElementString("Score", "0.0")
				objXmlWriter.WriteElementString("Threshold", "0.0")
				objXmlWriter.WriteElementString("SearchEngine", "MSGF")

				objXmlWriter.WriteStartElement("additional")
				objXmlWriter.WriteEndElement()

				objXmlWriter.WriteElementString("FastaSequenceReference", kvIndexAndSequence.Key.ToString())

				objXmlWriter.WriteEndElement()		' Identification


			Next

			objXmlWriter.WriteEndElement()			' Identifications

		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportXMLFileWriteIDs"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Function CreateMSGFReportXMLFileWriteProteins( _
	  ByRef objXmlWriter As Xml.XmlTextWriter,
	  ByRef lstPseudoMSGFData As Dictionary(Of String, List(Of udtPseudoMSGFDataType)),
	  ByRef dctProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String)), _
	  ByRef dctProteinPSMCounts As Generic.Dictionary(Of Integer, Integer), _
	  ByVal strOrgDBNameGenerated As String) As Boolean

		Dim strProteinName As String
		Dim intProteinIndex As Integer
		Dim intPSMCount As Integer
		Try

			objXmlWriter.WriteStartElement("Fasta")
			objXmlWriter.WriteAttributeString("sourceDb", strOrgDBNameGenerated)
			objXmlWriter.WriteAttributeString("sourceDbVersion", "Unknown")

			' Step through dctProteins
			' For each entry, the key is the protein name
			' The value is itself a key-value pair, where Value.Key is the protein index and Value.Value is the protein sequence
			For Each kvEntry As Generic.KeyValuePair(Of String, Generic.KeyValuePair(Of Integer, String)) In dctProteins

				strProteinName = String.Copy(kvEntry.Key)
				intProteinIndex = kvEntry.Value.Key

				' Only write out this protein if it had 1 or more PSMs
				If dctProteinPSMCounts.TryGetValue(intProteinIndex, intPSMCount) Then
					If intPSMCount > 0 Then
						objXmlWriter.WriteStartElement("Sequence")
						objXmlWriter.WriteAttributeString("id", intProteinIndex.ToString())
						objXmlWriter.WriteAttributeString("accession", strProteinName)

						objXmlWriter.WriteValue(kvEntry.Value.Value)

						objXmlWriter.WriteEndElement()			' Sequence
					End If
				End If
			Next

			objXmlWriter.WriteEndElement()			' Fasta


			' In the future, we might write out customized PTMs here
			' For now, just copy over whatever is in the template msgf-report.xml file
			'
			'objXmlWriter.WriteStartElement("PTMs")
			'objXmlWriter.WriteFullEndElement()


		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportXMLFileWriteProteins"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Sub CreateMSGFReportXmlFileWriteSoftwareVersion(ByRef objXmlReader As Xml.XmlTextReader, ByRef objXmlWriter As Xml.XmlTextWriter, PeptideHitResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType)

		Dim strToolName As String = String.Empty
		Dim strToolVersion As String = String.Empty
		Dim strToolComments As String = String.Empty
		Dim intNodeDepth As Integer = objXmlReader.Depth

		' Read the name, version, and comments elements under software
		Do While objXmlReader.Read()
			Select Case objXmlReader.NodeType
				Case Xml.XmlNodeType.Element
					Select Case objXmlReader.Name
						Case "name"
							strToolName = objXmlReader.ReadElementContentAsString()
						Case "version"
							strToolVersion = objXmlReader.ReadElementContentAsString()
						Case "comments"
							strToolComments = objXmlReader.ReadElementContentAsString()
					End Select
				Case Xml.XmlNodeType.EndElement
					If objXmlReader.Depth <= intNodeDepth Then
						Exit Do
					End If
			End Select
		Loop

		If String.IsNullOrEmpty(strToolName) Then
			strToolName = PeptideHitResultType.ToString()
			strToolVersion = String.Empty
			strToolComments = String.Empty
		Else
			If PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB AndAlso strToolName.ToUpper().StartsWith("MSGF") Then
				' Tool Version in the template file is likely correct; use it
			ElseIf PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Sequest AndAlso strToolName.ToUpper().StartsWith("SEQUEST") Then
				' Tool Version in the template file is likely correct; use it
			ElseIf PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.XTandem AndAlso strToolName.ToUpper().Contains("TANDEM") Then
				' Tool Version in the template file is likely correct; use it
			Else
				' Tool Version is likely not known
				strToolName = PeptideHitResultType.ToString()
				strToolVersion = String.Empty
				strToolComments = String.Empty
			End If
		End If

		objXmlWriter.WriteStartElement("software")

		objXmlWriter.WriteElementString("name", strToolName)
		objXmlWriter.WriteElementString("version", strToolVersion)
		objXmlWriter.WriteElementString("comments", strToolComments)

		objXmlWriter.WriteEndElement()	' software

	End Sub

	Protected Function CreatePrideXMLFiles(ByVal dctPrideReportFiles As Generic.Dictionary(Of Integer, String)) As Boolean

		Dim blnSuccess As Boolean
		Dim intJob As Integer
		Dim strDataset As String = String.Empty
		Dim strCurrentTask As String

		Dim strBaseFileName As String
		Dim strMsgfResultsFilePath As String
		Dim strMzXMLFilePath As String
		Dim strPrideReportFilePath As String
		Dim strPrideXmlFilePath As String

		Dim intJobsProcessed As Integer = 0

		Try
			m_progress = PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES
			m_StatusTools.UpdateAndWrite(m_progress)

			Console.WriteLine()
			For Each kvItem As Generic.KeyValuePair(Of Integer, String) In dctPrideReportFiles

				intJob = kvItem.Key
				strDataset = LookupDatasetByJob(intJob)

				strBaseFileName = strDataset & "_Job" & intJob.ToString()
				strMsgfResultsFilePath = IO.Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_PSEUDO_MSGF)
				strMzXMLFilePath = IO.Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
				strPrideReportFilePath = IO.Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_MSGF_REPORT_XML)

				strCurrentTask = "Running PRIDE Converter for job " & intJob & ", " & strDataset
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strCurrentTask)
				End If
				Console.WriteLine((intJobsProcessed + 1).ToString & ": " & strCurrentTask)

				blnSuccess = RunPrideConverter(intJob, strDataset, strMsgfResultsFilePath, strMzXMLFilePath, strPrideReportFilePath)

				If Not blnSuccess Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Unknown error calling RunPrideConverter"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					End If

					If intJobsProcessed = 0 Then
						Return False
					Else
						' At least one job succeeded; skip this one and move onto the next

						strCurrentTask = "PRIDE Converter failed for job " & intJob & "; will continue processing because at least one job succeeded"
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strCurrentTask)
						End If
						Console.WriteLine((intJobsProcessed + 1).ToString & ": " & strCurrentTask)
						Console.WriteLine()

					End If

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

				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_CREATING_MSGF_REPORT_XML_FILES, PROGRESS_PCT_CREATING_PRIDE_XML_FILES, intJobsProcessed, dctPrideReportFiles.Count)
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
		Dim udtJobInfo As clsAnalysisResources.udtDataPackageJobInfoType = New clsAnalysisResources.udtDataPackageJobInfoType

		If Not mDataPackagePeptideHitJobs Is Nothing Then
			If mDataPackagePeptideHitJobs.TryGetValue(intJob, udtJobInfo) Then
				Return udtJobInfo.Dataset
			End If
		End If

		Return String.Empty
	End Function

	Protected Function LookupDataPackagePeptideHitJobs() As Boolean
		Dim intJob As Integer

		Dim dctDataPackageJobs As Generic.Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)
		dctDataPackageJobs = New Generic.Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)

		If mDataPackagePeptideHitJobs Is Nothing Then
			mDataPackagePeptideHitJobs = New Generic.Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)
		Else
			mDataPackagePeptideHitJobs.Clear()
		End If

		If Not MyBase.LoadDataPackageJobInfo(dctDataPackageJobs) Then
			m_message = "Error loading data package job info for"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": clsAnalysisToolRunnerBase.LoadDataPackageJobInfo() returned false")
			Return False
		Else
			Dim lstJobsToUse As Generic.List(Of String)
			lstJobsToUse = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS)

			If lstJobsToUse.Count = 0 Then
				m_message = "Job parameter " & clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS & " is empty; no jobs to process"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			Else
				' Populate mDataPackagePeptideHitJobs using the jobs in lstJobsToUse and dctDataPackagePeptideHitJobs
				For Each strJob As String In lstJobsToUse
					If Integer.TryParse(strJob, intJob) Then
						Dim udtJobInfo As clsAnalysisResources.udtDataPackageJobInfoType = New clsAnalysisResources.udtDataPackageJobInfoType
						If dctDataPackageJobs.TryGetValue(intJob, udtJobInfo) Then
							mDataPackagePeptideHitJobs.Add(intJob, udtJobInfo)
						End If
					End If
				Next
			End If
		End If

		Return True

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
			.WorkDir = m_WorkDir
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

	Protected Function UpdateMSGFReportXMLFileLocation(ByVal eFileLocation As eMSGFReportXMLFileLocation, ByVal strElementName As String, ByVal blnInsideMzDataDescription As Boolean) As eMSGFReportXMLFileLocation

		Select Case strElementName
			Case "SearchResultIdentifier"
				eFileLocation = eMSGFReportXMLFileLocation.SearchResultIdentifier
			Case "Metadata"
				eFileLocation = eMSGFReportXMLFileLocation.Metadata
			Case "Protocol"
				eFileLocation = eMSGFReportXMLFileLocation.Protocol
			Case "admin"
				If blnInsideMzDataDescription Then
					eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin
				End If
			Case "instrument"
				If blnInsideMzDataDescription Then
					eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument
				End If
			Case "dataProcessing"
				If blnInsideMzDataDescription Then
					eFileLocation = eMSGFReportXMLFileLocation.MzDataDataProcessing
				End If
			Case "ExperimentAdditional"
				eFileLocation = eMSGFReportXMLFileLocation.ExperimentAdditional
			Case "Identifications"
				eFileLocation = eMSGFReportXMLFileLocation.Identifications
			Case "PTMs"
				eFileLocation = eMSGFReportXMLFileLocation.PTMs
			Case "DatabaseMappings"
				eFileLocation = eMSGFReportXMLFileLocation.DatabaseMappings
			Case "ConfigurationOptions"
				eFileLocation = eMSGFReportXMLFileLocation.ConfigurationOptions
		End Select

		Return eFileLocation

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
		Dim ioToolFiles As New Generic.List(Of System.IO.FileInfo)
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

	Protected Sub WriteConfigurationOption(objXmlWriter As Xml.XmlTextWriter, KeyName As String, Value As String)

		objXmlWriter.WriteStartElement("Option")
		objXmlWriter.WriteElementString("Key", KeyName)
		objXmlWriter.WriteElementString("Value", Value)
		objXmlWriter.WriteEndElement()

	End Sub
	Protected Sub WriteUserParam(objXmlWriter As Xml.XmlTextWriter, Name As String, Value As String)

		objXmlWriter.WriteStartElement("userParam")
		objXmlWriter.WriteAttributeString("name", Name)
		objXmlWriter.WriteAttributeString("value", Value)
		objXmlWriter.WriteEndElement()

	End Sub

	Protected Sub WriteCVParam(objXmlWriter As Xml.XmlTextWriter, CVLabel As String, Accession As String, Name As String, Value As String)

		objXmlWriter.WriteStartElement("cvParam")
		objXmlWriter.WriteAttributeString("cvLabel", CVLabel)
		objXmlWriter.WriteAttributeString("accession", Accession)
		objXmlWriter.WriteAttributeString("name", Name)
		objXmlWriter.WriteAttributeString("value", Value)
		objXmlWriter.WriteEndElement()

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
