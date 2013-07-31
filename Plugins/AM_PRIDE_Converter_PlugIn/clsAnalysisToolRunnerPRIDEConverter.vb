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
	Protected Const PROGRESS_PCT_SAVING_RESULTS As Single = 95
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const FILE_EXTENSION_PSEUDO_MSGF As String = ".msgf"
	Protected Const FILE_EXTENSION_MSGF_REPORT_XML As String = ".msgf-report.xml"
	Protected Const FILE_EXTENSION_MSGF_PRIDE_XML As String = ".msgf-pride.xml"

	Protected Const DEFAULT_PVALUE_THRESHOLD As Double = 0.05

	Protected mConsoleOutputErrorMsg As String

	' This dictionary tracks the peptide hit jobs defined for this data package
	' The keys are job numbers and the values contains job info
	Protected mDataPackagePeptideHitJobs As Generic.Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)

	Protected mPrideConverterProgLoc As String = String.Empty

	Protected mJavaProgLoc As String = String.Empty
	Protected mMSXmlGeneratorAppPath As String = String.Empty

	Protected mCreateMSGFReportFilesOnly As Boolean
	Protected mCreateMGFFiles As Boolean
	Protected mCreatePrideXMLFiles As Boolean

	Protected mProcessMzIdFiles As Boolean

	Protected mPreviousDatasetName As String = String.Empty

	' This list contains full fill paths for files that will be deleted from the local work directory
	Protected mPreviousDatasetFilesToDelete As Generic.List(Of String)

	' This list contains full fill paths for files that will be copied from the local work directory to the transfer directory
	Protected mPreviousDatasetFilesToCopy As Generic.List(Of String)

	Protected mCachedOrgDBName As String = String.Empty

	' This dictionary holds protein name in the key 
	' The value is a key-value pair where the key is the Protein Index and the value is the protein sequence
	Protected mCachedProteins As Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String))

	' This dictionary holds the protein index as the key and tracks the number of filter-passing PSMs for each protein as the value
	Protected mCachedProteinPSMCounts As Generic.Dictionary(Of Integer, Integer)

	' Keys in this dictionary are filenames
	' Values contain info on each file
	' Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
	' However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
	Protected mPxMasterFileList As Generic.Dictionary(Of String, clsPXFileInfoBase)

	' Keys in this dictionary are PXFileIDs
	' Values contain info on each file, including the PXFileType and the FileIDs that map to this file (empty list if no mapped files)
	' Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
	' However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
	Protected mPxResultFiles As Generic.Dictionary(Of Integer, clsPXFileInfo)

	Protected mFilterThresholdsUsed As udtFilterThresholdsType

	' Keys in this dictionary are instrument group names
	' Values are the specific instrument names
	Protected mInstrumentGroupsStored As Generic.Dictionary(Of String, Generic.List(Of String))
	Protected mSearchToolsUsed As Generic.SortedSet(Of String)

	' Keys in this dictionary are NEWT IDs
	' Values are the NEWT name for the given ID
	Protected mExperimentNEWTInfo As Generic.Dictionary(Of Integer, String)

	' Keys in this dictionary are Unimod accession names (e.g. UNIMOD:35)
	' Values are CvParam data for the modification
	Protected mModificationsUsed As Generic.Dictionary(Of String, udtCvParamInfoType)

	' Keys in this dictionary are _dta.txt file names
	' Values contain info on each file
	Protected mCDTAFileStats As Generic.Dictionary(Of String, clsPXFileInfoBase)

	Protected WithEvents mMSXmlCreator As AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator
	Protected WithEvents mDTAtoMGF As DTAtoMGF.clsDTAtoMGF

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures and Enums"
	Protected Structure udtFilterThresholdsType
		Public PValueThreshold As Single
		Public FDRThreshold As Single
		Public PepFDRThreshold As Single
		Public MSGFSpecProbThreshold As Single
		Public UseFDRThreshold As Boolean
		Public UsePepFDRThreshold As Boolean
		Public UseMSGFSpecProb As Boolean
		Public Sub Clear()
			PValueThreshold = DEFAULT_PVALUE_THRESHOLD
			UseFDRThreshold = False
			UsePepFDRThreshold = False
			UseMSGFSpecProb = True
			FDRThreshold = 0.01
			PepFDRThreshold = 0.01
			MSGFSpecProbThreshold = 0.000000001
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
		Public Protein As String
	End Structure

	Protected Structure udtResultFileContainerType
		Public MGFFilePath As String
		Public MzIDFilePath As String
		Public PrideXmlFilePath As String
	End Structure

	Protected Structure udtCvParamInfoType
		Public Accession As String
		Public CvRef As String
		Public Value As String
		Public Name As String
		Public unitCvRef As String
		Public unitName As String
		Public unitAccession As String
		Public Sub Clear()
			Accession = String.Empty
			CvRef = String.Empty
			Value = String.Empty
			Name = String.Empty
			unitCvRef = String.Empty
			unitName = String.Empty
			unitAccession = String.Empty
		End Sub
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

	Protected Enum eMzIDXMLFileLocation
		Header = 0
		SequenceCollection = 1
		AnalysisCollection = 2
		AnalysisProtocolCollection = 3
		DataCollection = 4
		Inputs = 5
		InputSearchDatabase = 6
		InputSpectraData = 7
		AnalysisData = 8
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
		Dim blnSuccess As Boolean

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPRIDEConverter.RunTool(): Enter")
			End If

			Dim udtFilterThresholds As udtFilterThresholdsType

			' Initialize the class-wide variables
			udtFilterThresholds = InitializeOptions()

			' Verify that program files exist
			If Not DefineProgramPaths() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the PRIDE Converter version info in the database
			If Not StoreToolVersionInfo(mPrideConverterProgLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining PRIDE Converter version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			mConsoleOutputErrorMsg = String.Empty

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PRIDEConverter")

			' Initialize mDataPackagePeptideHitJobs			
			If Not LookupDataPackagePeptideHitJobs() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' The clsAnalysisResults object is used to copy files to/from this computer
			Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

			' Extract the dataset raw file paths
			Dim dctDatasetRawFilePaths As Generic.Dictionary(Of String, String)
			dctDatasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS)

			' Process each job in mDataPackagePeptideHitJobs
			' Sort the jobs by dataset so that we can use the same .mzXML file for datasets with multiple jobs
			Dim linqJobsSortedByDataset = (From item In mDataPackagePeptideHitJobs Select item Order By item.Value.Dataset)

			Dim blnContinueOnError As Boolean = True
			Dim intJobsProcessed As Integer = 0

			For Each kvJobInfo As Generic.KeyValuePair(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType) In linqJobsSortedByDataset
				Console.WriteLine()
				Console.WriteLine((intJobsProcessed + 1).ToString() & ": Processing job " & kvJobInfo.Value.Job & ", dataset " & kvJobInfo.Value.Dataset)

				result = ProcessJob(kvJobInfo, udtFilterThresholds, objAnalysisResults, dctDatasetRawFilePaths)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS AndAlso Not blnContinueOnError Then Exit For

				intJobsProcessed += 1
				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_SAVING_RESULTS, intJobsProcessed, mDataPackagePeptideHitJobs.Count)
				m_StatusTools.UpdateAndWrite(m_progress)
			Next

			TransferPreviousDatasetFiles(objAnalysisResults)

			' Create the PX Submission file
			blnSuccess = CreatePXSubmissionFile()

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

			DefineFilesToSkipTransfer()

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

	Protected Sub AddNewtInfo(ByVal intNEWTID As Integer, strNEWTName As String)
		If Not mExperimentNEWTInfo.ContainsKey(intNEWTID) Then
			mExperimentNEWTInfo.Add(intNEWTID, strNEWTName)
		End If
	End Sub

	Protected Function AddPxFileToMasterList(ByVal strFilePath As String, ByVal intJob As Integer, ByVal strDataset As String) As Integer

		Dim fiFile As IO.FileInfo = New IO.FileInfo(strFilePath)

		Dim oPXFileInfo As clsPXFileInfoBase = Nothing
		If mPxMasterFileList.TryGetValue(fiFile.Name, oPXFileInfo) Then
			' File already exists
			Return oPXFileInfo.FileID
		Else
			Dim strFilename As String = CheckFilenameCase(fiFile, strDataset)

			oPXFileInfo = New clsPXFileInfoBase(strFilename)

			oPXFileInfo.FileID = mPxMasterFileList.Count + 1
			oPXFileInfo.Job = intJob

			If fiFile.Exists Then
				oPXFileInfo.Length = fiFile.Length
				oPXFileInfo.MD5Hash = String.Empty		' Don't compute the hash; it's not needed
			Else
				oPXFileInfo.Length = 0
				oPXFileInfo.MD5Hash = String.Empty
			End If

			mPxMasterFileList.Add(fiFile.Name, oPXFileInfo)

			Return oPXFileInfo.FileID
		End If

	End Function

	Protected Function AddPxResultFile(ByVal intFileID As Integer, eFileType As clsPXFileInfoBase.ePXFileType, ByVal strFilePath As String, ByVal strDataset As String) As Boolean

		Dim fiFile As IO.FileInfo = New IO.FileInfo(strFilePath)

		Dim oPXFileInfo As clsPXFileInfo = Nothing

		If mPxResultFiles.TryGetValue(intFileID, oPXFileInfo) Then
			' File already defined in the mapping list
			Return True
		Else

			Dim oMasterPXFileInfo As clsPXFileInfoBase = Nothing
			If Not mPxMasterFileList.TryGetValue(fiFile.Name, oMasterPXFileInfo) Then
				' File not found in mPxMasterFileList, we cannot add the mapping
				m_message = "File " & fiFile.Name & " not found in mPxMasterFileList; unable to add to mPxResultFiles"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If oMasterPXFileInfo.FileID <> intFileID Then
				m_message = "FileID mismatch for " & fiFile.Name
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ":  mPxMasterFileList.FileID = " & oMasterPXFileInfo.FileID & " vs. FileID " & intFileID & " passed into AddPxFileToMapping")
				Return False
			End If

			Dim strFilename As String = CheckFilenameCase(fiFile, strDataset)

			oPXFileInfo = New clsPXFileInfo(strFilename)
			oPXFileInfo.Update(oMasterPXFileInfo)
			oPXFileInfo.PXFileType = eFileType

			mPxResultFiles.Add(intFileID, oPXFileInfo)

			Return True
		End If

	End Function

	''' <summary>
	''' Adds strValue to lstList only if the value is not yet present in the list
	''' </summary>
	''' <param name="lstList"></param>
	''' <param name="strValue"></param>
	''' <remarks></remarks>
	Protected Sub AddToListIfNew(ByRef lstList As Generic.List(Of String), ByVal strValue As String)
		If Not lstList.Contains(strValue) Then
			lstList.Add(strValue)
		End If
	End Sub

	Protected Function AppendToPXFileInfo(intJob As Integer, strDataset As String, dctDatasetRawFilePaths As Generic.Dictionary(Of String, String), udtResultFiles As udtResultFileContainerType) As Boolean

		' Add the files to be submitted to ProteomeXchange to the master file list
		' In addition, append new mappings to the ProteomeXchange mapping list

		Dim intPrideXMLFileID As Integer = 0
		If Not String.IsNullOrEmpty(udtResultFiles.PrideXmlFilePath) Then
			AddToListIfNew(mPreviousDatasetFilesToCopy, udtResultFiles.PrideXmlFilePath)

			intPrideXMLFileID = AddPxFileToMasterList(udtResultFiles.PrideXmlFilePath, intJob, strDataset)
			If Not AddPxResultFile(intPrideXMLFileID, clsPXFileInfoBase.ePXFileType.Result, udtResultFiles.PrideXmlFilePath, strDataset) Then
				Return False
			End If
		End If

		Dim intRawFileID As Integer = 0
		Dim strDatasetRawFilePath As String = String.Empty
		If dctDatasetRawFilePaths.TryGetValue(strDataset, strDatasetRawFilePath) Then
			If Not String.IsNullOrEmpty(strDatasetRawFilePath) Then
				intRawFileID = AddPxFileToMasterList(strDatasetRawFilePath, intJob, strDataset)
				If Not AddPxResultFile(intRawFileID, clsPXFileInfoBase.ePXFileType.Raw, strDatasetRawFilePath, strDataset) Then
					Return False
				End If

				If intPrideXMLFileID > 0 Then
					If Not DefinePxFileMapping(intPrideXMLFileID, intRawFileID) Then
						Return False
					End If
				End If
			End If
		End If

		Dim intPeakfileID As Integer = 0
		If Not String.IsNullOrEmpty(udtResultFiles.MGFFilePath) Then
			AddToListIfNew(mPreviousDatasetFilesToCopy, udtResultFiles.MGFFilePath)

			intPeakfileID = AddPxFileToMasterList(udtResultFiles.MGFFilePath, intJob, strDataset)
			If Not AddPxResultFile(intPeakfileID, clsPXFileInfoBase.ePXFileType.Peak, udtResultFiles.MGFFilePath, strDataset) Then
				Return False
			End If

			If intPrideXMLFileID = 0 Then
				' Pride XML file was not created
				If intRawFileID > 0 AndAlso String.IsNullOrEmpty(udtResultFiles.MzIDFilePath) Then
					' Only associate Peak files with .Raw files if we do not have a .MzId file
					If Not DefinePxFileMapping(intPeakfileID, intRawFileID) Then
						Return False
					End If
				End If
			Else
				' Pride XML file was created
				If Not DefinePxFileMapping(intPrideXMLFileID, intPeakfileID) Then
					Return False
				End If
			End If

		End If

		Dim intMzIdFileID As Integer = 0
		If Not String.IsNullOrEmpty(udtResultFiles.MzIDFilePath) Then
			AddToListIfNew(mPreviousDatasetFilesToCopy, udtResultFiles.MzIDFilePath)

			intMzIdFileID = AddPxFileToMasterList(udtResultFiles.MzIDFilePath, intJob, strDataset)
			If Not AddPxResultFile(intMzIdFileID, clsPXFileInfoBase.ePXFileType.Search, udtResultFiles.MzIDFilePath, strDataset) Then
				Return False
			End If

			If intPrideXMLFileID = 0 Then
				' Pride XML file was not created
				If intPeakfileID > 0 Then
					If Not DefinePxFileMapping(intMzIdFileID, intPeakfileID) Then
						Return False
					End If
				End If

				If intRawFileID > 0 Then
					If Not DefinePxFileMapping(intMzIdFileID, intRawFileID) Then
						Return False
					End If
				End If

			Else
				' Pride XML file was created
				If Not DefinePxFileMapping(intPrideXMLFileID, intMzIdFileID) Then
					Return False
				End If
			End If

		End If

		Return True

	End Function

	Protected Function CheckFilenameCase(ByVal fiFile As IO.FileInfo, ByVal strDataset As String) As String

		Dim strFilename As String = fiFile.Name


		If Not String.IsNullOrEmpty(fiFile.Extension) Then
			Dim strFileBaseName As String = System.IO.Path.GetFileNameWithoutExtension(fiFile.Name)

			If strFileBaseName.ToLower().StartsWith(strDataset.ToLower()) Then
				If Not strFileBaseName.StartsWith(strDataset) Then
					' Case-mismatch; fix it
					If strFileBaseName.Length = strDataset.Length Then
						strFileBaseName = strDataset
					Else
						strFileBaseName = strDataset & strFileBaseName.Substring(strDataset.Length)
					End If
				End If
			End If

			strFilename = strFileBaseName & fiFile.Extension.ToLower()
		End If

		Return strFilename

	End Function

	Protected Function ComputeApproximatePValue(ByVal dblMSGFSpecProb As Double) As Double
		Dim dblSpecProb As Double
		Dim dblPValueEstimate As Double = dblMSGFSpecProb

		Try

			' Estimate Log10(PValue) using 10^(Log10(SpecProb) x 0.9988 + 6.43)
			' This was determined using Job 893431 for dataset QC_Shew_12_02_0pt25_Frac-08_7Nov12_Tiger_12-09-36
			'
			dblPValueEstimate = Math.Log10(dblSpecProb) * 0.9988 + 6.43
			dblPValueEstimate = Math.Pow(10, dblPValueEstimate)

		Catch ex As Exception
			' Ignore errors here
			' We will simply return strMSGFSpecProb
		End Try

		Return dblPValueEstimate

	End Function

	''' <summary>
	''' Convert the _dta.txt file to a .mgf file
	''' </summary>
	''' <param name="intJob"></param>
	''' <param name="strDataset"></param>
	''' <param name="strMGFFilePath">Output parameter: path of the newly created .mgf file</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ConvertCDTAToMGF(ByVal intJob As Integer, ByVal strDataset As String, ByRef strMGFFilePath As String) As Boolean

		Try
			strMGFFilePath = String.Empty

			mDTAtoMGF = New DTAtoMGF.clsDTAtoMGF()
			mDTAtoMGF.Combine2And3PlusCharges = False
			mDTAtoMGF.FilterSpectra = False
			mDTAtoMGF.MaximumIonsPer100MzInterval = 40
			mDTAtoMGF.NoMerge = True

			' Convert the _dta.txt file for this dataset
			Dim fiCDTAFile As IO.FileInfo = New IO.FileInfo(IO.Path.Combine(m_WorkDir, strDataset & "_dta.txt"))

			If Not fiCDTAFile.Exists Then
				m_message = "_dta.txt file not found for job " & intJob
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiCDTAFile.FullName)
				Return False
			End If

			' Compute the MD5 hash for this _dta.txt file
			Dim strMD5Hash As String
			strMD5Hash = clsGlobal.ComputeFileHashMD5(fiCDTAFile.FullName)

			' Make sure this is either a new_dta.txt file or identical to a previous one
			' Abort processing if the job list contains multiple jobs for the same dataset but those jobs used different _dta.txt files
			Dim oFileInfo As clsPXFileInfoBase = Nothing
			If mCDTAFileStats.TryGetValue(fiCDTAFile.Name, oFileInfo) Then
				If fiCDTAFile.Length <> oFileInfo.Length Then
					m_message = "Dataset " & strDataset & " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": file size mismatch of " & fiCDTAFile.Length & " for job " & intJob & " vs " & oFileInfo.Length & " for job " & oFileInfo.Job)
					Return False
				ElseIf strMD5Hash <> oFileInfo.MD5Hash Then
					m_message = "Dataset " & strDataset & " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": MD5 hash mismatch of " & strMD5Hash & " for job " & intJob & " vs. " & oFileInfo.MD5Hash & " for job " & oFileInfo.Job)
					Return False
				End If
			Else

				Dim strFilename As String = CheckFilenameCase(fiCDTAFile, strDataset)

				oFileInfo = New clsPXFileInfoBase(strFilename)

				' File ID doesn't matter; just use 0
				oFileInfo.FileID = 0
				oFileInfo.Length = fiCDTAFile.Length
				oFileInfo.MD5Hash = strMD5Hash
				oFileInfo.Job = intJob

				mCDTAFileStats.Add(fiCDTAFile.Name, oFileInfo)
			End If

			If Not mDTAtoMGF.ProcessFile(fiCDTAFile.FullName) Then
				m_message = "Error converting " & fiCDTAFile.Name & " to a .mgf file for job " & intJob
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & mDTAtoMGF.GetErrorMessage())
				Return False
			Else
				' Delete the _dta.txt file
				Try
					fiCDTAFile.Delete()
				Catch ex As Exception
					' Ignore errors here
				End Try
			End If

			System.Threading.Thread.Sleep(125)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Dim fiNewMGFFile As IO.FileInfo
			fiNewMGFFile = New IO.FileInfo(IO.Path.Combine(m_WorkDir, strDataset & ".mgf"))

			If Not fiNewMGFFile.Exists Then
				' MGF file was not created
				m_message = "A .mgf file was not created for the _dta.txt file for job " & intJob
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & mDTAtoMGF.GetErrorMessage())
				Return False
			End If

			strMGFFilePath = fiNewMGFFile.FullName

		Catch ex As Exception
			m_message = "Exception in ConvertCDTAToMGF"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

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

	''' <summary>
	''' Counts the number of items of type eFileType in mPxResultFiles
	''' </summary>
	''' <param name="eFileType"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CountResultFilesByType(ByVal eFileType As clsPXFileInfoBase.ePXFileType) As Integer
		Dim intCount As Integer
		intCount = (From item In mPxResultFiles Where item.Value.PXFileType = eFileType Select item).ToList().Count

		Return intCount

	End Function

	''' <summary>
	''' Creates (or retrieves) the .mzXML file for this dataset if it does not exist in the working directory
	''' Utilizes dataset info stored in several packed job parameters
	''' Newly created .mzXML files will be copied to the MSXML_Cache folder
	''' </summary>
	''' <returns>True if the file exists or was created</returns>
	''' <remarks></remarks>
	Protected Function CreateMzXMLFileIfMissing(ByVal intJob As Integer, ByVal strDataset As String, ByVal objAnalysisResults As clsAnalysisResults, ByVal dctDatasetRawFilePaths As Generic.Dictionary(Of String, String)) As Boolean
		Dim blnSuccess As Boolean
		Dim strDestPath As String = String.Empty

		'Dim intDatasetsProcessed As Integer = 0
		'Dim strMSXMLCacheFolderPath As String

		Try
			' Look in m_WorkDir for the .mzXML file for this dataset
			Dim fiMzXmlFilePathLocal As IO.FileInfo
			fiMzXmlFilePathLocal = New IO.FileInfo(IO.Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZXML_EXTENSION))

			If fiMzXmlFilePathLocal.Exists Then
				If Not mPreviousDatasetFilesToDelete.Contains(fiMzXmlFilePathLocal.FullName) Then
					AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName)
				End If
				Return True
			End If

			' .mzXML file not found
			' Look for a StoragePathInfo file
			Dim strMzXmlStoragePathFile As String = fiMzXmlFilePathLocal.FullName & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX

			If IO.File.Exists(strMzXmlStoragePathFile) Then
				blnSuccess = RetrieveStoragePathInfoTargetFile(strMzXmlStoragePathFile, objAnalysisResults, strDestPath)
				If blnSuccess Then
					AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath)
					Return True
				End If
			End If

			' Need to create the .mzXML file
			' We will copy the newly created MSXml files to the MSXML_Cache folder
			Dim strMSXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)

			Dim dctDatasetYearQuarter As Generic.Dictionary(Of String, String)
			dctDatasetYearQuarter = ExtractPackedJobParameterDictionary(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER)

			If Not dctDatasetRawFilePaths.ContainsKey(strDataset) Then
				m_message = "Dataset " & strDataset & " not found in job parameter " & clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS & "; unable to create the missing .mzXML file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt")

			mMSXmlCreator = New AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams)
			mMSXmlCreator.UpdateDatasetName(strDataset)

			' Make sure the dataset file is present in the working directory
			' Copy it locally if necessary

			Dim strDatasetFilePathRemote As String = String.Empty
			Dim strDatasetFilePathLocal As String = String.Empty
			Dim blnDatasetFileIsAFolder As Boolean = False

			strDatasetFilePathRemote = dctDatasetRawFilePaths(strDataset)

			If IO.Directory.Exists(strDatasetFilePathRemote) Then
				blnDatasetFileIsAFolder = True
			End If

			strDatasetFilePathLocal = IO.Path.Combine(m_WorkDir, IO.Path.GetFileName(strDatasetFilePathRemote))

			If blnDatasetFileIsAFolder Then
				' Confirm that the dataset folder exists in the working directory

				If Not IO.Directory.Exists(strDatasetFilePathLocal) Then
					' Directory not found; look for a storage path info file
					If IO.File.Exists(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
						blnSuccess = RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX, objAnalysisResults, IsFolder:=True, strDestPath:=strDestPath)
					Else
						' Copy the dataset folder locally
						objAnalysisResults.CopyDirectory(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite:=True)
					End If
				End If

			Else
				' Confirm that the dataset file exists in the working directory
				If Not IO.File.Exists(strDatasetFilePathLocal) Then
					' File not found; Look for a storage path info file
					If IO.File.Exists(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
						blnSuccess = RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX, objAnalysisResults, strDestPath)
						AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath)
					Else
						' Copy the dataset file locally
						objAnalysisResults.CopyFileWithRetry(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite:=True)
						AddToListIfNew(mPreviousDatasetFilesToDelete, strDatasetFilePathLocal)
					End If
				End If
				m_jobParams.AddResultFileToSkip(IO.Path.GetFileName(strDatasetFilePathLocal))
			End If

			blnSuccess = mMSXmlCreator.CreateMZXMLFile()

			If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
				m_message = mMSXmlCreator.ErrorMessage
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error creating the mzXML file for dataset " & strDataset
				ElseIf Not m_message.Contains(strDataset) Then
					m_message &= "; dataset " & strDataset
				End If
			End If

			If Not blnSuccess Then Return False

			fiMzXmlFilePathLocal.Refresh()
			If fiMzXmlFilePathLocal.Exists Then
				AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName)
			Else
				m_message = "MSXmlCreator did not create the .mzXML file for dataset " & strDataset
				Return False
			End If

			' Copy the .mzXML file to the cache

			Dim strMSXmlGeneratorName As String = IO.Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath)
			Dim strDatasetYearQuarter As String = String.Empty
			If Not dctDatasetYearQuarter.TryGetValue(strDataset, strDatasetYearQuarter) Then
				strDatasetYearQuarter = String.Empty
			End If

			CopyMzXMLFileToServerCache(fiMzXmlFilePathLocal.FullName, strDatasetYearQuarter, strMSXmlGeneratorName, blnPurgeOldFilesIfNeeded:=True)

			m_jobParams.AddResultFileToSkip(IO.Path.GetFileName(fiMzXmlFilePathLocal.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX))

			System.Threading.Thread.Sleep(250)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Try
				If blnDatasetFileIsAFolder Then
					' Delete the local dataset folder
					If IO.Directory.Exists(strDatasetFilePathLocal) Then
						IO.Directory.Delete(strDatasetFilePathLocal, True)
					End If
				Else
					' Delete the local dataset file
					If IO.File.Exists(strDatasetFilePathLocal) Then
						IO.File.Delete(strDatasetFilePathLocal)
					End If
				End If
			Catch ex As Exception
				' Ignore errors here
			End Try

		Catch ex As Exception
			m_message = "Exception in CreateMzXMLFileIfMissing"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Function CreatePseudoMSGFFileUsingPHRPReader( _
	  ByVal intJob As Integer, _
	  ByVal strDataset As String, _
	  ByVal udtFilterThresholds As udtFilterThresholdsType, _
	  ByRef lstPseudoMSGFData As Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType))) As String

		Const MSGF_SPECPROB_NOTDEFINED As Integer = 10
		Const PVALUE_NOTDEFINED As Integer = 10

		Dim dctBestMatchByScan As Generic.Dictionary(Of Integer, Generic.KeyValuePair(Of Double, String))
		Dim dctBestMatchByScanScoreValues As Generic.Dictionary(Of Integer, udtPseudoMSGFDataType)

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
		Dim dblPValue As Double

		Dim dblScoreForCurrentMatch As Double

		Dim blnValidPSM As Boolean
		Dim blnNewScanNumber As Boolean
		Dim blnThresholdChecked As Boolean

		Dim blnFDRValuesArePresent As Boolean = False
		Dim blnPepFDRValuesArePresent As Boolean = False
		Dim blnMSGFValuesArePresent As Boolean = False

		Try

			If Not mDataPackagePeptideHitJobs.TryGetValue(intJob, udtJobInfo) Then
				m_message = "Job " & intJob & " not found in mDataPackagePeptideHitJobs; this is unexpected"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return String.Empty
			End If

			If lstPseudoMSGFData.Count > 0 Then
				lstPseudoMSGFData.Clear()
			End If


			' The .MSGF file can only contain one match for each scan number
			' If it includes multiple matches, then PRIDE Converter crashes when reading the .mzXML file
			' Furthermore, the .msgf-report.xml file cannot have extra entries that are not in the .msgf file
			' Thus, only keep the best-scoring match for each spectrum
			'
			' The keys in each of dctBestMatchByScan and dctBestMatchByScanScoreValues are scan numbers
			' The value for dctBestMatchByScan is a KeyValue pair where the key is the score for this match
			dctBestMatchByScan = New Generic.Dictionary(Of Integer, Generic.KeyValuePair(Of Double, String))
			dctBestMatchByScanScoreValues = New Generic.Dictionary(Of Integer, udtPseudoMSGFDataType)


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

				' Read the data, filtering on either PepFDR or FDR if defined, or MSGF_SpecProb if PepFDR and/or FDR are not available
				While objReader.MoveNext()

					blnValidPSM = True
					blnThresholdChecked = False

					dblMSGFSpecProb = MSGF_SPECPROB_NOTDEFINED
					dblFDR = -1
					dblPepFDR = -1
					dblPValue = PVALUE_NOTDEFINED
					dblScoreForCurrentMatch = 100

					' Determine MSGFSpecProb; store 10 if we don't find a valid number
					If Not Double.TryParse(objReader.CurrentPSM.MSGFSpecProb, dblMSGFSpecProb) Then
						dblMSGFSpecProb = MSGF_SPECPROB_NOTDEFINED
					End If

					Select Case udtJobInfo.PeptideHitResultType
						Case clsPHRPReader.ePeptideHitResultType.Sequest
							If dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED Then
								dblPValue = ComputeApproximatePValue(dblMSGFSpecProb)
								dblScoreForCurrentMatch = dblMSGFSpecProb
								blnMSGFValuesArePresent = True
							Else
								If blnMSGFValuesArePresent Then
									' Skip this result; it had a score value too low to be processed with MSGF
									dblPValue = 1
									blnValidPSM = False
								Else
									dblPValue = 0.025
									' Note: storing 1000-XCorr so that lower values will be considered higher confidence
									dblScoreForCurrentMatch = 1000 - (objReader.CurrentPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_XCorr, 1))
								End If
							End If

						Case clsPHRPReader.ePeptideHitResultType.XTandem
							If dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED Then
								dblPValue = ComputeApproximatePValue(dblMSGFSpecProb)
								dblScoreForCurrentMatch = dblMSGFSpecProb
								blnMSGFValuesArePresent = True
							Else
								If blnMSGFValuesArePresent Then
									' Skip this result; it had a score value too low to be processed with MSGF
									dblPValue = 1
									blnValidPSM = False
								Else
									dblPValue = 0.025
									dblScoreForCurrentMatch = 1000 + objReader.CurrentPSM.GetScoreDbl(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 1)
								End If
							End If

						Case clsPHRPReader.ePeptideHitResultType.Inspect
							dblPValue = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue, PVALUE_NOTDEFINED)

							If dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED Then
								dblScoreForCurrentMatch = dblMSGFSpecProb
							Else
								If blnMSGFValuesArePresent Then
									' Skip this result; it had a score value too low to be processed with MSGF
									dblPValue = 1
									blnValidPSM = False
								Else
									' Note: storing 1000-TotalPRMScore so that lower values will be considered higher confidence
									dblScoreForCurrentMatch = 1000 - (objReader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore, 1))
								End If
							End If

						Case clsPHRPReader.ePeptideHitResultType.MSGFDB
							dblFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1)
							If dblFDR > -1 Then
								blnFDRValuesArePresent = True
							End If

							dblPepFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PepFDR, -1)
							If dblPepFDR > -1 Then
								blnPepFDRValuesArePresent = True
							End If

							dblPValue = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PValue, PVALUE_NOTDEFINED)
							dblScoreForCurrentMatch = dblMSGFSpecProb
					End Select


					If udtFilterThresholds.UseMSGFSpecProb Then
						If dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThreshold Then
							blnValidPSM = False
						End If
						blnThresholdChecked = True

						If Not mFilterThresholdsUsed.UseMSGFSpecProb Then
							mFilterThresholdsUsed.UseMSGFSpecProb = True
							mFilterThresholdsUsed.MSGFSpecProbThreshold = udtFilterThresholds.MSGFSpecProbThreshold
						End If
					End If

					If blnPepFDRValuesArePresent AndAlso udtFilterThresholds.UsePepFDRThreshold Then
						' Typically only MSGFDB results will have PepFDR values
						If dblPepFDR > udtFilterThresholds.PepFDRThreshold Then
							blnValidPSM = False
						End If
						blnThresholdChecked = True

						If Not mFilterThresholdsUsed.UsePepFDRThreshold Then
							mFilterThresholdsUsed.UsePepFDRThreshold = True
							mFilterThresholdsUsed.PepFDRThreshold = udtFilterThresholds.PepFDRThreshold
						End If
					End If

					If blnFDRValuesArePresent AndAlso udtFilterThresholds.UseFDRThreshold Then
						' Typically only MSGFDB results will have FDR values
						If dblFDR > udtFilterThresholds.FDRThreshold Then
							blnValidPSM = False
						End If
						blnThresholdChecked = True

						If Not mFilterThresholdsUsed.UseFDRThreshold Then
							mFilterThresholdsUsed.UseFDRThreshold = True
							mFilterThresholdsUsed.FDRThreshold = udtFilterThresholds.FDRThreshold
						End If

					End If

					If blnValidPSM And Not blnThresholdChecked Then
						' Switch to filtering on MSGFSpecProbThreshold instead of on FDR or PepFDR
						If dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED AndAlso udtFilterThresholds.MSGFSpecProbThreshold < 0.0001 Then
							If dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThreshold Then
								blnValidPSM = False
							End If

							If Not mFilterThresholdsUsed.UseMSGFSpecProb Then
								mFilterThresholdsUsed.UseMSGFSpecProb = True
								mFilterThresholdsUsed.MSGFSpecProbThreshold = udtFilterThresholds.MSGFSpecProbThreshold
							End If
						End If
					End If

					If blnValidPSM Then
						' Filter on P-value
						If dblPValue >= udtFilterThresholds.PValueThreshold Then
							blnValidPSM = False
						End If
					End If

					If blnValidPSM Then

						' Determine the protein index in mCachedProteins

						Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String) = Nothing

						If Not mCachedProteins.TryGetValue(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence) Then

							' Protein not found in mCachedProteins
							' If the search engine is MSGFDB and the protein name starts with REV_ or XXX_ then skip this protein since it's a decoy result
							' Otherwise, add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

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
								kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(mCachedProteins.Count, String.Empty)
								mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, 0)
								mCachedProteins.Add(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence)
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
								strPValue = dblPValue.ToString("0.00")
								strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn)
								strDeltaScoreOther = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn2)

							Case clsPHRPReader.ePeptideHitResultType.XTandem
								strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Hyperscore)
								strPValue = dblPValue.ToString("0.00")
								strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_DeltaCn2)

							Case clsPHRPReader.ePeptideHitResultType.Inspect
								strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore)
								strPValue = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_PValue)
								strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScore)
								strDeltaScoreOther = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScoreOther)

							Case clsPHRPReader.ePeptideHitResultType.MSGFDB
								strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_DeNovoScore)
								strPValue = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_PValue)

						End Select

						' Construct the text that we would write to the pseudo MSGF file
						Dim strMSGFText As String
						strMSGFText = strMzXMLFilename & ControlChars.Tab & _
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

						' Add or update dctBestMatchByScan and dctBestMatchByScanScoreValues
						Dim kvBestMatchForScan As Generic.KeyValuePair(Of Double, String) = Nothing

						If dctBestMatchByScan.TryGetValue(objReader.CurrentPSM.ScanNumber, kvBestMatchForScan) Then
							If dblScoreForCurrentMatch >= kvBestMatchForScan.Key Then
								' Skip this result since it has a lower score than the match already stored in dctBestMatchByScan
								blnValidPSM = False
							Else
								' Update dctBestMatchByScan
								dctBestMatchByScan(objReader.CurrentPSM.ScanNumber) = New Generic.KeyValuePair(Of Double, String)(dblScoreForCurrentMatch, strMSGFText)
								blnValidPSM = True
							End If
							blnNewScanNumber = False
						Else
							' Scan not yet present in dctBestMatchByScan; add it
							kvBestMatchForScan = New Generic.KeyValuePair(Of Double, String)(dblScoreForCurrentMatch, strMSGFText)
							dctBestMatchByScan.Add(objReader.CurrentPSM.ScanNumber, kvBestMatchForScan)
							blnValidPSM = True
							blnNewScanNumber = True
						End If

						If blnValidPSM Then

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
								.Protein = objReader.CurrentPSM.ProteinFirst
							End With

							If blnNewScanNumber Then
								dctBestMatchByScanScoreValues.Add(objReader.CurrentPSM.ScanNumber, udtPseudoMSGFData)
							Else
								dctBestMatchByScanScoreValues(objReader.CurrentPSM.ScanNumber) = udtPseudoMSGFData
							End If

						End If

					End If

				End While

			End Using

			If JobFileRenameRequired(intJob) Then
				strPseudoMsgfFilePath = IO.Path.Combine(m_WorkDir, udtJobInfo.Dataset & "_Job" & udtJobInfo.Job.ToString() & FILE_EXTENSION_PSEUDO_MSGF)
			Else
				strPseudoMsgfFilePath = IO.Path.Combine(m_WorkDir, udtJobInfo.Dataset & FILE_EXTENSION_PSEUDO_MSGF)
			End If

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

				' Write out the filter-passing matches to the pseudo MSGF text file
				For Each kvItem As Generic.KeyValuePair(Of Integer, Generic.KeyValuePair(Of Double, String)) In dctBestMatchByScan
					swMSGFFile.WriteLine(kvItem.Value.Value)
				Next

			End Using

			' Store the filter-passing matches in lstPseudoMSGFData
			For Each kvItem As Generic.KeyValuePair(Of Integer, udtPseudoMSGFDataType) In dctBestMatchByScanScoreValues

				Dim lstMatchesForProtein As Generic.List(Of udtPseudoMSGFDataType) = Nothing
				If lstPseudoMSGFData.TryGetValue(kvItem.Value.Protein, lstMatchesForProtein) Then
					lstMatchesForProtein.Add(kvItem.Value)
				Else
					lstMatchesForProtein = New Generic.List(Of udtPseudoMSGFDataType)
					lstMatchesForProtein.Add(kvItem.Value)
					lstPseudoMSGFData.Add(kvItem.Value.Protein, lstMatchesForProtein)
				End If

			Next


		Catch ex As Exception
			m_message = "Exception in CreatePseudoMSGFFileUsingPHRPReader"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return String.Empty
		End Try


		Return strPseudoMsgfFilePath

	End Function

	''' <summary>
	''' Create the .msgf-report.xml file
	''' </summary>
	''' <param name="intJob"></param>
	''' <param name="strDataset"></param>
	''' <param name="udtFilterThresholds"></param>
	''' <param name="strPrideReportXMLFilePath">Output parameter: the full path of the newly created .msgf-report.xml file</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function CreateMSGFReportFile(ByVal intJob As Integer, ByVal strDataset As String, ByVal udtFilterThresholds As udtFilterThresholdsType, ByRef strPrideReportXMLFilePath As String) As Boolean

		Dim blnSuccess As Boolean

		Dim strTemplateFileName As String
		Dim strPseudoMsgfFilePath As String

		Dim strLocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
		Dim strOrgDBNameGenerated As String
		Dim strProteinCollectionListOrFasta As String

		Dim lstPseudoMSGFData As Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType))
		lstPseudoMSGFData = New Generic.Dictionary(Of String, Generic.List(Of udtPseudoMSGFDataType))

		Try

			strPrideReportXMLFilePath = String.Empty

			strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing:=False)

			strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob), String.Empty)
			If String.IsNullOrEmpty(strOrgDBNameGenerated) Then
				m_message = "Job parameter " & clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob) & " was not found in CreateMSGFReportFile; unable to continue"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			Dim kvJobInfo As clsAnalysisResources.udtDataPackageJobInfoType = Nothing

			If Not mDataPackagePeptideHitJobs.TryGetValue(intJob, kvJobInfo) Then
				m_message = "Job " & intJob & " not found in mDataPackagePeptideHitJobs; unable to continue"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If Not String.IsNullOrEmpty(kvJobInfo.ProteinCollectionList) AndAlso kvJobInfo.ProteinCollectionList <> "na" Then
				strProteinCollectionListOrFasta = kvJobInfo.ProteinCollectionList
			Else
				strProteinCollectionListOrFasta = kvJobInfo.LegacyFastaFileName
			End If

			If mCachedOrgDBName <> strOrgDBNameGenerated Then
				' Need to read the proteins from the fasta file

				mCachedProteins.Clear()
				mCachedProteinPSMCounts.Clear()

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
						If Not mCachedProteins.ContainsKey(objFastaFileReader.ProteinName) Then
							Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String)
							kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(mCachedProteins.Count, objFastaFileReader.ProteinSequence)

							Try
								mCachedProteins.Add(objFastaFileReader.ProteinName, kvIndexAndSequence)
							Catch ex As Exception
								Throw New Exception("Dictionary error adding to mCachedProteins", ex)
							End Try

							Try
								mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, 0)
							Catch ex As Exception
								Throw New Exception("Dictionary error adding to mCachedProteinPSMCounts", ex)
							End Try

						End If
					Loop
					objFastaFileReader.CloseFile()
				End If

				mCachedOrgDBName = String.Copy(strOrgDBNameGenerated)

			Else
				' Reset the counts in mCachedProteinPSMCounts
				For intIndex As Integer = 0 To mCachedProteinPSMCounts.Count
					mCachedProteinPSMCounts(intIndex) = 0
				Next
			End If

			lstPseudoMSGFData.Clear()

			strPseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(intJob, strDataset, udtFilterThresholds, lstPseudoMSGFData)

			If String.IsNullOrEmpty(strPseudoMsgfFilePath) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Pseudo Msgf file not created for job " & intJob & ", dataset " & strDataset
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End If
				Return False
			End If

			AddToListIfNew(mPreviousDatasetFilesToDelete, strPseudoMsgfFilePath)

			If Not mCreateMSGFReportFilesOnly Then

				strPrideReportXMLFilePath = CreateMSGFReportXMLFile(strTemplateFileName, kvJobInfo, strPseudoMsgfFilePath, lstPseudoMSGFData, strOrgDBNameGenerated, strProteinCollectionListOrFasta, udtFilterThresholds)

				If String.IsNullOrEmpty(strPrideReportXMLFilePath) Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Pride report XML file not created for job " & intJob & ", dataset " & strDataset
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					End If
					Return False
				End If

			End If

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportFile for job " & intJob & ", dataset " & strDataset
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
	  ByVal strOrgDBNameGenerated As String, _
	  ByVal strProteinCollectionListOrFasta As String, _
	  ByVal udtFilterThresholds As udtFilterThresholdsType) As String


		Dim strPrideReportXMLFilePath As String = String.Empty

		Dim blnInsideMzDataDescription As Boolean
		Dim blnSkipNode As Boolean
		Dim blnInstrumentDetailsAutoDefined As Boolean = False

		Dim lstAttributeOverride As Generic.Dictionary(Of String, String) = New Generic.Dictionary(Of String, String)

		Dim lstElementCloseDepths As Generic.Stack(Of Integer)

		Dim eFileLocation As eMSGFReportXMLFileLocation = eMSGFReportXMLFileLocation.Header
		Dim lstRecentElements As Collections.Generic.Queue(Of String) = New Collections.Generic.Queue(Of String)


		Try
			lstElementCloseDepths = New Generic.Stack(Of Integer)

			' Open strTemplateFileName and parse it to create a new XML file
			' Use a forward-only XML reader, copying some elements verbatim and customizing others
			' When we reach <Identifications>, we write out the data that was cached from strPseudoMsgfFilePath
			'    Must write out data by protein

			' Next, append the protein sequences in mCachedProteinPSMCounts to the <Fasta></Fasta> section

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

								If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
								lstRecentElements.Enqueue("Element " & objXmlReader.Name)

								Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
									Dim intPoppedVal As Integer
									intPoppedVal = lstElementCloseDepths.Pop()

									objXmlWriter.WriteEndElement()
								Loop

								eFileLocation = UpdateMSGFReportXMLFileLocation(eFileLocation, objXmlReader.Name, blnInsideMzDataDescription)

								blnSkipNode = False
								lstAttributeOverride.Clear()

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

									Case "sampleDescription"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
											' Override the comment attribute for this node
											Dim strCommentOverride As String

											If Not String.IsNullOrWhiteSpace(udtJobInfo.Experiment_Reason) Then
												strCommentOverride = udtJobInfo.Experiment_Reason.TrimEnd()

												If Not String.IsNullOrWhiteSpace(udtJobInfo.Experiment_Comment) Then
													If strCommentOverride.EndsWith(".") Then
														strCommentOverride &= " " & udtJobInfo.Experiment_Comment
													Else
														strCommentOverride &= ". " & udtJobInfo.Experiment_Comment
													End If
												End If
											Else
												strCommentOverride = udtJobInfo.Experiment_Comment
											End If

											lstAttributeOverride.Add("comment", strCommentOverride)
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

									Case "instrumentName"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument Then
											' Write out the actual instrument name
											objXmlWriter.WriteElementString("instrumentName", udtJobInfo.Instrument)
											blnSkipNode = True

											blnInstrumentDetailsAutoDefined = WriteXMLInstrumentInfo(objXmlWriter, udtJobInfo.InstrumentGroup)

										End If

									Case "source", "analyzerList", "detector"
										If eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument AndAlso blnInstrumentDetailsAutoDefined Then
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
										If Not CreateMSGFReportXMLFileWriteIDs(objXmlWriter, lstPseudoMSGFData, strOrgDBNameGenerated, udtJobInfo.PeptideHitResultType) Then
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreateMSGFReportXMLFileWriteIDs returned false; aborting")
											Return String.Empty
										End If

										If Not CreateMSGFReportXMLFileWriteProteins(objXmlWriter, lstPseudoMSGFData, strOrgDBNameGenerated) Then
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
										WriteConfigurationOption(objXmlWriter, "peptide_threshold", udtFilterThresholds.PValueThreshold.ToString("0.00"))
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
											Dim strAttributeOverride As String = String.Empty
											If lstAttributeOverride.Count > 0 AndAlso lstAttributeOverride.TryGetValue(objXmlReader.Name, strAttributeOverride) Then
												objXmlWriter.WriteAttributeString(objXmlReader.Name, strAttributeOverride)
											Else
												objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)
											End If

										Loop While objXmlReader.MoveToNextAttribute()

										lstElementCloseDepths.Push(objXmlReader.Depth)

									ElseIf objXmlReader.IsEmptyElement Then
										objXmlWriter.WriteEndElement()
									End If

								End If

							Case Xml.XmlNodeType.EndElement

								If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
								lstRecentElements.Enqueue("EndElement " & objXmlReader.Name)

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

								If Not String.IsNullOrEmpty(objXmlReader.Value) Then
									If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
									If objXmlReader.Value.Length > 10 Then
										lstRecentElements.Enqueue(objXmlReader.Value.Substring(0, 10))
									Else
										lstRecentElements.Enqueue(objXmlReader.Value)
									End If
								End If

								objXmlWriter.WriteString(objXmlReader.Value)

						End Select

					Loop

				End Using

				objXmlWriter.WriteEndDocument()
			End Using


		Catch ex As Exception
			m_message = "Exception in CreateMSGFReportXMLFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Dim strRecentElements As String = String.Empty
			For Each strItem In lstRecentElements
				If String.IsNullOrEmpty(strRecentElements) Then
					strRecentElements = String.Copy(strItem)
				Else
					strRecentElements &= "; " & strItem
				End If
			Next

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strRecentElements)

			Return String.Empty
		End Try

		Return strPrideReportXMLFilePath

	End Function

	Protected Function CreateMSGFReportXMLFileWriteIDs( _
	  ByRef objXmlWriter As Xml.XmlTextWriter,
	  ByRef lstPseudoMSGFData As Dictionary(Of String, List(Of udtPseudoMSGFDataType)),
	  ByVal strOrgDBNameGenerated As String, _
	  ByVal ePeptideHitResultType As clsPHRPReader.ePeptideHitResultType) As Boolean

		Try

			objXmlWriter.WriteStartElement("Identifications")

			For Each kvProteinEntry As Generic.KeyValuePair(Of String, Generic.List(Of udtPseudoMSGFDataType)) In lstPseudoMSGFData

				Dim kvIndexAndSequence As Generic.KeyValuePair(Of Integer, String) = Nothing

				If Not mCachedProteins.TryGetValue(kvProteinEntry.Key, kvIndexAndSequence) Then
					' Protein not found in mCachedProteins; this is unexpected (should have already been added by CreatePseudoMSGFFileUsingPHRPReader()
					' Add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

					kvIndexAndSequence = New Generic.KeyValuePair(Of Integer, String)(mCachedProteins.Count, String.Empty)
					mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, kvProteinEntry.Value.Count)
					mCachedProteins.Add(kvProteinEntry.Key, kvIndexAndSequence)

				Else
					mCachedProteinPSMCounts(kvIndexAndSequence.Key) = kvProteinEntry.Value.Count
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

					' Possible ToDo: Write out details of dynamic mods
					'                Would need to update DMS to include the PSI-Compatible mod names, descriptions, and masses.
					'                However, since we're now submitting .mzID files to PRIDE and not .msgf-report.xml files, this update is not necessary
					'
					' XML format:
					' <ModificationItem>
					'     <ModLocation>10</ModLocation> 
					'     <ModAccession>MOD:00425</ModAccession>
					'     <ModDatabase>MOD</ModDatabase> 
					'     <ModMonoDelta>15.994915</ModMonoDelta>
					'     <additional> 
					'         <cvParam cvLabel="MOD" accession="MOD:00425" name="monohydroxylated residue" value="15.994915" /> 
					'     </additional> 
					' </ModificationItem>

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
	  ByVal strOrgDBNameGenerated As String) As Boolean

		Dim strProteinName As String
		Dim intProteinIndex As Integer
		Dim intPSMCount As Integer
		Try

			objXmlWriter.WriteStartElement("Fasta")
			objXmlWriter.WriteAttributeString("sourceDb", strOrgDBNameGenerated)
			objXmlWriter.WriteAttributeString("sourceDbVersion", "Unknown")

			' Step through mCachedProteins
			' For each entry, the key is the protein name
			' The value is itself a key-value pair, where Value.Key is the protein index and Value.Value is the protein sequence
			For Each kvEntry As Generic.KeyValuePair(Of String, Generic.KeyValuePair(Of Integer, String)) In mCachedProteins

				strProteinName = String.Copy(kvEntry.Key)
				intProteinIndex = kvEntry.Value.Key

				' Only write out this protein if it had 1 or more PSMs
				If mCachedProteinPSMCounts.TryGetValue(intProteinIndex, intPSMCount) Then
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

	''' <summary>
	''' Create the .msgf-pride.xml file using the .msgf-report.xml file
	''' </summary>
	''' <param name="intJob"></param>
	''' <param name="strDataset"></param>
	''' <param name="strPrideReportXMLFilePath"></param>
	''' <param name="strPrideXmlFilePath">Output parameter: the full path of the newly created .msgf-pride.xml file</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function CreatePrideXMLFile(ByVal intJob As Integer, ByVal strDataset As String, ByVal strPrideReportXMLFilePath As String, ByRef strPrideXmlFilePath As String) As Boolean

		Dim blnSuccess As Boolean
		Dim strCurrentTask As String

		Dim strBaseFileName As String
		Dim strMsgfResultsFilePath As String
		Dim strMzXMLFilePath As String

		Try
			strPrideXmlFilePath = String.Empty

			strBaseFileName = IO.Path.GetFileName(strPrideReportXMLFilePath).Replace(FILE_EXTENSION_MSGF_REPORT_XML, String.Empty)
			strMsgfResultsFilePath = IO.Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_PSEUDO_MSGF)
			strMzXMLFilePath = IO.Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
			strPrideReportXMLFilePath = IO.Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_MSGF_REPORT_XML)

			strCurrentTask = "Running PRIDE Converter for job " & intJob & ", " & strDataset
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strCurrentTask)
			End If

			blnSuccess = RunPrideConverter(intJob, strDataset, strMsgfResultsFilePath, strMzXMLFilePath, strPrideReportXMLFilePath)

			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error calling RunPrideConverter"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End If
			Else
				' Make sure the result file was created
				strPrideXmlFilePath = IO.Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_MSGF_PRIDE_XML)
				If Not IO.File.Exists(strPrideXmlFilePath) Then
					m_message = "Pride XML file not created for job " & intJob & ": " & strPrideXmlFilePath
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If
			End If

			blnSuccess = True

		Catch ex As Exception
			m_message = "Exception in CreatePrideXMLFile for job " & intJob & ", dataset " & strDataset
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return blnSuccess

	End Function

	Protected Sub WritePXInstruments(ByVal swPXFile As IO.StreamWriter)

		For Each kvInstrumentGroup In mInstrumentGroupsStored

			Dim strAccession As String = String.Empty
			Dim strDescription As String = String.Empty

			Select Case kvInstrumentGroup.Key

				Case "Agilent_GC-MS"
					' This is an Agilent 7890A with a 5975C detector
					' The closest match is an LC/MS system

					strAccession = "MS:1000471"
					strDescription = "6140 Quadrupole LC/MS"

				Case "Agilent_TOF_V2"
					strAccession = "MS:1000472"
					strDescription = "6210 Time-of-Flight LC/MS"

				Case "Bruker_Amazon_Ion_Trap"
					strAccession = "MS:1001542"
					strDescription = "amaZon ETD"

				Case "Bruker_FTMS"
					strAccession = "MS:1001549"
					strDescription = "solariX"

				Case "Bruker_QTOF"
					strAccession = "MS:1001537"
					strDescription = "BioTOF"

				Case "Exactive"
					strAccession = "MS:1000649"
					strDescription = "Exactive"

				Case "TSQ", "GC-TSQ"
					If kvInstrumentGroup.Value.Contains("TSQ_2") AndAlso kvInstrumentGroup.Value.Count = 1 Then
						' TSQ_1 is a TSQ Quantum Ultra
						strAccession = "MS:1000751"
						strDescription = "TSQ Quantum Ultra"
					Else
						' TSQ_3 and TSQ_4 are TSQ Vantage instruments
						strAccession = "MS:1001510"
						strDescription = "TSQ Vantage"
					End If

				Case "LCQ"
					strAccession = "MS:1000554"
					strDescription = "LCQ Deca"

				Case "LTQ", "LTQ-Prep"
					strAccession = "MS:1000447"
					strDescription = "LTQ"

				Case "LTQ-ETD"
					strAccession = "MS:1000638"
					strDescription = "LTQ XL ETD"

				Case "LTQ-FT"
					strAccession = "MS:1000448"
					strDescription = "LTQ FT"

				Case "Orbitrap"
					strAccession = "MS:1000449"
					strDescription = "LTQ Orbitrap"

				Case "QExactive"
					strAccession = "MS:1001911"
					strDescription = "Q Exactive"

				Case "Sciex_QTrap"
					strAccession = "MS:1000931"
					strDescription = "QTRAP 5500"

				Case "Sciex_TripleTOF"
					strAccession = "MS:1000932"
					strDescription = "TripleTOF 5600"

				Case "VelosOrbi"
					strAccession = "MS:1001742"
					strDescription = "LTQ Orbitrap Velos"

				Case "VelosPro"
					' Note that VPro01 is actually a Velos Pro
					strAccession = "MS:1000855"
					strDescription = "LTQ Velos"

			End Select

			Dim strInstrumentCV As String
			If String.IsNullOrEmpty(strAccession) Then
				strInstrumentCV = "[MS," & "MS:1000449" & "," & "LTQ Orbitrap" & ",]"
			Else
				strInstrumentCV = "[MS," & strAccession & "," & strDescription & ",]"
			End If

			WritePXHeader(swPXFile, "instrument", strInstrumentCV)
		Next

	End Sub

	Protected Sub WritePXMods(ByVal swPXFile As IO.StreamWriter)

		If mModificationsUsed.Count = 0 Then
			WritePXHeader(swPXFile, "modification", "[PRIDE,PRIDE:0000398,No PTMs are included in the dataset,]")
		Else
			' Write out each modification, for example:
			' modification	[UNIMOD,UNIMOD:35,Oxidation,]
			For Each item In mModificationsUsed
				WritePXHeader(swPXFile, "modification", "[" & item.Value.CvRef & "," & item.Value.Accession & "," & item.Value.Name & "," & item.Value.Value & "]")
			Next
		End If

	End Sub

	Protected Function CreatePXSubmissionFile() As Boolean

		Const TBD As String = "******* UPDATE ****** "

		Dim intPrideXmlFilesCreated As Integer
		Dim intRawFilesStored As Integer
		Dim intPeakFilesStored As Integer
		Dim intMzIDFilesStored As Integer

		Dim strSubmissionType As String
		Dim strFilterText As String = String.Empty

		Dim dctParameters As Generic.Dictionary(Of String, String)
		Dim strPXFilePath As String

		Try

			' Read the PX_Submission_Template.px file
			dctParameters = ReadTemplatePXSubmissionFile()

			strPXFilePath = IO.Path.Combine(m_WorkDir, "PX_Submission_" & System.DateTime.Now.ToString("yyyy-MM-dd_HH-mm") & ".px")

			intPrideXmlFilesCreated = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Result)
			intRawFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Raw)
			intPeakFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Peak)
			intMzIDFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Search)

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating PXSubmission file: " & strPXFilePath)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intPrideXmlFilesCreated & " Result (.msgf-pride.xml) files")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intRawFilesStored & " Raw files")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intPeakFilesStored & " Peak (.mgf) files")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intMzIDFilesStored & " Search (.mzid) files")
			End If

			If intPrideXmlFilesCreated = 0 Then
				strSubmissionType = "UNSUPPORTED"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Did not create any Pride XML result files; submission type is " & strSubmissionType)

			ElseIf intMzIDFilesStored > intPrideXmlFilesCreated Then
				strSubmissionType = "UNSUPPORTED"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Stored more Search (.mzid) files than Pride XML result files; submission type is " & strSubmissionType)

			ElseIf intRawFilesStored > intPrideXmlFilesCreated Then
				strSubmissionType = "UNSUPPORTED"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Stored more Raw files than Pride XML result files; submission type is " & strSubmissionType)

			Else
				strSubmissionType = "SUPPORTED"

				If mFilterThresholdsUsed.UseFDRThreshold OrElse mFilterThresholdsUsed.UsePepFDRThreshold OrElse mFilterThresholdsUsed.UseMSGFSpecProb Then
					Dim strFilterTextBase As String = "msgf-pride.xml files are filtered on "
					strFilterText = String.Empty

					If mFilterThresholdsUsed.UseFDRThreshold Then
						If String.IsNullOrEmpty(strFilterText) Then
							strFilterText = strFilterTextBase
						Else
							strFilterText &= " and "
						End If

						strFilterText &= (mFilterThresholdsUsed.FDRThreshold * 100).ToString("0.0") & "% FDR at the PSM level"
					End If

					If mFilterThresholdsUsed.UsePepFDRThreshold Then
						If String.IsNullOrEmpty(strFilterText) Then
							strFilterText = strFilterTextBase
						Else
							strFilterText &= " and "
						End If

						strFilterText &= (mFilterThresholdsUsed.PepFDRThreshold * 100).ToString("0.0") & "% FDR at the peptide level"
					End If

					If mFilterThresholdsUsed.UseMSGFSpecProb Then
						If String.IsNullOrEmpty(strFilterText) Then
							strFilterText = strFilterTextBase
						Else
							strFilterText &= " and "
						End If

						strFilterText &= "MSGF Spectral Probability <= " & mFilterThresholdsUsed.MSGFSpecProbThreshold.ToString("0.0E+00")
					End If

				End If

			End If

			Using swPXFile As IO.StreamWriter = New IO.StreamWriter(New IO.FileStream(strPXFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

				WritePXHeader(swPXFile, "name", "Matthew Monroe", dctParameters)
				WritePXHeader(swPXFile, "email", "matthew.monroe@pnnl.gov", dctParameters)
				WritePXHeader(swPXFile, "affiliation", "Pacific Northwest National Laboratory", dctParameters)
				WritePXHeader(swPXFile, "pride_login", "alchemistmatt", dctParameters)

				WritePXHeader(swPXFile, "title", TBD & "Journal Article Title", dctParameters)
				WritePXHeader(swPXFile, "description", TBD & "Summary sentence", dctParameters)

				If dctParameters.ContainsKey("pubmed") Then
					WritePXHeader(swPXFile, "pubmed", TBD, dctParameters)
				End If

				WritePXHeader(swPXFile, "keywords", TBD, dctParameters)
				WritePXHeader(swPXFile, "type", strSubmissionType, dctParameters)

				If strSubmissionType = "SUPPORTED" Then
					WritePXHeader(swPXFile, "comment", strFilterText)
				Else
					Dim strComment As String = "Data produced by the DMS Processing pipeline using "
					If mSearchToolsUsed.Count = 1 Then
						strComment &= "search tool " & mSearchToolsUsed.First
					ElseIf mSearchToolsUsed.Count = 2 Then
						strComment &= "search tools " & mSearchToolsUsed.First & " and " & mSearchToolsUsed.Last
					ElseIf mSearchToolsUsed.Count > 2 Then
						strComment &= "search tools " & clsGlobal.FlattenList((From item In mSearchToolsUsed Where item <> mSearchToolsUsed.Last Order By item).ToList, ","c) & " and " & mSearchToolsUsed.Last
					End If

					WritePXHeader(swPXFile, "comment", strComment)
				End If

				If mExperimentNEWTInfo.Count = 0 Then
					' None of the data package jobs had valid NEWT info
					WritePXHeader(swPXFile, "species", TBD & "[NEWT,10090,Mus musculus (Mouse),]", dctParameters)
				Else
					' NEWT info is defined; write it out
					For Each item In mExperimentNEWTInfo
						WritePXHeader(swPXFile, "species", "[NEWT," & item.Key & "," & item.Value & ",]")
					Next
				End If

				If mInstrumentGroupsStored.Count > 0 Then
					WritePXInstruments(swPXFile)
				Else
					WritePXHeader(swPXFile, "instrument", "[MS," & "MS:1000449" & "," & "LTQ Orbitrap" & ",]", dctParameters)
				End If

				WritePXMods(swPXFile)

				' Add a blank line
				swPXFile.WriteLine()

				' Write the header row for the files
				WritePXLine(swPXFile, New Generic.List(Of String) From {"FMH", "file_id", "file_type", "file_path", "file_mapping"})

				Dim lstFileInfoCols As Generic.List(Of String) = New Generic.List(Of String)

				' Append the files and mapping information to the ProteomeXchange PX file
				For Each item In mPxResultFiles
					lstFileInfoCols.Clear()

					lstFileInfoCols.Add("FME")
					lstFileInfoCols.Add(item.Key.ToString)
					lstFileInfoCols.Add(PXFileTypeName(item.Value.PXFileType))
					lstFileInfoCols.Add(IO.Path.Combine("D:\Upload", m_ResFolderName, item.Value.Filename))

					Dim strFileMappings As Generic.List(Of String) = New Generic.List(Of String)
					For Each mapID In item.Value.FileMappings
						strFileMappings.Add(mapID.ToString())
					Next

					lstFileInfoCols.Add(clsGlobal.FlattenList(strFileMappings, ","c))

					WritePXLine(swPXFile, lstFileInfoCols)

				Next

			End Using

		Catch ex As Exception
			m_message = "Exception in CreatePXSubmissionFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Sub DefineFilesToSkipTransfer()

		m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_PSEUDO_MSGF)
		m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_MSGF_REPORT_XML)
		m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX)

		m_jobParams.AddResultFileToSkip("PRIDEConverter_ConsoleOutput.txt")
		m_jobParams.AddResultFileToSkip("PRIDEConverter_Version.txt")

		Dim diWorkDir As System.IO.DirectoryInfo = New System.IO.DirectoryInfo(m_WorkDir)
		For Each fiFile In diWorkDir.GetFiles(clsAnalysisResources.JOB_INFO_FILE_PREFIX & "*.txt")
			m_jobParams.AddResultFileToSkip(fiFile.Name)
		Next

	End Sub

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

	Protected Function DefinePxFileMapping(ByVal intFileID As Integer, ByVal intParentFileID As Integer) As Boolean

		Dim oPXFileInfo As clsPXFileInfo = Nothing

		If Not mPxResultFiles.TryGetValue(intFileID, oPXFileInfo) Then
			m_message = "FileID " & intFileID & " not found in mPxResultFiles; unable to add parent file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		oPXFileInfo.AddFileMapping(intParentFileID)

		Return True

	End Function

	Protected Function ExtractPackedJobParameterDictionary(ByVal strPackedJobParameterName As String) As Generic.Dictionary(Of String, String)

		Dim lstData As Generic.List(Of String)
		Dim dctData As Generic.Dictionary(Of String, String) = New Generic.Dictionary(Of String, String)

		lstData = ExtractPackedJobParameterList(strPackedJobParameterName)

		For Each strItem In lstData
			Dim intEqualsIndex = strItem.LastIndexOf("="c)
			If intEqualsIndex > 0 Then
				Dim strKey As String = strItem.Substring(0, intEqualsIndex)
				Dim strValue As String = strItem.Substring(intEqualsIndex + 1)

				If Not dctData.ContainsKey(strKey) Then
					dctData.Add(strKey, strValue)
				End If
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Packed dictionary item does not contain an equals sign: " & strItem)
			End If
		Next

		Return dctData

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

	Protected Function GetPrideConverterVersion(ByVal strPrideConverterProgLoc As String) As String

		Dim CmdStr As String
		Dim strVersionFilePath As String
		Dim strPRIDEConverterVersion As String = "unknown"

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		m_StatusTools.CurrentOperation = "Determining PrideConverter Version"
		m_StatusTools.UpdateAndWrite(m_progress)
		strVersionFilePath = System.IO.Path.Combine(m_WorkDir, "PRIDEConverter_Version.txt")

		CmdStr = "-jar " & PossiblyQuotePath(strPrideConverterProgLoc)

		CmdStr &= " -converter -version"

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc & " " & CmdStr)
		End If

		With CmdRunner
			.CreateNoWindow = False
			.CacheStandardOutput = False
			.EchoOutputToConsole = False

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = strVersionFilePath
			.WorkDir = m_WorkDir
		End With

		Dim blnSuccess As Boolean
		blnSuccess = CmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", True)

		' Assure that the console output file has been parsed
		ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			m_message = "Error running PrideConverter to determine its version"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
		Else
			Dim fiVersionFile As System.IO.FileInfo
			fiVersionFile = New System.IO.FileInfo(strVersionFilePath)

			If fiVersionFile.Exists Then
				' Open the version file and read the version
				Using srVersionFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(fiVersionFile.FullName, IO.FileMode.Open, IO.FileAccess.Read))
					If srVersionFile.Peek > -1 Then
						strPRIDEConverterVersion = srVersionFile.ReadLine()
					End If
				End Using
			End If

		End If

		Return strPRIDEConverterVersion

	End Function

	Protected Function InitializeOptions() As udtFilterThresholdsType

		' Update the processing options
		mCreatePrideXMLFiles = m_jobParams.GetJobParameter("CreatePrideXMLFiles", False)

		mCreateMSGFReportFilesOnly = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", False)
		mCreateMGFFiles = m_jobParams.GetJobParameter("CreateMGFFiles", True)
		mProcessMzIdFiles = m_jobParams.GetJobParameter("IncludeMzIdFiles", True)

		If mCreateMSGFReportFilesOnly Then
			mCreateMGFFiles = False
			mProcessMzIdFiles = False
			mCreatePrideXMLFiles = False
		End If

		mCachedOrgDBName = String.Empty

		' Initialize the protein dictionaries			
		mCachedProteins = New Generic.Dictionary(Of String, Generic.KeyValuePair(Of Integer, String))
		mCachedProteinPSMCounts = New Generic.Dictionary(Of Integer, Integer)

		' Initialize the PXFile lists
		mPxMasterFileList = New Generic.Dictionary(Of String, clsPXFileInfoBase)(StringComparer.CurrentCultureIgnoreCase)
		mPxResultFiles = New Generic.Dictionary(Of Integer, clsPXFileInfo)

		' Initialize the CDTAFileStats dictionary
		mCDTAFileStats = New Generic.Dictionary(Of String, clsPXFileInfoBase)(StringComparer.CurrentCultureIgnoreCase)

		' Clear the previous dataset objects
		mPreviousDatasetName = String.Empty
		mPreviousDatasetFilesToDelete = New Generic.List(Of String)
		mPreviousDatasetFilesToCopy = New Generic.List(Of String)

		' Initialize additional items
		mFilterThresholdsUsed = New udtFilterThresholdsType
		mInstrumentGroupsStored = New Generic.Dictionary(Of String, Generic.List(Of String))
		mSearchToolsUsed = New Generic.SortedSet(Of String)
		mExperimentNEWTInfo = New Generic.Dictionary(Of Integer, String)

		mModificationsUsed = New Generic.Dictionary(Of String, udtCvParamInfoType)(StringComparer.CurrentCultureIgnoreCase)

		' Determine the filter thresholds
		Dim udtFilterThresholds As udtFilterThresholdsType
		udtFilterThresholds.Clear()
		With udtFilterThresholds
			.PValueThreshold = m_jobParams.GetJobParameter("PValueThreshold", udtFilterThresholds.PValueThreshold)
			.FDRThreshold = m_jobParams.GetJobParameter("FDRThreshold", udtFilterThresholds.FDRThreshold)
			.PepFDRThreshold = m_jobParams.GetJobParameter("PepFDRThreshold", udtFilterThresholds.PepFDRThreshold)
			.MSGFSpecProbThreshold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.MSGFSpecProbThreshold)

			.UseFDRThreshold = m_jobParams.GetJobParameter("UseFDRThreshold", udtFilterThresholds.UseFDRThreshold)
			.UsePepFDRThreshold = m_jobParams.GetJobParameter("UsePepFDRThreshold", udtFilterThresholds.UsePepFDRThreshold)
			.UseMSGFSpecProb = m_jobParams.GetJobParameter("UseMSGFSpecProb", udtFilterThresholds.UseMSGFSpecProb)
		End With

		Return udtFilterThresholds

	End Function

	''' <summary>
	''' Returns True if the there are multiple jobs in mDataPackagePeptideHitJobs for the dataset for the specified job
	''' </summary>
	''' <param name="intJob"></param>
	''' <returns>True if this job's dataset has multiple jobs in mDataPackagePeptideHitJobs, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function JobFileRenameRequired(ByVal intJob As Integer) As Boolean

		Dim udtJobInfo As clsAnalysisResourcesPRIDEConverter.udtDataPackageJobInfoType = Nothing

		If mDataPackagePeptideHitJobs.TryGetValue(intJob, udtJobInfo) Then
			Dim strDataset As String = udtJobInfo.Dataset

			Dim intJobsForDataset As Integer = (From item In mDataPackagePeptideHitJobs Where item.Value.Dataset = strDataset).ToList.Count()

			If intJobsForDataset > 1 Then
				Return True
			Else
				Return False
			End If
		End If

		Return False

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
				m_message = "Packed job parameter " & clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS & " is empty; no jobs to process"
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
		' 2012-11-20 16:58:47,333 INFO ReportUnmarshallerFactory - Unmarshaller Initialized
		' 2012-11-20 16:58:47,333 INFO ReportReader - Creating index:
		' 2012-11-20 16:58:49,860 INFO ReportMarshallerFactory - Marshaller Initialized
		' Writing PRIDE XML to F:\DMS_WorkDir5\AID_MAC_001_R1_20Nov07_Draco_07-07-19_Job863734.msgf-pride.xml
		' 2012-11-20 16:58:49,860 INFO PrideXmlWriter - DAO Configuration: {search_engine=MSGF, peptide_threshold=0.05, add_carbamidomethylation=false}
		' 2012-11-20 16:58:49,860 WARN PrideXmlWriter - Writing file : F:\DMS_WorkDir5\AID_MAC_001_R1_20Nov07_Draco_07-07-19_Job863734.msgf-pride.xml
		' 2012-11-20 16:59:01,124 INFO PrideXmlWriter - Marshalled 1000 spectra
		' 2012-11-20 16:59:01,124 INFO PrideXmlWriter - Used: 50 Free: 320 Heap size: 371 Xmx: 2728
		' 2012-11-20 16:59:02,231 INFO PrideXmlWriter - Marshalled 2000 spectra
		' 2012-11-20 16:59:02,231 INFO PrideXmlWriter - Used: 214 Free: 156 Heap size: 371 Xmx: 2728
		' 2012-11-20 16:59:03,152 INFO PrideXmlWriter - Marshalled 3000 spectra
		' 2012-11-20 16:59:03,152 INFO PrideXmlWriter - Used: 128 Free: 223 Heap size: 351 Xmx: 2728
		' 2012-11-20 16:59:04,103 INFO PrideXmlWriter - Marshalled 4000 spectra
		' 2012-11-20 16:59:04,103 INFO PrideXmlWriter - Used: 64 Free: 278 Heap size: 342 Xmx: 2728
		' 2012-11-20 16:59:05,258 INFO PrideXmlWriter - Marshalled 5000 spectra
		' 2012-11-20 16:59:05,258 INFO PrideXmlWriter - Used: 21 Free: 312 Heap size: 333 Xmx: 2728
		' 2012-11-20 16:59:06,693 ERROR StandardXpathAccess - The index does not contain any entry for the requested xpath: /Report/PTMs/PTM

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

				mConsoleOutputErrorMsg = String.Empty

				intLinesRead = 0
				Do While srInFile.Peek() > -1
					strLineIn = srInFile.ReadLine()
					intLinesRead += 1

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						If strLineIn.ToLower.Contains(" error ") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running Pride Converter:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
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

	Protected Function ProcessJob(
	  ByVal kvJobInfo As Generic.KeyValuePair(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType),
	  ByVal udtFilterThresholds As udtFilterThresholdsType,
	  ByVal objAnalysisResults As clsAnalysisResults,
	  ByVal dctDatasetRawFilePaths As Generic.Dictionary(Of String, String)) As IJobParams.CloseOutType

		Dim intJob As Integer
		Dim strDataset As String
		Dim blnSuccess As Boolean
		Dim udtResultFiles As udtResultFileContainerType

		intJob = kvJobInfo.Value.Job
		strDataset = kvJobInfo.Value.Dataset

		If mPreviousDatasetName <> strDataset Then

			TransferPreviousDatasetFiles(objAnalysisResults)

			' Retrieve the dataset files for this dataset
			mPreviousDatasetName = strDataset

			If mCreatePrideXMLFiles And Not mCreateMSGFReportFilesOnly Then
				' Create the .mzXML files if it is missing
				blnSuccess = CreateMzXMLFileIfMissing(intJob, strDataset, objAnalysisResults, dctDatasetRawFilePaths)
				If Not blnSuccess Then
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If
			End If

		End If

		' Update the cached analysis tool names
		If Not mSearchToolsUsed.Contains(kvJobInfo.Value.Tool) Then
			mSearchToolsUsed.Add(kvJobInfo.Value.Tool)
		End If

		' Update the cached NEWT info
		AddNewtInfo(kvJobInfo.Value.Experiment_NEWT_ID, kvJobInfo.Value.Experiment_NEWT_Name)

		' Retrieve the PHRP files, MSGF+ results, and _dta.txt file for this job
		blnSuccess = RetrievePHRPFiles(intJob, strDataset, objAnalysisResults)
		If Not blnSuccess Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		udtResultFiles.MGFFilePath = String.Empty
		If mCreateMGFFiles Then
			' Convert the _dta.txt file to .mgf files
			blnSuccess = ConvertCDTAToMGF(intJob, strDataset, udtResultFiles.MGFFilePath)
			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Else
			' Store the path to the _dta.txt file instead of the path to the .mgf file
			udtResultFiles.MGFFilePath = IO.Path.Combine(m_WorkDir, strDataset & "_dta.txt")
			If Not IO.File.Exists(udtResultFiles.MGFFilePath) Then
				udtResultFiles.MGFFilePath = String.Empty
			End If
		End If

		' Update the .mzID file for this job
		udtResultFiles.MzIDFilePath = String.Empty
		If mProcessMzIdFiles AndAlso kvJobInfo.Value.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then

			blnSuccess = UpdateMzIdFile(intJob, strDataset, mCreateMGFFiles, udtResultFiles.MzIDFilePath)
			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		End If

		' Store the instrument group and instrument name
		StoreInstrumentInfo(kvJobInfo.Value)

		udtResultFiles.PrideXmlFilePath = String.Empty
		If mCreatePrideXMLFiles Then

			' Create the .msgf-report.xml file for this job
			Dim strPrideReportXMLFilePath As String = String.Empty
			blnSuccess = CreateMSGFReportFile(intJob, strDataset, udtFilterThresholds, strPrideReportXMLFilePath)
			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			AddToListIfNew(mPreviousDatasetFilesToDelete, strPrideReportXMLFilePath)

			If Not mCreateMSGFReportFilesOnly Then
				' Create the .msgf-Pride.xml file for this job
				blnSuccess = CreatePrideXMLFile(intJob, strDataset, strPrideReportXMLFilePath, udtResultFiles.PrideXmlFilePath)
				If Not blnSuccess Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

		End If

		blnSuccess = AppendToPXFileInfo(intJob, strDataset, dctDatasetRawFilePaths, udtResultFiles)

		If blnSuccess Then
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

	End Function

	Protected Function PXFileTypeName(ePXFileType As clsPXFileInfo.ePXFileType) As String
		Select Case ePXFileType
			Case clsPXFileInfoBase.ePXFileType.Result
				Return "result"
			Case clsPXFileInfoBase.ePXFileType.Raw
				Return "raw"
			Case clsPXFileInfoBase.ePXFileType.Search
				Return "search"
			Case clsPXFileInfoBase.ePXFileType.Peak
				Return "peak"
			Case Else
				Return "other"
		End Select
	End Function

	''' <summary>
	''' Reads the template PX Submission file
	''' Caches the keys and values for the method lines (which start with MTD)
	''' </summary>
	''' <returns>Dictionary of keys and values</returns>
	''' <remarks></remarks>
	Protected Function ReadTemplatePXSubmissionFile() As Generic.Dictionary(Of String, String)

		Dim strTemplateFileName As String
		Dim strTemplateFilePath As String
		Dim strLineIn As String

		Dim dctParameters As Generic.Dictionary(Of String, String)
		dctParameters = New Generic.Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

		Try
			strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing:=False)
			strTemplateFilePath = IO.Path.Combine(m_WorkDir, strTemplateFileName)

			If IO.File.Exists(strTemplateFilePath) Then
				Using srTemplateFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(strTemplateFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
					While srTemplateFile.Peek > -1
						strLineIn = srTemplateFile.ReadLine

						If Not String.IsNullOrEmpty(strLineIn) Then
							If strLineIn.StartsWith("MTD") Then

								Dim lstColumns As Generic.List(Of String)
								lstColumns = strLineIn.Split(New Char() {ControlChars.Tab}, 3).ToList()

								If lstColumns.Count >= 3 AndAlso Not String.IsNullOrEmpty(lstColumns(1)) Then
									If Not dctParameters.ContainsKey(lstColumns(1)) Then
										dctParameters.Add(lstColumns(1), lstColumns(2))
									End If
								End If

							End If
						End If
					End While
				End Using
			End If

		Catch ex As Exception
			m_message = "Error in ReadTemplatePX"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return dctParameters
		End Try

		Return dctParameters

	End Function

	Protected Function ReadWriteCvParam(ByVal objXmlReader As Xml.XmlTextReader, ByVal objXmlWriter As Xml.XmlTextWriter, ByRef lstElementCloseDepths As Generic.Stack(Of Integer)) As udtCvParamInfoType

		Dim udtCvParam As udtCvParamInfoType = New udtCvParamInfoType
		udtCvParam.Clear()

		objXmlWriter.WriteStartElement(objXmlReader.Name)

		If objXmlReader.HasAttributes() Then
			objXmlReader.MoveToFirstAttribute()
			Do
				objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)

				Select Case objXmlReader.Name
					Case "accession"
						udtCvParam.Accession = objXmlReader.Value

					Case "cvRef"
						udtCvParam.CvRef = objXmlReader.Value

					Case "name"
						udtCvParam.Name = objXmlReader.Value

					Case "value"
						udtCvParam.Value = objXmlReader.Value

					Case "unitCvRef"
						udtCvParam.unitCvRef = objXmlReader.Value

					Case "unitName"
						udtCvParam.unitName = objXmlReader.Value

					Case "unitAccession"
						udtCvParam.unitAccession = objXmlReader.Value

				End Select
			Loop While objXmlReader.MoveToNextAttribute()

			lstElementCloseDepths.Push(objXmlReader.Depth)

		ElseIf objXmlReader.IsEmptyElement Then
			objXmlWriter.WriteEndElement()
		End If

		Return udtCvParam

	End Function

	Protected Function RetrievePHRPFiles(ByVal intJob As Integer, ByVal strDataset As String, ByVal objAnalysisResults As clsAnalysisResults) As Boolean
		Dim strJobInfoFilePath As String
		Dim lstFilesToCopy As Generic.List(Of String) = New Generic.List(Of String)

		Try

			strJobInfoFilePath = clsAnalysisResources.GetJobInfoFilePath(intJob, m_WorkDir)

			If Not IO.File.Exists(strJobInfoFilePath) Then
				' Assume all of the files already exist
				Return True
			End If

			' Read the contents of the JobInfo file
			Using srInFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(strJobInfoFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
				Do While srInFile.Peek > -1
					lstFilesToCopy.Add(srInFile.ReadLine)
				Loop
			End Using

			' Retrieve the files
			' If the same dataset has multiple jobs then we might overwrite existing files; 
			'   that's OK since results files that we care about will have been auto-renamed based on the call to JobFileRenameRequired

			For Each strSourceFilePath As String In lstFilesToCopy

				Dim strSourceFileName As String = IO.Path.GetFileName(strSourceFilePath)

				Dim strTargetFilePath As String
				strTargetFilePath = IO.Path.Combine(m_WorkDir, strSourceFileName)

				objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strTargetFilePath, True)

				Dim fiLocalFile As IO.FileInfo = New IO.FileInfo(strTargetFilePath)
				If Not fiLocalFile.Exists Then
					m_message = "PHRP file was not copied locally: " & fiLocalFile.Name
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If

				If fiLocalFile.Extension.ToLower() = ".zip" Then
					' Need to unzip the file
					m_IonicZipTools.UnzipFile(fiLocalFile.FullName, m_WorkDir)

					For Each kvUnzippedFile In m_IonicZipTools.MostRecentUnzippedFiles
						AddToListIfNew(mPreviousDatasetFilesToDelete, kvUnzippedFile.Value)
					Next
				End If

				AddToListIfNew(mPreviousDatasetFilesToDelete, fiLocalFile.FullName)

			Next

		Catch ex As Exception
			m_message = "Error in RetrievePHRPFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Function RetrieveStoragePathInfoTargetFile(ByVal strStoragePathInfoFilePath As String, ByVal objAnalysisResults As clsAnalysisResults, ByRef strDestPath As String) As Boolean
		Return RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath, objAnalysisResults, IsFolder:=False, strDestPath:=strDestPath)
	End Function

	Protected Function RetrieveStoragePathInfoTargetFile(ByVal strStoragePathInfoFilePath As String, ByVal objAnalysisResults As clsAnalysisResults, ByVal IsFolder As Boolean, ByRef strDestPath As String) As Boolean
		Dim strSourceFilePath As String = String.Empty

		Try
			strDestPath = String.Empty

			If Not IO.File.Exists(strStoragePathInfoFilePath) Then
				m_message = "StoragePathInfo file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strStoragePathInfoFilePath)
				Return False
			End If

			Using srInfoFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(strStoragePathInfoFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
				If srInfoFile.Peek > -1 Then
					strSourceFilePath = srInfoFile.ReadLine()
				End If
			End Using

			If String.IsNullOrEmpty(strSourceFilePath) Then
				m_message = "StoragePathInfo file was empty"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strStoragePathInfoFilePath)
				Return False
			End If

			strDestPath = IO.Path.Combine(m_WorkDir, IO.Path.GetFileName(strSourceFilePath))

			If IsFolder Then
				objAnalysisResults.CopyDirectory(strSourceFilePath, strDestPath, Overwrite:=True)
			Else
				objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strDestPath, Overwrite:=True)
			End If

		Catch ex As Exception
			m_message = "Error in RetrieveStoragePathInfoTargetFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

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
		CmdStr &= " -debug"

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

		' Assure that the console output file has been parsed
		ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			If mConsoleOutputErrorMsg.Contains("/Report/PTMs/PTM") Then
				' Ignore this error
				mConsoleOutputErrorMsg = String.Empty
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			End If
		End If

		If Not blnSuccess Then
			m_message = "Error running PrideConverter, dataset " & strDataset & ", job " & intJob.ToString()
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
		End If

		Return blnSuccess

	End Function

	Protected Sub StoreInstrumentInfo(ByVal udtJobInfo As clsAnalysisResources.udtDataPackageJobInfoType)

		Dim lstInstruments As Generic.List(Of String) = Nothing
		If mInstrumentGroupsStored.TryGetValue(udtJobInfo.InstrumentGroup, lstInstruments) Then
			If Not lstInstruments.Contains(udtJobInfo.Instrument) Then
				lstInstruments.Add(udtJobInfo.Instrument)
			End If
		Else
			lstInstruments = New Generic.List(Of String) From {udtJobInfo.Instrument}
			mInstrumentGroupsStored.Add(udtJobInfo.InstrumentGroup, lstInstruments)
		End If

	End Sub

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

	Protected Sub TransferPreviousDatasetFiles(objAnalysisResults As clsAnalysisResults)

		' Delete the dataset files for the previous dataset
		Dim lstFilesToRetry As Generic.List(Of String) = New Generic.List(Of String)

		If mPreviousDatasetFilesToCopy.Count > 0 Then
			lstFilesToRetry.Clear()

			Dim strRemoteTransferFolder As String
			strRemoteTransferFolder = CreateRemoteTransferFolder(objAnalysisResults)

			If String.IsNullOrEmpty(strRemoteTransferFolder) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreateRemoteTransferFolder returned an empty string; unable to copy files to the transfer folder")
				lstFilesToRetry.AddRange(mPreviousDatasetFilesToCopy)
			Else

				Try
					' Create the remote Transfer Directory
					If Not IO.Directory.Exists(strRemoteTransferFolder) Then
						IO.Directory.CreateDirectory(strRemoteTransferFolder)
					End If

					' Copy the files we want to keep to the remote Transfer Directory
					For Each strSrcFilePath In mPreviousDatasetFilesToCopy
						Dim strTargetFilePath As String = IO.Path.Combine(strRemoteTransferFolder, IO.Path.GetFileName(strSrcFilePath))

						Try
							objAnalysisResults.CopyFileWithRetry(strSrcFilePath, strTargetFilePath, True)
							AddToListIfNew(mPreviousDatasetFilesToDelete, strSrcFilePath)
						Catch ex As Exception
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying file to transfer directory: " & ex.Message)
							lstFilesToRetry.Add(strSrcFilePath)
						End Try

					Next

				Catch ex As Exception
					' Folder creation error
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating transfer directory folder: " & ex.Message)
					lstFilesToRetry.AddRange(mPreviousDatasetFilesToCopy)
				End Try

			End If

			mPreviousDatasetFilesToCopy.Clear()

			If lstFilesToRetry.Count > 0 Then
				mPreviousDatasetFilesToCopy.AddRange(lstFilesToRetry)

				For Each item In lstFilesToRetry
					If mPreviousDatasetFilesToDelete.Contains(item, StringComparer.CurrentCultureIgnoreCase) Then
						mPreviousDatasetFilesToDelete.Remove(item)
					End If
				Next
			End If

		End If

		If mPreviousDatasetFilesToDelete.Count > 0 Then
			lstFilesToRetry.Clear()

			For Each item In mPreviousDatasetFilesToDelete
				Try
					If IO.File.Exists(item) Then
						IO.File.Delete(item)
					End If
				Catch ex As Exception
					lstFilesToRetry.Add(item)
				End Try
			Next

			mPreviousDatasetFilesToDelete.Clear()

			If lstFilesToRetry.Count > 0 Then
				mPreviousDatasetFilesToDelete.AddRange(lstFilesToRetry)
			End If

		End If

	End Sub

	Protected Function UpdateMZidXMLFileLocation(ByVal eFileLocation As eMzIDXMLFileLocation, ByVal strElementName As String) As eMzIDXMLFileLocation

		Select Case strElementName
			Case "SequenceCollection"
				eFileLocation = eMzIDXMLFileLocation.SequenceCollection
			Case "AnalysisCollection"
				eFileLocation = eMzIDXMLFileLocation.AnalysisCollection
			Case "AnalysisProtocolCollection"
				eFileLocation = eMzIDXMLFileLocation.AnalysisProtocolCollection
			Case "DataCollection"
				eFileLocation = eMzIDXMLFileLocation.DataCollection
			Case "Inputs"
				eFileLocation = eMzIDXMLFileLocation.Inputs
			Case "SearchDatabase"
				eFileLocation = eMzIDXMLFileLocation.InputSearchDatabase
			Case "SpectraData"
				eFileLocation = eMzIDXMLFileLocation.InputSpectraData
			Case "AnalysisData"
				eFileLocation = eMzIDXMLFileLocation.AnalysisData
		End Select

		Return eFileLocation

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <param name="strPrideConverterProgLoc"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strPrideConverterProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim fiPrideConverter As System.IO.FileInfo

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New Generic.List(Of System.IO.FileInfo)

		If mCreatePrideXMLFiles Then
			fiPrideConverter = New System.IO.FileInfo(strPrideConverterProgLoc)
			If Not fiPrideConverter.Exists Then
				Try
					strToolVersionInfo = "Unknown"
					Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
					Return False
				End Try

				Return False
			End If

			' Run the PRIDE Converter using the -version switch to determine its version
			strToolVersionInfo = GetPrideConverterVersion(fiPrideConverter.FullName)

			ioToolFiles.Add(fiPrideConverter)
		Else

			' Lookup the version of the AnalysisManagerPrideConverter plugin
			If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "AnalysisManagerPRIDEConverterPlugIn", blnIncludeRevision:=False) Then
				Return False
			End If

		End If

		ioToolFiles.Add(New System.IO.FileInfo(mMSXmlGeneratorAppPath))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex)
			Return False
		End Try

	End Function

	''' <summary>
	''' Update the .mzid file for the given job and dataaset to have the correct Accession value for FileFormat
	''' Will also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
	''' </summary>
	''' <param name="intJob">Job number</param>
	''' <param name="strDataset">Dataset name</param>
	''' <param name="blnCreatedMGFFiles">Set to true if we converted _dta.txt files to .mgf files</param>
	''' <param name="strMzIDFilePath">Output parameter: path to the .mzID file for this job</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Private Function UpdateMzIdFile(ByVal intJob As Integer, ByVal strDataset As String, ByVal blnCreatedMGFFiles As Boolean, ByRef strMzIDFilePath As String) As Boolean

		Dim blnNodeWritten As Boolean
		Dim blnSkipNode As Boolean
		Dim blnReadModAccession As Boolean = False

		Dim lstAttributeOverride As Generic.Dictionary(Of String, String) = New Generic.Dictionary(Of String, String)

		Dim lstElementCloseDepths As Generic.Stack(Of Integer)

		Dim eFileLocation As eMzIDXMLFileLocation = eMzIDXMLFileLocation.Header
		Dim lstRecentElements As Collections.Generic.Queue(Of String) = New Collections.Generic.Queue(Of String)

		Try
			' Open the .mzid and parse it to create a new .mzid file
			' Use a forward-only XML reader, copying most of the elements verbatim, but customizing some of them

			' For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>
			' For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
			' Will also need to update the location and name attributes of the SpectraData element
			' <SpectraData location="E:\DMS_WorkDir3\QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" name="QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" id="SID_1">

			lstElementCloseDepths = New Generic.Stack(Of Integer)
			strMzIDFilePath = String.Empty

			Dim strSourceFileName As String
			Dim strUpdatedFilePathTemp As String

			' First look for a job-specific version of the .mzid file
			strSourceFileName = "Job" & intJob.ToString() & "_" & strDataset & "_msgfplus.mzid"
			strMzIDFilePath = IO.Path.Combine(m_WorkDir, strSourceFileName)

			If Not IO.File.Exists(strMzIDFilePath) Then
				' Job-specific version not found
				' Look for one that simply starts with the dataset name
				strSourceFileName = strDataset & "_msgfplus.mzid"
				strMzIDFilePath = IO.Path.Combine(m_WorkDir, strSourceFileName)

				If Not IO.File.Exists(strMzIDFilePath) Then
					m_message = "MzID file not found for job " & intJob & ": " & strSourceFileName
					Return False
				End If
			End If

			AddToListIfNew(mPreviousDatasetFilesToDelete, strMzIDFilePath)

			strUpdatedFilePathTemp = strMzIDFilePath & ".tmp"
			Using objXmlWriter As Xml.XmlTextWriter = New Xml.XmlTextWriter(New IO.FileStream(strUpdatedFilePathTemp, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read), Text.Encoding.UTF8)
				objXmlWriter.Formatting = Xml.Formatting.Indented
				objXmlWriter.Indentation = 4

				objXmlWriter.WriteStartDocument()

				' Note that the following Using command will not work if the .mzid file has an encoding string of <?xml version="1.0" encoding="Cp1252"?>
				' Using objXmlReader As Xml.XmlTextReader = New Xml.XmlTextReader(New IO.FileStream(strMzIDFilePath, IO.FileMode.Open, IO.FileAccess.Read))
				' Thus, we instead first insantiate a streamreader using explicit encodings
				' Then instantiate the XmlTextReader

				Using srSourceFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(strMzIDFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read), Text.Encoding.GetEncoding("ISO-8859-1"))

					Using objXmlReader As Xml.XmlTextReader = New Xml.XmlTextReader(srSourceFile)

						Do While objXmlReader.Read()

							Select Case objXmlReader.NodeType
								Case Xml.XmlNodeType.Whitespace
									' Skip whitespace since the writer should be auto-formatting things
									' objXmlWriter.WriteWhitespace(objXmlReader.Value)

								Case Xml.XmlNodeType.Comment
									objXmlWriter.WriteComment(objXmlReader.Value)

								Case Xml.XmlNodeType.Element
									' Start element

									If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
									lstRecentElements.Enqueue("Element " & objXmlReader.Name)

									Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
										Dim intPoppedVal As Integer
										intPoppedVal = lstElementCloseDepths.Pop()

										objXmlWriter.WriteEndElement()
									Loop

									eFileLocation = UpdateMZidXMLFileLocation(eFileLocation, objXmlReader.Name)

									blnNodeWritten = False
									blnSkipNode = False

									lstAttributeOverride.Clear()

									Select Case objXmlReader.Name

										Case "SpectraData"
											' Override the location and name attributes for this node

											Dim strSpectraDataFilename As String

											If blnCreatedMGFFiles Then
												strSpectraDataFilename = strDataset & ".mgf"
											Else
												strSpectraDataFilename = strDataset & "_dta.txt"
											End If

											lstAttributeOverride.Add("location", "C:\DMS_WorkDir\" & strSpectraDataFilename)
											lstAttributeOverride.Add("name", strSpectraDataFilename)

										Case "FileFormat"
											If eFileLocation = eMzIDXMLFileLocation.InputSpectraData Then
												' Override the accession and name attributes for this node

												' For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
												' For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>

												Dim strAccession As String
												Dim strFormatName As String

												If blnCreatedMGFFiles Then
													strAccession = "MS:1001062"
													strFormatName = "Mascot MGF file"
												Else
													strAccession = "MS:1001369"
													strFormatName = "text file"
												End If

												objXmlWriter.WriteStartElement("FileFormat")
												objXmlWriter.WriteStartElement("cvParam")

												objXmlWriter.WriteAttributeString("accession", strAccession)
												objXmlWriter.WriteAttributeString("cvRef", "PSI-MS")
												objXmlWriter.WriteAttributeString("name", strFormatName)

												objXmlWriter.WriteEndElement()	' cvParam
												objXmlWriter.WriteEndElement()	' FileFormat

												blnSkipNode = True
											End If

										Case "SearchModification"
											If eFileLocation = eMzIDXMLFileLocation.AnalysisProtocolCollection Then
												' The next cvParam entry that we read should have the Unimod accession
												blnReadModAccession = True
											End If

										Case "cvParam"
											If blnReadModAccession Then
												Dim udtModInfo As udtCvParamInfoType
												udtModInfo = ReadWriteCvParam(objXmlReader, objXmlWriter, lstElementCloseDepths)

												If Not String.IsNullOrEmpty(udtModInfo.Accession) Then
													If Not mModificationsUsed.ContainsKey(udtModInfo.Accession) Then
														mModificationsUsed.Add(udtModInfo.Accession, udtModInfo)
													End If
												End If

												blnNodeWritten = True
												blnReadModAccession = False
											End If
									End Select


									If blnSkipNode Then
										If objXmlReader.NodeType <> Xml.XmlNodeType.EndElement Then
											' Skip this element (and any children nodes enclosed in this elemnt)
											' Likely should not do this when objXmlReader.NodeType is Xml.XmlNodeType.EndElement
											objXmlReader.Skip()
										End If

									ElseIf Not blnNodeWritten Then
										' Copy this element from the source file to the target file

										objXmlWriter.WriteStartElement(objXmlReader.Name)

										If objXmlReader.HasAttributes() Then
											objXmlReader.MoveToFirstAttribute()
											Do
												Dim strAttributeOverride As String = String.Empty
												If lstAttributeOverride.Count > 0 AndAlso lstAttributeOverride.TryGetValue(objXmlReader.Name, strAttributeOverride) Then
													objXmlWriter.WriteAttributeString(objXmlReader.Name, strAttributeOverride)
												Else
													objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)
												End If

											Loop While objXmlReader.MoveToNextAttribute()

											lstElementCloseDepths.Push(objXmlReader.Depth)

										ElseIf objXmlReader.IsEmptyElement Then
											objXmlWriter.WriteEndElement()
										End If

									End If

								Case Xml.XmlNodeType.EndElement

									If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
									lstRecentElements.Enqueue("EndElement " & objXmlReader.Name)

									Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth + 1
										Dim intPoppedVal As Integer
										intPoppedVal = lstElementCloseDepths.Pop()
										objXmlWriter.WriteEndElement()
									Loop

									objXmlWriter.WriteEndElement()

									Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
										Dim intPoppedVal As Integer
										intPoppedVal = lstElementCloseDepths.Pop()
									Loop

									If objXmlReader.Name = "SearchModification" Then
										blnReadModAccession = False
									End If

								Case Xml.XmlNodeType.Text

									If Not String.IsNullOrEmpty(objXmlReader.Value) Then
										If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
										If objXmlReader.Value.Length > 10 Then
											lstRecentElements.Enqueue(objXmlReader.Value.Substring(0, 10))
										Else
											lstRecentElements.Enqueue(objXmlReader.Value)
										End If
									End If

									objXmlWriter.WriteString(objXmlReader.Value)

							End Select

						Loop

					End Using

				End Using

				objXmlWriter.WriteEndDocument()
			End Using

			System.Threading.Thread.Sleep(250)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Try
				' Replace the original .mzID file with the updated one
				IO.File.Delete(strMzIDFilePath)

				If JobFileRenameRequired(intJob) Then
					strMzIDFilePath = IO.Path.Combine(m_WorkDir, strDataset & "_Job" & intJob.ToString() & "_msgfplus.mzid")
				Else
					strMzIDFilePath = IO.Path.Combine(m_WorkDir, strDataset & "_msgfplus.mzid")
				End If

				IO.File.Move(strUpdatedFilePathTemp, strMzIDFilePath)

			Catch ex As Exception
				m_message = "Exception replacing the original .mzID file with the updated one for job " & intJob & ", dataset " & strDataset
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
				Return False
			End Try

		Catch ex As Exception
			m_message = "Exception in UpdateMzIdFile for job " & intJob & ", dataset " & strDataset
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Dim strRecentElements As String = String.Empty
			For Each strItem In lstRecentElements
				If String.IsNullOrEmpty(strRecentElements) Then
					strRecentElements = String.Copy(strItem)
				Else
					strRecentElements &= "; " & strItem
				End If
			Next

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strRecentElements)

			Return False
		End Try

		Return True

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

	Protected Sub WritePXHeader(ByVal swPXFile As IO.StreamWriter, ByVal strType As String, ByVal strValue As String)
		WritePXHeader(swPXFile, strType, strValue, New Generic.Dictionary(Of String, String))
	End Sub

	Protected Sub WritePXHeader(ByVal swPXFile As IO.StreamWriter, ByVal strType As String, ByVal strValue As String, ByVal dctParameters As Generic.Dictionary(Of String, String))
		Dim strValueOverride As String = String.Empty

		If dctParameters.TryGetValue(strType, strValueOverride) Then
			strValue = strValueOverride
		End If

		WritePXLine(swPXFile, New Generic.List(Of String) From {"MTD", strType, strValue})

	End Sub

	Protected Sub WritePXLine(ByVal swPXFile As IO.StreamWriter, ByVal lstItems As Generic.List(Of String))

		swPXFile.WriteLine(clsGlobal.FlattenList(lstItems, ControlChars.Tab))
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

	Protected Function WriteXMLInstrumentInfo(ByVal oWriter As Xml.XmlTextWriter, ByVal strInstrumentGroup As String) As Boolean

		Dim blnInstrumentDetailsAutoDefined As Boolean

		Dim blnIsLCQ As Boolean
		Dim blnIsLTQ As Boolean

		Select Case strInstrumentGroup
			Case "Orbitrap", "VelosOrbi", "QExactive"
				blnInstrumentDetailsAutoDefined = True

				WriteXMLInstrumentInfoESI(oWriter, "positive")

				oWriter.WriteStartElement("analyzerList")
				oWriter.WriteAttributeString("count", "2")

				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap")
				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap")

				oWriter.WriteEndElement()	' analyzerList

				WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector")

			Case "LCQ"
				blnIsLCQ = True

			Case "LTQ", "LTQ-ETD", "LTQ-Prep", "VelosPro"
				blnIsLTQ = True

			Case "LTQ-FT"
				blnInstrumentDetailsAutoDefined = True

				WriteXMLInstrumentInfoESI(oWriter, "positive")

				oWriter.WriteStartElement("analyzerList")
				oWriter.WriteAttributeString("count", "2")

				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap")
				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000079", "fourier transform ion cyclotron resonance mass spectrometer")

				oWriter.WriteEndElement()	' analyzerList

				WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector")

			Case "Exactive"
				blnInstrumentDetailsAutoDefined = True

				WriteXMLInstrumentInfoESI(oWriter, "positive")

				oWriter.WriteStartElement("analyzerList")
				oWriter.WriteAttributeString("count", "1")

				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap")

				oWriter.WriteEndElement()	' analyzerList

				WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector")

			Case Else
				If strInstrumentGroup.StartsWith("LTQ") Then
					blnIsLTQ = True
				ElseIf strInstrumentGroup.StartsWith("LCQ") Then
					blnIsLCQ = True
				End If
		End Select

		If blnIsLTQ Or blnIsLCQ Then
			blnInstrumentDetailsAutoDefined = True

			WriteXMLInstrumentInfoESI(oWriter, "positive")

			oWriter.WriteStartElement("analyzerList")
			oWriter.WriteAttributeString("count", "1")

			If blnIsLCQ Then
				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000082", "quadrupole ion trap")
			Else
				WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap")
			End If

			oWriter.WriteEndElement()	' analyzerList

			WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000347", "dynode")
		End If

		Return blnInstrumentDetailsAutoDefined

	End Function

	Protected Sub WriteXMLInstrumentInfoAnalyzer(ByVal oWriter As Xml.XmlTextWriter, ByVal strNamespace As String, ByVal strAccession As String, ByVal strDescription As String)

		oWriter.WriteStartElement("analyzer")
		WriteCVParam(oWriter, strNamespace, strAccession, strDescription, String.Empty)
		oWriter.WriteEndElement()

	End Sub

	Protected Sub WriteXMLInstrumentInfoDetector(ByVal oWriter As Xml.XmlTextWriter, ByVal strNamespace As String, ByVal strAccession As String, ByVal strDescription As String)

		oWriter.WriteStartElement("detector")
		WriteCVParam(oWriter, strNamespace, strAccession, strDescription, String.Empty)
		oWriter.WriteEndElement()

	End Sub


	Protected Sub WriteXMLInstrumentInfoESI(ByVal oWriter As Xml.XmlTextWriter, ByVal strPolarity As String)

		If String.IsNullOrEmpty(strPolarity) Then strPolarity = "positive"

		oWriter.WriteStartElement("source")
		WriteCVParam(oWriter, "MS", "MS:1000073", "electrospray ionization", String.Empty)
		WriteCVParam(oWriter, "MS", "MS:1000037", "polarity", strPolarity)
		oWriter.WriteEndElement()

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
		End If

	End Sub

	Private Sub mDTAtoMGF_ErrorEvent(strMessage As String) Handles mDTAtoMGF.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error from DTAtoMGF converter: " & mDTAtoMGF.GetErrorMessage())
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
