Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices
Imports PHRPReader
Imports System.Xml
Imports MyEMSLReader

Public Class clsAnalysisToolRunnerPRIDEConverter
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running PRIDEConverter
    '*********************************************************************************************************

#Region "Module Variables"
    Private Const PRIDEConverter_CONSOLE_OUTPUT As String = "PRIDEConverter_ConsoleOutput.txt"
    Public Const PROGRESS_PCT_TOOL_RUNNER_STARTING As Single = 20
    Private Const PROGRESS_PCT_SAVING_RESULTS As Single = 95
    Private Const PROGRESS_PCT_COMPLETE As Single = 99

    Private Const FILE_EXTENSION_PSEUDO_MSGF As String = ".msgf"
    Private Const FILE_EXTENSION_MSGF_REPORT_XML As String = ".msgf-report.xml"
    Private Const FILE_EXTENSION_MSGF_PRIDE_XML As String = ".msgf-pride.xml"

    Private Const PARTIAL_SUBMISSION As String = "PARTIAL"
    Private Const COMPLETE_SUBMISSION As String = "COMPLETE"

    Private Const PNNL_NAME_COUNTRY As String = "Pacific Northwest National Laboratory, USA"

    Private Const DEFAULT_TISSUE_CV As String = "[BTO, BTO:0000089, blood, ]"
    Private Const DEFAULT_CELL_TYPE_CV As String = "[CL, CL:0000081, blood cell, ]"
    Private Const DEFAULT_DISEASE_TYPE_CV As String = "[DOID, DOID:1612, breast cancer, ]"
    Private Const DEFAULT_QUANTIFICATION_TYPE_CV As String = "[PRIDE, PRIDE:0000436, Spectral counting,]"
    Private Const DELETION_WARNING As String = " -- If you delete this line, assure that the corresponding column values on the SME rows are empty (leave the 'cell_type' and 'disease' column headers on the SMH line, but assure that the SME lines have blank entries for this column)"

    Private Const DEFAULT_PVALUE_THRESHOLD As Double = 0.05

    Private mConsoleOutputErrorMsg As String

    ' This dictionary tracks the peptide hit jobs defined for this data package
    ' The keys are job numbers and the values contains job info
    Private mDataPackagePeptideHitJobs As Dictionary(Of Integer, clsDataPackageJobInfo)

    Private mPrideConverterProgLoc As String = String.Empty

    Private ReadOnly mJavaProgLoc As String = String.Empty
    Private mMSXmlGeneratorAppPath As String = String.Empty

    Private mCreateMSGFReportFilesOnly As Boolean
    Private mCreateMGFFiles As Boolean
    Private mCreatePrideXMLFiles As Boolean

    Private mIncludePepXMLFiles As Boolean
    Private mProcessMzIdFiles As Boolean

    Private mCacheFolderPath As String = String.Empty
    Private mPreviousDatasetName As String = String.Empty

    ' This list contains full file paths for files that will be deleted from the local work directory
    Private mPreviousDatasetFilesToDelete As List(Of String)

    ' This list contains full file paths for files that will be copied from the local work directory to the transfer directory
    Private mPreviousDatasetFilesToCopy As List(Of String)

    Private mCachedOrgDBName As String = String.Empty

    ' This dictionary holds protein name in the key 
    ' The value is a key-value pair where the key is the Protein Index and the value is the protein sequence
    Private mCachedProteins As Dictionary(Of String, KeyValuePair(Of Integer, String))

    ' This dictionary holds the protein index as the key and tracks the number of filter-passing PSMs for each protein as the value
    Private mCachedProteinPSMCounts As Dictionary(Of Integer, Integer)

    ' Keys in this dictionary are filenames
    ' Values contain info on each file
    ' Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
    ' However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
    Private mPxMasterFileList As Dictionary(Of String, clsPXFileInfoBase)

    ' Keys in this dictionary are PXFileIDs
    ' Values contain info on each file, including the PXFileType and the FileIDs that map to this file (empty list if no mapped files)
    ' Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
    ' However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
    Private mPxResultFiles As Dictionary(Of Integer, clsPXFileInfo)

    Private mFilterThresholdsUsed As udtFilterThresholdsType

    ' Keys in this dictionary are instrument group names
    ' Values are the specific instrument names
    Private mInstrumentGroupsStored As Dictionary(Of String, List(Of String))
    Private mSearchToolsUsed As SortedSet(Of String)

    ' Keys in this dictionary are NEWT IDs
    ' Values are the NEWT name for the given ID
    Private mExperimentNEWTInfo As Dictionary(Of Integer, String)

    ' Keys in this dictionary are Unimod accession names (e.g. UNIMOD:35)
    ' Values are CvParam data for the modification
    Private mModificationsUsed As Dictionary(Of String, clsSampleMetadata.udtCvParamInfoType)

    ' Keys in this dictionary are mzid.gz file names
    ' Values are the sample info for the file
    Private mMzIdSampleInfo As Dictionary(Of String, clsSampleMetadata)

    ' Keys in this dictionary are _dta.txt file names
    ' Values contain info on each file
    Private mCDTAFileStats As Dictionary(Of String, clsPXFileInfoBase)

    Private WithEvents mMSXmlCreator As AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator
    Private WithEvents mDTAtoMGF As DTAtoMGF.clsDTAtoMGF

    Private mCmdRunner As clsRunDosProgram
#End Region

#Region "Structures and Enums"
    Private Structure udtFilterThresholdsType
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

    Private Structure udtPseudoMSGFDataType
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

    Private Enum eMSGFReportXMLFileLocation
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

    Private Enum eMzIDXMLFileLocation
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
            ' Call base class for initial setup
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
                LogError("Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining PRIDE Converter version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            mConsoleOutputErrorMsg = String.Empty

            mCacheFolderPath = m_jobParams.GetJobParameter("CacheFolderPath", "\\protoapps\PeptideAtlas_Staging")

            Dim assumeInstrumentDataUnpurged = m_jobParams.GetJobParameter("AssumeInstrumentDataUnpurged", True)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PRIDEConverter")

            ' Initialize dctDataPackageDatasets
            Dim dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo) = Nothing
            If Not LoadDataPackageDatasetInfo(dctDataPackageDatasets) Then
                Dim msg = "Error loading data package dataset info"
                LogError(msg & ": clsAnalysisToolRunnerBase.LoadDataPackageDatasetInfo returned false")
                m_message = msg
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Initialize mDataPackagePeptideHitJobs			
            If Not LookupDataPackagePeptideHitJobs() Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Monitor the FileDownloaded event in this class
            AddHandler m_MyEMSLUtilities.FileDownloaded, AddressOf m_MyEMSLDatasetListInfo_FileDownloadedEvent

            ' The objAnalysisResults object is used to copy files to/from this computer
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

            ' Extract the dataset raw file paths
            Dim dctDatasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS)

            ' Process each job in mDataPackagePeptideHitJobs
            ' Sort the jobs by dataset so that we can use the same .mzXML file for datasets with multiple jobs
            Dim linqJobsSortedByDataset = (From item In mDataPackagePeptideHitJobs Select item Order By item.Value.Dataset, SortPreference(item.Value.Tool))

            ' Read the PX_Submission_Template.px file
            Dim dctTemplateParameters = ReadTemplatePXSubmissionFile()

            Const blnContinueOnError = True
            Const maxErrorCount = 10
            Dim intJobsProcessed = 0
            Dim intJobFailureCount = 0
            Dim dtLastLogTime = DateTime.UtcNow

            ' This dictionary tracks the datasets that have been processed
            ' Keys are dataset ID, values are dataset name
            Dim dctDatasetsProcessed = New Dictionary(Of Integer, String)

            For Each kvJobInfo As KeyValuePair(Of Integer, clsDataPackageJobInfo) In linqJobsSortedByDataset

                Dim udtCurrentJobInfo = kvJobInfo.Value

                m_StatusTools.CurrentOperation = "Processing job " & udtCurrentJobInfo.Job & ", dataset " & udtCurrentJobInfo.Dataset

                Console.WriteLine()
                Console.WriteLine((intJobsProcessed + 1).ToString() & ": " & m_StatusTools.CurrentOperation)

                result = ProcessJob(kvJobInfo, udtFilterThresholds, objAnalysisResults, dctDatasetRawFilePaths, dctTemplateParameters, assumeInstrumentDataUnpurged)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    intJobFailureCount += 1
                    If Not blnContinueOnError OrElse intJobFailureCount > maxErrorCount Then Exit For
                End If

                If Not dctDatasetsProcessed.ContainsKey(udtCurrentJobInfo.DatasetID) Then
                    dctDatasetsProcessed.Add(udtCurrentJobInfo.DatasetID, udtCurrentJobInfo.Dataset)
                End If

                intJobsProcessed += 1
                m_progress = ComputeIncrementalProgress(PROGRESS_PCT_TOOL_RUNNER_STARTING, PROGRESS_PCT_SAVING_RESULTS, intJobsProcessed, mDataPackagePeptideHitJobs.Count)
                m_StatusTools.UpdateAndWrite(m_progress)

                If DateTime.UtcNow.Subtract(dtLastLogTime).TotalMinutes >= 5 OrElse m_DebugLevel >= 2 Then
                    dtLastLogTime = DateTime.UtcNow
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... processed " & intJobsProcessed & " / " & mDataPackagePeptideHitJobs.Count & " jobs")
                End If
            Next

            TransferPreviousDatasetFiles(objAnalysisResults)

            ' Look for datasets associated with the data package that have no PeptideHit jobs
            ' Create fake PeptideHit jobs in the .px file to alert the user of the missing jobs

            For Each kvDatasetInfo In dctDataPackageDatasets
                If Not dctDatasetsProcessed.ContainsKey(kvDatasetInfo.Key) Then
                    m_StatusTools.CurrentOperation = "Adding dataset " & kvDatasetInfo.Value.Dataset & " (no associated PeptideHit job)"

                    Console.WriteLine()
                    Console.WriteLine(m_StatusTools.CurrentOperation)

                    AddPlaceholderDatasetEntry(kvDatasetInfo)
                End If
            Next

            ' If we were still unable to delete some files, we want to make sure that they don't end up in the results folder
            For Each fileToDelete In mPreviousDatasetFilesToDelete
                m_jobParams.AddResultFileToSkip(fileToDelete)
            Next

            ' Create the PX Submission file
            blnSuccess = CreatePXSubmissionFile(dctTemplateParameters)

            m_progress = PROGRESS_PCT_COMPLETE
            m_StatusTools.UpdateAndWrite(m_progress)

            If blnSuccess Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PRIDEConverter Complete")
                End If
            End If

            ' Stop the job timer
            m_StopTime = DateTime.UtcNow

            ' Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                LogWarning("Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            ' Make sure objects are released
            Threading.Thread.Sleep(500)         ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not blnSuccess Or intJobFailureCount > 0 Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            DefineFilesToSkipTransfer()

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer(mCacheFolderPath)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            LogError("Exception in PRIDEConverterPlugin->RunTool", ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' No failures so everything must have succeeded
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function SortPreference(tool As String) As Integer

        If tool.ToLower().StartsWith("msgfplus") Then Return 0

        If tool.ToLower().StartsWith("xtandem") Then Return 1

        Return 5

    End Function

    Private Sub AddPlaceholderDatasetEntry(kvDatasetInfo As KeyValuePair(Of Integer, clsDataPackageDatasetInfo))

        AddNEWTInfo(kvDatasetInfo.Value.Experiment_NEWT_ID, kvDatasetInfo.Value.Experiment_NEWT_Name)

        ' Store the instrument group and instrument name
        StoreInstrumentInfo(kvDatasetInfo.Value)

        Dim udtDatasetInfo = kvDatasetInfo.Value
        Dim strDatasetRawFilePath = Path.Combine(udtDatasetInfo.ServerStoragePath, udtDatasetInfo.Dataset & ".raw")

        Dim dataPkgJob = clsAnalysisResources.GetPseudoDataPackageJobInfo(udtDatasetInfo)

        Dim rawFileID = AddPxFileToMasterList(strDatasetRawFilePath, dataPkgJob)

        AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, strDatasetRawFilePath, dataPkgJob)

    End Sub

    Private Sub AddNEWTInfo(newtID As Integer, newtName As String)

        If newtID = 0 Then
            newtID = 2323
            newtName = "unclassified Bacteria"
        End If

        If Not mExperimentNEWTInfo.ContainsKey(newtID) Then
            mExperimentNEWTInfo.Add(newtID, newtName)
        End If

    End Sub

    Private Function AddPxFileToMasterList(strFilePath As String, dataPkgJob As clsDataPackageJobInfo) As Integer

        Dim fiFile = New FileInfo(strFilePath)

        Dim oPXFileInfo As clsPXFileInfoBase = Nothing
        If mPxMasterFileList.TryGetValue(fiFile.Name, oPXFileInfo) Then
            ' File already exists
            Return oPXFileInfo.FileID
        Else
            Dim strFilename As String = CheckFilenameCase(fiFile, dataPkgJob.Dataset)

            oPXFileInfo = New clsPXFileInfoBase(strFilename, dataPkgJob)

            oPXFileInfo.FileID = mPxMasterFileList.Count + 1

            If fiFile.Exists Then
                oPXFileInfo.Length = fiFile.Length
                oPXFileInfo.MD5Hash = String.Empty      ' Don't compute the hash; it's not needed
            Else
                oPXFileInfo.Length = 0
                oPXFileInfo.MD5Hash = String.Empty
            End If

            mPxMasterFileList.Add(fiFile.Name, oPXFileInfo)

            Return oPXFileInfo.FileID
        End If

    End Function

    Private Function AddPxResultFile(
       intFileID As Integer,
       eFileType As clsPXFileInfoBase.ePXFileType,
       strFilePath As String,
       dataPkgJob As clsDataPackageJobInfo) As Boolean

        Dim fiFile = New FileInfo(strFilePath)

        Dim oPXFileInfo As clsPXFileInfo = Nothing

        If mPxResultFiles.TryGetValue(intFileID, oPXFileInfo) Then
            ' File already defined in the mapping list
            Return True
        Else

            Dim oMasterPXFileInfo As clsPXFileInfoBase = Nothing
            If Not mPxMasterFileList.TryGetValue(fiFile.Name, oMasterPXFileInfo) Then
                ' File not found in mPxMasterFileList, we cannot add the mapping
                LogError("File " & fiFile.Name & " not found in mPxMasterFileList; unable to add to mPxResultFiles")
                Return False
            End If

            If oMasterPXFileInfo.FileID <> intFileID Then
                Dim msg = "FileID mismatch for " & fiFile.Name
                LogError(msg & ":  mPxMasterFileList.FileID = " & oMasterPXFileInfo.FileID & " vs. FileID " & intFileID & " passed into AddPxFileToMapping")
                m_message = msg
                Return False
            End If

            Dim strFilename As String = CheckFilenameCase(fiFile, dataPkgJob.Dataset)

            oPXFileInfo = New clsPXFileInfo(strFilename, dataPkgJob)
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
    Private Sub AddToListIfNew(lstList As ICollection(Of String), strValue As String)
        If Not lstList.Contains(strValue) Then
            lstList.Add(strValue)
        End If
    End Sub

    Private Function AppendToPXFileInfo(
       dataPkgJob As clsDataPackageJobInfo,
       dctDatasetRawFilePaths As IReadOnlyDictionary(Of String, String),
       resultFiles As clsResultFileContainer) As Boolean

        ' Add the files to be submitted to ProteomeXchange to the master file list
        ' In addition, append new mappings to the ProteomeXchange mapping list

        Dim intPrideXMLFileID = 0
        If Not String.IsNullOrEmpty(resultFiles.PrideXmlFilePath) Then
            AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.PrideXmlFilePath)

            intPrideXMLFileID = AddPxFileToMasterList(resultFiles.PrideXmlFilePath, dataPkgJob)
            If Not AddPxResultFile(intPrideXMLFileID, clsPXFileInfoBase.ePXFileType.Result, resultFiles.PrideXmlFilePath, dataPkgJob) Then
                Return False
            End If
        End If

        Dim rawFileID As Integer
        Dim strDatasetRawFilePath As String = String.Empty
        If dctDatasetRawFilePaths.TryGetValue(dataPkgJob.Dataset, strDatasetRawFilePath) Then
            If Not String.IsNullOrEmpty(strDatasetRawFilePath) Then
                rawFileID = AddPxFileToMasterList(strDatasetRawFilePath, dataPkgJob)
                If Not AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, strDatasetRawFilePath, dataPkgJob) Then
                    Return False
                End If

                If intPrideXMLFileID > 0 Then
                    If Not DefinePxFileMapping(intPrideXMLFileID, rawFileID) Then
                        Return False
                    End If
                End If
            End If
        End If

        Dim intPeakfileID = 0
        If Not String.IsNullOrEmpty(resultFiles.MGFFilePath) Then
            AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.MGFFilePath)

            intPeakfileID = AddPxFileToMasterList(resultFiles.MGFFilePath, dataPkgJob)
            If Not AddPxResultFile(intPeakfileID, clsPXFileInfoBase.ePXFileType.Peak, resultFiles.MGFFilePath, dataPkgJob) Then
                Return False
            End If

            If intPrideXMLFileID = 0 Then
                ' Pride XML file was not created
                If rawFileID > 0 AndAlso resultFiles.MzIDFilePaths.Count = 0 Then
                    ' Only associate Peak files with .Raw files if we do not have a .MzId.gz file
                    If Not DefinePxFileMapping(intPeakfileID, rawFileID) Then
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

        For Each mzIdResultFile In resultFiles.MzIDFilePaths
            Dim success = AddMzidOrPepXmlFileToPX(dataPkgJob, mzIdResultFile, clsPXFileInfoBase.ePXFileType.ResultMzId, intPrideXMLFileID, rawFileID, intPeakfileID)
            If Not success Then Return False
        Next

        If Not String.IsNullOrWhiteSpace(resultFiles.PepXMLFile) Then
            Dim success = AddMzidOrPepXmlFileToPX(dataPkgJob, resultFiles.PepXMLFile, clsPXFileInfoBase.ePXFileType.Search, intPrideXMLFileID, rawFileID, intPeakfileID)
            If Not success Then Return False
        End If

        Return True

    End Function

    Private Function AddMzidOrPepXmlFileToPX(
      dataPkgJob As clsDataPackageJobInfo,
      resultFilePath As String,
      ePxFileType As clsPXFileInfoBase.ePXFileType,
      intPrideXMLFileID As Integer,
      rawFileID As Integer,
      intPeakfileID As Integer) As Boolean

        AddToListIfNew(mPreviousDatasetFilesToCopy, resultFilePath)

        Dim dataFileID = AddPxFileToMasterList(resultFilePath, dataPkgJob)
        If Not AddPxResultFile(dataFileID, ePxFileType, resultFilePath, dataPkgJob) Then
            Return False
        End If

        If intPrideXMLFileID = 0 Then
            ' Pride XML file was not created
            If intPeakfileID > 0 Then
                If Not DefinePxFileMapping(dataFileID, intPeakfileID) Then
                    Return False
                End If
            End If

            If rawFileID > 0 Then
                If Not DefinePxFileMapping(dataFileID, rawFileID) Then
                    Return False
                End If
            End If

        Else
            ' Pride XML file was created
            If Not DefinePxFileMapping(intPrideXMLFileID, dataFileID) Then
                Return False
            End If
        End If

        Return True

    End Function

    Private Function CheckFilenameCase(fiFile As FileInfo, strDataset As String) As String

        Dim strFilename As String = fiFile.Name


        If Not String.IsNullOrEmpty(fiFile.Extension) Then
            Dim strFileBaseName As String = Path.GetFileNameWithoutExtension(fiFile.Name)

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

    Private Function ComputeApproximatePValue(dblMSGFSpecProb As Double) As Double
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
    ''' <param name="dataPkgJob"></param>
    ''' <param name="strMGFFilePath">Output parameter: path of the newly created .mgf file</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ConvertCDTAToMGF(
       dataPkgJob As clsDataPackageJobInfo,
       <Out()> ByRef strMGFFilePath As String) As Boolean

        strMGFFilePath = String.Empty

        Try

            mDTAtoMGF = New DTAtoMGF.clsDTAtoMGF()
            mDTAtoMGF.Combine2And3PlusCharges = False
            mDTAtoMGF.FilterSpectra = False
            mDTAtoMGF.MaximumIonsPer100MzInterval = 40
            mDTAtoMGF.NoMerge = True

            ' Convert the _dta.txt file for this dataset
            Dim fiCDTAFile = New FileInfo(Path.Combine(m_WorkDir, dataPkgJob.Dataset & "_dta.txt"))

            If Not fiCDTAFile.Exists Then
                Dim msg = "_dta.txt file not found for job " & dataPkgJob.Job
                LogError(msg & ": " & fiCDTAFile.FullName)
                m_message = msg
                Return False
            End If

            ' Compute the MD5 hash for this _dta.txt file
            Dim strMD5Hash As String
            strMD5Hash = clsGlobal.ComputeFileHashMD5(fiCDTAFile.FullName)

            ' Make sure this is either a new _dta.txt file or identical to a previous one
            ' Abort processing if the job list contains multiple jobs for the same dataset but those jobs used different _dta.txt files
            ' However, if one of the jobs is Sequest and one is MSGF+, preferentially use the _dta.txt file from the MSGF+ job
            Dim oFileInfo As clsPXFileInfoBase = Nothing
            If mCDTAFileStats.TryGetValue(fiCDTAFile.Name, oFileInfo) Then

                If oFileInfo.JobInfo.Tool.ToLower().StartsWith("msgf") Then
                    ' Existing job found, but it's a MSGF+ job (which is fully supported by PRIDE)
                    ' Just use the existing .mgf file
                    Return True
                End If

                If fiCDTAFile.Length <> oFileInfo.Length Then
                    Dim msg = "Dataset " & dataPkgJob.Dataset & " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported"
                    LogError(msg & ": file size mismatch of " & fiCDTAFile.Length & " for job " & dataPkgJob.Job &
                             " vs " & oFileInfo.Length & " for job " & oFileInfo.JobInfo.Job)
                    m_message = msg
                    Return False
                ElseIf strMD5Hash <> oFileInfo.MD5Hash Then
                    Dim msg = "Dataset " & dataPkgJob.Dataset & " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported"
                    LogError(msg & ": MD5 hash mismatch of " & strMD5Hash & " for job " & dataPkgJob.Job & " vs. " & oFileInfo.MD5Hash & " for job " & oFileInfo.JobInfo.Job)
                    m_message = msg
                    Return False
                End If

                ' The files match; no point in making a new .mgf file
                Return True
            Else

                Dim strFilename As String = CheckFilenameCase(fiCDTAFile, dataPkgJob.Dataset)

                oFileInfo = New clsPXFileInfoBase(strFilename, dataPkgJob)

                ' File ID doesn't matter; just use 0
                oFileInfo.FileID = 0
                oFileInfo.Length = fiCDTAFile.Length
                oFileInfo.MD5Hash = strMD5Hash

                mCDTAFileStats.Add(fiCDTAFile.Name, oFileInfo)
            End If

            If Not mDTAtoMGF.ProcessFile(fiCDTAFile.FullName) Then
                Dim msg = "Error converting " & fiCDTAFile.Name & " to a .mgf file for job " & dataPkgJob.Job
                LogError(msg & ": " & mDTAtoMGF.GetErrorMessage())
                m_message = msg
                Return False
            Else
                ' Delete the _dta.txt file
                Try
                    fiCDTAFile.Delete()
                Catch ex As Exception
                    ' Ignore errors here
                End Try
            End If

            Threading.Thread.Sleep(125)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            Dim fiNewMGFFile As FileInfo
            fiNewMGFFile = New FileInfo(Path.Combine(m_WorkDir, dataPkgJob.Dataset & ".mgf"))

            If Not fiNewMGFFile.Exists Then
                ' MGF file was not created
                Dim msg = "A .mgf file was not created for the _dta.txt file for job " & dataPkgJob.Job
                m_message = msg
                LogError(msg & ": " & mDTAtoMGF.GetErrorMessage())
                Return False
            End If

            strMGFFilePath = fiNewMGFFile.FullName

        Catch ex As Exception
            LogError("Exception in ConvertCDTAToMGF", ex)
            Return False
        End Try

        Return True

    End Function

    Private Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        LogWarning("Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Make sure the PRIDEConverter console output file is retained
        m_jobParams.RemoveResultFileToSkip(PRIDEConverter_CONSOLE_OUTPUT)

        ' Skip the .mgf files; no need to put them in the FailedResults folder
        m_jobParams.AddResultFileExtensionToSkip(".mgf")

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
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

    End Sub

    ''' <summary>
    ''' Counts the number of items of type eFileType in mPxResultFiles
    ''' </summary>
    ''' <param name="eFileType"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function CountResultFilesByType(eFileType As clsPXFileInfoBase.ePXFileType) As Integer
        Dim intCount As Integer
        intCount = (From item In mPxResultFiles Where item.Value.PXFileType = eFileType Select item).Count()

        Return intCount

    End Function

    ''' <summary>
    ''' Creates (or retrieves) the .mzXML file for this dataset if it does not exist in the working directory
    ''' Utilizes dataset info stored in several packed job parameters
    ''' Newly created .mzXML files will be copied to the MSXML_Cache folder
    ''' </summary>
    ''' <returns>True if the file exists or was created</returns>
    ''' <remarks></remarks>
    Private Function CreateMzXMLFileIfMissing(
       strDataset As String,
       objAnalysisResults As clsAnalysisResults,
       dctDatasetRawFilePaths As IReadOnlyDictionary(Of String, String)) As Boolean

        Dim blnSuccess As Boolean
        Dim strDestPath As String = String.Empty

        Try
            ' Look in m_WorkDir for the .mzXML file for this dataset
            Dim fiMzXmlFilePathLocal As FileInfo
            fiMzXmlFilePathLocal = New FileInfo(Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZXML_EXTENSION))

            If fiMzXmlFilePathLocal.Exists Then
                If Not mPreviousDatasetFilesToDelete.Contains(fiMzXmlFilePathLocal.FullName) Then
                    AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName)
                End If
                Return True
            End If

            ' .mzXML file not found
            ' Look for a StoragePathInfo file
            Dim strMzXmlStoragePathFile As String = fiMzXmlFilePathLocal.FullName & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX

            If File.Exists(strMzXmlStoragePathFile) Then
                blnSuccess = RetrieveStoragePathInfoTargetFile(strMzXmlStoragePathFile, objAnalysisResults, strDestPath)
                If blnSuccess Then
                    AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath)
                    Return True
                End If
            End If

            ' Need to create the .mzXML file

            Dim dctDatasetYearQuarter = ExtractPackedJobParameterDictionary(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER)

            If Not dctDatasetRawFilePaths.ContainsKey(strDataset) Then
                LogError("Dataset " & strDataset & " not found in job parameter " & clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS &
                         "; unable to create the missing .mzXML file")
                Return False
            End If

            m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt")

            mMSXmlCreator = New AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams)
            mMSXmlCreator.UpdateDatasetName(strDataset)

            ' Make sure the dataset file is present in the working directory
            ' Copy it locally if necessary

            Dim strDatasetFilePathRemote = dctDatasetRawFilePaths(strDataset)

            Dim blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathRemote)

            Dim strDatasetFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(strDatasetFilePathRemote))

            If blnDatasetFileIsAFolder Then
                ' Confirm that the dataset folder exists in the working directory

                If Not Directory.Exists(strDatasetFilePathLocal) Then
                    ' Directory not found; look for a storage path info file
                    If File.Exists(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
                        RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX, objAnalysisResults, IsFolder:=True, strDestPath:=strDestPath)
                    Else
                        ' Copy the dataset folder locally
                        objAnalysisResults.CopyDirectory(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite:=True)
                    End If
                End If

            Else
                ' Confirm that the dataset file exists in the working directory
                If Not File.Exists(strDatasetFilePathLocal) Then
                    ' File not found; Look for a storage path info file
                    If File.Exists(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
                        RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX, objAnalysisResults, strDestPath)
                        AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath)
                    Else
                        ' Copy the dataset file locally
                        objAnalysisResults.CopyFileWithRetry(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite:=True)
                        AddToListIfNew(mPreviousDatasetFilesToDelete, strDatasetFilePathLocal)
                    End If
                End If
                m_jobParams.AddResultFileToSkip(Path.GetFileName(strDatasetFilePathLocal))
            End If

            blnSuccess = mMSXmlCreator.CreateMZXMLFile()

            If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
                m_message = mMSXmlCreator.ErrorMessage
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Unknown error creating the mzXML file for dataset " & strDataset
                ElseIf Not m_message.Contains(strDataset) Then
                    m_message &= "; dataset " & strDataset
                End If
                LogError(m_message)
            End If

            If Not blnSuccess Then Return False

            fiMzXmlFilePathLocal.Refresh()
            If fiMzXmlFilePathLocal.Exists Then
                AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName)
            Else
                LogError("MSXmlCreator did not create the .mzXML file for dataset " & strDataset)
                Return False
            End If

            ' Copy the .mzXML file to the cache

            Dim strMSXmlGeneratorName As String = Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath)
            Dim strDatasetYearQuarter As String = String.Empty
            If Not dctDatasetYearQuarter.TryGetValue(strDataset, strDatasetYearQuarter) Then
                strDatasetYearQuarter = String.Empty
            End If

            CopyMzXMLFileToServerCache(fiMzXmlFilePathLocal.FullName, strDatasetYearQuarter, strMSXmlGeneratorName, blnPurgeOldFilesIfNeeded:=True)

            m_jobParams.AddResultFileToSkip(Path.GetFileName(fiMzXmlFilePathLocal.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX))

            Threading.Thread.Sleep(250)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            Try
                If blnDatasetFileIsAFolder Then
                    ' Delete the local dataset folder
                    If Directory.Exists(strDatasetFilePathLocal) Then
                        Directory.Delete(strDatasetFilePathLocal, True)
                    End If
                Else
                    ' Delete the local dataset file
                    If File.Exists(strDatasetFilePathLocal) Then
                        File.Delete(strDatasetFilePathLocal)
                    End If
                End If
            Catch ex As Exception
                ' Ignore errors here
            End Try

        Catch ex As Exception
            LogError("Exception in CreateMzXMLFileIfMissing", ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    Private Function CreatePseudoMSGFFileUsingPHRPReader(
      intJob As Integer,
      strDataset As String,
      udtFilterThresholds As udtFilterThresholdsType,
      lstPseudoMSGFData As IDictionary(Of String, List(Of udtPseudoMSGFDataType))) As String

        Const MSGF_SPECPROB_NOTDEFINED = 10
        Const PVALUE_NOTDEFINED = 10

        Dim strPseudoMsgfFilePath As String

        Dim blnFDRValuesArePresent = False
        Dim blnPepFDRValuesArePresent = False
        Dim blnMSGFValuesArePresent = False

        Try
            Dim dataPkgJob As clsDataPackageJobInfo = Nothing

            If Not mDataPackagePeptideHitJobs.TryGetValue(intJob, dataPkgJob) Then
                LogError("Job " & intJob & " not found in mDataPackagePeptideHitJobs; this is unexpected")
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
            Dim dctBestMatchByScan = New Dictionary(Of Integer, KeyValuePair(Of Double, String))
            Dim dctBestMatchByScanScoreValues = New Dictionary(Of Integer, udtPseudoMSGFDataType)

            Dim strMzXMLFilename = strDataset & ".mzXML"

            ' Determine the correct capitalization for the mzXML file
            Dim diWorkdir = New DirectoryInfo(m_WorkDir)
            Dim fiFiles() As FileInfo = diWorkdir.GetFiles(strMzXMLFilename)

            If fiFiles.Length > 0 Then
                strMzXMLFilename = fiFiles(0).Name
            Else
                ' mzXML file not found; don't worry about this right now (it's possible that CreateMSGFReportFilesOnly = True)
            End If

            Dim strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset)

            Dim strSynopsisFilePath = Path.Combine(m_WorkDir, strSynopsisFileName)

            If Not File.Exists(strSynopsisFilePath) Then
                Dim strSynopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynopsisFilePath, "Dataset_msgfdb.txt")
                If File.Exists(strSynopsisFilePathAlt) Then
                    strSynopsisFilePath = strSynopsisFilePathAlt
                End If
            End If

            ' Check whether PHRP files with a prefix of "Job12345_" exist
            ' This prefix is added by RetrieveDataPackagePeptideHitJobPHRPFiles if multiple peptide_hit jobs are included for the same dataset
            Dim strSynopsisFilePathWithJob = Path.Combine(m_WorkDir, "Job" & dataPkgJob.Job & "_" & strSynopsisFileName)

            If File.Exists(strSynopsisFilePathWithJob) Then
                strSynopsisFilePath = String.Copy(strSynopsisFilePathWithJob)
            ElseIf Not File.Exists(strSynopsisFilePath) Then
                Dim strSynopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynopsisFilePathWithJob, "Dataset_msgfdb.txt")
                If File.Exists(strSynopsisFilePathAlt) Then
                    strSynopsisFilePath = strSynopsisFilePathAlt
                End If
            End If

            Using objReader = New clsPHRPReader(strSynopsisFilePath, dataPkgJob.PeptideHitResultType, True, True)

                objReader.SkipDuplicatePSMs = False

                ' Read the data, filtering on either PepFDR or FDR if defined, or MSGF_SpecProb if PepFDR and/or FDR are not available
                While objReader.MoveNext()

                    Dim blnValidPSM = True
                    Dim blnThresholdChecked = False

                    Dim dblMSGFSpecProb = CDbl(MSGF_SPECPROB_NOTDEFINED)
                    Dim dblFDR = CDbl(-1)
                    Dim dblPepFDR = CDbl(-1)
                    Dim dblPValue = CDbl(PVALUE_NOTDEFINED)
                    Dim dblScoreForCurrentMatch = CDbl(100)

                    ' Determine MSGFSpecProb; store 10 if we don't find a valid number
                    If Not Double.TryParse(objReader.CurrentPSM.MSGFSpecProb, dblMSGFSpecProb) Then
                        dblMSGFSpecProb = MSGF_SPECPROB_NOTDEFINED
                    End If

                    Select Case dataPkgJob.PeptideHitResultType
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

                        Dim kvIndexAndSequence As KeyValuePair(Of Integer, String) = Nothing

                        If Not mCachedProteins.TryGetValue(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence) Then

                            ' Protein not found in mCachedProteins
                            ' If the search engine is MSGFDB and the protein name starts with REV_ or XXX_ then skip this protein since it's a decoy result
                            ' Otherwise, add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                            Dim strProteinUCase As String = objReader.CurrentPSM.ProteinFirst.ToUpper()

                            If dataPkgJob.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
                                If strProteinUCase.StartsWith("REV_") OrElse strProteinUCase.StartsWith("XXX_") Then
                                    blnValidPSM = False
                                End If
                            Else
                                If strProteinUCase.StartsWith("REVERSED_") OrElse strProteinUCase.StartsWith("SCRAMBLED_") OrElse strProteinUCase.StartsWith("XXX.") Then
                                    blnValidPSM = False
                                End If
                            End If

                            If blnValidPSM Then
                                kvIndexAndSequence = New KeyValuePair(Of Integer, String)(mCachedProteins.Count, String.Empty)
                                mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, 0)
                                mCachedProteins.Add(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence)
                            End If

                        End If

                    End If

                    If blnValidPSM Then

                        ' These fields are used to hold different scores depending on the search engine
                        Dim strTotalPRMScore = "0"
                        Dim strPValue = "0"
                        Dim strDeltaScore = "0"
                        Dim strDeltaScoreOther = "0"

                        Select Case dataPkgJob.PeptideHitResultType
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
                        strMSGFText = strMzXMLFilename & ControlChars.Tab &
                           objReader.CurrentPSM.ScanNumber & ControlChars.Tab &
                           objReader.CurrentPSM.Peptide & ControlChars.Tab &
                           objReader.CurrentPSM.ProteinFirst & ControlChars.Tab &
                           objReader.CurrentPSM.Charge & ControlChars.Tab &
                           objReader.CurrentPSM.MSGFSpecProb & ControlChars.Tab &
                           objReader.CurrentPSM.PeptideCleanSequence.Length() & ControlChars.Tab &
                           strTotalPRMScore & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           objReader.CurrentPSM.NumTrypticTerminii & ControlChars.Tab &
                           strPValue & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           strDeltaScore & ControlChars.Tab &
                           strDeltaScoreOther & ControlChars.Tab &
                           objReader.CurrentPSM.ResultID & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           "0" & ControlChars.Tab &
                           objReader.CurrentPSM.MSGFSpecProb

                        ' Add or update dctBestMatchByScan and dctBestMatchByScanScoreValues
                        Dim kvBestMatchForScan As KeyValuePair(Of Double, String) = Nothing
                        Dim blnNewScanNumber As Boolean

                        If dctBestMatchByScan.TryGetValue(objReader.CurrentPSM.ScanNumber, kvBestMatchForScan) Then
                            If dblScoreForCurrentMatch >= kvBestMatchForScan.Key Then
                                ' Skip this result since it has a lower score than the match already stored in dctBestMatchByScan
                                blnValidPSM = False
                            Else
                                ' Update dctBestMatchByScan
                                dctBestMatchByScan(objReader.CurrentPSM.ScanNumber) = New KeyValuePair(Of Double, String)(dblScoreForCurrentMatch, strMSGFText)
                                blnValidPSM = True
                            End If
                            blnNewScanNumber = False
                        Else
                            ' Scan not yet present in dctBestMatchByScan; add it
                            kvBestMatchForScan = New KeyValuePair(Of Double, String)(dblScoreForCurrentMatch, strMSGFText)
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

                            Dim udtPseudoMSGFData = New udtPseudoMSGFDataType
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
                strPseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset & "_Job" & dataPkgJob.Job.ToString() & FILE_EXTENSION_PSEUDO_MSGF)
            Else
                strPseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset & FILE_EXTENSION_PSEUDO_MSGF)
            End If

            Using swMSGFFile = New StreamWriter(New FileStream(strPseudoMsgfFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                ' Write the header line
                swMSGFFile.WriteLine(
                 "#SpectrumFile" & ControlChars.Tab &
                 "Scan#" & ControlChars.Tab &
                 "Annotation" & ControlChars.Tab &
                 "Protein" & ControlChars.Tab &
                 "Charge" & ControlChars.Tab &
                 "MQScore" & ControlChars.Tab &
                 "Length" & ControlChars.Tab &
                 "TotalPRMScore" & ControlChars.Tab &
                 "MedianPRMScore" & ControlChars.Tab &
                 "FractionY" & ControlChars.Tab &
                 "FractionB" & ControlChars.Tab &
                 "Intensity" & ControlChars.Tab &
                 "NTT" & ControlChars.Tab &
                 "p-value" & ControlChars.Tab &
                 "F-Score" & ControlChars.Tab &
                 "DeltaScore" & ControlChars.Tab &
                 "DeltaScoreOther" & ControlChars.Tab &
                 "RecordNumber" & ControlChars.Tab &
                 "DBFilePos" & ControlChars.Tab &
                 "SpecFilePos" & ControlChars.Tab &
                 "SpecProb"
                 )

                ' Write out the filter-passing matches to the pseudo MSGF text file
                For Each kvItem As KeyValuePair(Of Integer, KeyValuePair(Of Double, String)) In dctBestMatchByScan
                    swMSGFFile.WriteLine(kvItem.Value.Value)
                Next

            End Using

            ' Store the filter-passing matches in lstPseudoMSGFData
            For Each kvItem As KeyValuePair(Of Integer, udtPseudoMSGFDataType) In dctBestMatchByScanScoreValues

                Dim lstMatchesForProtein As List(Of udtPseudoMSGFDataType) = Nothing
                If lstPseudoMSGFData.TryGetValue(kvItem.Value.Protein, lstMatchesForProtein) Then
                    lstMatchesForProtein.Add(kvItem.Value)
                Else
                    lstMatchesForProtein = New List(Of udtPseudoMSGFDataType)
                    lstMatchesForProtein.Add(kvItem.Value)
                    lstPseudoMSGFData.Add(kvItem.Value.Protein, lstMatchesForProtein)
                End If

            Next


        Catch ex As Exception
            LogError("Exception in CreatePseudoMSGFFileUsingPHRPReader", ex)
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
    Private Function CreateMSGFReportFile(
       intJob As Integer,
       strDataset As String,
       udtFilterThresholds As udtFilterThresholdsType,
       <Out()> ByRef strPrideReportXMLFilePath As String) As Boolean

        Dim blnSuccess As Boolean

        Dim strTemplateFileName As String
        Dim strPseudoMsgfFilePath As String

        Dim strLocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
        Dim strOrgDBNameGenerated As String
        Dim strProteinCollectionListOrFasta As String

        Dim lstPseudoMSGFData = New Dictionary(Of String, List(Of udtPseudoMSGFDataType))

        strPrideReportXMLFilePath = String.Empty

        Try

            strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing:=False)

            strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob), String.Empty)
            If String.IsNullOrEmpty(strOrgDBNameGenerated) Then
                LogError("Job parameter " & clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob) &
                         " was not found in CreateMSGFReportFile; unable to continue")
                Return False
            End If

            Dim dataPkgJob As clsDataPackageJobInfo = Nothing

            If Not mDataPackagePeptideHitJobs.TryGetValue(intJob, dataPkgJob) Then
                LogError("Job " & intJob & " not found in mDataPackagePeptideHitJobs; unable to continue")
                Return False
            End If

            If Not String.IsNullOrEmpty(dataPkgJob.ProteinCollectionList) AndAlso dataPkgJob.ProteinCollectionList <> "na" Then
                strProteinCollectionListOrFasta = dataPkgJob.ProteinCollectionList
            Else
                strProteinCollectionListOrFasta = dataPkgJob.LegacyFastaFileName
            End If

            If mCachedOrgDBName <> strOrgDBNameGenerated Then
                ' Need to read the proteins from the fasta file

                mCachedProteins.Clear()
                mCachedProteinPSMCounts.Clear()

                Dim objFastaFileReader As ProteinFileReader.FastaFileReader
                Dim strFastaFilePath As String = Path.Combine(strLocalOrgDBFolder, strOrgDBNameGenerated)
                objFastaFileReader = New ProteinFileReader.FastaFileReader()

                If Not objFastaFileReader.OpenFile(strFastaFilePath) Then
                    Dim msg = "Error opening fasta file " & strOrgDBNameGenerated & "; objFastaFileReader.OpenFile() returned false"
                    LogError(msg & "; see " & strLocalOrgDBFolder)
                    m_message = msg
                    Return False
                Else
                    Console.WriteLine()
                    Console.WriteLine("Reading proteins from " & strFastaFilePath)

                    Do While objFastaFileReader.ReadNextProteinEntry()
                        If Not mCachedProteins.ContainsKey(objFastaFileReader.ProteinName) Then
                            Dim kvIndexAndSequence As KeyValuePair(Of Integer, String)
                            kvIndexAndSequence = New KeyValuePair(Of Integer, String)(mCachedProteins.Count, objFastaFileReader.ProteinSequence)

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
                For intIndex = 0 To mCachedProteinPSMCounts.Count
                    mCachedProteinPSMCounts(intIndex) = 0
                Next
            End If

            lstPseudoMSGFData.Clear()

            strPseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(intJob, strDataset, udtFilterThresholds, lstPseudoMSGFData)

            If String.IsNullOrEmpty(strPseudoMsgfFilePath) Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("Pseudo Msgf file not created for job " & intJob & ", dataset " & strDataset)
                End If
                Return False
            End If

            AddToListIfNew(mPreviousDatasetFilesToDelete, strPseudoMsgfFilePath)

            If Not mCreateMSGFReportFilesOnly Then

                strPrideReportXMLFilePath = CreateMSGFReportXMLFile(strTemplateFileName, dataPkgJob, strPseudoMsgfFilePath, lstPseudoMSGFData, strOrgDBNameGenerated, strProteinCollectionListOrFasta, udtFilterThresholds)

                If String.IsNullOrEmpty(strPrideReportXMLFilePath) Then
                    If String.IsNullOrEmpty(m_message) Then
                        LogError("Pride report XML file not created for job " & intJob & ", dataset " & strDataset)
                    End If
                    Return False
                End If

            End If

            blnSuccess = True

        Catch ex As Exception
            LogError("Exception in CreateMSGFReportFile for job " & intJob & ", dataset " & strDataset, ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    Private Function CreateMSGFReportXMLFile(
      strTemplateFileName As String,
      dataPkgJob As clsDataPackageJobInfo,
      strPseudoMsgfFilePath As String,
      lstPseudoMSGFData As IReadOnlyDictionary(Of String, List(Of udtPseudoMSGFDataType)),
      strOrgDBNameGenerated As String,
      strProteinCollectionListOrFasta As String,
      udtFilterThresholds As udtFilterThresholdsType) As String


        Dim strPrideReportXMLFilePath As String

        Dim blnInsideMzDataDescription As Boolean
        Dim blnSkipNode As Boolean
        Dim blnInstrumentDetailsAutoDefined = False

        Dim lstAttributeOverride = New Dictionary(Of String, String)

        Dim lstElementCloseDepths As Stack(Of Integer)

        Dim eFileLocation = eMSGFReportXMLFileLocation.Header
        Dim lstRecentElements = New Queue(Of String)


        Try
            lstElementCloseDepths = New Stack(Of Integer)

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

            Using objXmlWriter = New XmlTextWriter(New FileStream(strPrideReportXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), New Text.UTF8Encoding(False))
                objXmlWriter.Formatting = Formatting.Indented
                objXmlWriter.Indentation = 4

                objXmlWriter.WriteStartDocument()

                Using objXmlReader = New XmlTextReader(New FileStream(Path.Combine(m_WorkDir, strTemplateFileName), FileMode.Open, FileAccess.Read, FileShare.Read))

                    Do While objXmlReader.Read()

                        Select Case objXmlReader.NodeType
                            Case XmlNodeType.Whitespace
                                ' Skip whitespace since the writer should be auto-formatting things
                                ' objXmlWriter.WriteWhitespace(objXmlReader.Value)

                            Case XmlNodeType.Comment
                                objXmlWriter.WriteComment(objXmlReader.Value)

                            Case XmlNodeType.Element
                                ' Start element

                                If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
                                lstRecentElements.Enqueue("Element " & objXmlReader.Name)

                                Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
                                    lstElementCloseDepths.Pop()

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
                                        objXmlWriter.WriteElementString("timeCreated", DateTime.Now.ToUniversalTime().ToString("s") & "Z")
                                        blnSkipNode = True

                                    Case "MzDataDescription"
                                        blnInsideMzDataDescription = True

                                    Case "sampleName"
                                        If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
                                            ' Write out the current job's Experiment Name
                                            objXmlWriter.WriteElementString("sampleName", dataPkgJob.Experiment)
                                            blnSkipNode = True
                                        End If

                                    Case "sampleDescription"
                                        If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
                                            ' Override the comment attribute for this node
                                            Dim strCommentOverride As String

                                            If Not String.IsNullOrWhiteSpace(dataPkgJob.Experiment_Reason) Then
                                                strCommentOverride = dataPkgJob.Experiment_Reason.TrimEnd()

                                                If Not String.IsNullOrWhiteSpace(dataPkgJob.Experiment_Comment) Then
                                                    If strCommentOverride.EndsWith(".") Then
                                                        strCommentOverride &= " " & dataPkgJob.Experiment_Comment
                                                    Else
                                                        strCommentOverride &= ". " & dataPkgJob.Experiment_Comment
                                                    End If
                                                End If
                                            Else
                                                strCommentOverride = dataPkgJob.Experiment_Comment
                                            End If

                                            lstAttributeOverride.Add("comment", strCommentOverride)
                                        End If

                                    Case "sourceFile"
                                        If eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin Then
                                            objXmlWriter.WriteStartElement("sourceFile")

                                            objXmlWriter.WriteElementString("nameOfFile", Path.GetFileName(strPseudoMsgfFilePath))
                                            objXmlWriter.WriteElementString("pathToFile", strPseudoMsgfFilePath)
                                            objXmlWriter.WriteElementString("fileType", "MSGF file")

                                            objXmlWriter.WriteEndElement()  ' sourceFile
                                            blnSkipNode = True

                                        End If

                                    Case "software"
                                        If eFileLocation = eMSGFReportXMLFileLocation.MzDataDataProcessing Then
                                            CreateMSGFReportXmlFileWriteSoftwareVersion(objXmlReader, objXmlWriter, dataPkgJob.PeptideHitResultType)
                                            blnSkipNode = True
                                        End If

                                    Case "instrumentName"
                                        If eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument Then
                                            ' Write out the actual instrument name
                                            objXmlWriter.WriteElementString("instrumentName", dataPkgJob.Instrument)
                                            blnSkipNode = True

                                            blnInstrumentDetailsAutoDefined = WriteXMLInstrumentInfo(objXmlWriter, dataPkgJob.InstrumentGroup)

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
                                                        strValueOverride = "DMS PRIDE_Converter " & Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
                                                    End If

                                                    If objXmlReader.Name = "value" AndAlso strValueOverride.Length > 0 Then
                                                        objXmlWriter.WriteAttributeString(objXmlReader.Name, strValueOverride)
                                                    Else
                                                        objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value)
                                                    End If

                                                Loop While objXmlReader.MoveToNextAttribute()
                                            End If

                                            objXmlWriter.WriteEndElement()  ' cvParam
                                            blnSkipNode = True

                                        End If

                                    Case "Identifications"
                                        If Not CreateMSGFReportXMLFileWriteIDs(objXmlWriter, lstPseudoMSGFData, strOrgDBNameGenerated) Then
                                            LogError("CreateMSGFReportXMLFileWriteIDs returned false; aborting")
                                            Return String.Empty
                                        End If

                                        If Not CreateMSGFReportXMLFileWriteProteins(objXmlWriter, strOrgDBNameGenerated) Then
                                            LogError("CreateMSGFReportXMLFileWriteProteins returned false; aborting")
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

                                        objXmlWriter.WriteEndElement()      ' DatabaseMapping
                                        objXmlWriter.WriteEndElement()      ' DatabaseMappings

                                        blnSkipNode = True

                                    Case "ConfigurationOptions"
                                        objXmlWriter.WriteStartElement("ConfigurationOptions")

                                        WriteConfigurationOption(objXmlWriter, "search_engine", "MSGF")
                                        WriteConfigurationOption(objXmlWriter, "peptide_threshold", udtFilterThresholds.PValueThreshold.ToString("0.00"))
                                        WriteConfigurationOption(objXmlWriter, "add_carbamidomethylation", "false")

                                        objXmlWriter.WriteEndElement()      ' ConfigurationOptions

                                        blnSkipNode = True

                                End Select


                                If blnSkipNode Then
                                    If objXmlReader.NodeType <> XmlNodeType.EndElement Then
                                        ' Skip this element (and any children nodes enclosed in this elemnt)
                                        ' Likely should not do this when objXmlReader.NodeType is XmlNodeType.EndElement
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

                            Case XmlNodeType.EndElement

                                If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
                                lstRecentElements.Enqueue("EndElement " & objXmlReader.Name)

                                Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth + 1
                                    lstElementCloseDepths.Pop()
                                    objXmlWriter.WriteEndElement()
                                Loop

                                objXmlWriter.WriteEndElement()

                                If objXmlReader.Name = "MzDataDescription" Then
                                    blnInsideMzDataDescription = False
                                End If

                                Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
                                    lstElementCloseDepths.Pop()
                                Loop

                            Case XmlNodeType.Text

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
            LogError("Exception in CreateMSGFReportXMLFile", ex)

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

    Private Function CreateMSGFReportXMLFileWriteIDs(
      objXmlWriter As XmlTextWriter,
      lstPseudoMSGFData As IReadOnlyDictionary(Of String, List(Of udtPseudoMSGFDataType)),
      strOrgDBNameGenerated As String) As Boolean

        Try

            objXmlWriter.WriteStartElement("Identifications")

            For Each kvProteinEntry As KeyValuePair(Of String, List(Of udtPseudoMSGFDataType)) In lstPseudoMSGFData

                Dim kvIndexAndSequence As KeyValuePair(Of Integer, String) = Nothing

                If Not mCachedProteins.TryGetValue(kvProteinEntry.Key, kvIndexAndSequence) Then
                    ' Protein not found in mCachedProteins; this is unexpected (should have already been added by CreatePseudoMSGFFileUsingPHRPReader()
                    ' Add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                    kvIndexAndSequence = New KeyValuePair(Of Integer, String)(mCachedProteins.Count, String.Empty)
                    mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, kvProteinEntry.Value.Count)
                    mCachedProteins.Add(kvProteinEntry.Key, kvIndexAndSequence)

                Else
                    mCachedProteinPSMCounts(kvIndexAndSequence.Key) = kvProteinEntry.Value.Count
                End If

                objXmlWriter.WriteStartElement("Identification")

                objXmlWriter.WriteElementString("Accession", kvProteinEntry.Key)            ' Protein name
                ' objXmlWriter.WriteElementString("CuratedAccession", kvProteinEntry.Key)		' Cleaned-up version of the Protein name; for example, for ref|NP_035862.2 we would put "NP_035862" here
                objXmlWriter.WriteElementString("UniqueIdentifier", kvProteinEntry.Key)     ' Protein name
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

                    ' Could write out details of dynamic mods
                    '    Would need to update DMS to include the PSI-Compatible mod names, descriptions, and masses.
                    '    However, since we're now submitting .mzid.gz files to PRIDE and not .msgf-report.xml files, this update is not necessary
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

                    objXmlWriter.WriteElementString("UniqueIdentifier", udtPeptide.ScanNumber.ToString())       ' I wanted to record ResultID here, but we instead have to record Scan Number; otherwise PRIDE Converter Crashes

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


                    objXmlWriter.WriteEndElement()      ' additional

                    objXmlWriter.WriteEndElement()      ' Peptide
                Next

                ' Protein level-scores
                objXmlWriter.WriteElementString("Score", "0.0")
                objXmlWriter.WriteElementString("Threshold", "0.0")
                objXmlWriter.WriteElementString("SearchEngine", "MSGF")

                objXmlWriter.WriteStartElement("additional")
                objXmlWriter.WriteEndElement()

                objXmlWriter.WriteElementString("FastaSequenceReference", kvIndexAndSequence.Key.ToString())

                objXmlWriter.WriteEndElement()      ' Identification


            Next

            objXmlWriter.WriteEndElement()          ' Identifications

        Catch ex As Exception
            LogError("Exception in CreateMSGFReportXMLFileWriteIDs", ex)
            Return False
        End Try

        Return True

    End Function

    Private Function CreateMSGFReportXMLFileWriteProteins(
      objXmlWriter As XmlTextWriter,
      strOrgDBNameGenerated As String) As Boolean

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
            For Each kvEntry As KeyValuePair(Of String, KeyValuePair(Of Integer, String)) In mCachedProteins

                strProteinName = String.Copy(kvEntry.Key)
                intProteinIndex = kvEntry.Value.Key

                ' Only write out this protein if it had 1 or more PSMs
                If mCachedProteinPSMCounts.TryGetValue(intProteinIndex, intPSMCount) Then
                    If intPSMCount > 0 Then
                        objXmlWriter.WriteStartElement("Sequence")
                        objXmlWriter.WriteAttributeString("id", intProteinIndex.ToString())
                        objXmlWriter.WriteAttributeString("accession", strProteinName)

                        objXmlWriter.WriteValue(kvEntry.Value.Value)

                        objXmlWriter.WriteEndElement()          ' Sequence
                    End If
                End If
            Next

            objXmlWriter.WriteEndElement()          ' Fasta


            ' In the future, we might write out customized PTMs here
            ' For now, just copy over whatever is in the template msgf-report.xml file
            '
            'objXmlWriter.WriteStartElement("PTMs")
            'objXmlWriter.WriteFullEndElement()


        Catch ex As Exception
            LogError("Exception in CreateMSGFReportXMLFileWriteProteins", ex)
            Return False
        End Try

        Return True

    End Function

    Private Sub CreateMSGFReportXmlFileWriteSoftwareVersion(objXmlReader As XmlTextReader, objXmlWriter As XmlTextWriter, PeptideHitResultType As clsPHRPReader.ePeptideHitResultType)

        Dim strToolName As String = String.Empty
        Dim strToolVersion As String = String.Empty
        Dim strToolComments As String = String.Empty
        Dim intNodeDepth As Integer = objXmlReader.Depth

        ' Read the name, version, and comments elements under software
        Do While objXmlReader.Read()
            Select Case objXmlReader.NodeType
                Case XmlNodeType.Element
                    Select Case objXmlReader.Name
                        Case "name"
                            strToolName = objXmlReader.ReadElementContentAsString()
                        Case "version"
                            strToolVersion = objXmlReader.ReadElementContentAsString()
                        Case "comments"
                            strToolComments = objXmlReader.ReadElementContentAsString()
                    End Select
                Case XmlNodeType.EndElement
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

        objXmlWriter.WriteEndElement()  ' software

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
    Private Function CreatePrideXMLFile(
       intJob As Integer,
       strDataset As String,
       strPrideReportXMLFilePath As String,
       <Out()> ByRef strPrideXmlFilePath As String) As Boolean

        Dim blnSuccess As Boolean
        Dim strCurrentTask As String

        Dim strBaseFileName As String
        Dim strMsgfResultsFilePath As String
        Dim strMzXMLFilePath As String

        strPrideXmlFilePath = String.Empty

        Try

            strBaseFileName = Path.GetFileName(strPrideReportXMLFilePath).Replace(FILE_EXTENSION_MSGF_REPORT_XML, String.Empty)
            strMsgfResultsFilePath = Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_PSEUDO_MSGF)
            strMzXMLFilePath = Path.Combine(m_WorkDir, strDataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
            strPrideReportXMLFilePath = Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_MSGF_REPORT_XML)

            strCurrentTask = "Running PRIDE Converter for job " & intJob & ", " & strDataset
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strCurrentTask)
            End If

            blnSuccess = RunPrideConverter(intJob, strDataset, strMsgfResultsFilePath, strMzXMLFilePath, strPrideReportXMLFilePath)

            If Not blnSuccess Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("Unknown error calling RunPrideConverter", m_message)
                End If
            Else
                ' Make sure the result file was created
                strPrideXmlFilePath = Path.Combine(m_WorkDir, strBaseFileName & FILE_EXTENSION_MSGF_PRIDE_XML)
                If Not File.Exists(strPrideXmlFilePath) Then
                    LogError("Pride XML file not created for job " & intJob & ": " & strPrideXmlFilePath)
                    Return False
                End If
            End If

            blnSuccess = True

        Catch ex As Exception
            LogError("Exception in CreatePrideXMLFile for job " & intJob & ", dataset " & strDataset, ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    Private Function CreatePXSubmissionFile(dctTemplateParameters As IReadOnlyDictionary(Of String, String)) As Boolean

        Const TBD = "******* UPDATE ****** "

        Dim intPrideXmlFilesCreated As Integer
        Dim intRawFilesStored As Integer
        Dim intPeakFilesStored As Integer
        Dim intMzIDFilesStored As Integer

        Dim strSubmissionType As String
        Dim strFilterText As String = String.Empty

        Dim strPXFilePath As String

        Try

            strPXFilePath = Path.Combine(m_WorkDir, "PX_Submission_" & DateTime.Now.ToString("yyyy-MM-dd_HH-mm") & ".px")

            intPrideXmlFilesCreated = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Result)
            intRawFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Raw)
            intPeakFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Peak)
            intMzIDFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.ResultMzId)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating PXSubmission file: " & strPXFilePath)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intPrideXmlFilesCreated & " Result (.msgf-pride.xml) files")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intRawFilesStored & " Raw files")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intPeakFilesStored & " Peak (.mgf) files")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Result stats: " & intMzIDFilesStored & " Search (.mzid.gz) files")
            End If

            If intMzIDFilesStored = 0 AndAlso intPrideXmlFilesCreated = 0 Then
                strSubmissionType = PARTIAL_SUBMISSION
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Did not create any Pride XML result files; submission type is " & strSubmissionType)

            ElseIf intPrideXmlFilesCreated > 0 AndAlso intMzIDFilesStored > intPrideXmlFilesCreated Then
                strSubmissionType = PARTIAL_SUBMISSION
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Stored more Search (.mzid.gz) files than Pride XML result files; submission type is " & strSubmissionType)

            ElseIf intPrideXmlFilesCreated > 0 AndAlso intRawFilesStored > intPrideXmlFilesCreated Then
                strSubmissionType = PARTIAL_SUBMISSION
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Stored more Raw files than Pride XML result files; submission type is " & strSubmissionType)

            ElseIf intMzIDFilesStored = 0 Then
                strSubmissionType = PARTIAL_SUBMISSION
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Did not have any .mzid.gz files and did not create any Pride XML result files; submission type is " & strSubmissionType)

            Else
                strSubmissionType = COMPLETE_SUBMISSION

                If mFilterThresholdsUsed.UseFDRThreshold OrElse mFilterThresholdsUsed.UsePepFDRThreshold OrElse mFilterThresholdsUsed.UseMSGFSpecProb Then
                    Const strFilterTextBase = "msgf-pride.xml files are filtered on "
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

            Using swPXFile = New StreamWriter(New FileStream(strPXFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                WritePXHeader(swPXFile, "submitter_name", "Matthew Monroe", dctTemplateParameters)
                WritePXHeader(swPXFile, "submitter_email", "matthew.monroe@pnnl.gov", dctTemplateParameters)
                WritePXHeader(swPXFile, "submitter_affiliation", PNNL_NAME_COUNTRY, dctTemplateParameters)
                WritePXHeader(swPXFile, "submitter_pride_login", "matthew.monroe@pnl.gov", dctTemplateParameters)

                WritePXHeader(swPXFile, "lab_head_name", "Richard D. Smith", dctTemplateParameters)
                WritePXHeader(swPXFile, "lab_head_email", "dick.smith@pnnl.gov", dctTemplateParameters)
                WritePXHeader(swPXFile, "lab_head_affiliation", PNNL_NAME_COUNTRY, dctTemplateParameters)

                WritePXHeader(swPXFile, "project_title", TBD & "User-friendly Article Title", dctTemplateParameters)
                WritePXHeader(swPXFile, "project_description", TBD & "Summary sentence", dctTemplateParameters, 50)         ' Minimum 50 characterse, max 5000 characters

                ' We don't normally use the project_tag field, so it is commented out
                ' Example official tags are:
                '  Human proteome project
                '  Human plasma project
                'WritePXHeader(swPXFile, "project_tag", TBD & "Official project tag assigned by the repository", dctTemplateParameters)

                If dctTemplateParameters.ContainsKey("pubmed_id") Then
                    WritePXHeader(swPXFile, "pubmed_id", TBD, dctTemplateParameters)
                End If

                ' We don't normally use this field, so it is commented out
                ' WritePXHeader(swPXFile, "other_omics_link", "Related data is available from PeptideAtlas at http://www.peptideatlas.org/PASS/PASS00297")

                WritePXHeader(swPXFile, "keywords", TBD, dctTemplateParameters)                             ' Comma separated list; suggest at least 3 keywords
                WritePXHeader(swPXFile, "sample_processing_protocol", TBD, dctTemplateParameters, 50)           ' Minimum 50 characters, max 5000 characters
                WritePXHeader(swPXFile, "data_processing_protocol", TBD, dctTemplateParameters, 50)             ' Minimum 50 characters, max 5000 characters

                ' Example values for experiment_type (a given submission can have more than one experiment_type listed)
                '   [PRIDE, PRIDE:0000427, Top-down proteomics, ]
                '   [PRIDE, PRIDE:0000429, Shotgun proteomics, ]
                '   [PRIDE, PRIDE:0000430, Chemical cross-linking coupled with mass spectrometry proteomics, ]
                '   [PRIDE, PRIDE:0000433, Affinity purification coupled with mass spectrometry proteomics, ]
                '   [PRIDE, PRIDE:0000311, SRM/MRM, ]
                '   [PRIDE, PRIDE:0000447, SWATH MS, ]
                '   [PRIDE, PRIDE:0000451, MSE, ]
                '   [PRIDE, PRIDE:0000452, HDMSE, ]
                '   [PRIDE, PRIDE:0000453, PAcIFIC, ]
                '   [PRIDE, PRIDE:0000454, All-ion fragmentation, ]
                '   [MS, MS:1002521, Mass spectrometry imaging,]
                '   [MS, MS:1002521, Mass spectrometry imaging,]
                WritePXHeader(swPXFile, "experiment_type", GetCVString("PRIDE", "PRIDE:0000429", "Shotgun proteomics", ""), dctTemplateParameters)

                WritePXLine(swPXFile, New List(Of String) From {"MTD", "submission_type", strSubmissionType})

                If strSubmissionType = COMPLETE_SUBMISSION Then
                    ' Note that the comment field has been deprecated in v2.x of the px file
                    ' However, we don't have a good alternative place to put this comment, so we'll include it anyway
                    If Not String.IsNullOrWhiteSpace(strFilterText) Then
                        WritePXHeader(swPXFile, "comment", strFilterText)
                    End If
                Else
                    Dim strComment = "Data produced by the DMS Processing pipeline using "
                    If mSearchToolsUsed.Count = 1 Then
                        strComment &= "search tool " & mSearchToolsUsed.First
                    ElseIf mSearchToolsUsed.Count = 2 Then
                        strComment &= "search tools " & mSearchToolsUsed.First & " and " & mSearchToolsUsed.Last
                    ElseIf mSearchToolsUsed.Count > 2 Then
                        strComment &= "search tools " & String.Join(", ", (From item In mSearchToolsUsed Where item <> mSearchToolsUsed.Last Order By item).ToList())
                        strComment &= ", and " & mSearchToolsUsed.Last
                    End If

                    WritePXHeader(swPXFile, "reason_for_partial", strComment)
                End If

                If mExperimentNEWTInfo.Count = 0 Then
                    ' None of the data package jobs had valid NEWT info
                    WritePXHeader(swPXFile, "species", TBD & GetCVString("NEWT", "2323", "unclassified Bacteria", ""), dctTemplateParameters)
                Else
                    ' NEWT info is defined; write it out
                    For Each item In mExperimentNEWTInfo
                        WritePXHeader(swPXFile, "species", GetNEWTCv(item.Key, item.Value))
                    Next
                End If

                WritePXHeader(swPXFile, "tissue", TBD & DEFAULT_TISSUE_CV, dctTemplateParameters)
                WritePXHeader(swPXFile, "cell_type", TBD & "Optional, e.g. " & DEFAULT_CELL_TYPE_CV & DELETION_WARNING, dctTemplateParameters)
                WritePXHeader(swPXFile, "disease", TBD & "Optional, e.g. " & DEFAULT_DISEASE_TYPE_CV & DELETION_WARNING, dctTemplateParameters)

                ' Example values for quantification (a given submission can have more than one type listed)
                '   [PRIDE, PRIDE:0000318, 18O,]
                '   [PRIDE, PRIDE:0000320, AQUA,]
                '   [PRIDE, PRIDE:0000319, ICAT,]
                '   [PRIDE, PRIDE:0000321, ICPL,]
                '   [PRIDE, PRIDE:0000315, SILAC,]
                '   [PRIDE, PRIDE:0000314, TMT,]
                '   [PRIDE, PRIDE:0000313, iTRAQ,] 
                '   [PRIDE, PRIDE:0000323, TIC,]
                '   [PRIDE, PRIDE:0000322, emPAI,]
                '   [PRIDE, PRIDE:0000435, Peptide counting,]
                '   [PRIDE, PRIDE:0000436, Spectral counting,]
                '   [PRIDE, PRIDE:0000437, Protein Abundance Index – PAI,]
                '   [PRIDE, PRIDE:0000438, Spectrum count/molecular weight,]
                '   [PRIDE, PRIDE:0000439, Spectral Abundance Factor – SAF,]
                '   [PRIDE, PRIDE:0000440, Normalized Spectral Abundance Factor – NSAF,]
                '   [PRIDE, PRIDE:0000441, APEX - Absolute Protein Expression,]
                WritePXHeader(swPXFile, "quantification", TBD & "Optional, e.g. " & DEFAULT_QUANTIFICATION_TYPE_CV, dctTemplateParameters)

                If mInstrumentGroupsStored.Count > 0 Then
                    WritePXInstruments(swPXFile)
                Else
                    ' Instrument type is unknown
                    WritePXHeader(swPXFile, "instrument", TBD & GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC"), dctTemplateParameters)
                End If

                ' Note that the modification terms are optional for complete submissions
                ' However, it doesn't hurt to include them
                WritePXMods(swPXFile)

                ' Could write additional terms here
                ' WritePXHeader(swPXFile, "additional", GetCVString("", "", "Patient", "Colorectal cancer patient 1"), dctTemplateParameters)

                ' If this is a re-submission or re-analysis, then use these:
                ' WritePXHeader(swPXFile, "resubmission_px", "PXD00001", dctTemplateParameters)
                ' WritePXHeader(swPXFile, "reanalysis_px", "PXD00001", dctTemplateParameters)


                ' Add a blank line
                swPXFile.WriteLine()

                ' Write the header row for the files
                WritePXLine(swPXFile, New List(Of String) From {"FMH", "file_id", "file_type", "file_path", "file_mapping"})

                Dim lstFileInfoCols = New List(Of String)

                ' Keys in this dictionary are fileIDs, values are file names
                Dim lstResultFileIDs = New Dictionary(Of Integer, String)

                ' Append the files and mapping information to the ProteomeXchange PX file
                For Each item In mPxResultFiles
                    lstFileInfoCols.Clear()

                    lstFileInfoCols.Add("FME")
                    lstFileInfoCols.Add(item.Key.ToString)                      ' file_id
                    Dim fileTypeName = PXFileTypeName(item.Value.PXFileType)
                    lstFileInfoCols.Add(fileTypeName)                           ' file_type; allowed values are result, raw, peak, search, quantification, gel, other
                    lstFileInfoCols.Add(Path.Combine("D:\Upload", m_ResFolderName, item.Value.Filename))    ' file_path

                    Dim lstFileMappings = New List(Of String)
                    For Each mapID In item.Value.FileMappings
                        lstFileMappings.Add(mapID.ToString())                   ' file_mapping
                    Next

                    lstFileInfoCols.Add(String.Join(",", lstFileMappings))

                    WritePXLine(swPXFile, lstFileInfoCols)

                    If fileTypeName = "RESULT" Then
                        lstResultFileIDs.Add(item.Key, item.Value.Filename)
                    End If
                Next

                ' Determine whether the tissue or cell_type columns will bein the SMH section
                Dim smhIncludesCellType As Boolean = DictionaryHasDefinedValue(dctTemplateParameters, "cell_type")
                Dim smhIncludesDisease As Boolean = DictionaryHasDefinedValue(dctTemplateParameters, "disease")

                Dim reJobAddon = New Text.RegularExpressions.Regex("(_Job\d+)(_msgfplus)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

                swPXFile.WriteLine()

                ' Write the header row for the SMH section
                Dim columnNames = New List(Of String) From {"SMH", "file_id", "species", "tissue", "cell_type", "disease", "modification", "instrument", "quantification", "experimental_factor"}

                WritePXLine(swPXFile, columnNames)

                ' Add the SME lines below the SMH line
                For Each resultFile In lstResultFileIDs
                    lstFileInfoCols.Clear()

                    lstFileInfoCols.Add("SME")
                    lstFileInfoCols.Add(resultFile.Key.ToString())          ' file_id

                    Dim sampleMetadata = New clsSampleMetadata()
                    Dim resultFileName = resultFile.Value
                    If Not mMzIdSampleInfo.TryGetValue(resultFile.Value, sampleMetadata) Then
                        ' Result file name may have been customized to include _Job1000000
                        ' Check for this, and update resultFileName if required

                        Dim reMatch = reJobAddon.Match(resultFileName)
                        If reMatch.Success() Then
                            Dim resultFileNameNew = resultFileName.Substring(0, reMatch.Index) + reMatch.Groups(2).Value.ToString + resultFileName.Substring(reMatch.Index + reMatch.Length)
                            resultFileName = resultFileNameNew
                        End If
                    End If

                    If mMzIdSampleInfo.TryGetValue(resultFileName, sampleMetadata) Then
                        lstFileInfoCols.Add(sampleMetadata.Species)      ' species
                        lstFileInfoCols.Add(sampleMetadata.Tissue)       ' tissue

                        If smhIncludesCellType Then
                            lstFileInfoCols.Add(sampleMetadata.CellType)     ' cell_type
                        Else
                            lstFileInfoCols.Add(String.Empty)
                        End If

                        If smhIncludesDisease Then
                            lstFileInfoCols.Add(sampleMetadata.Disease)      ' disease
                        Else
                            lstFileInfoCols.Add(String.Empty)
                        End If

                        Dim strMods As String = String.Empty
                        For Each modEntry In sampleMetadata.Modifications
                            If strMods.Length > 0 Then strMods &= ", "
                            strMods &= GetCVString(modEntry.Value)
                        Next
                        lstFileInfoCols.Add(strMods)                                    ' modification	

                        Dim instrumentAccession = String.Empty
                        Dim instrumentDescription = String.Empty
                        GetInstrumentAccession(sampleMetadata.InstrumentGroup, instrumentAccession, instrumentDescription)

                        Dim strInstrumentCV = GetInstrumentCv(instrumentAccession, instrumentDescription)
                        lstFileInfoCols.Add(strInstrumentCV)                            ' instrument

                        lstFileInfoCols.Add(GetValueOrDefault("quantification)", dctTemplateParameters, sampleMetadata.Quantification))           ' quantification
                        lstFileInfoCols.Add(sampleMetadata.ExperimentalFactor)               ' experimental_factor
                    Else
                        LogWarning(" Sample Metadata not found for " & resultFile.Value)
                    End If

                    WritePXLine(swPXFile, lstFileInfoCols)
                Next

            End Using

        Catch ex As Exception
            LogError("Exception in CreatePXSubmissionFile", ex)
            Return False
        End Try

        Return True

    End Function

    Private Sub DefineFilesToSkipTransfer()

        m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_PSEUDO_MSGF)
        m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_MSGF_REPORT_XML)
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX)

        m_jobParams.AddResultFileToSkip("PRIDEConverter_ConsoleOutput.txt")
        m_jobParams.AddResultFileToSkip("PRIDEConverter_Version.txt")

        Dim diWorkDir = New DirectoryInfo(m_WorkDir)
        For Each fiFile In diWorkDir.GetFiles(clsDataPackageFileHandler.JOB_INFO_FILE_PREFIX & "*.txt")
            m_jobParams.AddResultFileToSkip(fiFile.Name)
        Next

    End Sub

    Private Function DefineProgramPaths() As Boolean

        ' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
        Dim JavaProgLoc = GetJavaProgLoc()
        If String.IsNullOrEmpty(JavaProgLoc) Then
            Return False
        End If

        ' Determine the path to the PRIDEConverter program
        mPrideConverterProgLoc = DetermineProgramLocation("PRIDEConverter", "PRIDEConverterProgLoc", "pride-converter-2.0-SNAPSHOT.jar")

        If String.IsNullOrEmpty(mPrideConverterProgLoc) Then
            If String.IsNullOrEmpty(m_message) Then
                LogError("Error determining PrideConverter program location")
            End If
            Return False
        End If

        mMSXmlGeneratorAppPath = MyBase.GetMSXmlGeneratorAppPath()

        Return True

    End Function

    Private Function DefinePxFileMapping(intFileID As Integer, intParentFileID As Integer) As Boolean

        Dim oPXFileInfo As clsPXFileInfo = Nothing

        If Not mPxResultFiles.TryGetValue(intFileID, oPXFileInfo) Then
            LogError("FileID " & intFileID & " not found in mPxResultFiles; unable to add parent file")
            Return False
        End If

        oPXFileInfo.AddFileMapping(intParentFileID)

        Return True

    End Function

    Private Function DictionaryHasDefinedValue(dctTemplateParameters As IReadOnlyDictionary(Of String, String), termName As String) As Boolean
        Dim value As String = String.Empty

        If dctTemplateParameters.TryGetValue(termName, value) Then
            If Not String.IsNullOrWhiteSpace(value) Then Return True
        End If

        Return False

    End Function

    Private Function GetCVString(cvParamInfo As clsSampleMetadata.udtCvParamInfoType) As String
        Return GetCVString(cvParamInfo.CvRef, cvParamInfo.Accession, cvParamInfo.Name, cvParamInfo.Value)
    End Function

    Private Function GetCVString(cvRef As String, accession As String, name As String, value As String) As String

        If String.IsNullOrEmpty(value) Then
            value = String.Empty
        ElseIf value.Length > 200 Then
            LogWarning("CV value parameter truncated since too long: " & value)
            value = value.Substring(0, 200)
        End If

        Return "[" & cvRef & ", " & accession & ", " & name & ", " & value & "]"
    End Function

    Private Function GetInstrumentCv(accession As String, description As String) As String
        Dim strInstrumentCV As String

        If String.IsNullOrEmpty(accession) Then
            strInstrumentCV = GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC")
        Else
            strInstrumentCV = GetCVString("MS", accession, description, "")
        End If

        Return strInstrumentCV
    End Function

    Private Function GetNEWTCv(newtID As Integer, newtName As String) As String

        If newtID = 0 And String.IsNullOrWhiteSpace(newtName) Then
            newtID = 2323
            newtName = "unclassified Bacteria"
        End If

        Return GetCVString("NEWT", newtID.ToString(), newtName, "")
    End Function

    ''' <summary>
    ''' Determines the Accession and Desription for the given instrument group
    ''' </summary>
    ''' <param name="instrumentGroup"></param>
    ''' <param name="accession">Output parameter</param>
    ''' <param name="description">Output parameter</param>
    ''' <remarks></remarks>
    Private Sub GetInstrumentAccession(instrumentGroup As String, <Out()> ByRef accession As String, <Out()> ByRef description As String)

        accession = String.Empty
        description = String.Empty

        Select Case instrumentGroup

            Case "Agilent_GC-MS"
                ' This is an Agilent 7890A with a 5975C detector
                ' The closest match is an LC/MS system

                accession = "MS:1000471"
                description = "6140 Quadrupole LC/MS"

            Case "Agilent_TOF_V2"
                accession = "MS:1000472"
                description = "6210 Time-of-Flight LC/MS"

            Case "Bruker_Amazon_Ion_Trap"
                accession = "MS:1001542"
                description = "amaZon ETD"

            Case "Bruker_FTMS"
                accession = "MS:1001549"
                description = "solariX"

            Case "Bruker_QTOF"
                accession = "MS:1001537"
                description = "BioTOF"

            Case "Exactive"
                accession = "MS:1000649"
                description = "Exactive"

            Case "TSQ", "GC-TSQ"
                ' TSQ_3 and TSQ_4 are TSQ Vantage instruments
                accession = "MS:1001510"
                description = "TSQ Vantage"
            Case "LCQ"
                accession = "MS:1000554"
                description = "LCQ Deca"

            Case "LTQ", "LTQ-Prep"
                accession = "MS:1000447"
                description = "LTQ"

            Case "LTQ-ETD"
                accession = "MS:1000638"
                description = "LTQ XL ETD"

            Case "LTQ-FT"
                accession = "MS:1000448"
                description = "LTQ FT"

            Case "Orbitrap"
                accession = "MS:1000449"
                description = "LTQ Orbitrap"

            Case "QExactive"
                accession = "MS:1001911"
                description = "Q Exactive"

            Case "Sciex_QTrap"
                accession = "MS:1000931"
                description = "QTRAP 5500"

            Case "Sciex_TripleTOF"
                accession = "MS:1000932"
                description = "TripleTOF 5600"

            Case "VelosOrbi"
                accession = "MS:1001742"
                description = "LTQ Orbitrap Velos"

            Case "VelosPro"
                ' Note that VPro01 is actually a Velos Pro
                accession = "MS:1000855"
                description = "LTQ Velos"

        End Select

    End Sub

    Private Function GetPrideConverterVersion(strPrideConverterProgLoc As String) As String

        Dim CmdStr As String
        Dim strVersionFilePath As String
        Dim strPRIDEConverterVersion = "unknown"

        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        m_StatusTools.CurrentOperation = "Determining PrideConverter Version"
        m_StatusTools.UpdateAndWrite(m_progress)
        strVersionFilePath = Path.Combine(m_WorkDir, "PRIDEConverter_Version.txt")

        CmdStr = "-jar " & PossiblyQuotePath(strPrideConverterProgLoc)

        CmdStr &= " -converter -version"

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc & " " & CmdStr)
        End If

        With mCmdRunner
            .CreateNoWindow = False
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = strVersionFilePath
            .WorkDir = m_WorkDir
        End With

        Dim blnSuccess As Boolean
        blnSuccess = mCmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", True)

        ' Assure that the console output file has been parsed
        ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            LogError(mConsoleOutputErrorMsg)
        End If

        If Not blnSuccess Then
            LogError("Error running PrideConverter to determine its version")
        Else
            Dim fiVersionFile As FileInfo
            fiVersionFile = New FileInfo(strVersionFilePath)

            If fiVersionFile.Exists Then
                ' Open the version file and read the version
                Using srVersionFile = New StreamReader(New FileStream(fiVersionFile.FullName, FileMode.Open, FileAccess.Read))
                    If Not srVersionFile.EndOfStream Then
                        strPRIDEConverterVersion = srVersionFile.ReadLine()
                    End If
                End Using
            End If

        End If

        Return strPRIDEConverterVersion

    End Function

    Private Function GetValueOrDefault(strType As String, dctParameters As IReadOnlyDictionary(Of String, String), defaultValue As String) As String

        Dim strValueOverride As String = String.Empty

        If dctParameters.TryGetValue(strType, strValueOverride) Then
            Return strValueOverride
        End If

        Return defaultValue

    End Function

    Private Function InitializeOptions() As udtFilterThresholdsType

        ' Update the processing options
        mCreatePrideXMLFiles = m_jobParams.GetJobParameter("CreatePrideXMLFiles", False)

        mCreateMSGFReportFilesOnly = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", False)
        mCreateMGFFiles = m_jobParams.GetJobParameter("CreateMGFFiles", True)

        mIncludePepXMLFiles = m_jobParams.GetJobParameter("IncludePepXMLFiles", True)
        mProcessMzIdFiles = m_jobParams.GetJobParameter("IncludeMzIdFiles", True)

        If mCreateMSGFReportFilesOnly Then
            mCreateMGFFiles = False
            mIncludePepXMLFiles = False
            mProcessMzIdFiles = False
            mCreatePrideXMLFiles = False
        End If

        mCachedOrgDBName = String.Empty

        ' Initialize the protein dictionaries			
        mCachedProteins = New Dictionary(Of String, KeyValuePair(Of Integer, String))
        mCachedProteinPSMCounts = New Dictionary(Of Integer, Integer)

        ' Initialize the PXFile lists
        mPxMasterFileList = New Dictionary(Of String, clsPXFileInfoBase)(StringComparer.CurrentCultureIgnoreCase)
        mPxResultFiles = New Dictionary(Of Integer, clsPXFileInfo)

        ' Initialize the CDTAFileStats dictionary
        mCDTAFileStats = New Dictionary(Of String, clsPXFileInfoBase)(StringComparer.CurrentCultureIgnoreCase)

        ' Clear the previous dataset objects
        mPreviousDatasetName = String.Empty
        mPreviousDatasetFilesToDelete = New List(Of String)
        mPreviousDatasetFilesToCopy = New List(Of String)

        ' Initialize additional items
        mFilterThresholdsUsed = New udtFilterThresholdsType
        mInstrumentGroupsStored = New Dictionary(Of String, List(Of String))
        mSearchToolsUsed = New SortedSet(Of String)
        mExperimentNEWTInfo = New Dictionary(Of Integer, String)

        mModificationsUsed = New Dictionary(Of String, clsSampleMetadata.udtCvParamInfoType)(StringComparer.CurrentCultureIgnoreCase)

        mMzIdSampleInfo = New Dictionary(Of String, clsSampleMetadata)(StringComparer.CurrentCultureIgnoreCase)

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
    Private Function JobFileRenameRequired(intJob As Integer) As Boolean

        Dim dataPkgJob As clsDataPackageJobInfo = Nothing

        If mDataPackagePeptideHitJobs.TryGetValue(intJob, dataPkgJob) Then
            Dim strDataset As String = dataPkgJob.Dataset

            Dim intJobsForDataset As Integer = (From item In mDataPackagePeptideHitJobs Where item.Value.Dataset = strDataset).ToList.Count()

            If intJobsForDataset > 1 Then
                Return True
            Else
                Return False
            End If
        End If

        Return False

    End Function

    Private Function LookupDataPackagePeptideHitJobs() As Boolean
        Dim intJob As Integer

        Dim dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)

        If mDataPackagePeptideHitJobs Is Nothing Then
            mDataPackagePeptideHitJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)
        Else
            mDataPackagePeptideHitJobs.Clear()
        End If

        If Not LoadDataPackageJobInfo(dctDataPackageJobs) Then
            Dim msg = "Error loading data package job info"
            LogError(msg & ": clsAnalysisToolRunnerBase.LoadDataPackageJobInfo() returned false")
            m_message = msg
            Return False
        End If

        Dim lstJobsToUse = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS)

        If lstJobsToUse.Count = 0 Then
            LogWarning("Packed job parameter " & clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS & " is empty; no jobs to process")
        Else
            ' Populate mDataPackagePeptideHitJobs using the jobs in lstJobsToUse and dctDataPackagePeptideHitJobs
            For Each strJob As String In lstJobsToUse
                If Integer.TryParse(strJob, intJob) Then
                    Dim dataPkgJob As clsDataPackageJobInfo = Nothing
                    If dctDataPackageJobs.TryGetValue(intJob, dataPkgJob) Then
                        mDataPackagePeptideHitJobs.Add(intJob, dataPkgJob)
                    End If
                End If
            Next
        End If

        Return True

    End Function

    ''' <summary>
    ''' Parse the PRIDEConverter console output file to determine the PRIDE Version
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

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

            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

            Dim strLineIn As String

            mConsoleOutputErrorMsg = String.Empty

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                mConsoleOutputErrorMsg = String.Empty

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
                Console.WriteLine("Error parsing console output file (" & Path.GetFileName(strConsoleOutputFilePath) & ")")
            End If
        End Try

    End Sub

    Private Function ProcessJob(
      kvJobInfo As KeyValuePair(Of Integer, clsDataPackageJobInfo),
      udtFilterThresholds As udtFilterThresholdsType,
      objAnalysisResults As clsAnalysisResults,
      dctDatasetRawFilePaths As IReadOnlyDictionary(Of String, String),
      dctTemplateParameters As IReadOnlyDictionary(Of String, String),
      assumeInstrumentDataUnpurged As Boolean) As IJobParams.CloseOutType

        Dim blnSuccess As Boolean
        Dim resultFiles = New clsResultFileContainer()

        Dim intJob = kvJobInfo.Value.Job
        Dim strDataset = kvJobInfo.Value.Dataset

        If mPreviousDatasetName <> strDataset Then

            TransferPreviousDatasetFiles(objAnalysisResults)

            ' Retrieve the dataset files for this dataset
            mPreviousDatasetName = strDataset

            If mCreatePrideXMLFiles And Not mCreateMSGFReportFilesOnly Then
                ' Create the .mzXML files if it is missing
                blnSuccess = CreateMzXMLFileIfMissing(strDataset, objAnalysisResults, dctDatasetRawFilePaths)
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
        AddNEWTInfo(kvJobInfo.Value.Experiment_NEWT_ID, kvJobInfo.Value.Experiment_NEWT_Name)

        ' Retrieve the PHRP files, MSGF+ results, and _dta.txt file for this job
        blnSuccess = RetrievePHRPFiles(intJob, strDataset, objAnalysisResults)
        If Not blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        resultFiles.MGFFilePath = String.Empty
        If mCreateMGFFiles Then
            ' Convert the _dta.txt file to .mgf files
            blnSuccess = ConvertCDTAToMGF(kvJobInfo.Value, resultFiles.MGFFilePath)
            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Else
            ' Store the path to the _dta.txt file instead of the path to the .mgf file
            resultFiles.MGFFilePath = Path.Combine(m_WorkDir, strDataset & "_dta.txt")
            If Not assumeInstrumentDataUnpurged AndAlso Not File.Exists(resultFiles.MGFFilePath) Then
                ' .mgf file not found
                resultFiles.MGFFilePath = String.Empty
            End If
        End If

        ' Update the .mzID file(s) for this job
        ' Gzip after updating

        If mProcessMzIdFiles AndAlso kvJobInfo.Value.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then

            m_message = String.Empty

            Dim mzIdFilePaths As List(Of String) = Nothing
            blnSuccess = UpdateMzIdFiles(kvJobInfo.Value, mzIdFilePaths, dctTemplateParameters)

            If Not blnSuccess OrElse mzIdFilePaths Is Nothing OrElse mzIdFilePaths.Count = 0 Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("UpdateMzIdFiles returned false for job " & intJob & ", dataset " & strDataset)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            resultFiles.MzIDFilePaths.Clear()

            For Each mzidFilePath In mzIdFilePaths
                Dim mzidFile = New FileInfo(mzidFilePath)

                ' Note that the original file will be auto-deleted after the .gz file is created
                Dim gzippedMZidFile = GZipFile(mzidFile)

                If gzippedMZidFile Is Nothing Then
                    If String.IsNullOrEmpty(m_message) Then
                        LogError("GZipFile returned false for " & mzidFilePath)
                    End If
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                resultFiles.MzIDFilePaths.Add(gzippedMZidFile.FullName)
            Next

        End If

        If mIncludePepXMLFiles AndAlso kvJobInfo.Value.PeptideHitResultType <> clsPHRPReader.ePeptideHitResultType.Unknown Then
            Dim pepXmlFilename = kvJobInfo.Value.Dataset & ".pepXML"
            Dim pepXMLFile = New FileInfo(Path.Combine(m_WorkDir, pepXmlFilename))
            If pepXMLFile.Exists Then
                ' Make sure it is capitalized correctly, then gzip it

                If Not String.Equals(pepXMLFile.Name, pepXmlFilename, StringComparison.InvariantCulture) Then
                    pepXMLFile.MoveTo(pepXMLFile.FullName & ".tmp")
                    Threading.Thread.Sleep(50)
                    pepXMLFile.MoveTo(Path.Combine(m_WorkDir, pepXmlFilename))
                    Threading.Thread.Sleep(50)
                End If

                ' Note that the original file will be auto-deleted after the .gz file is created
                Dim gzippedMZidFile = GZipFile(pepXMLFile)

                If gzippedMZidFile Is Nothing Then
                    If String.IsNullOrEmpty(m_message) Then
                        LogError("GZipFile returned false for " & pepXMLFile.FullName)
                    End If
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                resultFiles.PepXMLFile = gzippedMZidFile.FullName

            End If
        End If

        ' Store the instrument group and instrument name
        StoreInstrumentInfo(kvJobInfo.Value)

        resultFiles.PrideXmlFilePath = String.Empty
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
                blnSuccess = CreatePrideXMLFile(intJob, strDataset, strPrideReportXMLFilePath, resultFiles.PrideXmlFilePath)
                If Not blnSuccess Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

        End If

        blnSuccess = AppendToPXFileInfo(kvJobInfo.Value, dctDatasetRawFilePaths, resultFiles)

        If blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

    End Function

    Private Function PXFileTypeName(ePXFileType As clsPXFileInfo.ePXFileType) As String
        Select Case ePXFileType
            Case clsPXFileInfoBase.ePXFileType.Result, clsPXFileInfoBase.ePXFileType.ResultMzId
                Return "RESULT"
            Case clsPXFileInfoBase.ePXFileType.Raw
                Return "RAW"
            Case clsPXFileInfoBase.ePXFileType.Search
                Return "SEARCH"
            Case clsPXFileInfoBase.ePXFileType.Peak
                Return "PEAK"
            Case Else
                Return "OTHER"
        End Select
    End Function

    ''' <summary>
    ''' Reads the template PX Submission file
    ''' Caches the keys and values for the method lines (which start with MTD)
    ''' </summary>
    ''' <returns>Dictionary of keys and values</returns>
    ''' <remarks></remarks>
    Private Function ReadTemplatePXSubmissionFile() As Dictionary(Of String, String)

        Const OBSOLETE_FIELD_FLAG = "SKIP_OBSOLETE_FIELD"

        Dim strTemplateFileName As String
        Dim strTemplateFilePath As String
        Dim strLineIn As String

        Dim dctParameters = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

        Dim dctKeyNameOverrides = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
        dctKeyNameOverrides.Add("name", "submitter_name")
        dctKeyNameOverrides.Add("email", "submitter_email")
        dctKeyNameOverrides.Add("affiliation", "submitter_affiliation")
        dctKeyNameOverrides.Add("title", "project_title")
        dctKeyNameOverrides.Add("description", "project_description")
        dctKeyNameOverrides.Add("type", "submission_type")
        dctKeyNameOverrides.Add("comment", OBSOLETE_FIELD_FLAG)
        dctKeyNameOverrides.Add("pride_login", "submitter_pride_login")
        dctKeyNameOverrides.Add("pubmed", "pubmed_id")

        Try
            strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing:=False)
            strTemplateFilePath = Path.Combine(m_WorkDir, strTemplateFileName)

            If Not File.Exists(strTemplateFilePath) Then
                Return dctParameters
            End If

            Using srTemplateFile = New StreamReader(New FileStream(strTemplateFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                While Not srTemplateFile.EndOfStream
                    strLineIn = srTemplateFile.ReadLine

                    If Not String.IsNullOrEmpty(strLineIn) Then
                        If strLineIn.StartsWith("MTD") Then

                            Dim lstColumns As List(Of String)
                            lstColumns = strLineIn.Split(New Char() {ControlChars.Tab}, 3).ToList()

                            If lstColumns.Count >= 3 AndAlso Not String.IsNullOrEmpty(lstColumns(1)) Then
                                Dim keyName = lstColumns(1)

                                ' Automatically rename parameters updated from v1.x to v2.x of the .px file format
                                Dim keyNameNew As String = String.Empty
                                If dctKeyNameOverrides.TryGetValue(keyName, keyNameNew) Then
                                    keyName = keyNameNew
                                End If

                                If Not String.Equals(keyName, OBSOLETE_FIELD_FLAG) AndAlso Not dctParameters.ContainsKey(keyName) Then
                                    dctParameters.Add(keyName, lstColumns(2))
                                End If
                            End If

                        End If
                    End If
                End While
            End Using

        Catch ex As Exception
            LogError("Error in ReadTemplatePXSubmissionFile", ex)
            Return dctParameters
        End Try

        Return dctParameters

    End Function

    Private Function ReadWriteCvParam(
      objXmlReader As XmlTextReader,
      objXmlWriter As XmlTextWriter,
      lstElementCloseDepths As Stack(Of Integer)) As clsSampleMetadata.udtCvParamInfoType

        Dim udtCvParam = New clsSampleMetadata.udtCvParamInfoType
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

    Private Function RetrievePHRPFiles(intJob As Integer, strDataset As String, objAnalysisResults As clsAnalysisResults) As Boolean
        Dim strJobInfoFilePath As String
        Dim lstFilesToCopy = New List(Of String)

        Try

            strJobInfoFilePath = clsDataPackageFileHandler.GetJobInfoFilePath(intJob, m_WorkDir)

            If Not File.Exists(strJobInfoFilePath) Then
                ' Assume all of the files already exist
                Return True
            End If

            ' Read the contents of the JobInfo file
            ' It will be empty if no PHRP files are required
            Using srInFile = New StreamReader(New FileStream(strJobInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                Do While Not srInFile.EndOfStream
                    lstFilesToCopy.Add(srInFile.ReadLine)
                Loop
            End Using

            ' Retrieve the files
            ' If the same dataset has multiple jobs then we might overwrite existing files; 
            '   that's OK since results files that we care about will have been auto-renamed based on the call to JobFileRenameRequired

            For Each sourceFilePath As String In lstFilesToCopy

                If sourceFilePath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG) Then

                    ' Make sure the myEMSLUtilities object knows about this dataset
                    m_MyEMSLUtilities.AddDataset(strDataset)
                    Dim cleanFilePath As String = Nothing
                    DatasetInfoBase.ExtractMyEMSLFileID(sourceFilePath, cleanFilePath)

                    Dim fiSourceFile = New FileInfo(cleanFilePath)
                    Dim unzipRequired = (fiSourceFile.Extension.ToLower() = ".zip" OrElse fiSourceFile.Extension.ToLower() = ".gz")

                    m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath, unzipRequired)

                    Continue For
                End If

                Dim sourceFileName As String = Path.GetFileName(sourceFilePath)

                Dim targetFilePath As String
                targetFilePath = Path.Combine(m_WorkDir, sourceFileName)

                ' Retrieve the file, allowing for up to 3 attempts (uses CopyFileUsingLocks)
                objAnalysisResults.CopyFileWithRetry(sourceFilePath, targetFilePath, True)

                Dim fiLocalFile = New FileInfo(targetFilePath)
                If Not fiLocalFile.Exists Then
                    LogError("PHRP file was not copied locally: " & fiLocalFile.Name)
                    Return False
                End If

                Dim blnUnzipped = False

                If fiLocalFile.Extension.ToLower() = ".zip" Then
                    ' Decompress the .zip file
                    m_IonicZipTools.UnzipFile(fiLocalFile.FullName, m_WorkDir)
                    blnUnzipped = True
                ElseIf fiLocalFile.Extension.ToLower() = ".gz" Then
                    ' Decompress the .gz file
                    m_IonicZipTools.GUnzipFile(fiLocalFile.FullName, m_WorkDir)
                    blnUnzipped = True
                End If

                If blnUnzipped Then
                    For Each kvUnzippedFile In m_IonicZipTools.MostRecentUnzippedFiles
                        AddToListIfNew(mPreviousDatasetFilesToDelete, kvUnzippedFile.Value)
                    Next
                End If

                AddToListIfNew(mPreviousDatasetFilesToDelete, fiLocalFile.FullName)

            Next

            If m_MyEMSLUtilities.FilesToDownload.Count > 0 Then
                If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkDir, Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                    If String.IsNullOrWhiteSpace(m_message) Then
                        m_message = "ProcessMyEMSLDownloadQueue return false"
                    End If
                    Return False
                End If

                If m_MyEMSLUtilities.FilesToDownload.Count > 0 Then
                    ' The queue should have already been cleared; checking just in case
                    m_MyEMSLUtilities.ClearDownloadQueue()
                End If

            End If

        Catch ex As Exception
            LogError("Error in RetrievePHRPFiles", ex)
            Return False
        End Try

        Return True

    End Function

    Private Function RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath As String, objAnalysisResults As clsAnalysisResults, <Out()> ByRef strDestPath As String) As Boolean
        Return RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath, objAnalysisResults, IsFolder:=False, strDestPath:=strDestPath)
    End Function

    Private Function RetrieveStoragePathInfoTargetFile(
       strStoragePathInfoFilePath As String,
       objAnalysisResults As clsAnalysisResults,
       IsFolder As Boolean,
       <Out()> ByRef strDestPath As String) As Boolean

        Dim strSourceFilePath As String = String.Empty

        strDestPath = String.Empty

        Try

            If Not File.Exists(strStoragePathInfoFilePath) Then
                Dim msg = "StoragePathInfo file not found"
                LogError(msg & ": " & strStoragePathInfoFilePath)
                m_message = msg
                Return False
            End If

            Using srInfoFile = New StreamReader(New FileStream(strStoragePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                If Not srInfoFile.EndOfStream Then
                    strSourceFilePath = srInfoFile.ReadLine()
                End If
            End Using

            If String.IsNullOrEmpty(strSourceFilePath) Then
                Dim msg = "StoragePathInfo file was empty"
                LogError(msg & ": " & strStoragePathInfoFilePath)
                m_message = msg
                Return False
            End If

            strDestPath = Path.Combine(m_WorkDir, Path.GetFileName(strSourceFilePath))

            If IsFolder Then
                objAnalysisResults.CopyDirectory(strSourceFilePath, strDestPath, Overwrite:=True)
            Else
                objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strDestPath, Overwrite:=True)
            End If

        Catch ex As Exception
            LogError("Error in RetrieveStoragePathInfoTargetFile", ex)
            Return False
        End Try

        Return True

    End Function

    Private Function RunPrideConverter(intJob As Integer, strDataset As String, strMsgfResultsFilePath As String, strMzXMLFilePath As String, strPrideReportFilePath As String) As Boolean

        Dim CmdStr As String

        If String.IsNullOrEmpty(strMsgfResultsFilePath) Then
            LogError("strMsgfResultsFilePath has not been defined; unable to continue")
            Return False
        End If

        If String.IsNullOrEmpty(strMzXMLFilePath) Then
            LogError("strMzXMLFilePath has not been defined; unable to continue")
            Return False
        End If

        If String.IsNullOrEmpty(strPrideReportFilePath) Then
            LogError("strPrideReportFilePath has not been defined; unable to continue")
            Return False
        End If

        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PrideConverter on " & Path.GetFileName(strMsgfResultsFilePath))
        End If

        m_StatusTools.CurrentOperation = "Running PrideConverter"
        m_StatusTools.UpdateAndWrite(m_progress)

        CmdStr = "-jar " & PossiblyQuotePath(mPrideConverterProgLoc)

        CmdStr &= " -converter -mode convert -engine msgf -sourcefile " & PossiblyQuotePath(strMsgfResultsFilePath)     ' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf
        CmdStr &= " -spectrafile " & PossiblyQuotePath(strMzXMLFilePath)                                                ' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.mzXML
        CmdStr &= " -reportfile " & PossiblyQuotePath(strPrideReportFilePath)                                           ' QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf-report.xml
        CmdStr &= " -reportOnlyIdentifiedSpectra"
        CmdStr &= " -debug"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc & " " & CmdStr)

        With mCmdRunner
            .CreateNoWindow = False
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT)
            .WorkDir = m_WorkDir
        End With

        Dim blnSuccess As Boolean
        blnSuccess = mCmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", True)

        ' Assure that the console output file has been parsed
        ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            If mConsoleOutputErrorMsg.Contains("/Report/PTMs/PTM") Then
                ' Ignore this error
                mConsoleOutputErrorMsg = String.Empty
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
                Console.WriteLine(mConsoleOutputErrorMsg)
            End If
        End If

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running PrideConverter, dataset " & strDataset & ", job " & intJob)
            If String.IsNullOrWhiteSpace(m_message) Then
                m_message = "Error running PrideConverter"
                Console.WriteLine(m_message)
            End If
        End If

        Return blnSuccess

    End Function

    Private Sub StoreInstrumentInfo(datasetInfo As clsDataPackageDatasetInfo)
        StoreInstrumentInfo(datasetInfo.InstrumentGroup, datasetInfo.Instrument)
    End Sub

    Private Sub StoreInstrumentInfo(dataPkgJob As clsDataPackageJobInfo)
        StoreInstrumentInfo(dataPkgJob.InstrumentGroup, dataPkgJob.Instrument)
    End Sub

    Private Sub StoreInstrumentInfo(instrumentGroup As String, instrumentName As String)

        Dim lstInstruments As List(Of String) = Nothing
        If mInstrumentGroupsStored.TryGetValue(instrumentGroup, lstInstruments) Then
            If Not lstInstruments.Contains(instrumentName) Then
                lstInstruments.Add(instrumentName)
            End If
        Else
            lstInstruments = New List(Of String) From {instrumentName}
            mInstrumentGroupsStored.Add(instrumentGroup, lstInstruments)
        End If

    End Sub

    Private Sub StoreMzIdSampleInfo(strMzIdFilePath As String, sampleMetadata As clsSampleMetadata)
        Dim fiFile = New FileInfo(strMzIdFilePath)

        If Not mMzIdSampleInfo.ContainsKey(fiFile.Name) Then
            mMzIdSampleInfo.Add(fiFile.Name, sampleMetadata)
        End If

    End Sub

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <param name="strPrideConverterProgLoc"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo(strPrideConverterProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim fiPrideConverter As FileInfo

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)

        If mCreatePrideXMLFiles Then
            fiPrideConverter = New FileInfo(strPrideConverterProgLoc)
            If Not fiPrideConverter.Exists Then
                Try
                    strToolVersionInfo = "Unknown"
                    Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
                Catch ex As Exception
                    Dim msg = "Exception calling SetStepTaskToolVersion: " & ex.Message
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
                    Console.WriteLine(msg)
                    Return False
                End Try

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

        ioToolFiles.Add(New FileInfo(mMSXmlGeneratorAppPath))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            LogError("Exception calling SetStepTaskToolVersion", ex)
            Return False
        End Try

    End Function

    Private Sub TransferPreviousDatasetFiles(objAnalysisResults As clsAnalysisResults)

        ' Delete the dataset files for the previous dataset
        Dim lstFilesToRetry = New List(Of String)

        If mPreviousDatasetFilesToCopy.Count > 0 Then
            lstFilesToRetry.Clear()

            Dim strRemoteTransferFolder = CreateRemoteTransferFolder(objAnalysisResults, mCacheFolderPath)

            If String.IsNullOrEmpty(strRemoteTransferFolder) Then
                LogError("CreateRemoteTransferFolder returned an empty string; unable to copy files to the transfer folder")
                lstFilesToRetry.AddRange(mPreviousDatasetFilesToCopy)
            Else

                Try
                    ' Create the remote Transfer Directory
                    If Not Directory.Exists(strRemoteTransferFolder) Then
                        Directory.CreateDirectory(strRemoteTransferFolder)
                    End If

                    ' Copy the files we want to keep to the remote Transfer Directory
                    For Each strSrcFilePath In mPreviousDatasetFilesToCopy
                        Dim strTargetFilePath As String = Path.Combine(strRemoteTransferFolder, Path.GetFileName(strSrcFilePath))

                        If File.Exists(strSrcFilePath) Then

                            Try
                                objAnalysisResults.CopyFileWithRetry(strSrcFilePath, strTargetFilePath, True)
                                AddToListIfNew(mPreviousDatasetFilesToDelete, strSrcFilePath)
                            Catch ex As Exception
                                LogError("Exception copying file to transfer directory", ex)
                                lstFilesToRetry.Add(strSrcFilePath)
                            End Try

                        End If
                    Next

                Catch ex As Exception
                    ' Folder creation error
                    LogError("Exception creating transfer directory folder", ex)
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
                    If File.Exists(item) Then
                        File.Delete(item)
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

    Private Function UpdateMSGFReportXMLFileLocation(eFileLocation As eMSGFReportXMLFileLocation, strElementName As String, blnInsideMzDataDescription As Boolean) As eMSGFReportXMLFileLocation

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
    ''' Update the .mzid file for the given job and dataset to have the correct Accession value for FileFormat
    ''' Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
    ''' </summary>
    ''' <param name="dataPkgJob">Data package job info</param>
    ''' <param name="mzIdFilePaths">Output parameter: path to the .mzid file for this job (will be multiple files if a SplitFasta search was performed)</param>
    ''' <param name="dctTemplateParameters"></param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Private Function UpdateMzIdFiles(
      dataPkgJob As clsDataPackageJobInfo,
      <Out()> ByRef mzIdFilePaths As List(Of String),
      dctTemplateParameters As IReadOnlyDictionary(Of String, String)) As Boolean

        Dim sampleMetadata = New clsSampleMetadata()
        sampleMetadata.Clear()

        sampleMetadata.Species = GetNEWTCv(dataPkgJob.Experiment_NEWT_ID, dataPkgJob.Experiment_NEWT_Name)
        sampleMetadata.Tissue = GetValueOrDefault("tissue", dctTemplateParameters, DEFAULT_TISSUE_CV)

        Dim value As String = String.Empty

        If (dctTemplateParameters.TryGetValue("cell_type", value)) Then
            sampleMetadata.CellType = value
        Else
            sampleMetadata.CellType = String.Empty
        End If

        If (dctTemplateParameters.TryGetValue("disease", value)) Then
            sampleMetadata.Disease = value
        Else
            sampleMetadata.Disease = String.Empty
        End If

        sampleMetadata.Modifications.Clear()
        sampleMetadata.InstrumentGroup = dataPkgJob.InstrumentGroup
        sampleMetadata.Quantification = String.Empty
        sampleMetadata.ExperimentalFactor = dataPkgJob.Experiment

        mzIdFilePaths = New List(Of String)

        Try
            ' Open each .mzid and parse it to create a new .mzid file
            ' Use a forward-only XML reader, copying most of the elements verbatim, but customizing some of them

            ' For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>
            ' For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
            ' Will also need to update the location and name attributes of the SpectraData element
            ' <SpectraData location="E:\DMS_WorkDir3\QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" name="QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" id="SID_1">

            ' For split FASTA files each job step should have a custom .FASTA file, but we're ignoring that fact for now

            Dim success As Boolean
            Dim strMzIDFilePath As String = Nothing

            If dataPkgJob.NumberOfClonedSteps > 0 Then
                For splitFastaResultID = 1 To dataPkgJob.NumberOfClonedSteps
                    success = UpdateMzIdFile(dataPkgJob.Job, dataPkgJob.Dataset, splitFastaResultID, sampleMetadata, strMzIDFilePath)
                    If success Then
                        mzIdFilePaths.Add(strMzIDFilePath)
                    Else
                        Exit For
                    End If

                Next
            Else
                success = UpdateMzIdFile(dataPkgJob.Job, dataPkgJob.Dataset, 0, sampleMetadata, strMzIDFilePath)
                If success Then
                    mzIdFilePaths.Add(strMzIDFilePath)
                End If
            End If

            If Not success Then
                If String.IsNullOrWhiteSpace(m_message) Then
                    LogError("UpdateMzIdFile returned false (unknown error)")
                End If
                Return False
            End If

            Return True
        Catch ex As Exception
            LogError("Exception in UpdateMzIdFiles for job " & dataPkgJob.Job & ", dataset " & dataPkgJob.Dataset, ex)
            mzIdFilePaths = New List(Of String)

            Return False
        End Try

    End Function

    ''' <summary>
    ''' Update a single .mzid file to have the correct Accession value for FileFormat
    ''' Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
    ''' </summary>
    ''' <param name="dataPkgJob">Data package job</param>
    ''' <param name="dataPkgDataset">Data package dataset</param>
    ''' <param name="splitFastaResultID">For SplitFasta jobs, the part number being processed; 0 for non-SplitFasta jobs</param>
    ''' <param name="sampleMetadata">Sample Metadata</param>
    ''' <param name="strMzIDFilePath">Output parameter: path to the .mzid file being processed</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Private Function UpdateMzIdFile(
      dataPkgJob As Integer,
      dataPkgDataset As String,
      splitFastaResultID As Integer,
      sampleMetadata As clsSampleMetadata,
      <Out()> ByRef strMzIDFilePath As String) As Boolean

        Dim nodeWritten As Boolean
        Dim skipNode As Boolean
        Dim readModAccession = False
        Dim readingSpecificityRules = False

        Dim lstAttributeOverride = New Dictionary(Of String, String)

        Dim lstElementCloseDepths = New Stack(Of Integer)

        Dim eFileLocation = eMzIDXMLFileLocation.Header
        Dim lstRecentElements = New Queue(Of String)

        Try

            Dim strSourceFileName As String
            Dim strUpdatedFilePathTemp As String
            Dim filePartText = String.Empty

            If splitFastaResultID > 0 Then
                filePartText = "_Part" & splitFastaResultID
            End If

            ' First look for a job-specific version of the .mzid file
            strSourceFileName = "Job" & dataPkgJob.ToString() & "_" & dataPkgDataset & "_msgfplus" & filePartText & ".mzid"
            strMzIDFilePath = Path.Combine(m_WorkDir, strSourceFileName)

            If Not File.Exists(strMzIDFilePath) Then
                ' Job-specific version not found
                ' Look for one that simply starts with the dataset name
                strSourceFileName = dataPkgDataset & "_msgfplus" & filePartText & ".mzid"
                strMzIDFilePath = Path.Combine(m_WorkDir, strSourceFileName)

                If Not File.Exists(strMzIDFilePath) Then
                    LogError("MzID file not found for job " & dataPkgJob & ": " & strSourceFileName)
                    Return False
                End If
            End If

            AddToListIfNew(mPreviousDatasetFilesToDelete, strMzIDFilePath)
            AddToListIfNew(mPreviousDatasetFilesToDelete, strMzIDFilePath & ".gz")

            strUpdatedFilePathTemp = strMzIDFilePath & ".tmp"

            ' Important: instantiate the XmlTextWriter using an instance of the UTF8Encoding class where the byte order mark (BOM) is not emitted
            ' The ProteomeXchange import pipeline breaks if the .mzid files have the BOM at the start of the file

            Using objXmlWriter = New XmlTextWriter(New FileStream(strUpdatedFilePathTemp, FileMode.Create, FileAccess.Write, FileShare.Read), New Text.UTF8Encoding(False))
                objXmlWriter.Formatting = Formatting.Indented
                objXmlWriter.Indentation = 4

                objXmlWriter.WriteStartDocument()

                ' Note that the following Using command will not work if the .mzid file has an encoding string of <?xml version="1.0" encoding="Cp1252"?>
                ' Using objXmlReader As XmlTextReader = New XmlTextReader(New FileStream(strMzIDFilePath, FileMode.Open, FileAccess.Read))
                ' Thus, we instead first insantiate a streamreader using explicit encodings
                ' Then instantiate the XmlTextReader

                Using srSourceFile = New StreamReader(New FileStream(strMzIDFilePath, FileMode.Open, FileAccess.Read, FileShare.Read), Text.Encoding.GetEncoding("ISO-8859-1"))

                    Using objXmlReader = New XmlTextReader(srSourceFile)

                        Do While objXmlReader.Read()

                            Select Case objXmlReader.NodeType
                                Case XmlNodeType.Whitespace
                                    ' Skip whitespace since the writer should be auto-formatting things
                                    ' objXmlWriter.WriteWhitespace(objXmlReader.Value)

                                Case XmlNodeType.Comment
                                    objXmlWriter.WriteComment(objXmlReader.Value)

                                Case XmlNodeType.Element
                                    ' Start element

                                    If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
                                    lstRecentElements.Enqueue("Element " & objXmlReader.Name)

                                    Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
                                        lstElementCloseDepths.Pop()

                                        objXmlWriter.WriteEndElement()
                                    Loop

                                    eFileLocation = UpdateMZidXMLFileLocation(eFileLocation, objXmlReader.Name)

                                    nodeWritten = False
                                    skipNode = False

                                    lstAttributeOverride.Clear()

                                    Select Case objXmlReader.Name

                                        Case "SpectraData"
                                            ' Override the location and name attributes for this node

                                            Dim strSpectraDataFilename As String

                                            If mCreateMGFFiles Then
                                                strSpectraDataFilename = dataPkgDataset & ".mgf"
                                            Else
                                                strSpectraDataFilename = dataPkgDataset & "_dta.txt"
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

                                                If mCreateMGFFiles Then
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

                                                objXmlWriter.WriteEndElement()  ' cvParam
                                                objXmlWriter.WriteEndElement()  ' FileFormat

                                                skipNode = True
                                            End If

                                        Case "SearchModification"
                                            If eFileLocation = eMzIDXMLFileLocation.AnalysisProtocolCollection Then
                                                ' The next cvParam entry that we read should have the Unimod accession
                                                readModAccession = True
                                            End If

                                        Case "SpecificityRules"
                                            If readModAccession Then
                                                readingSpecificityRules = True
                                            End If

                                        Case "cvParam"
                                            If readModAccession And Not readingSpecificityRules Then
                                                Dim udtModInfo As clsSampleMetadata.udtCvParamInfoType
                                                udtModInfo = ReadWriteCvParam(objXmlReader, objXmlWriter, lstElementCloseDepths)

                                                If Not String.IsNullOrEmpty(udtModInfo.Accession) Then
                                                    If Not mModificationsUsed.ContainsKey(udtModInfo.Accession) Then
                                                        mModificationsUsed.Add(udtModInfo.Accession, udtModInfo)
                                                    End If

                                                    If Not sampleMetadata.Modifications.ContainsKey(udtModInfo.Accession) Then
                                                        sampleMetadata.Modifications.Add(udtModInfo.Accession, udtModInfo)
                                                    End If
                                                End If

                                                nodeWritten = True
                                                readModAccession = False
                                            End If
                                    End Select


                                    If skipNode Then
                                        If objXmlReader.NodeType <> XmlNodeType.EndElement Then
                                            ' Skip this element (and any children nodes enclosed in this elemnt)
                                            ' Likely should not do this when objXmlReader.NodeType is XmlNodeType.EndElement
                                            objXmlReader.Skip()
                                        End If

                                    ElseIf Not nodeWritten Then
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

                                Case XmlNodeType.EndElement

                                    If lstRecentElements.Count > 10 Then lstRecentElements.Dequeue()
                                    lstRecentElements.Enqueue("EndElement " & objXmlReader.Name)

                                    Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth + 1
                                        lstElementCloseDepths.Pop()
                                        objXmlWriter.WriteEndElement()
                                    Loop

                                    objXmlWriter.WriteEndElement()

                                    Do While lstElementCloseDepths.Count > 0 AndAlso lstElementCloseDepths.Peek > objXmlReader.Depth
                                        lstElementCloseDepths.Pop()
                                    Loop

                                    If objXmlReader.Name = "SearchModification" Then
                                        readModAccession = False
                                    End If

                                    If objXmlReader.Name = "SpecificityRules" Then
                                        readingSpecificityRules = False
                                    End If

                                Case XmlNodeType.Text

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

            ' Must append .gz to the .mzid file name to allow for successful lookups in function CreatePXSubmissionFile
            StoreMzIdSampleInfo(strMzIDFilePath & ".gz", sampleMetadata)

            Threading.Thread.Sleep(250)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            Try
                ' Replace the original .mzid file with the updated one
                File.Delete(strMzIDFilePath)

                If JobFileRenameRequired(dataPkgJob) Then
                    strMzIDFilePath = Path.Combine(m_WorkDir, dataPkgDataset & "_Job" & dataPkgJob.ToString() & "_msgfplus.mzid")
                Else
                    strMzIDFilePath = Path.Combine(m_WorkDir, dataPkgDataset & "_msgfplus.mzid")
                End If

                File.Move(strUpdatedFilePathTemp, strMzIDFilePath)

            Catch ex As Exception
                LogError("Exception replacing the original .mzID file with the updated one for job " & dataPkgJob & ", dataset " & dataPkgDataset, ex)
                Return False
            End Try

            Return True

        Catch ex As Exception
            LogError("Exception in UpdateMzIdFile for job " & dataPkgJob & ", dataset " & dataPkgDataset, ex)

            Dim strRecentElements As String = String.Empty
            For Each strItem In lstRecentElements
                If String.IsNullOrEmpty(strRecentElements) Then
                    strRecentElements = String.Copy(strItem)
                Else
                    strRecentElements &= "; " & strItem
                End If
            Next

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strRecentElements)

            strMzIDFilePath = String.Empty
            Return False
        End Try

    End Function

    Private Function UpdateMZidXMLFileLocation(eFileLocation As eMzIDXMLFileLocation, strElementName As String) As eMzIDXMLFileLocation

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

    Private Sub WriteConfigurationOption(objXmlWriter As XmlTextWriter, KeyName As String, Value As String)

        objXmlWriter.WriteStartElement("Option")
        objXmlWriter.WriteElementString("Key", KeyName)
        objXmlWriter.WriteElementString("Value", Value)
        objXmlWriter.WriteEndElement()

    End Sub

    ''' <summary>
    ''' Append a new header line to the .px file
    ''' </summary>
    ''' <param name="swPXFile"></param>
    ''' <param name="strType"></param>
    ''' <param name="strValue"></param>
    ''' <remarks></remarks>
    Private Sub WritePXHeader(swPXFile As StreamWriter, strType As String, strValue As String)
        WritePXHeader(swPXFile, strType, strValue, New Dictionary(Of String, String))
    End Sub

    ''' <summary>
    ''' Append a new header line to the .px file
    ''' </summary>
    ''' <param name="swPXFile"></param>
    ''' <param name="strType"></param>
    ''' <param name="strValue"></param>
    ''' <param name="dctParameters"></param>
    ''' <remarks></remarks>
    Private Sub WritePXHeader(swPXFile As StreamWriter, strType As String, strValue As String, dctParameters As IReadOnlyDictionary(Of String, String))
        WritePXHeader(swPXFile, strType, strValue, dctParameters, intMinimumValueLength:=0)
    End Sub

    ''' <summary>
    ''' Append a new header line to the .px file
    ''' </summary>
    ''' <param name="swPXFile"></param>
    ''' <param name="strType"></param>
    ''' <param name="strValue"></param>
    ''' <param name="dctParameters"></param>
    ''' <param name="intMinimumValueLength"></param>
    ''' <remarks></remarks>
    Private Sub WritePXHeader(
      swPXFile As StreamWriter,
      strType As String,
      strValue As String,
      dctParameters As IReadOnlyDictionary(Of String, String),
      intMinimumValueLength As Integer)

        Dim strValueOverride As String = String.Empty

        If dctParameters.TryGetValue(strType, strValueOverride) Then
            strValue = strValueOverride
        End If

        If intMinimumValueLength > 0 Then
            If String.IsNullOrEmpty(strValue) Then
                strValue = "**** Value must be at least " & intMinimumValueLength & " characters long **** "
            End If

            Do While strValue.Length < intMinimumValueLength
                strValue &= "__"
            Loop
        End If

        WritePXLine(swPXFile, New List(Of String) From {"MTD", strType, strValue})

    End Sub

    Private Sub WritePXInstruments(swPXFile As StreamWriter)

        For Each kvInstrumentGroup In mInstrumentGroupsStored

            Dim accession = String.Empty
            Dim description = String.Empty

            GetInstrumentAccession(kvInstrumentGroup.Key, accession, description)

            If kvInstrumentGroup.Value.Contains("TSQ_2") AndAlso kvInstrumentGroup.Value.Count = 1 Then
                ' TSQ_1 is a TSQ Quantum Ultra
                accession = "MS:1000751"
                description = "TSQ Quantum Ultra"
            End If

            Dim strInstrumentCV = GetInstrumentCv(accession, description)
            WritePXHeader(swPXFile, "instrument", strInstrumentCV)
        Next

    End Sub

    Private Sub WritePXLine(swPXFile As TextWriter, lstItems As IReadOnlyCollection(Of String))
        If lstItems.Count > 0 Then
            swPXFile.WriteLine(String.Join(ControlChars.Tab, lstItems))
        End If
    End Sub

    Private Sub WritePXMods(swPXFile As StreamWriter)

        If mModificationsUsed.Count = 0 Then
            WritePXHeader(swPXFile, "modification", GetCVString("PRIDE", "PRIDE:0000398", "No PTMs are included in the dataset", ""))
        Else
            ' Write out each modification, for example, for Unimod:
            '   modification	[UNIMOD,UNIMOD:35,Oxidation,]
            ' Or for PSI-mod
            '   modification	[MOD,MOD:00394,acetylated residue,]

            For Each item In mModificationsUsed
                WritePXHeader(swPXFile, "modification", GetCVString(item.Value))
            Next
        End If

    End Sub

    Private Sub WriteUserParam(objXmlWriter As XmlTextWriter, Name As String, Value As String)

        objXmlWriter.WriteStartElement("userParam")
        objXmlWriter.WriteAttributeString("name", Name)
        objXmlWriter.WriteAttributeString("value", Value)
        objXmlWriter.WriteEndElement()

    End Sub

    Private Sub WriteCVParam(objXmlWriter As XmlTextWriter, CVLabel As String, Accession As String, Name As String, Value As String)

        objXmlWriter.WriteStartElement("cvParam")
        objXmlWriter.WriteAttributeString("cvLabel", CVLabel)
        objXmlWriter.WriteAttributeString("accession", Accession)
        objXmlWriter.WriteAttributeString("name", Name)
        objXmlWriter.WriteAttributeString("value", Value)
        objXmlWriter.WriteEndElement()

    End Sub

    Private Function WriteXMLInstrumentInfo(oWriter As XmlTextWriter, strInstrumentGroup As String) As Boolean

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

                oWriter.WriteEndElement()   ' analyzerList

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

                oWriter.WriteEndElement()   ' analyzerList

                WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector")

            Case "Exactive"
                blnInstrumentDetailsAutoDefined = True

                WriteXMLInstrumentInfoESI(oWriter, "positive")

                oWriter.WriteStartElement("analyzerList")
                oWriter.WriteAttributeString("count", "1")

                WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap")

                oWriter.WriteEndElement()   ' analyzerList

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

            oWriter.WriteEndElement()   ' analyzerList

            WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000347", "dynode")
        End If

        Return blnInstrumentDetailsAutoDefined

    End Function

    Private Sub WriteXMLInstrumentInfoAnalyzer(oWriter As XmlTextWriter, strNamespace As String, strAccession As String, strDescription As String)

        oWriter.WriteStartElement("analyzer")
        WriteCVParam(oWriter, strNamespace, strAccession, strDescription, String.Empty)
        oWriter.WriteEndElement()

    End Sub

    Private Sub WriteXMLInstrumentInfoDetector(oWriter As XmlTextWriter, strNamespace As String, strAccession As String, strDescription As String)

        oWriter.WriteStartElement("detector")
        WriteCVParam(oWriter, strNamespace, strAccession, strDescription, String.Empty)
        oWriter.WriteEndElement()

    End Sub


    Private Sub WriteXMLInstrumentInfoESI(oWriter As XmlTextWriter, strPolarity As String)

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
    Private Sub CmdRunner_LoopWaiting()
        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT))

            LogProgress("PRIDEConverter")
        End If

    End Sub

    Private Sub mDTAtoMGF_ErrorEvent(strMessage As String) Handles mDTAtoMGF.ErrorEvent
        LogError("Error from DTAtoMGF converter: " & mDTAtoMGF.GetErrorMessage())
    End Sub

    Private Sub mMSXmlCreator_DebugEvent(strMessage As String) Handles mMSXmlCreator.DebugEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
    End Sub

    Private Sub mMSXmlCreator_ErrorEvent(strMessage As String) Handles mMSXmlCreator.ErrorEvent
        LogError("Error from MSXmlCreator: " & strMessage)
    End Sub

    Private Sub mMSXmlCreator_WarningEvent(strMessage As String) Handles mMSXmlCreator.WarningEvent
        LogWarning(strMessage)
    End Sub

    Private Sub mMSXmlCreator_LoopWaiting() Handles mMSXmlCreator.LoopWaiting

        UpdateStatusFile()

        LogProgress("MSXmlCreator (PRIDEConverter)")

    End Sub

    Private Sub m_MyEMSLDatasetListInfo_FileDownloadedEvent(sender As Object, e As FileDownloadedEventArgs)

        If e.UnzipRequired Then
            For Each kvUnzippedFile In m_MyEMSLUtilities.MostRecentUnzippedFiles
                AddToListIfNew(mPreviousDatasetFilesToDelete, kvUnzippedFile.Value)
            Next
        End If

        AddToListIfNew(mPreviousDatasetFilesToDelete, Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename))

    End Sub
#End Region

End Class
