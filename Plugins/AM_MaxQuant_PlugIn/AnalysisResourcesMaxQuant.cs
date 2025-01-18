using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMaxQuantPlugIn
{
    /// <summary>
    /// Retrieve resources for the MaxQuant plugin
    /// </summary>
    public class AnalysisResourcesMaxQuant : AnalysisResources
    {
        // Ignore Spelling: conf, Maxq, MaxQuant, Parm, plex, Quant, resourcer

        private const string MAXQUANT_PEAK_STEP_TOOL = "MaxqPeak";

        internal const string JOB_PARAM_PROTEIN_DESCRIPTION_PARSE_RULE = "ProteinDescriptionParseRule";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                var workingDirectory = new DirectoryInfo(mWorkDir);

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var retrieveMsXmlFiles = mJobParams.GetJobParameter("CreateMzMLFiles", false);

                // Determine the transfer directory path

                // Caveats for retrieveMsXmlFiles:
                //   When true, we want useInputDirectory to be true so that the resourcer will look for .mzML (or .mzXML) files in a MSXML_Gen directory
                //   When retrieveMsXmlFiles is false, we want useInputDirectory to be false so that the resourcer will look for .Raw files in the dataset directory

                // Caveats for dataPackageID
                //   When 0, we are processing a single dataset, and we thus need to include the dataset name, generating a path like \\proto-4\DMS3_Xfer\QC_Dataset\MXQ202103151122_Auto1880613
                //   When positive, we are processing datasets in a data package, and we thus want a path without the dataset name, generating a path like \\proto-9\MaxQuant_Staging\MXQ202103161252_Auto1880833

                var useInputDirectory = retrieveMsXmlFiles;
                var includeDatasetName = dataPackageID <= 0;

                var transferDirectoryPath = GetTransferDirectoryPathForJobStep(useInputDirectory, includeDatasetName);

                LogMessage("Retrieving the MaxQuant param file", 2);

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag, MaxQuant_Mod_Name, UniMod_Mod_Name
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                if (!RetrieveGeneratedParamFile(paramFileName))
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var validModifications = ValidateMaxQuantModificationNames(mWorkDir, paramFileName);

                if (!validModifications)
                    return CloseOutType.CLOSEOUT_FAILED;

                var success = GetExistingToolParametersFile(workingDirectory, transferDirectoryPath, paramFileName, out var previousJobStepParameterFilePath);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                string proteinDescriptionParseRule;

                if (string.IsNullOrWhiteSpace(previousJobStepParameterFilePath))
                {
                    proteinDescriptionParseRule = string.Empty;
                }
                else
                {
                    var skipStepToolPrevJobStep = CheckSkipMaxQuant(
                        workingDirectory,
                        Path.GetFileName(previousJobStepParameterFilePath),
                        true,
                        out var abortProcessingPrevJobStep,
                        out var skipReason,
                        out var dmsSteps,
                        out proteinDescriptionParseRule);

                    if (abortProcessingPrevJobStep)
                    {
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (skipStepToolPrevJobStep)
                    {
                        // This message should have already been logged
                        EvalMessage = skipReason;
                        return CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT;
                    }

                    var targetParameterFile = new FileInfo(Path.Combine(workingDirectory.FullName, paramFileName));

                    var stepIdUpdateResult = AnalysisToolRunnerMaxQuant.UpdateMaxQuantParameterFileStartStepIDs(
                        DatasetName, targetParameterFile, dmsSteps, out var errorMessage);

                    if (stepIdUpdateResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        LogError("Error updating the DMSSteps in {0}: {1}", targetParameterFile.Name, errorMessage);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Also examine the original parameter file, in case it has numeric values defined for startStepID
                var skipStepTool = CheckSkipMaxQuant(
                    workingDirectory, paramFileName, false,
                    out var abortProcessing,
                    out var skipReason2,
                    out _, out _);

                if (abortProcessing)
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                if (skipStepTool)
                {
                    // This message should have already been logged
                    EvalMessage = skipReason2;
                    return CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT;
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                if (StepToolName.Equals(MAXQUANT_PEAK_STEP_TOOL, StringComparison.OrdinalIgnoreCase))
                {
                    var parseRuleResult = DetermineProteinDescriptionParseRule(out proteinDescriptionParseRule);

                    if (parseRuleResult != CloseOutType.CLOSEOUT_SUCCESS)
                        return parseRuleResult;
                }

                if (!string.IsNullOrWhiteSpace(proteinDescriptionParseRule))
                {
                    // Store proteinDescriptionParseRule as a job parameter so that the tool runner can use it
                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_PROTEIN_DESCRIPTION_PARSE_RULE,
                        proteinDescriptionParseRule);
                }

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMsXmlFiles,
                    AnalysisToolRunnerMaxQuant.PROGRESS_PCT_TOOL_RUNNER_STARTING,
                    false,
                    out var dataPackageInfo,
                    out var dataPackageDatasets);

                if (datasetCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (!string.IsNullOrWhiteSpace(datasetFileRetriever.ErrorMessage))
                    {
                        mMessage = datasetFileRetriever.ErrorMessage;
                    }

                    return datasetCopyResult;
                }

                // Store information about the datasets in several packed job parameters
                dataPackageInfo.StorePackedDictionaries(this);

                // Find files in the transfer directory that should be copied locally
                // For zip files, copy and unzip each one
                var fileCopyResult = RetrieveTransferDirectoryFiles(workingDirectory, transferDirectoryPath);

                if (fileCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                    return fileCopyResult;

                if (StepToolName.Equals(MAXQUANT_PEAK_STEP_TOOL, StringComparison.OrdinalIgnoreCase))
                {
                    // Retrieve files ScanStats.txt and ScanStatsEx.txt for each dataset
                    // PHRP uses the ScanStatsEx.txt file to compute mass errors for each PSM
                    var precursorInfoResult = GetScanStatsAndCreatePrecursorInfo(dataPackageDatasets);

                    if (precursorInfoResult != CloseOutType.CLOSEOUT_SUCCESS)
                        return precursorInfoResult;
                }

                var subdirectoryCompressor = new SubdirectoryFileCompressor(workingDirectory, mDebugLevel);
                RegisterEvents(subdirectoryCompressor);

                // Create the working directory metadata file
                var metadataFileSuccess = subdirectoryCompressor.CreateWorkingDirectoryMetadataFile();
                return metadataFileSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Read the DMS step nodes from a MaxQuant parameter file (either from a previous job step or the master parameter file)
        /// Examine the nodes to determine if this job step should be skipped
        /// Also reads and returns the value of the protein description parse rule node
        /// </summary>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="maxQuantParameterFileName">MaxQuant parameter file name</param>
        /// <param name="requireFastaFileNodes">If true, require that the FASTA file nodes be defined</param>
        /// <param name="abortProcessing">Output: will be true if an error occurred</param>
        /// <param name="skipReason">Output: skip reason</param>
        /// <param name="dmsSteps">Output: dictionary of step numbers and MaxQuant processing step names</param>
        /// <param name="proteinDescriptionParseRule">Output: protein description parse rule (RegEx for extracting protein descriptions from the FASTA file)</param>
        /// <returns>True if this job step should be skipped, otherwise false</returns>
        private bool CheckSkipMaxQuant(
            FileSystemInfo workingDirectory,
            string maxQuantParameterFileName,
            bool requireFastaFileNodes,
            out bool abortProcessing,
            out string skipReason,
            out Dictionary<int, DmsStepInfo> dmsSteps,
            out string proteinDescriptionParseRule)
        {
            abortProcessing = false;
            skipReason = string.Empty;
            dmsSteps = new Dictionary<int, DmsStepInfo>();
            proteinDescriptionParseRule = string.Empty;

            try
            {
                var sourceFile = new FileInfo(Path.Combine(workingDirectory.FullName, maxQuantParameterFileName));

                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                var descriptionParseRuleNodes = doc.Elements("MaxQuantParams").Elements("fastaFiles").Elements("FastaFileInfo").Elements("descriptionParseRule").ToList();

                if (descriptionParseRuleNodes.Count == 0 && requireFastaFileNodes)
                {
                    LogError("MaxQuant parameter file from previous job step is missing node <fastaFiles><FastaFileInfo><descriptionParseRule>");
                    abortProcessing = true;
                    return false;
                }

                if (descriptionParseRuleNodes.Count > 0)
                {
                    proteinDescriptionParseRule = descriptionParseRuleNodes[0].Value;
                }

                var dmsStepNodes = doc.Elements("MaxQuantParams").Elements("dmsSteps").Elements("step").ToList();

                if (dmsStepNodes.Count == 0)
                {
                    // Step tool MaxqPeak will run each of the MaxQuant steps
                    // Skip processing for step tools MaxqS1, MaxqS2, and MaxqS3

                    if (!StepToolName.Equals(MAXQUANT_PEAK_STEP_TOOL, StringComparison.OrdinalIgnoreCase))
                    {
                        skipReason = string.Format(
                            "Skipping '{0}' since step tool '{1}' should have already run MaxQuant to completion",
                            StepToolName, MAXQUANT_PEAK_STEP_TOOL);

                        LogMessage(skipReason);
                        return true;
                    }
                }

                // Get the DMS step info
                foreach (var item in dmsStepNodes)
                {
                    if (!AnalysisToolRunnerMaxQuant.GetDmsStepDetails(item, out var dmsStepInfo, out var errorMessage))
                    {
                        LogError(errorMessage);
                        abortProcessing = true;
                        return false;
                    }

                    dmsSteps.Add(dmsStepInfo.ID, dmsStepInfo);
                }

                var countWithValue = dmsSteps.Count(item => item.Value.StartStepID.HasValue);

                if (countWithValue == 0)
                {
                    // None of the steps has an integer defined for StartStepID
                    // Either this is an unmodified MaxQuant parameter file or MaxQuant has not yet been run
                    return false;
                }

                if (countWithValue < dmsSteps.Count)
                {
                    LogError("DMS steps in the MaxQuant parameter file have a mix of integer startStepID's and text=based startStepID's; " +
                             "either all should have 'auto' or all should have integers");
                    abortProcessing = true;
                    return false;
                }

                // Each of the steps has a step ID defined
                // Examine the StartStepID for the step that matches this step tool
                foreach (var dmsStep in dmsSteps.Where(dmsStep => dmsStep.Value.Tool.Equals(StepToolName)))
                {
                    if (dmsStep.Value.StartStepID >= 0)
                    {
                        // Do not skip this step tool
                        return false;
                    }

                    // Skip this step tool
                    LogMessage("Skipping step tool {0} since the StartStepID value in the MaxQuant parameter file is negative", StepToolName);

                    return true;
                }

                // Match not found
                LogMessage("Skipping step tool {0} since none of the tool names in the dmsSteps section of the MaxQuant parameter file matched this step tool", StepToolName);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CheckSkipMaxQuant", ex);
                abortProcessing = true;
                return false;
            }
        }

        private CloseOutType CreatePrecursorInfoFile(string datasetName)
        {
            try
            {
                var processor = new PrecursorInfoFileCreator();
                RegisterEvents(processor);

                var success = processor.CreatePrecursorInfoFile(mWorkDir, datasetName);

                if (success)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                LogWarning("CreatePrecursorInfoFile returned false");
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in CreatePrecursorInfoFile", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Examine the protein header lines in this job's FASTA file
        /// Define the protein description parse rule that MaxQuant should use, depending on whether every header line has a protein name and protein description
        /// </summary>
        /// <returns>Result Code</returns>
        private CloseOutType DetermineProteinDescriptionParseRule(out string proteinDescriptionParseRule)
        {
            try
            {
                var localOrgDbDirectory = new DirectoryInfo(mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR));
                var generatedFastaFileName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, JOB_PARAM_GENERATED_FASTA_NAME);

                var generatedFastaFile = new FileInfo(Path.Combine(localOrgDbDirectory.FullName, generatedFastaFileName));

                if (!generatedFastaFile.Exists)
                {
                    LogError("Generated FASTA file not found; cannot determine the protein description parse rule");
                    proteinDescriptionParseRule = string.Empty;
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var fastaFileReader = new ProteinFileReader.FastaFileReader();

                if (!fastaFileReader.OpenFile(generatedFastaFile.FullName))
                {
                    LogError("Error reading FASTA file with ProteinFileReader to determine the protein description parse rule");
                    proteinDescriptionParseRule = string.Empty;
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var missingProteinDescription = false;

                while (true)
                {
                    var inputProteinFound = fastaFileReader.ReadNextProteinEntry();

                    if (!inputProteinFound)
                    {
                        break;
                    }

                    if (!string.IsNullOrWhiteSpace(fastaFileReader.ProteinDescription))
                    {
                        continue;
                    }

                    missingProteinDescription = true;
                    break;
                }

                if (missingProteinDescription)
                {
                    // One or more proteins does not have a protein description
                    proteinDescriptionParseRule = AnalysisToolRunnerMaxQuant.PROTEIN_NAME_AND_DESCRIPTION_REGEX;
                }
                else
                {
                    // Every protein has a description
                    proteinDescriptionParseRule = AnalysisToolRunnerMaxQuant.PROTEIN_DESCRIPTION_REGEX;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in DetermineProteinDescriptionParseRule", ex);
                proteinDescriptionParseRule = string.Empty;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Look for a step tool parameter file from the previous job step
        /// If found, copy to the working directory, naming it ToolParameters_JobNum_PreviousStep.xml
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool GetExistingToolParametersFile(
            FileSystemInfo workingDirectory,
            string transferDirectoryPath,
            string paramFileName,
            out string previousJobStepParameterFilePath)
        {
            previousJobStepParameterFilePath = string.Empty;

            if (Global.OfflineMode)
                return true;

            try
            {
                var stepNum = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 1);

                if (stepNum == 1)
                {
                    // This is the first step; nothing to retrieve
                    return true;
                }

                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    // Transfer directory parameter is empty; nothing to retrieve
                    return true;
                }

                var sourceFile = new FileInfo(Path.Combine(transferDirectoryPath, paramFileName));

                if (!sourceFile.Exists)
                {
                    // File not found; nothing to copy
                    return true;
                }

                // Copy the file, renaming to avoid a naming collision
                var destinationFilePath = Path.Combine(workingDirectory.FullName, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_PreviousStep.xml");

                if (mFileCopyUtilities.CopyFileWithRetry(sourceFile.FullName, destinationFilePath, overwrite: true, maxCopyAttempts: 3))
                {
                    if (mDebugLevel > 3)
                    {
                        LogDebugMessage("GetExistingToolParametersFile, File copied:  " + sourceFile.FullName);
                    }

                    previousJobStepParameterFilePath = destinationFilePath;
                    mJobParams.AddResultFileToSkip(sourceFile.Name);
                    return true;
                }

                LogError("Error in GetExistingToolParametersFile copying file " + sourceFile.FullName);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in GetExistingToolParametersFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve files ScanStats.txt and ScanStatsEx.txt for each dataset
        /// Use them to create the _PrecursorInfo.txt file
        /// </summary>
        /// <param name="dataPackageDatasets">Keys are Dataset ID, values are dataset info</param>
        private CloseOutType GetScanStatsAndCreatePrecursorInfo(Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            // Cache the current dataset and job info
            CacheCurrentDataAndJobInfo();

            var datasetsProcessed = 0;

            foreach (var dataPkgDataset in dataPackageDatasets.Values)
            {
                if (!OverrideCurrentDatasetInfo(dataPkgDataset))
                {
                    // Error message has already been logged
                    RestoreCachedDataAndJobInfo();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var rawDataType = GetRawDataType(rawDataTypeName);

                if (rawDataType == RawDataTypeConstants.AgilentDFolder)
                {
                    // The ScanStatsGenerator class uses the MSFileInfoScanner to generate the ScanStats files
                    // The logic for determining parent ion m/z values only works for Thermo .raw files
                    // Thus, no point in creating the ScanStats files for Agilent datasets
                    LogMessage("Not creating ScanStats files for Agilent dataset {0}", DatasetName);
                    continue;
                }

                var retrieveResult = GetScanStatsFiles(dataPkgDataset);

                if (retrieveResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Error message has already been logged
                    RestoreCachedDataAndJobInfo();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Use the ScanStats files to create the _PrecursorInfo.txt file
                var createResult = CreatePrecursorInfoFile(dataPkgDataset.Dataset);

                if (createResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Error message has already been logged
                    RestoreCachedDataAndJobInfo();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                datasetsProcessed++;
            }

            mJobParams.AddResultFileExtensionToSkip(PHRPReader.ReaderFactory.SCAN_STATS_FILENAME_SUFFIX);
            mJobParams.AddResultFileExtensionToSkip(PHRPReader.ReaderFactory.EXTENDED_SCAN_STATS_FILENAME_SUFFIX);

            RestoreCachedDataAndJobInfo();

            if (datasetsProcessed > 1)
            {
                LogMessage(string.Format("Retrieved the ScanStats files for {0} datasets", datasetsProcessed));
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve files ScanStats.txt and ScanStatsEx.txt for the dataset
        /// </summary>
        /// <remarks>The calling method should have already called OverrideCurrentDatasetInfo</remarks>
        /// <param name="dataPkgDataset">Data package dataset info</param>
        private CloseOutType GetScanStatsFiles(DataPackageDatasetInfo dataPkgDataset)
        {
            try
            {
                var success = FileSearchTool.RetrieveScanStatsFiles(createStoragePathInfoOnly: false, retrieveScanStatsFile: true, retrieveScanStatsExFile: true);

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (success)
                {
                    LogMessage(string.Format("Retrieved MASIC ScanStats and ScanStatsEx files for {0}", dataPkgDataset.Dataset), 1);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // ScanStats files not found
                // If processing a .Raw file, .UIMF file, or Agilent .D directory, we can create the file using the MSFileInfoScanner

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var rawDataType = GetRawDataType(rawDataTypeName);
                var deleteLocalDatasetFileOrDirectory = rawDataType == RawDataTypeConstants.AgilentDFolder;

                if (!GenerateScanStatsFiles(deleteLocalDatasetFileOrDirectory))
                {
                    // Error message should already have been logged and stored in mMessage
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in GetScanStatsFiles", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Read modification names from file modifications.xml
        /// </summary>
        /// <param name="modificationsFilePath">Output: modifications file path</param>
        /// <returns>List of modification names</returns>
        private SortedSet<string> LoadMaxQuantModificationNames(out string modificationsFilePath)
        {
            modificationsFilePath = string.Empty;

            try
            {
                // Determine the path to MaxQuantCmd.exe
                var maxQuantProgLoc = AnalysisToolRunnerBase.DetermineProgramLocation(
                    mMgrParams, mJobParams, StepToolName,
                    "MaxQuantProgLoc", AnalysisToolRunnerMaxQuant.MAXQUANT_EXE_NAME,
                    out var errorMessage, out _);

                if (!string.IsNullOrEmpty(errorMessage))
                {
                    // The error has already been logged, but we need to update mMessage
                    mMessage = Global.AppendToComment(mMessage, errorMessage);
                    return new SortedSet<string>();
                }

                if (string.IsNullOrWhiteSpace(maxQuantProgLoc))
                {
                    LogError("MaxQuant location could not be determined using manager parameter {0} and relative path {1}", "MaxQuantProgLoc", AnalysisToolRunnerMaxQuant.MAXQUANT_EXE_NAME);
                    return new SortedSet<string>();
                }

                var maxQuantExecutable = new FileInfo(maxQuantProgLoc);

                if (maxQuantExecutable.Directory == null)
                {
                    LogError(string.Format("Unable to determine the MaxQuant parent directory using {0}", maxQuantProgLoc));
                    return new SortedSet<string>();
                }

                var confDirectory = new DirectoryInfo(Path.Combine(maxQuantExecutable.Directory.FullName, "conf"));

                if (!confDirectory.Exists)
                {
                    LogError(string.Format("MaxQuant conf directory not found: {0}", confDirectory.FullName));
                    return new SortedSet<string>();
                }

                var sourceFile = new FileInfo(Path.Combine(confDirectory.FullName, "modifications.xml"));

                if (!sourceFile.Exists)
                {
                    LogError(string.Format("MaxQuant modifications file not found: {0}", sourceFile.FullName));
                    return new SortedSet<string>();
                }

                modificationsFilePath = sourceFile.FullName;

                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                var modificationNodes = doc.Elements("modifications").Elements("modification").ToList();

                var maxQuantModificationNames = new SortedSet<string>();

                foreach (var item in modificationNodes)
                {
                    if (!Global.TryGetAttribute(item, "title", out var modTitle))
                    {
                        LogWarning("Modification in the MaxQuant modifications.xml file is missing the title attribute: {0}", item);
                        continue;
                    }

                    // ReSharper disable once CanSimplifySetAddingWithSingleCall
                    if (maxQuantModificationNames.Contains(modTitle))
                    {
                        LogError(string.Format("Modification {0} is defined twice in the MaxQuant modifications.xml file", modTitle));
                        LogWarning("File path {0}", sourceFile.FullName);
                        return new SortedSet<string>();
                    }

                    maxQuantModificationNames.Add(modTitle);
                }

                return maxQuantModificationNames;
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMaxQuantModificationNames", ex);
                return new SortedSet<string>();
            }
        }

        private CloseOutType RetrieveTransferDirectoryFiles(FileSystemInfo workingDirectory, string transferDirectoryPath)
        {
            var targetFilePath = "??";

            try
            {
                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    LogError("Transfer directory not defined in the job parameters");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var transferDirectory = new DirectoryInfo(transferDirectoryPath);

                if (!transferDirectory.Exists)
                {
                    // The transfer directory may not yet exist
                    // This is allowed if this is step 2
                    var stepNumber = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 1);

                    if (stepNumber == 2)
                        return CloseOutType.CLOSEOUT_SUCCESS;

                    LogError("Transfer directory not found, cannot retrieve results from the previous job step: " + transferDirectoryPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var zipTools = new ZipFileTools(mDebugLevel, workingDirectory.FullName);
                RegisterEvents(zipTools);

                // Populate a list with relative file paths of files to not retrieve from the transfer directory
                var relativeFilePathsToSkip = new SortedSet<string> {
                    "MaxQuant_ConsoleOutput.txt",
                    "MaxqPeak_ConsoleOutput.txt",
                    "MaxqS1_ConsoleOutput.txt",
                    "MaxqS2_ConsoleOutput.txt",
                    "MaxqS3_ConsoleOutput.txt"
                };

                foreach (var item in transferDirectory.GetFiles("*", SearchOption.AllDirectories))
                {
                    var relativeFilePath = item.FullName.Substring(transferDirectory.FullName.Length + 1);

                    if (relativeFilePathsToSkip.Contains(relativeFilePath))
                        continue;

                    targetFilePath = Path.Combine(workingDirectory.FullName, relativeFilePath);
                    var targetFile = new FileInfo(targetFilePath);

                    if (targetFile.Exists)
                        continue;

                    var copySuccess = mFileTools.CopyFileUsingLocks(item, targetFile.FullName, true);

                    if (!copySuccess)
                    {
                        LogError("Error copying file {0} to {1}", item.FullName, targetFile.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!targetFile.Extension.Equals(".zip", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (targetFile.Directory == null)
                    {
                        LogError(string.Format("Unable to determine the parent directory of zip file {0}", targetFile.FullName));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    var targetDirectory = Path.Combine(targetFile.Directory.FullName, Path.GetFileNameWithoutExtension(relativeFilePath));

                    // Unzip the file
                    var unzipSuccess = zipTools.UnzipFile(targetFile.FullName, targetDirectory);

                    if (!unzipSuccess)
                    {
                        LogError("Error unzipping file {0} to {1}", targetFile.FullName, targetDirectory);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Delete the zip file
                    targetFile.Delete();
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveTransferDirectoryFiles for file " + targetFilePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Look for static and dynamic modifications in the MaxQuant parameter file for this job
        /// Assure that they are defined in the local MaxQuant modifications.xml file
        /// </summary>
        /// <param name="workingDirectoryPath">Working directory path</param>
        /// <param name="maxQuantParameterFileName">MaxQuant parameter file name</param>
        /// <returns>True if the modifications are valid, false if missing</returns>
        private bool ValidateMaxQuantModificationNames(
            string workingDirectoryPath,
            string maxQuantParameterFileName)
        {
            try
            {
                var maxQuantModificationNames = LoadMaxQuantModificationNames(out var modificationsFilePath);

                if (maxQuantModificationNames.Count == 0)
                {
                    // The error has already been logged
                    return false;
                }

                var modificationGroups = new Dictionary<string, List<XElement>>();

                var sourceFile = new FileInfo(Path.Combine(workingDirectoryPath, maxQuantParameterFileName));

                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                modificationGroups.Add("restrictMods", doc.Elements("MaxQuantParams").Elements("restrictMods").Elements("string").ToList());

                var parameterGroupNodes = doc.Elements("MaxQuantParams").Elements("parameterGroups").Elements("parameterGroup").ToList();

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (parameterGroupNodes.Count == 0)
                {
                    LogWarning("MaxQuant parameter file is missing the <parameterGroup> element; cannot extract modification info");
                    return false;
                }

                Console.WriteLine();

                var groupNumber = 0;

                foreach (var parameterGroup in parameterGroupNodes)
                {
                    groupNumber++;

                    if (parameterGroupNodes.Count > 1)
                    {
                        Console.WriteLine("Parameter group {0}", groupNumber);
                    }

                    modificationGroups.Add("variableModificationsFirstSearch", parameterGroup.Elements("variableModificationsFirstSearch").Elements("string").ToList());

                    modificationGroups.Add("fixedModifications", parameterGroup.Elements("fixedModifications").Elements("string").ToList());

                    modificationGroups.Add("variableModifications", parameterGroup.Elements("variableModifications").Elements("string").ToList());

                    // Check for isobaric mods, e.g. 6-plex or 10-plex TMT
                    modificationGroups.Add("IsobaricLabelInfo_internalLabel", parameterGroup.Elements("isobaricLabels").Elements("IsobaricLabelInfo").Elements("internalLabel").ToList());

                    modificationGroups.Add("IsobaricLabelInfo_terminalLabel", parameterGroup.Elements("isobaricLabels").Elements("IsobaricLabelInfo").Elements("terminalLabel").ToList());
                }

                var hasUnknownModifications = false;

                foreach (var item in modificationGroups)
                {
                    if (!ValidateMaxQuantModificationNames(maxQuantModificationNames, item))
                        hasUnknownModifications = true;
                }

                if (!hasUnknownModifications)
                    return true;

                LogError(string.Format(
                    "Parameter file {0} has one or more modifications not defined in {1}", sourceFile.Name, modificationsFilePath), true);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidateMaxQuantModificationNames", ex);
                return false;
            }
        }

        private bool ValidateMaxQuantModificationNames(ICollection<string> maxQuantModificationNames, KeyValuePair<string, List<XElement>> modificationGroup)
        {
            var hasUnknownModifications = false;

            foreach (var modification in modificationGroup.Value)
            {
                if (maxQuantModificationNames.Contains(modification.Value))
                {
                    continue;
                }

                LogError(
                    "The MaxQuant parameter file has a modification not defined in the local MaxQuant modifications.xml file: {0} in section {1}",
                    modification.Value, modificationGroup.Key);

                hasUnknownModifications = true;
            }

            return !hasUnknownModifications;
        }
    }
}
