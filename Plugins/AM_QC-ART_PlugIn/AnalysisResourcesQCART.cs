/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 11/02/2015                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerQCARTPlugin
{
    /// <summary>
    /// Retrieve resources for the QC-ART plugin
    /// </summary>
    public class AnalysisResourcesQCART : AnalysisResources
    {
        // Ignore Spelling: LockFile, QCART, SMAQC, tryptic

        /// <summary>
        /// Job parameter tracking the dataset names and jobs to process
        /// </summary>
        public const string JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS = "QC-ART_Baseline_Dataset_NamesAndJobs";

        /// <summary>
        /// Job parameter tracking the baseline results cache folder
        /// </summary>
        public const string JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER = "QC-ART_Baseline_Results_Cache_Folder_Path";

        /// <summary>
        /// Job parameter tracking the baseline results metadata filename
        /// </summary>
        public const string JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME = "QC-ART_Baseline_Metadata_File_Name";

        /// <summary>
        /// Job parameter tracking the baseline metadata lock file
        /// </summary>
        public const string JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE = "QC-ART_Baseline_Metadata_LockFilePath";

        // Existing baseline results data file name
        /// <summary>
        /// Job parameter tracking the baseline results filename
        /// </summary>
        public const string JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME = "QC-ART_Baseline_Results_File_Name";

        /// <summary>
        /// Job parameter tracking the QC-ART project name
        /// </summary>
        public const string JOB_PARAMETER_QCART_PROJECT_NAME = "QC-ART_Project_Name";

        /// <summary>
        /// SMAQC data file name
        /// </summary>
        public const string SMAQC_DATA_FILE_NAME = "SMAQC_Data.csv";

        /// <summary>
        /// Reporter ions file suffix
        /// </summary>
        public const string REPORTER_IONS_FILE_SUFFIX = "_ReporterIons.txt";

        /// <summary>
        /// QC-ART processing script name
        /// </summary>
        public const string QCART_PROCESSING_SCRIPT_NAME = "QC-ART_Processing_Script.R";

        /// <summary>
        /// QC-ART results file suffix
        /// </summary>
        public const string QCART_RESULTS_FILE_SUFFIX = "_QC-ART.txt";

        /// <summary>
        /// New baseline datasets metadata file
        /// </summary>
        public const string NEW_BASELINE_DATASETS_METADATA_FILE = "NewBaselineDatasets_Metadata.csv";

        /// <summary>
        /// New baseline datasets cache file
        /// </summary>
        public const string NEW_BASELINE_DATASETS_CACHE_FILE = "NewBaselineDatasets_Data_Cache.csv";

        private string mProjectName = string.Empty;

        private int mTargetDatasetFraction;

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            mProjectName = string.Empty;

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

                var success = FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the QC_ART R script
                // This can be defined in a settings file using parameter QCARTRScriptName
                // However, as of November 2015, QC-ART jobs do not use settings files, and we thus simply use the default
                currentTask = "Retrieve the R script";
                var rScriptName = mJobParams.GetJobParameter("QCARTRScriptName", "QC_ART_2015-11-11.R");
                var rScriptStoragePath = Path.Combine(paramFileStoragePath, "Template_Scripts");

                success = FileSearchTool.RetrieveFile(rScriptName, rScriptStoragePath);

                if (!success)
                {
                    mMessage = "Template QC-ART R Script not found: " + rScriptName;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Parse the parameter file to discover the baseline datasets
                currentTask = "Read the parameter file";

                var paramFilePathRemote = Path.Combine(paramFileStoragePath, paramFileName);
                var paramFilePathLocal = Path.Combine(mWorkDir, paramFileName);

                // out var param notes:
                // In baselineDatasets, keys are dataset names and values are MASIC job numbers
                // baselineMetadataKey tracks a unique key defined as the MD5 hash of the dataset names, appended with JobFirst_JobLast

                success = ParseQCARTParamFile(paramFilePathLocal, out var baselineDatasets, out var baselineMetadataKey);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        LogError("ParseQCARTParamFile returned false (unknown error)");

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for an existing QC-ART baseline results file for the given set of datasets
                // If one is not found, we need to obtain the necessary files for the
                // baseline datasets so that the QC-ART tool can compute a new baseline

                currentTask = "Get existing baseline results";

                var baselineResultsFound = FindBaselineResults(paramFilePathRemote, baselineMetadataKey, out var baselineMetadataFilePath, out var criticalError);

                if (criticalError)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!baselineResultsFound)
                {
                    // Create a lock file indicating that this manager will be creating the baseline results data file
                    var lockFilePath = CreateLockFile(baselineMetadataFilePath, "Creating QCART baseline data via " + mMgrName);

                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE, lockFilePath);
                }
                // This list contains the dataset names for which we need to obtain QC Metric values (P_2C, MS1_2B, etc.)
                var datasetNamesToRetrieveMetrics = new SortedSet<string>
                {
                    DatasetName
                };

                // Also need to obtain the QC Metric values for the baseline datasets
                // (regardless of whether we're using an existing baseline file)
                foreach (var datasetName in baselineDatasets.Keys.Except(datasetNamesToRetrieveMetrics))
                {
                    datasetNamesToRetrieveMetrics.Add(datasetName);
                }

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                currentTask = "Retrieve " + REPORTER_IONS_FILE_SUFFIX + " file for " + DatasetName;

                success = RetrieveReporterIonsFile(currentDatasetAndJobInfo);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!baselineResultsFound)
                {
                    currentTask = "Retrieve " + REPORTER_IONS_FILE_SUFFIX + " files for the baseline datasets";

                    success = RetrieveDataForBaselineDatasets(paramFileName, baselineDatasets);

                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;

                    // Restore the dataset and job info using currentDatasetAndJobInfo
                    OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);

                    success = CreateBaselineDatasetInfoFile(baselineDatasets);

                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                currentTask = "Process the MyEMSL download queue";

                success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the baseline dataset names and MASIC Jobs so that they can be used by AnalysisToolRunnerQCART
                StorePackedJobParameterDictionary(baselineDatasets, JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS);

                currentTask = "Retrieve QC Metrics from DMS";
                success = RetrieveQCMetricsFromDB(datasetNamesToRetrieveMetrics);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Customize the QC-ART R Script
                success = CustomizeQCRScript(rScriptName, baselineResultsFound);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources; task = " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Customize the RScript to replace the template parameters with actual values
        /// </summary>
        /// <param name="rScriptName"></param>
        /// <param name="baselineResultsFound"></param>
        private bool CustomizeQCRScript(string rScriptName, bool baselineResultsFound)
        {
            try
            {
                var templateMatcher = new Regex(@"{\$([^}]+)}", RegexOptions.Compiled);

                var templateFile = new FileInfo(Path.Combine(mWorkDir, rScriptName));
                var customizedScript = new FileInfo(Path.Combine(mWorkDir, QCART_PROCESSING_SCRIPT_NAME));

                using (var reader = new StreamReader(new FileStream(templateFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(customizedScript.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var reMatch = templateMatcher.Match(dataLine);

                        if (!reMatch.Success)
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var paramName = reMatch.Groups[1].Value;
                        string customValue;

                        switch (paramName)
                        {
                            case "WORKING_DIRECTORY_PATH":
                                // Example value: E:/DMS_WorkDir1/
                                customValue = ToUnixFolderPath(mWorkDir);
                                break;

                            case "TARGET_DATASET_NAME":
                                customValue = DatasetName;
                                break;

                            case "TARGET_DATASET_FRACTION":
                                if (mTargetDatasetFraction == 0)
                                {
                                    var errorMsg = "Error in CustomizeQCRScript: SCX fraction for dataset " + DatasetName + " is 0";
                                    LogError(errorMsg, errorMsg + "; should have been determined in RetrieveQCMetricsFromDB");
                                    return false;
                                }
                                customValue = mTargetDatasetFraction.ToString();
                                break;

                            case "USE_EXISTING_BASELINE":
                                customValue = baselineResultsFound ? "TRUE" : "FALSE";
                                break;

                            case "NEW_BASELINE_DATASET_INFO_FILENAME":
                                customValue = baselineResultsFound ? "NULL" : NEW_BASELINE_DATASETS_METADATA_FILE;
                                break;

                            case "EXISTING_BASELINE_CSV_NAME":
                                if (baselineResultsFound)
                                {
                                    customValue = mJobParams.GetJobParameter(JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME, string.Empty);

                                    if (string.IsNullOrWhiteSpace(customValue))
                                    {
                                        const string errorMsg = "Error in CustomizeQCRScript: " + JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME + " is undefined";
                                        LogError(errorMsg, errorMsg + "; should have been stored in RetrieveExistingBaselineResultFile");
                                        return false;
                                    }
                                }
                                else
                                {
                                    customValue = "UndefinedFile_since_NewBaselineDatasets.csv";
                                }
                                break;

                            case "NEW_BASELINE_DATA_FILENAME":
                                if (baselineResultsFound)
                                {
                                    customValue = "UndefinedFile_sinceExistingBaselineDatasetFile.csv";
                                }
                                else
                                {
                                    customValue = NEW_BASELINE_DATASETS_CACHE_FILE;
                                }
                                break;

                            case "SMAQC_DATA_FILENAME":
                                customValue = SMAQC_DATA_FILE_NAME;
                                break;

                            case "TARGET_DATASET_RESULTS":
                                customValue = DatasetName + QCART_RESULTS_FILE_SUFFIX;
                                break;

                            default:
                                LogError("Unrecognized template name in the QC-ART template script: " + paramName + " in " + rScriptName);
                                return false;
                        }

                        var updatedLine = templateMatcher.Replace(dataLine, customValue);

                        writer.WriteLine(updatedLine);
                    }
                }

                mJobParams.AddResultFileToSkip(templateFile.Name);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CustomizeQCRScript", ex);
                return false;
            }
        }

        /// <summary>
        /// Create a CSV file listing dataset name and fraction number for each baseline dataset
        /// </summary>
        /// <param name="baselineDatasets"></param>
        /// <returns>True if success, otherwise false</returns>
        private bool CreateBaselineDatasetInfoFile(Dictionary<string, int> baselineDatasets)
        {
            try
            {
                var datasetParseErrors = new List<string>();

                var baselineDataInfoFilePath = Path.Combine(mWorkDir, NEW_BASELINE_DATASETS_METADATA_FILE);

                using (var writer = new StreamWriter(new FileStream(baselineDataInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("DatasetName,Fraction");

                    foreach (var dataset in baselineDatasets)
                    {
                        var fractionNumber = ExtractFractionFromDatasetName(dataset.Key);

                        if (fractionNumber <= 0)
                        {
                            datasetParseErrors.Add(dataset.Key);
                        }
                        writer.WriteLine(dataset.Key + "," + fractionNumber);
                    }
                }

                if (datasetParseErrors.Count == 0)
                    return true;

                // Error parsing out the SCX fraction number from one or more datasets
                const string errorMessage = "Could not determine SCX fraction number";
                LogErrorForOneOrMoreDatasets(errorMessage, datasetParseErrors, "CreateBaselineDatasetInfoFile");

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateBaselineDatasetInfoFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Extract the SCX fraction number from the dataset name, which must itself be in a canonical format
        /// </summary>
        /// <remarks>
        /// Supported format #1, based on TEDDY_DISCOVERY_SET_34_23_20Oct15_Frodo_15-08-15    and
        ///                               TEDDY_DISCOVERY_SET_32_10rr_13Oct15_Frodo_15-08-15  and
        ///                               TEDDY_DISCOVERY_SET_54_15-rr_01May16_Frodo_16-03-33
        /// Look for "_SET_\d+_\d+_" or "_SET_\d+_\d+[a-z-]+_"
        /// SCX fraction is the second number
        /// </remarks>
        /// <param name="datasetName"></param>
        /// <returns>Fraction number on success; 0 if an error</returns>
        private static int ExtractFractionFromDatasetName(string datasetName)
        {
            // RegEx to extract the fraction number
            // Must include [a-z]* for dataset names with SET_31_16rr or SET_32_22a
            var reSCXMatcher = new Regex(@"_SET_\d+_(\d+)[a-z-]*_", RegexOptions.IgnoreCase);

            var match = reSCXMatcher.Match(datasetName);

            if (!match.Success)
                return 0;

            var scxFraction = int.Parse(match.Groups[1].Value);
            return scxFraction;
        }

        /// <summary>
        /// Look for existing baseline results
        /// </summary>
        /// <remarks>Also uses mProjectName and baselineDatasets</remarks>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="baselineMetadataKey">Baseline metadata file unique key</param>
        /// <param name="baselineMetadataFilePath">Output: baseline metadata file path (remote path in the cache folder)</param>
        /// <param name="criticalError">Output: true if a critical error occurred and the job should be aborted</param>
        /// <returns>True if existing results were found and successfully copied locally; otherwise false</returns>
        private bool FindBaselineResults(
            string paramFilePath,
            string baselineMetadataKey,
            out string baselineMetadataFilePath,
            out bool criticalError)
        {
            baselineMetadataFilePath = string.Empty;
            criticalError = false;
            var currentTask = "Initializing";

            try
            {
                string errorMessage;

                // Find the parameter file in the remote location
                currentTask = "Finding parameter file";
                var paramFile = new FileInfo(paramFilePath);

                if (!paramFile.Exists)
                {
                    errorMessage = "Parameter file not found";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                if (paramFile.Directory == null)
                {
                    errorMessage = "Parameter file directory is null";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                currentTask = "Finding cache folder";
                var cacheFolder = new DirectoryInfo(Path.Combine(paramFile.Directory.FullName, "Cache"));

                if (!cacheFolder.Exists)
                {
                    // Create the directory (we'll use it later)
                    cacheFolder.Create();
                    return false;
                }

                currentTask = "Finding project-specific cache folder";
                var projectFolder = new DirectoryInfo(Path.Combine(cacheFolder.FullName, mProjectName));

                if (!projectFolder.Exists)
                {
                    // Create the directory (we'll use it later)
                    projectFolder.Create();
                    return false;
                }

                baselineMetadataFilePath = GetBaselineMetadataFilePath(projectFolder.FullName, baselineMetadataKey);
                var baselineMetadataFileName = Path.GetFileName(baselineMetadataFilePath);

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER, projectFolder.FullName);
                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME, baselineMetadataFileName);

                currentTask = "Looking for a QCART baseline metadata lock file";

                // Look for a QCART_Cache lock file in the Project folder
                // Wait up to 1 hour for an existing lock file to be deleted or age
                CheckForLockFile(baselineMetadataFilePath, "QCART baseline metadata file", mStatusTools, 60);

                // Now check for an existing baseline results file
                currentTask = "Looking for " + baselineMetadataFilePath;

                var baselineMetadata = new FileInfo(baselineMetadataFilePath);

                if (!baselineMetadata.Exists)
                {
                    // Valid baseline results metadata file was not found
                    return false;
                }

                currentTask = "Retrieving cached results";

                var success = RetrieveExistingBaselineResultFile(baselineMetadata, out criticalError);
                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception in FindBaselineResults: " + currentTask, ex);
                criticalError = true;
                return false;
            }
        }

        /// <summary>
        /// Construct the baseline metadata file path
        /// </summary>
        /// <param name="outputFolderPath"></param>
        /// <param name="baselineMetadataKey"></param>
        private static string GetBaselineMetadataFilePath(string outputFolderPath, string baselineMetadataKey)
        {
            var baselineMetadataFileName = "QCART_Cache_" + baselineMetadataKey + ".xml";
            var baselineMetadataPath = Path.Combine(outputFolderPath, baselineMetadataFileName);

            return baselineMetadataPath;
        }

        /// <summary>
        /// Log an error message with one of these formats:
        ///   Could not do X for A
        ///   Could not do X for A or B
        ///   Could not do X for A or 3 other datasets
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="datasetNames">List of dataset names</param>
        /// <param name="callingProcedure">Name of the calling procedure</param>
        private void LogErrorForOneOrMoreDatasets(string errorMessage, ICollection<string> datasetNames, string callingProcedure)
        {
            var fullMessage = datasetNames.Count switch
            {
                1 => errorMessage + " for " + datasetNames.FirstOrDefault(),
                2 => errorMessage + " for " + datasetNames.FirstOrDefault() + " or " + datasetNames.LastOrDefault(),
                _ => errorMessage + " for " + datasetNames.FirstOrDefault() + " or the other " + (datasetNames.Count - 1) + " datasets"
            };

            LogError(fullMessage + " (" + callingProcedure + ")");
        }

        /// <summary>
        /// Parse the QC-ART parameter file
        /// </summary>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="baselineDatasets">Output: List of baseline datasets (dataset name and dataset ID)</param>
        /// <param name="baselineMetadataKey">Output: baseline file unique key</param>
        private bool ParseQCARTParamFile(string paramFilePath, out Dictionary<string, int> baselineDatasets, out string baselineMetadataKey)
        {
            const string NO_BASELINE_INFO = "One or more baseline datasets not found in the QC-ART parameter file; " +
                                            "expected at <Parameters><BaselineList><BaselineDataset>";

            baselineDatasets = new Dictionary<string, int>();
            baselineMetadataKey = string.Empty;

            try
            {
                var paramFile = new XPathDocument(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Metadata/Project");

                if (!projectNode.MoveNext())
                {
                    LogError("Project node not found in the QC-ART parameter file; " +
                             "expected at <Parameters><Metadata><Project>");
                    return false;
                }

                mProjectName = projectNode.Current.Value;

                if (string.IsNullOrWhiteSpace(mProjectName))
                {
                    mProjectName = "Unknown";
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAMETER_QCART_PROJECT_NAME, mProjectName);

                var baselineDatasetEntries = contents.Select("/Parameters/BaselineList/BaselineDataset");

                if (baselineDatasetEntries.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }

                while (baselineDatasetEntries.MoveNext())
                {
                    if (baselineDatasetEntries.Current.HasChildren)
                    {
                        var masicJob = baselineDatasetEntries.Current.SelectChildren("MasicJob", string.Empty);
                        var datasetName = baselineDatasetEntries.Current.SelectChildren("Dataset", string.Empty);

                        if (masicJob.MoveNext() && datasetName.MoveNext())
                        {
                            baselineDatasets.Add(datasetName.Current.Value, masicJob.Current.ValueAsInt);
                            continue;
                        }
                    }

                    LogError("Nodes MasicJob and Dataset not found beneath a BaselineDataset node " +
                             "in the QC-ART parameter file");
                    return false;
                }

                if (baselineDatasets.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }

                // Compute the MD5 Hash of the datasets and jobs in baselineDatasets

                var textToHash = new StringBuilder();
                var query = (from item in baselineDatasets orderby item.Key select item);

                foreach (var item in query)
                {
                    textToHash.Append(item.Key);
                    textToHash.Append(item.Value);
                }

                // Hash contents of this stream
                // We will use the first 8 characters of the hash as a uniquifier for the baseline unique key
                var md5Hash = PRISM.HashUtilities.ComputeStringHashMD5(textToHash.ToString());

                // Key format: FirstJob_LastJob_DatasetCount_FirstEightCharsFromHash
                baselineMetadataKey = baselineDatasets.Values.Min() + "_" + baselineDatasets.Values.Max() + "_" + baselineDatasets.Count + "_" + md5Hash.Substring(0, 8);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseQCARTParamFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file for the baseline datasets
        /// </summary>
        /// <param name="paramFileName">Parameter file name</param>
        /// <param name="baselineDatasets">List of baseline datasets (dataset name and dataset ID)</param>
        /// <returns>True if success; otherwise false</returns>
        private bool RetrieveDataForBaselineDatasets(string paramFileName, Dictionary<string, int> baselineDatasets)
        {
            foreach (var datasetJobInfo in baselineDatasets)
            {
                var baselineDatasetName = datasetJobInfo.Key;
                var baselineDatasetJob = datasetJobInfo.Value;

                if (!LookupJobInfo(baselineDatasetJob, out var dataPkgJob))
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Unknown error retrieving the details for baseline job " + baselineDatasetJob);
                    }

                    return false;
                }

                if (!Global.IsMatch(dataPkgJob.Dataset, baselineDatasetName))
                {
                    var logMessage = "Mismatch in the QC-ART parameter file (" + paramFileName + "); " +
                                     "baseline job " + baselineDatasetJob + " is not dataset " + baselineDatasetName;

                    LogError(logMessage, logMessage + "; it is actually dataset " + dataPkgJob.Dataset);

                    return false;
                }

                OverrideCurrentDatasetAndJobInfo(dataPkgJob);

                RetrieveReporterIonsFile(dataPkgJob);
            }

            return true;
        }

        /// <summary>
        /// Parse the baseline results metadata file to determine the baseline results filename then copy it locally
        /// </summary>
        /// <param name="baselineMetadata"></param>
        /// <param name="criticalError"></param>
        /// <returns>True if the baseline results were successfully copied locally; otherwise false</returns>
        private bool RetrieveExistingBaselineResultFile(FileInfo baselineMetadata, out bool criticalError)
        {
            criticalError = false;

            try
            {
                if (baselineMetadata.Directory == null)
                {
                    const string warningMessage = "QC-ART baseline metadata file directory is null";
                    LogError(warningMessage);
                    return false;
                }

                var paramFile = new XPathDocument(new FileStream(baselineMetadata.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Results/BaselineDataCacheFile");

                if (!projectNode.MoveNext())
                {
                    const string warningMessage = "BaselineDataCacheFile node not found in the QC-ART baseline results metadata file; " +
                                         "expected at <Parameters><Results><BaselineDataCacheFile>";
                    LogError(warningMessage);
                    return false;
                }

                var baselineDataCacheFileName = projectNode.Current.Value;

                if (string.IsNullOrWhiteSpace(baselineDataCacheFileName))
                {
                    const string warningMessage = "BaselineDataCacheFile node is empty in the QC-ART baseline results metadata file";
                    LogError(warningMessage);
                    return false;
                }

                var baselineResultsFilePath = Path.Combine(baselineMetadata.Directory.FullName, baselineDataCacheFileName);

                var baselineResultsFileSource = new FileInfo(baselineResultsFilePath);

                if (!baselineResultsFileSource.Exists || baselineResultsFileSource.Directory == null)
                {
                    var warningMessage = "QC-ART baseline results data file not found: " + baselineResultsFilePath;
                    LogError(warningMessage);

                    warningMessage = "Deleting invalid QC-ART baseline metadata file: " + baselineMetadata.FullName;
                    LogError(warningMessage);

                    baselineMetadata.Delete();
                    return false;
                }

                var success = CopyFileToWorkDir(baselineResultsFileSource.Name, baselineResultsFileSource.Directory.FullName, mWorkDir);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        LogError("Unknown error retrieving " + baselineResultsFileSource.Name);

                    criticalError = true;
                    return false;
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION,
                                                   JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME,
                                                   baselineResultsFileSource.Name);

                mJobParams.AddResultFileToSkip(baselineResultsFileSource.Name);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveExistingBaselineResultFile", ex);
                criticalError = true;
                return false;
            }
        }

        /// <summary>
        /// Query the database for specific QC Metrics required by QC-ART
        /// </summary>
        /// <param name="datasetNamesToRetrieveMetrics"></param>
        /// <returns>True if success, otherwise an error</returns>
        private bool RetrieveQCMetricsFromDB(ICollection<string> datasetNamesToRetrieveMetrics)
        {
            const string DATASET_COLUMN = "dataset_name";
            const string FRACTION_COLUMN = "fraction";

            try
            {
                var datasetParseErrors = new List<string>();
                var datasetsMatched = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                if (datasetNamesToRetrieveMetrics.Count == 0)
                {
                    LogError("datasetNamesToRetrieveMetrics is empty");
                    return false;
                }

                var metricNames = new List<string>
                {
                    "p_2c",         // Number of tryptic peptides; unique peptide count
                    "ms1_2b",       // Median TIC value for identified peptides from run start through middle 50% of separation
                    "rt_ms_q1",     // The interval for the first 25% of all MS events divided by RT-Duration
                    "rt_ms_q4",     // The interval for the fourth 25% of all MS events divided by RT-Duration
                    "rt_msms_q1",   // The interval for the first 25% of all MS/MS events divided by RT-Duration
                    "rt_msms_q4"    // The interval for the fourth 25% of all MS/MS events divided by RT-Duration
                };

                var sqlStr = new StringBuilder();

                sqlStr.AppendLine("SELECT dataset AS " + DATASET_COLUMN + ", 0 AS " + FRACTION_COLUMN + ",");
                sqlStr.AppendLine("       dataset_id, acq_time_start AS date, dataset_rating AS rating,");
                sqlStr.AppendFormat("     {0} \n", string.Join(", ", metricNames));
                sqlStr.AppendLine("FROM V_Dataset_QC_Metrics_Export ");
                sqlStr.AppendLine("WHERE dataset IN (");

                sqlStr.AppendFormat("'{0}'", string.Join("', '", datasetNamesToRetrieveMetrics));
                sqlStr.AppendLine(")");

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
                RegisterEvents(dbTools);

                // Get a table to hold the results of the query
                var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

                string errorMessage;

                if (!success)
                {
                    mMessage = "Excessive failures attempting to retrieve QC metric data from database";
                    errorMessage = "RetrieveQCMetricsFromDB; " + mMessage;
                    LogError(errorMessage);
                    resultSet.Dispose();
                    return false;
                }

                // Verify at least one row returned
                if (resultSet.Rows.Count < 1)
                {
                    // No data was returned
                    errorMessage = "QC Metrics not found";
                    LogErrorForOneOrMoreDatasets(errorMessage, datasetNamesToRetrieveMetrics, "RetrieveQCMetricsFromDB");

                    return false;
                }

                var datasetAndMetricFilePath = Path.Combine(mWorkDir, SMAQC_DATA_FILE_NAME);
                using (var writer = new StreamWriter(new FileStream(datasetAndMetricFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Parse the headers
                    var headers = (from DataColumn item in resultSet.Columns select item.ColumnName).ToList();

                    var datasetNameIndex = 0;
                    var fractionColIndex = -1;

                    for (var colIndex = 0; colIndex < headers.Count; colIndex++)
                    {
                        if (string.Equals(headers[colIndex], FRACTION_COLUMN))
                            fractionColIndex = colIndex;

                        if (string.Equals(headers[colIndex], DATASET_COLUMN))
                            datasetNameIndex = colIndex;
                    }

                    if (fractionColIndex < 0)
                    {
                        LogError("RetrieveQCMetricsFromDB: column " + FRACTION_COLUMN + " not found in query results");
                        return false;
                    }

                    // Write the header line
                    writer.WriteLine(Global.FlattenList(headers, ","));

                    // Write each data line
                    foreach (DataRow row in resultSet.Rows)
                    {
                        var dataValues = new List<string>();

                        foreach (var dataVal in row.ItemArray)
                        {
                            dataValues.Add(dataVal.CastDBVal<string>());
                        }

                        var datasetName = dataValues[datasetNameIndex];

                        // Parse the dataset name to determine the SCX Fraction
                        var fractionNumber = ExtractFractionFromDatasetName(datasetName);

                        if (fractionNumber <= 0)
                        {
                            datasetParseErrors.Add(datasetName);
                        }
                        else if (fractionColIndex >= 0)
                        {
                            dataValues[fractionColIndex] = fractionNumber.ToString();

                            if (Global.IsMatch(datasetName, DatasetName))
                                mTargetDatasetFraction = fractionNumber;
                        }

                        if (datasetsMatched.Contains(datasetName))
                        {
                            // Dataset already defined
                            // This is unexpected; we will skip it
                            continue;
                        }

                        datasetsMatched.Add(datasetName);
                        writer.WriteLine(Global.FlattenList(dataValues, ","));
                    }
                }

                if (datasetParseErrors.Count == 0)
                {
                    // No errors; confirm that each dataset was found
                    if (datasetsMatched.Count == datasetNamesToRetrieveMetrics.Count)
                    {
                        return true;
                    }

                    var missingDatasets = datasetNamesToRetrieveMetrics.Except(datasetsMatched).ToList();

                    errorMessage = "QC metrics not found in V_Dataset_QC_Metrics_Export";
                    LogErrorForOneOrMoreDatasets(errorMessage, missingDatasets, "RetrieveQCMetricsFromDB");

                    return false;
                }

                // Error parsing out the SCX fraction number from one or more datasets
                errorMessage = "Could not determine SCX fraction number";
                LogErrorForOneOrMoreDatasets(errorMessage, datasetParseErrors, "RetrieveQCMetricsFromDB");

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveQCMetricsFromDB", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file
        /// for the dataset and job in dataPkgJob
        /// </summary>
        /// <param name="dataPkgJob"></param>
        private bool RetrieveReporterIonsFile(DataPackageJobInfo dataPkgJob)
        {
            try
            {
                var targetDatasetName = mJobParams.GetJobParameter("SourceJob2Dataset", string.Empty);
                var targetDatasetMasicJob = mJobParams.GetJobParameter("SourceJob2", 0);

                if (string.IsNullOrWhiteSpace(targetDatasetName) || targetDatasetMasicJob == 0)
                {
                    const string baseMessage = "Job parameters SourceJob2Dataset and SourceJob2 not found; populated via the [Special Processing] job parameter";
                    LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                    return false;
                }

                if (!Global.IsMatch(DatasetName, targetDatasetName))
                {
                    var warningMessage = "Warning: SourceJob2Dataset for job " + mJob + " does not match the dataset for this job; it is instead " + targetDatasetName;
                    LogErrorToDatabase(warningMessage);
                }

                var inputFolderNameCached = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "inputFolderName", string.Empty);

                if (Global.IsMatch(dataPkgJob.Dataset, targetDatasetName) && mJob == dataPkgJob.Job)
                {
                    // Retrieving the _ReporterIons.txt file for the dataset associated with this QC-ART job
                    var masicFolderPath = mJobParams.GetJobParameter("SourceJob2FolderPath", string.Empty);

                    if (string.IsNullOrWhiteSpace(masicFolderPath))
                    {
                        const string baseMessage = "Job parameter SourceJob2FolderPath not found; populated via the [Special Processing] job parameter";
                        LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                        return false;
                    }

                    // Override inputFolderName
                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "inputFolderName", Path.GetFileName(masicFolderPath));
                }
                else
                {
                    // Baseline dataset
                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "inputFolderName", dataPkgJob.ResultsFolderName);
                }

                var reporterIonsFileName = DatasetName + REPORTER_IONS_FILE_SUFFIX;

                var success = FileSearchTool.FindAndRetrieveMiscFiles(reporterIonsFileName, false);

                if (!success)
                {
                    LogError(REPORTER_IONS_FILE_SUFFIX + " file not found for dataset " + dataPkgJob.Dataset + ", job " + dataPkgJob.Job);
                    return false;
                }

                mJobParams.AddResultFileToSkip(reporterIonsFileName);

                // Restore the inputFolderName
                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "inputFolderName", inputFolderNameCached);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveReporterIonsFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Converts a Windows folder path to a Unix-style folder path
        /// </summary>
        /// <param name="folderPath"></param>
        /// <returns>New folder path, ending in /</returns>
        private static string ToUnixFolderPath(string folderPath)
        {
            var unixPath = folderPath.Replace("\\", "/");

            if (unixPath.EndsWith("/"))
                return unixPath;

            return unixPath + "/";
        }
    }
}
