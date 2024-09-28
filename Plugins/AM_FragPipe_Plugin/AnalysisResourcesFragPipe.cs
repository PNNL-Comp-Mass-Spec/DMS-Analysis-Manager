//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISM;

namespace AnalysisManagerFragPipePlugIn
{
    /// <summary>
    /// Retrieve resources for the FragPipe plugin
    /// </summary>
    public class AnalysisResourcesFragPipe : AnalysisResources
    {
        // Ignore Spelling: centroided, FASTA, Frag, numpy, resourcer

        internal const string DATABASE_SPLIT_COUNT_SECTION = "FragPipe";
        internal const string DATABASE_SPLIT_COUNT_PARAM = "DatabaseSplitCount";

        internal const string JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID = "PackedParam_ExperimentsByDatasetID";

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

                // Python 3 is required to run FragPipe
                if (!VerifyPythonAvailable())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure the machine has enough free memory to run FragPipe, based on the FragPipe memory size job parameter
                // Setting FragPipeMemorySizeMB is stored in the settings file for this job

                currentTask = "ValidateFreeMemorySize";

                if (!ValidateFreeMemorySize("FragPipeMemorySizeMB", false))
                {
                    mInsufficientFreeMemory = true;
                    mMessage = "Not enough free memory to run FragPipe";
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
                }

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // Require that the input files be mzML files (since PeptideProphet prefers them and TmtIntegrator requires them)
                // Furthermore, the .mzML files need to have centroided MS2 spectra
                // In contrast, MaxQuant can work with either .raw files, .mzML files, or .mzXML files
                const bool retrieveMsXmlFiles = true;

                LogMessage("Getting param file", 2);

                // Retrieve the workflow file (aka the parameter file)

                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag, MaxQuant_Mod_Name, UniMod_Mod_Name
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var workflowFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveGeneratedParamFile " + workflowFileName;

                if (!RetrieveGeneratedParamFile(workflowFileName))
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var databaseSplitCount = mJobParams.GetJobParameter(DATABASE_SPLIT_COUNT_SECTION, DATABASE_SPLIT_COUNT_PARAM, 1);

                var workflowFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName));

                currentTask = "Get DataPackageID";

                // Check whether this job is associated with a data package; if it is, count the number of datasets
                // Cache the experiment names in a packed job parameter
                var datasetCount = GetDatasetCountAndCacheExperimentNames();

                var options = new FragPipeOptions(mJobParams, datasetCount);
                RegisterEvents(options);

                if (!options.ValidateFragPipeOptions(workflowFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                const int maxLegacyFASTASizeGB = 100;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode, maxLegacyFASTASizeGB, out var fastaFileSizeGB))
                    return resultCode;

                // Abort the job if a split FASTA search is enabled and the FASTA file is less than 0.1 MB (which is around 250 proteins)
                // The user probably chose the wrong settings file

                var fastaFileSizeMB = fastaFileSizeGB * 1024;

                if (databaseSplitCount > 1 && fastaFileSizeMB < 0.1)
                {
                    LogError(
                        "FASTA file is too small to be used in a split FASTA search ({0:F0} KB); update the job to use a different settings file",
                        fastaFileSizeMB * 1024.0);

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Parse the FragPipe workflow file so that GetDynamicModResidueCount() will be able to consider the number of residues with a dynamic mod
                options.LoadFragPipeOptions(workflowFile.FullName);

                var dynamicModCount = options.GetDynamicModResidueCount();

                // Possibly require additional system memory, based on the size of the FASTA file
                // However, when FASTA file splitting is enabled, use the memory size defined by the settings file
                var memoryCheckResultCode = ValidateFragPipeMemorySize(fastaFileSizeGB * 1024, dynamicModCount, databaseSplitCount);

                if (memoryCheckResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    return memoryCheckResultCode;

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMsXmlFiles,
                    AnalysisToolRunnerFragPipe.PROGRESS_PCT_INITIALIZING,
                    false,
                    out var dataPackageInfo,
                    out _);

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

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private int GetDatasetCountAndCacheExperimentNames()
        {
            var dataPackageDefined = LoadDataPackageDatasetInfo(out var dataPackageDatasets, out _, true);

            // Populate a SortedSet with the experiments in the data package (or, if no data package, this job's experiment)
            var experimentNames = new SortedSet<string>();

            if (dataPackageDatasets.Count == 0)
            {
                var experiment = mJobParams.GetJobParameter("Experiment", string.Empty);

                if (experiment.Length == 0)
                {
                    LogWarning("Job parameter 'Experiment' is not defined");
                }

                experimentNames.Add(experiment);
            }
            else
            {
                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var item in dataPackageDatasets)
                {
                    // Add the experiment name if not yet present
                    experimentNames.Add(item.Value.Experiment);
                }
            }

            // Store the list of experiment in a packed job parameter
            StorePackedJobParameterList(experimentNames, JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID);

            return dataPackageDefined ? dataPackageDatasets.Count : 1;
        }

        /// <summary>
        /// Get the maximum amount of memory (in GB) that FragPipe is allowed to use
        /// </summary>
        /// <remarks>
        /// Larger FASTA files need more memory
        /// 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods
        /// </remarks>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="fastaFileSizeMB">FASTA file size, in MB</param>
        /// <param name="dynamicModCount">Number of dynamic mods</param>
        /// <param name="fragPipeMemorySizeMB">FragPipeMemorySizeMB job parameter value</param>
        /// <returns>Memory size (in GB) to use with FragPipe argument --ram</returns>
        public static int GetFragPipeMemorySizeToUse(IJobParams jobParams, double fastaFileSizeMB, int dynamicModCount, out int fragPipeMemorySizeMB)
        {
            // This formula is based on FASTA file size and the number of dynamic mods
            // An additional 5000 MB of memory is reserved for each dynamic mod above 2 dynamic mods

            // Example values:

            // FASTA File Size (MB)   Dynamic Mods   Recommended Memory Size (GB)
            // 25                     2              22
            // 25                     3              27
            // 25                     4              32
            // 25                     5              37

            // 50                     2              35
            // 50                     3              40
            // 50                     4              45
            // 50                     5              50

            // 100                    2              60
            // 100                    3              65
            // 100                    4              70
            // 100                    5              75

            var recommendedMemorySizeMB = (int)(fastaFileSizeMB * 0.5 + 10) * 1024 + (Math.Max(2, dynamicModCount) - 2) * 5000;

            // Setting FragPipeMemorySizeMB is stored in the settings file for this job
            fragPipeMemorySizeMB = Math.Max(2000, jobParams.GetJobParameter("FragPipeMemorySizeMB", 10000));

            var fragPipeMemorySizeGB = recommendedMemorySizeMB > fragPipeMemorySizeMB
                ? recommendedMemorySizeMB / 1024.0
                : fragPipeMemorySizeMB / 1024.0;

            return (int)Math.Ceiling(fragPipeMemorySizeGB);
        }

        /// <summary>
        /// Verify that the system has enough free memory to run FragPipe
        /// </summary>
        /// <remarks>
        /// Larger FASTA files need more memory
        /// 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods
        /// </remarks>
        /// <param name="fastaFileSizeMB">FASTA file size, in MB</param>
        /// <param name="dynamicModCount">Number of dynamic mods</param>
        /// <param name="databaseSplitCount"></param>
        public CloseOutType ValidateFragPipeMemorySize(double fastaFileSizeMB, int dynamicModCount, int databaseSplitCount)
        {
            var recommendedMemorySizeGB = GetFragPipeMemorySizeToUse(mJobParams, fastaFileSizeMB, dynamicModCount, out var fragPipeMemorySizeMB);

            if (recommendedMemorySizeGB * 1024 < fragPipeMemorySizeMB || databaseSplitCount > 1)
            {
                if (ValidateFreeMemorySize(fragPipeMemorySizeMB, StepToolName, true))
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                mMessage = string.Format(
                    "Not enough free memory to run FragPipe; need {0:N0} MB, as defined by the settings file",
                    fragPipeMemorySizeMB);

                mInsufficientFreeMemory = true;
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
            }

            var validFreeMemory = ValidateFreeMemorySize(recommendedMemorySizeGB * 1024, StepToolName, true);

            var dynamicModCountDescription = FragPipeOptions.GetDynamicModCountDescription(dynamicModCount);

            if (!validFreeMemory)
            {
                mMessage = string.Format(
                    "Not enough free memory to run FragPipe; need {0:N0} GB due to a {1:N0} MB FASTA file and {2}",
                    recommendedMemorySizeGB, fastaFileSizeMB, dynamicModCountDescription);

                if (Global.RunningOnDeveloperComputer())
                {
                    ConsoleMsgUtils.ShowWarning(mMessage);
                    ConsoleMsgUtils.ShowWarning("However, running on development machine, so will ignore this error");
                    ConsoleMsgUtils.SleepSeconds(2);

                    mMessage = string.Empty;
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                mInsufficientFreeMemory = true;
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
            }

            if (recommendedMemorySizeGB * 1024 > fragPipeMemorySizeMB)
            {
                LogMessage("Increasing the memory allocated to FragPipe from {0:N0} GB to {1:N0} GB, due to a {2:N0} MB FASTA file and {3}",
                    (int)Math.Floor(fragPipeMemorySizeMB / 1024.0), recommendedMemorySizeGB, fastaFileSizeMB, dynamicModCountDescription);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool VerifyPythonAvailable()
        {
            try
            {
                if (!FragPipeLibFinder.PythonInstalled)
                {
                    LogError("Could not find Python 3.x; cannot run FragPipe");
                    return false;
                }

                // Confirm that the required Python packages are installed
                if (!FragPipeLibFinder.PythonPackageInstalled("numpy", out var errorMessage1))
                {
                    LogError(errorMessage1);
                    return false;
                }

                if (!FragPipeLibFinder.PythonPackageInstalled("pandas", out var errorMessage2))
                {
                    LogError(errorMessage2);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in VerifyPythonAvailable", ex);
                return false;
            }
        }
    }
}
