//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMSFraggerPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSFragger plugin
    /// </summary>
    public class AnalysisResourcesMSFragger : AnalysisResources
    {
        // Ignore Spelling: centroided, Fragger, numpy, ParmFile, resourcer, Xmx

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

                // Make sure the machine has enough free memory to run MSFragger, based on the JavaMemorySize job parameter
                // Setting MSFraggerJavaMemorySize is stored in the settings file for this job

                currentTask = "ValidateFreeMemorySize";
                if (!ValidateFreeMemorySize("MSFraggerJavaMemorySize", false))
                {
                    mMessage = "Not enough free memory to run MSFragger";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // Require that the input files be mzML files (since PeptideProphet prefers them and TmtIntegrator requires them)
                // Furthermore, the .mzML files need to have centroided MS2 spectra
                // In contrast, MaxQuant can work with either .raw files or .mzML files
                const bool retrieveMzML = true;

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveParamFile " + paramFileName;

                // Retrieve param file
                if (!FileSearchTool.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

                var databaseSplitCount = mJobParams.GetJobParameter("MSFragger", "DatabaseSplitCount", 1);

                if (databaseSplitCount > 1 && !VerifyPythonAvailable())
                    return CloseOutType.CLOSEOUT_FAILED;

                // Retrieve FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                const int maxLegacyFASTASizeGB = 100;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode, maxLegacyFASTASizeGB, out var fastaFileSizeGB))
                    return resultCode;

                // Possibly require additional system memory, based on the size of the FASTA file
                // However, when FASTA file splitting is enabled, use the memory size defined by the settings file
                var javaMemoryCheckResultCode = ValidateJavaMemorySize(fastaFileSizeGB * 1024, databaseSplitCount);
                if (javaMemoryCheckResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    return javaMemoryCheckResultCode;

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    retrieveMzML,
                    AnalysisToolRunnerMSFragger.PROGRESS_PCT_INITIALIZING,
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

        /// <summary>
        /// Get the amount of memory (in MB) to reserve for Java when running MSFragger
        /// </summary>
        /// <remarks>
        /// Larger FASTA files need more memory
        /// 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods
        /// </remarks>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="fastaFileSizeMB">FASTA file size, in MB</param>
        /// <param name="msFraggerJavaMemorySizeMB">JavaMemorySize job parameter value</param>
        /// <returns>Memory size (in MB) to use with Java argument -Xmx</returns>
        public static int GetJavaMemorySizeToUse(IJobParams jobParams, double fastaFileSizeMB, out int msFraggerJavaMemorySizeMB)
        {
            // This formula is an estimate and may need to be updated in the future
            var recommendedMemorySizeMB = (int)(fastaFileSizeMB * 0.5 + 10) * 1024;

            // Setting MSFraggerJavaMemorySize is stored in the settings file for this job
            msFraggerJavaMemorySizeMB = Math.Max(2000, jobParams.GetJobParameter("MSFraggerJavaMemorySize", 10000));

            return recommendedMemorySizeMB > msFraggerJavaMemorySizeMB ? recommendedMemorySizeMB : msFraggerJavaMemorySizeMB;
        }

        /// <summary>
        /// Get the amount of memory (in MB) to reserve for Java when running MSFragger
        /// </summary>
        /// <remarks>
        /// Larger FASTA files need more memory
        /// 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods
        /// </remarks>
        /// <param name="fastaFileSizeMB"></param>
        /// <param name="databaseSplitCount"></param>
        public CloseOutType ValidateJavaMemorySize(double fastaFileSizeMB, int databaseSplitCount)
        {
            var recommendedMemorySizeMB = GetJavaMemorySizeToUse(mJobParams, fastaFileSizeMB, out var msFraggerJavaMemorySizeMB);

            if (recommendedMemorySizeMB < msFraggerJavaMemorySizeMB || databaseSplitCount > 1)
            {
                if (ValidateFreeMemorySize(msFraggerJavaMemorySizeMB, StepToolName, true))
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                mMessage = string.Format(
                    "Not enough free memory to run MSFragger; need {0:N0} MB, as defined by the settings file",
                    msFraggerJavaMemorySizeMB);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            var validFreeMemory = ValidateFreeMemorySize(recommendedMemorySizeMB, StepToolName, true);

            if (!validFreeMemory)
            {
                mMessage = string.Format(
                    "Not enough free memory to run MSFragger; need {0:N0} MB due to a {1:N0} MB FASTA file",
                    recommendedMemorySizeMB, fastaFileSizeMB);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage(string.Format(
                "Increasing the memory allocated to Java from {0:N0} MB to {1:N0} MB, due to a {2:N0} MB FASTA file",
                msFraggerJavaMemorySizeMB, recommendedMemorySizeMB, fastaFileSizeMB));

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool VerifyPythonAvailable()
        {
            try
            {
                if (!FragPipeLibFinder.PythonInstalled)
                {
                    LogError("Could not find Python 3.x; cannot run MSFragger with the split database option");
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
