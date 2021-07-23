//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Retrieve resources for the PepProtProphet plugin
    /// </summary>
    public class AnalysisResourcesPepProtProphet : AnalysisResources
    {
        // Ignore Spelling: resourcer

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

                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var paramFilePath = Path.Combine(mWorkDir, paramFileName);

                var optionsLoaded = LoadMSFraggerOptions(paramFilePath, out var options);
                if (!optionsLoaded)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (options.OpenSearch)
                {
                    // Make sure the machine has enough free memory to run Crystal-C
                    currentTask = "ValidateFreeMemorySize";

                    if (!ValidateFreeMemorySizeGB("Crystal-C", AnalysisToolRunnerPepProtProphet.CRYSTALC_MEMORY_SIZE_GB))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                if (options.RunIonQuant)
                {
                    // Make sure the machine has enough free memory to run IonQuant
                    // Setting MSFraggerJavaMemorySize is stored in the settings file for the job
                    currentTask = "ValidateFreeMemorySize";

                    if (!ValidateFreeMemorySizeGB("Crystal-C", AnalysisToolRunnerPepProtProphet.CRYSTALC_MEMORY_SIZE_GB))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                currentTask = "Get DataPackageID";

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // Require that the input files be mzML files (since PeptideProphet prefers them and TmtIntegrator requires them)
                // In contrast, MaxQuant can work with either .raw files or .mzML files
                const bool usingMzML = true;

                currentTask = "RetrieveParamFile " + paramFileName;

                // Retrieve param file
                if (!FileSearch.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

                // Retrieve Fasta file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                var datasetFileRetriever = new DatasetFileRetriever(this);
                RegisterEvents(datasetFileRetriever);

                var datasetCopyResult = datasetFileRetriever.RetrieveInstrumentFilesForJobDatasets(
                    dataPackageID,
                    usingMzML,
                    AnalysisToolRunnerPepProtProphet.PROGRESS_PCT_INITIALIZING,
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

                currentTask = "GetPepXMLFiles";

                var pepXmlResultCode = GetPepXMLFiles(dataPackageInfo);

                if (pepXmlResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return pepXmlResultCode;
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
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType GetPepXMLFiles(DataPackageInfo dataPackageInfo)
        {
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            if (!scriptName.StartsWith("MSFragger", StringComparison.OrdinalIgnoreCase))
            {
                LogError("The PepProtProphet step tool is not compatible with pipeline script " + scriptName);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            foreach (var item in dataPackageInfo.Datasets)
            {
                var fileToRetrieve = item.Value + "_pepXML.zip";
                var unzipRequired = true;

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToRetrieve, unzipRequired))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <param name="paramFilePath"></param>
        /// <param name="options">Output: instance of the MSFragger options class</param>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool LoadMSFraggerOptions(string paramFilePath, out MSFraggerOptions options)
        {
            options = new MSFraggerOptions(mJobParams, null, 1);
            RegisterEvents(options);

            try
            {
                options.LoadMSFraggerOptions(paramFilePath);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        private bool ValidateFreeMemorySizeGB(string programName, int memoryRequiredGB)
        {
            if (ValidateFreeMemorySize(memoryRequiredGB * 1024, StepToolName, false))
            {
                return true;
            }

            mMessage = "Not enough free memory to run " + programName;
            return false;
        }
    }
}
