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

namespace AnalysisManagerMSFraggerPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSFragger plugin
    /// </summary>
    public class AnalysisResourcesMSFragger : AnalysisResources
    {
        // Ignore Spelling: Fragger, ParmFile, resourcer

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

                // Make sure the machine has enough free memory to run MSFragger
                // Setting MSFraggerJavaMemorySize is stored in the settings file for the job
                currentTask = "ValidateFreeMemorySize";
                if (!ValidateFreeMemorySize("MSFraggerJavaMemorySize", false))
                {
                    mMessage = "Not enough free memory to run MSFragger";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var workingDirectory = new DirectoryInfo(mWorkDir);

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // Require that the input files be mzML files (since PeptideProphet prefers them and TmtIntegrator requires them)
                // In contrast, MaxQuant can work with either .raw files or .mzML files
                const bool usingMzML = true;

                // Determine the transfer directory path

                // Caveats for usingMzML:
                //   When true, we want useInputDirectory to be true so that the resourcer will look for .mzML files in a MSXML_Gen directory
                //   When usingMzML is false, we want useInputDirectory to be false so that the resourcer will look for .Raw files in the dataset directory

                // Caveats for dataPackageID
                //   When 0, we are processing a single dataset, and we thus need to include the dataset name, generating a path like \\proto-4\DMS3_Xfer\QC_Dataset\MXQ202103151122_Auto1880613
                //   When positive, we are processing datasets in a data package, and we thus want a path without the dataset name, generating a path like \\proto-9\MaxQuant_Staging\MXQ202103161252_Auto1880833

                var useInputDirectory = usingMzML;
                var includeDatasetName = dataPackageID <= 0;

                var transferDirectoryPath = GetTransferDirectoryPathForJobStep(useInputDirectory, includeDatasetName);

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
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
                    AnalysisToolRunnerMSFragger.PROGRESS_PCT_STARTING,
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
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }

}
