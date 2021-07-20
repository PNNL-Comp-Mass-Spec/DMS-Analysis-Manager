//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
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
    }
}
