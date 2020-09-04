using AnalysisManagerBase;
using System;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// Retrieve resources for the AScore plugin
    /// </summary>
    public class clsAnalysisResourcesAScore : clsAnalysisResources
    {
        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS) {
                return result;
            }

            // WARNING: AScore accesses the files over the network, retrieving the files using Mage
            //          If the files have been purged, they may not be accessible
            //          (Mage supports retrieving files from Aurora or MyEmsl,
            //           but this has not be tested as of July 16, 2014)
            //
            // bool blnSuccess = true;
            //  blnSuccess = RunAScoreGetResources();
            //
            // if (!blnSuccess) return CloseOutType.CLOSEOUT_FAILED;

            if (mDebugLevel > 2)
            {
                LogMessage("No input files to retrieve; AScore accesses the files over the network");
            }

            if (!RetrieveFastaFile(out _))
            {
                LogWarning("Unable to retrieve the fasta file; AScore results will not have protein information");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve the fasta file (if defined)
        /// </summary>
        /// <returns></returns>
        private bool RetrieveFastaFile(out CloseOutType resultCode)
        {
            var currentTask = "Initializing";


            try
            {
                // Retrieve the Fasta file
                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                var success = RetrieveOrgDB(orgDbDirectoryPath, out resultCode);

                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveFastaAndParamFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask, ex);
                resultCode = CloseOutType.CLOSEOUT_FAILED;
                return false;
            }
        }


        ///// <summary>
        ///// run the AScore pipeline(s) listed in "AScoreOperations" parameter
        ///// </summary>
        //protected bool RunAScoreGetResources()
        //{
        //    bool blnSuccess = false;

        //    string ascoreOperations = mJobParams.GetParam("AScoreOperations");

        //    if (string.IsNullOrWhiteSpace(ascoreOperations)) {
        //        mMessage = "AScoreOperations parameter is not defined";
        //        return false;
        //    }

        //    foreach (string ascoreOperation in ascoreOperations.Split(','))
        //    {
        //        if (!string.IsNullOrWhiteSpace(ascoreOperation)) {
        //            blnSuccess = RunAScoreOperation(ascoreOperation.Trim());
        //            if (!blnSuccess) {
        //                mMessage = "Error running AScore resources operation " + ascoreOperation;
        //                break;
        //            }
        //        }
        //    }

        //    return blnSuccess;

        //}

        ///// <summary>
        ///// Run a single AScore operation
        ///// </summary>
        ///// <param name="ascoreOperation"></param>
        ///// <returns></returns>
        //private bool RunAScoreOperation(string ascoreOperation)
        //{
        //    bool blnSuccess;

        //    // Note: case statements must be lowercase
        //    switch (ascoreOperation.ToLower())
        //    {
        //        case "runascorephospho":
        //            blnSuccess = GetAScoreFiles();
        //            break;
        //        default:
        //            throw new ArgumentException("Unrecognized value for ascoreOperation: " + ascoreOperation);
        //    }

        //    if (!ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
        //        return false;

        //    return blnSuccess;
        //}


        //#region AScore Operations

        //private bool GetAScoreFiles()
        //{
        //    const bool blnSuccess = true;

        //    //Add list the files to delete to global list
        //    var fileSpecList = mJobParams.GetParam("TargetJobFileList").Split(',').ToList();
        //    foreach (string fileSpec in fileSpecList)
        //    {
        //        var fileSpecTerms = fileSpec.Split(':').ToList();

        //        if (fileSpecTerms.Count <= 2 || fileSpecTerms[2].ToLower() != "copy")
        //        {
        //            mJobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
        //        }
        //    }

        //    LogMessage("Getting AScoreCIDParamFile param file");

        //    if (!string.IsNullOrEmpty(mJobParams.GetParam("AScoreCIDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(mJobParams.GetParam("AScoreCIDParamFile"), mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH)))
        //        {
        //            return false;
        //        }
        //    }

        //    LogMessage("Getting AScoreETDParamFile param file");

        //    if (!string.IsNullOrEmpty(mJobParams.GetParam("AScoreETDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(mJobParams.GetParam("AScoreETDParamFile"), mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH)))
        //        {
        //            return false;
        //        }
        //    }

        //    LogMessage("Getting AScoreHCDParamFile param file");

        //    if (!string.IsNullOrEmpty(mJobParams.GetParam("AScoreHCDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(mJobParams.GetParam("AScoreHCDParamFile"), mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH)))
        //        {
        //            return false;
        //        }
        //    }

        //    LogMessage("Getting AScoreHCDParamFile param file");

        //    {
        //        Dictionary<int, udtDataPackageJobInfoType> dctDataPackageJobs;
        //        if (!RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Ascore, out dctDataPackageJobs))
        //        {
        //            //Errors were reported in function call, so just return
        //            return false;
        //        }
        //    }

        //    return blnSuccess;
        //}

        //protected string GetDatasetID(string DatasetName)
        //{
        //    int DatasetID;

        //    if ( mJobParams.DatasetInfoList.TryGetValue(DatasetName, out DatasetID) )
        //        return DatasetID.ToString(CultureInfo.InvariantCulture);

        //    return string.Empty;
        //}

        //#endregion



    }
}
