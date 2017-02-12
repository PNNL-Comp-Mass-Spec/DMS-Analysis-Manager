using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManager_AScore_PlugIn
{
    public class clsAnalysisResourcesAScore : clsAnalysisResources
    {

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

            if (m_DebugLevel > 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "No input files to retrieve; AScore accesses the files over the network");
            }

            if (!RetrieveFastaFile())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to retrieve the fasta file; AScore results will not have protein information");		        
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve the fasta file (if defined)
        /// </summary>
        /// <returns></returns>
        private bool RetrieveFastaFile()
        {

            string currentTask = "Initializing";


            try
            {
                // Retrieve the Fasta file
                var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");

                currentTask = "RetrieveOrgDB to " + localOrgDbFolder;

                var success = RetrieveOrgDB(localOrgDbFolder);
                
                return success;

            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveFastaAndParamFile: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return false;
            }

        }


        ///// <summary>
        ///// run the AScore pipeline(s) listed in "AScoreOperations" parameter
        ///// </summary>        
        //protected bool RunAScoreGetResources()
        //{
        //    bool blnSuccess = false;

        //    string ascoreOperations = m_jobParams.GetParam("AScoreOperations");

        //    if (string.IsNullOrWhiteSpace(ascoreOperations)) {
        //        m_message = "AScoreOperations parameter is not defined";
        //        return false;
        //    }

        //    foreach (string ascoreOperation in ascoreOperations.Split(','))
        //    {
        //        if (!string.IsNullOrWhiteSpace(ascoreOperation)) {
        //            blnSuccess = RunAScoreOperation(ascoreOperation.Trim());
        //            if (!blnSuccess) {
        //                m_message = "Error running AScore resources operation " + ascoreOperation;
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

        //    if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
        //        return false;
            
        //    return blnSuccess;
        //}


        //#region AScore Operations

        //private bool GetAScoreFiles()
        //{
        //    const bool blnSuccess = true;

        //    //Add list the files to delete to global list
        //    var fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(',').ToList();
        //    foreach (string fileSpec in fileSpecList)
        //    {
        //        var fileSpecTerms = fileSpec.Split(':').ToList();

        //        if (fileSpecTerms.Count <= 2 || fileSpecTerms[2].ToLower() != "copy")
        //        {
        //            m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);               					     
        //        }
        //    }

        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreCIDParamFile param file");

        //    if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreCIDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(m_jobParams.GetParam("AScoreCIDParamFile"), m_jobParams.GetParam("transferFolderPath")))
        //        {
        //            return false;
        //        }
        //    }

        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreETDParamFile param file");

        //    if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreETDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(m_jobParams.GetParam("AScoreETDParamFile"), m_jobParams.GetParam("transferFolderPath")))
        //        {
        //            return false;
        //        }
        //    }

        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreHCDParamFile param file");

        //    if (!string.IsNullOrEmpty(m_jobParams.GetParam("AScoreHCDParamFile")))
        //    {
        //        if (!FileSearch.RetrieveFile(m_jobParams.GetParam("AScoreHCDParamFile"), m_jobParams.GetParam("transferFolderPath")))
        //        {
        //            return false;
        //        }
        //    }

        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting AScoreHCDParamFile param file");

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
                
        //    if ( m_jobParams.DatasetInfoList.TryGetValue(DatasetName, out DatasetID) )
        //        return DatasetID.ToString(CultureInfo.InvariantCulture);
            
        //    return string.Empty;
        //}

        //#endregion



    }
}
