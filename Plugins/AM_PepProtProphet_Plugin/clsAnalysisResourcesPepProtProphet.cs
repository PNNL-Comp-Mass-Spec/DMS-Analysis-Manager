//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;
using System.Collections.Generic;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Retrieve resources for the PepProtProphet plugin
    /// </summary>
    class clsAnalysisResourcesPepProtProphet : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools,
                                   clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
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

                // Retrieve Fasta file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);
                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                currentTask = "GetPepXMLFile";

                var pepXmlResultCode = GetPepXMLFile();

                if (pepXmlResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return pepXmlResultCode;
                }

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

        private CloseOutType GetPepXMLFile()
        {
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            string fileToRetrieve;
            bool unzipRequired;

            if (scriptName.IndexOf("MSFragger", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                fileToRetrieve = DatasetName + "_pepXML.zip";
                unzipRequired = true;
            }
            else
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToRetrieve, unzipRequired))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileToSkip(fileToRetrieve);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
