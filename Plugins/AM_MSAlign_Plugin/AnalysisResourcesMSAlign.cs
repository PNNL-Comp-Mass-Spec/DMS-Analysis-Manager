//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMSAlignPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSAlign plugin
    /// </summary>
    public class AnalysisResourcesMSAlign : AnalysisResources
    {
        /// <summary>
        /// MSDeconv .msalign file suffix
        /// </summary>
        public const string MSDECONV_MSALIGN_FILE_SUFFIX = "_msdeconv.msalign";

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
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Make sure the machine has enough free memory to run MSAlign
            if (!ValidateFreeMemorySize("MSAlignJavaMemorySize"))
            {
                mInsufficientFreeMemory = true;
                mMessage = "Not enough free memory to run MSAlign";
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY;
            }

            // Retrieve param file
            if (!FileSearchTool.RetrieveFile(mJobParams.GetParam("ParamFileName"), mJobParams.GetParam("ParamFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve FASTA file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve the MSAlign file
            LogMessage("Getting data files");
            var fileToGet = DatasetName + MSDECONV_MSALIGN_FILE_SUFFIX;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
