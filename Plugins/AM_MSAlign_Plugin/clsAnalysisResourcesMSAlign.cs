//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerMSAlignPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSAlign plugin
    /// </summary>
    public class clsAnalysisResourcesMSAlign : clsAnalysisResources
    {
        /// <summary>
        /// MSDeconv .msalign file suffix
        /// </summary>
        public const string MSDECONV_MSALIGN_FILE_SUFFIX = "_msdeconv.msalign";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
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
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Make sure the machine has enough free memory to run MSAlign
            if (!ValidateFreeMemorySize("MSAlignJavaMemorySize"))
            {
                mMessage = "Not enough free memory to run MSAlign";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve param file
            if (!FileSearch.RetrieveFile(mJobParams.GetParam("ParmFileName"), mJobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve Fasta file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve the MSAlign file
            LogMessage("Getting data files");
            var fileToGet = DatasetName + MSDECONV_MSALIGN_FILE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Errors were reported in function call, so just return
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
