using System;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManagerInSpecTPlugIn
{
    /// <summary>
    /// Subclass for Inspect-specific tasks:
    /// 1) Distributes OrgDB files
    /// 2) Uses ParamFileGenerator to create param file from database instead of copying it
    /// 3) Retrieves zipped DTA files, unzips, and un-concatenates them
    /// </summary>
    public class clsAnalysisResourcesIN : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for performance of Inspect analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve Fasta file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve the _DTA.txt file
            if (!RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip");  // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt");  // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");      // DTA files

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
        /// </summary>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks></remarks>
        public bool RetrieveDtaFiles()
        {
            // Retrieve zipped DTA file
            string dtaResultFileName;
            var isParallelized = false;

            var cloneStepRenum = mJobParams.GetParam("CloneStepRenumberStart");
            var stepNum = mJobParams.GetParam("Step");

            // Determine if this is parallelized inspect job
            if (string.IsNullOrEmpty(cloneStepRenum))
            {
                dtaResultFileName = DatasetName + "_dta.zip";
            }
            else
            {
                var parallelZipNum = Convert.ToInt32(stepNum) - Convert.ToInt32(cloneStepRenum) + 1;
                dtaResultFileName = DatasetName + "_" + Convert.ToString(parallelZipNum) + "_dta.txt";
                isParallelized = true;
                LogMessage("Processing parallelized Inspect segment " + parallelZipNum);
            }

            var DtaResultFolderName = FileSearch.FindDataFile(dtaResultFileName);

            if (string.IsNullOrEmpty(DtaResultFolderName))
            {
                // No folder found containing the zipped DTA files (error will have already been logged)
                if (mDebugLevel >= 3)
                {
                    LogError("FindDataFile returned False for " + dtaResultFileName);
                }
                return false;
            }

            if (DtaResultFolderName.StartsWith(MYEMSL_PATH_FLAG))
            {
                if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Downloaded " + mMyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL");
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // Copy the file
                if (!CopyFileToWorkDir(dtaResultFileName, DtaResultFolderName, mWorkDir))
                {
                    // Error copying file (error will have already been logged)
                    if (mDebugLevel >= 3)
                    {
                        LogError("CopyFileToWorkDir returned False for " + dtaResultFileName + " using directory " + DtaResultFolderName);
                    }
                    return false;
                }
            }

            // Check to see if the job is parallelized
            //  If it is parallelized, we do not need to unzip the concatenated DTA file (since it is already unzipped)
            //  If not parallelized, we do need to unzip
            if (!isParallelized || Path.GetExtension(dtaResultFileName).ToLower() == ".zip")
            {
                // Unzip concatenated DTA file
                LogMessage("Unzipping concatenated DTA file");
                if (UnzipFileStart(Path.Combine(mWorkDir, dtaResultFileName), mWorkDir, "clsAnalysisResourcesIN.RetrieveDtaFiles", false))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Concatenated DTA file unzipped");
                    }
                }
            }

            return true;
        }

    }
}
