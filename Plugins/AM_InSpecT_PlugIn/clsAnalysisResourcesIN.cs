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
        #region "Methods"

        public override void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(mgrParams, jobParams, statusTools, myEMSLUtilities);
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

            //Retrieve Fasta file
            if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                return CloseOutType.CLOSEOUT_FAILED;

            //Retrieve param file
            if (!RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the _DTA.txt file
            if (!RetrieveDtaFiles())
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");  //Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");  //Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");      //DTA files

            //All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
        /// </summary>
        /// <returns>TRUE for success, FALSE for error</returns>
        /// <remarks></remarks>
        public bool RetrieveDtaFiles()
        {
            //Retrieve zipped DTA file
            string DtaResultFileName = null;

            string CloneStepRenum = null;
            string stepNum = null;
            int parallelZipNum = 0;
            bool isParallelized = false;

            CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart");
            stepNum = m_jobParams.GetParam("Step");

            //Determine if this is parallelized inspect job
            if (string.IsNullOrEmpty(CloneStepRenum))
            {
                DtaResultFileName = DatasetName + "_dta.zip";
            }
            else
            {
                parallelZipNum = Convert.ToInt32(stepNum) - Convert.ToInt32(CloneStepRenum) + 1;
                DtaResultFileName = DatasetName + "_" + Convert.ToString(parallelZipNum) + "_dta.txt";
                isParallelized = true;
                LogMessage(
                    "Processing parallelized Inspect segment " + parallelZipNum.ToString());
            }

            string DtaResultFolderName = FileSearch.FindDataFile(DtaResultFileName);

            if (string.IsNullOrEmpty(DtaResultFolderName))
            {
                // No folder found containing the zipped DTA files (error will have already been logged)
                if (m_DebugLevel >= 3)
                {
                    LogError(
                        "FindDataFile returned False for " + DtaResultFileName);
                }
                return false;
            }

            if (DtaResultFolderName.StartsWith(MYEMSL_PATH_FLAG))
            {
                if (m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogMessage(
                            "Downloaded " + m_MyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL");
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                //Copy the file
                if (!CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, m_WorkingDir))
                {
                    // Error copying file (error will have already been logged)
                    if (m_DebugLevel >= 3)
                    {
                        LogError(
                            "CopyFileToWorkDir returned False for " + DtaResultFileName + " using folder " + DtaResultFolderName);
                    }
                    return false;
                }
            }

            // Check to see if the job is parallelized
            //  If it is parallelized, we do not need to unzip the concatenated DTA file (since it is already unzipped)
            //  If not parallelized, then we do need to unzip
            if (!isParallelized || Path.GetExtension(DtaResultFileName).ToLower() == ".zip")
            {
                //Unzip concatenated DTA file
                LogMessage("Unzipping concatenated DTA file");
                if (UnzipFileStart(Path.Combine(m_WorkingDir, DtaResultFileName), m_WorkingDir, "clsAnalysisResourcesIN.RetrieveDtaFiles", false))
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Concatenated DTA file unzipped");
                    }
                }
            }

            return true;
        }

        #endregion
    }
}
