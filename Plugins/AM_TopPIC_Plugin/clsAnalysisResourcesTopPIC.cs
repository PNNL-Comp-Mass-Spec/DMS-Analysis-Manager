//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System.Collections.Generic;
using AnalysisManagerBase;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopPIC plugin
    /// </summary>
    public class clsAnalysisResourcesTopPIC : clsAnalysisResources
    {

        /// <summary>
        /// .feature file created by TopFD
        /// </summary>
        /// <remarks>Tracks LC/MS features</remarks>
        public const string TOPFD_FEATURE_FILE_SUFFIX = ".feature";

        /// <summary>
        /// _ms2.msalign file created by TopFD
        /// </summary>
        public const string MSALIGN_FILE_SUFFIX = "_ms2.msalign";

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

            // Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve Fasta file
            var orgDbDirectoryPath = m_mgrParams.GetParam("OrgDBDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            LogMessage("Getting data files");

            var filesToRetrieve = new List<string> {
                DatasetName + TOPFD_FEATURE_FILE_SUFFIX,
                DatasetName + MSALIGN_FILE_SUFFIX
            };

            foreach (var fileToRetrieve in filesToRetrieve)
            {
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToRetrieve, false))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
                m_jobParams.AddResultFileToSkip(fileToRetrieve);
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
