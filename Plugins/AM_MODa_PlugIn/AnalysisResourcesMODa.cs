//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/26/2014
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMODaPlugIn
{
    /// <summary>
    /// Retrieve resources for the MODa plugin
    /// </summary>
    public class AnalysisResourcesMODa : AnalysisResources
    {
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

            // Make sure the machine has enough free memory to run MODa
            if (!ValidateFreeMemorySize("MODaJavaMemorySize"))
            {
                mInsufficientFreeMemory = true;
                mMessage = "Not enough free memory to run MODa";
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
            }

            // Retrieve param file
            if (!FileSearchTool.RetrieveFile(mJobParams.GetParam("ParamFileName"), mJobParams.GetParam("ParamFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve the FASTA file
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (!FileSearchTool.RetrieveDtaFiles())
            {
                var sharedResultsFolders = mJobParams.GetParam(JOB_PARAM_SHARED_RESULTS_FOLDERS);

                if (string.IsNullOrEmpty(sharedResultsFolders))
                {
                    mMessage = Global.AppendToComment(mMessage, "Job parameter SharedResultsFolders is empty");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (sharedResultsFolders.Contains(","))
                {
                    mMessage = Global.AppendToComment(mMessage, "shared results folders: " + sharedResultsFolders);
                }
                else
                {
                    mMessage = Global.AppendToComment(mMessage, "shared results folder " + sharedResultsFolders);
                }

                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(mWorkDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Remove any spectra from the _DTA.txt file with fewer than 3 ions
            if (!ValidateCDTAFileRemoveSparseSpectra(mWorkDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Convert the _dta.txt file to a mgf file
            if (!ConvertCDTAToMGF())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Convert the _dta.txt file to a .mgf file
        /// </summary>
        /// <returns>True on success, false if an error</returns>
        private bool ConvertCDTAToMGF()
        {
            try
            {
                var cdtaUtilities = new CDTAUtilities();
                RegisterEvents(cdtaUtilities);

                const bool combine2And3PlusCharges = false;
                const int maximumIonsPer100MzInterval = 0;
                const bool createIndexFile = true;

                // Convert the _dta.txt file for this dataset
                var cdtaFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + CDTA_EXTENSION));

                var success = cdtaUtilities.ConvertCDTAToMGF(cdtaFile, DatasetName, combine2And3PlusCharges, maximumIonsPer100MzInterval, createIndexFile);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("ConvertCDTAToMGF reports false");
                    }
                    return false;
                }

                // Delete the _dta.txt file
                try
                {
                    cdtaFile.Delete();
                }
                catch (Exception ex)
                {
                    LogWarning("Unable to delete the _dta.txt file after successfully converting it to .mgf: " + ex.Message);
                }

                mJobParams.AddResultFileExtensionToSkip(".mgf");

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ConvertCDTAToMGF";
                LogError(mMessage, ex);
                return false;
            }
        }
    }
}
