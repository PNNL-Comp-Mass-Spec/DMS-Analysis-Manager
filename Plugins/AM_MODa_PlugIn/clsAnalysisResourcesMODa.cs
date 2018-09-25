//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/26/2014
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMODaPlugIn
{
    /// <summary>
    /// Retrieve resources for the MODa plugin
    /// </summary>
    public class clsAnalysisResourcesMODa : clsAnalysisResources
    {
        private DTAtoMGF.clsDTAtoMGF mDTAtoMGF;

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

            // Make sure the machine has enough free memory to run MODa
            if (!ValidateFreeMemorySize("MODaJavaMemorySize"))
            {
                m_message = "Not enough free memory to run MODa";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

            // Retrieve Fasta file
            var orgDbDirectoryPath = m_mgrParams.GetParam("OrgDBDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (!FileSearch.RetrieveDtaFiles())
            {
                var sharedResultsFolders = m_jobParams.GetParam(JOB_PARAM_SHARED_RESULTS_FOLDERS);
                if (string.IsNullOrEmpty(sharedResultsFolders))
                {
                    m_message = clsGlobal.AppendToComment(m_message, "Job parameter SharedResultsFolders is empty");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (sharedResultsFolders.Contains(","))
                {
                    m_message = clsGlobal.AppendToComment(m_message, "shared results folders: " + sharedResultsFolders);
                }
                else
                {
                    m_message = clsGlobal.AppendToComment(m_message, "shared results folder " + sharedResultsFolders);
                }

                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(m_WorkingDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Remove any spectra from the _DTA.txt file with fewer than 3 ions
            if (!ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in function call, so just return
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
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ConvertCDTAToMGF()
        {
            try
            {
                mDTAtoMGF = new DTAtoMGF.clsDTAtoMGF
                {
                    Combine2And3PlusCharges = false,
                    FilterSpectra = false,
                    MaximumIonsPer100MzInterval = 0,
                    NoMerge = true,
                    CreateIndexFile = true
                };

                // Convert the _dta.txt file for this dataset
                var cdtaFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + "_dta.txt"));

                if (!cdtaFile.Exists)
                {
                    m_message = "_dta.txt file not found; cannot convert to .mgf";
                    LogError(m_message + ": " + cdtaFile.FullName);
                    return false;
                }

                if (!mDTAtoMGF.ProcessFile(cdtaFile.FullName))
                {
                    m_message = "Error converting " + cdtaFile.Name + " to a .mgf file";
                    LogError(m_message + ": " + mDTAtoMGF.GetErrorMessage());
                    return false;
                }

                // Delete the _dta.txt file
                try
                {
                    cdtaFile.Delete();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                PRISM.ProgRunner.GarbageCollectNow();

                var newMGFFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + ".mgf"));

                if (!newMGFFile.Exists)
                {
                    // MGF file was not created
                    m_message = "A .mgf file was not created using the _dta.txt file; unable to run MODa";
                    LogError(m_message + ": " + mDTAtoMGF.GetErrorMessage());
                    return false;
                }

                m_jobParams.AddResultFileExtensionToSkip(".mgf");
            }
            catch (Exception ex)
            {
                m_message = "Exception in ConvertCDTAToMGF";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }
    }
}
