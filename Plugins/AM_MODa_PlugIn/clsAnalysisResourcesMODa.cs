//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/26/2014
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMODaPlugIn
{
    public class clsAnalysisResourcesMODa : clsAnalysisResources
    {
        protected DTAtoMGF.clsDTAtoMGF mDTAtoMGF;

        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

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
                return CloseOutType.CLOSEOUT_FAILED;

            // Retrieve Fasta file
            if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                return CloseOutType.CLOSEOUT_FAILED;

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (!FileSearch.RetrieveDtaFiles())
            {
                var sharedResultsFolder = m_jobParams.GetParam("SharedResultsFolders");
                if (!string.IsNullOrEmpty(sharedResultsFolder))
                {
                    m_message += "; shared results folder is " + sharedResultsFolder;
                }

                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(m_WorkingDir, DatasetName + "_dta.txt"))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Remove any spectra from the _DTA.txt file with fewer than 3 ions
            if (!ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, DatasetName + "_dta.txt"))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Convert the _dta.txt file to a mgf file
            if (!ConvertCDTAToMGF())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Convert the _dta.txt file to a .mgf file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ConvertCDTAToMGF()
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
                var fiCDTAFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + "_dta.txt"));

                if (!fiCDTAFile.Exists)
                {
                    m_message = "_dta.txt file not found; cannot convert to .mgf";
                    LogError(m_message + ": " + fiCDTAFile.FullName);
                    return false;
                }

                if (!mDTAtoMGF.ProcessFile(fiCDTAFile.FullName))
                {
                    m_message = "Error converting " + fiCDTAFile.Name + " to a .mgf file";
                    LogError(m_message + ": " + mDTAtoMGF.GetErrorMessage());
                    return false;
                }
                else
                {
                    // Delete the _dta.txt file
                    try
                    {
                        fiCDTAFile.Delete();
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

                Thread.Sleep(125);
                PRISM.clsProgRunner.GarbageCollectNow();

                var fiNewMGFFile = new FileInfo(Path.Combine(m_WorkingDir, DatasetName + ".mgf"));

                if (!fiNewMGFFile.Exists)
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
