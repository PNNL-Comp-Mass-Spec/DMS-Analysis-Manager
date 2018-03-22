//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 04/10/2009
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerDtaImportPlugIn
{
    /// <summary>
    /// Class for running DTA Importer
    /// </summary>
    public class clsAnalysisToolRunnerDtaImport : clsAnalysisToolRunnerBase
    {
        #region "Methods"

        /// <summary>
        /// Runs DTA Import tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Start the job timer
                m_StartTime = DateTime.UtcNow;

                var result = CopyManualDtAs();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();
            }
            catch (Exception ex)
            {
                m_message = "Error in DtaImportPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // No failures so everything must have succeeded
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType CopyManualDtAs()
        {
            string sourceFolderNamePath;
            string targetFolderNamePath;
            string completeFolderNamePath;

            try
            {
                // Note: the DTAFolderLocation is defined in the Manager_Control DB, and is specific for this manager
                //       for example: \\pnl\projects\MSSHARE\SPurvine
                // This folder must contain subfolders whose name matches the output_folder name assigned to each job
                // Furthermore, each subfolder must have a file named Dataset_dta.zip

                sourceFolderNamePath = Path.Combine(m_mgrParams.GetParam("DTAFolderLocation"), m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));
                completeFolderNamePath = Path.Combine(m_mgrParams.GetParam("DTAProcessedFolderLocation"), m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));

                // Determine if Dta folder in transfer directory already exists; Make directory if it doesn't exist
                targetFolderNamePath = Path.Combine(m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH), m_Dataset);
                if (!Directory.Exists(targetFolderNamePath))
                {
                    // Make the DTA folder
                    try
                    {
                        Directory.CreateDirectory(targetFolderNamePath);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error creating results folder in transfer directory",
                                 "Error creating results folder " + targetFolderNamePath, ex);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Now append the output folder name to TargetFolderNamePath
                targetFolderNamePath = Path.Combine(targetFolderNamePath, m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));
            }
            catch (Exception ex)
            {
                LogError("Error creating results folder", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                // Copy the DTA folder to the transfer folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyDirectory(sourceFolderNamePath, targetFolderNamePath, false);

                // Now move the DTA folder to succeeded folder
                Directory.Move(sourceFolderNamePath, completeFolderNamePath);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {

                LogError("Error copying results folder to transfer directory",
                         "Error copying results folder to " + targetFolderNamePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion
    }
}
