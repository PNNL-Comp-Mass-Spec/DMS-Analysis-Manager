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
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerDtaImport : clsAnalysisToolRunnerBase
    {
        #region "Methods"

        /// <summary>
        /// Runs DTA Import tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Start the job timer
                m_StartTime = System.DateTime.UtcNow;

                var result = CopyManualDTAs();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //TODO: What do we do here?
                    return result;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in DtaImportPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //No failures so everything must have succeeded
        }

        private CloseOutType CopyManualDTAs()
        {
            string SourceFolderNamePath = string.Empty;
            string TargetFolderNamePath = string.Empty;
            string CompleteFolderNamePath = string.Empty;

            try
            {
                // Note: the DTAFolderLocation is defined in the Manager_Control DB, and is specific for this manager
                //       for example: \\pnl\projects\MSSHARE\SPurvine
                // This folder must contain subfolders whose name matches the output_folder name assigned to each job
                // Furthermore, each subfolder must have a file named Dataset_dta.zip

                SourceFolderNamePath = Path.Combine(m_mgrParams.GetParam("DTAFolderLocation"), m_jobParams.GetParam("OutputFolderName"));
                CompleteFolderNamePath = Path.Combine(m_mgrParams.GetParam("DTAProcessedFolderLocation"), m_jobParams.GetParam("OutputFolderName"));

                //Determine if Dta folder in transfer directory already exists; Make directory if it doesn't exist
                TargetFolderNamePath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_Dataset);
                if (!Directory.Exists(TargetFolderNamePath))
                {
                    //Make the DTA folder
                    try
                    {
                        Directory.CreateDirectory(TargetFolderNamePath);
                    }
                    catch (Exception ex)
                    {
                        m_message = clsGlobal.AppendToComment(m_message, "Error creating results folder on " + Path.GetPathRoot(TargetFolderNamePath)) + ": " + ex.Message;
                        return CloseOutType.CLOSEOUT_FAILED;
                        //TODO: Handle errors
                    }
                }

                // Now append the output folder name to TargetFolderNamePath
                TargetFolderNamePath = Path.Combine(TargetFolderNamePath, m_jobParams.GetParam("OutputFolderName"));
            }
            catch (Exception ex)
            {
                m_message = clsGlobal.AppendToComment(m_message, "Error creating results folder: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
                //TODO: Handle errors
            }

            try
            {
                //Copy the DTA folder to the transfer folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyDirectory(SourceFolderNamePath, TargetFolderNamePath, false);

                //Now move the DTA folder to succeeded folder
                Directory.Move(SourceFolderNamePath, CompleteFolderNamePath);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = clsGlobal.AppendToComment(m_message, "Error copying results folder to " + Path.GetPathRoot(TargetFolderNamePath) + " : " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion
    }
}
