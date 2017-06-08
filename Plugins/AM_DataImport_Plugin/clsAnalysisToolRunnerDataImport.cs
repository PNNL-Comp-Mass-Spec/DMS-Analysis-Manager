//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerDataImportPlugIn
{
    /// <summary>
    /// Class for importing data files from an external source into a job folder
    /// </summary>
    public class clsAnalysisToolRunnerDataImport : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected List<FileInfo> mSourceFiles;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs DataImport tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AnalysisManagerDataImportPlugIn version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AnalysisManagerDataImportPlugIn version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Import the files
                var importSuccess = PerformDataImport();
                if (!importSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error calling PerformDataImport";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;

                //Make sure objects are released
                System.Threading.Thread.Sleep(500);         // 1 second delay
                PRISM.clsProgRunner.GarbageCollectNow();

                // Skip two auto-generated files from the Results Folder since they're not necessary to keep
                m_jobParams.AddResultFileToSkip("DataImport_AnalysisSummary.txt");
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                var moveFilesAfterImport = m_jobParams.GetJobParameter("MoveFilesAfterImport", true);
                if (moveFilesAfterImport)
                {
                    MoveImportedFiles();
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in DataImportPlugin->RunTool: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            
        }

        /// <summary>
        /// Move the files from the source folder to a new subfolder below the source folder
        /// </summary>
        /// <returns></returns>
        /// <remarks>The name of the new subfolder comes from m_ResFolderName</remarks>
        protected bool MoveImportedFiles()
        {
            var strTargetFolder = "??";
            var strTargetFilePath = "??";

            try
            {
                if (mSourceFiles == null || mSourceFiles.Count == 0)
                {
                    // Nothing to do
                    LogWarning("mSourceFiles is empty; nothing for MoveImportedFiles to do");
                    return true;
                }

                strTargetFolder = Path.Combine(mSourceFiles[0].DirectoryName, m_ResFolderName);
                var fiTargetFolder = new DirectoryInfo(strTargetFolder);
                if (fiTargetFolder.Exists)
                {
                    // Need to rename the target folder
                    fiTargetFolder.MoveTo(fiTargetFolder.FullName + "_" + System.DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss"));
                    fiTargetFolder = new DirectoryInfo(strTargetFolder);
                }

                if (!fiTargetFolder.Exists)
                {
                    fiTargetFolder.Create();
                }

                foreach (var fiFile in mSourceFiles)
                {
                    try
                    {
                        strTargetFilePath = Path.Combine(strTargetFolder, fiFile.Name);
                        fiFile.MoveTo(strTargetFilePath);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Error moving file " + fiFile.Name + " to " + strTargetFilePath + ": " + ex.Message);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error moving files to " + strTargetFolder + ":" + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Import files from the source share to the analysis job folder
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool PerformDataImport()
        {
            const string MATCH_ALL_FILES = "*";

            string strSharePath = null;
            string strDataImportFolder = null;
            string strSourceFileSpec = null;
            string strSourceFolderPath = null;

            string strMessage = null;

            try
            {
                mSourceFiles = new List<FileInfo>();

                strSharePath = m_jobParams.GetJobParameter("DataImportSharePath", "");

                // If the user specifies a DataImportFolder using the "Special Processing" field of an analysis job, then the folder name will be stored in section "JobParameters"
                strDataImportFolder = m_jobParams.GetJobParameter("JobParameters", "DataImportFolder", "");

                if (string.IsNullOrEmpty(strDataImportFolder))
                {
                    // If the user specifies a DataImportFolder using the SettingsFile for an analysis job, then the folder name will be stored in section "JobParameters"
                    strDataImportFolder = m_jobParams.GetJobParameter("DataImport", "DataImportFolder", "");
                }

                if (string.IsNullOrEmpty(strDataImportFolder))
                {
                    // strDataImportFolder is still empty, look for a parameter named DataImportFolder in any section
                    strDataImportFolder = m_jobParams.GetJobParameter("DataImportFolder", "");
                }

                strSourceFileSpec = m_jobParams.GetJobParameter("DataImportFileMask", "");
                if (string.IsNullOrEmpty(strSourceFileSpec))
                    strSourceFileSpec = MATCH_ALL_FILES;

                if (string.IsNullOrEmpty(strSharePath))
                {
                    LogError(clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "DataImportSharePath"));
                    return false;
                }

                if (string.IsNullOrEmpty(strDataImportFolder))
                {
                    strMessage = "DataImportFolder not defined in the Special_Processing parameters or the settings file for this job; will assume the input folder is the dataset name";
                    LogMessage(m_message);
                    strDataImportFolder = string.Copy(m_Dataset);
                }

                var fiSourceShare = new DirectoryInfo(strSharePath);
                if (!fiSourceShare.Exists)
                {
                    LogError("Data Import Share not found: " + fiSourceShare.FullName);
                    return false;
                }

                strSourceFolderPath = Path.Combine(fiSourceShare.FullName, strDataImportFolder);
                var fiSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (!fiSourceFolder.Exists)
                {
                    LogError("Data Import Folder not found: " + fiSourceFolder.FullName);
                    return false;
                }

                // Copy files from the source folder to the working directory
                mSourceFiles.Clear();
                foreach (var fiFile in fiSourceFolder.GetFiles(strSourceFileSpec))
                {
                    try
                    {
                        fiFile.CopyTo(Path.Combine(m_WorkDir, fiFile.Name));
                        mSourceFiles.Add(fiFile);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error copying file " + fiFile.Name + ": " + ex.Message, ex);
                        return false;
                    }
                }

                if (mSourceFiles.Count == 0)
                {
                    string msg;
                    if (strSourceFileSpec == MATCH_ALL_FILES)
                    {
                        msg = "Source folder was empty; nothing to copy: " + fiSourceFolder.FullName;
                    }
                    else
                    {
                        msg = "No files matching " + strSourceFileSpec + " were found in the source folder: " + fiSourceFolder.FullName;
                    }

                    LogError(msg);
                    return false;
                }
                else
                {
                    strMessage = "Copied " + mSourceFiles.Count + " file";
                    if (mSourceFiles.Count > 1)
                        strMessage += "s";
                    strMessage += " from " + fiSourceFolder.FullName;

                    if (m_DebugLevel >= 2)
                    {
                        LogMessage(strMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error importing data files: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;
            var strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of AnalysisManagerDataImportPlugIn
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerDataImportPlugIn"))
            {
                return false;
            }

            // Store the path to AnalysisManagerDataImportPlugIn.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerDataImportPlugIn.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        #endregion
    }
}
