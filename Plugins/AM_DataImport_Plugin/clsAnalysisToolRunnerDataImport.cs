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
            bool blnMoveFilesAfterImport = false;
            bool blnSuccess = false;

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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AnalysisManagerDataImportPlugIn version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Import the files
                blnSuccess = PerformDataImport();
                if (!blnSuccess)
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
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete two auto-generated files from the Results Folder since they're not necessary to keep
                System.Threading.Thread.Sleep(500);
                DeleteFileFromResultFolder("DataImport_AnalysisSummary.txt");
                DeleteFileFromResultFolder("JobParameters_" + m_JobNum + ".xml");

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

                blnMoveFilesAfterImport = m_jobParams.GetJobParameter("MoveFilesAfterImport", true);
                if (blnMoveFilesAfterImport)
                {
                    MoveImportedFiles();
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in DataImportPlugin->RunTool: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected void DeleteFileFromResultFolder(string strFileName)
        {
            try
            {
                var fiFileToDelete = new FileInfo(Path.Combine(m_WorkDir, Path.Combine(m_ResFolderName, strFileName)));
                if (fiFileToDelete.Exists)
                    fiFileToDelete.Delete();
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Move the files from the source folder to a new subfolder below the source folder
        /// </summary>
        /// <returns></returns>
        /// <remarks>The name of the new subfolder comes from m_ResFolderName</remarks>
        protected bool MoveImportedFiles()
        {
            string strTargetFolder = "??";
            string strTargetFilePath = "??";

            try
            {
                if (mSourceFiles == null || mSourceFiles.Count == 0)
                {
                    // Nothing to do
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "mSourceFiles is empty; nothing for MoveImportedFiles to do");
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

                foreach (FileInfo fiFile in mSourceFiles)
                {
                    try
                    {
                        strTargetFilePath = Path.Combine(strTargetFolder, fiFile.Name);
                        fiFile.MoveTo(strTargetFilePath);
                    }
                    catch (Exception ex)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Error moving file " + fiFile.Name + " to " + strTargetFilePath + ": " + ex.Message);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Error moving files to " + strTargetFolder + ":" + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
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
                    m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "DataImportSharePath");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }

                if (string.IsNullOrEmpty(strDataImportFolder))
                {
                    strMessage = "DataImportFolder not defined in the Special_Processing parameters or the settings file for this job; will assume the input folder is the dataset name";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_message);
                    strDataImportFolder = string.Copy(m_Dataset);
                }

                var fiSourceShare = new DirectoryInfo(strSharePath);
                if (!fiSourceShare.Exists)
                {
                    m_message = "Data Import Share not found: " + fiSourceShare.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }

                strSourceFolderPath = Path.Combine(fiSourceShare.FullName, strDataImportFolder);
                var fiSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (!fiSourceFolder.Exists)
                {
                    m_message = "Data Import Folder not found: " + fiSourceFolder.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }

                // Copy files from the source folder to the working directory
                mSourceFiles.Clear();
                foreach (FileInfo fiFile in fiSourceFolder.GetFiles(strSourceFileSpec))
                {
                    try
                    {
                        fiFile.CopyTo(Path.Combine(m_WorkDir, fiFile.Name));
                        mSourceFiles.Add(fiFile);
                    }
                    catch (Exception ex)
                    {
                        m_message = "Error copying file " + fiFile.Name + ": " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                        return false;
                    }
                }

                if (mSourceFiles.Count == 0)
                {
                    if (strSourceFileSpec == MATCH_ALL_FILES)
                    {
                        m_message = "Source folder was empty; nothing to copy: " + fiSourceFolder.FullName;
                    }
                    else
                    {
                        m_message = "No files matching " + strSourceFileSpec + " were found in the source folder: " + fiSourceFolder.FullName;
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Error importing data files: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
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
            string strToolVersionInfo = string.Empty;
            string strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of AnalysisManagerDataImportPlugIn
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerDataImportPlugIn"))
            {
                return false;
            }

            // Store the path to AnalysisManagerDataImportPlugIn.dll in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerDataImportPlugIn.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
