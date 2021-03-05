//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerDataImportPlugIn
{
    /// <summary>
    /// Class for importing data files from an external source into a job folder
    /// </summary>
    public class AnalysisToolRunnerDataImport : AnalysisToolRunnerBase
    {
        // Ignore Spelling: yyyy-MM-dd_HH-mm-ss

        #region "Module Variables"

        protected List<FileInfo> mSourceFiles;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs DataImport tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AnalysisManagerDataImportPlugIn version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining AnalysisManagerDataImportPlugIn version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Import the files
                var importSuccess = PerformDataImport();
                if (!importSuccess)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error calling PerformDataImport";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                // Skip two auto-generated files from the Results Folder since they're not necessary to keep
                mJobParams.AddResultFileToSkip("DataImport_AnalysisSummary.txt");
                mJobParams.AddResultFileToSkip("JobParameters_" + mJob + ".xml");

                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                var moveFilesAfterImport = mJobParams.GetJobParameter("MoveFilesAfterImport", true);
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
        /// Move the files from the source directory to a new subdirectory below the source directory
        /// </summary>
        /// <remarks>The name of the new subdirectory comes from mResultsDirectoryName</remarks>
        protected bool MoveImportedFiles()
        {
            var targetDirectoryPath = "??";
            var targetFilePath = "??";

            try
            {
                if (mSourceFiles == null || mSourceFiles.Count == 0)
                {
                    // Nothing to do
                    LogWarning("mSourceFiles is empty; nothing for MoveImportedFiles to do");
                    return true;
                }

                targetDirectoryPath = Path.Combine(mSourceFiles[0].DirectoryName, mResultsDirectoryName);
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);
                if (targetDirectory.Exists)
                {
                    // Need to rename the target folder
                    targetDirectory.MoveTo(targetDirectory.FullName + "_" + System.DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss"));
                    targetDirectory = new DirectoryInfo(targetDirectoryPath);
                }

                if (!targetDirectory.Exists)
                {
                    targetDirectory.Create();
                }

                foreach (var sourceFile in mSourceFiles)
                {
                    try
                    {
                        targetFilePath = Path.Combine(targetDirectoryPath, sourceFile.Name);
                        sourceFile.MoveTo(targetFilePath);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Error moving file " + sourceFile.Name + " to " + targetFilePath + ": " + ex.Message);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error moving files to " + targetDirectoryPath + ":" + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Import files from the source share to the analysis job folder
        /// </summary>
        protected bool PerformDataImport()
        {
            const string MATCH_ALL_FILES = "*";

            try
            {
                mSourceFiles = new List<FileInfo>();

                var strSharePath = mJobParams.GetJobParameter("DataImportSharePath", "");

                // If the user specifies a DataImportFolder using the "Special Processing" field of an analysis job, the directory name will be stored in section JobParameters
                var dataImportFolder = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "DataImportFolder", "");

                if (string.IsNullOrEmpty(dataImportFolder))
                {
                    // If the user specifies a DataImportFolder using the SettingsFile for an analysis job, the directory name will be stored in section JobParameters
                    dataImportFolder = mJobParams.GetJobParameter("DataImport", "DataImportFolder", "");
                }

                if (string.IsNullOrEmpty(dataImportFolder))
                {
                    // dataImportFolder is still empty, look for a parameter named DataImportFolder in any section
                    dataImportFolder = mJobParams.GetJobParameter("DataImportFolder", "");
                }

                var sourceFileSpec = mJobParams.GetJobParameter("DataImportFileMask", "");
                if (string.IsNullOrEmpty(sourceFileSpec))
                    sourceFileSpec = MATCH_ALL_FILES;

                if (string.IsNullOrEmpty(strSharePath))
                {
                    LogError(AnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "DataImportSharePath"));
                    return false;
                }

                if (string.IsNullOrEmpty(dataImportFolder))
                {
                    mMessage = "DataImportFolder not defined in the Special_Processing parameters or the settings file for this job; will assume the input folder is the dataset name";
                    LogMessage(mMessage);
                    dataImportFolder = string.Copy(mDatasetName);
                }

                var fiSourceShare = new DirectoryInfo(strSharePath);
                if (!fiSourceShare.Exists)
                {
                    LogError("Data Import Share not found: " + fiSourceShare.FullName);
                    return false;
                }

                var strSourceFolderPath = Path.Combine(fiSourceShare.FullName, dataImportFolder);
                var fiSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (!fiSourceFolder.Exists)
                {
                    LogError("Data Import Folder not found: " + fiSourceFolder.FullName);
                    return false;
                }

                // Copy files from the source folder to the working directory
                mSourceFiles.Clear();
                foreach (var fiFile in fiSourceFolder.GetFiles(sourceFileSpec))
                {
                    try
                    {
                        fiFile.CopyTo(Path.Combine(mWorkDir, fiFile.Name));
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
                    if (sourceFileSpec == MATCH_ALL_FILES)
                    {
                        msg = "Source folder was empty; nothing to copy: " + fiSourceFolder.FullName;
                    }
                    else
                    {
                        msg = "No files matching " + sourceFileSpec + " were found in the source folder: " + fiSourceFolder.FullName;
                    }

                    LogError(msg);
                    return false;
                }
                else
                {
                    var message = "Copied " + mSourceFiles.Count + " file";
                    if (mSourceFiles.Count > 1)
                        message += "s";
                    message += " from " + fiSourceFolder.FullName;

                    if (mDebugLevel >= 2)
                    {
                        LogMessage(message);
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
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;
            var strAppFolderPath = Global.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of AnalysisManagerDataImportPlugIn
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerDataImportPlugIn"))
            {
                return false;
            }

            // Store the path to AnalysisManagerDataImportPlugIn.dll in toolFiles
            var toolFiles = new List<FileInfo> {
                new(Path.Combine(strAppFolderPath, "AnalysisManagerDataImportPlugIn.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, false);
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
