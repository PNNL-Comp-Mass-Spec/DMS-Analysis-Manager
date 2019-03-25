//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/17/2013
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerResultsCleanupPlugin
{
    /// <summary>
    /// Class for running Results Cleanup
    /// </summary>
    public class clsAnalysisToolRunnerResultsCleanup : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        private const string RESULTS_DB3_FILE = "Results.db3";

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs ResultsCleanup tool
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

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining AnalysisManager version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Cleanup results in the transfer directory
                var Result = PerformResultsCleanup();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error calling PerformResultsCleanup";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                mMessage = "Error in clsAnalysisToolRunnerResultsCleanup->RunTool";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType PerformResultsCleanup()
        {
            CloseOutType eResult;

            try
            {
                var strTransferDirectoryPath = mJobParams.GetJobParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH, string.Empty);
                var strResultsFolderName = mJobParams.GetJobParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, "InputFolderName", string.Empty);

                if (string.IsNullOrWhiteSpace(strTransferDirectoryPath))
                {
                    mMessage = "transferFolderPath not defined";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrWhiteSpace(strResultsFolderName))
                {
                    mMessage = "InputFolderName not defined";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var diTransferFolder = new DirectoryInfo(strTransferDirectoryPath);

                if (!diTransferFolder.Exists)
                {
                    mMessage = "transferFolder not found at " + strTransferDirectoryPath;
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var diResultsFolder = new DirectoryInfo(Path.Combine(diTransferFolder.FullName, strResultsFolderName));
                eResult = RemoveOldResultsDb3Files(diResultsFolder);
            }
            catch (Exception ex)
            {
                mMessage = "Error in PerformResultsCleanup";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return eResult;
        }

        private CloseOutType RemoveOldResultsDb3Files(DirectoryInfo diResultsFolder)
        {
            var intStepFolderCount = 0;

            try
            {
                var reStepNumber = new Regex(@"Step_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var dctResultsFiles = new Dictionary<int, FileInfo>();

                // Look for Results.db3 files in the subfolders of the transfer folder
                // Only process folders that start with the text "Step_"
                foreach (var diSubfolder in diResultsFolder.GetDirectories("Step_*"))
                {
                    intStepFolderCount += 1;

                    // Parse out the step number
                    var reMatch = reStepNumber.Match(diSubfolder.Name);

                    if (reMatch.Success && int.TryParse(reMatch.Groups[1].Value, out var intStepNumber))
                    {
                        if (!dctResultsFiles.ContainsKey(intStepNumber))
                        {
                            foreach (var fiFile in diSubfolder.GetFiles(RESULTS_DB3_FILE))
                            {
                                dctResultsFiles.Add(intStepNumber, fiFile);
                                break;
                            }
                        }
                    }
                }

                if (dctResultsFiles.Count > 1)
                {
                    // Delete the Results.db3 files for the steps prior to intLastStep
                    var intLastStep = dctResultsFiles.Keys.Max();
                    var intFileCountDeleted = 0;

                    var lnqQuery = from item in dctResultsFiles where item.Key < intLastStep select item;
                    foreach (var item in lnqQuery)
                    {
                        try
                        {
                            item.Value.Delete();
                            intFileCountDeleted += 1;
                        }
                        catch (Exception ex)
                        {
                            LogWarning("Error deleting extra " + RESULTS_DB3_FILE + " file: " + ex.Message);
                        }
                    }

                    mEvalMessage = "Deleted " + intFileCountDeleted + " extra " + RESULTS_DB3_FILE + " " +
                                    clsGlobal.CheckPlural(intFileCountDeleted, "file", "files");

                    LogMessage(mEvalMessage + " from " + diResultsFolder.FullName);
                }
                else if (dctResultsFiles.Count == 1)
                {
                    mEvalMessage = "Results folder has just one " + RESULTS_DB3_FILE + " file";
                    LogMessage(mEvalMessage);
                }
                else
                {
                    if (intStepFolderCount > 0)
                    {
                        LogWarning("None of the Step_# folders has a " + RESULTS_DB3_FILE + " file", true);
                    }
                    else
                    {
                        mMessage = "Results folder does not have any Step_# folders";
                        LogError(mMessage + ": " + diResultsFolder.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in RemoveOldResultsDb3Files";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;
            var strAppFolderPath = clsGlobal.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Analysis Manager
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerProg"))
            {
                return false;
            }

            // Lookup the version of AnalysisManagerResultsCleanupPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerResultsCleanupPlugin"))
            {
                return false;
            }

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsCleanupPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerProg.exe")),
                new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerResultsCleanupPlugin.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
