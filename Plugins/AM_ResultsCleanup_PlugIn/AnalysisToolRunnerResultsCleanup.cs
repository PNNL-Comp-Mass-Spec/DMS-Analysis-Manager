//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/17/2013
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AnalysisManagerResultsCleanupPlugin
{
    /// <summary>
    /// Class for running Results Cleanup
    /// </summary>
    /// <remarks>
    /// Applies to MAC jobs and MaxQuant jobs
    /// </remarks>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerResultsCleanup : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Quant

        private const string RESULTS_DB3_FILE = "Results.db3";

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
                var result = PerformResultsCleanup();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
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
                mMessage = "Error in AnalysisToolRunnerResultsCleanup->RunTool";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType PerformResultsCleanup()
        {
            try
            {
                var transferDirectoryPath = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION,
                    AnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH, string.Empty);
                var resultsDirectoryName = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "InputFolderName", string.Empty);

                if (string.IsNullOrWhiteSpace(transferDirectoryPath))
                {
                    mMessage = "transferFolderPath not defined";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrWhiteSpace(resultsDirectoryName))
                {
                    mMessage = "InputFolderName not defined";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var transferDirectory = new DirectoryInfo(transferDirectoryPath);

                if (!transferDirectory.Exists)
                {
                    mMessage = "transferFolder not found at " + transferDirectoryPath;
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var resultsDirectory = new DirectoryInfo(Path.Combine(transferDirectory.FullName, resultsDirectoryName));

                // The ToolName job parameter holds the name of the pipeline script we are executing
                var scriptName = mJobParams.GetJobParameter("JobParameters", "ToolName", string.Empty);

                if (scriptName.StartsWith("MaxQuant", StringComparison.OrdinalIgnoreCase))
                {
                    return MaxQuantResultsCleanup(resultsDirectory);
                }

                if (scriptName.StartsWith("MAC", StringComparison.OrdinalIgnoreCase))
                {
                    return MACResultsCleanup(resultsDirectory);
                }

                mMessage = string.Format("Results cleanup for script {0} is not supported", scriptName);
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in PerformResultsCleanup";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private CloseOutType MACResultsCleanup(DirectoryInfo resultsDirectory)
        {
            try
            {
                var result = RemoveOldResultsDb3Files(resultsDirectory);
                return result;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MACResultsCleanup";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType MaxQuantResultsCleanup(DirectoryInfo resultsDirectory)
        {
            try
            {
                // ToDo: implement this logic

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MaxQuantResultsCleanup";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RemoveOldResultsDb3Files(DirectoryInfo resultsDirectory)
        {
            var stepDirectoryCount = 0;

            try
            {
                var stepNumberMatcher = new Regex(@"Step_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var resultsFiles = new Dictionary<int, FileInfo>();

                // Look for Results.db3 files in the subdirectories of the transfer directory
                // Only process directories that start with the text "Step_"
                foreach (var subdirectory in resultsDirectory.GetDirectories("Step_*"))
                {
                    stepDirectoryCount++;

                    // Parse out the step number
                    var reMatch = stepNumberMatcher.Match(subdirectory.Name);

                    if (reMatch.Success && int.TryParse(reMatch.Groups[1].Value, out var intStepNumber))
                    {
                        if (!resultsFiles.ContainsKey(intStepNumber))
                        {
                            foreach (var fiFile in subdirectory.GetFiles(RESULTS_DB3_FILE))
                            {
                                resultsFiles.Add(intStepNumber, fiFile);
                                break;
                            }
                        }
                    }
                }

                if (resultsFiles.Count > 1)
                {
                    // Delete the Results.db3 files for the steps prior to intLastStep
                    var lastStep = resultsFiles.Keys.Max();
                    var fileCountDeleted = 0;

                    var lnqQuery = from item in resultsFiles where item.Key < lastStep select item;
                    foreach (var item in lnqQuery)
                    {
                        try
                        {
                            item.Value.Delete();
                            fileCountDeleted++;
                        }
                        catch (Exception ex)
                        {
                            LogWarning("Error deleting extra " + RESULTS_DB3_FILE + " file: " + ex.Message);
                        }
                    }

                    mEvalMessage = "Deleted " + fileCountDeleted + " extra " + RESULTS_DB3_FILE + " " +
                                    Global.CheckPlural(fileCountDeleted, "file", "files");

                    LogMessage(mEvalMessage + " from " + resultsDirectory.FullName);
                }
                else if (resultsFiles.Count == 1)
                {
                    mEvalMessage = "Results directories has just one " + RESULTS_DB3_FILE + " file";
                    LogMessage(mEvalMessage);
                }
                else
                {
                    if (stepDirectoryCount > 0)
                    {
                        LogWarning("None of the Step_# directories has a " + RESULTS_DB3_FILE + " file", true);
                    }
                    else
                    {
                        mMessage = "Results directory does not have any Step_# directories";
                        LogError(mMessage + ": " + resultsDirectory.FullName);
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
        protected bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var appDirectoryPath = Global.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Analysis Manager
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerProg"))
            {
                return false;
            }

            // Lookup the version of AnalysisManagerResultsCleanupPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerResultsCleanupPlugin"))
            {
                return false;
            }

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsCleanupPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(Path.Combine(appDirectoryPath, "AnalysisManagerProg.exe")),
                new(Path.Combine(appDirectoryPath, "AnalysisManagerResultsCleanupPlugin.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }
    }
}
