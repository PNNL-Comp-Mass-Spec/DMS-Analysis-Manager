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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

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
        // Ignore Spelling: Quant, txt

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
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var includeDatasetName = dataPackageID <= 0;

                var resultsDirectoryPath = AnalysisResources.GetTransferDirectoryPathForJobStep(
                    mJobParams, true,
                    out var missingJobParamTransferDirectoryPath,
                    out var missingJobParamResultsDirectoryName,
                    includeDatasetName, mDatasetName);

                if (missingJobParamTransferDirectoryPath)
                {
                    mMessage = "transferDirectoryPath not found in the job parameters";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (missingJobParamResultsDirectoryName)
                {
                    mMessage = "InputFolderName not found in the job parameters";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var resultsDirectory = new DirectoryInfo(Path.Combine(resultsDirectoryPath));

                if (!resultsDirectory.Exists)
                {
                    mMessage = "Results directory not found at " + resultsDirectoryPath;
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // The ToolName job parameter holds the name of the pipeline script we are executing
                var scriptName = mJobParams.GetJobParameter("JobParameters", "ToolName", string.Empty);

                if (string.IsNullOrWhiteSpace(scriptName))
                {
                    LogError("Job parameter ToolName is missing (or an empty string); it should hold the pipeline script name");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

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
                // Assure that the txt subdirectory exists

                var txtDirectory = new DirectoryInfo(Path.Combine(resultsDirectory.FullName, "txt"));

                if (!txtDirectory.Exists)
                {
                    LogError(string.Format(
                        "txt subdirectory not found in the results directory ({0}); " +
                        "if this is expected, manually delete the combined directory and any .index and .zip files",
                        resultsDirectory.FullName));

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var combinedDirectory = new DirectoryInfo(Path.Combine(resultsDirectory.FullName, "combined"));

                if (combinedDirectory.Exists)
                {
                    LogMessage("Deleting contents of " + combinedDirectory.FullName);
                    combinedDirectory.Delete(true);
                }
                else
                {
                    LogWarning("Combined directory not found ({0}); this is unexpected", combinedDirectory);
                }

                // Delete the .index files, along with the corresponding .zip files
                // There will be one .index file and one .zip file for each dataset

                var indexFilesDeleted = 0;
                var zipFilesDeleted = 0;

                foreach (var indexFile in resultsDirectory.GetFiles("*.index"))
                {
                    if (indexFile.Directory == null)
                    {
                        LogWarning("Unable to determine the parent directory of {0}; skipping", indexFile.FullName);
                        continue;
                    }

                    var zipFile = new FileInfo(Path.Combine(indexFile.Directory.FullName, Path.ChangeExtension(indexFile.Name, ".zip")));

                    if (zipFile.Exists)
                    {
                        zipFile.Delete();
                        zipFilesDeleted++;
                    }
                    else
                    {
                        LogWarning("Zip file for dataset not found ({0}); this is unexpected", zipFile.FullName);
                    }

                    indexFile.Delete();
                    indexFilesDeleted++;
                }

                if (indexFilesDeleted == 1 && zipFilesDeleted == 1)
                {
                    LogMessage("In the transfer directory, deleted the .index file and .zip file for this job's dataset");
                }
                else if (indexFilesDeleted > 1 || zipFilesDeleted > 1)
                {
                    LogMessage("In the transfer directory, deleted {0} .index files and {1} .zip files", indexFilesDeleted, zipFilesDeleted);
                }
                else if (indexFilesDeleted == 1)
                {
                    LogMessage("Deleted one .index file in the transfer directory; .zip file not found");
                }
                else if (indexFilesDeleted == 0)
                {
                    LogMessage("Did not find any .index files in the transfer directory");
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
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

                    if (reMatch.Success && int.TryParse(reMatch.Groups[1].Value, out var stepNumber))
                    {
                        if (!resultsFiles.ContainsKey(stepNumber))
                        {
                            foreach (var file in subdirectory.GetFiles(RESULTS_DB3_FILE))
                            {
                                resultsFiles.Add(stepNumber, file);
                                break;
                            }
                        }
                    }
                }

                if (resultsFiles.Count > 1)
                {
                    // Delete the Results.db3 files for the steps prior to lastStep
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

                    mEvalMessage = string.Format(
                        "Deleted {0} extra {1} {2}",
                        fileCountDeleted,
                        RESULTS_DB3_FILE,
                        Global.CheckPlural(fileCountDeleted, "file", "files"));

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
        private bool StoreToolVersionInfo()
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
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }
    }
}
