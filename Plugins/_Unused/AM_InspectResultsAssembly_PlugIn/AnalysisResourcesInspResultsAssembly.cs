using AnalysisManagerBase;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerInspResultsAssemblyPlugIn
{
    /// <summary>
    /// Retrieve resources for the Inspect Results Assembly plugin
    /// </summary>
    public class AnalysisResourcesInspResultsAssembly : AnalysisResources
    {
        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var transferDirectoryName = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH), DatasetName);
            var zippedResultName = DatasetName + "_inspect.zip";
            const string searchLogResultName = "InspectSearchLog.txt";

            transferDirectoryName = Path.Combine(transferDirectoryName, mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

            // Retrieve FASTA file (used by the PeptideToProteinMapper)
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Retrieve the Inspect Input Params file
            if (!FileSearchTool.RetrieveFile(AnalysisToolRunnerInspResultsAssembly.INSPECT_INPUT_PARAMS_FILENAME, transferDirectoryName))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var numClonedSteps = mJobParams.GetParam("NumberOfClonedSteps");

            if (string.IsNullOrEmpty(numClonedSteps))
            {
                // This is not a parallelized job
                // Retrieve the zipped Inspect result file
                if (!FileSearchTool.RetrieveFile(zippedResultName, transferDirectoryName))
                {
                    if (mDebugLevel >= 3)
                    {
                        LogError("RetrieveFile returned False for " + zippedResultName + " using directory " + transferDirectoryName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Unzip Inspect result file
                if (mDebugLevel >= 2)
                {
                    LogMessage("Unzipping Inspect result file");
                }
                if (UnzipFileStart(Path.Combine(mWorkDir, zippedResultName), mWorkDir, "AnalysisResourcesInspResultsAssembly.GetResources"))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Inspect result file unzipped");
                    }
                }

                // Retrieve the Inspect search log file
                if (!FileSearchTool.RetrieveFile(searchLogResultName, transferDirectoryName))
                {
                    if (mDebugLevel >= 3)
                    {
                        LogError("RetrieveFile returned False for " + searchLogResultName + " using directory " + transferDirectoryName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddResultFileExtensionToSkip(searchLogResultName);
            }
            else
            {
                // This is a parallelized job
                // Retrieve multi inspect result files
                if (!RetrieveMultiInspectResultFiles())
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieves inspect and inspect log and error files
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool RetrieveMultiInspectResultFiles()
        {
            int numOfResultFiles;
            var transferDirectoryName = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH), DatasetName);

            var fileCopyCount = 0;

            transferDirectoryName = Path.Combine(transferDirectoryName, mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

            try
            {
                numOfResultFiles = mJobParams.GetJobParameter("NumberOfClonedSteps", 0);
            }
            catch (Exception)
            {
                numOfResultFiles = 0;
            }

            if (numOfResultFiles < 1)
            {
                LogError("Job parameter 'NumberOfClonedSteps' is empty or 0; unable to continue");
                return false;
            }

            for (var fileNum = 1; fileNum <= numOfResultFiles; fileNum++)
            {
                // Copy each Inspect result file from the transfer directory
                var inspectResultsFile = DatasetName + "_" + fileNum + "_inspect.txt";
                var dtaFilename = DatasetName + "_" + fileNum + "_dta.txt";

                if (File.Exists(Path.Combine(transferDirectoryName, inspectResultsFile)))
                {
                    if (!CopyFileToWorkDir(inspectResultsFile, transferDirectoryName, mWorkDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (mDebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned False for " + inspectResultsFile + " using directory " + transferDirectoryName);
                        }
                        return false;
                    }
                    fileCopyCount++;

                    // Update the list of files to delete from the server
                    mJobParams.AddServerFileToDelete(Path.Combine(transferDirectoryName, inspectResultsFile));
                    mJobParams.AddServerFileToDelete(Path.Combine(transferDirectoryName, dtaFilename));

                    // Update the list of local files to delete
                    mJobParams.AddResultFileToSkip(inspectResultsFile);
                }

                // Copy the various log files
                for (var logFileIndex = 1; logFileIndex <= 3; logFileIndex++)
                {
                    var fileName = logFileIndex switch
                    {
                        // Copy the Inspect error file from the transfer directory
                        1 => DatasetName + "_" + fileNum + "_error.txt",

                        // Copy each Inspect search log file from the transfer directory
                        2 => "InspectSearchLog_" + fileNum + ".txt",

                        // Copy each Inspect console output file from the transfer directory
                        3 => "InspectConsoleOutput_" + fileNum + ".txt",
                        _ => string.Empty
                    };

                    if (string.IsNullOrWhiteSpace(fileName) || !File.Exists(Path.Combine(transferDirectoryName, fileName)))
                        continue;

                    if (!CopyFileToWorkDir(fileName, transferDirectoryName, mWorkDir))
                    {
                        // Error copying file (error will have already been logged)
                        if (mDebugLevel >= 3)
                        {
                            LogError("CopyFileToWorkDir returned False for " + fileName + " using directory " + transferDirectoryName);
                        }
                        return false;
                    }

                    fileCopyCount++;

                    // Update the list of files to delete from the server
                    mJobParams.AddServerFileToDelete(Path.Combine(transferDirectoryName, fileName));

                    // Update the list of local files to delete
                    mJobParams.AddResultFileToSkip(fileName);
                }
            }

            LogMessage(
                "Multi Inspect Result Files copied to local working directory; copied " + fileCopyCount + " files");

            return true;
        }
    }
}
