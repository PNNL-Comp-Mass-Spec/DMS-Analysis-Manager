//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/06
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Subclass for SEQUEST-specific tasks:
    /// 1) Distributes OrgDB files to cluster nodes if running on a cluster
    /// 2) Uses ParamFileGenerator to create SEQUEST param file from database instead of copying it
    /// 3) Retrieves zipped DTA files
    /// 4) Retrieves _out.txt.tmp file (if it exists)
    /// </summary>
    public class AnalysisResourcesSeq : AnalysisResources
    {
        // Ignore Spelling: deconcatenated, yyyy-MM-dd

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        private void ArchiveSequestParamFile()
        {
            var sourceFilePath = "";
            var targetFolderPath = "";

            try
            {
                sourceFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParmFileName"));
                targetFolderPath = mJobParams.GetParam("ParmFileStoragePath");

                if (mDebugLevel >= 3)
                {
                    LogDebug("Verifying that the SEQUEST parameter file " + mJobParams.GetParam("ParmFileName") + " exists in " + targetFolderPath);
                }

                ArchiveSequestParamFile(sourceFilePath, targetFolderPath);
            }
            catch (Exception ex)
            {
                targetFolderPath ??= "??";

                mMessage = "Error archiving param file to ParmFileStoragePath";
                LogErrorToDatabase(mMessage + ": " + sourceFilePath + " --> " + targetFolderPath + ": " + ex.Message);
            }
        }

        public void ArchiveSequestParamFile(string sourceFilePath, string targetFolderPath)
        {
            var lineIgnoreRegExSpecs = new List<Regex> {
                new("mass_type_parent *=.*")
            };

            var needToArchiveFile = false;

            var sourceFileName = Path.GetFileName(sourceFilePath);

            if (string.IsNullOrWhiteSpace(sourceFileName))
            {
                LogWarning("Null or empty source file path sent to ArchiveSequestParamFile");
                return;
            }

            var targetFilePath = Path.Combine(targetFolderPath, sourceFileName);

            if (!File.Exists(targetFilePath))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("SEQUEST parameter file not found in archive folder; copying to " + targetFilePath);
                }

                needToArchiveFile = true;
            }
            else
            {
                // Read the files line-by-line and compare
                // Since the first 2 lines of a SEQUEST parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

                const bool ignoreWhitespace = true;

                if (!Global.TextFilesMatch(sourceFilePath, targetFilePath, 4, 0, ignoreWhitespace, lineIgnoreRegExSpecs))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogDebug(
                            "SEQUEST parameter file in archive folder doesn't match parameter file for current job; renaming old file and copying new file to " +
                            targetFilePath);
                    }

                    // Files don't match; rename the old file
                    var archivedFile = new FileInfo(targetFilePath);

                    var newNameBase = Path.GetFileNameWithoutExtension(targetFilePath) + "_" + archivedFile.LastWriteTime.ToString("yyyy-MM-dd");
                    var newName = newNameBase + Path.GetExtension(targetFilePath);

                    // See if the renamed file exists; if it does, we'll have to tweak the name
                    var revisionNumber = 1;
                    string newFilePath;

                    while (true)
                    {
                        newFilePath = Path.Combine(targetFolderPath, newName);

                        if (!File.Exists(newFilePath))
                        {
                            break;
                        }

                        revisionNumber++;
                        newName = newNameBase + "_v" + revisionNumber + Path.GetExtension(targetFilePath);
                    }

                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Renaming " + targetFilePath + " to " + newFilePath);
                    }

                    archivedFile.MoveTo(newFilePath);

                    needToArchiveFile = true;
                }
            }

            if (needToArchiveFile)
            {
                // Copy the new parameter file to the archive

                if (mDebugLevel >= 4)
                {
                    LogDebug("Copying " + sourceFilePath + " to " + targetFilePath);
                }

                File.Copy(sourceFilePath, targetFilePath, true);
            }
        }

        /// <summary>
        /// Look for file _out.txt.tmp in the transfer folder
        /// Retrieves the file if it was found and if both JobParameters.xml file and the SEQUEST param file match the
        /// JobParameters.xml and SEQUEST param file in the local working directory
        /// </summary>
        /// <returns>
        /// CLOSEOUT_SUCCESS if an existing file was found and copied,
        /// CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and
        /// CLOSEOUT_FAILURE if an error
        /// </returns>
        private CloseOutType CheckForExistingConcatenatedOutFile()
        {
            try
            {
                var jobNum = mJobParams.GetParam("Job");
                var transferDirectoryPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_TRANSFER_DIRECTORY_PATH);

                if (string.IsNullOrWhiteSpace(transferDirectoryPath))
                {
                    // Transfer folder path is not defined
                    LogWarning("transferDirectoryPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATASET_FOLDER_NAME));
                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_OUTPUT_FOLDER_NAME));

                if (mDebugLevel >= 4)
                {
                    LogDebug("Checking for " + AnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file at " + transferDirectoryPath);
                }

                var sourceDirectory = new DirectoryInfo(transferDirectoryPath);

                if (!sourceDirectory.Exists)
                {
                    // Transfer folder not found; return false
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... Transfer folder not found: " + sourceDirectory.FullName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var concatenatedTempFilePath = Path.Combine(sourceDirectory.FullName,
                    DatasetName + AnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE);

                var tempOutFile = new FileInfo(concatenatedTempFilePath);

                if (!tempOutFile.Exists)
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... " + AnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file not found");
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(
                        AnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file found for job " + jobNum + " (file size = " +
                        (tempOutFile.Length / 1024.0).ToString("#,##0") +
                        " KB); comparing JobParameters.xml file and SEQUEST parameter file to local copies");
                }

                // Compare the remote and local copies of the JobParameters file
                var fileNameToCompare = "JobParameters_" + jobNum + ".xml";
                var remoteFilePath = Path.Combine(sourceDirectory.FullName, fileNameToCompare + ".tmp");
                var localFilePath = Path.Combine(mWorkDir, fileNameToCompare);

                var filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "Job Parameters");

                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Compare the remote and local copies of the SEQUEST Parameter file
                fileNameToCompare = mJobParams.GetParam("ParmFileName");
                remoteFilePath = Path.Combine(sourceDirectory.FullName, fileNameToCompare + ".tmp");
                localFilePath = Path.Combine(mWorkDir, fileNameToCompare);

                filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "SEQUEST Parameter");

                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Everything matches up; copy tempOutFile locally
                try
                {
                    tempOutFile.CopyTo(Path.Combine(mWorkDir, tempOutFile.Name), true);

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Copied " + tempOutFile.Name + " locally; will resume SEQUEST analysis");
                    }

                    // If the job succeeds, we should delete the _out.txt.tmp file from the transfer folder
                    // Add the full path to ServerFilesToDelete using AddServerFileToDelete
                    mJobParams.AddServerFileToDelete(tempOutFile.FullName);
                }
                catch (Exception ex)
                {
                    // Error copying the file; treat this as a failed job
                    mMessage = " Exception copying " + AnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file locally";
                    LogError("  ... Exception copying " + tempOutFile.FullName + " locally; unable to resume: " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for a sequest.log.tmp file
                var logFiles = sourceDirectory.GetFiles("sequest.log.tmp").ToList();

                if (logFiles.Count == 0)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                var firstLogFile = logFiles.First();

                // Copy the sequest.log.tmp file to the work directory, but rename it to include a time stamp
                var existingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(firstLogFile.Name);
                existingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(existingSeqLogFileRenamed);
                existingSeqLogFileRenamed += "_" + firstLogFile.LastWriteTime.ToString("yyyyMMdd_HHmm") + ".log";

                try
                {
                    localFilePath = Path.Combine(mWorkDir, existingSeqLogFileRenamed);
                    firstLogFile.CopyTo(localFilePath, true);

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Copied " + Path.GetFileName(firstLogFile.Name) + " locally, renaming to " + existingSeqLogFileRenamed);
                    }

                    mJobParams.AddServerFileToDelete(firstLogFile.FullName);

                    // Copy the new file back to the transfer folder (necessary in case this job fails)
                    File.Copy(localFilePath, Path.Combine(transferDirectoryPath, existingSeqLogFileRenamed));
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error in CheckForExistingConcatenatedOutFile";
                LogError("Error in CheckForExistingConcatenatedOutFile: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool CompareRemoteAndLocalFilesForResume(string remoteFilePath, string localFilePath, string fileDescription)
        {
            if (!File.Exists(remoteFilePath))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("  ... " + fileDescription + " file not found remotely; unable to resume: " + remoteFilePath);
                }
                return false;
            }

            if (!File.Exists(localFilePath))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("  ... " + fileDescription + " file not found locally; unable to resume: " + localFilePath);
                }
                return false;
            }

            const bool ignoreWhitespace = true;

            if (Global.TextFilesMatch(remoteFilePath, localFilePath, 0, 0, ignoreWhitespace))
            {
                return true;
            }

            LogDebug("  ... " + fileDescription + " file at " + remoteFilePath + " doesn't match local file; unable to resume");
            return false;
        }

        /// <summary>
        /// Retrieves files necessary for performance of SEQUEST analysis
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

            // Retrieve FASTA file (we'll distribute it to the cluster nodes later in this method)
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Make sure the SEQUEST parameter file is present in the parameter file storage path
            ArchiveSequestParamFile();

            // Look for an existing _out.txt.tmp file in the transfer folder on the storage server
            // If one exists, and if the parameter file and settings file associated with the file match the ones in the work folder, copy it locally
            var eExistingOutFileResult = CheckForExistingConcatenatedOutFile();

            if (eExistingOutFileResult == CloseOutType.CLOSEOUT_FAILED)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Call to CheckForExistingConcatenatedOutFile failed";
                }
                return resultCode;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            // The file will be deconcatenated by method AnalysisToolRunnerSeqBase.CheckForExistingConcatenatedOutFile
            if (!FileSearchTool.RetrieveDtaFiles())
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // If running on a cluster, distribute the database file across the nodes
            // We do this after we have successfully retrieved the DTA files and unzipped them
            if (mMgrParams.GetParam("cluster", true))
            {
                // Check the cluster nodes, updating local database copies as necessary
                var fastaFileName = mJobParams.GetParam("PeptideSearch", "generatedFastaName");

                if (string.IsNullOrEmpty(fastaFileName))
                {
                    mMessage = "generatedFastaName parameter is empty; RetrieveOrgDB did not create a FASTA file";
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!VerifyDatabase(fastaFileName, orgDbDirectoryPath))
                {
                    // Errors were reported in method call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            mJobParams.AddResultFileExtensionToSkip(".tmp");        // Temp files

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Verifies the FASTA file required by the job is distributed to all the cluster nodes
        /// </summary>
        /// <param name="fastaFileName">FASTA file name</param>
        /// <param name="orgDbDirectoryPath">FASTA file location on analysis machine</param>
        /// <returns>True if success, false if an error</returns>
        private bool VerifyDatabase(string fastaFileName, string orgDbDirectoryPath)
        {
            var hostFilePath = mMgrParams.GetParam("HostsFileLocation");
            var nodeDbLoc = mMgrParams.GetParam("NodeDbLocation");

            string logMessage;

            LogMessage("Copying database to nodes: " + Path.GetFileName(fastaFileName));

            // Get the list of nodes from the hosts file
            var nodes = GetHostList(hostFilePath);

            if (nodes == null || nodes.Count == 0)
            {
                mMessage = "Unable to determine node names from host file";
                LogError(mMessage + ": " + hostFilePath);
                return false;
            }

            // Define the path to the database on the head node
            var fastaFilePath = Path.Combine(orgDbDirectoryPath, fastaFileName);

            if (!File.Exists(fastaFilePath))
            {
                mMessage = "Database file can't be found on master";
                LogError(mMessage + ": " + fastaFilePath);
                return false;
            }

            // For each node, verify specified database file is present and matches file on host
            // Allow up to 25% of the nodes to fail (they should just get skipped when the SEQUEST search occurs)

            var nodeCountProcessed = 0;
            var nodeCountFailed = 0;
            var nodeCountFileAlreadyExists = 0;
            var nodeCountNotEnoughFreeSpace = 0;

            foreach (var nodeName in nodes)
            {
                if (!VerifyRemoteDatabase(fastaFilePath, @"\\" + nodeName + @"\" + nodeDbLoc, out var fileAlreadyExists, out var notEnoughFreeSpace))
                {
                    nodeCountFailed++;
                    fileAlreadyExists = true;

                    if (notEnoughFreeSpace)
                    {
                        nodeCountNotEnoughFreeSpace++;
                    }
                }

                nodeCountProcessed++;

                if (fileAlreadyExists)
                    nodeCountFileAlreadyExists++;
            }

            if (nodeCountProcessed == 0)
            {
                mMessage = "The Nodes collection is empty; unable to continue";
                LogError(mMessage);
                return false;
            }

            if (nodeCountFailed > 0)
            {
                const int MINIMUM_NODE_SUCCESS_PCT = 75;
                double nodeCountSuccessPct = (nodeCountProcessed - nodeCountFailed) / (float)nodeCountProcessed * 100;

                logMessage = "Error, unable to verify database on " + nodeCountFailed + " node";

                if (nodeCountFailed > 1)
                    logMessage += "s";

                logMessage += " (" + nodeCountSuccessPct.ToString("0") + "% succeeded)";

                LogError(logMessage);

                if (nodeCountSuccessPct < MINIMUM_NODE_SUCCESS_PCT)
                {
                    mMessage = "Unable to copy the database file one or more nodes; ";

                    if (nodeCountNotEnoughFreeSpace > 0)
                    {
                        mMessage = "not enough space on the disk";
                    }
                    else
                    {
                        mMessage = "see " + mMgrName + " manager log for details";
                    }

                    LogError("Aborting since did not succeed on at least " + MINIMUM_NODE_SUCCESS_PCT + "% of the nodes");
                    return false;
                }

                LogError("Warning, will continue analysis using the remaining nodes");

                // Decrement nodeCountProcessed by nodeCountFailed so the stats in the next If / EndIf block are valid
                nodeCountProcessed -= nodeCountFailed;
            }

            if (mDebugLevel >= 1)
            {
                if (nodeCountFileAlreadyExists == 0)
                {
                    LogMessage("Copied database to " + nodeCountProcessed + " nodes");
                }
                else
                {
                    logMessage = "Verified database exists on " + nodeCountProcessed + " nodes";

                    if (nodeCountProcessed - nodeCountFileAlreadyExists > 0)
                    {
                        logMessage += " (newly copied to " + (nodeCountProcessed - nodeCountFileAlreadyExists) + " nodes)";
                    }

                    LogMessage(logMessage);
                }
            }

            // Database file has been distributed, so return happy
            return true;
        }

        /// <summary>
        /// Reads the list of nodes from the hosts config file
        /// </summary>
        /// <param name="hostFilePath">Name of hosts file on cluster head node</param>
        /// <returns>returns a string collection containing IP addresses for each node</returns>
        private List<string> GetHostList(string hostFilePath)
        {
            var nodes = new List<string>();
            string[] separators = { " " };

            try
            {
                using var reader = new StreamReader(new FileStream(hostFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    // Read the line from the file and check to see if it contains a node IP address.
                    // If it does, add the IP address to the collection of addresses
                    var dataLine = reader.ReadLine();

                    // Verify the line isn't a comment line
                    if (!string.IsNullOrWhiteSpace(dataLine) && !dataLine.Contains("#"))
                    {
                        // Parse the node name and add it to the collection
                        var dataCols = dataLine.Split(separators, StringSplitOptions.RemoveEmptyEntries);

                        if (dataCols.Length >= 1)
                        {
                            if (!nodes.Contains(dataCols[0]))
                            {
                                nodes.Add(dataCols[0]);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading cluster config file '" + hostFilePath + "': " + ex.Message);
                return null;
            }

            // Return the list of nodes, if any
            return nodes;
        }

        private bool VerifyFilesMatchSizeAndDate(string file1, string file2)
        {
            const int DETAILED_LOG_THRESHOLD = 3;

            if (!File.Exists(file1) || !File.Exists(file2))
                return false;

            // Files both exist
            var fileA = new FileInfo(file1);
            var fileB = new FileInfo(file2);

            if (mDebugLevel > DETAILED_LOG_THRESHOLD)
            {
                LogDebug("Comparing files: " + fileA.FullName + " vs. " + fileB.FullName);
                LogDebug(" ... file sizes: " + fileA.Length + " vs. " + fileB.Length);
                LogDebug(" ... file dates: " + fileA.LastWriteTimeUtc + " vs. " + fileB.LastWriteTimeUtc);
            }

            if (fileA.Length != fileB.Length)
                return false;

            // Sizes match
            if (fileA.LastWriteTimeUtc == fileB.LastWriteTimeUtc)
            {
                // Dates match
                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match exactly");
                }

                return true;
            }

            // Dates don't match, are they off by one hour?
            var secondDiff = Math.Abs(fileA.LastWriteTimeUtc.Subtract(fileB.LastWriteTimeUtc).TotalSeconds);

            if (secondDiff <= 2)
            {
                // File times differ by less than 2 seconds; count this as the same

                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match within 2 seconds (" + secondDiff.ToString("0.0") + " seconds apart)");
                }

                return true;
            }

            if (secondDiff >= 3598 && secondDiff <= 3602)
            {
                // File times are an hour apart (give or take 2 seconds); count this as the same

                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match within 1 hour (" + secondDiff.ToString("0.0") + " seconds apart)");
                }

                return true;
            }

            if (mDebugLevel >= DETAILED_LOG_THRESHOLD)
            {
                if (mDebugLevel == DETAILED_LOG_THRESHOLD)
                {
                    // This message didn't get logged above; log it now.
                    LogDebug("Comparing files: " + fileA.FullName + " vs. " + fileB.FullName);
                }

                LogDebug(
                    " ... sizes match but times do not match within 2 seconds or 1 hour (" + secondDiff.ToString("0.0") +
                    " seconds apart)");
            }

            return false;
        }

        /// <summary>
        /// Verifies specified database is present on the node. If present, compares date and size. If not
        ///    present, copies database from master
        /// </summary>
        /// <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
        /// <param name="sourceFastaPath">Full path to the source file</param>
        /// <param name="destPath">FASTA storage location on cluster node</param>
        /// <param name="fileAlreadyExists">Output parameter: true if the file already exists</param>
        /// <param name="notEnoughFreeSpace">Output parameter: true if the target node does not have enough space for the file</param>
        /// <returns>True if success, false if an error</returns>
        private bool VerifyRemoteDatabase(string sourceFastaPath, string destPath, out bool fileAlreadyExists, out bool notEnoughFreeSpace)
        {
            fileAlreadyExists = false;
            notEnoughFreeSpace = false;

            if (mDebugLevel > 3)
            {
                LogMessage("Verifying database " + destPath);
            }

            if (string.IsNullOrWhiteSpace(sourceFastaPath))
            {
                LogError("Null or empty source FASTA path sent to VerifyRemoteDatabase");
                return false;
            }

            var destFilePath = Path.Combine(destPath, Path.GetFileName(sourceFastaPath));
            try
            {
                bool copyNeeded;

                if (File.Exists(destFilePath))
                {
                    // File was found on node, compare file size and date (allowing for a 1 hour difference in case of daylight savings)
                    if (VerifyFilesMatchSizeAndDate(sourceFastaPath, destFilePath))
                    {
                        fileAlreadyExists = true;
                        copyNeeded = false;
                    }
                    else
                    {
                        copyNeeded = true;
                    }
                }
                else
                {
                    // File wasn't on node, we'll have to copy it
                    copyNeeded = true;
                }

                // Does the file need to be copied to the node?
                if (copyNeeded)
                {
                    // Copy the file
                    if (mDebugLevel > 3)
                    {
                        LogMessage("Copying database file " + destFilePath);
                    }
                    File.Copy(sourceFastaPath, destFilePath, true);

                    // Now everything is in its proper place, so return
                    return true;
                }

                // File existed and was current, so everybody's happy
                if (mDebugLevel >= 3)
                {
                    LogMessage("Database file at " + destFilePath + " matches the source file's date and time; will not re-copy");
                }
                return true;
            }
            catch (Exception ex)
            {
                // Something bad happened
                LogError("Error copying database file to " + destFilePath + ": " + ex.Message, ex);

                if (ex.Message.Contains("not enough space"))
                {
                    notEnoughFreeSpace = true;
                }
                return false;
            }
        }
    }
}
