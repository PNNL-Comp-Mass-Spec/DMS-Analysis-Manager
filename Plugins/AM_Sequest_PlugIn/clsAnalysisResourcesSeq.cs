//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/06
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Subclass for Sequest-specific tasks:
    /// 1) Distributes OrgDB files to cluster nodes if running on a cluster
    /// 2) Uses ParamFileGenerator to create Sequest param file from database instead of copying it
    /// 3) Retrieves zipped DTA files
    /// 4) Retrieves _out.txt.tmp file (if it exists)
    /// </summary>
    public class clsAnalysisResourcesSeq : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        protected void ArchiveSequestParamFile()
        {
            var sourceFilePath = "";
            var targetFolderPath = "";

            try
            {
                sourceFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParmFileName"));
                targetFolderPath = mJobParams.GetParam("ParmFileStoragePath");

                if (mDebugLevel >= 3)
                {
                    LogDebug("Verifying that the Sequest parameter file " + mJobParams.GetParam("ParmFileName") + " exists in " + targetFolderPath);
                }

                ArchiveSequestParamFile(sourceFilePath, targetFolderPath);
            }
            catch (Exception ex)
            {
                if (targetFolderPath == null)
                    targetFolderPath = "??";

                mMessage = "Error archiving param file to ParmFileStoragePath";
                LogErrorToDatabase(mMessage + ": " + sourceFilePath + " --> " + targetFolderPath + ": " + ex.Message);
            }
        }

        public void ArchiveSequestParamFile(string sourceFilePath, string targetFolderPath)
        {
            var lstLineIgnoreRegExSpecs = new List<Regex> {
                new Regex(@"mass_type_parent *=.*")
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
                    LogDebug("Sequest parameter file not found in archive folder; copying to " + targetFilePath);
                }

                needToArchiveFile = true;
            }
            else
            {
                // Read the files line-by-line and compare
                // Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

                const bool ignoreWhitespace = true;

                if (!clsGlobal.TextFilesMatch(sourceFilePath, targetFilePath, 4, 0, ignoreWhitespace, lstLineIgnoreRegExSpecs))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogDebug(
                            "Sequest parameter file in archive folder doesn't match parameter file for current job; renaming old file and copying new file to " +
                            targetFilePath);
                    }

                    // Files don't match; rename the old file
                    var fiArchivedFile = new FileInfo(targetFilePath);

                    var newNameBase = Path.GetFileNameWithoutExtension(targetFilePath) + "_" + fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd");
                    var newName = newNameBase + Path.GetExtension(targetFilePath);

                    // See if the renamed file exists; if it does, we'll have to tweak the name
                    var revisionNumber = 1;
                    string newFilePath;
                    do
                    {
                        newFilePath = Path.Combine(targetFolderPath, newName);
                        if (!File.Exists(newFilePath))
                        {
                            break;
                        }

                        revisionNumber += 1;
                        newName = newNameBase + "_v" + revisionNumber + Path.GetExtension(targetFilePath);
                    } while (true);

                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Renaming " + targetFilePath + " to " + newFilePath);
                    }

                    fiArchivedFile.MoveTo(newFilePath);

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
        /// Retrieves the file if it was found and if both JobParameters.xml file and the sequest param file match the
        /// JobParameters.xml and sequest param file in the local working directory
        /// </summary>
        /// <returns>
        /// CLOSEOUT_SUCCESS if an existing file was found and copied,
        /// CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and
        /// CLOSEOUT_FAILURE if an error
        /// </returns>
        protected CloseOutType CheckForExistingConcatenatedOutFile()
        {
            try
            {
                var jobNum = mJobParams.GetParam("Job");
                var transferFolderPath = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_TRANSFER_FOLDER_PATH);

                if (string.IsNullOrWhiteSpace(transferFolderPath))
                {
                    // Transfer folder path is not defined
                    LogWarning("transferFolderPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                transferFolderPath = Path.Combine(transferFolderPath, mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATASET_FOLDER_NAME));
                transferFolderPath = Path.Combine(transferFolderPath, mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_OUTPUT_FOLDER_NAME));

                if (mDebugLevel >= 4)
                {
                    LogDebug("Checking for " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file at " + transferFolderPath);
                }

                var diSourceFolder = new DirectoryInfo(transferFolderPath);

                if (!diSourceFolder.Exists)
                {
                    // Transfer folder not found; return false
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... Transfer folder not found: " + diSourceFolder.FullName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var concatenatedTempFilePath = Path.Combine(diSourceFolder.FullName,
                    DatasetName + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE);

                var fiTempOutFile = new FileInfo(concatenatedTempFilePath);
                if (!fiTempOutFile.Exists)
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file not found");
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(
                        clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file found for job " + jobNum + " (file size = " +
                        (fiTempOutFile.Length / 1024.0).ToString("#,##0") +
                        " KB); comparing JobParameters.xml file and Sequest parameter file to local copies");
                }

                // Compare the remote and local copies of the JobParameters file
                var fileNameToCompare = "JobParameters_" + jobNum + ".xml";
                var remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare + ".tmp");
                var localFilePath = Path.Combine(mWorkDir, fileNameToCompare);

                var filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "Job Parameters");
                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Compare the remote and local copies of the Sequest Parameter file
                fileNameToCompare = mJobParams.GetParam("ParmFileName");
                remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare + ".tmp");
                localFilePath = Path.Combine(mWorkDir, fileNameToCompare);

                filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "Sequest Parameter");
                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Everything matches up; copy fiTempOutFile locally
                try
                {
                    fiTempOutFile.CopyTo(Path.Combine(mWorkDir, fiTempOutFile.Name), true);

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Copied " + fiTempOutFile.Name + " locally; will resume Sequest analysis");
                    }

                    // If the job succeeds, we should delete the _out.txt.tmp file from the transfer folder
                    // Add the full path to ServerFilesToDelete using AddServerFileToDelete
                    mJobParams.AddServerFileToDelete(fiTempOutFile.FullName);
                }
                catch (Exception ex)
                {
                    // Error copying the file; treat this as a failed job
                    mMessage = " Exception copying " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file locally";
                    LogError("  ... Exception copying " + fiTempOutFile.FullName + " locally; unable to resume: " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for a sequest.log.tmp file
                var lstLogFiles = diSourceFolder.GetFiles("sequest.log.tmp").ToList();

                if (lstLogFiles.Count <= 0)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                var fiFirstLogFile = lstLogFiles.First();

                // Copy the sequest.log.tmp file to the work directory, but rename it to include a time stamp
                var strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(fiFirstLogFile.Name);
                strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(strExistingSeqLogFileRenamed);
                strExistingSeqLogFileRenamed += "_" + fiFirstLogFile.LastWriteTime.ToString("yyyyMMdd_HHmm") + ".log";

                try
                {
                    localFilePath = Path.Combine(mWorkDir, strExistingSeqLogFileRenamed);
                    fiFirstLogFile.CopyTo(localFilePath, true);

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Copied " + Path.GetFileName(fiFirstLogFile.Name) + " locally, renaming to " + strExistingSeqLogFileRenamed);
                    }

                    mJobParams.AddServerFileToDelete(fiFirstLogFile.FullName);

                    // Copy the new file back to the transfer folder (necessary in case this job fails)
                    File.Copy(localFilePath, Path.Combine(transferFolderPath, strExistingSeqLogFileRenamed));
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

        protected bool CompareRemoteAndLocalFilesForResume(string strRemoteFilePath, string strLocalFilePath, string strFileDescription)
        {
            if (!File.Exists(strRemoteFilePath))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("  ... " + strFileDescription + " file not found remotely; unable to resume: " + strRemoteFilePath);
                }
                return false;
            }

            if (!File.Exists(strLocalFilePath))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("  ... " + strFileDescription + " file not found locally; unable to resume: " + strLocalFilePath);
                }
                return false;
            }

            const bool ignoreWhitespace = true;

            if (clsGlobal.TextFilesMatch(strRemoteFilePath, strLocalFilePath, 0, 0, ignoreWhitespace))
            {
                return true;
            }

            LogDebug("  ... " + strFileDescription + " file at " + strRemoteFilePath + " doesn't match local file; unable to resume");
            return false;
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

            // Retrieve Fasta file (we'll distribute it to the cluster nodes later in this function)
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
            if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                return resultCode;

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(mJobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Make sure the Sequest parameter file is present in the parameter file storage path
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
            // The file will be de-concatenated by function clsAnalysisToolRunnerSeqBase.CheckForExistingConcatenatedOutFile
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
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
                    mMessage = "generatedFastaName parameter is empty; RetrieveOrgDB did not create a fasta file";
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!VerifyDatabase(fastaFileName, orgDbDirectoryPath))
                {
                    // Errors were reported in function call, so just return
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
        /// Verifies the fasta file required by the job is distributed to all the cluster nodes
        /// </summary>
        /// <param name="fastaFileName">Fasta file name</param>
        /// <param name="orgDbDirectoryPath">Fasta file location on analysis machine</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool VerifyDatabase(string fastaFileName, string orgDbDirectoryPath)
        {
            var hostFilePath = mMgrParams.GetParam("hostsfilelocation");
            var nodeDbLoc = mMgrParams.GetParam("nodedblocation");

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
            // Allow up to 25% of the nodes to fail (they should just get skipped when the Sequest search occurs)

            var nodeCountProcessed = 0;
            var nodeCountFailed = 0;
            var nodeCountFileAlreadyExists = 0;
            var nodeCountNotEnoughFreeSpace = 0;

            foreach (var nodeName in nodes)
            {
                if (!VerifyRemoteDatabase(fastaFilePath, @"\\" + nodeName + @"\" + nodeDbLoc, out var fileAlreadyExists, out var notEnoughFreeSpace))
                {
                    nodeCountFailed += 1;
                    fileAlreadyExists = true;

                    if (notEnoughFreeSpace)
                    {
                        nodeCountNotEnoughFreeSpace += 1;
                    }
                }

                nodeCountProcessed += 1;
                if (fileAlreadyExists)
                    nodeCountFileAlreadyExists += 1;
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
                double dblNodeCountSuccessPct = (nodeCountProcessed - nodeCountFailed) / (float)nodeCountProcessed * 100;

                logMessage = "Error, unable to verify database on " + nodeCountFailed + " node";
                if (nodeCountFailed > 1)
                    logMessage += "s";

                logMessage += " (" + dblNodeCountSuccessPct.ToString("0") + "% succeeded)";

                LogError(logMessage);

                if (dblNodeCountSuccessPct < MINIMUM_NODE_SUCCESS_PCT)
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
        /// <param name="HostFilePath">Name of hosts file on cluster head node</param>
        /// <returns>returns a string collection containing IP addresses for each node</returns>
        /// <remarks></remarks>
        private List<string> GetHostList(string HostFilePath)
        {
            var lstNodes = new List<string>();
            string[] separators = { " " };

            try
            {
                using (var srHostFile = new StreamReader(new FileStream(HostFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srHostFile.EndOfStream)
                    {
                        // Read the line from the file and check to see if it contains a node IP address.
                        // If it does, add the IP address to the collection of addresses
                        var InpLine = srHostFile.ReadLine();

                        // Verify the line isn't a comment line
                        if (!string.IsNullOrWhiteSpace(InpLine) && !InpLine.Contains("#"))
                        {
                            // Parse the node name and add it to the collection
                            var LineFields = InpLine.Split(separators, StringSplitOptions.RemoveEmptyEntries);
                            if (LineFields.Length >= 1)
                            {
                                if (!lstNodes.Contains(LineFields[0]))
                                {
                                    lstNodes.Add(LineFields[0]);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading cluster config file '" + HostFilePath + "': " + ex.Message);
                return null;
            }

            // Return the list of nodes, if any
            return lstNodes;
        }

        private bool VerifyFilesMatchSizeAndDate(string file1, string file2)
        {
            const int DETAILED_LOG_THRESHOLD = 3;

            if (!File.Exists(file1) || !File.Exists(file2))
                return false;

            // Files both exist
            var ioFileA = new FileInfo(file1);
            var ioFileB = new FileInfo(file2);

            if (mDebugLevel > DETAILED_LOG_THRESHOLD)
            {
                LogDebug("Comparing files: " + ioFileA.FullName + " vs. " + ioFileB.FullName);
                LogDebug(" ... file sizes: " + ioFileA.Length + " vs. " + ioFileB.Length);
                LogDebug(" ... file dates: " + ioFileA.LastWriteTimeUtc + " vs. " + ioFileB.LastWriteTimeUtc);
            }

            if (ioFileA.Length != ioFileB.Length)
                return false;

            // Sizes match
            if (ioFileA.LastWriteTimeUtc == ioFileB.LastWriteTimeUtc)
            {
                // Dates match
                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match exactly");
                }

                return true;
            }

            // Dates don't match, are they off by one hour?
            var dblSecondDiff = Math.Abs(ioFileA.LastWriteTimeUtc.Subtract(ioFileB.LastWriteTimeUtc).TotalSeconds);

            if (dblSecondDiff <= 2)
            {
                // File times differ by less than 2 seconds; count this as the same

                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match within 2 seconds (" + dblSecondDiff.ToString("0.0") + " seconds apart)");
                }

                return true;
            }

            if (dblSecondDiff >= 3598 && dblSecondDiff <= 3602)
            {
                // File times are an hour apart (give or take 2 seconds); count this as the same

                if (mDebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug(" ... sizes match and dates match within 1 hour (" + dblSecondDiff.ToString("0.0") + " seconds apart)");
                }

                return true;
            }

            if (mDebugLevel >= DETAILED_LOG_THRESHOLD)
            {
                if (mDebugLevel == DETAILED_LOG_THRESHOLD)
                {
                    // This message didn't get logged above; log it now.
                    LogDebug("Comparing files: " + ioFileA.FullName + " vs. " + ioFileB.FullName);
                }

                LogDebug(
                    " ... sizes match but times do not match within 2 seconds or 1 hour (" + dblSecondDiff.ToString("0.0") +
                    " seconds apart)");
            }

            return false;
        }

        /// <summary>
        /// Verifies specified database is present on the node. If present, compares date and size. If not
        ///	present, copies database from master
        /// </summary>
        /// <param name="sourceFastaPath">Full path to the source file</param>
        /// <param name="destPath">Fasta storage location on cluster node</param>
        /// <param name="fileAlreadyExists">Output parameter: true if the file already exists</param>
        /// <param name="notEnoughFreeSpace">Output parameter: true if the target node does not have enough space for the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
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
