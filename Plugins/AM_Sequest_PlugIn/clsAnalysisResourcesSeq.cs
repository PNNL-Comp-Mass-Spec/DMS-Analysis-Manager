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
        #region "Methods"

        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        protected void ArchiveSequestParamFile()
        {
            var strSrcFilePath = "";
            var strTargetFolderPath = "";

            try
            {
                strSrcFilePath = Path.Combine(m_WorkingDir, m_jobParams.GetParam("ParmFileName"));
                strTargetFolderPath = m_jobParams.GetParam("ParmFileStoragePath");

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Verifying that the Sequest parameter file " + m_jobParams.GetParam("ParmFileName") + " exists in " + strTargetFolderPath);
                }

                ArchiveSequestParamFile(strSrcFilePath, strTargetFolderPath);
            }
            catch (Exception ex)
            {
                if (strSrcFilePath == null)
                    strSrcFilePath = "??";
                if (strTargetFolderPath == null)
                    strTargetFolderPath = "??";

                m_message = "Error archiving param file to ParmFileStoragePath";
                LogErrorToDatabase(m_message + ": " + strSrcFilePath + " --> " + strTargetFolderPath + ": " + ex.Message);
            }
        }

        public void ArchiveSequestParamFile(string strSrcFilePath, string strTargetFolderPath)
        {
            var blnNeedToArchiveFile = false;
            string strTargetFilePath = null;

            string strNewNameBase = null;
            string strNewName = null;
            string strNewPath = null;

            var intRevisionNumber = 0;

            var lstLineIgnoreRegExSpecs = new List<Regex>();
            lstLineIgnoreRegExSpecs.Add(new Regex(@"mass_type_parent *=.*"));

            blnNeedToArchiveFile = false;

            strTargetFilePath = Path.Combine(strTargetFolderPath, Path.GetFileName(strSrcFilePath));

            if (!File.Exists(strTargetFilePath))
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("Sequest parameter file not found in archive folder; copying to " + strTargetFilePath);
                }

                blnNeedToArchiveFile = true;
            }
            else
            {
                // Read the files line-by-line and compare
                // Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

                const bool ignoreWhitespace = true;

                if (!clsGlobal.TextFilesMatch(strSrcFilePath, strTargetFilePath, 4, 0, ignoreWhitespace, lstLineIgnoreRegExSpecs))
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogDebug(
                            "Sequest parameter file in archive folder doesn't match parameter file for current job; renaming old file and copying new file to " +
                            strTargetFilePath);
                    }

                    // Files don't match; rename the old file
                    var fiArchivedFile = new FileInfo(strTargetFilePath);

                    strNewNameBase = Path.GetFileNameWithoutExtension(strTargetFilePath) + "_" + fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd");
                    strNewName = strNewNameBase + Path.GetExtension(strTargetFilePath);

                    // See if the renamed file exists; if it does, we'll have to tweak the name
                    intRevisionNumber = 1;
                    do
                    {
                        strNewPath = Path.Combine(strTargetFolderPath, strNewName);
                        if (!File.Exists(strNewPath))
                        {
                            break;
                        }

                        intRevisionNumber += 1;
                        strNewName = strNewNameBase + "_v" + intRevisionNumber + Path.GetExtension(strTargetFilePath);
                    } while (true);

                    if (m_DebugLevel >= 2)
                    {
                        LogDebug("Renaming " + strTargetFilePath + " to " + strNewPath);
                    }

                    fiArchivedFile.MoveTo(strNewPath);

                    blnNeedToArchiveFile = true;
                }
            }

            if (blnNeedToArchiveFile)
            {
                // Copy the new parameter file to the archive

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Copying " + strSrcFilePath + " to " + strTargetFilePath);
                }

                File.Copy(strSrcFilePath, strTargetFilePath, true);
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
                var strJob = m_jobParams.GetParam("Job");
                var transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath");

                if (string.IsNullOrWhiteSpace(transferFolderPath))
                {
                    // Transfer folder path is not defined
                    LogWarning("transferFolderPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"));
                    transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"));
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Checking for " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file at " + transferFolderPath);
                }

                var diSourceFolder = new DirectoryInfo(transferFolderPath);

                if (!diSourceFolder.Exists)
                {
                    // Transfer folder not found; return false
                    if (m_DebugLevel >= 4)
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
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("  ... " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file not found");
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file found for job " + strJob + " (file size = " +
                        (fiTempOutFile.Length / 1024.0).ToString("#,##0") +
                        " KB); comparing JobParameters.xml file and Sequest parameter file to local copies");
                }

                // Compare the remote and local copies of the JobParameters file
                var fileNameToCompare = "JobParameters_" + strJob + ".xml";
                var remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare + ".tmp");
                var localFilePath = Path.Combine(m_WorkingDir, fileNameToCompare);

                var filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "JobParameters");
                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Compare the remote and local copies of the Sequest Parameter file
                fileNameToCompare = m_jobParams.GetParam("ParmFileName");
                remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare + ".tmp");
                localFilePath = Path.Combine(m_WorkingDir, fileNameToCompare);

                filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "Sequest Parameter");
                if (!filesMatch)
                {
                    // Files don't match; do not resume
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Everything matches up; copy fiTempOutFile locally
                try
                {
                    fiTempOutFile.CopyTo(Path.Combine(m_WorkingDir, fiTempOutFile.Name), true);

                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Copied " + fiTempOutFile.Name + " locally; will resume Sequest analysis");
                    }

                    // If the job succeeds, we should delete the _out.txt.tmp file from the transfer folder
                    // Add the full path to m_ServerFilesToDelete using AddServerFileToDelete
                    m_jobParams.AddServerFileToDelete(fiTempOutFile.FullName);
                }
                catch (Exception ex)
                {
                    // Error copying the file; treat this as a failed job
                    m_message = " Exception copying " + clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE + " file locally";
                    LogError("  ... Exception copying " + fiTempOutFile.FullName + " locally; unable to resume: " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for a sequest.log.tmp file
                var lstLogFiles = diSourceFolder.GetFiles("sequest.log.tmp").ToList();

                if (lstLogFiles.Count > 0)
                {
                    string strExistingSeqLogFileRenamed = null;
                    var fiFirstLogFile = lstLogFiles.First();

                    // Copy the sequest.log.tmp file to the work directory, but rename it to include a time stamp
                    strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(fiFirstLogFile.Name);
                    strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(strExistingSeqLogFileRenamed);
                    strExistingSeqLogFileRenamed += "_" + fiFirstLogFile.LastWriteTime.ToString("yyyyMMdd_HHmm") + ".log";

                    try
                    {
                        localFilePath = Path.Combine(m_WorkingDir, strExistingSeqLogFileRenamed);
                        fiFirstLogFile.CopyTo(localFilePath, true);

                        if (m_DebugLevel >= 3)
                        {
                            LogDebug("Copied " + Path.GetFileName(fiFirstLogFile.Name) + " locally, renaming to " + strExistingSeqLogFileRenamed);
                        }

                        m_jobParams.AddServerFileToDelete(fiFirstLogFile.FullName);

                        // Copy the new file back to the transfer folder (necessary in case this job fails)
                        File.Copy(localFilePath, Path.Combine(transferFolderPath, strExistingSeqLogFileRenamed));
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Error in CheckForExistingConcatenatedOutFile";
                LogError("Error in CheckForExistingConcatenatedOutFile: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        protected bool CompareRemoteAndLocalFilesForResume(string strRemoteFilePath, string strLocalFilePath, string strFileDescription)
        {
            if (!File.Exists(strRemoteFilePath))
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("  ... " + strFileDescription + " file not found remotely; unable to resume: " + strRemoteFilePath);
                }
                return false;
            }

            if (!File.Exists(strLocalFilePath))
            {
                if (m_DebugLevel >= 1)
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
            else
            {
                LogDebug("  ... " + strFileDescription + " file at " + strRemoteFilePath + " doesn't match local file; unable to resume");
                return false;
            }
        }

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve Fasta file (we'll distribute it to the cluster nodes later in this function)
            var LocOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
            if (!RetrieveOrgDB(LocOrgDBFolder))
                return CloseOutType.CLOSEOUT_FAILED;

            // Retrieve param file
            if (!RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the Sequest parameter file is present in the parameter file storage path
            ArchiveSequestParamFile();

            // Look for an existing _out.txt.tmp file in the transfer folder on the storage server
            // If one exists, and if the parameter file and settings file associated with the file match the ones in the work folder, then copy it locally
            var eExistingOutFileResult = CheckForExistingConcatenatedOutFile();

            if (eExistingOutFileResult == CloseOutType.CLOSEOUT_FAILED)
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Call to CheckForExistingConcatenatedOutFile failed";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            // The file will be de-concatenated by function clsAnalysisToolRunnerSeqBase.CheckForExistingConcatenatedOutFile
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If running on a cluster, then distribute the database file across the nodes
            // We do this after we have successfully retrieved the DTA files and unzipped them
            if (m_mgrParams.GetParam("cluster", true))
            {
                // Check the cluster nodes, updating local database copies as necessary
                var OrbDBName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
                if (string.IsNullOrEmpty(OrbDBName))
                {
                    m_message = "generatedFastaName parameter is empty; RetrieveOrgDB did not create a fasta file";
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!VerifyDatabase(OrbDBName, LocOrgDBFolder))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip");    // Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt");    // Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");        // DTA files
            m_jobParams.AddResultFileExtensionToSkip(".tmp");        // Temp files

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Verifies the fasta file required by the job is distributed to all the cluster nodes
        /// </summary>
        /// <param name="OrgDBName">Fasta file name</param>
        /// <param name="OrgDBPath">Fasta file location on analysis machine</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool VerifyDatabase(string OrgDBName, string OrgDBPath)
        {
            var HostFilePath = m_mgrParams.GetParam("hostsfilelocation");
            var NodeDbLoc = m_mgrParams.GetParam("nodedblocation");

            string strLogMessage = null;

            LogMessage("Copying database to nodes: " + Path.GetFileName(OrgDBName));

            // Get the list of nodes from the hosts file
            var Nodes = GetHostList(HostFilePath);
            if (Nodes == null || Nodes.Count == 0)
            {
                m_message = "Unable to determine node names from host file";
                LogError(m_message + ": " + HostFilePath);
                return false;
            }

            // Define the path to the database on the head node
            var OrgDBFilePath = Path.Combine(OrgDBPath, OrgDBName);
            if (!File.Exists(OrgDBFilePath))
            {
                m_message = "Database file can't be found on master";
                LogError(m_message + ": " + OrgDBFilePath);
                return false;
            }

            // For each node, verify specified database file is present and matches file on host
            // Allow up to 25% of the nodes to fail (they should just get skipped when the Sequest search occurs)

            var blnFileAlreadyExists = false;
            var blnNotEnoughFreeSpace = false;

            var intNodeCountProcessed = 0;
            var intNodeCountFailed = 0;
            var intNodeCountFileAlreadyExists = 0;
            var intNodeCountNotEnoughFreeSpace = 0;

            foreach (var NodeName in Nodes)
            {
                if (!VerifyRemoteDatabase(OrgDBFilePath, @"\\" + NodeName + @"\" + NodeDbLoc, ref blnFileAlreadyExists, ref blnNotEnoughFreeSpace))
                {
                    intNodeCountFailed += 1;
                    blnFileAlreadyExists = true;

                    if (blnNotEnoughFreeSpace)
                    {
                        intNodeCountNotEnoughFreeSpace += 1;
                    }
                }

                intNodeCountProcessed += 1;
                if (blnFileAlreadyExists)
                    intNodeCountFileAlreadyExists += 1;
            }

            if (intNodeCountProcessed == 0)
            {
                m_message = "The Nodes collection is empty; unable to continue";
                LogError(m_message);
                return false;
            }

            if (intNodeCountFailed > 0)
            {
                const int MINIMUM_NODE_SUCCESS_PCT = 75;
                double dblNodeCountSuccessPct = 0;
                dblNodeCountSuccessPct = (intNodeCountProcessed - intNodeCountFailed) / (float)intNodeCountProcessed * 100;

                strLogMessage = "Error, unable to verify database on " + intNodeCountFailed + " node";
                if (intNodeCountFailed > 1)
                    strLogMessage += "s";
                strLogMessage += " (" + dblNodeCountSuccessPct.ToString("0") + "% succeeded)";

                LogError(strLogMessage);

                if (dblNodeCountSuccessPct < MINIMUM_NODE_SUCCESS_PCT)
                {
                    m_message = "Unable to copy the database file one or more nodes; ";
                    if (intNodeCountNotEnoughFreeSpace > 0)
                    {
                        m_message = "not enough space on the disk";
                    }
                    else
                    {
                        m_message = "see " + m_MgrName + " manager log for details";
                    }

                    LogError("Aborting since did not succeed on at least " + MINIMUM_NODE_SUCCESS_PCT + "% of the nodes");
                    return false;
                }
                else
                {
                    LogError("Warning, will continue analysis using the remaining nodes");

                    // Decrement intNodeCountProcessed by intNodeCountFailed so the stats in the next If / EndIf block are valid
                    intNodeCountProcessed -= intNodeCountFailed;
                }
            }

            if (m_DebugLevel >= 1)
            {
                if (intNodeCountFileAlreadyExists == 0)
                {
                    LogMessage("Copied database to " + intNodeCountProcessed + " nodes");
                }
                else
                {
                    strLogMessage = "Verified database exists on " + intNodeCountProcessed + " nodes";

                    if (intNodeCountProcessed - intNodeCountFileAlreadyExists > 0)
                    {
                        strLogMessage += " (newly copied to " + (intNodeCountProcessed - intNodeCountFileAlreadyExists) + " nodes)";
                    }

                    LogMessage(strLogMessage);
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
            string InpLine = null;
            string[] LineFields = null;
            string[] Separators = { " " };

            try
            {
                using (var srHostFile = new StreamReader(new FileStream(HostFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srHostFile.EndOfStream)
                    {
                        // Read the line from the file and check to see if it contains a node IP address.
                        // If it does, add the IP address to the collection of addresses
                        InpLine = srHostFile.ReadLine();

                        // Verify the line isn't a comment line
                        if (!string.IsNullOrWhiteSpace(InpLine) && !InpLine.Contains("#"))
                        {
                            // Parse the node name and add it to the collection
                            LineFields = InpLine.Split(Separators, StringSplitOptions.RemoveEmptyEntries);
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
            catch (Exception Err)
            {
                LogError("Error reading cluster config file '" + HostFilePath + "': " + Err.Message);
                return null;
            }

            // Return the list of nodes, if any
            return lstNodes;
        }

        private bool VerifyFilesMatchSizeAndDate(string FileA, string FileB)
        {
            const int DETAILED_LOG_THRESHOLD = 3;

            var blnFilesMatch = false;
            ;
            double dblSecondDiff = 0;

            blnFilesMatch = false;
            if (File.Exists(FileA) && File.Exists(FileB))
            {
                // Files both exist
                var ioFileA = new FileInfo(FileA);
                var ioFileB = new FileInfo(FileB);

                if (m_DebugLevel > DETAILED_LOG_THRESHOLD)
                {
                    LogDebug("Comparing files: " + ioFileA.FullName + " vs. " + ioFileB.FullName);
                    LogDebug(" ... file sizes: " + ioFileA.Length + " vs. " + ioFileB.Length);
                    LogDebug(" ... file dates: " + ioFileA.LastWriteTimeUtc + " vs. " + ioFileB.LastWriteTimeUtc);
                }

                if (ioFileA.Length == ioFileB.Length)
                {
                    // Sizes match
                    if (ioFileA.LastWriteTimeUtc == ioFileB.LastWriteTimeUtc)
                    {
                        // Dates match
                        if (m_DebugLevel > DETAILED_LOG_THRESHOLD)
                        {
                            LogDebug(" ... sizes match and dates match exactly");
                        }

                        blnFilesMatch = true;
                    }
                    else
                    {
                        // Dates don't match, are they off by one hour?
                        dblSecondDiff = Math.Abs(ioFileA.LastWriteTimeUtc.Subtract(ioFileB.LastWriteTimeUtc).TotalSeconds);

                        if (dblSecondDiff <= 2)
                        {
                            // File times differ by less than 2 seconds; count this as the same

                            if (m_DebugLevel > DETAILED_LOG_THRESHOLD)
                            {
                                LogDebug(" ... sizes match and dates match within 2 seconds (" + dblSecondDiff.ToString("0.0") + " seconds apart)");
                            }

                            blnFilesMatch = true;
                        }
                        else if (dblSecondDiff >= 3598 && dblSecondDiff <= 3602)
                        {
                            // File times are an hour apart (give or take 2 seconds); count this as the same

                            if (m_DebugLevel > DETAILED_LOG_THRESHOLD)
                            {
                                LogDebug(" ... sizes match and dates match within 1 hour (" + dblSecondDiff.ToString("0.0") + " seconds apart)");
                            }

                            blnFilesMatch = true;
                        }
                        else
                        {
                            if (m_DebugLevel >= DETAILED_LOG_THRESHOLD)
                            {
                                if (m_DebugLevel == DETAILED_LOG_THRESHOLD)
                                {
                                    // This message didn't get logged above; log it now.
                                    LogDebug("Comparing files: " + ioFileA.FullName + " vs. " + ioFileB.FullName);
                                }
                                LogDebug(
                                    " ... sizes match but times do not match within 2 seconds or 1 hour (" + dblSecondDiff.ToString("0.0") +
                                    " seconds apart)");
                            }
                        }
                    }
                }
            }

            return blnFilesMatch;
        }

        /// <summary>
        /// Verifies specified database is present on the node. If present, compares date and size. If not
        ///	present, copies database from master
        /// </summary>
        /// <param name="OrgDBFilePath">Full path to the source file</param>
        /// <param name="DestPath">Fasta storage location on cluster node</param>
        /// <param name="blnFileAlreadyExists">Output parameter: true if the file already exists</param>
        /// <param name="blnNotEnoughFreeSpace">Output parameter: true if the target node does not have enough space for the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
        private bool VerifyRemoteDatabase(string OrgDBFilePath, string DestPath, ref bool blnFileAlreadyExists, ref bool blnNotEnoughFreeSpace)
        {
            var CopyNeeded = false;

            blnFileAlreadyExists = false;
            blnNotEnoughFreeSpace = false;

            if (m_DebugLevel > 3)
            {
                LogMessage("Verifying database " + DestPath);
            }

            var DestFile = Path.Combine(DestPath, Path.GetFileName(OrgDBFilePath));
            try
            {
                if (File.Exists(DestFile))
                {
                    // File was found on node, compare file size and date (allowing for a 1 hour difference in case of daylight savings)
                    if (VerifyFilesMatchSizeAndDate(OrgDBFilePath, DestFile))
                    {
                        blnFileAlreadyExists = true;
                        CopyNeeded = false;
                    }
                    else
                    {
                        CopyNeeded = true;
                    }
                }
                else
                {
                    // File wasn't on node, we'll have to copy it
                    CopyNeeded = true;
                }

                // Does the file need to be copied to the node?
                if (CopyNeeded)
                {
                    // Copy the file
                    if (m_DebugLevel > 3)
                    {
                        LogMessage("Copying database file " + DestFile);
                    }
                    File.Copy(OrgDBFilePath, DestFile, true);

                    // Now everything is in its proper place, so return
                    return true;
                }

                // File existed and was current, so everybody's happy
                if (m_DebugLevel >= 3)
                {
                    LogMessage("Database file at " + DestPath + " matches the source file's date and time; will not re-copy");
                }
                return true;
            }
            catch (Exception ex)
            {
                // Something bad happened
                LogError("Error copying database file to " + DestFile + ": " + ex.Message, ex);
                if (ex.Message.Contains("not enough space"))
                {
                    blnNotEnoughFreeSpace = true;
                }
                return false;
            }
        }

        #endregion
    }
}
