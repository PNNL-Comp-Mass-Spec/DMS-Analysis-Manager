//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Create MS-GF+ suffix array files
    /// </summary>
    public class CreateMSGFDBSuffixArrayFiles : EventNotifier
    {
        // Ignore Spelling: canno, cp, fasta, Loc, msgf, Prog, programmatically, tda, Utc, Xmx

        private const string MSGF_PLUS_INDEX_FILE_INFO_SUFFIX = ".MSGFPlusIndexFileInfo";

        private string mErrorMessage = string.Empty;

        private readonly string mMgrName;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// This will be set to true if the job cannot be run due to not enough free memory
        /// </summary>
        public bool InsufficientFreeMemory { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="managerName">Manager name</param>
        public CreateMSGFDBSuffixArrayFiles(string managerName)
        {
            mMgrName = managerName;
        }

        private CloseOutType CopyExistingIndexFilesFromRemote(
            FileInfo fastaFile, bool usingLegacyFasta, string remoteIndexDirPath,
            bool checkForLockFile, int debugLevel, float maxWaitTimeHours,
            out bool diskFreeSpaceBelowThreshold)
        {
            bool success;

            diskFreeSpaceBelowThreshold = false;

            try
            {
                var remoteIndexDirectory = new DirectoryInfo(remoteIndexDirPath);

                if (!remoteIndexDirectory.Exists)
                {
                    // This is not a critical error
                    OnDebugEvent("Remote index directory not found ({0}); indexing is required", remoteIndexDirectory);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (fastaFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of " + fastaFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (checkForLockFile)
                {
                    // Look for an existing lock file on the remote server
                    var remoteLockFile1 = new FileInfo(
                        Path.Combine(remoteIndexDirectory.FullName,
                                     fastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

                    WaitForExistingLockfile(remoteLockFile1, debugLevel, maxWaitTimeHours);
                }

                // Look for the .MSGFPlusIndexFileInfo file for this FASTA file
                var indexFileInfo = new FileInfo(
                    Path.Combine(remoteIndexDirectory.FullName,
                                 fastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                long fileSizeTotalKB = 0;

                if (!indexFileInfo.Exists)
                {
                    OnDebugEvent("{0} not found at {1}; indexing is required", indexFileInfo.Name, remoteIndexDirectory.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Read the filenames in the file
                // There should be 3 columns: FileName, FileSize, and FileDateUTC
                // When looking for existing files we only require that the FileSize match; FileDateUTC is not used

                var filesToCopy = new Dictionary<string, long>();

                using (var reader = new StreamReader(
                    new FileStream(indexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        var dataCols = dataLine.Split('\t').ToList();

                        if (dataCols.Count >= 3)
                        {
                            // Add this file to the list of files to copy
                            if (long.TryParse(dataCols[1], out var fileSizeBytes))
                            {
                                filesToCopy.Add(dataCols[0], fileSizeBytes);
                                fileSizeTotalKB += (long)(fileSizeBytes / 1024.0);
                            }
                        }
                    }
                }

                bool filesAreValid;

                if (filesToCopy.Count == 0)
                {
                    filesAreValid = false;
                }
                else
                {
                    // Confirm that each file in filesToCopy exists on the remote server
                    // If using a legacy FASTA file, must also confirm that each file is newer than the FASTA file that was indexed
                    filesAreValid = ValidateFiles(
                        remoteIndexDirectory.FullName, filesToCopy, usingLegacyFasta,
                        fastaFile.LastWriteTimeUtc, true);
                }

                if (!filesAreValid)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (debugLevel >= 1 && fileSizeTotalKB >= 1000)
                {
                    OnStatusEvent("Copying existing MS-GF+ index files from " + remoteIndexDirectory.FullName);
                }

                // Copy each file in filesToCopy (overwrite existing files)
                var manager = GetPseudoManagerName();

                var filesCopied = 0;
                var lastStatusUpdate = DateTime.UtcNow;

                var fileTools = new FileTools(manager, debugLevel);
                RegisterEvents(fileTools);

                // Compute the total disk space required
                long fileSizeTotalBytes = 0;

                foreach (var entry in filesToCopy)
                {
                    var sourceFile = new FileInfo(Path.Combine(remoteIndexDirectory.FullName, entry.Key));
                    fileSizeTotalBytes += sourceFile.Length;
                }

                var directorySpaceTools = new DirectorySpaceTools(true);

                const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 2048;

                // Convert fileSizeTotalBytes to MB, but add on a Default_Min_free_Space to assure we'll still have enough free space after copying over the files
                var minFreeSpaceMB = (int)(Global.BytesToMB(fileSizeTotalBytes) + DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                diskFreeSpaceBelowThreshold = !directorySpaceTools.ValidateFreeDiskSpace(
                    "Organism DB directory",
                    fastaFile.Directory.FullName,
                    minFreeSpaceMB,
                    out mErrorMessage);

                if (diskFreeSpaceBelowThreshold)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var remoteLockFileCreated = CreateRemoteSuffixArrayLockFile(
                    fastaFile.Name, fastaFile.Directory.FullName,
                    out var remoteLockFile2, debugLevel, maxWaitTimeHours);

                if (remoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files or while another manager was copying files locally,
                    // we should once again check to see if the required files exist

                    // Now confirm that each file was successfully copied locally
                    success = ValidateFiles(fastaFile.Directory.FullName, filesToCopy, usingLegacyFasta, fastaFile.LastWriteTimeUtc,
                                               false);

                    if (success)
                    {
                        // Files now exist
                        DeleteLockFile(remoteLockFile2);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }

                foreach (var entry in filesToCopy)
                {
                    var sourceFile = new FileInfo(Path.Combine(remoteIndexDirectory.FullName, entry.Key));

                    var targetFile = new FileInfo(Path.Combine(fastaFile.Directory.FullName, sourceFile.Name));

                    if (targetFile.Exists &&
                        string.Equals(targetFile.Extension, FileSyncUtils.LASTUSED_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                    {
                        // Do not overwrite the local .LastUsed file
                        continue;
                    }

                    fileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, manager, true);

                    filesCopied++;

                    if (debugLevel >= 1 && DateTime.UtcNow.Subtract(lastStatusUpdate).TotalSeconds >= 30)
                    {
                        lastStatusUpdate = DateTime.UtcNow;
                        OnStatusEvent("Retrieved " + filesCopied + " / " + filesToCopy.Count + " index files");
                    }
                }

                // Now confirm that each file was successfully copied locally
                success = ValidateFiles(fastaFile.Directory.FullName, filesToCopy, usingLegacyFasta, fastaFile.LastWriteTimeUtc, false);

                DeleteLockFile(remoteLockFile2);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyExistingIndexFilesFromRemote", ex);
                success = false;
            }

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        private void CopyIndexFilesToRemote(FileInfo fastaFile, string remoteIndexDirPath, int debugLevel)
        {
            var manager = GetPseudoManagerName();
            const bool createIndexFileForExistingFiles = false;

            var success = CopyIndexFilesToRemote(fastaFile, remoteIndexDirPath, debugLevel, manager,
                                                 createIndexFileForExistingFiles, out var errorMessage);

            if (!success)
            {
                OnErrorEvent(errorMessage);
            }
        }

        /// <summary>
        /// Copies the suffix array files for the specified FASTA file to the remote MSGFPlus_Index_File share
        /// </summary>
        /// <remarks>this method is used both by this class and by the MSGFPlusIndexFileCopier console application</remarks>
        /// <param name="fastaFile">FASTA file</param>
        /// <param name="remoteIndexDirPath">Remove index directory ptah</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="managerName">Manager name (only required because the constructor for PRISM.FileTools requires this)</param>
        /// <param name="createIndexFileForExistingFiles">
        /// When true, assumes that the index files were previously copied to remoteIndexDirPath,
        /// and we should simply create the .MSGFPlusIndexFileInfo file for the matching files
        /// This option is used by the MSGFPlusIndexFileCopier program when switch /X is provided
        /// </param>
        /// <param name="errorMessage">Output: Error message</param>
        public static bool CopyIndexFilesToRemote(FileInfo fastaFile, string remoteIndexDirPath, int debugLevel, string managerName,
                                                  bool createIndexFileForExistingFiles, out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                if (string.IsNullOrWhiteSpace(remoteIndexDirPath))
                    throw new ArgumentException("Remote index directory path cannot be empty", nameof(remoteIndexDirPath));

                var remoteIndexDirectory = new DirectoryInfo(remoteIndexDirPath);

                if (remoteIndexDirectory.Parent?.Exists != true)
                {
                    errorMessage = "Parent directory for the MS-GF+ index files directory not found: " + remoteIndexDirPath;
                    return false;
                }

                if (!remoteIndexDirectory.Exists)
                {
                    remoteIndexDirectory.Create();
                }

                if (createIndexFileForExistingFiles)
                {
                    var remoteFastaPath = Path.Combine(remoteIndexDirPath, fastaFile.Name);
                    fastaFile = new FileInfo(remoteFastaPath);
                }

                if (fastaFile.Directory == null)
                {
                    errorMessage = "Local FASTA file directory not found: " + fastaFile.FullName;
                    return false;
                }

                var filesToCopy = new Dictionary<string, long>();

                var fileInfo = new List<string>();

                // Find the index files for fastaFile
                foreach (var sourceFile in fastaFile.Directory.GetFiles(Path.GetFileNameWithoutExtension(fastaFile.Name) + ".*"))
                {
                    if (sourceFile.FullName == fastaFile.FullName)
                        continue;

                    // Skip the file if the extension is .hashcheck or .MSGFPlusIndexFileInfo
                    if (sourceFile.Extension == Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX ||
                        sourceFile.Extension == MSGF_PLUS_INDEX_FILE_INFO_SUFFIX)
                    {
                        continue;
                    }

                    filesToCopy.Add(sourceFile.Name, sourceFile.Length);
                    fileInfo.Add(sourceFile.Name + "\t" + sourceFile.Length + "\t" +
                                 sourceFile.LastWriteTimeUtc.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }

                if (!createIndexFileForExistingFiles)
                {
                    // Copy up each file
                    var fileTools = new FileTools(managerName, debugLevel);

                    foreach (var entry in filesToCopy)
                    {
                        var sourceFilePath = Path.Combine(fastaFile.Directory.FullName, entry.Key);
                        var targetFilePath = Path.Combine(remoteIndexDirectory.FullName, entry.Key);

                        var success = fileTools.CopyFileUsingLocks(sourceFilePath, targetFilePath, managerName, true);

                        if (!success)
                        {
                            errorMessage = "CopyFileUsingLocks returned false copying to " + targetFilePath;
                            return false;
                        }
                    }
                }

                // Create the .MSGFPlusIndexFileInfo file for this FASTA file
                var msgfPlusIndexFileInfo = new FileInfo(
                    Path.Combine(remoteIndexDirectory.FullName, fastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                using var writer = new StreamWriter(new FileStream(msgfPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                foreach (var entry in fileInfo)
                {
                    writer.WriteLine(entry);
                }

                return true;
            }
            catch (Exception ex)
            {
                errorMessage = "Error in CopyIndexFilesToRemote; " + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Convert the FASTA file to indexed DB files compatible with MSGFPlus
        /// Will copy the files from msgfPlusIndexFilesDirPathBase if they exist
        /// </summary>
        /// <param name="logFileDir">Log file directory</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="javaProgLoc">Path to Java executable</param>
        /// <param name="msgfPlusProgLoc">Path to the MS-GF+ .jar file</param>
        /// <param name="fastaFilePath">FASTA file path (on the local computer)</param>
        /// <param name="fastaFileIsDecoy">
        /// When true, only creates the forward-based index files.
        /// When false, creates both the forward and reverse index files
        /// </param>
        /// <param name="msgfPlusIndexFilesDirPathBase">Directory path from which to copy (or store) the index files</param>
        /// <param name="msgfPlusIndexFilesDirPathLegacyDB">
        /// Directory path from which to copy (or store) the index files for Legacy DBs
        /// (.fasta files not created from the protein sequences database)
        /// </param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public CloseOutType CreateSuffixArrayFiles(
            string logFileDir, short debugLevel, string javaProgLoc, string msgfPlusProgLoc,
            string fastaFilePath, bool fastaFileIsDecoy, string msgfPlusIndexFilesDirPathBase,
            string msgfPlusIndexFilesDirPathLegacyDB)
        {
            const float MAX_WAIT_TIME_HOURS = 1.0f;

            var currentTask = "Initializing";

            InsufficientFreeMemory = false;

            try
            {
                mErrorMessage = string.Empty;

                if (debugLevel > 4)
                {
                    OnDebugEvent("CreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter");
                }

                var fastaFile = new FileInfo(fastaFilePath);

                var msgfPlus = IsMSGFPlus(msgfPlusProgLoc);

                if (!msgfPlus)
                {
                    // Running legacy MSGFDB
                    throw new Exception("Legacy MSGFDB is no longer supported");
                }

                // Protein collection files will start with ID_ then have at least 6 integers, then an alphanumeric hash string, for example ID_004208_295531A4.fasta
                // If the filename does not match that pattern, we're using a legacy FASTA file
                var reProtectionCollectionFasta = new Regex(@"ID_\d{6,}_[0-9a-z]+\.fasta", RegexOptions.IgnoreCase);
                var usingLegacyFasta = !reProtectionCollectionFasta.IsMatch(fastaFile.Name);

                //  Look for existing suffix array files
                var outputNameBase = Path.GetFileNameWithoutExtension(fastaFile.Name);

                if (fastaFile.Directory == null || fastaFile.DirectoryName == null)
                {
                    mErrorMessage = "Cannot determine the parent directory of " + fastaFile.FullName;
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var lockFile = new FileInfo(Path.Combine(fastaFile.DirectoryName, outputNameBase + "_csarr.lock"));
                var dbCsArrayFilename = Path.Combine(fastaFile.DirectoryName, outputNameBase + ".csarr");

                // Check to see if another Analysis Manager is already creating the indexed DB files
                currentTask = "Looking for lock file " + lockFile.FullName;
                WaitForExistingLockfile(lockFile, debugLevel, MAX_WAIT_TIME_HOURS);

                // Validate that the expected files exist
                // If any are missing, need to repeat the call to "BuildSA"
                bool reindexingRequired;

                currentTask = "Validating that expected files exist";

                // Check for any FastaFileName.revConcat.* files
                // If they exist, delete them, since they are for legacy MSGFDB

                var legacyIndexedFiles = fastaFile.Directory.GetFiles(outputNameBase + ".revConcat.*");

                if (legacyIndexedFiles.Length > 0)
                {
                    reindexingRequired = true;

                    for (var index = 0; index <= legacyIndexedFiles.Length - 1; index++)
                    {
                        currentTask = "Deleting indexed file created by legacy MSGFDB: " + legacyIndexedFiles[index].FullName;

                        if (debugLevel >= 1)
                        {
                            OnStatusEvent(currentTask);
                        }
                        legacyIndexedFiles[index].Delete();
                    }
                }
                else
                {
                    currentTask = "Validating the canno file";
                    var fileIsValid = ValidateCannoFile(fastaFile, outputNameBase, debugLevel);

                    reindexingRequired = !fileIsValid;
                }

                // This dictionary contains file suffixes to look for
                // Keys will be "True" if the file exists and false if it does not exist
                var filesToFind = new List<string>();

                if (!reindexingRequired)
                {
                    currentTask = "Validating that expected files exist";
                    var existingFiles = FindExistingSuffixArrayFiles(
                        fastaFileIsDecoy, outputNameBase, fastaFile.DirectoryName,
                        filesToFind, out var existingFileList, out var missingFiles);

                    if (existingFiles.Count < filesToFind.Count)
                    {
                        reindexingRequired = true;

                        currentTask = "Some files are missing: " + existingFiles.Count + " vs. " + filesToFind.Count;

                        if (existingFiles.Count > 0)
                        {
                            if (debugLevel >= 1)
                            {
                                OnWarningEvent("Indexing of " + fastaFile.Name + " was incomplete (found " + existingFiles.Count + " out of " +
                                               filesToFind.Count + " index files)");
                                OnStatusEvent(" ... existing files: " + existingFileList);
                                OnStatusEvent(" ... missing files: " + missingFiles);
                            }
                        }
                    }
                    else if (usingLegacyFasta)
                    {
                        // Make sure the index files have a file modification date newer than the FASTA file
                        // We only do this for legacy FASTA files, since their file modification date will be the same on all pubs

                        // We can't do this for programmatically generated FASTA files (that use protein collections)
                        // since their modification date will be the time that the file was created

                        foreach (var indexFile in existingFiles)
                        {
                            if (indexFile.LastWriteTimeUtc < fastaFile.LastWriteTimeUtc.AddSeconds(-0.1))
                            {
                                OnStatusEvent("Index file is older than the FASTA file; " + indexFile.FullName + " modified " +
                                              indexFile.LastWriteTimeUtc.ToLocalTime().ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT) + " vs. " +
                                              fastaFile.LastWriteTimeUtc.ToLocalTime().ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                                reindexingRequired = true;
                                break;
                            }
                        }
                    }
                }

                var remoteIndexDirPath = DetermineRemoteMSGFPlusIndexFilesDirectoryPath(
                    fastaFile.Name, msgfPlusIndexFilesDirPathBase, msgfPlusIndexFilesDirPathLegacyDB);

                if (!reindexingRequired)
                {
                    if (Global.OfflineMode)
                        return CloseOutType.CLOSEOUT_SUCCESS;

                    // Update the .LastUsed file on the remote share
                    UpdateRemoteLastUsedFile(remoteIndexDirPath, fastaFile.Name);

                    // Delete old index files on the remote share
                    DeleteOldIndexFiles(remoteIndexDirPath, debugLevel);

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Index files are missing or out of date
                bool remoteLockFileCreated;
                FileInfo remoteLockFile = null;
                CloseOutType resultCode;

                if (Global.OfflineMode)
                {
                    // The manager that pushed the FASTA files to the remote host should have also indexed them and pushed the index files to this host
                    // We can still re-index the files using the local FASTA file
                    OnWarningEvent("Index files are missing or out of date for " + fastaFilePath + "; will re-generate them");
                    remoteLockFileCreated = false;
                    resultCode = CloseOutType.CLOSEOUT_SUCCESS;
                }
                else
                {
                    // Copy the missing index files from remoteIndexDirPath (if possible)
                    // Otherwise, create new index files

                    const bool CHECK_FOR_LOCK_FILE_A = true;
                    resultCode = CopyExistingIndexFilesFromRemote(fastaFile, usingLegacyFasta, remoteIndexDirPath, CHECK_FOR_LOCK_FILE_A,
                                                                   debugLevel, MAX_WAIT_TIME_HOURS, out var diskFreeSpaceBelowThreshold1);

                    if (diskFreeSpaceBelowThreshold1)
                    {
                        // Not enough free disk space; abort
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (resultCode == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Update the .LastUsed file on the remote share
                        UpdateRemoteLastUsedFile(remoteIndexDirPath, fastaFile.Name);

                        // Delete old index files on the remote share
                        DeleteOldIndexFiles(remoteIndexDirPath, debugLevel);

                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    // Files did not exist or were out of date, or an error occurred while copying them
                    currentTask = "Create a remote lock file";
                    remoteLockFileCreated = CreateRemoteSuffixArrayLockFile(
                        fastaFile.Name, remoteIndexDirPath,
                        out remoteLockFile, debugLevel, MAX_WAIT_TIME_HOURS);
                }

                if (remoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files, we should once again try to copy the files locally

                    const bool CHECK_FOR_LOCK_FILE_B = false;
                    resultCode = CopyExistingIndexFilesFromRemote(fastaFile, usingLegacyFasta, remoteIndexDirPath, CHECK_FOR_LOCK_FILE_B,
                                                               debugLevel, MAX_WAIT_TIME_HOURS, out var diskFreeSpaceBelowThreshold2);

                    if (resultCode == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Existing files were copied; this manager does not need to re-create them
                        reindexingRequired = false;
                    }

                    if (diskFreeSpaceBelowThreshold2)
                    {
                        // Not enough free disk space; abort
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                if (reindexingRequired)
                {
                    OnStatusEvent("Running BuildSA to index " + fastaFile.Name);

                    // Note that this method will create a local .lock file
                    resultCode = CreateSuffixArrayFilesWork(logFileDir, debugLevel, fastaFile, lockFile, javaProgLoc,
                                                            msgfPlusProgLoc, fastaFileIsDecoy, dbCsArrayFilename);

                    if (remoteLockFileCreated && resultCode == CloseOutType.CLOSEOUT_SUCCESS && !Global.OfflineMode)
                    {
                        OnStatusEvent("Copying index files to " + remoteIndexDirPath);
                        CopyIndexFilesToRemote(fastaFile, remoteIndexDirPath, debugLevel);
                    }
                }

                if (!Global.OfflineMode)
                {
                    // Update the .LastUsed file on the remote share
                    UpdateRemoteLastUsedFile(remoteIndexDirPath, fastaFile.Name);

                    // Delete old index files on the remote share
                    DeleteOldIndexFiles(remoteIndexDirPath, debugLevel);

                    if (remoteLockFileCreated)
                    {
                        // Delete the remote lock file
                        DeleteLockFile(remoteLockFile);
                    }
                }

                return resultCode;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error in .CreateIndexedDbFiles";
                OnErrorEvent(mErrorMessage + "; " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType CreateSuffixArrayFilesWork(string logFileDir, int debugLevel, FileInfo fastaFile,
                                                        FileSystemInfo lockFile, string javaProgLoc, string msgfPlusProgLoc, bool fastaFileIsDecoy,
                                                        string dbCsArrayFilename)
        {
            var currentTask = string.Empty;

            try
            {
                // Try to create the index files for FASTA file dBFileNameInput
                currentTask = "Look for java.exe and .jar file";

                // Verify that Java exists
                if (!File.Exists(javaProgLoc))
                {
                    mErrorMessage = "Cannot find Java program file";
                    OnErrorEvent(mErrorMessage + ": " + javaProgLoc);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Verify that the MSGFDB.Jar or MSGFPlus.jar file exists
                if (!File.Exists(msgfPlusProgLoc))
                {
                    mErrorMessage = "Cannot find " + Path.GetFileName(msgfPlusProgLoc) + " file";
                    OnErrorEvent(mErrorMessage + ": " + msgfPlusProgLoc);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Determine the amount of ram to reserve for BuildSA
                // Examine the size of the FASTA file to determine how much ram to reserve
                int javaMemorySizeMB;

                var fastaFileSizeMB = Global.BytesToMB(fastaFile.Length);

                if (fastaFileSizeMB <= 125)
                {
                    javaMemorySizeMB = 4000;
                }
                else if (fastaFileSizeMB <= 250)
                {
                    javaMemorySizeMB = 6000;
                }
                else if (fastaFileSizeMB <= 375)
                {
                    javaMemorySizeMB = 8000;
                }
                else
                {
                    javaMemorySizeMB = 12000;
                }

                currentTask = "Verify free memory";

                // Make sure the machine has enough free memory to run BuildSA
                if (!AnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "BuildSA", false))
                {
                    InsufficientFreeMemory = true;
                    mErrorMessage = "Cannot run BuildSA since less than " + javaMemorySizeMB + " MB of free memory";
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
                }

                // Create a lock file
                if (debugLevel >= 3)
                {
                    OnStatusEvent("Creating lock file: " + lockFile.FullName);
                }

                // Delay between 2 and 5 seconds
                var random = new Random();
                Global.IdleLoop(random.Next(2, 5));

                // Check one more time for a lock file
                // If it exists, another manager just created it and we should abort
                currentTask = "Look for the lock file one last time";
                lockFile.Refresh();

                if (lockFile.Exists)
                {
                    if (debugLevel >= 1)
                    {
                        OnStatusEvent("Warning: new lock file found: " + lockFile.FullName + "; aborting");
                        return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                    }
                }

                // Create a lock file in the directory that the index files will be created
                currentTask = "Create the local lock file: " + lockFile.FullName;
                var success = CreateLockFile(lockFile.FullName);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete any existing index files (BuildSA throws an error if they exist)
                currentTask = "Delete any existing files";

                var outputNameBase = Path.GetFileNameWithoutExtension(fastaFile.Name);

                var existingFiles = FindExistingSuffixArrayFiles(
                    fastaFileIsDecoy, outputNameBase, fastaFile.DirectoryName,
                    new List<string>(), out _, out _);

                foreach (var indexFileToDelete in existingFiles)
                {
                    if (indexFileToDelete.Exists)
                    {
                        indexFileToDelete.Delete();
                    }
                }

                if (debugLevel >= 2)
                {
                    OnStatusEvent("Creating Suffix Array database file: " + dbCsArrayFilename);
                }

                // Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)
                currentTask = "Construct BuildSA command line";

                var arguments = " -Xmx" + javaMemorySizeMB + "M -cp " + msgfPlusProgLoc;

                arguments += " edu.ucsd.msjava.msdbsearch.BuildSA -d " + fastaFile.FullName;

                if (fastaFileIsDecoy)
                {
                    arguments += " -tda 0";
                }
                else
                {
                    arguments += " -tda 2";
                }

                if (debugLevel >= 1)
                {
                    OnStatusEvent(javaProgLoc + " " + arguments);
                }

                var consoleOutputFilePath = Path.Combine(logFileDir, "MSGFPlus_BuildSA_ConsoleOutput.txt");
                var buildSA = new RunDosProgram(fastaFile.DirectoryName, debugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = consoleOutputFilePath
                };

                RegisterEvents(buildSA);

                currentTask = "Run BuildSA using " + arguments;

                // Run BuildSA and wait for it to exit
                // This process generally doesn't take that long, so we do not track CPU usage
                success = buildSA.RunProgram(javaProgLoc, arguments, "BuildSA", true);

                if (!success)
                {
                    mErrorMessage = "Error running BuildSA with " + Path.GetFileName(msgfPlusProgLoc) + " for " + fastaFile.Name;

                    if (!string.IsNullOrWhiteSpace(consoleOutputFilePath))
                    {
                        // Look for known errors in the console output file
                        var consoleOutputError = ParseConsoleOutputFile(consoleOutputFilePath);

                        if (!string.IsNullOrWhiteSpace(consoleOutputError))
                        {
                            mErrorMessage += ". " + consoleOutputError;
                        }
                    }

                    OnStatusEvent(mErrorMessage);
                    DeleteLockFile(lockFile);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (debugLevel >= 1)
                {
                    OnStatusEvent("Created suffix array files for " + fastaFile.Name);
                }

                if (debugLevel >= 3)
                {
                    OnStatusEvent("Deleting lock file: " + lockFile.FullName);
                }

                // Delete the lock file
                currentTask = "Delete the lock file";
                DeleteLockFile(lockFile);
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error in .CreateSuffixArrayFilesWork";
                OnErrorEvent(mErrorMessage + "; " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a lock file
        /// </summary>
        /// <remarks>Returns false if the lock file already exists</remarks>
        /// <returns>True if success; false if failure</returns>
        private bool CreateLockFile(string lockFilePath)
        {
            try
            {
                using var writer = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read));

                // Use local time for dates in lock files
                writer.WriteLine("Date: " + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                writer.WriteLine("Manager: " + mMgrName);

                return true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error creating lock file";
                OnErrorEvent("CreateMSGFDBSuffixArrayFiles.CreateLockFile, " + mErrorMessage, ex);
                return false;
            }
        }

        private bool CreateRemoteSuffixArrayLockFile(
            string fastaFileName, string remoteIndexDirPath,
            out FileInfo remoteLockFile, int debugLevel, float maxWaitTimeHours)
        {
            var remoteIndexDirectory = new DirectoryInfo(remoteIndexDirPath);

            if (remoteIndexDirectory.Parent?.Exists != true)
            {
                OnErrorEvent("Cannot read/write MS-GF+ index files from remote share; directory not found; " + remoteIndexDirectory.FullName);
                remoteLockFile = null;
                return false;
            }

            remoteLockFile =
                new FileInfo(Path.Combine(remoteIndexDirectory.FullName, fastaFileName + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

            var currentTask = "Looking for lock file " + remoteLockFile.FullName;
            WaitForExistingLockfile(remoteLockFile, debugLevel, maxWaitTimeHours);

            try
            {
                if (!remoteIndexDirectory.Exists)
                {
                    remoteIndexDirectory.Create();
                }

                // Create the remote lock file
                if (!CreateLockFile(remoteLockFile.FullName))
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception creating remote MS-GF+ suffix array lock file at " +
                             remoteIndexDirectory.FullName + "; " + currentTask, ex);
                return false;
            }

            return true;
        }

        private void DeleteLockFile(FileSystemInfo lockFile)
        {
            try
            {
                lockFile.Refresh();

                if (lockFile.Exists)
                {
                    lockFile.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Delete old MS-GF+ index files
        /// </summary>
        /// <param name="remoteIndexDirPath">Remote index directory path</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        private void DeleteOldIndexFiles(string remoteIndexDirPath, short debugLevel)
        {
            try
            {
                var remoteIndexDir = new DirectoryInfo(remoteIndexDirPath);

                if (!remoteIndexDir.Exists)
                {
                    OnWarningEvent("Remote index directory not found: " + remoteIndexDir.FullName);
                    return;
                }

                // MS-GF+ index files for protein collections are grouped by ArchiveOutputFile ID, rounded to the nearest 1000
                // For example \\gigasax\MSGFPlus_Index_Files\2000 and \\gigasax\MSGFPlus_Index_Files\3000
                // The MaxDirSize.txt file will be up one directory
                // Similarly, for Legacy FASTA files, the MaxDirSize.txt file is in the parent directory of \\Proto-7\MSGFPlus_Index_Files\Other

                var remoteIndexDirToUse = remoteIndexDir.Parent ?? remoteIndexDir;

                // Only delete old files once every 24 hours
                var purgeInfoFile = new FileInfo(Path.Combine(remoteIndexDirToUse.FullName, "PurgeInfoFile.txt"));

                if (purgeInfoFile.Exists && DateTime.UtcNow.Subtract(purgeInfoFile.LastWriteTimeUtc).TotalHours < 24)
                {
                    return;
                }

                try
                {
                    using var purgeInfoWriter = new StreamWriter(new FileStream(purgeInfoFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                    // Use local time for the date in the purge Info file
                    purgeInfoWriter.WriteLine(DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    purgeInfoWriter.WriteLine("Manager: " + mMgrName);
                }
                catch (Exception ex)
                {
                    // Unable to create or update the file
                    // It's possible another manager was trying to update it simultaneously
                    // Alternatively, we may not have write access (or the server might be offline)
                    OnWarningEvent("Unable to create/update the PurgeInfo file at " + purgeInfoFile.FullName + ": " + ex.Message);
                    return;
                }

                // Look for file MaxDirSize.txt which defines the maximum space that the files can use
                var maxDirSizeFile = new FileInfo(Path.Combine(remoteIndexDirToUse.FullName, "MaxDirSize.txt"));

                if (!maxDirSizeFile.Exists)
                {
                    OnWarningEvent("Remote index directory does not have file MaxDirSize.txt; cannot purge old index files in " + remoteIndexDirPath);
                    OnStatusEvent("Create file {0} with 'MaxSizeGB=50' on a single line. " +
                                  "Comment lines are allowed using # as a comment character", maxDirSizeFile.Name);

                    return;
                }

                // MaxDirSize.txt file exists; this file specifies the max total GB that files orgDbFolder can use
                AnalysisResources.PurgeFastaFilesUsingSpaceUsedThreshold(maxDirSizeFile, "", debugLevel, preview: false);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error deleting old index files in " + remoteIndexDirPath, ex);
            }
        }

        private string DetermineRemoteMSGFPlusIndexFilesDirectoryPath(
            string fastaFileName,
            string msgfPlusIndexFilesDirPathBase,
            string msgfPlusIndexFilesDirPathLegacyDB)
        {
            var reExtractNum = new Regex(@"^ID_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // DMS-generated FASTA files will have a name of the form ID_003949_3D6802EE.fasta
            // Parse out the number (003949 in this case)
            var reMatch = reExtractNum.Match(fastaFileName);

            if (reMatch.Success)
            {
                if (int.TryParse(reMatch.Groups[1].Value, out var generatedFastaFileNumber))
                {
                    // Round down to the nearest 1000
                    // Thus, 003949 will round to 3000
                    var directoryName = (Math.Floor(generatedFastaFileNumber / 1000.0) * 1000).ToString("0");

                    if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesDirPathBase))
                        return string.Empty;

                    return Path.Combine(msgfPlusIndexFilesDirPathBase, directoryName);
                }
            }

            if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesDirPathLegacyDB))
                return string.Empty;

            return Path.Combine(msgfPlusIndexFilesDirPathLegacyDB, "Other");
        }

        /// <summary>
        /// Constructs a list of suffix array files that should exist
        /// Looks for each of those files
        /// </summary>
        /// <param name="fastaFileIsDecoy">True if the FASTA file has forward and reverse sequences</param>
        /// <param name="outputNameBase">Base output name</param>
        /// <param name="directoryPathToSearch">Directory path to search</param>
        /// <param name="filesToFind">List of files that should exist; calling method must have initialized it</param>
        /// <param name="existingFileList">Output param: semicolon separated list of existing files</param>
        /// <param name="missingFiles">Output param: semicolon separated list of missing files</param>
        /// <returns>A list of the files that currently exist</returns>
        private List<FileInfo> FindExistingSuffixArrayFiles(
            bool fastaFileIsDecoy, string outputNameBase,
            string directoryPathToSearch, ICollection<string> filesToFind,
            out string existingFileList,
            out string missingFiles)
        {
            var existingFiles = new List<FileInfo>();

            filesToFind.Clear();

            existingFileList = string.Empty;
            missingFiles = string.Empty;

            // Suffixes for MSGFDB (effective 8/22/2011) and MS-GF+
            filesToFind.Add(".canno");
            filesToFind.Add(".cnlcp");
            filesToFind.Add(".csarr");
            filesToFind.Add(".cseq");

            // Note: Suffixes for MSPathFinder
            // filesToFind.Add(".icanno")
            // filesToFind.Add(".icplcp")
            // filesToFind.Add(".icseq")

            if (!fastaFileIsDecoy)
            {
                filesToFind.Add(".revCat.canno");
                filesToFind.Add(".revCat.cnlcp");
                filesToFind.Add(".revCat.csarr");
                filesToFind.Add(".revCat.cseq");
                filesToFind.Add(".revCat.fasta");
            }

            foreach (var suffix in filesToFind)
            {
                var fileNameToFind = outputNameBase + suffix;

                var file = new FileInfo(Path.Combine(directoryPathToSearch, fileNameToFind));

                if (file.Exists)
                {
                    existingFiles.Add(file);
                    existingFileList = Global.AppendToComment(existingFileList, fileNameToFind);
                }
                else
                {
                    missingFiles = Global.AppendToComment(missingFiles, fileNameToFind);
                }
            }

            return existingFiles;
        }

        private string GetPseudoManagerName()
        {
            return mMgrName + "_CreateMSGFDBSuffixArrayFiles";
        }

        /// <summary>
        /// Check whether jarFilePath matches MSGFDB.jar
        /// </summary>
        /// <param name="jarFilePath">.jar file path</param>
        public bool IsMSGFPlus(string jarFilePath)
        {
            const string MSGFDB_JAR_NAME = "MSGFDB.jar";

            var jarFile = new FileInfo(jarFilePath);

            if (string.Equals(jarFile.Name, MSGFDB_JAR_NAME, StringComparison.OrdinalIgnoreCase))
            {
                // Not MS-GF+
                return false;
            }

            // Using MS-GF+
            return true;
        }

        /// <summary>
        /// Look for errors in the console output file created by the call to BuildSA
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        private string ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    OnWarningEvent("BuildSA console output file not found: " + consoleOutputFilePath);
                    return string.Empty;
                }

                using var reader= new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (dataLine == null || !dataLine.StartsWith("Error", StringComparison.OrdinalIgnoreCase))
                        continue;

                    OnErrorEvent("BuildSA reports: " + dataLine);

                    if (dataLine.Contains("too many redundant proteins"))
                    {
                        return "Error while indexing, too many redundant proteins";
                    }
                    return dataLine;
                }

                return string.Empty;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error parsing the BuildSA console output file", ex);
                return string.Empty;
            }
        }

        private void UpdateRemoteLastUsedFile(string remoteIndexDirPath, string fastaFileName)
        {
            try
            {
                var remoteIndexDir = new DirectoryInfo(remoteIndexDirPath);

                if (!remoteIndexDir.Exists)
                {
                    OnErrorEvent("Remote index directory not found in UpdateRemoteLastUsedFile: " + remoteIndexDirPath);
                }

                var lastUsedFilePath = Path.Combine(remoteIndexDir.FullName,
                                                    fastaFileName + FileSyncUtils.LASTUSED_FILE_EXTENSION);

                try
                {
                    using var writer = new StreamWriter(new FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                    // Use UtcNow for dates in LastUsed files
                    writer.WriteLine(DateTime.UtcNow.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }
                catch (IOException)
                {
                    // The file is likely open by another manager; ignore this
                }
                catch (Exception ex)
                {
                    OnWarningEvent("Unable to create a new .LastUsed file at {0}: {1}", lastUsedFilePath, ex.Message);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateRemoteLastUsedFile", ex);
            }
        }

        /// <summary>
        /// Open the FastaFileName.canno file and read the first two lines
        /// If there is a number on the first line but the second line starts with the letter A, this file was created with the legacy MSGFDB
        /// </summary>
        /// <param name="fastaFile">FASTA file</param>
        /// <param name="outputNameBase">Base output name</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <returns>True if the file is valid, false if it is missing, corrupt, or from the legacy MSGFDB</returns>
        private bool ValidateCannoFile(FileInfo fastaFile, string outputNameBase, int debugLevel)
        {
            if (string.IsNullOrWhiteSpace(fastaFile.DirectoryName))
            {
                OnErrorEvent("Cannot determine the parent directory of the FASTA file, " + fastaFile.FullName);
                return false;
            }

            var cannoFile = new FileInfo(Path.Combine(fastaFile.DirectoryName, outputNameBase + ".canno"));

            if (!cannoFile.Exists)
            {
                if (debugLevel >= 1)
                {
                    OnStatusEvent("Canno file not found locally (" + cannoFile.FullName + "); copying from remote or re-indexing");
                }
                return false;
            }

            using (var reader = new StreamReader(new FileStream(cannoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                var corruptFile = true;

                if (!reader.EndOfStream)
                {
                    var line1 = reader.ReadLine();

                    if (!reader.EndOfStream)
                    {
                        var line2 = reader.ReadLine();

                        if (int.TryParse(line1, out _))
                        {
                            corruptFile = false;

                            if (!string.IsNullOrWhiteSpace(line2) && char.IsLetter(line2[0]))
                            {
                                if (debugLevel >= 1)
                                {
                                    OnStatusEvent("Legacy MSGFDB indexed file found (" + cannoFile.Name + "); re-indexing");
                                }

                                return false;
                            }
                        }
                    }
                }

                if (!corruptFile)
                {
                    return true;
                }
            }

            if (debugLevel >= 1)
            {
                OnStatusEvent("Canno file (" + cannoFile.Name + ") is not in the format expected by ValidateCannoFile; re-indexing");
            }

            return false;
        }

        /// <summary>
        /// Verifies that each of the files specified by filesToCopy exists at directoryPathToCheck and has the correct file size
        /// </summary>
        /// <param name="directoryPathToCheck">Directory to check</param>
        /// <param name="filesToCopy">Dictionary with filenames and file sizes</param>
        /// <param name="usingLegacyFasta">True when using a legacy FASTA file (not protein collection based)</param>
        /// <param name="minWriteTimeThresholdUTC">Minimum write time threshold for the index files, in UTC</param>
        /// <param name="verifyingRemoteDirectory">True when validating files on a remote server, false if verifying the local DMS_Temp_Org directory</param>
        /// <returns>True if all files are found and are the right size</returns>
        private bool ValidateFiles(string directoryPathToCheck, Dictionary<string, long> filesToCopy, bool usingLegacyFasta,
                                   DateTime minWriteTimeThresholdUTC, bool verifyingRemoteDirectory)
        {
            string sourceDescription;

            if (verifyingRemoteDirectory)
            {
                sourceDescription = "Remote MS-GF+ index file";
            }
            else
            {
                sourceDescription = "Local MS-GF+ index file";
            }

            foreach (var entry in filesToCopy)
            {
                var sourceFile = new FileInfo(Path.Combine(directoryPathToCheck, entry.Key));

                if (!sourceFile.Exists)
                {
                    // Remote MS-GF+ index file not found
                    // Local MS-GF+ index file not found
                    OnWarningEvent(sourceDescription + " not found: " + sourceFile.FullName);
                    return false;
                }

                if (sourceFile.Length != entry.Value)
                {
                    // Remote MS-GF+ index file is not the expected size
                    // Local MS-GF+ index file is not the expected size
                    OnWarningEvent(sourceDescription + " is not the expected size: " + sourceFile.FullName + " should be " + entry.Value +
                                   " bytes but is actually " + sourceFile.Length + " bytes");
                    return false;
                }

                if (usingLegacyFasta)
                {
                    // Require that the index files be newer than the FASTA file (ignore the .LastUsed file)
                    if (sourceFile.LastWriteTimeUtc < minWriteTimeThresholdUTC.AddSeconds(-0.1))
                    {
                        if (!string.Equals(sourceFile.Extension, FileSyncUtils.LASTUSED_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        {
                            var sourceFileDate = sourceFile.LastWriteTimeUtc.ToLocalTime().ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT);
                            var dateThreshold = minWriteTimeThresholdUTC.ToLocalTime().ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT);

                            OnStatusEvent("{0} is older than the FASTA file; {1} modified {2} vs. {3}; indexing is required", sourceDescription, sourceFile.FullName, sourceFileDate, dateThreshold);

                            return false;
                        }
                    }
                }
            }

            return true;
        }

        private void WaitForExistingLockfile(FileSystemInfo lockFile, int debugLevel, float maxWaitTimeHours)
        {
            // Check to see if another Analysis Manager is already creating the indexed DB files
            if (lockFile.Exists && DateTime.UtcNow.Subtract(lockFile.LastWriteTimeUtc).TotalMinutes >= 60)
            {
                // Lock file is over 60 minutes old; delete it
                if (debugLevel >= 1)
                {
                    OnStatusEvent("Lock file is over 60 minutes old (created " +
                        lockFile.LastWriteTime.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT) + "); " +
                        "deleting " + lockFile.FullName);
                }
                DeleteLockFile(lockFile);
                return;
            }

            if (!lockFile.Exists)
                return;

            if (debugLevel >= 1)
            {
                OnStatusEvent("Lock file found: " + lockFile.FullName +
                              "; waiting for file to be removed by other manager generating suffix array files");
            }

            // Lock file found; wait up to maxWaitTimeHours
            var staleFile = false;

            while (lockFile.Exists)
            {
                // Sleep for 2 seconds
                Global.IdleLoop(2);

                if (DateTime.UtcNow.Subtract(lockFile.CreationTimeUtc).TotalHours >= maxWaitTimeHours)
                {
                    staleFile = true;
                    break;
                }

                lockFile.Refresh();
            }

            // If the duration time has exceeded maxWaitTimeHours, delete the lock file and try again with this manager
            if (staleFile)
            {
                var logMessage = "Waited over " + maxWaitTimeHours.ToString("0.0") +
                                 " hour(s) for lock file to be deleted, but it is still present; " +
                                 "deleting the file now and continuing: " + lockFile.FullName;
                OnWarningEvent(logMessage);
                DeleteLockFile(lockFile);
            }
        }
    }
}
