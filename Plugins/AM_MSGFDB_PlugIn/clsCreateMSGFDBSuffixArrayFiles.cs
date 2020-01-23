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

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Create MS-GF+ suffix array files
    /// </summary>
    public class clsCreateMSGFDBSuffixArrayFiles : EventNotifier
    {
        #region "Constants"

        private const string MSGF_PLUS_INDEX_FILE_INFO_SUFFIX = ".MSGFPlusIndexFileInfo";

        #endregion

        #region "Module Variables"

        private string mErrorMessage = string.Empty;
        private readonly string mMgrName;

        #endregion

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="managerName"></param>
        public clsCreateMSGFDBSuffixArrayFiles(string managerName)
        {
            mMgrName = managerName;
        }

        private CloseOutType CopyExistingIndexFilesFromRemote(
            FileInfo fiFastaFile, bool usingLegacyFasta, string remoteIndexDirPath,
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
                    OnDebugEvent(string.Format("Remote index directory not found ({0}); indexing is required", remoteIndexDirectory));
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (fiFastaFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of " + fiFastaFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (checkForLockFile)
                {
                    // Look for an existing lock file on the remote server
                    var fiRemoteLockFile1 = new FileInfo(
                        Path.Combine(remoteIndexDirectory.FullName,
                                     fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

                    WaitForExistingLockfile(fiRemoteLockFile1, debugLevel, maxWaitTimeHours);
                }

                // Look for the .MSGFPlusIndexFileInfo file for this fasta file
                var fiMSGFPlusIndexFileInfo = new FileInfo(
                    Path.Combine(remoteIndexDirectory.FullName,
                                 fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                long fileSizeTotalKB = 0;

                if (!fiMSGFPlusIndexFileInfo.Exists)
                {
                    OnDebugEvent(string.Format("{0} not found at {1}; indexing is required",
                        fiMSGFPlusIndexFileInfo.Name, remoteIndexDirectory.FullName));
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Read the filenames in the file
                // There should be 3 columns: FileName, FileSize, and FileDateUTC
                // When looking for existing files we only require that the FileSize match; FileDateUTC is not used

                var filesToCopy = new Dictionary<string, long>();

                using (var reader = new StreamReader(
                    new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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
                    // If using a legacy fasta file, must also confirm that each file is newer than the fasta file that was indexed
                    filesAreValid = ValidateFiles(
                        remoteIndexDirectory.FullName, filesToCopy, usingLegacyFasta,
                        fiFastaFile.LastWriteTimeUtc, true);
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
                var dtLastStatusUpdate = DateTime.UtcNow;

                var oFileTools = new FileTools(manager, debugLevel);
                RegisterEvents(oFileTools);

                // Compute the total disk space required
                long fileSizeTotalBytes = 0;

                foreach (var entry in filesToCopy)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(remoteIndexDirectory.FullName, entry.Key));
                    fileSizeTotalBytes += fiSourceFile.Length;
                }

                const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750;

                // Convert fileSizeTotalBytes to MB, but add on a Default_Min_free_Space to assure we'll still have enough free space after copying over the files
                var minFreeSpaceMB = (int)(clsGlobal.BytesToMB(fileSizeTotalBytes) + DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                diskFreeSpaceBelowThreshold =
                    !clsGlobal.ValidateFreeDiskSpace("Organism DB directory", fiFastaFile.Directory.FullName, minFreeSpaceMB, out mErrorMessage);

                if (diskFreeSpaceBelowThreshold)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var remoteLockFileCreated = CreateRemoteSuffixArrayLockFile(
                    fiFastaFile.Name, fiFastaFile.Directory.FullName,
                    out var fiRemoteLockFile2, debugLevel, maxWaitTimeHours);

                if (remoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files or while another manager was copying files locally,
                    // we should once again check to see if the required files exist

                    // Now confirm that each file was successfully copied locally
                    success = ValidateFiles(fiFastaFile.Directory.FullName, filesToCopy, usingLegacyFasta, fiFastaFile.LastWriteTimeUtc,
                                               false);
                    if (success)
                    {
                        // Files now exist
                        DeleteLockFile(fiRemoteLockFile2);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }

                foreach (var entry in filesToCopy)
                {
                    var sourceFile = new FileInfo(Path.Combine(remoteIndexDirectory.FullName, entry.Key));

                    var targetFile = new FileInfo(Path.Combine(fiFastaFile.Directory.FullName, sourceFile.Name));
                    if (targetFile.Exists &&
                        string.Equals(targetFile.Extension, FileSyncUtils.LASTUSED_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                    {
                        // Do not overwrite the local .LastUsed file
                        continue;
                    }

                    oFileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, manager, true);

                    filesCopied += 1;

                    if (debugLevel >= 1 && DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 30)
                    {
                        dtLastStatusUpdate = DateTime.UtcNow;
                        OnStatusEvent("Retrieved " + filesCopied + " / " + filesToCopy.Count + " index files");
                    }
                }

                // Now confirm that each file was successfully copied locally
                success = ValidateFiles(fiFastaFile.Directory.FullName, filesToCopy, usingLegacyFasta, fiFastaFile.LastWriteTimeUtc, false);

                DeleteLockFile(fiRemoteLockFile2);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in CopyExistingIndexFilesFromRemote", ex);
                success = false;
            }

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

        }

        private void CopyIndexFilesToRemote(FileInfo fiFastaFile, string remoteIndexDirPath, int debugLevel)
        {
            var manager = GetPseudoManagerName();
            const bool createIndexFileForExistingFiles = false;

            var success = CopyIndexFilesToRemote(fiFastaFile, remoteIndexDirPath, debugLevel, manager,
                                                 createIndexFileForExistingFiles, out var errorMessage);
            if (!success)
            {
                OnErrorEvent(errorMessage);
            }
        }

        /// <summary>
        /// Copies the suffix array files for the specified fasta file to the remote MSGFPlus_Index_File share
        /// </summary>
        /// <param name="fiFastaFile"></param>
        /// <param name="remoteIndexDirPath"></param>
        /// <param name="debugLevel"></param>
        /// <param name="managerName">Manager name (only required because the constructor for PRISM.FileTools requires this)</param>
        /// <param name="createIndexFileForExistingFiles">
        /// When true, assumes that the index files were previously copied to remoteIndexDirPath,
        /// and we should simply create the .MSGFPlusIndexFileInfo file for the matching files
        /// This option is used by the MSGFPlusIndexFileCopier program when switch /X is provided
        /// </param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>This function is used both by this class and by the MSGFPlusIndexFileCopier console application</remarks>
        public static bool CopyIndexFilesToRemote(FileInfo fiFastaFile, string remoteIndexDirPath, int debugLevel, string managerName,
                                                  bool createIndexFileForExistingFiles, out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                if (string.IsNullOrWhiteSpace(remoteIndexDirPath))
                    throw new ArgumentException("Remote index directory path cannot be empty", nameof(remoteIndexDirPath));

                var remoteIndexDirectory = new DirectoryInfo(remoteIndexDirPath);

                if (remoteIndexDirectory.Parent == null || !remoteIndexDirectory.Parent.Exists)
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
                    var remoteFastaPath = Path.Combine(remoteIndexDirPath, fiFastaFile.Name);
                    fiFastaFile = new FileInfo(remoteFastaPath);
                }

                if (fiFastaFile.Directory == null)
                {
                    errorMessage = "Local FASTA file directory not found: " + fiFastaFile.FullName;
                    return false;
                }

                var filesToCopy = new Dictionary<string, long>();

                var fileInfo = new List<string>();

                // Find the index files for fiFastaFile
                foreach (var fiSourceFile in fiFastaFile.Directory.GetFiles(Path.GetFileNameWithoutExtension(fiFastaFile.Name) + ".*"))
                {
                    if (fiSourceFile.FullName == fiFastaFile.FullName)
                        continue;

                    // Skip the file if the extension is .hashcheck or .MSGFPlusIndexFileInfo
                    if (fiSourceFile.Extension == clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX ||
                        fiSourceFile.Extension == MSGF_PLUS_INDEX_FILE_INFO_SUFFIX)
                        continue;

                    filesToCopy.Add(fiSourceFile.Name, fiSourceFile.Length);
                    fileInfo.Add(fiSourceFile.Name + "\t" + fiSourceFile.Length + "\t" +
                                 fiSourceFile.LastWriteTimeUtc.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }

                if (!createIndexFileForExistingFiles)
                {
                    // Copy up each file
                    var oFileTools = new FileTools(managerName, debugLevel);

                    foreach (var entry in filesToCopy)
                    {
                        var sourceFilePath = Path.Combine(fiFastaFile.Directory.FullName, entry.Key);
                        var targetFilePath = Path.Combine(remoteIndexDirectory.FullName, entry.Key);

                        var success = oFileTools.CopyFileUsingLocks(sourceFilePath, targetFilePath, managerName, true);
                        if (!success)
                        {
                            errorMessage = "CopyFileUsingLocks returned false copying to " + targetFilePath;
                            return false;
                        }
                    }
                }

                // Create the .MSGFPlusIndexFileInfo file for this fasta file
                var fiMSGFPlusIndexFileInfo = new FileInfo(
                    Path.Combine(remoteIndexDirectory.FullName, fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                using (var writer = new StreamWriter(new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var entry in fileInfo)
                    {
                        writer.WriteLine(entry);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                errorMessage = "Exception in CopyIndexFilesToRemote; " + ex.Message;
                return false;
            }

        }

        /// <summary>
        /// Convert .Fasta file to indexed DB files compatible with MSGFPlus
        /// Will copy the files from msgfPlusIndexFilesDirPathBase if they exist
        /// </summary>
        /// <param name="logFileDir"></param>
        /// <param name="debugLevel">1 for normal, 2 for more verbose, 5 for the most verbose</param>
        /// <param name="javaProgLoc"></param>
        /// <param name="msgfPlusProgLoc">Path to the MS-GF+ .jar file</param>
        /// <param name="fastaFilePath">FASTA file path (on the local computer)</param>
        /// <param name="fastaFileIsDecoy">
        /// When True, only creates the forward-based index files.
        /// When False, creates both the forward and reverse index files
        /// </param>
        /// <param name="msgfPlusIndexFilesDirPathBase">Directory path from which to copy (or store) the index files</param>
        /// <param name="msgfPlusIndexFilesDirPathLegacyDB">
        /// Directory path from which to copy (or store) the index files for Legacy DBs
        /// (.fasta files not created from the protein sequences database)
        /// </param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateSuffixArrayFiles(string logFileDir, short debugLevel, string javaProgLoc, string msgfPlusProgLoc,
                                                   string fastaFilePath, bool fastaFileIsDecoy, string msgfPlusIndexFilesDirPathBase,
                                                   string msgfPlusIndexFilesDirPathLegacyDB)
        {
            const float MAX_WAIT_TIME_HOURS = 1.0f;

            var maxWaitTimeHours = MAX_WAIT_TIME_HOURS;

            var currentTask = "Initializing";

            try
            {
                mErrorMessage = string.Empty;

                if (debugLevel > 4)
                {
                    OnDebugEvent("clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter");
                }

                var fastaFile = new FileInfo(fastaFilePath);

                var msgfPlus = IsMSGFPlus(msgfPlusProgLoc);
                if (!msgfPlus)
                {
                    // Running legacy MSGFDB
                    throw new Exception("Legacy MSGFDB is no longer supported");
                }

                // Protein collection files will start with ID_ then have at least 6 integers, then an alphanumeric hash string, for example ID_004208_295531A4.fasta
                // If the filename does not match that pattern, we're using a legacy fasta file
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
                WaitForExistingLockfile(lockFile, debugLevel, maxWaitTimeHours);

                // Validate that all of the expected files exist
                // If any are missing, need to repeat the call to "BuildSA"
                bool reindexingRequired;

                currentTask = "Validating that expected files exist";

                // Check for any FastaFileName.revConcat.* files
                // If they exist, delete them, since they are for legacy MSGFDB

                var fiLegacyIndexedFiles = fastaFile.Directory.GetFiles(outputNameBase + ".revConcat.*");

                if (fiLegacyIndexedFiles.Length > 0)
                {
                    reindexingRequired = true;

                    for (var index = 0; index <= fiLegacyIndexedFiles.Length - 1; index++)
                    {
                        currentTask = "Deleting indexed file created by legacy MSGFDB: " + fiLegacyIndexedFiles[index].FullName;
                        if (debugLevel >= 1)
                        {
                            OnStatusEvent(currentTask);
                        }
                        fiLegacyIndexedFiles[index].Delete();
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
                        // Make sure all of the index files have a file modification date newer than the fasta file
                        // We only do this for legacy fasta files, since their file modification date will be the same on all pubs

                        // We can't do this for programatically generated fasta files (that use protein collections)
                        // since their modification date will be the time that the file was created

                        foreach (var fiIndexFile in existingFiles)
                        {
                            if (fiIndexFile.LastWriteTimeUtc < fastaFile.LastWriteTimeUtc.AddSeconds(-0.1))
                            {
                                OnStatusEvent("Index file is older than the fasta file; " + fiIndexFile.FullName + " modified " +
                                              fiIndexFile.LastWriteTimeUtc.ToLocalTime().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + " vs. " +
                                              fastaFile.LastWriteTimeUtc.ToLocalTime().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));

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
                    if (clsGlobal.OfflineMode)
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

                if (clsGlobal.OfflineMode)
                {
                    // The manager that pushed the FASTA files to the the remote host should have also indexed them and pushed all of the index files to this host
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
                                                                   debugLevel, maxWaitTimeHours, out var diskFreeSpaceBelowThreshold1);

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
                        out remoteLockFile, debugLevel, maxWaitTimeHours);
                }

                if (remoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files, we should once again try to copy the files locally

                    const bool CHECK_FOR_LOCK_FILE_B = false;
                    resultCode = CopyExistingIndexFilesFromRemote(fastaFile, usingLegacyFasta, remoteIndexDirPath, CHECK_FOR_LOCK_FILE_B,
                                                               debugLevel, maxWaitTimeHours, out var diskFreeSpaceBelowThreshold2);

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

                    if (remoteLockFileCreated && resultCode == CloseOutType.CLOSEOUT_SUCCESS && !clsGlobal.OfflineMode)
                    {
                        OnStatusEvent("Copying index files to " + remoteIndexDirPath);
                        CopyIndexFilesToRemote(fastaFile, remoteIndexDirPath, debugLevel);
                    }
                }

                if (!clsGlobal.OfflineMode)
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
                mErrorMessage = "Exception in .CreateIndexedDbFiles";
                OnErrorEvent(mErrorMessage + "; " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private CloseOutType CreateSuffixArrayFilesWork(string logFileDir, int debugLevel, FileInfo fiFastaFile,
                                                        FileSystemInfo fiLockFile, string javaProgLoc, string msgfPlusProgLoc, bool fastaFileIsDecoy,
                                                        string dbCsArrayFilename)
        {
            var currentTask = string.Empty;

            try
            {
                // Try to create the index files for fasta file dBFileNameInput
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
                // Examine the size of the .Fasta file to determine how much ram to reserve
                int javaMemorySizeMB;

                var fastaFileSizeMB = clsGlobal.BytesToMB(fiFastaFile.Length);

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
                if (!clsAnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "BuildSA", false))
                {
                    mErrorMessage = "Cannot run BuildSA since less than " + javaMemorySizeMB + " MB of free memory";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create a lock file
                if (debugLevel >= 3)
                {
                    OnStatusEvent("Creating lock file: " + fiLockFile.FullName);
                }

                // Delay between 2 and 5 seconds
                var oRandom = new Random();
                clsGlobal.IdleLoop(oRandom.Next(2, 5));

                // Check one more time for a lock file
                // If it exists, another manager just created it and we should abort
                currentTask = "Look for the lock file one last time";
                fiLockFile.Refresh();
                if (fiLockFile.Exists)
                {
                    if (debugLevel >= 1)
                    {
                        OnStatusEvent("Warning: new lock file found: " + fiLockFile.FullName + "; aborting");
                        return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                    }
                }

                // Create a lock file in the directory that the index files will be created
                currentTask = "Create the local lock file: " + fiLockFile.FullName;
                var success = CreateLockFile(fiLockFile.FullName);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete any existing index files (BuildSA throws an error if they exist)
                currentTask = "Delete any existing files";

                var outputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name);

                var existingFiles = FindExistingSuffixArrayFiles(
                    fastaFileIsDecoy, outputNameBase, fiFastaFile.DirectoryName,
                    new List<string>(), out _, out _);

                foreach (var fiIndexFileToDelete in existingFiles)
                {
                    if (fiIndexFileToDelete.Exists)
                    {
                        fiIndexFileToDelete.Delete();
                    }
                }

                if (debugLevel >= 2)
                {
                    OnStatusEvent("Creating Suffix Array database file: " + dbCsArrayFilename);
                }

                // Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)
                currentTask = "Construct BuildSA command line";

                var arguments = " -Xmx" + javaMemorySizeMB + "M -cp " + msgfPlusProgLoc;

                arguments += " edu.ucsd.msjava.msdbsearch.BuildSA -d " + fiFastaFile.FullName;

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
                var buildSA = new clsRunDosProgram(fiFastaFile.DirectoryName, debugLevel)
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
                // This process generally doesn't take that long so we do not track CPU usage
                success = buildSA.RunProgram(javaProgLoc, arguments, "BuildSA", true);

                if (!success)
                {
                    mErrorMessage = "Error running BuildSA with " + Path.GetFileName(msgfPlusProgLoc) + " for " + fiFastaFile.Name;

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
                    DeleteLockFile(fiLockFile);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (debugLevel >= 1)
                {
                    OnStatusEvent("Created suffix array files for " + fiFastaFile.Name);
                }

                if (debugLevel >= 3)
                {
                    OnStatusEvent("Deleting lock file: " + fiLockFile.FullName);
                }

                // Delete the lock file
                currentTask = "Delete the lock file";
                DeleteLockFile(fiLockFile);
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in .CreateSuffixArrayFilesWork";
                OnErrorEvent(mErrorMessage + "; " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a lock file
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        /// <remarks>Returns false if the lock file already exists</remarks>
        private bool CreateLockFile(string lockFilePath)
        {
            try
            {
                using (var writer = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    // Use local time for dates in lock files
                    writer.WriteLine("Date: " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    writer.WriteLine("Manager: " + mMgrName);
                }

                return true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error creating lock file";
                OnErrorEvent("clsCreateMSGFDBSuffixArrayFiles.CreateLockFile, " + mErrorMessage, ex);
                return false;
            }

        }

        private bool CreateRemoteSuffixArrayLockFile(
            string fastaFileName, string remoteIndexDirPath,
            out FileInfo fiRemoteLockFile, int debugLevel, float maxWaitTimeHours)
        {
            var remoteIndexDirectory = new DirectoryInfo(remoteIndexDirPath);

            if (remoteIndexDirectory.Parent == null || !remoteIndexDirectory.Parent.Exists)
            {
                OnErrorEvent("Cannot read/write MS-GF+ index files from remote share; directory not found; " + remoteIndexDirectory.FullName);
                fiRemoteLockFile = null;
                return false;
            }

            fiRemoteLockFile =
                new FileInfo(Path.Combine(remoteIndexDirectory.FullName, fastaFileName + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

            var currentTask = "Looking for lock file " + fiRemoteLockFile.FullName;
            WaitForExistingLockfile(fiRemoteLockFile, debugLevel, maxWaitTimeHours);

            try
            {
                if (!remoteIndexDirectory.Exists)
                {
                    remoteIndexDirectory.Create();
                }

                // Create the remote lock file
                if (!CreateLockFile(fiRemoteLockFile.FullName))
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

        private void DeleteLockFile(FileSystemInfo fiLockFile)
        {
            try
            {
                fiLockFile.Refresh();
                if (fiLockFile.Exists)
                {
                    fiLockFile.Delete();
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
        /// <param name="remoteIndexDirPath"></param>
        /// <param name="debugLevel"></param>
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
                    using (var purgeInfoWriter = new StreamWriter(new FileStream(purgeInfoFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Use local time for the date in the purge Info file
                        purgeInfoWriter.WriteLine(DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                        purgeInfoWriter.WriteLine("Manager: " + mMgrName);
                    }
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
                    OnStatusEvent(string.Format(
                        "Create file {0} with 'MaxSizeGB=50' on a single line. " +
                        "Comment lines are allowed using # as a comment character", maxDirSizeFile.Name));

                    return;
                }

                // MaxDirSize.txt file exists; this file specifies the max total GB that files orgDbFolder can use
                clsAnalysisResources.PurgeFastaFilesUsingSpaceUsedThreshold(maxDirSizeFile, "", debugLevel, preview: false);
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

            string remoteIndexDirectory;

            // DMS-generated fasta files will have a name of the form ID_003949_3D6802EE.fasta
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

                    remoteIndexDirectory = Path.Combine(msgfPlusIndexFilesDirPathBase, directoryName);
                    return remoteIndexDirectory;
                }
            }

            if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesDirPathLegacyDB))
                return string.Empty;

            remoteIndexDirectory = Path.Combine(msgfPlusIndexFilesDirPathLegacyDB, "Other");
            return remoteIndexDirectory;
        }

        /// <summary>
        /// Constructs a list of suffix array files that should exist
        /// Looks for each of those files
        /// </summary>
        /// <param name="fastaFileIsDecoy"></param>
        /// <param name="outputNameBase"></param>
        /// <param name="directoryPathToSearch"></param>
        /// <param name="filesToFind">List of files that should exist; calling function must have initialized it</param>
        /// <param name="existingFileList">Output param: semicolon separated list of existing files</param>
        /// <param name="missingFiles">Output param: semicolon separated list of missing files</param>
        /// <returns>A list of the files that currently exist</returns>
        /// <remarks></remarks>
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

                var fiFileToFind = new FileInfo(Path.Combine(directoryPathToSearch, fileNameToFind));

                if (fiFileToFind.Exists)
                {
                    existingFiles.Add(fiFileToFind);
                    existingFileList = clsGlobal.AppendToComment(existingFileList, fileNameToFind);
                }
                else
                {
                    missingFiles = clsGlobal.AppendToComment(missingFiles, fileNameToFind);
                }
            }

            return existingFiles;
        }

        private string GetPseudoManagerName()
        {
            var mgrName = mMgrName + "_CreateMSGFDBSuffixArrayFiles";

            return mgrName;
        }

        /// <summary>
        /// Check whether jarFilePath matches MSGFDB.jar
        /// </summary>
        /// <param name="jarFilePath"></param>
        /// <returns></returns>
        public bool IsMSGFPlus(string jarFilePath)
        {
            const string MSGFDB_JAR_NAME = "MSGFDB.jar";

            var fiJarFile = new FileInfo(jarFilePath);

            if (string.Equals(fiJarFile.Name, MSGFDB_JAR_NAME, StringComparison.OrdinalIgnoreCase))
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
        /// <param name="consoleOutputFilePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private string ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    OnWarningEvent("BuildSA console output file not found: " + consoleOutputFilePath);
                    return string.Empty;
                }

                using (var srReader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srReader.EndOfStream)
                    {
                        var dataLine = srReader.ReadLine();
                        if (dataLine != null && dataLine.StartsWith("Error", StringComparison.OrdinalIgnoreCase))
                        {
                            OnErrorEvent("BuildSA reports: " + dataLine);
                            if (dataLine.Contains("too many redundant proteins"))
                            {
                                return "Error while indexing, too many redundant proteins";
                            }
                            return dataLine;
                        }
                    }
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
                    using (var writer = new StreamWriter(new FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Use UtcNow for dates in LastUsed files
                        writer.WriteLine(DateTime.UtcNow.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    }

                }
                catch (IOException)
                {
                    // The file is likely open by another manager; ignore this
                }
                catch (Exception ex)
                {
                    OnWarningEvent(string.Format("Unable to create a new .LastUsed file at {0}: {1}", lastUsedFilePath, ex.Message));
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in UpdateRemoteLastUsedFile", ex);
            }
        }

        /// <summary>
        /// Open the FastaFileName.canno file and read the first two lines
        /// If there is a number on the first line but the second line starts with the letter A, this file was created with the legacy MSGFDB
        /// </summary>
        /// <param name="fastaFile">FASTA file</param>
        /// <param name="outputNameBase">Base output name</param>
        /// <param name="debugLevel">Debug level</param>
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
        /// <param name="dtMinWriteTimeThresholdUTC"></param>
        /// <param name="verifyingRemoteDirectory">True when validating files on a remote server, false if verifying the local DMS_Temp_Org directory</param>
        /// <returns>True if all files are found and are the right size</returns>
        /// <remarks></remarks>
        private bool ValidateFiles(string directoryPathToCheck, Dictionary<string, long> filesToCopy, bool usingLegacyFasta,
                                   DateTime dtMinWriteTimeThresholdUTC, bool verifyingRemoteDirectory)
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
                var fiSourceFile = new FileInfo(Path.Combine(directoryPathToCheck, entry.Key));

                if (!fiSourceFile.Exists)
                {
                    // Remote MS-GF+ index file not found
                    // Local MS-GF+ index file not found
                    OnWarningEvent(sourceDescription + " not found: " + fiSourceFile.FullName);
                    return false;
                }

                if (fiSourceFile.Length != entry.Value)
                {
                    // Remote MS-GF+ index file is not the expected size
                    // Local MS-GF+ index file is not the expected size
                    OnWarningEvent(sourceDescription + " is not the expected size: " + fiSourceFile.FullName + " should be " + entry.Value +
                                   " bytes but is actually " + fiSourceFile.Length + " bytes");
                    return false;
                }

                if (usingLegacyFasta)
                {
                    // Require that the index files be newer than the fasta file (ignore the .LastUsed file)
                    if (fiSourceFile.LastWriteTimeUtc < dtMinWriteTimeThresholdUTC.AddSeconds(-0.1))
                    {
                        if (!string.Equals(fiSourceFile.Extension, FileSyncUtils.LASTUSED_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        {
                            var sourceFileDate = fiSourceFile.LastWriteTimeUtc.ToLocalTime().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);
                            var dateThreshold= dtMinWriteTimeThresholdUTC.ToLocalTime().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);

                            OnStatusEvent(string.Format("{0} is older than the fasta file; {1} modified {2} vs. {3}; indexing is required",
                                                        sourceDescription,
                                                        fiSourceFile.FullName,
                                                        sourceFileDate,
                                                        dateThreshold));

                            return false;
                        }
                    }
                }
            }

            return true;
        }

        private void WaitForExistingLockfile(FileSystemInfo fiLockFile, int debugLevel, float maxWaitTimeHours)
        {
            // Check to see if another Analysis Manager is already creating the indexed DB files
            if (fiLockFile.Exists && DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes >= 60)
            {
                // Lock file is over 60 minutes old; delete it
                if (debugLevel >= 1)
                {
                    OnStatusEvent("Lock file is over 60 minutes old (created " +
                        fiLockFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + "); " +
                        "deleting " + fiLockFile.FullName);
                }
                DeleteLockFile(fiLockFile);
                return;
            }

            if (!fiLockFile.Exists)
                return;

            if (debugLevel >= 1)
            {
                OnStatusEvent("Lock file found: " + fiLockFile.FullName +
                              "; waiting for file to be removed by other manager generating suffix array files");
            }

            // Lock file found; wait up to maxWaitTimeHours hours
            var staleFile = false;
            while (fiLockFile.Exists)
            {
                // Sleep for 2 seconds
                clsGlobal.IdleLoop(2);

                if (DateTime.UtcNow.Subtract(fiLockFile.CreationTimeUtc).TotalHours >= maxWaitTimeHours)
                {
                    staleFile = true;
                    break;
                }

                fiLockFile.Refresh();
            }

            // If the duration time has exceeded maxWaitTimeHours, delete the lock file and try again with this manager
            if (staleFile)
            {
                var logMessage = "Waited over " + maxWaitTimeHours.ToString("0.0") +
                                 " hour(s) for lock file to be deleted, but it is still present; " +
                                 "deleting the file now and continuing: " + fiLockFile.FullName;
                OnWarningEvent(logMessage);
                DeleteLockFile(fiLockFile);
            }
        }

    }
}
