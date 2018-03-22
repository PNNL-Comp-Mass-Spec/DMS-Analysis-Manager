//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Create MSGF+ suffix array files
    /// </summary>
    public class clsCreateMSGFDBSuffixArrayFiles : clsEventNotifier
    {
        #region "Constants"

        private const string MSGF_PLUS_INDEX_FILE_INFO_SUFFIX = ".MSGFPlusIndexFileInfo";

        #endregion

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

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

        private CloseOutType CopyExistingIndexFilesFromRemote(FileInfo fiFastaFile, bool usingLegacyFasta, string remoteIndexFolderPath,
                                                              bool checkForLockFile, int debugLevel, float maxWaitTimeHours,
                                                              out bool diskFreeSpaceBelowThreshold)
        {
            bool success;

            diskFreeSpaceBelowThreshold = false;

            try
            {
                var diRemoteIndexFolderPath = new DirectoryInfo(remoteIndexFolderPath);

                if (!diRemoteIndexFolderPath.Exists)
                {
                    // This is not a critical error
                    OnDebugEvent(string.Format("Remote index folder not found ({0}); indexing is required", remoteIndexFolderPath));
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
                    var fiRemoteLockFile1 = new FileInfo(Path.Combine(
                        diRemoteIndexFolderPath.FullName,
                        fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

                    WaitForExistingLockfile(fiRemoteLockFile1, debugLevel, maxWaitTimeHours);
                }

                // Look for the .MSGFPlusIndexFileInfo file for this fasta file
                var fiMSGFPlusIndexFileInfo = new FileInfo(Path.Combine(
                    diRemoteIndexFolderPath.FullName,
                    fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                long fileSizeTotalKB = 0;

                if (!fiMSGFPlusIndexFileInfo.Exists)
                {
                    OnDebugEvent(string.Format("{0} not found at {1}; indexing is required",
                        fiMSGFPlusIndexFileInfo.Name, diRemoteIndexFolderPath.FullName));
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Read the filenames in the file
                // There should be 3 columns: FileName, FileSize, and FileDateUTC
                // When looking for existing files we only require that the FileSize match; FileDateUTC is not used

                var filesToCopy = new Dictionary<string, long>();

                using (
                    var srInFile =
                        new StreamReader(new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var dataLine = srInFile.ReadLine();
                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        var data = dataLine.Split('\t').ToList();

                        if (data.Count >= 3)
                        {
                            // Add this file to the list of files to copy
                            if (long.TryParse(data[1], out var fileSizeBytes))
                            {
                                filesToCopy.Add(data[0], fileSizeBytes);
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
                        diRemoteIndexFolderPath.FullName, filesToCopy, usingLegacyFasta,
                        fiFastaFile.LastWriteTimeUtc, true);
                }

                if (!filesAreValid)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (debugLevel >= 1 && fileSizeTotalKB >= 1000)
                {
                    OnStatusEvent("Copying existing MSGF+ index files from " + diRemoteIndexFolderPath.FullName);
                }

                // Copy each file in filesToCopy (overwrite existing files)
                var manager = GetPseudoManagerName();

                var filesCopied = 0;
                var dtLastStatusUpdate = DateTime.UtcNow;

                var oFileTools = new clsFileTools(manager, debugLevel);
                RegisterEvents(oFileTools);

                // Compute the total disk space required
                long fileSizeTotalBytes = 0;

                foreach (var entry in filesToCopy)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key));
                    fileSizeTotalBytes += fiSourceFile.Length;
                }

                const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750;

                // Convert fileSizeTotalBytes to MB, but add on a Default_Min_free_Space to assure we'll still have enough free space after copying over the files
                var minFreeSpaceMB = (int)(fileSizeTotalBytes / 1024.0 / 1024.0 + DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

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
                    // then we should once again check to see if the required files exist

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
                    var fiSourceFile = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key));

                    var targetFilePath = Path.Combine(fiFastaFile.Directory.FullName, fiSourceFile.Name);
                    oFileTools.CopyFileUsingLocks(fiSourceFile, targetFilePath, manager, true);

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

        private bool CopyIndexFilesToRemote(FileInfo fiFastaFile, string remoteIndexFolderPath, int debugLevel)
        {
            var manager = GetPseudoManagerName();
            const bool createIndexFileForExistingFiles = false;

            var success = CopyIndexFilesToRemote(fiFastaFile, remoteIndexFolderPath, debugLevel, manager, createIndexFileForExistingFiles,
                                                 out var errorMessage);
            if (!success)
            {
                OnErrorEvent(errorMessage);
            }

            return success;
        }

        /// <summary>
        /// Copies the suffix array files for the specified fasta file to the remote MSGFPlus_Index_File folder share
        /// </summary>
        /// <param name="fiFastaFile"></param>
        /// <param name="remoteIndexFolderPath"></param>
        /// <param name="debugLevel"></param>
        /// <param name="managerName">Manager name (only required because the constructor for PRISM.clsFileTools requires this)</param>
        /// <param name="createIndexFileForExistingFiles">
        /// When true, assumes that the index files were previously copied to remoteIndexFolderPath,
        /// and we should simply create the .MSGFPlusIndexFileInfo file for the matching files
        /// This option is used by the MSGFPlusIndexFileCopier program when switch /X is provided
        /// </param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>This function is used both by this class and by the MSGFPlusIndexFileCopier console application</remarks>
        public static bool CopyIndexFilesToRemote(FileInfo fiFastaFile, string remoteIndexFolderPath, int debugLevel, string managerName,
                                                  bool createIndexFileForExistingFiles, out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                if (string.IsNullOrWhiteSpace(remoteIndexFolderPath))
                    throw new ArgumentException("Remote index folder path cannot be empty", nameof(remoteIndexFolderPath));

                var diRemoteIndexFolderPath = new DirectoryInfo(remoteIndexFolderPath);

                if (diRemoteIndexFolderPath.Parent == null || !diRemoteIndexFolderPath.Parent.Exists)
                {
                    errorMessage = "Parent folder for the MSGF+ index files folder not found: " + remoteIndexFolderPath;
                    return false;
                }

                if (!diRemoteIndexFolderPath.Exists)
                {
                    diRemoteIndexFolderPath.Create();
                }

                if (createIndexFileForExistingFiles)
                {
                    var remoteFastaPath = Path.Combine(remoteIndexFolderPath, fiFastaFile.Name);
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
                                    fiSourceFile.LastWriteTimeUtc.ToString(DATE_TIME_FORMAT));
                }

                if (!createIndexFileForExistingFiles)
                {
                    // Copy up each file
                    var oFileTools = new clsFileTools(managerName, debugLevel);

                    foreach (var entry in filesToCopy)
                    {
                        var sourceFilePath = Path.Combine(fiFastaFile.Directory.FullName, entry.Key);
                        var targetFilePath = Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key);

                        var success = oFileTools.CopyFileUsingLocks(sourceFilePath, targetFilePath, managerName, true);
                        if (!success)
                        {
                            errorMessage = "CopyFileUsingLocks returned false copying to " + targetFilePath;
                            return false;
                        }
                    }
                }

                // Create the .MSGFPlusIndexFileInfo file for this fasta file
                var fiMSGFPlusIndexFileInfo = new FileInfo(Path.Combine(
                    diRemoteIndexFolderPath.FullName,
                    fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                using (var swOutFile = new StreamWriter(new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var entry in fileInfo)
                    {
                        swOutFile.WriteLine(entry);
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
        /// Will copy the files from msgfPlusIndexFilesFolderPathBase if they exist
        /// </summary>
        /// <param name="logFileDir"></param>
        /// <param name="debugLevel"></param>
        /// <param name="javaProgLoc"></param>
        /// <param name="msgfPlusProgLoc">Path to the MSGF+ .jar file</param>
        /// <param name="fastaFilePath">FASTA file path (on the local computer)</param>
        /// <param name="fastaFileIsDecoy">
        /// When True, only creates the forward-based index files.
        /// When False, creates both the forward and reverse index files
        /// </param>
        /// <param name="msgfPlusIndexFilesFolderPathBase">Folder path from which to copy (or store) the index files</param>
        /// <param name="msgfPlusIndexFilesFolderPathLegacyDB">
        /// Folder path from which to copy (or store) the index files for Legacy DBs
        /// (.fasta files not created from the protein sequences database)
        /// </param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateSuffixArrayFiles(string logFileDir, int debugLevel, string javaProgLoc, string msgfPlusProgLoc,
                                                   string fastaFilePath, bool fastaFileIsDecoy, string msgfPlusIndexFilesFolderPathBase,
                                                   string msgfPlusIndexFilesFolderPathLegacyDB)
        {
            const float MAX_WAITTIME_HOURS = 1.0f;

            var maxWaitTimeHours = MAX_WAITTIME_HOURS;

            var currentTask = "Initializing";

            try
            {
                mErrorMessage = string.Empty;

                if (debugLevel > 4)
                {
                    OnDebugEvent("clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter");
                }

                var fiFastaFile = new FileInfo(fastaFilePath);

                var msgfPlus = IsMSGFPlus(msgfPlusProgLoc);
                if (!msgfPlus)
                {
                    // Running legacy MS-GFDB
                    throw new Exception("Legacy MS-GFDB is no longer supported");
                }

                // Protein collection files will start with ID_ then have at least 6 integers, then an alphanumeric hash string, for example ID_004208_295531A4.fasta
                // If the filename does not match that pattern, we're using a legacy fasta file
                var reProtectionCollectionFasta = new Regex(@"ID_\d{6,}_[0-9a-z]+\.fasta", RegexOptions.IgnoreCase);
                var usingLegacyFasta = !reProtectionCollectionFasta.IsMatch(fiFastaFile.Name);

                //  Look for existing suffix array files
                var outputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name);

                if (fiFastaFile.Directory == null || fiFastaFile.DirectoryName == null)
                {
                    mErrorMessage = "Cannot determine the parent directory of " + fiFastaFile.FullName;
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var fiLockFile = new FileInfo(Path.Combine(fiFastaFile.DirectoryName, outputNameBase + "_csarr.lock"));
                var dbSarrayFilename = Path.Combine(fiFastaFile.DirectoryName, outputNameBase + ".csarr");

                // Check to see if another Analysis Manager is already creating the indexed DB files
                currentTask = "Looking for lock file " + fiLockFile.FullName;
                WaitForExistingLockfile(fiLockFile, debugLevel, maxWaitTimeHours);

                // Validate that all of the expected files exist
                // If any are missing, need to repeat the call to "BuildSA"
                var reindexingRequired = false;

                currentTask = "Validating that expected files exist";

                // Check for any FastaFileName.revConcat.* files
                // If they exist, delete them, since they are for legacy MSGFDB

                var fiLegacyIndexedFiles = fiFastaFile.Directory.GetFiles(outputNameBase + ".revConcat.*");

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
                    // Open the FastaFileName.canno file and read the first two lines
                    // If there is a number on the first line but the second line starts with the letter A, this file was created with the legacy MSGFDB
                    var fiCAnnoFile = new FileInfo(Path.Combine(fiFastaFile.DirectoryName, outputNameBase + ".canno"));
                    if (fiCAnnoFile.Exists)
                    {
                        currentTask = "Examining first two lines of " + fiCAnnoFile.FullName;
                        using (var srCannoFile = new StreamReader(new FileStream(fiCAnnoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            if (!srCannoFile.EndOfStream)
                            {
                                var line1 = srCannoFile.ReadLine();

                                if (!srCannoFile.EndOfStream)
                                {
                                    var line2 = srCannoFile.ReadLine();

                                    if (int.TryParse(line1, out _))
                                    {
                                        if (!string.IsNullOrWhiteSpace(line2) && char.IsLetter(line2[0]))
                                        {
                                            currentTask = "Legacy MSGFDB indexed file found (" + fiCAnnoFile.Name + "); re-indexing";
                                            if (debugLevel >= 1)
                                            {
                                                OnStatusEvent(currentTask);
                                            }
                                            reindexingRequired = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // This dictionary contains file suffixes to look for
                // Keys will be "True" if the file exists and false if it does not exist
                var filesToFind = new List<string>();

                if (!reindexingRequired)
                {

                    currentTask = "Validating that expected files exist";
                    var existingFiles = FindExistingSuffixArrayFiles(
                        fastaFileIsDecoy, outputNameBase, fiFastaFile.DirectoryName,
                        filesToFind, out var existingFileList, out var missingFiles);

                    if (existingFiles.Count < filesToFind.Count)
                    {
                        reindexingRequired = true;

                        currentTask = "Some files are missing: " + existingFiles.Count + " vs. " + filesToFind.Count;
                        if (existingFiles.Count > 0)
                        {
                            if (debugLevel >= 1)
                            {
                                OnWarningEvent("Indexing of " + fiFastaFile.Name + " was incomplete (found " + existingFiles.Count + " out of " +
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
                        //   since their modification date will be the time that the file was created

                        foreach (var fiIndexFile in existingFiles)
                        {
                            if (fiIndexFile.LastWriteTimeUtc < fiFastaFile.LastWriteTimeUtc.AddSeconds(-0.1))
                            {
                                OnStatusEvent("Index file is older than the fasta file; " + fiIndexFile.FullName + " modified " +
                                              fiIndexFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT) + " vs. " +
                                              fiFastaFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT));

                                reindexingRequired = true;
                                break;
                            }
                        }
                    }
                }

                if (!reindexingRequired)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                // Index files are missing or out of date

                if (clsGlobal.OfflineMode)
                {
                    // The manager that pushed the FASTA files to the the remote host should have also indexed them and pushed all of the index files to this host
                    // Fail out the job
                    mErrorMessage = "Index files are missing or out of date for " + fastaFilePath;

                    // Do not call OnErrorEvent; the calling procedure will do so
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Copy the missing index files from msgfPlusIndexFilesFolderPathBase or msgfPlusIndexFilesFolderPathLegacyDB if possible
                // Otherwise, create new index files

                var remoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(
                    fiFastaFile.Name, msgfPlusIndexFilesFolderPathBase, msgfPlusIndexFilesFolderPathLegacyDB);

                const bool CHECK_FOR_LOCK_FILE_A = true;
                var eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, usingLegacyFasta, remoteIndexFolderPath, CHECK_FOR_LOCK_FILE_A,
                                                                        debugLevel, maxWaitTimeHours, out var diskFreeSpaceBelowThreshold1);

                if (diskFreeSpaceBelowThreshold1)
                {
                    // Not enough free disk space; abort
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }

                // Files did not exist or were out of date, or an error occurred while copying them
                currentTask = "Create a remote lock file";
                var remoteLockFileCreated = CreateRemoteSuffixArrayLockFile(
                    fiFastaFile.Name, remoteIndexFolderPath,
                    out var fiRemoteLockFile, debugLevel, maxWaitTimeHours);

                if (remoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files, we should once again try to copy the files locally

                    const bool CHECK_FOR_LOCK_FILE_B = false;
                    eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, usingLegacyFasta, remoteIndexFolderPath, CHECK_FOR_LOCK_FILE_B,
                                                               debugLevel, maxWaitTimeHours, out var diskFreeSpaceBelowThreshold2);

                    if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
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
                    OnStatusEvent("Running BuildSA to index " + fiFastaFile.Name);

                    // Note that this method will create a local .lock file
                    eResult = CreateSuffixArrayFilesWork(logFileDir, debugLevel, fiFastaFile, fiLockFile, javaProgLoc,
                                                         msgfPlusProgLoc, fastaFileIsDecoy, dbSarrayFilename);

                    if (remoteLockFileCreated && eResult == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        OnStatusEvent("Copying index files to " + remoteIndexFolderPath);
                        CopyIndexFilesToRemote(fiFastaFile, remoteIndexFolderPath, debugLevel);
                    }
                }

                if (remoteLockFileCreated)
                {
                    // Delete the remote lock file
                    DeleteLockFile(fiRemoteLockFile);
                }

                return eResult;
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
                                                        string dbSarrayFilename)
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

                var fastaFileSizeMB = (int)(fiFastaFile.Length / 1024.0 / 1024.0);

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
                Thread.Sleep(oRandom.Next(2, 5) * 1000);

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

                // Create a lock file in the folder that the index files will be created
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
                    OnStatusEvent("Creating Suffix Array database file: " + dbSarrayFilename);
                }

                // Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)
                currentTask = "Construct BuildSA command line";

                var cmdStr = " -Xmx" + javaMemorySizeMB + "M -cp " + msgfPlusProgLoc;

                cmdStr += " edu.ucsd.msjava.msdbsearch.BuildSA -d " + fiFastaFile.FullName;

                if (fastaFileIsDecoy)
                {
                    cmdStr += " -tda 0";
                }
                else
                {
                    cmdStr += " -tda 2";
                }

                if (debugLevel >= 1)
                {
                    OnStatusEvent(javaProgLoc + " " + cmdStr);
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

                currentTask = "Run BuildSA using " + cmdStr;

                // Run BuildSA and wait for it to exit
                // This process generally doesn't take that long so we do not track CPU usage
                success = buildSA.RunProgram(javaProgLoc, cmdStr, "BuildSA", true);

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
                using (var swLockFile = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
                {
                    swLockFile.WriteLine("Date: " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    swLockFile.WriteLine("Manager: " + mMgrName);
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

        private bool CreateRemoteSuffixArrayLockFile(string fastaFileName, string remoteIndexFolderPath, out FileInfo fiRemoteLockFile,
                                                     int debugLevel, float maxWaitTimeHours)
        {
            var diRemoteIndexFolderPath = new DirectoryInfo(remoteIndexFolderPath);

            if (diRemoteIndexFolderPath.Parent == null || !diRemoteIndexFolderPath.Parent.Exists)
            {
                OnErrorEvent("Cannot read/write MSGF+ index files from remote share; folder not found; " + diRemoteIndexFolderPath.FullName);
                fiRemoteLockFile = null;
                return false;
            }

            fiRemoteLockFile =
                new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fastaFileName + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

            var currentTask = "Looking for lock file " + fiRemoteLockFile.FullName;
            WaitForExistingLockfile(fiRemoteLockFile, debugLevel, maxWaitTimeHours);

            try
            {
                if (!diRemoteIndexFolderPath.Exists)
                {
                    diRemoteIndexFolderPath.Create();
                }

                // Create the remote lock file
                if (!CreateLockFile(fiRemoteLockFile.FullName))
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception creating remote MSGF+ suffix array lock file at " + diRemoteIndexFolderPath.FullName + "; " + currentTask,
                             ex);
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

        private string DetermineRemoteMSGFPlusIndexFilesFolderPath(string fastaFileName, string msgfPlusIndexFilesFolderPathBase,
                                                                   string msgfPlusIndexFilesFolderPathLegacyDB)
        {
            var reExtractNum = new Regex(@"^ID_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            string remoteIndexFolderPath;

            // DMS-generated fasta files will have a name of the form ID_003949_3D6802EE.fasta
            // Parse out the number (003949 in this case)
            var reMatch = reExtractNum.Match(fastaFileName);
            if (reMatch.Success)
            {

                if (int.TryParse(reMatch.Groups[1].Value, out var generatedFastaFileNumber))
                {
                    // Round down to the nearest 1000
                    // Thus, 003949 will round to 3000
                    var folderName = (Math.Floor(generatedFastaFileNumber / 1000.0) * 1000).ToString("0");

                    if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesFolderPathBase))
                        return string.Empty;

                    remoteIndexFolderPath = Path.Combine(msgfPlusIndexFilesFolderPathBase, folderName);
                    return remoteIndexFolderPath;
                }
            }

            if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesFolderPathLegacyDB))
                return string.Empty;

            remoteIndexFolderPath = Path.Combine(msgfPlusIndexFilesFolderPathLegacyDB, "Other");
            return remoteIndexFolderPath;
        }

        /// <summary>
        /// Constructs a list of suffix array files that should exist
        /// Looks for each of those files
        /// </summary>
        /// <param name="fastaFileIsDecoy"></param>
        /// <param name="outputNameBase"></param>
        /// <param name="folderPathToSearch"></param>
        /// <param name="filesToFind">List of files that should exist; calling function must have initialized it</param>
        /// <param name="existingFileList">Output param: semicolon separated list of existing files</param>
        /// <param name="missingFiles">Output param: semicolon separated list of missing files</param>
        /// <returns>A list of the files that currently exist</returns>
        /// <remarks></remarks>
        private List<FileInfo> FindExistingSuffixArrayFiles(
            bool fastaFileIsDecoy, string outputNameBase,
            string folderPathToSearch, ICollection<string> filesToFind,
            out string existingFileList,
            out string missingFiles)
        {
            var existingFiles = new List<FileInfo>();

            filesToFind.Clear();

            existingFileList = string.Empty;
            missingFiles = string.Empty;

            // Suffixes for MSGFDB (effective 8/22/2011) and MSGF+
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

                var fiFileToFind = new FileInfo(Path.Combine(folderPathToSearch, fileNameToFind));

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

            if (string.Compare(fiJarFile.Name, MSGFDB_JAR_NAME, StringComparison.OrdinalIgnoreCase) == 0)
            {
                // Not MSGF+
                return false;
            }

            // Using MSGF+
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

        /// <summary>
        /// Verifies that each of the files specified by filesToCopy exists at folderPathToCheck and has the correct file size
        /// </summary>
        /// <param name="folderPathToCheck">folder to check</param>
        /// <param name="filesToCopy">Dictionary with filenames and file sizes</param>
        /// <param name="usingLegacyFasta">True when using a legacy FASTA file (not protein collection based)</param>
        /// <param name="dtMinWriteTimeThresholdUTC"></param>
        /// <param name="verifyingRemoteFolder">True when validating files on a remote server, false if verifying the local DMS_Temp_Org folder</param>
        /// <returns>True if all files are found and are the right size</returns>
        /// <remarks></remarks>
        [SuppressMessage("ReSharper", "UseFormatSpecifierInFormatString")]
        private bool ValidateFiles(string folderPathToCheck, Dictionary<string, long> filesToCopy, bool usingLegacyFasta,
                                   DateTime dtMinWriteTimeThresholdUTC, bool verifyingRemoteFolder)
        {
            string sourceDescription;
            if (verifyingRemoteFolder)
            {
                sourceDescription = "Remote MSGF+ index file";
            }
            else
            {
                sourceDescription = "Local MSGF+ index file";
            }

            foreach (var entry in filesToCopy)
            {
                var fiSourceFile = new FileInfo(Path.Combine(folderPathToCheck, entry.Key));

                if (!fiSourceFile.Exists)
                {
                    // Remote MSGF+ index file not found
                    // Local MSGF+ index file not found
                    OnWarningEvent(sourceDescription + " not found: " + fiSourceFile.FullName);
                    return false;
                }

                if (fiSourceFile.Length != entry.Value)
                {
                    // Remote MSGF+ index file is not the expected size
                    // Local MSGF+ index file is not the expected size
                    OnWarningEvent(sourceDescription + " is not the expected size: " + fiSourceFile.FullName + " should be " + entry.Value +
                                   " bytes but is actually " + fiSourceFile.Length + " bytes");
                    return false;
                }

                if (usingLegacyFasta)
                {
                    // Require that the index files be newer than the fasta file
                    if (fiSourceFile.LastWriteTimeUtc < dtMinWriteTimeThresholdUTC.AddSeconds(-0.1))
                    {
                        OnStatusEvent(string.Format("{0} is older than the fasta file; {1} modified {2} vs. {3}; indexing is required",
                                      sourceDescription,
                                      fiSourceFile.FullName,
                                      fiSourceFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT),
                                      dtMinWriteTimeThresholdUTC.ToLocalTime().ToString(DATE_TIME_FORMAT)));

                        return false;
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
