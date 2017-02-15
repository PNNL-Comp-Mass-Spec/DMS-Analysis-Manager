//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMSGFDBPlugIn
{
    public class clsCreateMSGFDBSuffixArrayFiles : clsEventNotifier
    {
        #region "Constants"

        public const string LEGACY_MSGFDB_SUBDIRECTORY_NAME = "Legacy_MSGFDB";
        private const string MSGF_PLUS_INDEX_FILE_INFO_SUFFIX = ".MSGFPlusIndexFileInfo";

        #endregion

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        #region "Module Variables"

        private string mErrorMessage = string.Empty;
        private readonly string mMgrName;

        private string mPICHPCUser;
        private string mPICHPCPassword;

#if EnableHPC
        private HPC_Submit.WindowsHPC2012 mComputeCluster;
#endif

        #endregion

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strManagerName"></param>
        public clsCreateMSGFDBSuffixArrayFiles(string strManagerName) : this(strManagerName, string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strManagerName"></param>
        public clsCreateMSGFDBSuffixArrayFiles(string strManagerName, string sPICHPCUser, string sPICHPCPassword)
        {
            mMgrName = strManagerName;
            mPICHPCUser = sPICHPCUser;
            mPICHPCPassword = sPICHPCPassword;
        }

        private CloseOutType CopyExistingIndexFilesFromRemote(FileInfo fiFastaFile, bool blnUsingLegacyFasta, string strRemoteIndexFolderPath,
            bool blnCheckForLockFile, int intDebugLevel, float sngMaxWaitTimeHours, out bool diskFreeSpaceBelowThreshold)
        {
            var blnSuccess = false;

            diskFreeSpaceBelowThreshold = false;

            try
            {
                var diRemoteIndexFolderPath = new DirectoryInfo(strRemoteIndexFolderPath);

                if (!diRemoteIndexFolderPath.Exists)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (blnCheckForLockFile)
                {
                    // Look for an existing lock file
                    var fiRemoteLockFile1 = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

                    WaitForExistingLockfile(fiRemoteLockFile1, intDebugLevel, sngMaxWaitTimeHours);
                }

                // Look for the .MSGFPlusIndexFileInfo file for this fasta file
                var fiMSGFPlusIndexFileInfo = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                long fileSizeTotalKB = 0;

                if (!fiMSGFPlusIndexFileInfo.Exists)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Read the filenames in the file
                // There should be 3 columns: FileName, FileSize, and FileDateUTC
                // When looking for existing files we only require that the filesize match; FileDateUTC is not used

                var dctFilesToCopy = new Dictionary<string, long>();

                using (var srInFile = new StreamReader(new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    string strLineIn = null;

                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        var lstData = strLineIn.Split('\t').ToList();

                        if (lstData.Count >= 3)
                        {
                            // Add this file to the list of files to copy
                            long intFileSizeBytes = 0;
                            if (long.TryParse(lstData[1], out intFileSizeBytes))
                            {
                                dctFilesToCopy.Add(lstData[0], intFileSizeBytes);
                                fileSizeTotalKB += (long)(intFileSizeBytes / 1024.0);
                            }
                        }
                    }
                }

                bool blnFilesAreValid = false;

                if (dctFilesToCopy.Count == 0)
                {
                    blnFilesAreValid = false;
                }
                else
                {
                    // Confirm that each file in dctFilesToCopy exists on the remote server
                    // If using a legacy fasta file, must also confirm that each file is newer than the fasta file that was indexed
                    blnFilesAreValid = ValidateFiles(diRemoteIndexFolderPath.FullName, dctFilesToCopy, blnUsingLegacyFasta,
                        fiFastaFile.LastWriteTimeUtc, true);
                }

                if (!blnFilesAreValid)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (intDebugLevel >= 1 && fileSizeTotalKB >= 1000)
                {
                    OnStatusEvent("Copying existing MSGF+ index files from " + diRemoteIndexFolderPath.FullName);
                }

                // Copy each file in lstFilesToCopy (overwrite existing files)
                var strManager = GetPseudoManagerName();

                var filesCopied = 0;
                var dtLastStatusUpdate = System.DateTime.UtcNow;

                var oFileTools = new PRISM.Files.clsFileTools(strManager, intDebugLevel);

                // Compute the total disk space required
                long fileSizeTotalBytes = 0;

                foreach (KeyValuePair<string, long> entry in dctFilesToCopy)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key));
                    fileSizeTotalBytes += fiSourceFile.Length;
                }

                const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750;

                // Convert fileSizeTotalBytes to MB, but add on a Default_Min_free_Space to assure we'll still have enough free space after copying over the files
                var minFreeSpaceMB = (int)(fileSizeTotalBytes / 1024.0 / 1024.0 + DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                diskFreeSpaceBelowThreshold = !clsGlobal.ValidateFreeDiskSpace("Organism DB directory", fiFastaFile.Directory.FullName, minFreeSpaceMB, clsLogTools.LoggerTypes.LogFile, out mErrorMessage);

                if (diskFreeSpaceBelowThreshold)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                FileInfo fiRemoteLockFile2 = null;
                bool blnRemoteLockFileCreated = false;

                blnRemoteLockFileCreated = CreateRemoteSuffixArrayLockFile(fiFastaFile.Name, fiFastaFile.Directory.FullName, out fiRemoteLockFile2,
                    intDebugLevel, sngMaxWaitTimeHours);

                if (blnRemoteLockFileCreated)
                {
                    // Lock file successfully created
                    // If this manager ended up waiting while another manager was indexing the files or while another manager was copying files locally,
                    // then we should once again check to see if the required files exist

                    // Now confirm that each file was successfully copied locally
                    blnSuccess = ValidateFiles(fiFastaFile.Directory.FullName, dctFilesToCopy, blnUsingLegacyFasta, fiFastaFile.LastWriteTimeUtc, false);
                    if (blnSuccess)
                    {
                        // Files now exist
                        DeleteLockFile(fiRemoteLockFile2);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }

                foreach (KeyValuePair<string, long> entry in dctFilesToCopy)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key));

                    var strTargetFilePath = Path.Combine(fiFastaFile.Directory.FullName, fiSourceFile.Name);
                    oFileTools.CopyFileUsingLocks(fiSourceFile, strTargetFilePath, strManager, true);

                    filesCopied += 1;

                    if (intDebugLevel >= 1 && System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 30)
                    {
                        dtLastStatusUpdate = System.DateTime.UtcNow;
                        OnStatusEvent("Retrieved " + filesCopied + " / " + dctFilesToCopy.Count + " index files");
                    }
                }

                // Now confirm that each file was successfully copied locally
                blnSuccess = ValidateFiles(fiFastaFile.Directory.FullName, dctFilesToCopy, blnUsingLegacyFasta, fiFastaFile.LastWriteTimeUtc, false);

                DeleteLockFile(fiRemoteLockFile2);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in CopyExistingIndexFilesFromRemote", ex);
                blnSuccess = false;
            }

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
        }

        private bool CopyIndexFilesToRemote(FileInfo fiFastaFile, string strRemoteIndexFolderPath, int intDebugLevel)
        {
            var strErrorMessage = string.Empty;
            var strManager = GetPseudoManagerName();
            const bool createIndexFileForExistingFiles = false;

            var success = CopyIndexFilesToRemote(fiFastaFile, strRemoteIndexFolderPath, intDebugLevel, strManager, createIndexFileForExistingFiles, out strErrorMessage);
            if (!success)
            {
                OnErrorEvent(strErrorMessage);
            }

            return success;
        }

        /// <summary>
        /// Copies the suffix array files for the specified fasta file to the remote MSGFPlus_Index_File folder share
        /// </summary>
        /// <param name="fiFastaFile"></param>
        /// <param name="remoteIndexFolderPath"></param>
        /// <param name="debugLevel"></param>
        /// <param name="managerName">Manager name (only required because the constructor for PRISM.Files.clsFileTools requires this)</param>
        /// <param name="createIndexFileForExistingFiles">When true, assumes that the index files were previously copied to remoteIndexFolderPath, and we should simply create the .MSGFPlusIndexFileInfo file for the matching files</param>
        /// <param name="strErrorMessage"></param>
        /// <returns></returns>
        /// <remarks>This function is used both by this class and by the MSGFPlusIndexFileCopier console application</remarks>
        public static bool CopyIndexFilesToRemote(FileInfo fiFastaFile, string remoteIndexFolderPath, int debugLevel, string managerName,
            bool createIndexFileForExistingFiles, out string strErrorMessage)
        {
            var blnSuccess = false;
            strErrorMessage = string.Empty;

            try
            {
                var diRemoteIndexFolderPath = new DirectoryInfo(remoteIndexFolderPath);

                if (!diRemoteIndexFolderPath.Parent.Exists)
                {
                    strErrorMessage = "MSGF+ index files folder not found: " + diRemoteIndexFolderPath.Parent.FullName;
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

                var dctFilesToCopy = new Dictionary<string, long>();

                var lstFileInfo = new List<string>();

                // Find the index files for fiFastaFile
                foreach (FileInfo fiSourceFile in fiFastaFile.Directory.GetFiles(Path.GetFileNameWithoutExtension(fiFastaFile.Name) + ".*"))
                {
                    if (fiSourceFile.FullName != fiFastaFile.FullName)
                    {
                        if (fiSourceFile.Extension != ".hashcheck" && fiSourceFile.Extension != ".MSGFPlusIndexFileInfo")
                        {
                            dctFilesToCopy.Add(fiSourceFile.Name, fiSourceFile.Length);
                            lstFileInfo.Add(fiSourceFile.Name + "\t" + fiSourceFile.Length + "\t" + fiSourceFile.LastWriteTimeUtc.ToString(DATE_TIME_FORMAT));
                        }
                    }
                }

                if (createIndexFileForExistingFiles)
                {
                    blnSuccess = true;
                }
                else
                {
                    // Copy up each file
                    var oFileTools = new PRISM.Files.clsFileTools(managerName, debugLevel);

                    foreach (KeyValuePair<string, long> entry in dctFilesToCopy)
                    {
                        string strSourceFilePath = null;
                        string strTargetFilePath = null;

                        strSourceFilePath = Path.Combine(fiFastaFile.Directory.FullName, entry.Key);
                        strTargetFilePath = Path.Combine(diRemoteIndexFolderPath.FullName, entry.Key);

                        blnSuccess = oFileTools.CopyFileUsingLocks(strSourceFilePath, strTargetFilePath, managerName, true);
                        if (!blnSuccess)
                        {
                            strErrorMessage = "CopyFileUsingLocks returned false copying to " + strTargetFilePath;
                            break;
                        }
                    }
                }

                if (blnSuccess)
                {
                    // Create the .MSGFPlusIndexFileInfo file for this fasta file
                    var fiMSGFPlusIndexFileInfo = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, fiFastaFile.Name + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX));

                    using (var swOutFile = new StreamWriter(new FileStream(fiMSGFPlusIndexFileInfo.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var entry in lstFileInfo)
                        {
                            swOutFile.WriteLine(entry);
                        }
                    }

                    blnSuccess = true;
                }
            }
            catch (Exception ex)
            {
                strErrorMessage = "Exception in CopyIndexFilesToRemote; " + ex.Message;
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Convert .Fasta file to indexed DB files compatible with MSGFPlus
        /// Will copy the files from strMSGFPlusIndexFilesFolderPathBase if they exist
        /// </summary>
        /// <param name="strLogfileDir"></param>
        /// <param name="intDebugLevel"></param>
        /// <param name="JobNum"></param>
        /// <param name="javaProgLoc"></param>
        /// <param name="msgfDbProgLoc"></param>
        /// <param name="strFASTAFilePath">FASTA file path</param>
        /// <param name="blnFastaFileIsDecoy">When True, only creates the forward-based index files.  When False, creates both the forward and reverse index files</param>
        /// <param name="strMSGFPlusIndexFilesFolderPathBase">Folder path from which to copy (or store) the index files</param>
        /// <param name="strMSGFPlusIndexFilesFolderPathLegacyDB">Folder path from which to copy (or store) the index files for Legacy DBs (.fasta files not created from the protein sequences database)</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateSuffixArrayFiles(string strLogFileDir, int intDebugLevel, string JobNum, string javaProgLoc, string msgfDbProgLoc,
            string strFASTAFilePath, bool blnFastaFileIsDecoy, string strMSGFPlusIndexFilesFolderPathBase,
            string strMSGFPlusIndexFilesFolderPathLegacyDB, clsAnalysisResources.udtHPCOptionsType udtHPCOptions)
        {
            const float MAX_WAITTIME_HOURS = 1.0f;

            string strOutputNameBase = null;

            string dbSarrayFilename = null;

            float sngMaxWaitTimeHours = MAX_WAITTIME_HOURS;

            bool blnMSGFPlus = false;
            var strCurrentTask = "Initializing";
            CloseOutType eResult;

            try
            {
                mErrorMessage = string.Empty;

                if (intDebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsCreateMSGFDBSuffixArrayFiles.CreateIndexedDbFiles(): Enter");
                }

                var fiFastaFile = new FileInfo(strFASTAFilePath);

                blnMSGFPlus = IsMSGFPlus(msgfDbProgLoc);
                if (!blnMSGFPlus)
                {
                    // Running legacy MS-GFDB
                    throw new Exception("Legacy MS-GFDB is no longer supported");
                }

                // Protein collection files will start with ID_ then have at least 6 integers, then an alphanumeric hash string, for example ID_004208_295531A4.fasta
                // If the filename does not match that pattern, we're using a legacy fasta file
                var reProtectionCollectionFasta = new Regex(@"ID_\d{6,}_[0-9a-z]+\.fasta", RegexOptions.IgnoreCase);
                var blnUsingLegacyFasta = !reProtectionCollectionFasta.IsMatch(fiFastaFile.Name);

                //  Look for existing suffix array files
                strOutputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name);

                var fiLockFile = new FileInfo(Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase + "_csarr.lock"));
                dbSarrayFilename = Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase + ".csarr");

                if (udtHPCOptions.UsingHPC)
                {
                    // Increase the maximum wait time to 24 hours; useful in case another manager has created a BuildSA job, and that job is stuck in the queue
                    sngMaxWaitTimeHours = 24;
                }

                // Check to see if another Analysis Manager is already creating the indexed DB files
                strCurrentTask = "Looking for lock file " + fiLockFile.FullName;
                WaitForExistingLockfile(fiLockFile, intDebugLevel, sngMaxWaitTimeHours);

                // Validate that all of the expected files exist
                // If any are missing, then need to repeat the call to "BuildSA"
                var blnReindexingRequired = false;

                strCurrentTask = "Validating that expected files exist";
                if (blnMSGFPlus)
                {
                    // Check for any FastaFileName.revConcat.* files
                    // If they exist, delete them, since they are for legacy MSGFDB

                    FileInfo[] fiLegacyIndexedFiles = null;
                    fiLegacyIndexedFiles = fiFastaFile.Directory.GetFiles(strOutputNameBase + ".revConcat.*");

                    if (fiLegacyIndexedFiles.Length > 0)
                    {
                        blnReindexingRequired = true;

                        for (var intIndex = 0; intIndex <= fiLegacyIndexedFiles.Length - 1; intIndex++)
                        {
                            strCurrentTask = "Deleting indexed file created by legacy MSGFDB: " + fiLegacyIndexedFiles[intIndex].FullName;
                            if (intDebugLevel >= 1)
                            {
                                OnStatusEvent(strCurrentTask);
                            }
                            fiLegacyIndexedFiles[intIndex].Delete();
                        }
                    }
                    else
                    {
                        // Open the FastaFileName.canno file and read the first two lines
                        // If there is a number on the first line but the second line starts with the letter A, then this file was created with the legacy MSGFDB
                        var fiCAnnoFile = new FileInfo(Path.Combine(fiFastaFile.DirectoryName, strOutputNameBase + ".canno"));
                        if (fiCAnnoFile.Exists)
                        {
                            strCurrentTask = "Examining first two lines of " + fiCAnnoFile.FullName;
                            using (var srCannoFile = new StreamReader(new FileStream(fiCAnnoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                            {
                                if (!srCannoFile.EndOfStream)
                                {
                                    string strLine1 = null;
                                    string strLine2 = null;
                                    int intLine1Value = 0;

                                    strLine1 = srCannoFile.ReadLine();

                                    if (!srCannoFile.EndOfStream)
                                    {
                                        strLine2 = srCannoFile.ReadLine();

                                        if (int.TryParse(strLine1, out intLine1Value))
                                        {
                                            if (char.IsLetter(strLine2[0]))
                                            {
                                                strCurrentTask = "Legacy MSGFDB indexed file found (" + fiCAnnoFile.Name + "); re-indexing";
                                                if (intDebugLevel >= 1)
                                                {
                                                    OnStatusEvent(strCurrentTask);
                                                }
                                                blnReindexingRequired = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // This dictionary contains file suffixes to look for
                // Keys will be "True" if the file exists and false if it does not exist
                var lstFilesToFind = new List<string>();

                if (!blnReindexingRequired)
                {
                    var strExistingFiles = string.Empty;
                    var strMissingFiles = string.Empty;

                    strCurrentTask = "Validating that expected files exist";
                    var lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName,
                        lstFilesToFind, out strExistingFiles, out strMissingFiles);

                    if (lstExistingFiles.Count < lstFilesToFind.Count)
                    {
                        blnReindexingRequired = true;

                        strCurrentTask = "Some files are missing: " + lstExistingFiles.Count + " vs. " + lstFilesToFind.Count;
                        if (lstExistingFiles.Count > 0)
                        {
                            if (intDebugLevel >= 1)
                            {
                                OnWarningEvent("Indexing of " + fiFastaFile.Name + " was incomplete (found " + lstExistingFiles.Count + " out of " +
                                               lstFilesToFind.Count + " index files)");
                                OnStatusEvent(" ... existing files: " + strExistingFiles);
                                OnStatusEvent(" ... missing files: " + strMissingFiles);
                            }
                        }
                    }
                    else if (blnUsingLegacyFasta)
                    {
                        // Make sure all of the index files have a file modification date newer than the fasta file
                        // We only do this for legacy fasta files, since their file modification date will be the same on all pubs
                        // We can't do this for programatically generated fasta files (that use protein collections)
                        //   since their modification date will be the time that the file was created

                        blnReindexingRequired = false;

                        foreach (var fiIndexFile in lstExistingFiles)
                        {
                            if (fiIndexFile.LastWriteTimeUtc < fiFastaFile.LastWriteTimeUtc.AddSeconds(-0.1))
                            {
                                OnStatusEvent("Index file is older than the fasta file; " + fiIndexFile.FullName + " modified " +
                                              fiIndexFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT) + " vs. " +
                                              fiFastaFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT));

                                blnReindexingRequired = true;
                                break;
                            }
                        }
                    }
                }

                if (blnReindexingRequired)
                {
                    // Index files are missing or out of date
                    // Copy them from strMSGFPlusIndexFilesFolderPathBase or strMSGFPlusIndexFilesFolderPathLegacyDB if possible
                    // Otherwise, create new index files

                    string strRemoteIndexFolderPath = null;
                    strRemoteIndexFolderPath = DetermineRemoteMSGFPlusIndexFilesFolderPath(fiFastaFile.Name, strMSGFPlusIndexFilesFolderPathBase,
                        strMSGFPlusIndexFilesFolderPathLegacyDB);

                    bool blnCheckForLockFile = false;
                    var diskFreeSpaceBelowThreshold = false;

                    blnCheckForLockFile = true;
                    eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, blnUsingLegacyFasta, strRemoteIndexFolderPath, blnCheckForLockFile,
                        intDebugLevel, sngMaxWaitTimeHours, out diskFreeSpaceBelowThreshold);

                    if (diskFreeSpaceBelowThreshold)
                    {
                        // Not enough free disk space; abort
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Files did not exist or were out of date, or an error occurred while copying them

                        // Create a remote lock file

                        FileInfo fiRemoteLockFile = null;
                        bool blnRemoteLockFileCreated = false;

                        strCurrentTask = "Create the remote lock file";
                        blnRemoteLockFileCreated = CreateRemoteSuffixArrayLockFile(fiFastaFile.Name, strRemoteIndexFolderPath, out fiRemoteLockFile,
                            intDebugLevel, sngMaxWaitTimeHours);

                        if (blnRemoteLockFileCreated)
                        {
                            // Lock file successfully created
                            // If this manager ended up waiting while another manager was indexing the files, then we should once again try to copy the files locally

                            blnCheckForLockFile = false;
                            eResult = CopyExistingIndexFilesFromRemote(fiFastaFile, blnUsingLegacyFasta, strRemoteIndexFolderPath, blnCheckForLockFile,
                                intDebugLevel, sngMaxWaitTimeHours, out diskFreeSpaceBelowThreshold);

                            if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                // Existing files were copied; this manager does not need to re-create them
                                blnReindexingRequired = false;
                            }

                            if (diskFreeSpaceBelowThreshold)
                            {
                                // Not enough free disk space; abort
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }

                        if (blnReindexingRequired)
                        {
                            OnStatusEvent("Running BuildSA to index " + fiFastaFile.Name);

                            eResult = CreateSuffixArrayFilesWork(strLogFileDir, intDebugLevel, JobNum, fiFastaFile, fiLockFile, javaProgLoc,
                                msgfDbProgLoc, blnFastaFileIsDecoy, blnMSGFPlus, dbSarrayFilename,
                                udtHPCOptions);

                            if (blnRemoteLockFileCreated && eResult == CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                OnStatusEvent("Copying index files to " + strRemoteIndexFolderPath);
                                CopyIndexFilesToRemote(fiFastaFile, strRemoteIndexFolderPath, intDebugLevel);
                            }
                        }

                        if (blnRemoteLockFileCreated)
                        {
                            // Delete the remote lock file
                            DeleteLockFile(fiRemoteLockFile);
                        }
                    }
                }
                else
                {
                    eResult = CloseOutType.CLOSEOUT_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in .CreateIndexedDbFiles";
                OnErrorEvent(mErrorMessage + "; " + strCurrentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return eResult;
        }

        private CloseOutType CreateSuffixArrayFilesWork(string strLogFileDir, int intDebugLevel, string JobNum, FileInfo fiFastaFile,
            FileInfo fiLockFile, string JavaProgLoc, string msgfDbProgLoc, bool blnFastaFileIsDecoy, bool blnMSGFPlus, string dbSarrayFilename,
            clsAnalysisResources.udtHPCOptionsType udtHPCOptions)
        {
            var strCurrentTask = string.Empty;

            try
            {
                // Try to create the index files for fasta file strDBFileNameInput
                strCurrentTask = "Look for java.exe and .jar file";

                // Verify that Java exists
                if (!File.Exists(JavaProgLoc))
                {
                    mErrorMessage = "Cannot find Java program file";
                    OnErrorEvent(mErrorMessage + ": " + JavaProgLoc);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Verify that the MSGFDB.Jar or MSGFPlus.jar file exists
                if (!File.Exists(msgfDbProgLoc))
                {
                    mErrorMessage = "Cannot find " + Path.GetFileName(msgfDbProgLoc) + " file";
                    OnErrorEvent(mErrorMessage + ": " + msgfDbProgLoc);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Determine the amount of ram to reserve for BuildSA
                // Examine the size of the .Fasta file to determine how much ram to reserve
                int intJavaMemorySizeMB = 0;

                var intFastaFileSizeMB = Convert.ToInt32(fiFastaFile.Length / 1024.0 / 1024.0);

                if (intFastaFileSizeMB <= 125)
                {
                    intJavaMemorySizeMB = 4000;
                }
                else if (intFastaFileSizeMB <= 250)
                {
                    intJavaMemorySizeMB = 6000;
                }
                else if (intFastaFileSizeMB <= 375)
                {
                    intJavaMemorySizeMB = 8000;
                }
                else
                {
                    intJavaMemorySizeMB = 12000;
                }

                if (udtHPCOptions.UsingHPC)
                {
                    intJavaMemorySizeMB = udtHPCOptions.MinimumMemoryMB;
                }
                else
                {
                    strCurrentTask = "Verify free memory";

                    // Make sure the machine has enough free memory to run BuildSA
                    if (!clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySizeMB, "BuildSA", false))
                    {
                        mErrorMessage = "Cannot run BuildSA since less than " + intJavaMemorySizeMB + " MB of free memory";
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Create a lock file
                if (intDebugLevel >= 3)
                {
                    OnStatusEvent("Creating lock file: " + fiLockFile.FullName);
                }

                // Delay between 2 and 5 seconds
                var oRandom = new Random();
                Thread.Sleep(oRandom.Next(2, 5) * 1000);

                // Check one more time for a lock file
                // If it exists, another manager just created it and we should abort
                strCurrentTask = "Look for the lock file one last time";
                fiLockFile.Refresh();
                if (fiLockFile.Exists)
                {
                    if (intDebugLevel >= 1)
                    {
                        OnStatusEvent("Warning: new lock file found: " + fiLockFile.FullName + "; aborting");
                        return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                    }
                }

                // Create a lock file in the folder that the index files will be created
                bool success = false;
                strCurrentTask = "Create the local lock file: " + fiLockFile.FullName;
                success = CreateLockFile(fiLockFile.FullName);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete any existing index files (BuildSA throws an error if they exist)
                strCurrentTask = "Delete any existing files";

                var strOutputNameBase = Path.GetFileNameWithoutExtension(fiFastaFile.Name);

                string existingFiles;
                string missingfiles;
                var lstExistingFiles = FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, fiFastaFile.DirectoryName,
                    new List<string>(), out existingFiles, out missingfiles);

                foreach (var fiIndexFileToDelete in lstExistingFiles)
                {
                    if (fiIndexFileToDelete.Exists)
                    {
                        fiIndexFileToDelete.Delete();
                    }
                }

                if (intDebugLevel >= 2)
                {
                    OnStatusEvent("Creating Suffix Array database file: " + dbSarrayFilename);
                }

                //Set up and execute a program runner to invoke BuildSA (which is in MSGFDB.jar or MSGFPlus.jar)
                strCurrentTask = "Construct BuildSA command line";
                string CmdStr = null;
                CmdStr = " -Xmx" + intJavaMemorySizeMB.ToString() + "M -cp " + msgfDbProgLoc;

                if (blnMSGFPlus)
                {
                    CmdStr += " edu.ucsd.msjava.msdbsearch.BuildSA -d " + fiFastaFile.FullName;
                }
                else
                {
                    CmdStr += " msdbsearch.BuildSA -d " + fiFastaFile.FullName;
                }

                if (blnFastaFileIsDecoy)
                {
                    CmdStr += " -tda 0";
                }
                else
                {
                    CmdStr += " -tda 2";
                }

                if (intDebugLevel >= 1)
                {
                    OnStatusEvent(JavaProgLoc + " " + CmdStr);
                }

                var consoleOutputFilePath = string.Empty;

                if (udtHPCOptions.UsingHPC)
                {
#if EnableHPC
                    var jobName = "BuildSA_" + fiFastaFile.Name;
                    const string taskName = "BuildSA";

                    var buildSAJobInfo = new HPC_Connector.JobToHPC(udtHPCOptions.HeadNode, jobName, taskName);

                    buildSAJobInfo.JobParameters.PriorityLevel = HPC_Connector.PriorityLevel.Normal;
                    buildSAJobInfo.JobParameters.TemplateName = "DMS"; // If using 32 cores, could use Template "Single"
                    buildSAJobInfo.JobParameters.ProjectName = "DMS";

                    // April 2014 note: If using picfs.pnl.gov then we must reserve an entire node due to file system issues of the Windows Nodes talking to the Isilon file system
                    // Furthermore, we must set ".isExclusive" to True
                    // Note that each node has two sockets

                    //buildSAJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Socket
                    buildSAJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Node;
                    buildSAJobInfo.JobParameters.isExclusive = true;

                    // If requesting a socket or a node, there is no need to set the number of cores
                    // buildSAJobInfo.JobParameters.MinNumberOfCores = 0
                    // buildSAJobInfo.JobParameters.MaxNumberOfCores = 0

                    // Make a batch file that will run the java program, then issue a Ping command with a delay, which will allow the file system to release the file handles
                    var batchFilePath = clsAnalysisToolRunnerMSGFDB.MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_SuffixAray_Task.bat", JavaProgLoc + " " + CmdStr);

                    buildSAJobInfo.TaskParameters.CommandLine = batchFilePath;
                    buildSAJobInfo.TaskParameters.WorkDirectory = udtHPCOptions.WorkDirPath;
                    buildSAJobInfo.TaskParameters.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, "MSGFDB_BuildSA_ConsoleOutput.txt");
                    buildSAJobInfo.TaskParameters.TaskTypeOption = HPC_Connector.HPCTaskType.Basic;
                    buildSAJobInfo.TaskParameters.FailJobOnFailure = true;

                    if (string.IsNullOrEmpty(mPICHPCUser))
                    {
                        mComputeCluster = new HPC_Submit.WindowsHPC2012();
                    }
                    else
                    {
                        mComputeCluster = new HPC_Submit.WindowsHPC2012(mPICHPCUser, clsGlobal.DecodePassword(mPICHPCPassword));
                    }

                    mComputeCluster.ErrorEvent += mComputeCluster_ErrorEvent;
                    mComputeCluster.MessageEvent += mComputeCluster_MessageEvent;
                    mComputeCluster.ProgressEvent += mComputeCluster_ProgressEvent;

                    var jobID = mComputeCluster.Send(buildSAJobInfo);

                    if (jobID <= 0)
                    {
                        mErrorMessage = "BuildSA Job was not created in HPC: " + mComputeCluster.ErrorMessage;
                        DeleteLockFile(fiLockFile);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (mComputeCluster.Scheduler == null)
                    {
                        mErrorMessage = "Error: HPC Scheduler is null for BuildSA Job";
                        DeleteLockFile(fiLockFile);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    var buildSAJob = mComputeCluster.Scheduler.OpenJob(jobID);

                    success = mComputeCluster.MonitorJob(buildSAJob);
                    if (!success)
                    {
                        mErrorMessage = "HPC Job Monitor returned false: " + mComputeCluster.ErrorMessage;
                        OnErrorEvent(mErrorMessage);
                    }

                    try
                    {
                        File.Delete(batchFilePath);
                    }
                    catch (Exception ex)
                    {
                        // Ignore errors here
                    }
#else
                    throw new Exception("HPC Support is disabled in project AnalysisManagerMSGFDBPlugin");
#endif
                }
                else
                {
                    consoleOutputFilePath = Path.Combine(strLogFileDir, "MSGFDB_BuildSA_ConsoleOutput.txt");
                    var objBuildSA = new clsRunDosProgram(fiFastaFile.DirectoryName)
                    {
                        CreateNoWindow = true,
                        CacheStandardOutput = true,
                        EchoOutputToConsole = true,
                        WriteConsoleOutputToFile = true,
                        ConsoleOutputFilePath = consoleOutputFilePath
                    };
                    objBuildSA.ErrorEvent += CmdRunner_ErrorEvent;

                    strCurrentTask = "Run BuildSA using " + CmdStr;

                    // Run BuildSA and wait for it to exit
                    // This process generally doesn't take that long so we do not track CPU usage
                    success = objBuildSA.RunProgram(JavaProgLoc, CmdStr, "BuildSA", true);
                }

                if (!success)
                {
                    mErrorMessage = "Error running BuildSA with " + Path.GetFileName(msgfDbProgLoc) + " for " + fiFastaFile.Name;
                    if (udtHPCOptions.UsingHPC)
                    {
                        mErrorMessage += " using HPC";
                    }

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
                else
                {
                    if (intDebugLevel >= 1)
                    {
                        OnStatusEvent("Created suffix array files for " + fiFastaFile.Name);
                    }
                }

                if (intDebugLevel >= 3)
                {
                    OnStatusEvent("Deleting lock file: " + fiLockFile.FullName);
                }

                // Delete the lock file
                strCurrentTask = "Delete the lock file";
                DeleteLockFile(fiLockFile);
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in .CreateSuffixArrayFilesWork";
                OnErrorEvent(mErrorMessage + "; " + strCurrentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a lock file
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        private bool CreateLockFile(string strLockFilePath)
        {
            try
            {
                using (var swLockFile = new StreamWriter(strLockFilePath))
                {
                    swLockFile.WriteLine("Date: " + System.DateTime.Now.ToString());
                    swLockFile.WriteLine("Manager: " + mMgrName);
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error creating lock file";
                OnErrorEvent("clsCreateMSGFDBSuffixArrayFiles.CreateLockFile, " + mErrorMessage, ex);
                return false;
            }

            return true;
        }

        private bool CreateRemoteSuffixArrayLockFile(string strFastaFileName, string strRemoteIndexFolderPath, out FileInfo fiRemoteLockFile,
            int intDebugLevel, float sngMaxWaitTimeHours)
        {
            // ReSharper disable once RedundantAssignment
            var strCurrentTask = "Initializing";

            // ReSharper disable once RedundantAssignment
            strCurrentTask = "Looking for folder " + strRemoteIndexFolderPath;

            var diRemoteIndexFolderPath = new DirectoryInfo(strRemoteIndexFolderPath);

            if (!diRemoteIndexFolderPath.Parent.Exists)
            {
                OnErrorEvent("Cannot read/write MSGF+ index files from remote share; folder not found; " + diRemoteIndexFolderPath.FullName);
                fiRemoteLockFile = null;
                return false;
            }

            fiRemoteLockFile = new FileInfo(Path.Combine(diRemoteIndexFolderPath.FullName, strFastaFileName + MSGF_PLUS_INDEX_FILE_INFO_SUFFIX + ".lock"));

            strCurrentTask = "Looking for lock file " + fiRemoteLockFile.FullName;
            WaitForExistingLockfile(fiRemoteLockFile, intDebugLevel, sngMaxWaitTimeHours);

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
                OnErrorEvent("Exception creating remote MSGF+ suffix array lock file at " + diRemoteIndexFolderPath.FullName + "; " + strCurrentTask, ex);
                return false;
            }

            return true;
        }

        private void DeleteLockFile(FileInfo fiLockFile)
        {
            try
            {
                fiLockFile.Refresh();
                if (fiLockFile.Exists)
                {
                    fiLockFile.Delete();
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }
        }

        private string DetermineRemoteMSGFPlusIndexFilesFolderPath(string strFastaFileName, string strMSGFPlusIndexFilesFolderPathBase,
            string strMSGFPlusIndexFilesFolderPathLegacyDB)
        {
            var reExtractNum = new Regex(@"^ID_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var strRemoteIndexFolderPath = string.Empty;

            // DMS-generated fasta files will have a name of the form ID_003949_3D6802EE.fasta
            // Parse out the number (003949 in this case)
            var reMatch = reExtractNum.Match(strFastaFileName);
            if (reMatch.Success)
            {
                int intGeneratedFastaFileNumber = 0;

                if (int.TryParse(reMatch.Groups[1].Value, out intGeneratedFastaFileNumber))
                {
                    // Round down to the nearest 1000
                    // Thus, 003949 will round to 3000
                    var strFolderName = (Math.Floor(intGeneratedFastaFileNumber / 1000.0) * 1000).ToString("0");
                    strRemoteIndexFolderPath = Path.Combine(strMSGFPlusIndexFilesFolderPathBase, strFolderName);
                }
            }

            if (string.IsNullOrEmpty(strRemoteIndexFolderPath))
            {
                strRemoteIndexFolderPath = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other");
            }

            return strRemoteIndexFolderPath;
        }

        /// <summary>
        /// Constructs a list of suffix array files that should exist
        /// Looks for each of those files
        /// </summary>
        /// <param name="blnFastaFileIsDecoy"></param>
        /// <param name="blnMSGFPlus"></param>
        /// <param name="strOutputNameBase"></param>
        /// <param name="strFolderPathToSearch"></param>
        /// <param name="lstFilesToFind">List of files that should exist; calling function must have initialized it</param>
        /// <returns>A list of the files that currently exist</returns>
        /// <remarks></remarks>
        private List<FileInfo> FindExistingSuffixArrayFiles(bool blnFastaFileIsDecoy, bool blnMSGFPlus, string strOutputNameBase,
            string strFolderPathToSearch, List<string> lstFilesToFind)
        {
            var strExistingFiles = string.Empty;
            var strMissingFiles = string.Empty;

            return FindExistingSuffixArrayFiles(blnFastaFileIsDecoy, blnMSGFPlus, strOutputNameBase, strFolderPathToSearch, lstFilesToFind,
                out strExistingFiles, out strMissingFiles);
        }

        /// <summary>
        /// Constructs a list of suffix array files that should exist
        /// Looks for each of those files
        /// </summary>
        /// <param name="blnFastaFileIsDecoy"></param>
        /// <param name="blnMSGFPlus"></param>
        /// <param name="strOutputNameBase"></param>
        /// <param name="strFolderPathToSearch"></param>
        /// <param name="lstFilesToFind">List of files that should exist; calling function must have initialized it</param>
        /// <param name="strExistingFiles">Output param: semicolon separated list of existing files</param>
        /// <param name="strMissingFiles">Output param: semicolon separated list of missing files</param>
        /// <returns>A list of the files that currently exist</returns>
        /// <remarks></remarks>
        private List<FileInfo> FindExistingSuffixArrayFiles(bool blnFastaFileIsDecoy, bool blnMSGFPlus, string strOutputNameBase,
            string strFolderPathToSearch, List<string> lstFilesToFind, out string strExistingFiles, out string strMissingFiles)
        {
            var lstExistingFiles = new List<FileInfo>();

            lstFilesToFind.Clear();

            strExistingFiles = string.Empty;
            strMissingFiles = string.Empty;

            // Old suffixes (used prior to August 2011)
            //lstFilesToFind.Add(".revConcat.fasta")
            //lstFilesToFind.Add(".seq")
            //lstFilesToFind.Add(".seqanno")
            //lstFilesToFind.Add(".revConcat.seq")
            //lstFilesToFind.Add(".revConcat.seqanno")
            //lstFilesToFind.Add(".sarray")
            //lstFilesToFind.Add(".revConcat.sarray")

            // Suffixes for MSGFDB (effective 8/22/2011) and MSGF+
            lstFilesToFind.Add(".canno");
            lstFilesToFind.Add(".cnlcp");
            lstFilesToFind.Add(".csarr");
            lstFilesToFind.Add(".cseq");

            // Note: Suffixes for MSPathFinder
            // lstFilesToFind.Add(".icanno")
            // lstFilesToFind.Add(".icplcp")
            // lstFilesToFind.Add(".icseq")

            if (!blnFastaFileIsDecoy)
            {
                if (blnMSGFPlus)
                {
                    lstFilesToFind.Add(".revCat.canno");
                    lstFilesToFind.Add(".revCat.cnlcp");
                    lstFilesToFind.Add(".revCat.csarr");
                    lstFilesToFind.Add(".revCat.cseq");
                    lstFilesToFind.Add(".revCat.fasta");
                }
                else
                {
                    lstFilesToFind.Add(".revConcat.canno");
                    lstFilesToFind.Add(".revConcat.cnlcp");
                    lstFilesToFind.Add(".revConcat.csarr");
                    lstFilesToFind.Add(".revConcat.cseq");
                    lstFilesToFind.Add(".revConcat.fasta");
                }
            }

            foreach (var strSuffix in lstFilesToFind)
            {
                var strFileNameToFind = strOutputNameBase + strSuffix;

                var fiFileToFind = new FileInfo(Path.Combine(strFolderPathToSearch, strFileNameToFind));

                if (fiFileToFind.Exists)
                {
                    lstExistingFiles.Add(fiFileToFind);
                    strExistingFiles = clsGlobal.AppendToComment(strExistingFiles, strFileNameToFind);
                }
                else
                {
                    strMissingFiles = clsGlobal.AppendToComment(strMissingFiles, strFileNameToFind);
                }
            }

            return lstExistingFiles;
        }

        private string GetPseudoManagerName()
        {
            string strMgrName = null;
            strMgrName = mMgrName + "_CreateMSGFDBSuffixArrayFiles";

            return strMgrName;
        }

        public bool IsMSGFPlus(string MSGFDBJarFilePath)
        {
            const string MSGFDB_JAR_NAME = "MSGFDB.jar";

            var fiJarFile = new FileInfo(MSGFDBJarFilePath);

            if (string.Compare(fiJarFile.Name, MSGFDB_JAR_NAME, true) == 0)
            {
                // Not MSGF+
                return false;
            }
            else
            {
                // Using MSGF+
                return true;
            }
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
                        if (dataLine.StartsWith("Error"))
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
        /// Verifies that each of the files specified by dctFilesToCopy exists at strFolderPathToCheck and has the correct file size
        /// </summary>
        /// <param name="strFolderPathToCheck">folder to check</param>
        /// <param name="dctFilesToCopy">Dictionary with filenames and file sizes</param>
        /// <param name="blnUsingLegacyFasta"></param>
        /// <param name="dtMinWriteTimeThresholdUTC"></param>
        /// <param name="verifyingRemoteFolder">True when validating files on a remote server, false if verifying the local DMS_Temp_Org folder</param>
        /// <returns>True if all files are found and are the right size</returns>
        /// <remarks></remarks>
        private bool ValidateFiles(string strFolderPathToCheck, Dictionary<string, long> dctFilesToCopy, bool blnUsingLegacyFasta,
            DateTime dtMinWriteTimeThresholdUTC, bool verifyingRemoteFolder)
        {
            string sourceDescription = null;
            if (verifyingRemoteFolder)
            {
                sourceDescription = "Remote MSGF+ index file";
            }
            else
            {
                sourceDescription = "Local MSGF+ index file";
            }

            foreach (KeyValuePair<string, long> entry in dctFilesToCopy)
            {
                var fiSourceFile = new FileInfo(Path.Combine(strFolderPathToCheck, entry.Key));

                if (!fiSourceFile.Exists)
                {
                    // Remote MSGF+ index file not found
                    // Local MSGF+ index file not found
                    OnWarningEvent(sourceDescription + " not found: " + fiSourceFile.FullName);
                    return false;
                }
                else if (fiSourceFile.Length != entry.Value)
                {
                    // Remote MSGF+ index file is not the expected size
                    // Local MSGF+ index file is not the expected size
                    OnWarningEvent(sourceDescription + " is not the expected size: " + fiSourceFile.FullName + " should be " + entry.Value +
                                   " bytes but is actually " + fiSourceFile.Length + " bytes");
                    return false;
                }
                else if (blnUsingLegacyFasta)
                {
                    // Require that the index files be newer than the fasta file
                    if (fiSourceFile.LastWriteTimeUtc < dtMinWriteTimeThresholdUTC.AddSeconds(-0.1))
                    {
                        OnStatusEvent(sourceDescription + " is older than the fasta file; " + fiSourceFile.FullName + " modified " +
                                      fiSourceFile.LastWriteTimeUtc.ToLocalTime().ToString(DATE_TIME_FORMAT) + " vs. " +
                                      dtMinWriteTimeThresholdUTC.ToLocalTime().ToString(DATE_TIME_FORMAT));

                        return false;
                    }
                }
            }

            return true;
        }

        private void WaitForExistingLockfile(FileInfo fiLockFile, int intDebugLevel, float sngMaxWaitTimeHours)
        {
            // Check to see if another Analysis Manager is already creating the indexed DB files
            if (fiLockFile.Exists && System.DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes >= 60)
            {
                // Lock file is over 60 minutes old; delete it
                if (intDebugLevel >= 1)
                {
                    OnStatusEvent("Lock file is over 60 minutes old (created " + fiLockFile.LastWriteTime.ToString() + "); deleting " + fiLockFile.FullName);
                }
                DeleteLockFile(fiLockFile);
            }
            else if (fiLockFile.Exists)
            {
                if (intDebugLevel >= 1)
                {
                    OnStatusEvent("Lock file found: " + fiLockFile.FullName +
                                  "; waiting for file to be removed by other manager generating suffix array files");
                }

                // Lock file found; wait up to sngMaxWaitTimeHours hours
                var blnStaleFile = false;
                while (fiLockFile.Exists)
                {
                    // Sleep for 2 seconds
                    Thread.Sleep(2000);

                    if (System.DateTime.UtcNow.Subtract(fiLockFile.CreationTimeUtc).TotalHours >= sngMaxWaitTimeHours)
                    {
                        blnStaleFile = true;
                        break;
                    }
                    else
                    {
                        fiLockFile.Refresh();
                    }
                }

                // If the duration time has exceeded sngMaxWaitTimeHours, delete the lock file and try again with this manager
                if (blnStaleFile)
                {
                    string strLogMessage = null;
                    strLogMessage = "Waited over " + sngMaxWaitTimeHours.ToString("0.0") +
                                    " hour(s) for lock file to be deleted, but it is still present; " + "deleting the file now and continuing: " +
                                    fiLockFile.FullName;
                    OnWarningEvent(strLogMessage);
                    DeleteLockFile(fiLockFile);
                }
            }
        }

        #region "Event Methods"

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            OnErrorEvent(strMessage, ex);
        }

#if EnableHPC
        private void mComputeCluster_ErrorEvent(object sender, HPC_Submit.MessageEventArgs e)
        {
            OnErrorEvent(e.Message);
        }

        private void mComputeCluster_MessageEvent(object sender, HPC_Submit.MessageEventArgs e)
        {
            OnStatusEvent(e.Message);
        }

        private DateTime dtLastStatusUpdate = DateTime.MaxValue;

        private void mComputeCluster_ProgressEvent(object sender, HPC_Submit.ProgressEventArgs e)
        {
            if (System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 60)
            {
                dtLastStatusUpdate = System.DateTime.UtcNow;
                OnStatusEvent("Running BuildSA with HPC, " + (e.HoursElapsed * 60).ToString("0.00") + " minutes elapsed");
            }
        }
#endif

        #endregion
    }
}
