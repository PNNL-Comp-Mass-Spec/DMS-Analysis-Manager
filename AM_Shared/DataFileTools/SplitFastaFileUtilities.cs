using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using FastaFileSplitterDLL;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// FASTA file splitting utilities
    /// </summary>
    /// <remarks>These are not used by MSFragger or FragPipe, since those tools natively support FASTA file splitting</remarks>
    public class SplitFastaFileUtilities : EventNotifier
    {
        // Ignore Spelling: admins, dms, FASTA, Lockfile, Seqs, Utils

        /// <summary>
        /// LockFile name
        /// </summary>
        public const string LOCK_FILE_PROGRESS_TEXT = "Lockfile";

        private const string SP_NAME_UPDATE_ORGANISM_DB_FILE = "add_update_organism_db_file";

        private const string SP_NAME_REFRESH_CACHED_ORG_DB_INFO = "refresh_cached_organism_db_info";

        /// <summary>
        /// FASTA File Utilities
        /// </summary>
        private readonly FastaFileUtilities mFastaUtils;

        /// <summary>
        /// File copy utilities
        /// </summary>
        private readonly FileCopyUtilities mFileCopyUtilities;

        private readonly int mNumSplitParts;

        private clsFastaFileSplitter mSplitter;

        /// <summary>
        /// Most recent error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// MS-GF+ index files folder path
        /// </summary>
        public string MSGFPlusIndexFilesFolderPathLegacyDB { get; set; }

        /// <summary>
        /// True if waiting for a lock file
        /// </summary>
        public bool WaitingForLockFile { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Call method DefineConnectionStrings() after instantiating this class and connecting the event handlers</remarks>
        /// <param name="numSplitParts">Number of parts to split the FASTA file info</param>
        /// <param name="fileCopyUtils">File copy utilities</param>
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        public SplitFastaFileUtilities(
            int numSplitParts,
            FileCopyUtilities fileCopyUtils,
            IMgrParams mgrParams,
            IJobParams jobParams
            )
        {
            mNumSplitParts = numSplitParts;

            MSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";

            ErrorMessage = string.Empty;
            WaitingForLockFile = false;

            mFastaUtils = new FastaFileUtilities(mgrParams, jobParams);
            RegisterEvents(mFastaUtils);

            mFileCopyUtilities = fileCopyUtils;
        }

        private static int BoolToTinyInt(bool value)
        {
            return value ? 1 : 0;
        }

        /// <summary>
        /// Creates a new lock file to allow the calling process to either create the split FASTA file or validate that the split FASTA file exists
        /// </summary>
        /// <param name="baseFastaFile">Base FASTA file</param>
        /// <param name="lockFilePath">Output: path to the newly created lock file</param>
        /// <returns>Lock file handle</returns>
        private StreamWriter CreateLockStream(FileSystemInfo baseFastaFile, out string lockFilePath)
        {
            var startTime = DateTime.UtcNow;
            var attemptCount = 0;

            StreamWriter lockStream;

            lockFilePath = Path.Combine(baseFastaFile.FullName + Global.LOCK_FILE_EXTENSION);
            var lockFi = new FileInfo(lockFilePath);

            while (true)
            {
                attemptCount++;
                var creatingLockFile = false;

                try
                {
                    lockFi.Refresh();

                    if (lockFi.Exists)
                    {
                        WaitingForLockFile = true;

                        var lockTimeoutTime = lockFi.LastWriteTimeUtc.AddMinutes(60);
                        OnStatusEvent(LOCK_FILE_PROGRESS_TEXT + " found; waiting until it is deleted or until " +
                                      lockTimeoutTime.ToLocalTime() + ": " + lockFi.Name);

                        while (lockFi.Exists && DateTime.UtcNow < lockTimeoutTime)
                        {
                            Global.IdleLoop(5);
                            lockFi.Refresh();

                            if (DateTime.UtcNow.Subtract(startTime).TotalMinutes >= 60)
                            {
                                break;
                            }
                        }

                        lockFi.Refresh();

                        if (lockFi.Exists)
                        {
                            OnStatusEvent(LOCK_FILE_PROGRESS_TEXT + " still exists; assuming another process timed out; thus, now deleting file " + lockFi.Name);
                            lockFi.Delete();
                        }

                        WaitingForLockFile = false;
                    }

                    // Try to create a lock file so that the calling procedure can create the required FASTA file (or validate that it now exists)
                    creatingLockFile = true;

                    // Try to create the lock file
                    // If another process is still using it, an exception will be thrown
                    lockStream = new StreamWriter(new FileStream(lockFi.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite));

                    // We have successfully created a lock file,
                    // so we should exit the Do Loop
                    break;
                }
                catch (Exception ex)
                {
                    if (creatingLockFile)
                    {
                        if (ex.Message.Contains("being used by another process"))
                        {
                            OnStatusEvent("Another process has already created a " + LOCK_FILE_PROGRESS_TEXT + " at " + lockFi.FullName + "; will try again to monitor or create a new one");
                        }
                        else
                        {
                            OnWarningEvent("Exception while creating a new " + LOCK_FILE_PROGRESS_TEXT + " at " + lockFi.FullName + ": " + ex.Message);
                        }
                    }
                    else
                    {
                        OnWarningEvent("Exception while monitoring " + LOCK_FILE_PROGRESS_TEXT + " " + lockFi.FullName + ": " + ex.Message);
                    }
                }

                // Something went wrong; wait for 15 seconds then try again
                Global.IdleLoop(15);

                if (attemptCount >= 4)
                {
                    // Something went wrong 4 times in a row (typically either creating or deleting the .Lock file)
                    // Abort

                    // Exception: Unable to create Lockfile required to split FASTA file ...
                    throw new Exception("Unable to create " + LOCK_FILE_PROGRESS_TEXT + " required to split FASTA file " + baseFastaFile.FullName + "; tried 4 times without success");
                }
            }

            return lockStream;
        }

        /// <summary>
        /// Define the connection strings using manager parameters
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool DefineConnectionStrings()
        {
            return mFastaUtils.DefineConnectionStrings();
        }

        private void DeleteLockStream(string lockFilePath, TextWriter lockStream)
        {
            try
            {
                lockStream?.Close();

                var retryCount = 3;

                while (retryCount > 0)
                {
                    try
                    {
                        var lockFi = new FileInfo(lockFilePath);

                        if (lockFi.Exists)
                        {
                            lockFi.Delete();
                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception deleting lock file in DeleteLockStream: " + ex.Message);
                        retryCount--;
                        var random = new Random();
                        Global.IdleLoop(0.25 + random.NextDouble());
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in DeleteLockStream: " + ex.Message);
            }
        }

        /// <summary>
        /// Add each FASTA file in splitFastaFiles to table t_organism_db_file in the database
        /// </summary>
        /// <param name="organismName">Organism name</param>
        /// <param name="splitFastaFiles">List of FASTA files</param>
        /// <param name="isDecoyFASTA">True if the FASTA files have forward and reverse protein sequences</param>
        /// <returns>True if successful, false if an error</returns>
        private bool StoreSplitFastaFileNames(string organismName, IEnumerable<clsFastaFileSplitter.FastaFileInfoType> splitFastaFiles, bool isDecoyFASTA)
        {
            var splitFastaName = "??";

            if (string.IsNullOrWhiteSpace(mFastaUtils.DMSConnectionString))
            {
                if (Global.OfflineMode)
                {
                    // This procedure should not be called when running offline since the FASTA file
                    // should have already been split prior to the remote task starting
                    OnWarningEvent("Skipping call to " + SP_NAME_UPDATE_ORGANISM_DB_FILE + " since offline");
                    return true;
                }

                OnErrorEvent("Cannot call " + SP_NAME_UPDATE_ORGANISM_DB_FILE + " since the DMS Connection string is empty");
                return false;
            }

            try
            {
                foreach (var currentSplitFasta in splitFastaFiles)
                {
                    // Add/update each split file

                    var splitFastaFileInfo = new FileInfo(currentSplitFasta.FilePath);
                    splitFastaName = splitFastaFileInfo.Name;

                    var dbTools = DbToolsFactory.GetDBTools(mFastaUtils.DMSConnectionString, debugMode: mFastaUtils.MgrParams.TraceMode);
                    RegisterEvents(dbTools);

                    // Setup for execution of the procedure
                    var cmd = dbTools.CreateCommand(SP_NAME_UPDATE_ORGANISM_DB_FILE, CommandType.StoredProcedure);

                    dbTools.AddParameter(cmd, "@fastaFileName", SqlType.VarChar, 128, splitFastaName);
                    dbTools.AddParameter(cmd, "@organismName", SqlType.VarChar, 128, organismName);
                    dbTools.AddTypedParameter(cmd, "@numProteins", SqlType.Int, value: currentSplitFasta.NumProteins);
                    dbTools.AddTypedParameter(cmd, "@numResidues", SqlType.BigInt, value: currentSplitFasta.NumResidues);
                    dbTools.AddTypedParameter(cmd, "@fileSizeKB", SqlType.Int, value: (int)Math.Round(splitFastaFileInfo.Length / 1024.0));

                    if (dbTools.DbServerType == DbServerTypes.MSSQLServer)
                    {
                        dbTools.AddTypedParameter(cmd, "@isDecoy", SqlType.TinyInt, value: BoolToTinyInt(isDecoyFASTA));
                    }
                    else
                    {
                        dbTools.AddTypedParameter(cmd, "@isDecoy", SqlType.Boolean, value: isDecoyFASTA);
                    }

                    var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);
                    var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

                    const int retryCount = 3;
                    var resCode = dbTools.ExecuteSP(cmd, retryCount, 2);

                    var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                    if (resCode == 0 && returnCode == 0)
                        continue;

                    ErrorMessage = resCode != 0 && returnCode == 0
                        ? string.Format("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_UPDATE_ORGANISM_DB_FILE)
                        : string.Format("{0} reported return code {1}", SP_NAME_UPDATE_ORGANISM_DB_FILE, returnCodeParam.Value.CastDBVal<string>());

                    var message = messageParam.Value.CastDBVal<string>();

                    if (!string.IsNullOrWhiteSpace(message))
                    {
                        ErrorMessage = ErrorMessage + "; message: " + message;
                    }

                    OnErrorEvent(ErrorMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in StoreSplitFastaFileNames for " + splitFastaName + ": " + ex.Message;
                OnErrorEvent(ErrorMessage);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Call refresh_cached_organism_db_info in the Protein Sequences database
        /// to update T_DMS_Organism_DB_Info in MT_Main on the ProteinSeqs server
        /// using data from V_DMS_Organism_DB_File_Import
        /// (which pulls from V_Organism_DB_File_Export in DMS5)
        /// </summary>
        /// <remarks>This procedure exits if the ProteinSeqs DB connection string does not point to ProteinSeqs or CBDMS</remarks>
        private void UpdateCachedOrganismDBInfo()
        {
            if (string.IsNullOrWhiteSpace(mFastaUtils.ProteinSeqsDBConnectionString))
            {
                if (Global.OfflineMode)
                {
                    // This procedure should not be called when running offline since the FASTA file
                    // should have already been split prior to the remote task starting
                    OnWarningEvent("Skipping call to " + SP_NAME_REFRESH_CACHED_ORG_DB_INFO + " since offline");
                    return;
                }

                OnErrorEvent("Cannot call " + SP_NAME_REFRESH_CACHED_ORG_DB_INFO + " since the ProteinSeqs Connection string is empty");
                return;
            }

            try
            {
                // Only call procedure refresh_cached_organism_db_info if the connection string points to ProteinSeqs or CBDMS
                if (mFastaUtils.ProteinSeqsDBConnectionString.IndexOf("Data Source=proteinseqs", StringComparison.OrdinalIgnoreCase) < 0 &&
                    mFastaUtils.ProteinSeqsDBConnectionString.IndexOf("Data Source=cbdms", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    // Most likely the connection string is "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms",
                    // which is PostgreSQL-based and does not have procedure refresh_cached_organism_db_info
                    return;
                }

                var dbTools = DbToolsFactory.GetDBTools(mFastaUtils.ProteinSeqsDBConnectionString, debugMode: mFastaUtils.MgrParams.TraceMode);
                RegisterEvents(dbTools);

                // Setup for execution of the procedure
                var cmd = dbTools.CreateCommand(SP_NAME_REFRESH_CACHED_ORG_DB_INFO, CommandType.StoredProcedure);

                var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

                const int retryCount = 3;
                var resCode = dbTools.ExecuteSP(cmd, retryCount, 2);

                var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                if (resCode != 0 && returnCode == 0)
                {
                    OnErrorEvent("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_REFRESH_CACHED_ORG_DB_INFO);
                }

                if (returnCode != 0)
                {
                    OnErrorEvent("Error calling {0}, return code {1}",
                        SP_NAME_REFRESH_CACHED_ORG_DB_INFO, returnCodeParam.Value.CastDBVal<string>());
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in UpdateCachedOrganismDBInfo: " + ex.Message;
                OnErrorEvent(ErrorMessage);
            }
        }

        /// <summary>
        /// Validate that the split FASTA file exists
        /// </summary>
        /// <remarks>If the split file is not found, will automatically split the original file and update DMS with the split file information</remarks>
        /// <param name="baseFastaName">Original (non-split) filename, e.g. RefSoil_2013-11-07.fasta</param>
        /// <param name="splitFastaName">Split FASTA filename, e.g. RefSoil_2013-11-07_10x_05.fasta</param>
        /// <returns>True if the split FASTA file is defined in DMS</returns>
        public bool ValidateSplitFastaFile(string baseFastaName, string splitFastaName)
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "GetLegacyFastaFilePath for splitFastaName";
                var knownSplitFastaFilePath = mFastaUtils.GetLegacyFastaFilePath(splitFastaName, out _);
                var reSplitFiles = false;

                if (!string.IsNullOrWhiteSpace(knownSplitFastaFilePath))
                {
                    // Split file is defined in the database
                    // Make sure it exists on disk (admins occasionally delete split FASTA files to reclaim disk space)
                    try
                    {
                        if (!mFileCopyUtilities.FileExistsWithRetry(knownSplitFastaFilePath, BaseLogger.LogLevels.DEBUG))
                        {
                            // If the directory exists but no split FASTA files exist, assume that we need to re-split the FASTA file
                            var existingSplitFastaFile = new FileInfo(knownSplitFastaFilePath);

                            if (existingSplitFastaFile.Directory?.Exists != true)
                            {
                                ErrorMessage = "Cannot find directory with the base FASTA file: " + knownSplitFastaFilePath;
                                OnErrorEvent(ErrorMessage);
                                return false;
                            }

                            // Extract out the base split FASTA name
                            // For example, extract out "OrgDB_2018-07-14_25x" from "OrgDB_2018-07-14_25x_01.fasta"
                            var reBaseName = new System.Text.RegularExpressions.Regex(@"(?<BaseName>.+\d+x)_\d+");
                            var reMatch = reBaseName.Match(Path.GetFileNameWithoutExtension(knownSplitFastaFilePath));

                            if (!reMatch.Success)
                            {
                                ErrorMessage = "Cannot determine the base split FASTA file name from: " + knownSplitFastaFilePath;
                                OnErrorEvent(ErrorMessage);
                                return false;
                            }

                            // Look for files matching the base name

                            var splitFastaMatchSpec = reMatch.Groups["BaseName"].Value + "*.fasta";

                            var existingSplitFastaFiles = existingSplitFastaFile.Directory.GetFiles(splitFastaMatchSpec);
                            long totalSize = 0;

                            foreach (var splitFastaFile in existingSplitFastaFiles)
                            {
                                totalSize += splitFastaFile.Length;
                            }

                            if (existingSplitFastaFiles.Length == 0 || totalSize == 0)
                            {
                                OnWarningEvent("Split FASTA files not found; will re-generate them to obtain " + knownSplitFastaFilePath);
                                reSplitFiles = true;
                            }
                            else
                            {
                                ErrorMessage = "One or more split FASTA files exist, but the required one is missing: " + knownSplitFastaFilePath;
                                OnErrorEvent(ErrorMessage);
                                return false;
                            }
                        }
                    }
                    catch (Exception ex2)
                    {
                        OnErrorEvent("Exception while checking for file " + knownSplitFastaFilePath, ex2);
                    }

                    if (!reSplitFiles)
                    {
                        ErrorMessage = string.Empty;
                        return true;
                    }
                }

                // Split file not found
                // Query DMS for the location of baseFastaName
                currentTask = "GetLegacyFastaFilePath for baseFastaName";
                var baseFastaFilePath = mFastaUtils.GetLegacyFastaFilePath(baseFastaName, out var organismNameBaseFasta);

                if (string.IsNullOrWhiteSpace(baseFastaFilePath))
                {
                    // Base file not found
                    ErrorMessage = "Cannot find base FASTA file in DMS using V_Legacy_Static_File_Locations: " + baseFastaFilePath + "; ConnectionString: " + mFastaUtils.ProteinSeqsDBConnectionString;
                    OnErrorEvent(ErrorMessage);
                    return false;
                }

                var baseFastaFile = new FileInfo(baseFastaFilePath);

                if (!baseFastaFile.Exists)
                {
                    ErrorMessage = "Cannot split FASTA file; file not found: " + baseFastaFilePath;
                    OnErrorEvent(ErrorMessage);
                    return false;
                }

                // Try to create a lock file
                currentTask = "CreateLockStream";
                var lockStream = CreateLockStream(baseFastaFile, out var lockFilePath);

                if (lockStream == null)
                {
                    // Unable to create a lock stream; an exception has likely already been thrown
                    throw new Exception("Unable to create lock file required to split " + baseFastaFile.FullName);
                }

                lockStream.WriteLine("ValidateSplitFastaFile, started at " + DateTime.Now + " by " + mFastaUtils.MgrParams.ManagerName);

                // Check again for the existence of the desired FASTA file
                // It's possible another process created the FASTA file while this process was waiting for the other process's lock file to disappear

                currentTask = "GetLegacyFastaFilePath for splitFastaName (2nd time)";
                var fastaFilePath = mFastaUtils.GetLegacyFastaFilePath(splitFastaName, out _);

                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // The file now exists
                    ErrorMessage = string.Empty;
                    currentTask = "DeleteLockStream (FASTA file now exists)";
                    DeleteLockStream(lockFilePath, lockStream);
                    return true;
                }

                OnSplittingBaseFastaFile(baseFastaFile.FullName, mNumSplitParts);

                // Perform the splitting
                //    Call SplitFastaFile to create a split file, using mNumSplitParts

                mSplitter = new clsFastaFileSplitter
                {
                    LogMessagesToFile = false
                };

                mSplitter.ErrorEvent += Splitter_ErrorEvent;
                mSplitter.WarningEvent += Splitter_WarningEvent;
                mSplitter.ProgressUpdate += Splitter_ProgressChanged;

                currentTask = "SplitFastaFile " + baseFastaFile.FullName;
                var success = mSplitter.SplitFastaFile(baseFastaFile.FullName, baseFastaFile.DirectoryName, mNumSplitParts);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(ErrorMessage))
                    {
                        ErrorMessage = "FastaFileSplitter returned false; unknown error";
                        OnErrorEvent(ErrorMessage);
                    }
                    DeleteLockStream(lockFilePath, lockStream);
                    return false;
                }

                // Verify that the FASTA files were created
                currentTask = "Verify new files";

                foreach (var currentSplitFile in mSplitter.SplitFastaFileInfo)
                {
                    var splitFastaFileInfo = new FileInfo(currentSplitFile.FilePath);

                    if (!splitFastaFileInfo.Exists)
                    {
                        ErrorMessage = "Newly created split FASTA file not found: " + currentSplitFile.FilePath;
                        OnErrorEvent(ErrorMessage);
                        DeleteLockStream(lockFilePath, lockStream);
                        return false;
                    }
                }

                OnStatusEvent("FASTA file successfully split into " + mNumSplitParts + " parts");

                OnStatusEvent("Determining if the base FASTA file has decoy proteins");

                var decoyStatusSuccess = mFastaUtils.IsDecoyFastaFile(fastaFilePath, out var isDecoyFASTA);

                if (!decoyStatusSuccess)
                {
                    if (string.IsNullOrWhiteSpace(ErrorMessage))
                    {
                        ErrorMessage = "IsDecoyFastaFile returned false for " + fastaFilePath;
                    }

                    return false;
                }

                // Store the newly created FASTA file names, plus their protein and residue stats, in DMS
                currentTask = "StoreSplitFastaFileNames";
                success = StoreSplitFastaFileNames(organismNameBaseFasta, mSplitter.SplitFastaFileInfo, isDecoyFASTA);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(ErrorMessage))
                    {
                        ErrorMessage = "StoreSplitFastaFileNames returned false; unknown error";
                        OnErrorEvent(ErrorMessage);
                    }

                    DeleteLockStream(lockFilePath, lockStream);
                    return false;
                }

                // Call the procedure that syncs up this information with ProteinSeqs (only applicable if using SQL Server on ProteinSeqs or CBDMS)
                currentTask = "UpdateCachedOrganismDBInfo";
                UpdateCachedOrganismDBInfo();

                // Delete any cached MSGFPlus index files corresponding to the split FASTA files

                if (!string.IsNullOrWhiteSpace(MSGFPlusIndexFilesFolderPathLegacyDB))
                {
                    var indexFileDirectory = new DirectoryInfo(MSGFPlusIndexFilesFolderPathLegacyDB);

                    if (indexFileDirectory.Exists)
                    {
                        foreach (var currentSplitFile in mSplitter.SplitFastaFileInfo)
                        {
                            var fileSpecBase = Path.GetFileNameWithoutExtension(currentSplitFile.FilePath);

                            var fileSpecsToFind = new List<string>
                            {
                                fileSpecBase + ".*",
                                fileSpecBase + ".fasta.LastUsed",
                                fileSpecBase + ".fasta.MSGFPlusIndexFileInfo",
                                fileSpecBase + ".fasta.MSGFPlusIndexFileInfo.Lock"
                            };

                            foreach (var fileSpec in fileSpecsToFind)
                            {
                                foreach (var fileToDelete in indexFileDirectory.GetFiles(fileSpec))
                                {
                                    try
                                    {
                                        fileToDelete.Delete();
                                    }
                                    catch (Exception)
                                    {
                                        // Ignore errors here
                                    }
                                }
                            }
                        }
                    }
                }

                // Delete the lock file
                currentTask = "DeleteLockStream (FASTA file created)";
                DeleteLockStream(lockFilePath, lockStream);
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in ValidateSplitFastaFile for " + splitFastaName + " at " + currentTask + ": " + ex.Message;
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Event raised when splitting starts
        /// </summary>
        public event SplittingBaseFastaFileEventHandler SplittingBaseFastaFile;

        /// <summary>
        /// Delegate for SplittingBaseFastaFile
        /// </summary>
        /// <param name="baseFastaFileName">Base FASTA file name</param>
        /// <param name="numSplitParts">Number of parts to split the FASTA file into</param>
        public delegate void SplittingBaseFastaFileEventHandler(string baseFastaFileName, int numSplitParts);

        /// <summary>
        /// Raise event SplittingBaseFastaFile
        /// </summary>
        private void OnSplittingBaseFastaFile(string baseFastaFileName, int numSplitParts)
        {
            SplittingBaseFastaFile?.Invoke(baseFastaFileName, numSplitParts);
        }

        private void Splitter_ErrorEvent(string message, Exception ex)
        {
            ErrorMessage = "FASTA Splitter Error: " + message;
            OnErrorEvent(message, ex);
        }

        private void Splitter_WarningEvent(string message)
        {
            OnWarningEvent(message);
        }

        private void Splitter_ProgressChanged(string taskDescription, float percentComplete)
        {
            OnProgressUpdate(taskDescription, (int)percentComplete);
        }
    }
}
