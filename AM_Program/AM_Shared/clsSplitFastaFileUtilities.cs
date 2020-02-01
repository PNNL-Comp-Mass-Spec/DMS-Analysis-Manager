
using FastaFileSplitterDLL;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace AnalysisManagerBase
{
    /// <summary>
    /// FASTA file utilities
    /// </summary>
    public class clsSplitFastaFileUtilities : EventNotifier
    {

        /// <summary>
        /// LockFile name
        /// </summary>
        public const string LOCK_FILE_PROGRESS_TEXT = "Lockfile";

        private const string SP_NAME_UPDATE_ORGANISM_DB_FILE = "AddUpdateOrganismDBFile";

        private const string SP_NAME_REFRESH_CACHED_ORG_DB_INFO = "RefreshCachedOrganismDBInfo";

        /// <summary>
        /// DMS5 database connection string
        /// </summary>
        /// <remarks>Gigasax.DMS5</remarks>
        private readonly string mDMSConnectionString;

        /// <summary>
        /// File copy utilities
        /// </summary>
        private readonly clsFileCopyUtilities mFileCopyUtilities;

        /// <summary>
        /// Protein Sequences DB connection string
        /// </summary>
        /// <remarks>Proteinseqs.Protein_Sequences</remarks>
        private readonly string mProteinSeqsDBConnectionString;

        private readonly int mNumSplitParts;

        private readonly string mManagerName;

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
        /// <param name="dmsConnectionString"></param>
        /// <param name="proteinSeqsDBConnectionString"></param>
        /// <param name="numSplitParts"></param>
        /// <param name="managerName"></param>
        /// <param name="fileCopyUtils"></param>
        /// <remarks></remarks>
        public clsSplitFastaFileUtilities(
            string dmsConnectionString,
            string proteinSeqsDBConnectionString,
            int numSplitParts,
            string managerName,
            clsFileCopyUtilities fileCopyUtils)
        {
            mDMSConnectionString = dmsConnectionString;
            mProteinSeqsDBConnectionString = proteinSeqsDBConnectionString;
            mNumSplitParts = numSplitParts;
            mManagerName = managerName;

            MSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";

            ErrorMessage = string.Empty;
            WaitingForLockFile = false;

            mFileCopyUtilities = fileCopyUtils;
        }

        /// <summary>
        /// Creates a new lock file to allow the calling process to either create the split fasta file or validate that the split fasta file exists
        /// </summary>
        /// <param name="baseFastaFile"></param>
        /// <param name="lockFilePath">Output parameter: path to the newly created lock file</param>
        /// <returns>Lock file handle</returns>
        /// <remarks></remarks>
        private StreamWriter CreateLockStream(FileSystemInfo baseFastaFile, out string lockFilePath)
        {

            var startTime = DateTime.UtcNow;
            var attemptCount = 0;

            StreamWriter lockStream;

            lockFilePath = Path.Combine(baseFastaFile.FullName + clsGlobal.LOCK_FILE_EXTENSION);
            var lockFi = new FileInfo(lockFilePath);

            do
            {
                attemptCount += 1;
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
                            clsGlobal.IdleLoop(5);
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

                    // Try to create a lock file so that the calling procedure can create the required .Fasta file (or validate that it now exists)
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
                clsGlobal.IdleLoop(15);

                if (attemptCount >= 4)
                {
                    // Something went wrong 4 times in a row (typically either creating or deleting the .Lock file)
                    // Abort

                    // Exception: Unable to create Lockfile required to split fasta file ...
                    throw new Exception("Unable to create " + LOCK_FILE_PROGRESS_TEXT + " required to split fasta file " + baseFastaFile.FullName + "; tried 4 times without success");
                }
            } while (true);

            return lockStream;

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
                        retryCount -= 1;
                        var oRandom = new Random();
                        clsGlobal.IdleLoop(0.25 + oRandom.NextDouble());
                    }
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in DeleteLockStream: " + ex.Message);
            }

        }

        /// <summary>
        /// Lookup the details for LegacyFASTAFileName in the database
        /// </summary>
        /// <param name="legacyFASTAFileName"></param>
        /// <param name="organismName">Output parameter: the organism name for this fasta file</param>
        /// <returns>The path to the file if found; empty string if no match</returns>
        /// <remarks></remarks>
        private string GetLegacyFastaFilePath(string legacyFASTAFileName, out string organismName)
        {

            const short retryCount = 3;
            const int timeoutSeconds = 120;

            var sqlQuery = new System.Text.StringBuilder();

            organismName = string.Empty;

            // Query V_Legacy_Static_File_Locations in the Protein_Sequences database for the path to the fasta file
            // This queries table T_DMS_Organism_DB_Info in MT_Main
            // That table is updated using data in DMS5
            //
            sqlQuery.Append(" SELECT TOP 1 Full_Path, Organism_Name ");
            sqlQuery.Append(" FROM V_Legacy_Static_File_Locations");
            sqlQuery.Append(" WHERE FileName = '" + legacyFASTAFileName + "'");

            var success = clsGlobal.GetDataTableByQuery(sqlQuery.ToString(), mProteinSeqsDBConnectionString, "GetLegacyFastaFilePath", retryCount, out var legacyStaticFiles, timeoutSeconds);

            if (!success)
            {
                return string.Empty;
            }

            foreach (DataRow dataRow in legacyStaticFiles.Rows)
            {
                var legacyFASTAFilePath = clsGlobal.DbCStr(dataRow[0]);
                organismName = clsGlobal.DbCStr(dataRow[1]);

                return legacyFASTAFilePath;
            }

            // Database query was successful, but no rows were returned
            return string.Empty;

        }

        private bool StoreSplitFastaFileNames(string organismName, IEnumerable<clsFastaFileSplitter.udtFastaFileInfoType> splitFastaFiles)
        {

            var splitFastaName = "??";

            if (string.IsNullOrWhiteSpace(mDMSConnectionString))
            {
                if (clsGlobal.OfflineMode)
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

                    // Setup for execution of the stored procedure
                    var cmd = new SqlCommand
                    {
                        CommandType = CommandType.StoredProcedure,
                        CommandText = SP_NAME_UPDATE_ORGANISM_DB_FILE
                    };

                    cmd.Parameters.Add(new SqlParameter("@FastaFileName", SqlDbType.VarChar, 128)).Value = splitFastaName;
                    cmd.Parameters.Add(new SqlParameter("@OrganismName", SqlDbType.VarChar, 128)).Value = organismName;
                    cmd.Parameters.Add(new SqlParameter("@NumProteins", SqlDbType.Int)).Value = currentSplitFasta.NumProteins;
                    cmd.Parameters.Add(new SqlParameter("@NumResidues", SqlDbType.BigInt)).Value = currentSplitFasta.NumResidues;
                    cmd.Parameters.Add(new SqlParameter("@FileSizeKB", SqlDbType.Int)).Value = (splitFastaFileInfo.Length / 1024.0).ToString("0");
                    cmd.Parameters.Add(new SqlParameter("@Message", SqlDbType.VarChar, 512)).Value = string.Empty;
                    cmd.Parameters.Add(new SqlParameter("@returnCode", SqlDbType.VarChar, 64)).Direction = ParameterDirection.Output;

                    var retryCount = 3;
                    while (retryCount > 0)
                    {
                        try
                        {
                            using (var connection = new SqlConnection(mDMSConnectionString))
                            {
                                connection.Open();
                                cmd.Connection = connection;
                                cmd.ExecuteNonQuery();

                                var returnCode = cmd.Parameters["@returnCode"].Value.ToString();

                                if (!string.IsNullOrWhiteSpace(returnCode))
                                {
                                    // Error occurred
                                    ErrorMessage = SP_NAME_UPDATE_ORGANISM_DB_FILE + " reported return code " + returnCode;

                                    var statusMessage = cmd.Parameters["@Message"].Value;
                                    if (statusMessage != null)
                                    {
                                        ErrorMessage = ErrorMessage + "; " + Convert.ToString(statusMessage);
                                    }

                                    OnErrorEvent(ErrorMessage);
                                    return false;
                                }

                            }

                            break;

                        }
                        catch (Exception ex)
                        {
                            retryCount -= 1;
                            ErrorMessage = "Exception storing fasta file " + splitFastaName + " in T_Organism_DB_File: " + ex.Message;
                            OnErrorEvent(ErrorMessage);
                            // Delay for 2 seconds before trying again
                            clsGlobal.IdleLoop(2);

                        }

                    }

                }

            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in StoreSplitFastaFileNames for " + splitFastaName + ": " + ex.Message;
                OnErrorEvent(ErrorMessage);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Call RefreshCachedOrganismDBInfo in the Protein Sequences database
        /// to update T_DMS_Organism_DB_Info in MT_Main on the ProteinSeqs server
        /// using data from V_DMS_Organism_DB_File_Import
        /// (which pulls from V_Organism_DB_File_Export in DMS5)
        /// </summary>
        private void UpdateCachedOrganismDBInfo()
        {
            if (string.IsNullOrWhiteSpace(mProteinSeqsDBConnectionString))
            {
                if (clsGlobal.OfflineMode)
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
                // Setup for execution of the stored procedure
                var cmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REFRESH_CACHED_ORG_DB_INFO
                };

                cmd.Parameters.Add(new SqlParameter("@returnCode", SqlDbType.VarChar, 64)).Direction = ParameterDirection.Output;

                var retryCount = 3;
                while (retryCount > 0)
                {
                    try
                    {
                        using (var connection = new SqlConnection(mProteinSeqsDBConnectionString))
                        {
                            connection.Open();
                            cmd.Connection = connection;
                            cmd.ExecuteNonQuery();

                            var returnCode = cmd.Parameters["@returnCode"].Value.ToString();

                            if (!string.IsNullOrWhiteSpace(returnCode))
                            {
                                // Error occurred
                                OnErrorEvent("Call to " + SP_NAME_REFRESH_CACHED_ORG_DB_INFO + " reported return code : " + returnCode);
                            }

                        }

                        break;

                    }
                    catch (Exception ex)
                    {
                        retryCount -= 1;
                        ErrorMessage = "Exception updating the cached organism DB info on ProteinSeqs: " + ex.Message;
                        OnErrorEvent(ErrorMessage);
                        // Delay for 2 seconds before trying again
                        clsGlobal.IdleLoop(2);

                    }

                }

            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in UpdateCachedOrganismDBInfo: " + ex.Message;
                OnErrorEvent(ErrorMessage);
            }

        }

        /// <summary>
        /// Validate that the split fasta file exists
        /// </summary>
        /// <param name="baseFastaName">Original (non-split) filename, e.g. RefSoil_2013-11-07.fasta</param>
        /// <param name="splitFastaName">Split fasta filename, e.g. RefSoil_2013-11-07_10x_05.fasta</param>
        /// <returns>True if the split fasta file is defined in DMS</returns>
        /// <remarks>If the split file is not found, will automatically split the original file and update DMS with the split file information</remarks>
        public bool ValidateSplitFastaFile(string baseFastaName, string splitFastaName)
        {

            var currentTask = "Initializing";

            try
            {

                currentTask = "GetLegacyFastaFilePath for splitFastaName";
                var knownSplitFastaFilePath = GetLegacyFastaFilePath(splitFastaName, out _);
                var reSplitFiles = false;

                if (!string.IsNullOrWhiteSpace(knownSplitFastaFilePath))
                {
                    // Split file is defined in the database
                    // Make sure it exists on disk (admins occasionally delete split FASTA files to reclaim disk space)
                    try
                    {
                        if (!mFileCopyUtilities.FileExistsWithRetry(knownSplitFastaFilePath, BaseLogger.LogLevels.DEBUG))
                        {
                            // If the directory exists but no split fasta files exist, assume that we need to re-split the FASTA file
                            var existingSplitFastaFile = new FileInfo(knownSplitFastaFilePath);
                            if (existingSplitFastaFile.Directory == null || !existingSplitFastaFile.Directory.Exists)
                            {
                                ErrorMessage = "Cannot find directory with the base FASTA file: " + knownSplitFastaFilePath;
                                OnErrorEvent(ErrorMessage);
                                return false;
                            }

                            // Extract out the base split fasta name
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
                var baseFastaFilePath = GetLegacyFastaFilePath(baseFastaName, out var organismNameBaseFasta);
                if (string.IsNullOrWhiteSpace(baseFastaFilePath))
                {
                    // Base file not found
                    ErrorMessage = "Cannot find base FASTA file in DMS using V_Legacy_Static_File_Locations: " + baseFastaFilePath + "; ConnectionString: " + mProteinSeqsDBConnectionString;
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

                lockStream.WriteLine("ValidateSplitFastaFile, started at " + DateTime.Now + " by " + mManagerName);

                // Check again for the existence of the desired .Fasta file
                // It's possible another process created the .Fasta file while this process was waiting for the other process's lock file to disappear

                currentTask = "GetLegacyFastaFilePath for splitFastaName (2nd time)";
                var fastaFilePath = GetLegacyFastaFilePath(splitFastaName, out _);
                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // The file now exists
                    ErrorMessage = string.Empty;
                    currentTask = "DeleteLockStream (fasta file now exists)";
                    DeleteLockStream(lockFilePath, lockStream);
                    return true;
                }

                OnSplittingBaseFastaFile(baseFastaFile.FullName, mNumSplitParts);

                // Perform the splitting
                //    Call SplitFastaFile to create a split file, using mNumSplitParts parts

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

                // Verify that the fasta files were created
                currentTask = "Verify new files";
                foreach (var currentSplitFile in mSplitter.SplitFastaFileInfo)
                {
                    var splitFastaFileInfo = new FileInfo(currentSplitFile.FilePath);
                    if (!splitFastaFileInfo.Exists)
                    {
                        ErrorMessage = "Newly created split fasta file not found: " + currentSplitFile.FilePath;
                        OnErrorEvent(ErrorMessage);
                        DeleteLockStream(lockFilePath, lockStream);
                        return false;
                    }
                }

                OnStatusEvent("Fasta file successfully split into " + mNumSplitParts + " parts");

                // Store the newly created Fasta file names, plus their protein and residue stats, in DMS
                currentTask = "StoreSplitFastaFileNames";
                success = StoreSplitFastaFileNames(organismNameBaseFasta, mSplitter.SplitFastaFileInfo);
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

                // Call the procedure that syncs up this information with ProteinSeqs
                currentTask = "UpdateCachedOrganismDBInfo";
                UpdateCachedOrganismDBInfo();

                // Delete any cached MSGFPlus index files corresponding to the split fasta files

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
                currentTask = "DeleteLockStream (fasta file created)";
                DeleteLockStream(lockFilePath, lockStream);

            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in ValidateSplitFastaFile for " + splitFastaName + " at " + currentTask + ": " + ex.Message;
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }

            return true;

        }

        #region "Events and Event Handlers"

        /// <summary>
        /// Event raised when splitting starts
        /// </summary>
        public event SplittingBaseFastaFileEventHandler SplittingBaseFastaFile;

        /// <summary>
        /// Delegate for SplittingBaseFastaFile
        /// </summary>
        /// <param name="baseFastaFileName"></param>
        /// <param name="numSplitParts"></param>
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
            ErrorMessage = "Fasta Splitter Error: " + message;
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

        #endregion

    }
}