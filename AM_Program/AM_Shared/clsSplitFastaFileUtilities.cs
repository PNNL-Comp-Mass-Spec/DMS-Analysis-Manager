
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using FastaFileSplitterDLL;
using PRISM;


namespace AnalysisManagerBase
{
    /// <summary>
    /// FASTA file utilities
    /// </summary>
    public class clsSplitFastaFileUtilities : clsEventNotifier
    {

        /// <summary>
        /// LockFile name
        /// </summary>
        public const string LOCK_FILE_PROGRESS_TEXT = "Lockfile";

        private const string SP_NAME_UPDATE_ORGANISM_DB_FILE = "AddUpdateOrganismDBFile";

        private const string SP_NAME_REFRESH_CACHED_ORG_DB_INFO = "RefreshCachedOrganismDBInfo";
        private readonly string mDMSConnectionString;
        private readonly string mProteinSeqsDBConnectionString;

        private string mMSGFPlusIndexFilesFolderPathLegacyDB;
        private readonly int mNumSplitParts;

        private readonly string mManagerName;

        private string mErrorMessage;

        private bool mWaitingForLockFile;
        private clsFastaFileSplitter mSplitter;

        /// <summary>
        /// Most recent error message
        /// </summary>
        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// MSGF+ index files folder path
        /// </summary>
        public string MSGFPlusIndexFilesFolderPathLegacyDB
        {
            get => mMSGFPlusIndexFilesFolderPathLegacyDB;
            set => mMSGFPlusIndexFilesFolderPathLegacyDB = value;
        }

        /// <summary>
        /// True if waiting for a lock file
        /// </summary>
        public bool WaitingForLockFile => mWaitingForLockFile;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dmsConnectionString"></param>
        /// <param name="proteinSeqsDBConnectionString"></param>
        /// <param name="numSplitParts"></param>
        /// <param name="managerName"></param>
        /// <remarks></remarks>
        public clsSplitFastaFileUtilities(string dmsConnectionString, string proteinSeqsDBConnectionString, int numSplitParts, string managerName)
        {
            mDMSConnectionString = dmsConnectionString;
            mProteinSeqsDBConnectionString = proteinSeqsDBConnectionString;
            mNumSplitParts = numSplitParts;
            mManagerName = managerName;

            mMSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";

            mErrorMessage = string.Empty;
            mWaitingForLockFile = false;

        }

        /// <summary>
        /// Creates a new lock file to allow the calling process to either create the split fasta file or validate that the split fasta file exists
        /// </summary>
        /// <param name="fiBaseFastaFile"></param>
        /// <param name="lockFilePath">Output parameter: path to the newly created lock file</param>
        /// <returns>Lock file handle</returns>
        /// <remarks></remarks>
        private StreamWriter CreateLockStream(FileSystemInfo fiBaseFastaFile, out string lockFilePath)
        {

            var startTime = DateTime.UtcNow;
            var attemptCount = 0;

            StreamWriter lockStream;

            lockFilePath = Path.Combine(fiBaseFastaFile.FullName + clsGlobal.LOCK_FILE_EXTENSION);
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
                        mWaitingForLockFile = true;

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

                        mWaitingForLockFile = false;

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
                    throw new Exception("Unable to create " + LOCK_FILE_PROGRESS_TEXT + " required to split fasta file " + fiBaseFastaFile.FullName + "; tried 4 times without success");
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
                        Thread.Sleep(oRandom.Next(100, 500));
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

            var SqlStr = new System.Text.StringBuilder();

            organismName = string.Empty;

            // Query V_Legacy_Static_File_Locations for the path to the fasta file
            //
            SqlStr.Append(" SELECT TOP 1 Full_Path, Organism_Name ");
            SqlStr.Append(" FROM V_Legacy_Static_File_Locations");
            SqlStr.Append(" WHERE FileName = '" + legacyFASTAFileName + "'");


            var success = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), mProteinSeqsDBConnectionString, "GetLegacyFastaFilePath", retryCount, out var dtResults);

            if (!success)
            {
                return string.Empty;
            }


            foreach (DataRow CurRow in dtResults.Rows)
            {
                var legacyFASTAFilePath = clsGlobal.DbCStr(CurRow[0]);
                organismName = clsGlobal.DbCStr(CurRow[1]);

                return legacyFASTAFilePath;
            }

            // Database query was successful, but no rows were returned
            return string.Empty;

        }

        private bool StoreSplitFastaFileNames(string organismName, IEnumerable<clsFastaFileSplitter.udtFastaFileInfoType> lstSplitFastaInfo)
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
                foreach (var udtFileInfo in lstSplitFastaInfo)
                {
                    // Add/update each split file

                    var fiSplitFastaFile = new FileInfo(udtFileInfo.FilePath);
                    splitFastaName = fiSplitFastaFile.Name;

                    // Setup for execution of the stored procedure
                    var cmd = new SqlCommand
                    {
                        CommandType = CommandType.StoredProcedure,
                        CommandText = SP_NAME_UPDATE_ORGANISM_DB_FILE
                    };

                    cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(new SqlParameter("@FastaFileName", SqlDbType.VarChar, 128)).Value = splitFastaName;
                    cmd.Parameters.Add(new SqlParameter("@OrganismName", SqlDbType.VarChar, 128)).Value = organismName;
                    cmd.Parameters.Add(new SqlParameter("@NumProteins", SqlDbType.Int)).Value = udtFileInfo.NumProteins;
                    cmd.Parameters.Add(new SqlParameter("@NumResidues", SqlDbType.BigInt)).Value = udtFileInfo.NumResidues;
                    cmd.Parameters.Add(new SqlParameter("@FileSizeKB", SqlDbType.Int)).Value = (fiSplitFastaFile.Length / 1024.0).ToString("0");
                    cmd.Parameters.Add(new SqlParameter("@Message", SqlDbType.VarChar, 512)).Value = string.Empty;

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

                                var resultCode = Convert.ToInt32(cmd.Parameters["@Return"].Value);

                                if (resultCode != 0)
                                {
                                    // Error occurred
                                    mErrorMessage = SP_NAME_UPDATE_ORGANISM_DB_FILE + " returned a non-zero error code of " + resultCode;

                                    var statusMessage = cmd.Parameters["@Message"].Value;
                                    if ((statusMessage != null))
                                    {
                                        mErrorMessage = mErrorMessage + "; " + Convert.ToString(statusMessage);
                                    }

                                    OnErrorEvent(mErrorMessage);
                                    return false;
                                }

                            }

                            break;

                        }
                        catch (Exception ex)
                        {
                            retryCount -= 1;
                            mErrorMessage = "Exception storing fasta file " + splitFastaName + " in T_Organism_DB_File: " + ex.Message;
                            OnErrorEvent(mErrorMessage);
                            // Delay for 2 seconds before trying again
                            clsGlobal.IdleLoop(2);

                        }

                    }

                }

            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in StoreSplitFastaFileNames for " + splitFastaName + ": " + ex.Message;
                OnErrorEvent(mErrorMessage);
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

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

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

                            var resultCode = Convert.ToInt32(cmd.Parameters["@Return"].Value);

                            if (resultCode != 0)
                            {
                                // Error occurred
                                OnErrorEvent("Call to " + SP_NAME_REFRESH_CACHED_ORG_DB_INFO + " returned a non-zero error code: " + resultCode);
                            }

                        }

                        break;

                    }
                    catch (Exception ex)
                    {
                        retryCount -= 1;
                        mErrorMessage = "Exception updating the cached organism DB info on ProteinSeqs: " + ex.Message;
                        OnErrorEvent(mErrorMessage);
                        // Delay for 2 seconds before trying again
                        clsGlobal.IdleLoop(2);

                    }

                }

            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in UpdateCachedOrganismDBInfo: " + ex.Message;
                OnErrorEvent(mErrorMessage);
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
                var fastaFilePath = GetLegacyFastaFilePath(splitFastaName, out var _);

                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // Split file is defined in the database
                    mErrorMessage = string.Empty;
                    return true;
                }

                // Split file not found
                // Query DMS for the location of baseFastaName
                currentTask = "GetLegacyFastaFilePath for baseFastaName";
                var baseFastaFilePath = GetLegacyFastaFilePath(baseFastaName, out var organismNameBaseFasta);
                if (string.IsNullOrWhiteSpace(baseFastaFilePath))
                {
                    // Base file not found
                    mErrorMessage = "Cannot find base FASTA file in DMS using V_Legacy_Static_File_Locations: " + baseFastaFilePath + "; ConnectionString: " + mProteinSeqsDBConnectionString;
                    OnErrorEvent(mErrorMessage);
                    return false;
                }

                var fiBaseFastaFile = new FileInfo(baseFastaFilePath);

                if (!fiBaseFastaFile.Exists)
                {
                    mErrorMessage = "Cannot split FASTA file; file not found: " + baseFastaFilePath;
                    OnErrorEvent(mErrorMessage);
                    return false;
                }

                // Try to create a lock file
                currentTask = "CreateLockStream";
                var lockStream = CreateLockStream(fiBaseFastaFile, out var lockFilePath);

                if (lockStream == null)
                {
                    // Unable to create a lock stream; an exception has likely already been thrown
                    throw new Exception("Unable to create lock file required to split " + fiBaseFastaFile.FullName);
                }

                lockStream.WriteLine("ValidateSplitFastaFile, started at " + DateTime.Now + " by " + mManagerName);

                // Check again for the existence of the desired .Fasta file
                // It's possible another process created the .Fasta file while this process was waiting for the other process's lock file to disappear

                currentTask = "GetLegacyFastaFilePath for splitFastaName (2nd time)";
                fastaFilePath = GetLegacyFastaFilePath(splitFastaName, out _);
                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // The file now exists
                    mErrorMessage = string.Empty;
                    currentTask = "DeleteLockStream (fasta file now exists)";
                    DeleteLockStream(lockFilePath, lockStream);
                    return true;
                }

                OnSplittingBaseFastafile(fiBaseFastaFile.FullName, mNumSplitParts);

                // Perform the splitting
                //    Call SplitFastaFile to create a split file, using mNumSplitParts parts

                mSplitter = new clsFastaFileSplitter
                {
                    ShowMessages = true,
                    LogMessagesToFile = false
                };

                mSplitter.ErrorEvent += mSplitter_ErrorEvent;
                mSplitter.WarningEvent += mSplitter_WarningEvent;
                mSplitter.ProgressChanged += mSplitter_ProgressChanged;

                currentTask = "SplitFastaFile " + fiBaseFastaFile.FullName;
                var success = mSplitter.SplitFastaFile(fiBaseFastaFile.FullName, fiBaseFastaFile.DirectoryName, mNumSplitParts);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mErrorMessage))
                    {
                        mErrorMessage = "FastaFileSplitter returned false; unknown error";
                        OnErrorEvent(mErrorMessage);
                    }
                    DeleteLockStream(lockFilePath, lockStream);
                    return false;
                }

                // Verify that the fasta files were created
                currentTask = "Verify new files";
                foreach (var splitFileInfo in mSplitter.SplitFastaFileInfo)
                {
                    var fiSplitFastaFile = new FileInfo(splitFileInfo.FilePath);
                    if (!fiSplitFastaFile.Exists)
                    {
                        mErrorMessage = "Newly created split fasta file not found: " + splitFileInfo.FilePath;
                        OnErrorEvent(mErrorMessage);
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
                    if (string.IsNullOrWhiteSpace(mErrorMessage))
                    {
                        mErrorMessage = "StoreSplitFastaFileNames returned false; unknown error";
                        OnErrorEvent(mErrorMessage);
                    }
                    DeleteLockStream(lockFilePath, lockStream);
                    return false;
                }

                // Call the procedure that syncs up this information with ProteinSeqs
                currentTask = "UpdateCachedOrganismDBInfo";
                UpdateCachedOrganismDBInfo();

                // Delete any cached MSGFPlus index files corresponding to the split fasta files

                if (!string.IsNullOrWhiteSpace(mMSGFPlusIndexFilesFolderPathLegacyDB))
                {
                    var diIndexFolder = new DirectoryInfo(mMSGFPlusIndexFilesFolderPathLegacyDB);


                    if (diIndexFolder.Exists)
                    {
                        foreach (var splitFileInfo in mSplitter.SplitFastaFileInfo)
                        {
                            var fileSpecBase = Path.GetFileNameWithoutExtension(splitFileInfo.FilePath);

                            var lstFileSpecsToFind = new List<string>
                            {
                                fileSpecBase + ".*",
                                fileSpecBase + ".fasta.LastUsed",
                                fileSpecBase + ".fasta.MSGFPlusIndexFileInfo",
                                fileSpecBase + ".fasta.MSGFPlusIndexFileInfo.Lock"
                            };

                            foreach (var fileSpec in lstFileSpecsToFind)
                            {
                                foreach (var fiFile in diIndexFolder.GetFiles(fileSpec))
                                {
                                    try
                                    {
                                        fiFile.Delete();
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
                mErrorMessage = "Exception in ValidateSplitFastaFile for " + splitFastaName + " at " + currentTask + ": " + ex.Message;
                OnErrorEvent(mErrorMessage);
                return false;
            }

            return true;

        }

        #region "Events and Event Handlers"

        /// <summary>
        /// Event raised when splitting starts
        /// </summary>
        public event SplittingBaseFastafileEventHandler SplittingBaseFastafile;

        /// <summary>
        /// Delegate for SplittingBaseFastafile
        /// </summary>
        /// <param name="baseFastaFileName"></param>
        /// <param name="numSplitParts"></param>
        public delegate void SplittingBaseFastafileEventHandler(string baseFastaFileName, int numSplitParts);

        /// <summary>
        /// Raise event SplittingBaseFastafile
        /// </summary>
        private void OnSplittingBaseFastafile(string baseFastaFileName, int numSplitParts)
        {
            SplittingBaseFastafile?.Invoke(baseFastaFileName, numSplitParts);
        }

        private void mSplitter_ErrorEvent(string message)
        {
            mErrorMessage = "Fasta Splitter Error: " + message;
            OnErrorEvent(message);
        }

        private void mSplitter_WarningEvent(string message)
        {
            OnWarningEvent(message);
        }

        private void mSplitter_ProgressChanged(string taskDescription, float percentComplete)
        {
            OnProgressUpdate(taskDescription, (int)percentComplete);
        }

        #endregion

    }
}