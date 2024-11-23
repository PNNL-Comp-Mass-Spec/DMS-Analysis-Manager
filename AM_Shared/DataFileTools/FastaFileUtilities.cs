using System;
using System.Data;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.FileAndDirectoryTools;
using PRISM.Logging;
using PRISM;
using PRISMDatabaseUtils;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// FASTA File Utilities
    /// </summary>
    public class FastaFileUtilities : EventNotifier
    {
        private const string DECOY_PREFIX = "XXX_";

        private const string SP_NAME_REFRESH_CACHED_ORG_DB_INFO = "refresh_cached_organism_db_info";

        /// <summary>
        /// Procedure name for adding or updating a standalone FASTA file
        /// </summary>
        public const string SP_NAME_UPDATE_ORGANISM_DB_FILE = "add_update_organism_db_file";

        /// <summary>
        /// LockFile name
        /// </summary>
        public const string LOCK_FILE_PROGRESS_TEXT = "Lockfile";

        /// <summary>
        /// File copy utilities
        /// </summary>
        private readonly FileCopyUtilities mFileCopyUtilities;

        private readonly DirectoryInfo mWorkingDirectory;

        /// <summary>
        /// DMS5 database connection string
        /// </summary>
        // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
        // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
        public string DMSConnectionString { get; private set; }

        /// <summary>
        /// Job parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisJob</remarks>
        public IJobParams JobParams { get; }

        /// <summary>
        /// Local FASTA file path (in the work directory)
        /// </summary>
        /// <remarks>Only applicable when running FragPipe or MSFragger</remarks>
        public string LocalFASTAFilePath { get; private set; }

        /// <summary>
        /// Manager parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisMgrSettings</remarks>
        public IMgrParams MgrParams { get; }

        /// <summary>
        /// Protein Sequences DB connection string
        /// </summary>
        /// <remarks>
        /// SQL Server: Data Source=proteinseqs;Initial Catalog=manager_control
        /// PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
        /// </remarks>
        public string ProteinSeqsDBConnectionString { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Call method DefineConnectionStrings() after instantiating this class and connecting the event handlers</remarks>
        /// <param name="fileCopyUtils">File copy utilities</param>
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        public FastaFileUtilities(FileCopyUtilities fileCopyUtils, IMgrParams mgrParams, IJobParams jobParams)
        {
            MgrParams = mgrParams;
            JobParams = jobParams;
            mWorkingDirectory = new DirectoryInfo(MgrParams.GetParam("WorkDir"));

            mFileCopyUtilities = fileCopyUtils;

            LocalFASTAFilePath = string.Empty;
        }

        private static int BoolToTinyInt(bool value)
        {
            return value ? 1 : 0;
        }


        /// <summary>
        /// Creates a new lock file to allow the calling process to either create the required FASTA file or validate that it exists
        /// </summary>
        /// <param name="parentFastaFile">Parent FASTA file</param>
        /// <param name="taskDescription">Description of the action being performed by the calling method</param>
        /// <param name="lockFilePath">Output: path to the newly created lock file</param>
        /// <returns>Lock file handle</returns>
        public StreamWriter CreateLockStream(FileSystemInfo parentFastaFile, string taskDescription, out string lockFilePath)
        {
            var startTime = DateTime.UtcNow;
            var attemptCount = 0;

            StreamWriter lockStream;

            lockFilePath = Path.Combine(parentFastaFile.FullName + Global.LOCK_FILE_EXTENSION);
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
                        var lockTimeoutTime = lockFi.LastWriteTimeUtc.AddMinutes(60);
                        OnStatusEvent("{0} found; waiting until it is deleted or until {1}: {2}", LOCK_FILE_PROGRESS_TEXT, lockTimeoutTime.ToLocalTime(), lockFi.Name);

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
                            OnStatusEvent("{0} still exists; assuming another process timed out; thus, now deleting file {1}", LOCK_FILE_PROGRESS_TEXT, lockFi.Name);
                            lockFi.Delete();
                        }
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
                            OnStatusEvent("Another process has already created a {0} at {1}; will try again to monitor or create a new one", LOCK_FILE_PROGRESS_TEXT, lockFi.FullName);
                        }
                        else
                        {
                            OnWarningEvent("Exception while creating a new {0} at {1}: {2}", LOCK_FILE_PROGRESS_TEXT, lockFi.FullName, ex.Message);
                        }
                    }
                    else
                    {
                        OnWarningEvent("Exception while monitoring {0} at {1}: {2}", LOCK_FILE_PROGRESS_TEXT, lockFi.FullName, ex.Message);
                    }
                }

                // Something went wrong; wait for 15 seconds then try again
                Global.IdleLoop(15);

                if (attemptCount >= 4)
                {
                    // Something went wrong 4 times in a row (typically either creating or deleting the .Lock file)
                    // Abort

                    // Exception: Unable to create decoy FASTA file ...
                    // Exception: Unable to create the Lockfile required to split FASTA file ...
                    throw new Exception(string.Format(
                        "Unable to create the {0} required to {1}; tried 4 times without success",
                        LOCK_FILE_PROGRESS_TEXT, taskDescription));
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
            // SQL Server: Data Source=proteinseqs;Initial Catalog=manager_control
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var proteinSeqsDBConnectionString = MgrParams.GetParam("FastaCnString");

            if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
            {
                OnErrorEvent("Error in FastaFileUtilities.DefineConnectionStrings: manager parameter FastaCnString is not defined");
                return false;
            }

            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var dmsConnectionString = MgrParams.GetParam("ConnectionString");

            if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
            {
                OnErrorEvent("Error in FastaFileUtilities.DefineConnectionStrings: manager parameter ConnectionString is not defined");
                return false;
            }

            DMSConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, MgrParams.ManagerName);
            ProteinSeqsDBConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(proteinSeqsDBConnectionString, MgrParams.ManagerName);

            return true;
        }

        /// <summary>
        /// Close the lock file StreamWriter then delete the lock file
        /// </summary>
        /// <param name="lockFilePath"></param>
        /// <param name="lockStream"></param>
        public void DeleteLockFile(string lockFilePath, TextWriter lockStream)
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
                        OnErrorEvent("Exception deleting lock file in DeleteLockFile: {0}", ex.Message);
                        retryCount--;
                        var random = new Random();
                        Global.IdleLoop(0.25 + random.NextDouble());
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in DeleteLockFile: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Examine the protein names to count the number of decoy proteins (whose names start with XXX_)
        /// </summary>
        /// <remarks>Stops reading the FASTA file once 100,000 decoy proteins have been found</remarks>
        /// <param name="fastaFile">FASTA file to examine</param>
        /// <param name="isDecoyFASTA">Output: True if any of the proteins start with XXX_, otherwise false</param>
        /// <param name="debugMessage">Output: debug message</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if no errors, false if an error</returns>
        public static bool DetermineIfDecoyFastaFile(FileInfo fastaFile, out bool isDecoyFASTA, out string debugMessage, out string errorMessage)
        {
            const int DECOY_COUNT_THRESHOLD = 100000;

            try
            {
                var forwardCount = 0;
                var decoyCount = 0;

                var reader = new ProteinFileReader.FastaFileReader(fastaFile.FullName);

                while (reader.ReadNextProteinEntry() && decoyCount < DECOY_COUNT_THRESHOLD)
                {
                    if (reader.ProteinName.StartsWith(DECOY_PREFIX))
                        decoyCount++;
                    else
                        forwardCount++;
                }

                var fileSizeMB = fastaFile.Length / 1024.0 / 1024;

                if (decoyCount == 0)
                {
                    debugMessage = string.Format("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins, but no decoy proteins", fastaFile.Name, fileSizeMB, forwardCount);
                    errorMessage = string.Empty;
                    isDecoyFASTA = false;
                    return true;
                }

                // Example messages:
                //   FASTA file H_sapiens_UniProt_decoy.fasta is 76.1 MB and has 69,731 forward proteins and 69,731 decoy proteins
                //   FASTA file TAD_TrypPigBov_decoy.fasta is 46.5 MB and has 120,441 forward proteins and over 100,000 decoy proteins

                debugMessage = string.Format("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins and {3}{4:N0} decoy proteins",
                    fastaFile.Name, fileSizeMB, forwardCount, decoyCount < DECOY_COUNT_THRESHOLD ? string.Empty : "over ", decoyCount);

                errorMessage = string.Empty;
                isDecoyFASTA = true;
                return true;
            }
            catch (Exception ex)
            {
                debugMessage = string.Empty;
                errorMessage = string.Format("Error in DetermineIfDecoyFastaFile: {0}; {1}", ex.Message, StackTraceFormatter.GetExceptionStackTrace(ex));
                isDecoyFASTA = false;
                return false;
            }
        }

        /// <summary>
        /// Lookup the details for LegacyFASTAFileName in the database
        /// </summary>
        /// <param name="legacyFASTAFileName">Legacy FASTA file name</param>
        /// <param name="organismName">Output: the organism name for this FASTA file</param>
        /// <returns>The path to the file if found; empty string if no match</returns>
        public string GetLegacyFastaFilePath(string legacyFASTAFileName, out string organismName)
        {
            const int timeoutSeconds = 120;

            var sqlQuery = new System.Text.StringBuilder();

            // Query V_Legacy_Static_File_Locations (i.e., pc.v_legacy_static_file_locations) for the path to the FASTA file
            // This query should only return one row

            // On PostgreSQL, view V_Legacy_Static_File_Locations determines the path to the file using tables t_organisms and t_organism_db_file
            // Table pc.t_legacy_file_upload_requests is included as a left outer join to obtain the file hash

            // On SQL Server, view V_Legacy_Static_File_Locations queries tables T_DMS_Organisms and T_DMS_Organism_DB_Info in MT_Main (those tables are updated using data in DMS5)
            // Table T_Legacy_File_Upload_Requests in the Protein_Sequences database is included as a left outer join to obtain the file hash

            sqlQuery.Append("SELECT full_path, organism_name ");
            sqlQuery.Append("FROM V_Legacy_Static_File_Locations ");
            sqlQuery.AppendFormat("WHERE file_name = '{0}'", legacyFASTAFileName);

            var dbTools = DbToolsFactory.GetDBTools(ProteinSeqsDBConnectionString, timeoutSeconds, debugMode: MgrParams.TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(sqlQuery.ToString(), out var legacyStaticFiles);

            if (!success)
            {
                organismName = string.Empty;
                return string.Empty;
            }

            foreach (DataRow dataRow in legacyStaticFiles.Rows)
            {
                var legacyFASTAFilePath = dataRow[0].CastDBVal<string>();
                organismName = dataRow[1].CastDBVal<string>();

                return legacyFASTAFilePath;
            }

            // Database query was successful, but no rows were returned
            organismName = string.Empty;
            return string.Empty;
        }

        /// <summary>
        /// Determine if the FASTA file has one or more decoy proteins (whose names start with XXX_)
        /// </summary>
        /// <remarks>Stops reading the FASTA file once 100,000 decoy proteins have been found</remarks>
        /// <param name="fastaFilePath">FASTA file to examine</param>
        /// <param name="isDecoyFASTA">Output: True if any of the proteins start with XXX_, otherwise false</param>
        /// <returns>True if no errors, false if an error</returns>
        public bool IsDecoyFastaFile(string fastaFilePath, out bool isDecoyFASTA)
        {
            var fastaFile = new FileInfo(fastaFilePath);

            var success = DetermineIfDecoyFastaFile(fastaFile, out isDecoyFASTA, out var debugMessage, out var errorMessage);

            if (!string.IsNullOrWhiteSpace(debugMessage))
                OnDebugEvent(debugMessage);

            if (!string.IsNullOrWhiteSpace(errorMessage))
                OnErrorEvent(errorMessage);

            return success && isDecoyFASTA;
        }

        /// <summary>
        /// Retrieve the decoy version of the FASTA file (if it does not exist, auto-create it)
        /// </summary>
        /// <remarks>This only applies to standalone (legacy) FASTA files</remarks>
        /// <param name="fastaFile">Non-decoy version of the FASTA file (should be local to this computer, e.g. C:\DMS_Temp_Org)</param>
        /// <param name="localFastaFilePath">Output: path the FASTA file that was copied to the working directory</param>
        /// <returns>True if the file was found (or created) and copied to the working directory, otherwise false</returns>
        private bool RetrieveDecoyFASTA(FileInfo fastaFile, out string localFastaFilePath)
        {
            var currentTask = "Initializing";

            try
            {
                // ToDo: customize this code to check for _decoy.fasta files, and create them if missing
                // ToDo: For any created files, call add_update_organism_db_file

                localFastaFilePath = string.Empty;
                return false;

                /*
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

                // Check again for the existence of the desired FASTA file
                // It's possible another process created the FASTA file while this process was waiting for the other process's lock file to disappear

                currentTask = "GetLegacyFastaFilePath for splitFastaName (2nd time)";
                var fastaFilePath = GetLegacyFastaFilePath(splitFastaName, out _);

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

                var decoyStatusSuccess = IsDecoyFastaFile(fastaFilePath, out var isDecoyFASTA);

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

        /// <summary>
        /// Store the FASTA file in table t_organism_db_file in the database
        /// </summary>
        /// <param name="fastaFileName">FASTA file name</param>
        /// <param name="organismName">Organism name</param>
        /// <param name="proteinCount">Number of proteins in the FASTA file</param>
        /// <param name="residueCount">Number of residues in the FASTA file</param>
        /// <param name="fileSizeBytes">File size, in bytes</param>
        /// <param name="isDecoyFASTA">True if the FASTA file has decoy proteins</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if successful, false if an error</returns>
        public bool StoreFastaFileInfoInDatabase(
            string fastaFileName,
            string organismName,
            int proteinCount,
            long residueCount,
            long fileSizeBytes,
            bool isDecoyFASTA,
            out string errorMessage)
        {
            var dbTools = DbToolsFactory.GetDBTools(DMSConnectionString, debugMode: MgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Setup for execution of the procedure
            var cmd = dbTools.CreateCommand(SP_NAME_UPDATE_ORGANISM_DB_FILE, CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@fastaFileName", SqlType.VarChar, 128, fastaFileName);
            dbTools.AddParameter(cmd, "@organismName", SqlType.VarChar, 128, organismName);
            dbTools.AddTypedParameter(cmd, "@numProteins", SqlType.Int, value: proteinCount);
            dbTools.AddTypedParameter(cmd, "@numResidues", SqlType.BigInt, value: residueCount);
            dbTools.AddTypedParameter(cmd, "@fileSizeKB", SqlType.Int, value: (int)Math.Round(fileSizeBytes / 1024.0));

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
            {
                errorMessage = string.Empty;
                return true;
            }

            errorMessage = resCode != 0 && returnCode == 0
                ? string.Format("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_UPDATE_ORGANISM_DB_FILE)
                : string.Format("{0} reported return code {1}", SP_NAME_UPDATE_ORGANISM_DB_FILE, returnCodeParam.Value.CastDBVal<string>());

            var message = messageParam.Value.CastDBVal<string>();

            if (!string.IsNullOrWhiteSpace(message))
            {
                errorMessage = string.Format("{0}; message: {1}", errorMessage, message);
            }

            return false;
        }

        private bool ValidateFastaHasDecoyProteins(FileInfo fastaFile)
        {
            try
            {
                // If using a protein collection, could check for "seq_direction=decoy" in proteinOptions
                // However, we'll instead examine the actual protein names for both Protein Collection-based and Legacy FASTA-based jobs

                OnDebugEvent("Verifying that the FASTA file has decoy proteins");

                var success = DetermineIfDecoyFastaFile(fastaFile, out var isDecoyFASTA, out var debugMessage, out var errorMessage);

                if (!string.IsNullOrWhiteSpace(debugMessage))
                    OnDebugEvent(debugMessage);

                if (!string.IsNullOrWhiteSpace(errorMessage))
                    OnDebugEvent(errorMessage);

                return success && isDecoyFASTA;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFastaHasDecoyProteins", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate that the FASTA file exists and has decoy proteins
        /// </summary>
        /// <remarks>Copies the FASTA file to the working directory</remarks>
        /// <param name="fastaFile">Output: FASTA file</param>
        /// <returns>True if successful, false if an error</returns>
        public bool ValidateFragPipeFastaFile(out FileInfo fastaFile)
        {
            try
            {
                // Define the path to the FASTA file
                var localOrgDbFolder = MgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

                // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
                var fastaFilePath = Path.Combine(localOrgDbFolder, JobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

                fastaFile = new FileInfo(fastaFilePath);

                if (!fastaFile.Exists)
                {
                    // FASTA file not found
                    OnErrorEvent("FASTA file not found: " + fastaFile.FullName);
                    return false;
                }

                var proteinCollectionList = JobParams.GetParam("ProteinCollectionList");

                var fastaHasDecoys = ValidateFastaHasDecoyProteins(fastaFile);

                string localFastaFilePath;

                if (fastaHasDecoys)
                {
                    localFastaFilePath = Path.Combine(mWorkingDirectory.FullName, fastaFile.Name);

                    // ReSharper disable once CommentTypo

                    // Copy the FASTA file to the working directory

                    // This is done because FragPipe indexes the file based on the dynamic and static mods,
                    // and we want that index file to be in the working directory
                    // Example index file name: ID_007564_FEA6EC69.fasta.1.pepindex

                    fastaFile.CopyTo(localFastaFilePath, true);
                }
                else
                {
                    bool decoyFileRetrieved;
                    string warningMessage;

                    if (string.IsNullOrWhiteSpace(proteinCollectionList) || proteinCollectionList.Equals("na", StringComparison.OrdinalIgnoreCase))
                    {
                        // Using a standalone (legacy) FASTA file that does not have decoy proteins; this would lead to errors with Peptide Prophet or Percolator
                        // Retrieve the decoy version of the FASTA file (if it does not exist, auto-create it)

                        decoyFileRetrieved = RetrieveDecoyFASTA(fastaFile, out localFastaFilePath);
                        warningMessage = string.Empty;
                    }
                    else
                    {
                        decoyFileRetrieved = false;
                        localFastaFilePath = string.Empty;

                        warningMessage = "Protein options for this analysis job contain seq_direction=forward; " +
                                         "decoy proteins will not be used (which would lead to errors with Peptide Prophet or Percolator)";
                    }

                    if (!decoyFileRetrieved)
                    {
                        // The FASTA file does not have decoy sequences
                        // FragPipe will be unable to optimize parameters and Peptide Prophet will likely fail

                        if (!string.IsNullOrWhiteSpace(warningMessage))
                        {
                            OnErrorEvent(warningMessage);
                        }

                        return false;
                    }
                }

                LocalFASTAFilePath = localFastaFilePath;

                // Add the FASTA file and the associated index files to the list of files to skip when copying results to the transfer directory
                JobParams.AddResultFileToSkip(Path.GetFileName(LocalFASTAFilePath));

                // ReSharper disable once StringLiteralTypo
                JobParams.AddResultFileExtensionToSkip(".pepindex");

                // This file was created by older versions of MSFragger, but has not been seen with MSFragger 4.1
                JobParams.AddResultFileExtensionToSkip("peptide_idx_dict");

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFragPipeFastaFile", ex);
                fastaFile = null;
                return false;
            }
        }

        /// <summary>
        /// Call refresh_cached_organism_db_info in the Protein Sequences database to update T_DMS_Organism_DB_Info in MT_Main
        /// on the ProteinSeqs server using data from V_DMS_Organism_DB_File_Import (which pulls from V_Organism_DB_File_Export in DMS5)
        /// </summary>
        /// <remarks>This procedure exits if the ProteinSeqs DB connection string does not point to ProteinSeqs or CBDMS</remarks>
        /// <param name="errorMessage">Output: error message</param>
        public void UpdateCachedOrganismDBInfo(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(ProteinSeqsDBConnectionString))
            {
                if (Global.OfflineMode)
                {
                    // This procedure should not be called when running offline since the FASTA file
                    // should have already been created prior to the remote task starting
                    OnWarningEvent("Skipping call to {0} since offline", SP_NAME_REFRESH_CACHED_ORG_DB_INFO);
                    errorMessage = string.Empty;
                    return;
                }

                errorMessage = string.Format("Cannot call {0} since the ProteinSeqs Connection string is empty", SP_NAME_REFRESH_CACHED_ORG_DB_INFO);
                OnErrorEvent(errorMessage);
                return;
            }

            try
            {
                // Only call procedure refresh_cached_organism_db_info if the connection string points to ProteinSeqs or CBDMS
                if (ProteinSeqsDBConnectionString.IndexOf("Data Source=proteinseqs", StringComparison.OrdinalIgnoreCase) < 0 &&
                    ProteinSeqsDBConnectionString.IndexOf("Data Source=cbdms", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    // Most likely the connection string is "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms",
                    // which is PostgreSQL-based and does not have procedure refresh_cached_organism_db_info
                    errorMessage = string.Empty;
                    return;
                }

                var dbTools = DbToolsFactory.GetDBTools(ProteinSeqsDBConnectionString, debugMode: MgrParams.TraceMode);
                RegisterEvents(dbTools);

                // Setup for execution of the procedure
                var cmd = dbTools.CreateCommand(SP_NAME_REFRESH_CACHED_ORG_DB_INFO, CommandType.StoredProcedure);

                var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

                const int retryCount = 3;
                var resCode = dbTools.ExecuteSP(cmd, retryCount, 2);

                var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                if (resCode != 0 && returnCode == 0)
                {
                    errorMessage = string.Format("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_REFRESH_CACHED_ORG_DB_INFO);
                    OnErrorEvent(errorMessage);
                }
                else if (returnCode != 0)
                {
                    errorMessage = string.Format("Error calling {0}, return code {1}", SP_NAME_REFRESH_CACHED_ORG_DB_INFO, returnCodeParam.Value.CastDBVal<string>());
                    OnErrorEvent(errorMessage);
                }
                else
                {
                    errorMessage = string.Empty;
                }
            }
            catch (Exception ex)
            {
                errorMessage = string.Format("Error in UpdateCachedOrganismDBInfo: {0}", ex.Message);
                OnErrorEvent(errorMessage);
            }
        }

            }
        }
    }
}
