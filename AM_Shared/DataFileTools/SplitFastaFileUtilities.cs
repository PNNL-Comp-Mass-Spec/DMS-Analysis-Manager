using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using FastaFileSplitterDLL;
using PRISM;
using PRISM.Logging;

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

            ErrorMessage = string.Empty;
            MSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";

            mFastaUtils = new FastaFileUtilities(fileCopyUtils, mgrParams, jobParams);
            RegisterEvents(mFastaUtils);

            mFileCopyUtilities = fileCopyUtils;
        }

        /// <summary>
        /// Define the connection strings using manager parameters
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool DefineConnectionStrings()
        {
            return mFastaUtils.DefineConnectionStrings();
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

                    var success = mFastaUtils.StoreFastaFileInfoInDatabase(
                        splitFastaName,
                        organismName,
                        currentSplitFasta.NumProteins,
                        currentSplitFasta.NumResidues,
                        splitFastaFileInfo.Length,
                        isDecoyFASTA,
                        out var errorMessage
                    );

                    if (success)
                        continue;

                    ErrorMessage = errorMessage;
                    OnErrorEvent(errorMessage);
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
                    ErrorMessage = string.Format(
                        "Cannot find base FASTA file in DMS using V_Legacy_Static_File_Locations: {0}; ConnectionString: {1}",
                        baseFastaFilePath, mFastaUtils.ProteinSeqsDBConnectionString);

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

                var taskDescription = string.Format("split FASTA file {0}", baseFastaFile.FullName);

                var lockStream = mFastaUtils.CreateLockStream(baseFastaFile, taskDescription, out var lockFilePath);

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
                    currentTask = "DeleteLockFile (FASTA file now exists)";
                    mFastaUtils.DeleteLockFile(lockFilePath, lockStream);
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

                    mFastaUtils.DeleteLockFile(lockFilePath, lockStream);
                    return false;
                }

                // Verify that the FASTA files were created
                currentTask = "Verify new files";

                foreach (var currentSplitFile in mSplitter.SplitFastaFileInfo)
                {
                    var splitFastaFileInfo = new FileInfo(currentSplitFile.FilePath);

                    if (!splitFastaFileInfo.Exists)
                    {
                        ErrorMessage = string.Format("Newly created split FASTA file not found: {0}", currentSplitFile.FilePath);
                        OnErrorEvent(ErrorMessage);
                        mFastaUtils.DeleteLockFile(lockFilePath, lockStream);
                        return false;
                    }
                }

                OnStatusEvent("FASTA file successfully split into {0} parts", mNumSplitParts);

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

                    mFastaUtils.DeleteLockFile(lockFilePath, lockStream);
                    return false;
                }

                // Call the procedure that syncs up this information with ProteinSeqs (only applicable if using SQL Server on ProteinSeqs or CBDMS)
                currentTask = "UpdateCachedOrganismDBInfo";
                mFastaUtils.UpdateCachedOrganismDBInfo(out var errorMessage);

                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    ErrorMessage = errorMessage;
                }

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
                currentTask = "DeleteLockFile (FASTA file created)";
                mFastaUtils.DeleteLockFile(lockFilePath, lockStream);
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
