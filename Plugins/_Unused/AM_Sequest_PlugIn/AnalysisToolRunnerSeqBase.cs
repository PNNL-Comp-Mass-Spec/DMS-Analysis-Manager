//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Base class for SEQUEST analysis
    /// </summary>
    /// <remarks>
    /// Note that MakeOUTFiles() in this class calls a standalone Sequest.Exe program for groups of DTA files
    /// See AnalysisToolRunnerSeqCluster for the code used to interface with the SEQUEST cluster program
    /// </remarks>
    public class AnalysisToolRunnerSeqBase : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: de, dta, endscan, rootname, seqcluster, startscan

        // ReSharper restore CommentTypo

        public const string CONCATENATED_OUT_TEMP_FILE = "_out.txt.tmp";
        private const int MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK = 500;
        private const string REGEX_FILE_SEPARATOR = @"^\s*[=]{5,}\s*\""(?<filename>.+)""\s*[=]{5,}\s*$";

        private int mDtaCountAddon;

        protected int mTotalOutFileCount;
        protected string mTempConcatenatedOutFilePath = string.Empty;
        protected SortedSet<string> mOutFileNamesAppended = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Out file search times (in seconds) for recently created .out files
        /// </summary>
        protected Queue<float> mRecentOutFileSearchTimes = new(MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK);

        private readonly Regex mOutFileNameRegEx = new(@"^(?<rootname>.+)\.(?<startscan>\d+)\.(?<endscan>\d+)\.(?<cs>\d+)\.(?<extension>\S{3})",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        private readonly Regex mOutFileSearchTimeRegEx = new(@"\d+/\d+/\d+, \d+\:\d+ [A-Z]+, (?<time>[0-9.]+) sec",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        protected long mOutFileHandlerInUse;

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseOutType value indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether or not we are resuming a job that stopped prematurely
            // Look for a file named Dataset_out.txt.tmp in mWorkDir
            // This procedure will also de-concatenate the _dta.txt file
            if (!CheckForExistingConcatenatedOutFile())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure at least one .DTA file exists
            if (!ValidateDTAFiles())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            // Count the number of .Dta files and cache in mDtaCount
            CalculateNewStatus(updateDTACount: true);

            // Run SEQUEST
            UpdateStatusRunning(mProgress, mDtaCount);

            // Make the .out files
            LogMessage("Making OUT files, job " + mJob + ", step " + mJobParams.GetParam("Step"));

            CloseOutType eResult;
            bool processingError;

            try
            {
                eResult = MakeOUTFiles();
                processingError = eResult != CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerSeqBase.RunTool(), Exception making OUT files", ex);
                processingError = true;
                eResult = CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            CloseOutType eReturnCode;

            if (processingError)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                eReturnCode = eResult;
            }

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            ProgRunner.GarbageCollectNow();

            // Parse the SEQUEST .Log file to make sure the expected number of nodes was used in the analysis

            if (mMgrParams.GetParam("cluster", true))
            {
                // Running on a SEQUEST cluster
                var sequestLogFilePath = Path.Combine(mWorkDir, "sequest.log");
                ValidateSequestNodeCount(sequestLogFilePath);
            }

            if (processingError)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();

                if (eReturnCode == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                return eReturnCode;
            }

            var success = CopyResultsToTransferDirectory();

            if (!success)
                return CloseOutType.CLOSEOUT_FAILED;

            if (!RemoveNonResultServerFiles())
            {
                // Do not treat this as a fatal error
                LogWarning("Error deleting .tmp files in folder " + mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH));
            }

            return eReturnCode;
        }

        /// <summary>
        /// Appends the given .out file to the target file
        /// </summary>
        /// <param name="sourceOutFile"></param>
        /// <param name="outFileWriter"></param>
        protected void AppendOutFile(FileInfo sourceOutFile, StreamWriter outFileWriter)
        {
            const string hdrLeft = "=================================== \"";
            const string hdrRight = "\" ==================================";

            if (!sourceOutFile.Exists)
            {
                Console.WriteLine("Warning, out file not found: " + sourceOutFile.FullName);
                return;
            }

            if (!mOutFileNamesAppended.Contains(sourceOutFile.Name))
            {
                // Note: do not put a Try/Catch block here
                // Let the calling method catch any errors

                using (var reader = new StreamReader(new FileStream(sourceOutFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var reMatch = mOutFileNameRegEx.Match(sourceOutFile.Name);

                    string cleanedFileName;

                    if (reMatch.Success)
                    {
                        cleanedFileName = reMatch.Groups["rootname"].Value + "." + Convert.ToInt32(reMatch.Groups["startscan"].Value) + "." +
                                          Convert.ToInt32(reMatch.Groups["endscan"].Value) + "." +
                                          Convert.ToInt32(reMatch.Groups["cs"].Value) + "." + reMatch.Groups["extension"].Value;
                    }
                    else
                    {
                        cleanedFileName = sourceOutFile.Name;
                    }

                    outFileWriter.WriteLine();
                    outFileWriter.WriteLine(hdrLeft + cleanedFileName + hdrRight);

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        outFileWriter.WriteLine(dataLine);

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        reMatch = mOutFileSearchTimeRegEx.Match(dataLine);

                        if (reMatch.Success)
                        {
                            if (float.TryParse(reMatch.Groups["time"].Value, out var outFileSearchTimeSeconds))
                            {
                                if (mRecentOutFileSearchTimes.Count >= MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK)
                                {
                                    mRecentOutFileSearchTimes.Dequeue();
                                }

                                mRecentOutFileSearchTimes.Enqueue(outFileSearchTimeSeconds);
                            }

                            // Append the remainder of the out file (no need to continue reading line-by-line)
                            if (!reader.EndOfStream)
                            {
                                outFileWriter.Write(reader.ReadToEnd());
                            }
                        }
                    }
                }

                if (!mOutFileNamesAppended.Contains(sourceOutFile.Name))
                {
                    mOutFileNamesAppended.Add(sourceOutFile.Name);
                }

                mTotalOutFileCount++;
            }

            var dtaFilePath = Path.ChangeExtension(sourceOutFile.FullName, "dta");

            try
            {
                if (sourceOutFile.Exists)
                {
                    sourceOutFile.Delete();
                }

                if (File.Exists(dtaFilePath))
                {
                    File.Delete(dtaFilePath);
                }
            }
            catch (Exception ex)
            {
                // Ignore deletion errors; we'll delete these files later
                Console.WriteLine("Warning, exception deleting file: " + ex.Message);
            }
        }

        /// <summary>
        /// Calculates status information for progress file by counting the number of .out files
        /// </summary>
        protected void CalculateNewStatus()
        {
            CalculateNewStatus(updateDTACount: false);
        }

        /// <summary>
        /// Calculates status information for progress file by counting the number of .out files
        /// </summary>
        /// <param name="updateDTACount">Set to True to update mDtaCount</param>
        private void CalculateNewStatus(bool updateDTACount)
        {
            if (updateDTACount)
            {
                // Get DTA count
                mDtaCount = GetDTAFileCountRemaining() + mDtaCountAddon;
            }

            // Get OUT file count
            var outFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount;

            // Calculate % complete (value between 0 and 100)
            if (mDtaCount > 0)
            {
                mProgress = 100f * (outFileCount / (float)mDtaCount);
            }
            else
            {
                mProgress = 0;
            }
        }

        private bool CheckForExistingConcatenatedOutFile()
        {
            try
            {
                mDtaCountAddon = 0;
                mTotalOutFileCount = 0;

                mTempConcatenatedOutFilePath = string.Empty;
                mOutFileNamesAppended.Clear();

                var concatenatedTempFilePath = Path.Combine(mWorkDir, mDatasetName + CONCATENATED_OUT_TEMP_FILE);

                SortedSet<string> dtaFilesToSkip;

                if (File.Exists(concatenatedTempFilePath))
                {
                    // Parse the _out.txt.tmp to determine the .out files that it contains
                    dtaFilesToSkip = ConstructDTASkipList(concatenatedTempFilePath);
                    LogMessage(
                        "Splitting concatenated DTA file (skipping " + dtaFilesToSkip.Count.ToString("#,##0") +
                        " existing DTAs with existing .Out files)");
                }
                else
                {
                    dtaFilesToSkip = new SortedSet<string>();
                    LogMessage("Splitting concatenated DTA file");
                }

                // Now split the DTA file, skipping DTAs corresponding to .Out files that were copied over
                var fileSplitter = new SplitCattedFiles();

                var success = fileSplitter.SplitCattedDTAsOnly(mDatasetName, mWorkDir, dtaFilesToSkip);

                if (!success)
                {
                    mMessage = "SplitCattedDTAsOnly returned false";
                    LogDebug(mMessage + "; aborting");
                    return false;
                }

                mDtaCountAddon = dtaFilesToSkip.Count;
                mTotalOutFileCount = mDtaCountAddon;

                if (mDebugLevel >= 1)
                {
                    LogDebug("Completed splitting concatenated DTA file, created " + GetDTAFileCountRemaining().ToString("#,##0") + " DTAs");
                }
            }
            catch (Exception ex)
            {
                LogError("Error in CheckForExistingConcatenatedOutFile: " + ex.Message);
                return false;
            }

            return true;
        }

        private SortedSet<string> ConstructDTASkipList(string concatenatedTempFilePath)
        {
            var dtaFilesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                var reFileSeparator = new Regex(REGEX_FILE_SEPARATOR, RegexOptions.CultureInvariant | RegexOptions.Compiled);

                using var reader = new StreamReader(new FileStream(concatenatedTempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var fileSepMatch = reFileSeparator.Match(dataLine);

                    if (fileSepMatch.Success)
                    {
                        dtaFilesToSkip.Add(Path.ChangeExtension(fileSepMatch.Groups["filename"].Value, "dta"));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ConstructDTASkipList: " + ex.Message);
                throw new Exception("Error parsing temporary concatenated temp file", ex);
            }

            return dtaFilesToSkip;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(mDatasetName + "_dta.zip");
            mJobParams.AddResultFileToSkip(mDatasetName + "_dta.txt");

            base.CopyFailedResultsToArchiveDirectory();
        }

        protected int GetDTAFileCountRemaining()
        {
            var workDir = new DirectoryInfo(mWorkDir);
            return workDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly).Length;
        }

        protected int GetOUTFileCountRemaining()
        {
            var workDir = new DirectoryInfo(mWorkDir);
            return workDir.GetFiles("*.out", SearchOption.TopDirectoryOnly).Length;
        }

        /// <summary>
        /// Runs SEQUEST to make .out files
        /// this method uses the standalone SEQUEST.exe program; it is not used by the SEQUEST clusters
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        protected virtual CloseOutType MakeOUTFiles()
        {
            // Creates SEQUEST .out files from DTA files

            // 12/19/2008 - The number of processors used to be configurable but now this is done with clustering.
            // This code is left here so we can still debug to make sure everything still works
            // var processorsToUse = (int)mMgrParams.GetParam("NumberOfProcessors");
            const int processorsToUse = 1;

            // Get a list of .dta file names
            var dtaFiles = Directory.GetFiles(mWorkDir, "*.dta");
            var dtaFileCount = dtaFiles.GetLength(0);

            if (dtaFileCount == 0)
            {
                LogError("No dta files found for SEQUEST processing");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up a program runner and text file for each processor
            var progRunners = new ProgRunner[processorsToUse];
            var dtaWriters = new StreamWriter[processorsToUse];

            var arguments = " -D" + Path.Combine(mMgrParams.GetParam("OrgDbDir"), mJobParams.GetParam("PeptideSearch", "generatedFastaName")) +
                            " -P" + mJobParams.GetParam("parmFileName") +
                            " -R";

            for (var processorIndex = 0; processorIndex <= processorsToUse - 1; processorIndex++)
            {
                var fileListFile = Path.Combine(mWorkDir, "FileList" + processorIndex + ".txt");
                mJobParams.AddResultFileToSkip(fileListFile);

                progRunners[processorIndex] = new ProgRunner
                {
                    Name = "Seq" + processorIndex,
                    CreateNoWindow = Convert.ToBoolean(mMgrParams.GetParam("CreateNoWindow")),
                    Program = mMgrParams.GetParam("SeqProgLoc"),
                    Arguments = arguments + fileListFile,
                    WorkDir = mWorkDir
                };

                dtaWriters[processorIndex] = new StreamWriter(fileListFile, false);
                LogDebug(
                    mMgrParams.GetParam("SeqProgLoc") + arguments + fileListFile);
            }

            // Break up file list into lists for each processor
            var dtaFileIndex = 0;

            foreach (var dtaFile in dtaFiles)
            {
                dtaWriters[dtaFileIndex].WriteLine(dtaFile);
                dtaFileIndex++;

                if (dtaFileIndex > (processorsToUse - 1))
                    dtaFileIndex = 0;
            }

            // Close all the file lists
            for (var processorIndex = 0; processorIndex <= dtaWriters.GetUpperBound(0); processorIndex++)
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("AnalysisToolRunnerSeqBase.MakeOutFiles: Closing FileList" + processorIndex);
                }
                try
                {
                    dtaWriters[processorIndex].Close();
                    dtaWriters[processorIndex] = null;
                }
                catch (Exception ex)
                {
                    LogError("AnalysisToolRunnerSeqBase.MakeOutFiles: " + ex.Message + "; " + Global.GetExceptionStackTrace(ex));
                }
            }

            // Run all the programs
            for (var processorIndex = 0; processorIndex <= progRunners.GetUpperBound(0); processorIndex++)
            {
                progRunners[processorIndex].StartAndMonitorProgram();
                Global.IdleLoop(1);
            }

            // Wait for completion
            var stillRunning = false;

            do
            {
                // Wait 5 seconds
                Global.IdleLoop(5);

                CalculateNewStatus();
                UpdateStatusRunning(mProgress, mDtaCount);

                for (var processorIndex = 0; processorIndex <= progRunners.GetUpperBound(0); processorIndex++)
                {
                    if (mDebugLevel > 4)
                    {
                        LogDebug(
                            "AnalysisToolRunnerSeqBase.MakeOutFiles(): progRunners(" + processorIndex + ").State = " +
                            progRunners[processorIndex].State);
                    }
                    if (progRunners[processorIndex].State != 0)
                    {
                        if (mDebugLevel > 4)
                        {
                            LogDebug(
                                "AnalysisToolRunnerSeqBase.MakeOutFiles()_2: progRunners(" + processorIndex + ").State = " +
                                progRunners[processorIndex].State);
                        }
                        if ((int)progRunners[processorIndex].State != 10)
                        {
                            if (mDebugLevel > 4)
                            {
                                LogDebug(
                                    "AnalysisToolRunnerSeqBase.MakeOutFiles()_3: progRunners(" + processorIndex + ").State = " +
                                    progRunners[processorIndex].State);
                            }
                            stillRunning = true;
                            break;
                        }

                        if (mDebugLevel >= 1)
                        {
                            LogDebug(
                                "AnalysisToolRunnerSeqBase.MakeOutFiles()_4: progRunners(" + processorIndex + ").State = " +
                                progRunners[processorIndex].State);
                        }
                    }
                }

                LogProgress("SEQUEST");
            } while (stillRunning);

            // Clean up our object references
            if (mDebugLevel >= 1)
            {
                LogDebug("AnalysisToolRunnerSeqBase.MakeOutFiles(), cleaning up progRunner object references");
            }
            for (var processorIndex = 0; processorIndex <= progRunners.GetUpperBound(0); processorIndex++)
            {
                progRunners[processorIndex] = null;

                if (mDebugLevel >= 1)
                {
                    LogDebug("Set progRunners(" + processorIndex + ") to Nothing");
                }
            }

            // Make sure objects are released
            ProgRunner.GarbageCollectNow();

            // Verify out file creation
            if (mDebugLevel >= 1)
            {
                LogDebug("AnalysisToolRunnerSeqBase.MakeOutFiles(), verifying out file creation");
            }

            if (GetOUTFileCountRemaining() < 1)
            {
                const string msg = "No OUT files created";
                LogErrorToDatabase(msg + ", job " + mJob + ", step " + mJobParams.GetParam("Step"));
                UpdateStatusMessage(msg);
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }

            // Add .out extension to list of file extensions to delete
            mJobParams.AddResultFileExtensionToSkip(".out");

            // Package out files into concatenated text files
            if (!ConcatOutFiles(mWorkDir, mDatasetName, mJob))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Try to ensure there are no open objects with file handles
            ProgRunner.GarbageCollectNow();

            // Zip concatenated .out files
            if (!ZipConcatOutFile(mWorkDir, mJob))
            {
                return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
            }

            // If we got here, everything worked
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Concatenates any .out files that still remain in the working directory
        /// If running on the SEQUEST Cluster, the majority of the files should have already been appended to _out.txt.tmp
        /// </summary>
        /// <param name="workDirPath">Working directory path</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="job">Job number</param>
        /// <returns>True if success, false if an error</returns>
        protected virtual bool ConcatOutFiles(string workDirPath, string datasetName, int job)
        {
            const int MAX_RETRY_ATTEMPTS = 5;
            const int MAX_INTERLOCK_WAIT_TIME_MINUTES = 30;
            bool success;

            var random = new Random();

            if (mDebugLevel >= 2)
            {
                LogDebug("Concatenating .out files");
            }

            var retriesRemaining = MAX_RETRY_ATTEMPTS;

            do
            {
                var interlockWaitStartTime = DateTime.UtcNow;

                while (System.Threading.Interlocked.Read(ref mOutFileHandlerInUse) > 0)
                {
                    // Need to wait for ProcessCandidateOutFiles to exit
                    Global.IdleLoop(3);

                    if (DateTime.UtcNow.Subtract(interlockWaitStartTime).TotalMinutes >= MAX_INTERLOCK_WAIT_TIME_MINUTES)
                    {
                        mMessage = "Unable to verify that all .out files have been appended to the _out.txt.tmp file";
                        LogDebug(
                            mMessage + ": ConcatOutFiles has waited over " + MAX_INTERLOCK_WAIT_TIME_MINUTES +
                            " minutes for mOutFileHandlerInUse to be zero; aborting");
                        return false;
                    }

                    if (DateTime.UtcNow.Subtract(interlockWaitStartTime).TotalSeconds >= 30)
                    {
                        interlockWaitStartTime = DateTime.UtcNow;

                        if (mDebugLevel >= 1)
                        {
                            LogDebug("ConcatOutFiles is waiting for mOutFileHandlerInUse to be zero");
                        }
                    }
                }

                // Make sure objects are released
                ProgRunner.GarbageCollectNow();

                try
                {
                    if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                    {
                        mTempConcatenatedOutFilePath = Path.Combine(mWorkDir, mDatasetName + "_out.txt.tmp");
                    }

                    var workDir = new DirectoryInfo(workDirPath);

                    using (var writer = new StreamWriter(new FileStream(mTempConcatenatedOutFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var outFile in workDir.GetFiles("*.out"))
                        {
                            AppendOutFile(outFile, writer);
                        }
                    }
                    success = true;
                }
                catch (Exception ex)
                {
                    LogWarning("Error appending .out files to the _out.txt.tmp file: " + ex.Message);
                    // Delay for a random length between 15 and 30 seconds
                    Global.IdleLoop(random.Next(15, 30));
                    success = false;
                }

                retriesRemaining--;
            } while (!success && retriesRemaining > 0);

            if (!success)
            {
                mMessage = "Error appending .out files to the _out.txt.tmp file";
                LogError(mMessage + "; aborting after " + MAX_RETRY_ATTEMPTS + " attempts");
                return false;
            }

            try
            {
                if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                {
                    // No .out files were created
                    mMessage = "No out files were created";
                    return false;
                }

                // Now rename the _out.txt.tmp file to _out.txt
                var concatenatedOutFile = new FileInfo(mTempConcatenatedOutFilePath);

                var outFilePathNew = Path.Combine(mWorkDir, mDatasetName + "_out.txt");

                if (File.Exists(outFilePathNew))
                {
                    LogWarning("Existing _out.txt file found; overwriting");
                    File.Delete(outFilePathNew);
                }

                concatenatedOutFile.MoveTo(outFilePathNew);
            }
            catch (Exception ex)
            {
                mMessage = "Error renaming _out.txt.tmp file to _out.txt file";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the SEQUEST tool version info in the database
        /// If outFilePath is defined, looks up the specific SEQUEST version using the given .Out file
        /// Also records the file date of the SEQUEST Program .exe
        /// </summary>
        /// <param name="outFilePath"></param>
        protected bool StoreToolVersionInfo(string outFilePath)
        {
            var toolFiles = new List<FileInfo>();
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Param file generator
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "ParamFileGenerator"))
            {
                return false;
            }

            // Lookup the version of SEQUEST using the .Out file
            try
            {
                if (!string.IsNullOrEmpty(outFilePath))
                {
                    using var reader = new StreamReader(new FileStream(outFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        var dataLineTrimmed = dataLine.Trim();

                        if (!dataLineTrimmed.StartsWith("TurboSEQUEST", StringComparison.OrdinalIgnoreCase))
                            continue;

                        toolVersionInfo = dataLineTrimmed;

                        if (mDebugLevel >= 2)
                        {
                            LogDebug("SEQUEST Version: " + toolVersionInfo);
                        }

                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception parsing .Out file in StoreToolVersionInfo: " + ex.Message);
            }

            // Store the path to the SEQUEST .Exe in toolFiles
            try
            {
                toolFiles.Add(new FileInfo(mMgrParams.GetParam("SeqProgLoc")));
            }
            catch (Exception ex)
            {
                LogError("Exception adding SEQUEST .Exe to toolFiles in StoreToolVersionInfo: " + ex.Message);
            }

            try
            {
                // Note that IDPicker uses Tool_Version_Info_Sequest.txt when creating pepXML files
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Make sure at least one .DTA file exists
        /// Also makes sure at least one of the .DTA files has data
        /// </summary>
        private bool ValidateDTAFiles()
        {
            var dataFound = false;
            var filesChecked = 0;

            try
            {
                var workDir = new DirectoryInfo(mWorkDir);

                var dtaFiles = workDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly);

                if (dtaFiles.Length == 0)
                {
                    mMessage = "No .DTA files are present";
                    LogError(mMessage);
                    return false;
                }

                foreach (var dtaFile in dtaFiles)
                {
                    using (var reader = new StreamReader(new FileStream(dtaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (!string.IsNullOrWhiteSpace(dataLine))
                            {
                                dataFound = true;
                                break;
                            }
                        }
                    }

                    filesChecked++;

                    if (dataFound)
                        break;
                }

                if (!dataFound)
                {
                    if (filesChecked == 1)
                    {
                        mMessage = "One .DTA file is present, but it is empty";
                    }
                    else
                    {
                        mMessage = dtaFiles.Length + " .DTA files are present, but each is empty";
                    }
                    LogError(mMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ValidateDTAFiles";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Opens the sequest.log file in the working directory
        /// Parses out the number of nodes used and the number of slave processes spawned
        /// Counts the number of DTA files analyzed by each process
        /// </summary>
        /// <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
        private bool ValidateSequestNodeCount(string logFilePath)
        {
            return ValidateSequestNodeCount(logFilePath, logToConsole: false);
        }

        /// <summary>
        /// Opens the sequest.log file in the working directory
        /// Parses out the number of nodes used and the number of slave processes spawned
        /// Counts the number of DTA files analyzed by each process
        /// </summary>
        /// <param name="logFilePath">Path to the sequest.log file to parse</param>
        /// <param name="logToConsole">If true, displays the various status messages at the console</param>
        /// <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
        private bool ValidateSequestNodeCount(string logFilePath, bool logToConsole)
        {
            const int ERROR_CODE_A = 2;
            const int ERROR_CODE_B = 4;
            const int ERROR_CODE_C = 8;
            const int ERROR_CODE_D = 16;
            const int ERROR_CODE_E = 32;

            var showDetailedRates = false;

            string processingMsg;

            try
            {
                if (!File.Exists(logFilePath))
                {
                    processingMsg = "Sequest.log file not found; cannot verify the SEQUEST node count";

                    if (logToConsole)
                        Console.WriteLine(processingMsg + ": " + logFilePath);
                    LogWarning(processingMsg);
                    return false;
                }

                // Initialize the RegEx objects
                var reStartingTask = new Regex(@"Starting the SEQUEST task on (\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reWaitingForReadyMsg = new Regex(@"Waiting for ready messages from (\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reReceivedReadyMsg = new Regex(@"received ready message from (.+)\(", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSpawnedSlaveProcesses = new Regex(@"Spawned (\d+) slave processes", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSearchedDTAFile = new Regex("Searched dta file .+ on (.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

                var hostCount = 0;
                var nodeCountStarted = 0;
                var nodeCountActive = 0;
                var dtaCount = 0;

                // Note: This value is obtained when the manager params are grabbed from the Manager Control DB
                // Use this query to view/update expected node counts'
                //  SELECT M.M_Name, PV.MgrID, PV.Value
                //  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
                //  WHERE (PV.TypeID = 122)

                var nodeCountExpected = mMgrParams.GetParam("SequestNodeCountExpected", 0);

                // This dictionary tracks the number of DTAs processed by each node
                // Initialize the dictionary that will track the number of spectra processed by each host
                var hostDtaCounts = new Dictionary<string, int>();

                // This dictionary tracks the number of distinct nodes on each host
                // Initialize the dictionary that will track the number of distinct nodes on each host
                var hostNodeCounts = new Dictionary<string, int>();

                // This dictionary tracks the number of DTAs processed per node on each host
                // Head node rates are ignored when computing medians and reporting warnings since the head nodes typically process far fewer DTAs than the slave nodes
                // Initialize the dictionary that will track processing rates
                var hostProcessingRates = new Dictionary<string, float>();

                using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Read each line from the input file
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // See if the line matches one of the expected RegEx values
                        var reMatch = reStartingTask.Match(dataLine);

                        if (reMatch.Success)
                        {
                            if (!int.TryParse(reMatch.Groups[1].Value, out hostCount))
                            {
                                processingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the sequest.log file";

                                if (logToConsole)
                                    Console.WriteLine(processingMsg);
                                LogWarning(processingMsg);
                            }
                            continue;
                        }

                        reMatch = reWaitingForReadyMsg.Match(dataLine);

                        if (reMatch.Success)
                        {
                            if (!int.TryParse(reMatch.Groups[1].Value, out nodeCountStarted))
                            {
                                processingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the sequest.log file";

                                if (logToConsole)
                                    Console.WriteLine(processingMsg);
                                LogWarning(processingMsg);
                            }
                            continue;
                        }

                        reMatch = reReceivedReadyMsg.Match(dataLine);

                        if (reMatch.Success)
                        {
                            var hostNameForNodeCount = reMatch.Groups[1].Value;

                            if (hostNodeCounts.TryGetValue(hostNameForNodeCount, out var nodeCountOnHost))
                            {
                                hostNodeCounts[hostNameForNodeCount] = nodeCountOnHost + 1;
                            }
                            else
                            {
                                hostNodeCounts.Add(hostNameForNodeCount, 1);
                            }
                            continue;
                        }

                        reMatch = reSpawnedSlaveProcesses.Match(dataLine);

                        if (reMatch.Success)
                        {
                            if (!int.TryParse(reMatch.Groups[1].Value, out nodeCountActive))
                            {
                                processingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the sequest.log file";

                                if (logToConsole)
                                    Console.WriteLine(processingMsg);
                                LogWarning(processingMsg);
                            }
                            continue;
                        }

                        reMatch = reSearchedDTAFile.Match(dataLine);

                        if (!reMatch.Success)
                            continue;

                        var hostNameForDtaCount = reMatch.Groups[1].Value;

                        if (string.IsNullOrWhiteSpace(hostNameForDtaCount))
                            continue;

                        if (hostDtaCounts.TryGetValue(hostNameForDtaCount, out var dtaCountOnHost))
                        {
                            hostDtaCounts[hostNameForDtaCount] = dtaCountOnHost + 1;
                        }
                        else
                        {
                            hostDtaCounts.Add(hostNameForDtaCount, 1);
                        }

                        dtaCount++;
                    }
                }

                try
                {
                    // Validate the stats

                    processingMsg = "HostCount=" + hostCount + "; NodeCountActive=" + nodeCountActive;

                    if (mDebugLevel >= 1)
                    {
                        if (logToConsole)
                            Console.WriteLine(processingMsg);
                        LogDebug(processingMsg);
                    }
                    mEvalMessage = processingMsg;

                    if (nodeCountActive < nodeCountExpected || nodeCountExpected == 0)
                    {
                        processingMsg = "Error: NodeCountActive less than expected value (" + nodeCountActive + " vs. " + nodeCountExpected + ")";

                        if (logToConsole)
                            Console.WriteLine(processingMsg);
                        LogError(processingMsg);

                        // Update the evaluation message and evaluation code
                        // These will be used by method CloseTask in AnalysisJob

                        // An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
                        //  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
                        //  "SEQUEST node count is less than the expected value"

                        mEvalMessage += "; " + processingMsg;
                        mEvalCode |= ERROR_CODE_A;
                    }
                    else
                    {
                        if (nodeCountStarted != nodeCountActive)
                        {
                            processingMsg = "Warning: NodeCountStarted (" + nodeCountStarted + ") <> NodeCountActive";

                            if (logToConsole)
                                Console.WriteLine(processingMsg);
                            LogWarning(processingMsg);
                            mEvalMessage += "; " + processingMsg;
                            mEvalCode |= ERROR_CODE_B;

                            // Update the evaluation message and evaluation code
                            // These will be used by method CloseTask in AnalysisJob
                            // An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
                            //  "SEQUEST node count is less than the expected value"
                        }
                    }

                    if (hostDtaCounts.Count < hostCount)
                    {
                        // Only record an error here if the number of DTAs processed was at least 2x the number of nodes
                        if (dtaCount >= 2 * nodeCountActive)
                        {
                            processingMsg = "Error: only " + hostDtaCounts.Count + " host" + CheckForPlurality(hostDtaCounts.Count) + " processed DTAs";

                            if (logToConsole)
                                Console.WriteLine(processingMsg);
                            LogError(processingMsg);
                            mEvalMessage += "; " + processingMsg;
                            mEvalCode |= ERROR_CODE_C;
                        }
                    }

                    // See if any of the hosts processed far fewer or far more spectra than the other hosts
                    // When comparing hosts, we need to scale by the number of active nodes on each host
                    // We'll populate hostProcessingRates() with the number of DTAs processed per node on each host

                    const float LOW_THRESHOLD_MULTIPLIER = 0.25f;
                    const float HIGH_THRESHOLD_MULTIPLIER = 4;

                    foreach (var item in hostDtaCounts)
                    {
                        var hostName = item.Key;
                        var dtaCountThisHost = item.Value;

                        hostNodeCounts.TryGetValue(hostName, out var nodeCountThisHost);

                        if (nodeCountThisHost < 1)
                            nodeCountThisHost = 1;

                        var processingRate = dtaCountThisHost / (float)nodeCountThisHost;
                        hostProcessingRates.Add(hostName, processingRate);
                    }

                    // Determine the median number of spectra processed (ignoring the head nodes)
                    var ratesFiltered = (from item in hostProcessingRates
                                         where item.Key.IndexOf("seqcluster", StringComparison.OrdinalIgnoreCase) < 0
                                         select item.Value).ToList();

                    var processingRateMedian = ComputeMedian(ratesFiltered);

                    // Only show warnings if processingRateMedian is at least 10; otherwise, we don't have enough sampling statistics

                    if (processingRateMedian >= 10)
                    {
                        // Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the median value
                        var warningCount = 0;
                        var lowThresholdRate = LOW_THRESHOLD_MULTIPLIER * processingRateMedian;

                        foreach (var item in hostProcessingRates)
                        {
                            var hostName = item.Key;
                            var hostProcessingRate = item.Value;

                            if (hostProcessingRate < lowThresholdRate && hostName.IndexOf("seqcluster", StringComparison.OrdinalIgnoreCase) < 0)
                            {
                                warningCount = +1;
                            }
                        }

                        if (warningCount > 0)
                        {
                            processingMsg = "Warning: " + warningCount + " host" + CheckForPlurality(warningCount) + " processed fewer than " +
                                            lowThresholdRate.ToString("0.0") + " DTAs/node, which is " + LOW_THRESHOLD_MULTIPLIER +
                                            " times the median value of " + processingRateMedian.ToString("0.0");

                            if (logToConsole)
                                Console.WriteLine(processingMsg);
                            LogWarning(processingMsg);

                            mEvalMessage += "; " + processingMsg;
                            mEvalCode |= ERROR_CODE_D;
                            showDetailedRates = true;
                        }

                        // Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value
                        // When comparing hosts, have to scale by the number of active nodes on each host
                        warningCount = 0;
                        var highThresholdRate = HIGH_THRESHOLD_MULTIPLIER * processingRateMedian;

                        foreach (var item in hostProcessingRates)
                        {
                            var hostName = item.Key;
                            var hostProcessingRate = item.Value;

                            if (hostProcessingRate > highThresholdRate && hostName.IndexOf("seqcluster", StringComparison.OrdinalIgnoreCase) < 0)
                            {
                                warningCount = +1;
                            }
                        }

                        if (warningCount > 0)
                        {
                            processingMsg = "Warning: " + warningCount + " host" + CheckForPlurality(warningCount) + " processed more than " +
                                            highThresholdRate.ToString("0.0") + " DTAs/node, which is " + HIGH_THRESHOLD_MULTIPLIER +
                                            " times the median value of " + processingRateMedian.ToString("0.0");

                            if (logToConsole)
                                Console.WriteLine(processingMsg);
                            LogWarning(processingMsg);

                            mEvalMessage += "; " + processingMsg;
                            mEvalCode |= ERROR_CODE_E;
                            showDetailedRates = true;
                        }
                    }

                    if (mDebugLevel >= 2 || showDetailedRates)
                    {
                        // Log the number of DTAs processed by each host

                        var qHosts = from item in hostDtaCounts orderby item.Key select item;

                        foreach (var item in qHosts)
                        {
                            var hostName = item.Key;
                            var dtaCountThisHost = item.Value;

                            hostNodeCounts.TryGetValue(hostName, out var nodeCountThisHost);

                            if (nodeCountThisHost < 1)
                                nodeCountThisHost = 1;

                            hostProcessingRates.TryGetValue(hostName, out var processingRate);

                            processingMsg = "Host " + hostName + " processed " + dtaCountThisHost + " DTA" + CheckForPlurality(dtaCountThisHost) +
                                            " using " + nodeCountThisHost + " node" + CheckForPlurality(nodeCountThisHost) + " (" +
                                            processingRate.ToString("0.0") + " DTAs/node)";

                            if (logToConsole)
                                Console.WriteLine(processingMsg);
                            LogDebug(processingMsg);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Error occurred

                    processingMsg = "Error in validating the stats in ValidateSequestNodeCount" + ex.Message;

                    if (logToConsole)
                    {
                        Console.WriteLine("====================================================================");
                        Console.WriteLine(processingMsg);
                        Console.WriteLine("====================================================================");
                    }

                    LogError(processingMsg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                // Error occurred

                processingMsg = "Error parsing sequest.log file in ValidateSequestNodeCount" + ex.Message;

                if (logToConsole)
                {
                    Console.WriteLine("====================================================================");
                    Console.WriteLine(processingMsg);
                    Console.WriteLine("====================================================================");
                }

                LogError(processingMsg);
                return false;
            }

            return true;
        }

        protected string CheckForPlurality(int value)
        {
            if (value == 1)
            {
                return string.Empty;
            }

            return "s";
        }

        private float ComputeMedian(List<float> values)
        {
            var sortedValues = (from item in values orderby item select item).ToList();

            if (sortedValues.Count == 0)
            {
                return 0;
            }

            if (sortedValues.Count == 1)
            {
                return sortedValues[0];
            }

            var midpoint = (int)Math.Floor(sortedValues.Count / 2f);

            if (sortedValues.Count % 2 == 0)
            {
                // Even number of values; return the average of the values around the midpoint
                return (sortedValues[midpoint] + sortedValues[midpoint - 1]) / 2f;
            }

            // Odd number of values
            return sortedValues[midpoint];
        }

        /// <summary>
        /// Zips the concatenated .out file
        /// </summary>
        /// <param name="workDir">Working directory</param>
        /// <param name="jobNum">Job number</param>
        /// <returns>True if success, false if an error</returns>
        protected virtual bool ZipConcatOutFile(string workDir, int jobNum)
        {
            var outFileName = mDatasetName + "_out.txt";
            var outFilePath = Path.Combine(workDir, outFileName);

            LogMessage("Zipping concatenated output file, job " + mJob + ", step " + mJobParams.GetParam("Step"));

            // Verify file exists
            if (!File.Exists(outFilePath))
            {
                mMessage = "Unable to find concatenated .out file";
                LogError(mMessage);
                return false;
            }

            try
            {
                // Zip the file
                if (!ZipFile(outFilePath, false))
                {
                    mMessage = "Error zipping concatenated out file";

                    LogError("{0}, job {1}, step {2}", mMessage, mJob, mJobParams.GetParam("Step"));
                    return false;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception zipping concatenated out file";
                LogError("{0}, job {1}, step {2}: {3}; {4}",
                    mMessage, mJob, mJobParams.GetParam("Step"), ex.Message, Global.GetExceptionStackTrace(ex));

                return false;
            }

            mJobParams.AddResultFileToSkip(outFileName);

            if (mDebugLevel >= 1)
            {
                LogDebug(" ... successfully zipped");
            }

            return true;
        }
    }
}
