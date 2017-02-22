//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Base class for Sequest analysis
    /// </summary>
    /// <remarks>
    /// Note that MakeOUTFiles() in this class calls a standalone Sequest.Exe program for groups of DTA files
    /// See clsAnalysisToolRunnerSeqCluster for the code used to interface with the Sequest cluster program
    /// </remarks>
    public class clsAnalysisToolRunnerSeqBase : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        public const string CONCATENATED_OUT_TEMP_FILE = "_out.txt.tmp";
        protected const int MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK = 500;
        private const string REGEX_FILE_SEPARATOR = @"^\s*[=]{5,}\s*\""(?<filename>.+)""\s*[=]{5,}\s*$";

        #endregion

        #region "Member variables"

        protected int mDtaCountAddon = 0;

        protected int mTotalOutFileCount = 0;
        protected string mTempConcatenatedOutFilePath = string.Empty;
        protected SortedSet<string> mOutFileNamesAppended = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        // Out file search times (in seconds) for recently created .out files
        protected Queue<float> mRecentOutFileSearchTimes = new Queue<float>(MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK);

        protected Regex m_OutFileNameRegEx = new Regex(@"^(?<rootname>.+)\.(?<startscan>\d+)\.(?<endscan>\d+)\.(?<cs>\d+)\.(?<extension>\S{3})",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        protected Regex m_OutFileSearchTimeRegEx = new Regex(@"\d+/\d+/\d+, \d+\:\d+ [A-Z]+, (?<time>[0-9.]+) sec",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        protected long mOutFileHandlerInUse;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseOutType value indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            CloseOutType Result = CloseOutType.CLOSEOUT_SUCCESS;
            CloseOutType eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;
            var blnProcessingError = false;

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether or not we are resuming a job that stopped prematurely
            // Look for a file named Dataset_out.txt.tmp in m_WorkDir
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

            // Count the number of .Dta files and cache in m_DtaCount
            CalculateNewStatus(blnUpdateDTACount: true);

            //Run Sequest
            UpdateStatusRunning(m_progress, m_DtaCount);

            //Make the .out files
            LogMessage(
                "Making OUT files, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            try
            {
                Result = MakeOUTFiles();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    blnProcessingError = true;
                }
            }
            catch (Exception Err)
            {
                LogError(
                    "clsAnalysisToolRunnerSeqBase.RunTool(), Exception making OUT files, " + Err.Message + "; " +
                    clsGlobal.GetExceptionStackTrace(Err));
                blnProcessingError = true;
            }

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;

            if (blnProcessingError)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                eReturnCode = Result;
            }

            //Add the current job data to the summary file
            UpdateSummaryFile();

            //Make sure objects are released
            Thread.Sleep(500);          // 500 msec delay
            clsProgRunner.GarbageCollectNow();

            // Parse the Sequest .Log file to make sure the expected number of nodes was used in the analysis
            string strSequestLogFilePath = null;
            bool blnSuccess = false;

            if (m_mgrParams.GetParam("cluster", true))
            {
                // Running on a Sequest cluster
                strSequestLogFilePath = Path.Combine(m_WorkDir, "sequest.log");
                blnSuccess = ValidateSequestNodeCount(strSequestLogFilePath);
            }
            else
            {
                blnSuccess = true;
            }

            if (blnProcessingError)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging Sequest problems
                CopyFailedResultsToArchiveFolder();
                if (eReturnCode == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
                return eReturnCode;
            }

            Result = MakeResultsFolder();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return Result;
            }

            Result = MoveResultFiles();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder";
                return Result;
            }

            Result = CopyResultsFolderToServer();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return Result;
            }

            if (!base.RemoveNonResultServerFiles())
            {
                // Do not treat this as a fatal error
                LogWarning(
                    "Error deleting .tmp files in folder " + m_jobParams.GetParam("JobParameters", "transferFolderPath"));
            }

            return eReturnCode;
        }

        /// <summary>
        /// Appends the given .out file to the target file
        /// </summary>
        /// <param name="fiSourceOutFile"></param>
        /// <param name="swTargetFile"></param>
        /// <remarks></remarks>
        protected void AppendOutFile(FileInfo fiSourceOutFile, StreamWriter swTargetFile)
        {
            const string hdrLeft = "=================================== " + "\"";
            const string hdrRight = "\"" + " ==================================";

            string cleanedFileName = null;
            string strLineIn = null;

            float sngOutFileSearchTimeSeconds = 0;

            if (!fiSourceOutFile.Exists)
            {
                Console.WriteLine("Warning, out file not found: " + fiSourceOutFile.FullName);
                return;
            }

            if (!mOutFileNamesAppended.Contains(fiSourceOutFile.Name))
            {
                // Note: do not put a Try/Catch block here
                // Let the calling function catch any errors

                using (var srSrcFile = new StreamReader(new FileStream(fiSourceOutFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var reMatch = m_OutFileNameRegEx.Match(fiSourceOutFile.Name);

                    if (reMatch.Success)
                    {
                        cleanedFileName = reMatch.Groups["rootname"].Value + "." + Convert.ToInt32(reMatch.Groups["startscan"].Value).ToString() + "." +
                                          Convert.ToInt32(reMatch.Groups["endscan"].Value).ToString() + "." +
                                          Convert.ToInt32(reMatch.Groups["cs"].Value).ToString() + "." + reMatch.Groups["extension"].Value;
                    }
                    else
                    {
                        cleanedFileName = fiSourceOutFile.Name;
                    }

                    swTargetFile.WriteLine();
                    swTargetFile.WriteLine(hdrLeft + cleanedFileName + hdrRight);

                    while (!srSrcFile.EndOfStream)
                    {
                        strLineIn = srSrcFile.ReadLine();
                        swTargetFile.WriteLine(strLineIn);

                        reMatch = m_OutFileSearchTimeRegEx.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            if (float.TryParse(reMatch.Groups["time"].Value, out sngOutFileSearchTimeSeconds))
                            {
                                if (mRecentOutFileSearchTimes.Count >= MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK)
                                {
                                    mRecentOutFileSearchTimes.Dequeue();
                                }

                                mRecentOutFileSearchTimes.Enqueue(sngOutFileSearchTimeSeconds);
                            }

                            // Append the remainder of the out file (no need to continue reading line-by-line)
                            if (!srSrcFile.EndOfStream)
                            {
                                swTargetFile.Write(srSrcFile.ReadToEnd());
                            }
                        }
                    }
                }

                if (!mOutFileNamesAppended.Contains(fiSourceOutFile.Name))
                {
                    mOutFileNamesAppended.Add(fiSourceOutFile.Name);
                }

                mTotalOutFileCount += 1;
            }

            string strDtaFilePath = null;
            strDtaFilePath = Path.ChangeExtension(fiSourceOutFile.FullName, "dta");

            try
            {
                if (fiSourceOutFile.Exists)
                {
                    fiSourceOutFile.Delete();
                }

                if (File.Exists(strDtaFilePath))
                {
                    File.Delete(strDtaFilePath);
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
            CalculateNewStatus(blnUpdateDTACount: false);
        }

        /// <summary>
        /// Calculates status information for progress file by counting the number of .out files
        /// </summary>
        /// <param name="blnUpdateDTACount">Set to True to update m_DtaCount</param>
        protected void CalculateNewStatus(bool blnUpdateDTACount)
        {
            int OutFileCount = 0;

            if (blnUpdateDTACount)
            {
                // Get DTA count
                m_DtaCount = GetDTAFileCountRemaining() + mDtaCountAddon;
            }

            // Get OUT file count
            OutFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount;

            // Calculate % complete (value between 0 and 100)
            if (m_DtaCount > 0)
            {
                m_progress = 100f * (OutFileCount / (float)m_DtaCount);
            }
            else
            {
                m_progress = 0;
            }
        }

        protected bool CheckForExistingConcatenatedOutFile()
        {
            string strConcatenatedTempFilePath = null;
            SortedSet<string> lstDTAsToSkip;

            try
            {
                mDtaCountAddon = 0;
                mTotalOutFileCount = 0;

                mTempConcatenatedOutFilePath = string.Empty;
                mOutFileNamesAppended.Clear();

                strConcatenatedTempFilePath = Path.Combine(m_WorkDir, m_Dataset + CONCATENATED_OUT_TEMP_FILE);

                if (File.Exists(strConcatenatedTempFilePath))
                {
                    // Parse the _out.txt.tmp to determine the .out files that it contains
                    lstDTAsToSkip = ConstructDTASkipList(strConcatenatedTempFilePath);
                    LogMessage(
                        "Splitting concatenated DTA file (skipping " + lstDTAsToSkip.Count.ToString("#,##0") +
                        " existing DTAs with existing .Out files)");
                }
                else
                {
                    lstDTAsToSkip = new SortedSet<string>();
                    LogMessage("Splitting concatenated DTA file");
                }

                // Now split the DTA file, skipping DTAs corresponding to .Out files that were copied over
                clsSplitCattedFiles FileSplitter = new clsSplitCattedFiles();
                bool blnSuccess = false;
                blnSuccess = FileSplitter.SplitCattedDTAsOnly(m_Dataset, m_WorkDir, lstDTAsToSkip);

                if (!blnSuccess)
                {
                    m_message = "SplitCattedDTAsOnly returned false";
                    LogDebug(m_message + "; aborting");
                    return false;
                }

                mDtaCountAddon = lstDTAsToSkip.Count;
                mTotalOutFileCount = mDtaCountAddon;

                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        "Completed splitting concatenated DTA file, created " + GetDTAFileCountRemaining().ToString("#,##0") + " DTAs");
                }
            }
            catch (Exception ex)
            {
                LogError(
                    "Error in CheckForExistingConcatenatedOutFile: " + ex.Message);
                return false;
            }

            return true;
        }

        protected SortedSet<string> ConstructDTASkipList(string strConcatenatedTempFilePath)
        {
            string strLineIn = null;

            var lstDTAsToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                var reFileSeparator = new Regex(REGEX_FILE_SEPARATOR, RegexOptions.CultureInvariant | RegexOptions.Compiled);

                using (var srInFile = new StreamReader(new FileStream(strConcatenatedTempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        var objFileSepMatch = reFileSeparator.Match(strLineIn);

                        if (objFileSepMatch.Success)
                        {
                            lstDTAsToSkip.Add(Path.ChangeExtension(objFileSepMatch.Groups["filename"].Value, "dta"));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ConstructDTASkipList: " + ex.Message);
                throw new Exception("Error parsing temporary concatenated temp file", ex);
            }

            return lstDTAsToSkip;
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning(
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
            // We don't need to delete .Dta files since MoveResultFiles() will skip them
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + "_dta.zip"));
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + "_dta.txt"));
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        protected int GetDTAFileCountRemaining()
        {
            var diWorkDir = new DirectoryInfo(m_WorkDir);
            return diWorkDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly).Length;
        }

        protected int GetOUTFileCountRemaining()
        {
            var diWorkDir = new DirectoryInfo(m_WorkDir);
            return diWorkDir.GetFiles("*.out", SearchOption.TopDirectoryOnly).Length;
        }

        /// <summary>
        /// Runs Sequest to make .out files
        /// This function uses the standalone Sequest.exe program; it is not used by the Sequest clusters
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        protected virtual CloseOutType MakeOUTFiles()
        {
            //Creates Sequest .out files from DTA files
            string CmdStr = null;
            string[] DtaFiles = null;
            clsProgRunner[] RunProgs = null;
            StreamWriter[] Textfiles = null;
            int NumFiles = 0;
            int ProcIndx = 0;
            bool StillRunning = false;

            //12/19/2008 - The number of processors used to be configurable but now this is done with clustering.
            //This code is left here so we can still debug to make sure everything still works
            //var NumProcessors = (int)m_mgrParams.GetParam("numberofprocessors");
            var NumProcessors = 1;

            //Get a list of .dta file names
            DtaFiles = Directory.GetFiles(m_WorkDir, "*.dta");
            NumFiles = DtaFiles.GetLength(0);
            if (NumFiles == 0)
            {
                m_message = clsGlobal.AppendToComment(m_message, "No dta files found for Sequest processing");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Set up a program runner and text file for each processor
            RunProgs = new clsProgRunner[NumProcessors];
            Textfiles = new StreamWriter[NumProcessors];
            CmdStr = "-D" + Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName")) + " -P" +
                     m_jobParams.GetParam("parmFileName") + " -R";

            for (ProcIndx = 0; ProcIndx <= NumProcessors - 1; ProcIndx++)
            {
                var DumStr = Path.Combine(m_WorkDir, "FileList" + ProcIndx.ToString() + ".txt");
                m_jobParams.AddResultFileToSkip(DumStr);

                RunProgs[ProcIndx] = new clsProgRunner();
                RunProgs[ProcIndx].Name = "Seq" + ProcIndx.ToString();
                RunProgs[ProcIndx].CreateNoWindow = Convert.ToBoolean(m_mgrParams.GetParam("createnowindow"));
                RunProgs[ProcIndx].Program = m_mgrParams.GetParam("seqprogloc");
                RunProgs[ProcIndx].Arguments = CmdStr + DumStr;
                RunProgs[ProcIndx].WorkDir = m_WorkDir;
                Textfiles[ProcIndx] = new StreamWriter(DumStr, false);
                LogDebug(
                    m_mgrParams.GetParam("seqprogloc") + CmdStr + DumStr);
            }

            //Break up file list into lists for each processor
            ProcIndx = 0;
            foreach (string DumStr in DtaFiles)
            {
                Textfiles[ProcIndx].WriteLine(DumStr);
                ProcIndx += 1;
                if (ProcIndx > (NumProcessors - 1))
                    ProcIndx = 0;
            }

            //Close all the file lists
            for (ProcIndx = 0; ProcIndx <= Textfiles.GetUpperBound(0); ProcIndx++)
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        "clsAnalysisToolRunnerSeqBase.MakeOutFiles: Closing FileList" + ProcIndx);
                }
                try
                {
                    Textfiles[ProcIndx].Close();
                    Textfiles[ProcIndx] = null;
                }
                catch (Exception Err)
                {
                    LogError(
                        "clsAnalysisToolRunnerSeqBase.MakeOutFiles: " + Err.Message + "; " + clsGlobal.GetExceptionStackTrace(Err));
                }
            }

            //Run all the programs
            for (ProcIndx = 0; ProcIndx <= RunProgs.GetUpperBound(0); ProcIndx++)
            {
                RunProgs[ProcIndx].StartAndMonitorProgram();
                Thread.Sleep(1000);
            }

            //Wait for completion

            do
            {
                StillRunning = false;

                // Wait 5 seconds
                Thread.Sleep(5000);

                CalculateNewStatus();
                UpdateStatusRunning(m_progress, m_DtaCount);

                for (ProcIndx = 0; ProcIndx <= RunProgs.GetUpperBound(0); ProcIndx++)
                {
                    if (m_DebugLevel > 4)
                    {
                        LogDebug(
                            "clsAnalysisToolRunnerSeqBase.MakeOutFiles(): RunProgs(" + ProcIndx.ToString() + ").State = " +
                            RunProgs[ProcIndx].State.ToString());
                    }
                    if ((RunProgs[ProcIndx].State != 0))
                    {
                        if (m_DebugLevel > 4)
                        {
                            LogDebug(
                                "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_2: RunProgs(" + ProcIndx.ToString() + ").State = " +
                                RunProgs[ProcIndx].State.ToString());
                        }
                        if (((int) RunProgs[ProcIndx].State != 10))
                        {
                            if (m_DebugLevel > 4)
                            {
                                LogDebug(
                                    "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_3: RunProgs(" + ProcIndx.ToString() + ").State = " +
                                    RunProgs[ProcIndx].State.ToString());
                            }
                            StillRunning = true;
                            break;
                        }
                        else
                        {
                            if (m_DebugLevel >= 1)
                            {
                                LogDebug(
                                    "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_4: RunProgs(" + ProcIndx.ToString() + ").State = " +
                                    RunProgs[ProcIndx].State.ToString());
                            }
                        }
                    }
                }

                LogProgress("Sequest");
            } while (StillRunning);

            //Clean up our object references
            if (m_DebugLevel >= 1)
            {
                LogDebug(
                    "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), cleaning up runprog object references");
            }
            for (ProcIndx = 0; ProcIndx <= RunProgs.GetUpperBound(0); ProcIndx++)
            {
                RunProgs[ProcIndx] = null;
                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        "Set RunProgs(" + ProcIndx.ToString() + ") to Nothing");
                }
            }

            //Make sure objects are released
            Thread.Sleep(500);        // 500 msec delay
            clsProgRunner.GarbageCollectNow();

            //Verify out file creation
            if (m_DebugLevel >= 1)
            {
                LogDebug(
                    "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), verifying out file creation");
            }

            if (GetOUTFileCountRemaining() < 1)
            {
                var msg = "No OUT files created";
                LogErrorToDatabase(msg + ", job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                m_message = clsGlobal.AppendToComment(m_message, msg);
                return CloseOutType.CLOSEOUT_NO_OUT_FILES;
            }
            else
            {
                //Add .out extension to list of file extensions to delete
                m_jobParams.AddResultFileExtensionToSkip(".out");
            }

            //Package out files into concatenated text files
            if (!ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Try to ensure there are no open objects with file handles
            Thread.Sleep(500);         // 1 second delay
            clsProgRunner.GarbageCollectNow();

            //Zip concatenated .out files
            if (!ZipConcatOutFile(m_WorkDir, m_JobNum))
            {
                return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
            }

            //If we got here, everything worked
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Concatenates any .out files that still remain in the the working directory
        /// If running on the Sequest Cluster, then the majority of the files should have already been appended to _out.txt.tmp
        /// </summary>
        /// <param name="WorkDir">Working directory</param>
        /// <param name="DSName">Dataset name</param>
        /// <param name="JobNum">Job number</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        protected virtual bool ConcatOutFiles(string WorkDir, string DSName, string JobNum)
        {
            var MAX_RETRY_ATTEMPTS = 5;
            var MAX_INTERLOCK_WAIT_TIME_MINUTES = 30;
            int intRetriesRemaining = 0;
            bool blnSuccess = false;
            var oRandom = new Random();

            if (m_DebugLevel >= 2)
            {
                LogDebug("Concatenating .out files");
            }

            intRetriesRemaining = MAX_RETRY_ATTEMPTS;

            do
            {
                DateTime dtInterlockWaitStartTime = DateTime.UtcNow;
                DateTime dtInterlockWaitLastLogtime = DateTime.UtcNow;

                while (Interlocked.Read(ref mOutFileHandlerInUse) > 0)
                {
                    // Need to wait for ProcessCandidateOutFiles to exit
                    Thread.Sleep(3000);

                    if (DateTime.UtcNow.Subtract(dtInterlockWaitStartTime).TotalMinutes >= MAX_INTERLOCK_WAIT_TIME_MINUTES)
                    {
                        m_message = "Unable to verify that all .out files have been appended to the _out.txt.tmp file";
                        LogDebug(
                            m_message + ": ConcatOutFiles has waited over " + MAX_INTERLOCK_WAIT_TIME_MINUTES +
                            " minutes for mOutFileHandlerInUse to be zero; aborting");
                        return false;
                    }
                    else if (DateTime.UtcNow.Subtract(dtInterlockWaitStartTime).TotalSeconds >= 30)
                    {
                        dtInterlockWaitStartTime = DateTime.UtcNow;
                        if (m_DebugLevel >= 1)
                        {
                            LogDebug(
                                "ConcatOutFiles is waiting for mOutFileHandlerInUse to be zero");
                        }
                    }
                }

                //Make sure objects are released
                Thread.Sleep(1000);         // 1 second delay
                clsProgRunner.GarbageCollectNow();

                try
                {
                    if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                    {
                        mTempConcatenatedOutFilePath = Path.Combine(m_WorkDir, m_Dataset + "_out.txt.tmp");
                    }

                    var diWorkDir = new DirectoryInfo(WorkDir);

                    using (var swTargetFile = new StreamWriter(new FileStream(mTempConcatenatedOutFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (FileInfo fiOutFile in diWorkDir.GetFileSystemInfos("*.out"))
                        {
                            AppendOutFile(fiOutFile, swTargetFile);
                        }
                    }
                    blnSuccess = true;
                }
                catch (Exception ex)
                {
                    LogWarning(
                        "Error appending .out files to the _out.txt.tmp file" + ": " + ex.Message);
                    Thread.Sleep(oRandom.Next(15, 30) * 1000);           // Delay for a random length between 15 and 30 seconds
                    blnSuccess = false;
                }

                intRetriesRemaining -= 1;
            } while (!blnSuccess && intRetriesRemaining > 0);

            if (!blnSuccess)
            {
                m_message = "Error appending .out files to the _out.txt.tmp file";
                LogError(
                    m_message + "; aborting after " + MAX_RETRY_ATTEMPTS + " attempts");
                return false;
            }

            try
            {
                if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                {
                    // No .out files were created
                    m_message = "No out files were created";
                    return false;
                }

                // Now rename the _out.txt.tmp file to _out.txt
                var fiConcatenatedOutFile = new FileInfo(mTempConcatenatedOutFilePath);

                string strOutFilePathNew = null;
                strOutFilePathNew = Path.Combine(m_WorkDir, m_Dataset + "_out.txt");

                if (File.Exists(strOutFilePathNew))
                {
                    LogWarning("Existing _out.txt file found; overrwriting");
                    File.Delete(strOutFilePathNew);
                }

                fiConcatenatedOutFile.MoveTo(strOutFilePathNew);
            }
            catch (Exception ex)
            {
                m_message = "Error renaming _out.txt.tmp file to _out.txt file";
                LogError(m_message + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the Sequest tool version info in the database
        /// If strOutFilePath is defined, then looks up the specific Sequest version using the given .Out file
        /// Also records the file date of the Sequest Program .exe
        /// </summary>
        /// <param name="strOutFilePath"></param>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strOutFilePath)
        {
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Param file generator
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "ParamFileGenerator"))
            {
                return false;
            }

            // Lookup the version of Sequest using the .Out file
            try
            {
                if (!string.IsNullOrEmpty(strOutFilePath))
                {
                    string strLineIn = null;

                    using (var srOutFile = new StreamReader(new FileStream(strOutFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!srOutFile.EndOfStream)
                        {
                            strLineIn = srOutFile.ReadLine();
                            if (!string.IsNullOrEmpty(strLineIn))
                            {
                                strLineIn = strLineIn.Trim();
                                if (strLineIn.ToLower().StartsWith("TurboSEQUEST".ToLower()))
                                {
                                    strToolVersionInfo = strLineIn;

                                    if (m_DebugLevel >= 2)
                                    {
                                        LogDebug(
                                            "Sequest Version: " + strToolVersionInfo);
                                    }

                                    break;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception parsing .Out file in StoreToolVersionInfo: " + ex.Message);
            }

            // Store the path to the Sequest .Exe in ioToolFiles
            try
            {
                ioToolFiles.Add(new FileInfo(m_mgrParams.GetParam("seqprogloc")));
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception adding Sequest .Exe to ioToolFiles in StoreToolVersionInfo: " + ex.Message);
            }

            try
            {
                // Note that IDPicker uses Tool_Version_Info_Sequest.txt when creating pepXML files
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Make sure at least one .DTA file exists
        /// Also makes sure at least one of the .DTA files has data
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ValidateDTAFiles()
        {
            FileInfo[] ioFiles = null;

            string strLineIn = null;
            var blnDataFound = false;
            var intFilesChecked = 0;

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                ioFiles = diWorkDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly);

                if (ioFiles.Length == 0)
                {
                    m_message = "No .DTA files are present";
                    LogError(m_message);
                    return false;
                }
                else
                {
                    foreach (var ioFile in ioFiles)
                    {
                        using (var srReader = new StreamReader(new FileStream(ioFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                        {
                            while (!srReader.EndOfStream)
                            {
                                strLineIn = srReader.ReadLine();

                                if (!string.IsNullOrWhiteSpace(strLineIn))
                                {
                                    blnDataFound = true;
                                    break;
                                }
                            }
                        }

                        intFilesChecked += 1;

                        if (blnDataFound)
                            break;
                    }

                    if (!blnDataFound)
                    {
                        if (intFilesChecked == 1)
                        {
                            m_message = "One .DTA file is present, but it is empty";
                        }
                        else
                        {
                            m_message = ioFiles.Length.ToString() + " .DTA files are present, but each is empty";
                        }
                        LogError(m_message);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in ValidateDTAFiles";
                LogError(m_message + ": " + ex.Message);
                return false;
            }

            return blnDataFound;
        }

        /// <summary>
        /// Opens the sequest.log file in the working directory
        /// Parses out the number of nodes used and the number of slave processes spawned
        /// Counts the number of DTA files analyzed by each process
        /// </summary>
        /// <remarks></remarks>
        /// <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
        protected bool ValidateSequestNodeCount(string strLogFilePath)
        {
            return ValidateSequestNodeCount(strLogFilePath, blnLogToConsole: false);
        }

        /// <summary>
        /// Opens the sequest.log file in the working directory
        /// Parses out the number of nodes used and the number of slave processes spawned
        /// Counts the number of DTA files analyzed by each process
        /// </summary>
        /// <param name="strLogFilePath">Path to the sequest.log file to parse</param>
        /// <param name="blnLogToConsole">If true, then displays the various status messages at the console</param>
        /// <remarks></remarks>
        /// <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
        protected bool ValidateSequestNodeCount(string strLogFilePath, bool blnLogToConsole)
        {
            const int ERROR_CODE_A = 2;
            const int ERROR_CODE_B = 4;
            const int ERROR_CODE_C = 8;
            const int ERROR_CODE_D = 16;
            const int ERROR_CODE_E = 32;

            string strLineIn = null;
            string strHostName = null;

            int intValue = 0;

            bool blnShowDetailedRates = false;

            int intHostCount = 0;
            int intNodeCountStarted = 0;
            int intNodeCountActive = 0;
            int intDTACount = 0;

            int intNodeCountExpected = 0;

            string strProcessingMsg = null;

            try
            {
                blnShowDetailedRates = false;

                if (!File.Exists(strLogFilePath))
                {
                    strProcessingMsg = "Sequest.log file not found; cannot verify the sequest node count";
                    if (blnLogToConsole)
                        Console.WriteLine(strProcessingMsg + ": " + strLogFilePath);
                    LogWarning(strProcessingMsg);
                    return false;
                }

                // Initialize the RegEx objects
                var reStartingTask = new Regex(@"Starting the SEQUEST task on (\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reWaitingForReadyMsg = new Regex(@"Waiting for ready messages from (\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reReceivedReadyMsg = new Regex(@"received ready messsage from (.+)\(", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSpawnedSlaveProcesses = new Regex(@"Spawned (\d+) slave processes", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSearchedDTAFile = new Regex(@"Searched dta file .+ on (.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

                intHostCount = 0;            // Value for reStartingTask
                intNodeCountStarted = 0;     // Value for reWaitingForReadyMsg
                intNodeCountActive = 0;      // Value for reSpawnedSlaveProcesses
                intDTACount = 0;

                // Note: This value is obtained when the manager params are grabbed from the Manager Control DB
                // Use this query to view/update expected node counts'
                //  SELECT M.M_Name, PV.MgrID, PV.Value
                //  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
                //  WHERE (PV.TypeID = 122)

                intNodeCountExpected = m_mgrParams.GetParam("SequestNodeCountExpected", 0);

                // This dictionary tracks the number of DTAs processed by each node
                // Initialize the dictionary that will track the number of spectra processed by each host
                var dctHostCounts = new Dictionary<string, int>();

                // This dictionary tracks the number of distinct nodes on each host
                // Initialize the dictionary that will track the number of distinct nodes on each host
                var dctHostNodeCount = new Dictionary<string, int>();

                // This dictionary tracks the number of DTAs processed per node on each host
                // Head node rates are ignored when computing medians and reporting warnings since the head nodes typically process far fewer DTAs than the slave nodes
                // Initialize the dictionary that will track processing rates
                var dctHostProcessingRate = new Dictionary<string, float>();

                using (var srLogFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Read each line from the input file
                    while (!srLogFile.EndOfStream)
                    {
                        strLineIn = srLogFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            // See if the line matches one of the expected RegEx values
                            var reMatch = reStartingTask.Match(strLineIn);
                            if ((reMatch != null) && reMatch.Success)
                            {
                                if (!int.TryParse(reMatch.Groups[1].Value, out intHostCount))
                                {
                                    strProcessingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the Sequest.log file";
                                    if (blnLogToConsole)
                                        Console.WriteLine(strProcessingMsg);
                                    LogWarning(strProcessingMsg);
                                }
                            }
                            else
                            {
                                reMatch = reWaitingForReadyMsg.Match(strLineIn);
                                if ((reMatch != null) && reMatch.Success)
                                {
                                    if (!int.TryParse(reMatch.Groups[1].Value, out intNodeCountStarted))
                                    {
                                        strProcessingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the Sequest.log file";
                                        if (blnLogToConsole)
                                            Console.WriteLine(strProcessingMsg);
                                        LogWarning(strProcessingMsg);
                                    }
                                }
                                else
                                {
                                    reMatch = reReceivedReadyMsg.Match(strLineIn);
                                    if ((reMatch != null) && reMatch.Success)
                                    {
                                        strHostName = reMatch.Groups[1].Value;

                                        if (dctHostNodeCount.TryGetValue(strHostName, out intValue))
                                        {
                                            dctHostNodeCount[strHostName] = intValue + 1;
                                        }
                                        else
                                        {
                                            dctHostNodeCount.Add(strHostName, 1);
                                        }
                                    }
                                    else
                                    {
                                        reMatch = reSpawnedSlaveProcesses.Match(strLineIn);
                                        if ((reMatch != null) && reMatch.Success)
                                        {
                                            if (!int.TryParse(reMatch.Groups[1].Value, out intNodeCountActive))
                                            {
                                                strProcessingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the Sequest.log file";
                                                if (blnLogToConsole)
                                                    Console.WriteLine(strProcessingMsg);
                                                LogWarning(strProcessingMsg);
                                            }
                                        }
                                        else
                                        {
                                            reMatch = reSearchedDTAFile.Match(strLineIn);
                                            if ((reMatch != null) && reMatch.Success)
                                            {
                                                strHostName = reMatch.Groups[1].Value;

                                                if ((strHostName != null))
                                                {
                                                    if (dctHostCounts.TryGetValue(strHostName, out intValue))
                                                    {
                                                        dctHostCounts[strHostName] = intValue + 1;
                                                    }
                                                    else
                                                    {
                                                        dctHostCounts.Add(strHostName, 1);
                                                    }

                                                    intDTACount += 1;
                                                }
                                            }
                                            else
                                            {
                                                // Ignore this line
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                try
                {
                    // Validate the stats

                    strProcessingMsg = "HostCount=" + intHostCount + "; NodeCountActive=" + intNodeCountActive;
                    if (m_DebugLevel >= 1)
                    {
                        if (blnLogToConsole)
                            Console.WriteLine(strProcessingMsg);
                        LogDebug(strProcessingMsg);
                    }
                    m_EvalMessage = string.Copy(strProcessingMsg);

                    if (intNodeCountActive < intNodeCountExpected || intNodeCountExpected == 0)
                    {
                        strProcessingMsg = "Error: NodeCountActive less than expected value (" + intNodeCountActive + " vs. " + intNodeCountExpected + ")";
                        if (blnLogToConsole)
                            Console.WriteLine(strProcessingMsg);
                        LogError(strProcessingMsg);

                        // Update the evaluation message and evaluation code
                        // These will be used by sub CloseTask in clsAnalysisJob
                        //
                        // An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
                        //  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
                        //  "SEQUEST node count is less than the expected value"

                        m_EvalMessage += "; " + strProcessingMsg;
                        m_EvalCode = m_EvalCode | ERROR_CODE_A;
                    }
                    else
                    {
                        if (intNodeCountStarted != intNodeCountActive)
                        {
                            strProcessingMsg = "Warning: NodeCountStarted (" + intNodeCountStarted + ") <> NodeCountActive";
                            if (blnLogToConsole)
                                Console.WriteLine(strProcessingMsg);
                            LogWarning(strProcessingMsg);
                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_B;

                            // Update the evaluation message and evaluation code
                            // These will be used by sub CloseTask in clsAnalysisJob
                            // An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
                            //  "SEQUEST node count is less than the expected value"
                        }
                    }

                    if (dctHostCounts.Count < intHostCount)
                    {
                        // Only record an error here if the number of DTAs processed was at least 2x the number of nodes
                        if (intDTACount >= 2 * intNodeCountActive)
                        {
                            strProcessingMsg = "Error: only " + dctHostCounts.Count + " host" + CheckForPlurality(dctHostCounts.Count) + " processed DTAs";
                            if (blnLogToConsole)
                                Console.WriteLine(strProcessingMsg);
                            LogError(strProcessingMsg);
                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_C;
                        }
                    }

                    // See if any of the hosts processed far fewer or far more spectra than the other hosts
                    // When comparing hosts, we need to scale by the number of active nodes on each host
                    // We'll populate intHostProcessingRate() with the number of DTAs processed per node on each host

                    const float LOW_THRESHOLD_MULTIPLIER = 0.25f;
                    const float HIGH_THRESHOLD_MULTIPLIER = 4;

                    int intNodeCountThisHost = 0;

                    float sngProcessingRate = 0;
                    float sngProcessingRateMedian = 0;

                    float sngThresholdRate = 0;
                    int intWarningCount = 0;

                    foreach (KeyValuePair<string, int> objItem in dctHostCounts)
                    {
                        intNodeCountThisHost = 0;
                        dctHostNodeCount.TryGetValue(objItem.Key, out intNodeCountThisHost);
                        if (intNodeCountThisHost < 1)
                            intNodeCountThisHost = 1;

                        sngProcessingRate = objItem.Value / (float)intNodeCountThisHost;
                        dctHostProcessingRate.Add(objItem.Key, sngProcessingRate);
                    }

                    // Determine the median number of spectra processed (ignoring the head nodes)
                    List<float> lstRatesFiltered = (from item in dctHostProcessingRate where !item.Key.ToLower().Contains("seqcluster") select item.Value).ToList();
                    sngProcessingRateMedian = ComputeMedian(lstRatesFiltered);

                    // Only show warnings if sngProcessingRateMedian is at least 10; otherwise, we don't have enough sampling statistics

                    if (sngProcessingRateMedian >= 10)
                    {
                        // Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the the median value
                        intWarningCount = 0;
                        sngThresholdRate = (float)(LOW_THRESHOLD_MULTIPLIER * sngProcessingRateMedian);

                        foreach (KeyValuePair<string, float> objItem in dctHostProcessingRate)
                        {
                            if (objItem.Value < sngThresholdRate && !objItem.Key.ToLower().Contains("seqcluster"))
                            {
                                intWarningCount = +1;
                            }
                        }

                        if (intWarningCount > 0)
                        {
                            strProcessingMsg = "Warning: " + intWarningCount + " host" + CheckForPlurality(intWarningCount) + " processed fewer than " +
                                               sngThresholdRate.ToString("0.0") + " DTAs/node, which is " + LOW_THRESHOLD_MULTIPLIER +
                                               " times the median value of " + sngProcessingRateMedian.ToString("0.0");
                            if (blnLogToConsole)
                                Console.WriteLine(strProcessingMsg);
                            LogWarning(strProcessingMsg);

                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_D;
                            blnShowDetailedRates = true;
                        }

                        // Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value
                        // When comparing hosts, have to scale by the number of active nodes on each host
                        intWarningCount = 0;
                        sngThresholdRate = (float)(HIGH_THRESHOLD_MULTIPLIER * sngProcessingRateMedian);

                        foreach (KeyValuePair<string, float> objItem in dctHostProcessingRate)
                        {
                            if (objItem.Value > sngThresholdRate && !objItem.Key.ToLower().Contains("seqcluster"))
                            {
                                intWarningCount = +1;
                            }
                        }

                        if (intWarningCount > 0)
                        {
                            strProcessingMsg = "Warning: " + intWarningCount + " host" + CheckForPlurality(intWarningCount) + " processed more than " +
                                               sngThresholdRate.ToString("0.0") + " DTAs/node, which is " + HIGH_THRESHOLD_MULTIPLIER +
                                               " times the median value of " + sngProcessingRateMedian.ToString("0.0");
                            if (blnLogToConsole)
                                Console.WriteLine(strProcessingMsg);
                            LogWarning(strProcessingMsg);

                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_E;
                            blnShowDetailedRates = true;
                        }
                    }

                    if (m_DebugLevel >= 2 || blnShowDetailedRates)
                    {
                        // Log the number of DTAs processed by each host

                        var qHosts = from item in dctHostCounts orderby item.Key select item;

                        foreach (var objItem in qHosts)
                        {
                            intNodeCountThisHost = 0;
                            dctHostNodeCount.TryGetValue(objItem.Key, out intNodeCountThisHost);
                            if (intNodeCountThisHost < 1)
                                intNodeCountThisHost = 1;

                            sngProcessingRate = 0;
                            dctHostProcessingRate.TryGetValue(objItem.Key, out sngProcessingRate);

                            strProcessingMsg = "Host " + objItem.Key + " processed " + objItem.Value + " DTA" + CheckForPlurality(objItem.Value) +
                                               " using " + intNodeCountThisHost + " node" + CheckForPlurality(intNodeCountThisHost) + " (" +
                                               sngProcessingRate.ToString("0.0") + " DTAs/node)";

                            if (blnLogToConsole)
                                Console.WriteLine(strProcessingMsg);
                            LogDebug(strProcessingMsg);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Error occurred

                    strProcessingMsg = "Error in validating the stats in ValidateSequestNodeCount" + ex.Message;
                    if (blnLogToConsole)
                    {
                        Console.WriteLine("====================================================================");
                        Console.WriteLine(strProcessingMsg);
                        Console.WriteLine("====================================================================");
                    }

                    LogError(strProcessingMsg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                // Error occurred

                strProcessingMsg = "Error parsing Sequest.log file in ValidateSequestNodeCount" + ex.Message;
                if (blnLogToConsole)
                {
                    Console.WriteLine("====================================================================");
                    Console.WriteLine(strProcessingMsg);
                    Console.WriteLine("====================================================================");
                }

                LogError(strProcessingMsg);
                return false;
            }

            return true;
        }

        protected string CheckForPlurality(int intValue)
        {
            if (intValue == 1)
            {
                return string.Empty;
            }
            else
            {
                return "s";
            }
        }

        protected float ComputeMedian(List<float> lstValues)
        {
            int intMidpoint = 0;
            List<float> lstSortedValues = (from item in lstValues orderby item select item).ToList();

            if (lstSortedValues.Count == 0)
            {
                return 0;
            }
            else if (lstSortedValues.Count == 1)
            {
                return lstSortedValues[0];
            }
            else
            {
                intMidpoint = (int)Math.Floor(lstSortedValues.Count / 2f);

                if (lstSortedValues.Count % 2 == 0)
                {
                    // Even number of values; return the average of the values around the midpoint
                    return (lstSortedValues[intMidpoint] + lstSortedValues[intMidpoint - 1]) / 2f;
                }
                else
                {
                    // Odd number of values
                    return lstSortedValues[intMidpoint];
                }
            }
        }

        /// <summary>
        /// Zips the concatenated .out file
        /// </summary>
        /// <param name="WorkDir">Working directory</param>
        /// <param name="JobNum">Job number</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        protected virtual bool ZipConcatOutFile(string WorkDir, string JobNum)
        {
            string OutFileName = m_Dataset + "_out.txt";
            string OutFilePath = Path.Combine(WorkDir, OutFileName);

            LogMessage(
                "Zipping concatenated output file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));

            //Verify file exists
            if (!File.Exists(OutFilePath))
            {
                m_message = "Unable to find concatenated .out file";
                LogError(m_message);
                return false;
            }

            try
            {
                //Zip the file
                if (!base.ZipFile(OutFilePath, false))
                {
                    m_message = "Error zipping concat out file";
                    string Msg = m_message + ", job " + m_JobNum + ", step " + m_jobParams.GetParam("Step");
                    LogError(Msg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception zipping concat out file";
                string Msg = m_message + ", job " + m_JobNum + ", step " + m_jobParams.GetParam("Step") + ": " + ex.Message + "; " +
                             clsGlobal.GetExceptionStackTrace(ex);
                LogError(Msg);
                return false;
            }

            m_jobParams.AddResultFileToSkip(OutFileName);

            if (m_DebugLevel >= 1)
            {
                LogDebug(" ... successfully zipped");
            }

            return true;
        }

        #endregion
    }
}
