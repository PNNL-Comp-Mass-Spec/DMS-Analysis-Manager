/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerNOMSIPlugin
{
    public class clsAnalysisToolRunnerNOMSI : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const float PROGRESS_PCT_STARTING = 5;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string NOMSI_CONSOLE_OUTPUT_BASE = "NOMSI_ConsoleOutput_scan";
        protected const string COMPRESSED_NOMSI_RESULTS_BASE = "NOMSI_Results_scan";

        #endregion

        #region "Module Variables"

        protected string mCurrentConsoleOutputFile;
        protected string mConsoleOutputErrorMsg;
        protected bool mNoPeaksFound;

        protected DateTime mLastConsoleOutputParse;
        protected DateTime mLastProgressWriteTime;

        protected int mTotalSpectra;
        protected int mSpectraProcessed;

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data using NOMSI
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override IJobParams.CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerNOMSI.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                mTotalSpectra = 0;
                mSpectraProcessed = 0;

                // Determine the path to NOMSI
                var progLoc = DetermineProgramLocation("NOMSI", "NOMSIProgLoc", "NOMSI.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the NOMSI.exe version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining NOMSI version";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip the XML files
                var compressedXMLFiles = Path.Combine(m_WorkDir, m_Dataset + "_scans.zip");
                var success = UnzipFile(compressedXMLFiles, m_WorkDir);
                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error extracting the XML spectra files";
                    }
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                bool noPeaksFound;

                // Process the XML files using NOMSI                
                success = ProcessScansWithNOMSI(progLoc, out noPeaksFound);

                var eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

                if (noPeaksFound)
                {
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_NO_DATA;                   
                }
                else if (!success)
                {
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Look for the result files

                    var diWorkDir = new DirectoryInfo(m_WorkDir);
                    var fiResultsFiles = diWorkDir.GetFiles("Distributions.zip").ToList();
                    if (fiResultsFiles.Count == 0)
                    {
                        fiResultsFiles = diWorkDir.GetFiles(COMPRESSED_NOMSI_RESULTS_BASE + "*.zip").ToList();
                    }

                    if (fiResultsFiles.Count == 0)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "NOMSI results not found";
                            success = false;
                            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // if (!UpdateSummaryFile())
                // {
                //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                // }

                // Make sure objects are released
                Thread.Sleep(500);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                if (!success)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging problems
                    CopyFailedResultsToArchiveFolder();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                var result = MakeResultsFolder();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                return eReturnCode;
            }
            catch (Exception ex)
            {
                m_message = "Error in NOMSIPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            var strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the XML files first)
            var strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var fiSpectraFiles = GetXMLSpectraFiles(diWorkDir);

                foreach (var file in fiSpectraFiles)
                    file.Delete();

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);

        }

        private string GetUsername()
        {
            var currentUser = System.Security.Principal.WindowsIdentity.GetCurrent();
            if (currentUser != null)
            {
                var userName = currentUser.Name;
                var lastSlashIndex = userName.LastIndexOf('\\');
                if (lastSlashIndex >= 0)
                    userName = userName.Substring(lastSlashIndex + 1);

                return userName;
            }

            return string.Empty;
        }

        private List<FileInfo> GetXMLSpectraFiles(DirectoryInfo diWorkDir)
        {
            var fiSpectraFiles = diWorkDir.GetFiles(m_Dataset + "_scan*.xml").ToList();
            return fiSpectraFiles;
        }


        private int MoveWorkDirFiles(DirectoryInfo diZipWork, string fileMask)
        {
            var diWorkDir = new DirectoryInfo(m_WorkDir);
            var filesToMove = diWorkDir.GetFiles(fileMask).ToList();

            foreach (var fiFile in filesToMove)
            {
                fiFile.MoveTo(Path.Combine(diZipWork.FullName, fiFile.Name));
            }

            return filesToMove.Count;

        }

        /// <summary>
        /// Parse the NOMSI console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // start=5/4/2015 2:16:45 PM
            // dataset_path=E:\DMS_WorkDir\2015_04_05_ESIL_Pos_ESIL_HighMass_TOF1p4_000001_scan1.xml
            // param_file_path=E:\DMS_WorkDir\NOMSI_DI_Diagnostics_Targets_Pairs_2015-04-30.param
            // 	err	[Only one peak found; cannot run diagnostics]
            // diagnostics=failure
            // summary=success
            // end=5/4/2015 2:16:45 PM
            // 

            try
            {

                var reErrorMessage = new Regex(@"err\t\[(.+)\]", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        if (strLineIn.ToLower().StartsWith("error "))
                        {
                            StoreConsoleErrorMessage(srInFile, strLineIn);
                        }

                        var reMatch = reErrorMessage.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            var errorMessage = reMatch.Groups[1].Value;

                            if (errorMessage.Contains("No peaks found") ||
                                errorMessage.Contains("Only one peak found"))
                            {
                                mNoPeaksFound = true;
                            }
                            else
                            {
                                mConsoleOutputErrorMsg = clsGlobal.AppendToComment(mConsoleOutputErrorMsg, errorMessage);                                    
                            }
                        }
                        else if (strLineIn.Contains("No peaks found") ||
                            strLineIn.Contains("Only one peak found"))
                        {
                            mNoPeaksFound = true;
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        private void PostProcessResultsOneScan(DirectoryInfo diZipWork, int scanCount, int scanNumber)
        {
            diZipWork.Refresh();
            if (diZipWork.Exists)
            {
                foreach (var fileToRemove in diZipWork.GetFiles("*").ToList())
                {
                    fileToRemove.Delete();
                }
            }
            else
            {
                diZipWork.Create();
            }

            var filesMatched = 0;

            if (scanCount == 1)
            {
                // Skip the console output file and nomsi summary file
                m_jobParams.AddResultFileToSkip(mCurrentConsoleOutputFile);
                m_jobParams.AddResultFileToSkip("nomsi_summary.txt");

                // Combine the distribution files into a single .zip file
                filesMatched += MoveWorkDirFiles(diZipWork, "distribution*.txt");

                if (filesMatched > 0)
                    m_IonicZipTools.ZipDirectory(diZipWork.FullName, Path.Combine(m_WorkDir, "Distributions.zip"));
            }
            else
            {
                // Combine the distribution files, the dm_ files, and the console output file into a zip file
                // Example name: NOMSI_Results_scan1.zip

                filesMatched += MoveWorkDirFiles(diZipWork, "distribution*.txt");
                filesMatched += MoveWorkDirFiles(diZipWork, "dm_pairs*.txt");
                filesMatched += MoveWorkDirFiles(diZipWork, "dm_stats*.txt");
                filesMatched += MoveWorkDirFiles(diZipWork, "NOMSI_ConsoleOutput_scan*.txt");

                if (filesMatched > 0)
                    m_IonicZipTools.ZipDirectory(diZipWork.FullName, Path.Combine(m_WorkDir, COMPRESSED_NOMSI_RESULTS_BASE + scanNumber + ".zip"));
            }
        }

        private bool ProcessOneFileWithNOMSI(
            string progLoc,
            FileInfo spectrumFile,
            string paramFilePath,
            int filesProcessed,
            out int scanNumber)
        {
            // Set up and execute a program runner to run NOMSI

            var reGetScanNumber = new Regex(@"scan(\d+).xml", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var cmdStr = string.Empty;
            cmdStr += " " + PossiblyQuotePath(spectrumFile.FullName);
            cmdStr += " " + PossiblyQuotePath(paramFilePath);

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc + " " + cmdStr);
            }

            scanNumber = filesProcessed;

            var reMatch = reGetScanNumber.Match(spectrumFile.Name);
            if (reMatch.Success)
            {
                int.TryParse(reMatch.Groups[1].Value, out scanNumber);
            }

            mCurrentConsoleOutputFile = Path.Combine(m_WorkDir, NOMSI_CONSOLE_OUTPUT_BASE + scanNumber + ".txt");

            var cmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false
                // ConsoleOutputFilePath = mCurrentConsoleOutputFile
            };
            RegisterEvents(cmdRunner);

            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            var subTaskProgress = filesProcessed / (float)mTotalSpectra * 100;

            m_progress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, subTaskProgress);

            var success = cmdRunner.RunProgram(progLoc, cmdStr, "NOMSI", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile =
                    new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create,
                                                    FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     mConsoleOutputErrorMsg);
            }

            // Parse the nomsi_summary file to look for errors
            Thread.Sleep(250);
            var fiLogSummaryFile = new FileInfo(Path.Combine(m_WorkDir, "nomsi_summary.txt"));
            if (!fiLogSummaryFile.Exists)
            {
                // Summary file not created
                // Look for a log file in folder C:\Users\d3l243\AppData\Local\
                var alternateLogPath = Path.Combine(@"C:\Users", GetUsername(), @"AppData\Local\NOMSI.log");

                var fiAlternateLogFile = new FileInfo(alternateLogPath);
                if (fiAlternateLogFile.Exists)
                    fiAlternateLogFile.CopyTo(fiLogSummaryFile.FullName, true);
            }

            ParseConsoleOutputFile(fiLogSummaryFile.FullName);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            var msg = "Error processing scan " + scanNumber + " using NOMSI";
            m_message = clsGlobal.AppendToComment(m_message, msg);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                 msg + ", job " + m_JobNum);

            if (cmdRunner.ExitCode != 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "NOMSI returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "NOMSI failed (but exit code is 0)");
            }

            return false;
        }

        private bool ProcessScansWithNOMSI(string progLoc, out bool noPeaksFound)
        {

            noPeaksFound = false;

            try
            {

                mConsoleOutputErrorMsg = string.Empty;

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing data using NOMSI");

                var paramFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));

                if (!File.Exists(paramFilePath))
                {
                    LogError("Parameter file not found", "Parameter file not found: " + paramFilePath);
                    return false;
                }

                var targetsFileName = m_jobParams.GetParam("dm_target_file");

                // Update the parameter file to use the targets file specified by the settings file
                var success = UpdateParameterFile(paramFilePath, targetsFileName);
                if (!success)
                    return false;

                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var spectraFiles = GetXMLSpectraFiles(diWorkDir);

                mTotalSpectra = spectraFiles.Count;

                if (mTotalSpectra == 0)
                {
                    m_message = "XML spectrum files not found";
                    return false;
                }

                m_progress = PROGRESS_PCT_STARTING;

                var diZipWork = new DirectoryInfo(Path.Combine(m_WorkDir, "ScanResultsZipWork"));

                var filesProcessed = 0;
                var fileCountNoPeaks = 0;

                foreach (var spectrumFile in spectraFiles)
                {
                    mNoPeaksFound = false;

                    int scanNumber;
                    success = ProcessOneFileWithNOMSI(progLoc, spectrumFile, paramFilePath, filesProcessed, out scanNumber);
                    if (!success)
                        return false;

                    m_jobParams.AddResultFileToSkip(spectrumFile.Name);

                    PostProcessResultsOneScan(diZipWork, spectraFiles.Count, scanNumber);

                    if (mNoPeaksFound)
                        fileCountNoPeaks++;

                    filesProcessed++;
                }

                m_jobParams.AddResultFileToSkip(targetsFileName);

                m_progress = PROGRESS_PCT_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "NOMSI processing Complete");
                }

                if (fileCountNoPeaks <= 0)
                {
                    return true;
                }

                if (filesProcessed == 1 || fileCountNoPeaks >= filesProcessed)
                {
                    // None of the scans had peaks
                    m_message = "No peaks found";
                    if (filesProcessed > 1)
                        m_EvalMessage = "None of the scans had peaks";
                    else
                        m_EvalMessage = "Scan did not have peaks";

                    noPeaksFound = true;

                    m_jobParams.AddResultFileToSkip(paramFilePath);
                }
                else
                {
                    // Some of the scans had no peaks
                    m_EvalMessage = fileCountNoPeaks + " / " + filesProcessed + " scans had no peaks";
                }

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Error in NOMSIPlugin->StartNOMSI";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return false;
            }

        }

        private void StoreConsoleErrorMessage(StreamReader srInFile, string strLineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running NOMSI:";
            }
            mConsoleOutputErrorMsg += "; " + strLineIn;

            while (!srInFile.EndOfStream)
            {
                // Store the remaining console output lines
                strLineIn = srInFile.ReadLine();

                if (!string.IsNullOrWhiteSpace(strLineIn) && !strLineIn.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + strLineIn;
                }

            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strProgLoc)
        {

            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            // Lookup the version of the .NET program
            StoreToolVersionInfoOneFile(ref strToolVersionInfo, fiProgram.FullName);

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                fiProgram
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        private bool UpdateParameterFile(string paramFilePath, string targetsFileName)
        {
            try
            {
                var fiParamFile = new FileInfo(paramFilePath);
                if (fiParamFile.DirectoryName == null)
                {
                    LogError("Directory for parameter file found to be null in UpdateParameterFile");
                    return false;
                }

                if (string.IsNullOrWhiteSpace(targetsFileName))
                {
                    // Leave the parameter file unchanged
                    m_EvalMessage = "Warning: targets file was empty; parameter file used as-is";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage);
                    return true;
                }

                var fiParamFileNew = new FileInfo(fiParamFile.FullName + ".new");

                using (var reader = new StreamReader(new FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(fiParamFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        if (dataLine.Trim().ToLower().StartsWith("param_dm_target_file"))
                        {
                            writer.WriteLine("param_dm_target_file=" + Path.Combine(m_WorkDir, targetsFileName));
                        }
                        else
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                fiParamFile.MoveTo(Path.Combine(fiParamFile.DirectoryName, fiParamFile.Name + ".old"));
                Thread.Sleep(100);

                fiParamFileNew.MoveTo(paramFilePath);

                // Skip the old parameter file
                m_jobParams.AddResultFileToSkip(fiParamFile.Name);
                return true;
            }
            catch (Exception ex)
            {
                m_message = "Error in NOMSIPlugin->UpdateParameterFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mCurrentConsoleOutputFile));

                    LogProgress("NOMSI");
                }
                
            }

        }

        #endregion
    }
}
