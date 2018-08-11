//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Class for running TopPIC analysis
    /// </summary>
    public class clsAnalysisToolRunnerTopPIC : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const string TopPIC_CONSOLE_OUTPUT = "TopPIC_ConsoleOutput.txt";
        private const string TopPIC_EXE_NAME = "toppic.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string RESULT_TABLE_NAME_SUFFIX = "_TopPIC_ResultTable.txt";
        private const string RESULT_TABLE_NAME_LEGACY = "result_table.txt";

        private const string RESULT_DETAILS_NAME_SUFFIX = "_TopPIC_ResultDetails.txt";
        private const string RESULT_DETAILS_NAME_LEGACY = "result.txt";

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mTopPICVersion;

        private string mTopPICProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mValidatedFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs TopPIC tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerTopPIC.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to TopPIC
                mTopPICProgLoc = DetermineProgramLocation("TopPICProgLoc", TopPIC_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mTopPICProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TopPIC version info in the database after the first line is written to file TopPIC_ConsoleOutput.txt

                mToolVersionWritten = false;
                mTopPICVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;


                // Validate the FASTA file (to remove invalid residues)
                // Create the static mods file
                // Optionally create the dynamic mods file
                if (!CreateInputFiles())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }


                // Process the XML files using TopPIC
                var processingResult = RunTopPIC(mTopPICProgLoc);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsGlobal.IdleLoop(0.5);
                PRISM.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                m_message = "Error in TopPICPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private CloseOutType RunTopPIC(string progLoc)
        {

            LogMessage("Running TopPIC");

            // Set up and execute a program runner to run TopPIC
            // By default uses all cores; limit the number of cores to 4 with "--thread-number 4"

            var cmdStr = " --fixed-mod StaticMods.txt" +
                         " --decoy" +
                         " --error-tolerance 15" +
                         " --max-shift 500" +
                         " --num-shift 2" +
                         " -t EVALUE" +
                         " -v 0.01" +
                         " -T EVALUE" +
                         " -V 0.01" +
                         " " + mValidatedFASTAFilePath +
                         Dataset + clsAnalysisResourcesTopPIC.MSALIGN_FILE_SUFFIX;

            LogDebug(progLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "TopPIC", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopPICVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            CloseOutType eResult;
            if (!processingSuccess)
            {
                LogError("Error running TopPIC");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopPIC returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopPIC failed (but exit code is 0)");
                }

                eResult = CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                // Make sure the output files were created

                var processingError = !ValidateAndCopyResultFiles();

                var strResultTableFilePath = Path.Combine(m_WorkDir, m_Dataset + RESULT_TABLE_NAME_SUFFIX);

                // Make sure the output files are not empty
                if (processingError)
                {
                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    if (ValidateResultTableFile(strResultTableFilePath))
                    {
                        eResult = CloseOutType.CLOSEOUT_SUCCESS;
                    }
                    else
                    {
                        eResult = CloseOutType.CLOSEOUT_NO_DATA;
                    }
                }

                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("TopPIC Search Complete");
                }
            }

            return eResult;
        }

        private bool CreateInputFiles()
        {

            // ToDo
            //if (!CreateModsFile(mTopPICWorkFolderPath))
            //{
            //    return false;
            //}

            // ToDo: Maybe call this
            // CopyFastaCheckResidues();

            return false;

        }

        private bool CopyFastaCheckResidues(string strSourceFilePath, string strTargetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            var intWarningCount = 0;

            try
            {
                var reInvalidResidues = new Regex(@"[BJOUXZ]", RegexOptions.Compiled);

                var oReader = new ProteinFileReader.FastaFileReader();
                if (!oReader.OpenFile(strSourceFilePath))
                {
                    m_message = "Error opening fasta file in CopyFastaCheckResidues";
                    return false;
                }

                using (var swNewFasta = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (oReader.ReadNextProteinEntry())
                    {
                        swNewFasta.WriteLine(oReader.ProteinLineStartChar + oReader.HeaderLine);
                        var strProteinResidues = reInvalidResidues.Replace(oReader.ProteinSequence, "-");

                        if (intWarningCount < 5 && strProteinResidues.GetHashCode() != oReader.ProteinSequence.GetHashCode())
                        {
                            LogWarning("Changed invalid residues to '-' in protein " + oReader.ProteinName);
                            intWarningCount += 1;
                        }

                        var intIndex = 0;
                        var intResidueCount = strProteinResidues.Length;
                        while (intIndex < strProteinResidues.Length)
                        {
                            var intLength = Math.Min(RESIDUES_PER_LINE, intResidueCount - intIndex);
                            swNewFasta.WriteLine(strProteinResidues.Substring(intIndex, intLength));
                            intIndex += RESIDUES_PER_LINE;
                        }
                    }
                }

                oReader.CloseFile();
            }
            catch (Exception ex)
            {
                m_message = "Exception in CopyFastaCheckResidues";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Parse the TopPIC console output file to determine the TopPIC version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output
            //
            // Zero PTM filtering - started.
            // Zero PTM filtering - block 1 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 1 finished.
            // Zero PTM filtering - block 2 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 2 finished.
            // Zero PTM filtering - block 3 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 3 finished.
            // Zero PTM filtering - combining blocks started.
            // Zero PTM filtering - combining blocks finished.
            // Zero PTM filtering - finished.
            // Zero PTM search - started.
            // Zero PTM search - processing 1504 of 1504 spectra.
            // Zero PTM search - finished.
            // One PTM filtering - started.
            // One PTM filtering - block 1 out of 3 started.
            // One PTM filtering - processing 1504 of 1504 spectra.
            // One PTM filtering - block 1 finished.
            // ...
            // One PTM filtering - finished.
            // One PTM search - started.
            // One PTM search - processing 1504 of 1504 spectra.
            // One PTM search - finished.
            // Diagonal PTM filtering - started.
            // Diagonal filtering - block 1 out of 3 started.
            // ...
            // Diagonal filtering - finished.
            // Two PTM search - started.
            // PTM search - processing 1504 of 1504 spectra.
            // Two PTM search - finished.
            // Combining PRSMs - started.
            // Combining PRSMs - finished.
            // E-value computation - started.
            // E-value computation - processing 1504 of 1504 spectra.
            // E-value computation - finished.
            // Finding protein species - started.
            // Finding protein species - finished.
            // Top PRSM selecting - started
            // Top PRSM selecting - finished.
            // FDR computation - started.
            // FDR computation - finished.
            // PRSM selecting by cutoff - started.
            // PRSM selecting by cutoff - finished.
            // Outputting the PRSM result table - started.
            // Outputting the PRSM result table - finished.
            // Generating the PRSM xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // Generating xml files - preprocessing 466 Proteoforms.
            // Generating xml files - processing 466 Proteoforms.
            // Generating xml files - preprocessing 110 Proteins.
            // Generating xml files - processing 110 Proteins.
            // Generating the PRSM xml files - finished.
            // Converting the PRSM xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting the PRSM xml files to html files - finished.
            // Proteoform selecting by cutoff - started.
            // Proteoform selecting by cutoff - finished.
            // Proteoform filtering - started.
            // Proteoform filtering - finished.
            // Outputting the proteoform result table - started.
            // Outputting the proteoform result table - finished.
            // Generating the proteoform xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // ...
            // Generating the proteoform xml files - finished.
            // Converting the proteoform xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting the proteoform xml files to html files - finished.
            // Deleting temporary files - started.
            // Deleting temporary files - finished.
            // TopPIC finished.

            var processingSteps = new SortedList<string, int>
            {
                {"Zero PTM filtering", 0},
                {"Zero PTM search", 10},
                {"One PTM filtering", 15},
                {"One PTM search", 20},
                {"Diagonal filtering", 25},
                {"Two PTM search", 45},
                {"Combining PRSMs", 80},
                {"E-value computation", 88},
                {"Finding protein species", 89},
                {"Generating the PRSM xml files", 90},
                {"Converting the PRSM xml files to html files", 93},
                {"Generating the proteoform xml files", 95},
                {"Converting the proteoform xml files to html files", 98},
                {"Deleting temporary files", 99},
                {"TopPIC finished", 100}
            };

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var actualProgress = 0;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrWhiteSpace(dataLine)) continue;

                        var dataLineLcase = dataLine.ToLower();

                        if (linesRead <= 3)
                        {
                            // The first line has the TopPIC version
                            if (string.IsNullOrEmpty(mTopPICVersion) && dataLineLcase.Contains("toppic"))
                            {
                                if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mTopPICVersion))
                                {
                                    LogDebug("TopPIC version: " + dataLine);
                                }

                                mTopPICVersion = string.Copy(dataLine);
                            }
                        }

                        foreach (var processingStep in processingSteps)
                        {
                            if (!dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase))
                                continue;

                            if (actualProgress < processingStep.Value)
                                actualProgress = processingStep.Value;
                        }

                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg) &&
                            dataLineLcase.Contains("error") && !dataLineLcase.StartsWith("error tolerance:"))
                        {
                            mConsoleOutputErrorMsg += "Error running TopPIC: " + dataLine;
                        }

                    }
                }

                if (m_progress < actualProgress)
                {
                    m_progress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mTopPICVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(mTopPICProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private readonly Regex reExtractScan = new Regex(@"Processing spectrum scan (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Reads the console output file and removes the majority of "procesesing" messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + consoleOutputFilePath);
                }

                var strMostRecentProgressLine = string.Empty;
                var strMostRecentProgressLineWritten = string.Empty;

                var strTrimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intScanNumberOutputThreshold = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            swOutFile.WriteLine(dataLine);
                            continue;
                        }

                        var blnKeepLine = true;

                        var oMatch = reExtractScan.Match(dataLine);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out var intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                    strMostRecentProgressLineWritten = string.Copy(dataLine);
                                }
                            }
                            strMostRecentProgressLine = string.Copy(dataLine);
                        }
                        else if (dataLine.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (string.CompareOrdinal(strMostRecentProgressLine, strMostRecentProgressLineWritten) != 0)
                            {
                                swOutFile.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            swOutFile.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(consoleOutputFilePath);
                    File.Move(strTrimmedFilePath, consoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + consoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        private bool ValidateAndCopyResultFiles()
        {
            var strResultsFolderPath = Path.Combine(m_WorkDir, "msoutput");
            var lstResultsFilesToMove = new List<string>();
            var processingError = false;

            try
            {
                // ToDo
                //lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultTableFileName));
                //lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultDetailsFileName));

                foreach (var resultFilePath in lstResultsFilesToMove)
                {
                    var fiSearchResultFile = new FileInfo(resultFilePath);

                    if (!fiSearchResultFile.Exists)
                    {
                        var msg = "TopPIC results file not found";

                        if (!processingError)
                        {
                            // This is the first missing file; update the base-class comment
                            LogError(msg + ": " + fiSearchResultFile.Name);
                            processingError = true;
                        }

                        LogErrorNoMessageUpdate(msg + ": " + fiSearchResultFile.FullName);
                    }
                    else
                    {
                        // Copy the results file to the work directory
                        var strTargetFileName = string.Copy(fiSearchResultFile.Name);

                        fiSearchResultFile.CopyTo(Path.Combine(m_WorkDir, strTargetFileName), true);
                    }
                }

                // Zip the Html and XML folders
                ZipTopPICResultFolder("html");
                ZipTopPICResultFolder("XML");
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateAndCopyResultFiles", ex);
                return false;
            }

            if (processingError)
            {
                return false;
            }

            return true;
        }

        private bool ValidateResultTableFile(string strSourceFilePath)
        {
            try
            {
                var blnValidFile = false;

                if (!File.Exists(strSourceFilePath))
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogWarning("TopPIC_ResultTable.txt file not found: " + strSourceFilePath);
                    }
                    return false;
                }

                if (m_DebugLevel >= 2)
                {
                    LogMessage("Validating that the TopPIC_ResultTable.txt file is not empty");
                }

                // Open the input file
                using (var reader = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            var strSplitLine = dataLine.Split('\t');

                            if (strSplitLine.Length > 1)
                            {
                                // Look for an integer in the first or second column
                                // Version 0.5 and 0.6 had Prsm_ID in the first column
                                // Version 0.7 moved Prsm_ID to the second column
                                if (int.TryParse(strSplitLine[1], out _) || int.TryParse(strSplitLine[0], out _))
                                {
                                    // Integer found; line is valid
                                    blnValidFile = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (!blnValidFile)
                {
                    LogError("TopPIC_ResultTable.txt file is empty");
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultTableFile", ex);
                return false;
            }

            return true;
        }

        private bool ZipTopPICResultFolder(string strFolderName)
        {
            // ToDo
            throw new NotImplementedException();

            try
            {
                var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_TopPIC_Results_" + strFolderName.ToUpper() + ".zip");

                var strSourceFolderPath = Path.Combine(m_WorkDir, strFolderName);

                // Confirm that the directory has one or more files or subfolders
                var diSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (diSourceFolder.GetFileSystemInfos().Length == 0)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogWarning("TopPIC results folder is empty; nothing to zip: " + strSourceFolderPath);
                    }
                    return false;
                }

                if (m_DebugLevel >= 1)
                {
                    var strLogMessage = "Zipping " + strFolderName.ToUpper() + " folder at " + strSourceFolderPath;

                    if (m_DebugLevel >= 2)
                    {
                        strLogMessage += ": " + strTargetFilePath;
                    }
                    LogMessage(strLogMessage);
                }

                var objZipper = new Ionic.Zip.ZipFile(strTargetFilePath);
                objZipper.AddDirectory(strSourceFolderPath);
                objZipper.Save();
            }
            catch (Exception ex)
            {
                LogError("Exception in ZipTopPICResultFolder", ex);
                return false;
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopPICVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopPIC");
        }

        #endregion
    }
}
