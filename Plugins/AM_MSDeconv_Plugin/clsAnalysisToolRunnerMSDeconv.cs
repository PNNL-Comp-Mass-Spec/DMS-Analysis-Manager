//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using MSDataFileReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AnalysisManagerMSDeconvPlugIn
{
    /// <summary>
    /// Class for running MSDeconv analysis
    /// </summary>
    public class clsAnalysisToolRunnerMSDeconv : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const string MSDECONV_CONSOLE_OUTPUT = "MSDeconv_ConsoleOutput.txt";
        protected const string MSDECONV_JAR_NAME = "MsDeconvConsole.jar";

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        protected string mMSDeconvVersion;

        protected string mMSDeconvProgLoc;
        protected string mConsoleOutputErrorMsg;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSDeconv tool
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

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMSDeconv.RunTool(): Enter");
                }

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
                // Note that we need to run MSDeconv with a 64-bit version of Java since it prefers to use 2 or more GB of ram
                var JavaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(JavaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Examine the mzXML file to look for large scan gaps (common for data from Agilent IMS TOFs, e.g. AgQTOF05)
                // Possibly generate a new mzXML file with renumbered scans
                var mzXmlValidated = RenumberMzXMLIfRequired();
                if (!mzXmlValidated)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "RenumberMzXMLIfRequired returned false";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MSDeconv program
                mMSDeconvProgLoc = DetermineProgramLocation("MSDeconvProgLoc", MSDECONV_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMSDeconvProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var strOutputFormat = mJobParams.GetParam("MSDeconvOutputFormat");
                string resultsFileName;

                if (string.IsNullOrEmpty(strOutputFormat))
                {
                    strOutputFormat = "msalign";
                }

                switch (strOutputFormat.ToLower())
                {
                    case "mgf":
                        strOutputFormat = "mgf";
                        resultsFileName = mDatasetName + "_msdeconv.mgf";
                        break;
                    case "text":
                        strOutputFormat = "text";
                        resultsFileName = mDatasetName + "_msdeconv.txt";
                        break;
                    case "msalign":
                        strOutputFormat = "msalign";
                        resultsFileName = mDatasetName + "_msdeconv.msalign";
                        break;
                    default:
                        mMessage = "Invalid output format: " + strOutputFormat;
                        LogError(mMessage);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                var processingSuccess = StartMSDeconv(JavaProgLoc, strOutputFormat);

                if (!processingSuccess)
                {
                    LogError("Error running MSDeconv");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSDeconv returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSDeconv failed (but exit code is 0)");
                    }

                }
                else
                {
                    // Make sure the output file was created and is not zero-bytes
                    // If the input .mzXML file only has MS spectra and no MS/MS spectra, the output file will be empty
                    var ioResultsFile = new FileInfo(Path.Combine(mWorkDir, resultsFileName));
                    if (!ioResultsFile.Exists)
                    {
                        var msg = "MSDeconv results file not found";
                        LogError(msg, msg + " (" + resultsFileName + ")");

                        processingSuccess = false;
                    }
                    else if (ioResultsFile.Length == 0)
                    {
                        var msg = "MSDeconv results file is empty; assure that the input .mzXML file has MS/MS spectra";
                        LogError(msg, msg + " (" + resultsFileName + ")");

                        processingSuccess = false;
                    }
                    else
                    {
                        mStatusTools.UpdateAndWrite(mProgress);
                        if (mDebugLevel >= 3)
                        {
                            LogDebug("MSDeconv Search Complete");
                        }
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(mWorkDir, MSDECONV_CONSOLE_OUTPUT));

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                mMessage = "Error in MSDeconvPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        // Example Console output:
        //
        // MS-Deconv 0.8.0.7199 2012-01-16
        // ********* parameters begin **********
        // output file format:    msalign
        // data type:             centroided
        // orignal precursor:     false
        // maximum charge:        30
        // maximum mass:          49000.0
        // m/z error tolerance:   0.02
        // sn ratio:              1.0
        // keep unused peak:      false
        // output multiple mass:  false
        // ********* parameters end   **********
        // Processing spectrum Scan_2...           0% finished.
        // Processing spectrum Scan_3...           0% finished.
        // Processing spectrum Scan_4...           0% finished.
        // Deconvolution finished.
        // Result is in Syne_LI_CID_09092011_msdeconv.msalign
        private readonly Regex reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSDeconv console output file to determine the MSDeconv version and to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                short actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            if (linesRead <= 3)
                            {
                                // Originally the first line was the MS-Deconv version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the MSDeconv version
                                if (string.IsNullOrEmpty(mMSDeconvVersion) && dataLine.ToLower().Contains("ms-deconv"))
                                {
                                    if (mDebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSDeconvVersion))
                                    {
                                        LogDebug("MSDeconv version: " + dataLine);
                                    }

                                    mMSDeconvVersion = string.Copy(dataLine);
                                }
                                else
                                {
                                    if (dataLine.ToLower().Contains("error"))
                                    {
                                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                        {
                                            mConsoleOutputErrorMsg = "Error running MSDeconv:";
                                        }
                                        mConsoleOutputErrorMsg += "; " + dataLine;
                                    }
                                }
                            }
                            else
                            {
                                // Update progress if the line starts with Processing spectrum
                                if (dataLine.StartsWith("Processing spectrum"))
                                {
                                    var oMatch = reExtractPercentFinished.Match(dataLine);
                                    if (oMatch.Success)
                                    {
                                        if (short.TryParse(oMatch.Groups[1].Value, out var intProgress))
                                        {
                                            actualProgress = intProgress;
                                        }
                                    }
                                }
                                else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    if (dataLine.ToLower().StartsWith("error"))
                                    {
                                        mConsoleOutputErrorMsg += "; " + dataLine;
                                    }
                                }
                            }
                        }
                    }
                }

                if (mProgress < actualProgress)
                {
                    mProgress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool RenumberMzXMLIfRequired()
        {
            try
            {
                var mzXmlFileName = mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION;
                var fiMzXmlFile = new FileInfo(Path.Combine(mWorkDir, mzXmlFileName));

                if (!fiMzXmlFile.Exists)
                {
                    mMessage = "mzXML file not found, " + fiMzXmlFile.FullName;
                    return false;
                }

                var reader = new clsMzXMLFileReader();
                reader.OpenFile(fiMzXmlFile.FullName);

                // Read the spectra and examine the scan gaps

                var lstScanGaps = new List<int>();
                var lastScanNumber = 0;

                while (reader.ReadNextSpectrum(out var objSpectrumInfo))
                {
                    if (lastScanNumber > 0)
                    {
                        lstScanGaps.Add(objSpectrumInfo.ScanNumber - lastScanNumber);
                    }

                    lastScanNumber = objSpectrumInfo.ScanNumber;
                }

                reader.CloseFile();

                if (lstScanGaps.Count > 0)
                {
                    // Compute the average scan gap
                    var scanGapSum = lstScanGaps.Sum();
                    var scanGapAverage = scanGapSum / (float)lstScanGaps.Count;

                    if (scanGapAverage >= 2)
                    {
                        // Renumber the .mzXML file
                        // May need to renumber if the scan gap is every greater than one; not sure

                        // Rename the file
                        fiMzXmlFile.MoveTo(Path.Combine(mWorkDir, mDatasetName + "_old" + clsAnalysisResources.DOT_MZXML_EXTENSION));
                        fiMzXmlFile.Refresh();
                        mJobParams.AddResultFileToSkip(fiMzXmlFile.Name);

                        LogMessage(
                            "The mzXML file has an average scan gap of " + scanGapAverage.ToString("0.0") +
                            " scans; will update the file's scan numbers to be 1, 2, 3, etc.");

                        var converter = new clsRenumberMzXMLScans(fiMzXmlFile.FullName);
                        var targetFilePath = Path.Combine(mWorkDir, mzXmlFileName);
                        var blnSuccess = converter.Process(targetFilePath);

                        if (!blnSuccess)
                        {
                            mMessage = converter.ErrorMessage;
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "clsRenumberMzXMLScans returned false while renumbering the scans in the .mzXML file";
                            }

                            return false;
                        }

                        mJobParams.AddResultFileToSkip(targetFilePath);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error renumbering the mzXML file: " + ex.Message;
                LogError("Error in RenumberMzXMLIfRequired", ex);
                return false;
            }
        }

        private bool StartMSDeconv(string JavaProgLoc, string strOutputFormat)
        {
            // Store the MSDeconv version info in the database after the first line is written to file MSDeconv_ConsoleOutput.txt
            mToolVersionWritten = false;
            mMSDeconvVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;

            var blnIncludeMS1Spectra = mJobParams.GetJobParameter("MSDeconvIncludeMS1", false);

            LogMessage("Running MSDeconv");

            // Lookup the amount of memory to reserve for Java; default to 2 GB
            var intJavaMemorySize = mJobParams.GetJobParameter("MSDeconvJavaMemorySize", 2000);
            if (intJavaMemorySize < 512)
                intJavaMemorySize = 512;

            // Set up and execute a program runner to run MSDeconv
            var arguments = " -Xmx" + intJavaMemorySize + "M"+
                            " -jar " + mMSDeconvProgLoc;

            // Define the input file and processing options
            // Note that capitalization matters for the extension; it must be .mzXML
            arguments += " " + mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION;
            arguments += " -o " + strOutputFormat + " -t centroided";

            if (blnIncludeMS1Spectra)
            {
                arguments += " -l";
            }

            LogDebug(JavaProgLoc + " " + arguments);

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSDECONV_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;

            var blnSuccess = mCmdRunner.RunProgram(JavaProgLoc, arguments, "MSDeconv", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSDeconvVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, MSDECONV_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            return blnSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSDeconvVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mMSDeconvProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private readonly Regex reExtractScan = new Regex(@"Processing spectrum Scan_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Reads the console output file and removes the majority of the percent finished messages
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + strConsoleOutputFilePath);
                }

                var strMostRecentProgressLine = string.Empty;
                var strMostRecentProgressLineWritten = string.Empty;

                var strTrimmedFilePath = strConsoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intScanNumberOutputThreshold = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
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
                            if (!clsGlobal.IsMatch(strMostRecentProgressLine, strMostRecentProgressLineWritten))
                            {
                                writer.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(strConsoleOutputFilePath);
                    File.Move(strTrimmedFilePath, strConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + strConsoleOutputFilePath + ") with trimmed version: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        #endregion

        #region "Event Handlers"

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MSDECONV_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSDeconvVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                LogProgress("MSDeconv");
            }
        }

        #endregion
    }
}
