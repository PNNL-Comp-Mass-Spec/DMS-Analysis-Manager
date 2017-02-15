//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;
using MSDataFileReader;

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
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsAnalysisToolRunnerMSDeconv.RunTool(): Enter");
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
                var blnSuccess = RenumberMzXMLIfRequired();
                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "RenumberMzXMLIfRequired returned false";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MSDeconv program
                mMSDeconvProgLoc = DetermineProgramLocation("MSDeconv", "MSDeconvProgLoc", MSDECONV_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMSDeconvProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var strOutputFormat = m_jobParams.GetParam("MSDeconvOutputFormat");
                var resultsFileName = "unknown";

                if (string.IsNullOrEmpty(strOutputFormat))
                {
                    strOutputFormat = "msalign";
                }

                switch (strOutputFormat.ToLower())
                {
                    case "mgf":
                        strOutputFormat = "mgf";
                        resultsFileName = m_Dataset + "_msdeconv.mgf";
                        break;
                    case "text":
                        strOutputFormat = "text";
                        resultsFileName = m_Dataset + "_msdeconv.txt";
                        break;
                    case "msalign":
                        strOutputFormat = "msalign";
                        resultsFileName = m_Dataset + "_msdeconv.msalign";
                        break;
                    default:
                        m_message = "Invalid output format: " + strOutputFormat;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                blnSuccess = StartMSDeconv(JavaProgLoc, strOutputFormat);

                var blnProcessingError = false;

                if (!blnSuccess)
                {
                    string Msg = null;
                    Msg = "Error running MSDeconv";
                    m_message = clsGlobal.AppendToComment(m_message, Msg);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);

                    if (mCmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "MSDeconv returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Call to MSDeconv failed (but exit code is 0)");
                    }

                    blnProcessingError = true;
                }
                else
                {
                    // Make sure the output file was created and is not zero-bytes
                    // If the input .mzXML file only has MS spectra and no MS/MS spectra, then the output file will be empty
                    var ioResultsFile = new FileInfo(Path.Combine(m_WorkDir, resultsFileName));
                    if (!ioResultsFile.Exists)
                    {
                        string Msg = null;
                        Msg = "MSDeconv results file not found";
                        m_message = clsGlobal.AppendToComment(m_message, Msg);

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            Msg + " (" + resultsFileName + ")" + ", job " + m_JobNum);

                        blnProcessingError = true;
                    }
                    else if (ioResultsFile.Length == 0)
                    {
                        string Msg = null;
                        Msg = "MSDeconv results file is empty; assure that the input .mzXML file has MS/MS spectra";
                        m_message = clsGlobal.AppendToComment(m_message, Msg);

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            Msg + " (" + resultsFileName + ")" + ", job " + m_JobNum);

                        blnProcessingError = true;
                    }
                    else
                    {
                        m_StatusTools.UpdateAndWrite(m_progress);
                        if (m_DebugLevel >= 3)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSDeconv Search Complete");
                        }
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                mCmdRunner = null;

                //Make sure objects are released
                Thread.Sleep(500);        // 500 msec delay
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT));

                if (blnProcessingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in MSDeconvPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;    //No failures so everything must have succeeded
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the .mzXML file first)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION));
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
        private Regex reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                int intLinesRead = 0;

                short intActualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            if (intLinesRead <= 3)
                            {
                                // Originally the first line was the MS-Deconv version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the MSDeconv version
                                if (string.IsNullOrEmpty(mMSDeconvVersion) && strLineIn.ToLower().Contains("ms-deconv"))
                                {
                                    if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSDeconvVersion))
                                    {
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                            "MSDeconv version: " + strLineIn);
                                    }

                                    mMSDeconvVersion = string.Copy(strLineIn);
                                }
                                else
                                {
                                    if (strLineIn.ToLower().Contains("error"))
                                    {
                                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                        {
                                            mConsoleOutputErrorMsg = "Error running MSDeconv:";
                                        }
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }
                            else
                            {
                                // Update progress if the line starts with Processing spectrum
                                if (strLineIn.StartsWith("Processing spectrum"))
                                {
                                    var oMatch = reExtractPercentFinished.Match(strLineIn);
                                    if (oMatch.Success)
                                    {
                                        short intProgress;
                                        if (short.TryParse(oMatch.Groups[1].Value, out intProgress))
                                        {
                                            intActualProgress = intProgress;
                                        }
                                    }
                                }
                                else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    if (strLineIn.ToLower().StartsWith("error"))
                                    {
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }
                        }
                    }
                }

                if (m_progress < intActualProgress)
                {
                    m_progress = intActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool RenumberMzXMLIfRequired()
        {
            try
            {
                var mzXmlFileName = m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION;
                var fiMzXmlFile = new FileInfo(Path.Combine(m_WorkDir, mzXmlFileName));

                if (!fiMzXmlFile.Exists)
                {
                    m_message = "mzXML file not found, " + fiMzXmlFile.FullName;
                    return false;
                }

                var reader = new clsMzXMLFileReader();
                reader.OpenFile(fiMzXmlFile.FullName);

                // Read the spectra and examine the scan gaps

                var lstScanGaps = new List<int>();
                clsSpectrumInfo objSpectrumInfo = null;
                var lastScanNumber = 0;

                while (reader.ReadNextSpectrum(out objSpectrumInfo))
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
                    int scanGapSum = lstScanGaps.Sum();
                    var scanGapAverage = scanGapSum / (float)lstScanGaps.Count;

                    if (scanGapAverage >= 2)
                    {
                        // Renumber the .mzXML file
                        // May need to renumber if the scan gap is every greater than one; not sure

                        Thread.Sleep(200);

                        // Rename the file
                        fiMzXmlFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset + "_old" + clsAnalysisResources.DOT_MZXML_EXTENSION));
                        fiMzXmlFile.Refresh();
                        m_jobParams.AddResultFileToSkip(fiMzXmlFile.Name);

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                            "The mzXML file has an average scan gap of " + scanGapAverage.ToString("0.0") +
                            " scans; will update the file's scan numbers to be 1, 2, 3, etc.");

                        var converter = new clsRenumberMzXMLScans(fiMzXmlFile.FullName);
                        var targetFilePath = Path.Combine(m_WorkDir, mzXmlFileName);
                        var blnSuccess = converter.Process(targetFilePath);

                        if (!blnSuccess)
                        {
                            m_message = converter.ErrorMessage;
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "clsRenumberMzXMLScans returned false while renumbering the scans in the .mzXML file";
                            }

                            return false;
                        }

                        m_jobParams.AddResultFileToSkip(targetFilePath);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Error renumbering the mzXML file: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RenumberMzXMLIfRequired", ex);
                return false;
            }
        }

        private bool StartMSDeconv(string JavaProgLoc, string strOutputFormat)
        {
            // Store the MSDeconv version info in the database after the first line is written to file MSDeconv_ConsoleOutput.txt
            mToolVersionWritten = false;
            mMSDeconvVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;

            var blnIncludeMS1Spectra = m_jobParams.GetJobParameter("MSDeconvIncludeMS1", false);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSDeconv");

            // Lookup the amount of memory to reserve for Java; default to 2 GB
            var intJavaMemorySize = m_jobParams.GetJobParameter("MSDeconvJavaMemorySize", 2000);
            if (intJavaMemorySize < 512)
                intJavaMemorySize = 512;

            //Set up and execute a program runner to run MSDeconv
            var CmdStr = " -Xmx" + intJavaMemorySize.ToString() + "M -jar " + mMSDeconvProgLoc;

            // Define the input file and processing options
            // Note that capitalization matters for the extension; it must be .mzXML
            CmdStr += " " + m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION;
            CmdStr += " -o " + strOutputFormat + " -t centroided";

            if (blnIncludeMS1Spectra)
            {
                CmdStr += " -l";
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc + " " + CmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;

            var blnSuccess = mCmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSDeconv", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSDeconvVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
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
            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSDeconvVersion);

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(mMSDeconvProgLoc));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private Regex reExtractScan = new Regex(@"Processing spectrum Scan_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Trimming console output file at " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                bool blnKeepLine = false;

                int intScanNumber = 0;
                string strMostRecentProgressLine = string.Empty;
                string strMostRecentProgressLineWritten = string.Empty;

                int intScanNumberOutputThreshold = 0;

                string strTrimmedFilePath = null;
                strTrimmedFilePath = strConsoleOutputFilePath + ".trimmed";

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    intScanNumberOutputThreshold = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        blnKeepLine = true;

                        var oMatch = reExtractScan.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                    strMostRecentProgressLineWritten = string.Copy(strLineIn);
                                }
                            }
                            strMostRecentProgressLine = string.Copy(strLineIn);
                        }
                        else if (strLineIn.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (string.Compare(strMostRecentProgressLine, strMostRecentProgressLineWritten) != 0)
                            {
                                swOutFile.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            swOutFile.WriteLine(strLineIn);
                        }
                    }
                }

                // Wait 500 msec, then swap the files
                Thread.Sleep(500);

                try
                {
                    File.Delete(strConsoleOutputFilePath);
                    File.Move(strTrimmedFilePath, strConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "Error replacing original console output file (" + strConsoleOutputFilePath + ") with trimmed version: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error trimming console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT));

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
