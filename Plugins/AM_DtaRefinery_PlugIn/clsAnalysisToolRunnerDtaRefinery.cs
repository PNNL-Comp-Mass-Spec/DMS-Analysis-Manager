//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using PRISM.Processes;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// Class for running DTA_Refinery analysis
    /// </summary>
    public class clsAnalysisToolRunnerDtaRefinery : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        private const float PROGRESS_PCT_DTA_REFINERY_RUNNING = 5;

        private bool mXTandemHasFinished;

        private clsRunDosProgram mCmdRunner;
        //--------------------------------------------------------------------------------------------
        //Future section to monitor DTA_Refinery log file for progress determination
        //--------------------------------------------------------------------------------------------
        //Dim WithEvents m_StatFileWatch As FileSystemWatcher
        //Private m_XtSetupFile As String = "default_input.xml"
        //--------------------------------------------------------------------------------------------
        //End future section
        //--------------------------------------------------------------------------------------------

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs DTA_Refinery tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            CloseOutType result;
            string OrgDBName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
            string LocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

            //Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel > 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDtaRefinery.RunTool(): Enter");
            }

            // Store the DTARefinery and X!Tandem version info in the database
            if (!StoreToolVersionInfo())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining DTA Refinery version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running DTA_Refinery");
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.EchoOutputToConsole = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.WriteConsoleOutputToFile = false;
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // verify that program file exists
            // DTARefineryLoc will be something like this: "c:\dms_programs\DTARefinery\dta_refinery.exe"
            string progLoc = m_mgrParams.GetParam("DTARefineryLoc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'DTARefineryLoc' not defined for this manager";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find DTA_Refinery program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            string CmdStr = null;
            CmdStr = Path.Combine(m_WorkDir, m_jobParams.GetParam("DTARefineryXMLFile"));
            CmdStr += " " + Path.Combine(m_WorkDir, m_Dataset + "_dta.txt");
            CmdStr += " " + Path.Combine(LocalOrgDBFolder, OrgDBName);

            // Create a batch file to run the command
            // Capture the console output (including output to the error stream) via redirection symbols:
            //    strExePath CmdStr > ConsoleOutputFile.txt 2>&1

            string strBatchFilePath = Path.Combine(m_WorkDir, "Run_DTARefinery.bat");
            var strConsoleOutputFileName = "DTARefinery_Console_Output.txt";
            m_jobParams.AddResultFileToSkip(Path.GetFileName(strBatchFilePath));

            string strBatchFileCmdLine = progLoc + " " + CmdStr + " > " + strConsoleOutputFileName + " 2>&1";

            // Create the batch file
            using (var swBatchFile = new StreamWriter(new FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strBatchFileCmdLine);
                }

                swBatchFile.WriteLine(strBatchFileCmdLine);
            }

            Thread.Sleep(100);

            m_progress = PROGRESS_PCT_DTA_REFINERY_RUNNING;
            ResetProgRunnerCpuUsage();
            mXTandemHasFinished = false;

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(strBatchFilePath, string.Empty, "DTARefinery", true);

            if (!success)
            {
                Thread.Sleep(500);

                // Open DTARefinery_Console_Output.txt and look for the last line with the text "error"
                var fiConsoleOutputFile = new FileInfo(Path.Combine(m_WorkDir, strConsoleOutputFileName));
                var consoleOutputErrorMessage = string.Empty;

                if (fiConsoleOutputFile.Exists)
                {
                    using (var consoleOutputReader = new StreamReader(new FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!consoleOutputReader.EndOfStream)
                        {
                            var dataLine = consoleOutputReader.ReadLine();
                            if (string.IsNullOrWhiteSpace(dataLine))
                            {
                                continue;
                            }

                            if (dataLine.IndexOf("error", StringComparison.InvariantCultureIgnoreCase) >= 0)
                            {
                                consoleOutputErrorMessage = string.Copy(dataLine);
                            }
                        }
                    }
                }

                m_message = "Error running DTARefinery";
                if (!string.IsNullOrWhiteSpace(consoleOutputErrorMessage))
                {
                    m_message += ": " + consoleOutputErrorMessage;
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);

                ValidateDTARefineryLogFile();

                // Move the source files and any results to the Failed Job folder
                // Useful for debugging DTA_Refinery problems
                CopyFailedResultsToArchiveFolder();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;

            //Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                    "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            }

            //Make sure objects are released
            Thread.Sleep(500);         // 1 second delay
            clsProgRunner.GarbageCollectNow();

            if (!ValidateDTARefineryLogFile())
            {
                result = CloseOutType.CLOSEOUT_NO_DATA;
            }
            else
            {
                var blnPostResultsToDB = true;
                var oMassErrorExtractor = new clsDtaRefLogMassErrorExtractor(m_mgrParams, m_WorkDir, m_DebugLevel, blnPostResultsToDB);
                bool blnSuccess = false;

                int intDatasetID = m_jobParams.GetJobParameter("DatasetID", 0);
                int intJob = 0;
                int.TryParse(m_JobNum, out intJob);

                blnSuccess = oMassErrorExtractor.ParseDTARefineryLogFile(m_Dataset, intDatasetID, intJob);

                if (!blnSuccess)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR,
                        "Error parsing DTA refinery log file to extract mass error stats, job " + m_JobNum);
                }

                //Zip the output file
                result = ZipMainOutputFile();
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging DTA_Refinery problems
                CopyFailedResultsToArchiveFolder();
                return result;
            }

            result = MakeResultsFolder();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            result = MoveResultFiles();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            result = CopyResultsFolderToServer();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //ZipResult
        }

        private void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + "_dta.zip"));
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + "_dta.txt"));
            }
            catch (Exception ex)
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

        /// <summary>
        /// Parses the _DTARefineryLog.txt file to check for a message regarding X!Tandem being finished
        /// </summary>
        /// <returns>True if finished, false if not</returns>
        /// <remarks></remarks>
        private bool IsXTandemFinished()
        {
            try
            {
                var fiSourceFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "DTA_Refinery log file not found by IsXtandenFinished: " + fiSourceFile.Name);
                    return false;
                }

                var tmpFilePath = fiSourceFile.FullName + ".tmp";
                fiSourceFile.CopyTo(tmpFilePath, true);
                m_jobParams.AddResultFileToSkip(tmpFilePath);
                Thread.Sleep(100);

                using (var srSourceFile = new StreamReader(new FileStream(tmpFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srSourceFile.EndOfStream)
                    {
                        var strLineIn = srSourceFile.ReadLine();

                        if (strLineIn.Contains("finished x!tandem"))
                        {
                            return true;
                        }
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                m_message = "Exception in IsXTandemFinished";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            FileInfo ioDtaRefineryFileInfo = new FileInfo(m_mgrParams.GetParam("DTARefineryLoc"));

            if (ioDtaRefineryFileInfo.Exists)
            {
                ioToolFiles.Add(ioDtaRefineryFileInfo);

                string strXTandemModuleLoc = Path.Combine(ioDtaRefineryFileInfo.DirectoryName, "aux_xtandem_module\\tandem_5digit_precision.exe");
                ioToolFiles.Add(new FileInfo(strXTandemModuleLoc));
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "DTARefinery not found: " + ioDtaRefineryFileInfo.FullName);
                return false;
            }

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

        /// <summary>
        /// Parses the _DTARefineryLog.txt file to look for errors
        /// </summary>
        /// <returns>True if no errors, false if a problem</returns>
        /// <remarks></remarks>
        private bool ValidateDTARefineryLogFile()
        {
            try
            {
                var fiSourceFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    m_message = "DtaRefinery Log file not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " (" + fiSourceFile.Name + ")");
                    return false;
                }

                using (var srSourceFile = new StreamReader(new FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srSourceFile.EndOfStream)
                    {
                        var strLineIn = srSourceFile.ReadLine();

                        if (strLineIn.StartsWith("number of spectra identified less than 2"))
                        {
                            if (!srSourceFile.EndOfStream)
                            {
                                strLineIn = srSourceFile.ReadLine();
                                if (strLineIn.StartsWith("stop processing"))
                                {
                                    m_message = "X!Tandem identified fewer than 2 peptides; unable to use DTARefinery with this dataset";
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                                    return false;
                                }
                            }

                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "Encountered message 'number of spectra identified less than 2' but did not find 'stop processing' on the next line; DTARefinery likely did not complete properly");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in ValidateDTARefineryLogFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Zips concatenated XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ZipMainOutputFile()
        {
            FileInfo[] ioFiles = null;
            string strFixedDTAFilePath = null;

            //Do we want to zip these output files?  Yes, we keep them all
            //* _dta_DtaRefineryLog.txt
            //* _dta_SETTINGS.xml
            //* _FIXED_dta.txt
            //* _HIST.png
            //* _HIST.txt
            // * scan number: _scanNum.png
            // * m/z: _mz.png
            // * log10 of ion intensity in the ICR/Orbitrap cell: _logTrappedIonInt.png
            // * total ion current in the ICR/Orbitrap cell: _trappedIonsTIC.png

            //Delete the original DTA files
            try
            {
                var ioWorkDirectory = new DirectoryInfo(m_WorkDir);
                ioFiles = ioWorkDirectory.GetFiles("*_dta.*");

                foreach (var ioFile in ioFiles)
                {
                    if (!ioFile.Name.ToUpper().EndsWith("_FIXED_dta.txt".ToUpper()))
                    {
                        ioFile.Attributes = ioFile.Attributes & (~FileAttributes.ReadOnly);
                        ioFile.Delete();
                    }
                }
            }
            catch (Exception Err)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error deleting _om.omx file, job " + m_JobNum + Err.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                strFixedDTAFilePath = Path.Combine(m_WorkDir, m_Dataset + "_FIXED_dta.txt");
                var ioFile = new FileInfo(strFixedDTAFilePath);

                if (!ioFile.Exists)
                {
                    var Msg = "DTARefinery output file not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ": " + ioFile.Name);
                    m_message = clsGlobal.AppendToComment(m_message, Msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ioFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset + "_dta.txt"));

                try
                {
                    if (!base.ZipFile(ioFile.FullName, true))
                    {
                        var Msg = "Error zipping DTARefinery output file";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ": " + ioFile.FullName);
                        m_message = clsGlobal.AppendToComment(m_message, Msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    string Msg = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error zipping DTARefinery output file: " + ex.Message;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
                    m_message = clsGlobal.AppendToComment(m_message, "Error zipping DTARefinery output file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                string Msg = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error renaming DTARefinery output file: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
                m_message = clsGlobal.AppendToComment(m_message, "Error renaming DTARefinery output file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime dtLastCpuUsageCheck = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const string DTA_REFINERY_PROCESS_NAME = "dta_refinery";
            const string XTANDEM_PROCESS_NAME = "tandem_5digit_precision";
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Push a new core usage value into the queue every 30 seconds
            if (DateTime.UtcNow.Subtract(dtLastCpuUsageCheck).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastCpuUsageCheck = DateTime.UtcNow;

                if (!mXTandemHasFinished)
                {
                    mXTandemHasFinished = IsXTandemFinished();
                }

                if (mXTandemHasFinished)
                {
                    // Determine the CPU usage of DTA_Refinery
                    UpdateCpuUsageByProcessName(DTA_REFINERY_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID);
                }
                else
                {
                    // Determine the CPU usage of X!Tandem
                    UpdateCpuUsageByProcessName(XTANDEM_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID);
                }

                LogProgress("DtaRefinery");
            }
        }

        //--------------------------------------------------------------------------------------------
        //Future section to monitor log file for progress determination
        //--------------------------------------------------------------------------------------------
        //	Private Sub StartFileWatcher(DirToWatch As String, FileToWatch As String)

        //'Watches the DTA_Refinery status file and reports changes

        //'Setup
        //m_StatFileWatch = New FileSystemWatcher
        //With m_StatFileWatch
        //	.BeginInit()
        //	.Path = DirToWatch
        //	.IncludeSubdirectories = False
        //	.Filter = FileToWatch
        //	.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
        //	.EndInit()
        //End With

        //'Start monitoring
        //m_StatFileWatch.EnableRaisingEvents = True

        //	End Sub
        //--------------------------------------------------------------------------------------------
        //End future section
        //--------------------------------------------------------------------------------------------

        #endregion
    }
}
