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

namespace AnalysisManagerOMSSAPlugIn
{
    /// <summary>
    /// Class for running OMSSA analysis
    /// </summary>
    public class clsAnalysisToolRunnerOM : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const float PROGRESS_PCT_OMSSA_RUNNING = 5;
        protected const float PROGRESS_PCT_PEPTIDEHIT_START = 95;
        protected const float PROGRESS_PCT_PEPTIDEHIT_COMPLETE = 99;

        //--------------------------------------------------------------------------------------------
        //Future section to monitor OMSSA log file for progress determination
        //--------------------------------------------------------------------------------------------
        //Dim WithEvents m_StatFileWatch As FileSystemWatcher
        //Protected m_XtSetupFile As String = "default_input.xml"
        //--------------------------------------------------------------------------------------------
        //End future section
        //--------------------------------------------------------------------------------------------

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs OMSSA tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string CmdStr = null;
            CloseOutType result;

            bool blnProcessingError = false;

            // Set this to success for now
            var eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            //Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the OMSSA version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError(
                    "Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining OMSSA version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            LogMessage("Running OMSSA");

            var cmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug(
                    "clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter");
            }

            // verify that program file exists
            string progLoc = m_mgrParams.GetParam("OMSSAprogloc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'OMSSAprogloc' not defined for this manager";
                LogError("Cannot find OMSSA program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //--------------------------------------------------------------------------------------------
            //Future section to monitor OMSSA log file for progress determination
            //--------------------------------------------------------------------------------------------
            //'Get the OMSSA log file name for a File Watcher to monitor
            //Dim OMSSALogFileName As String = GetOMSSALogFileName(Path.Combine(m_WorkDir, m_OMSSASetupFile))
            //If OMSSALogFileName = "" Then
            //	m_logger.PostEntry("Error getting OMSSA log file name", ILogger.logMsgType.logError, True)
            //	Return CloseOutType.CLOSEOUT_FAILED
            //End If

            //'Setup and start a File Watcher to monitor the OMSSA log file
            //StartFileWatcher(m_workdir, OMSSALogFileName)
            //--------------------------------------------------------------------------------------------
            //End future section
            //--------------------------------------------------------------------------------------------

            string inputFilename = Path.Combine(m_WorkDir, "OMSSA_Input.xml");
            //Set up and execute a program runner to run OMSSA
            CmdStr = " -pm " + inputFilename;

            if (m_DebugLevel >= 1)
            {
                LogDebug("Starting OMSSA: " + progLoc + " " + CmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

            if (!cmdRunner.RunProgram(progLoc, CmdStr, "OMSSA", true))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running OMSSA, job " + m_JobNum);
                blnProcessingError = true;
            }

            //--------------------------------------------------------------------------------------------
            //Future section to monitor OMSSA log file for progress determination
            //--------------------------------------------------------------------------------------------
            //'Turn off file watcher
            //m_StatFileWatch.EnableRaisingEvents = False
            //--------------------------------------------------------------------------------------------
            //End future section
            //--------------------------------------------------------------------------------------------

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;

            if (blnProcessingError)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            if (!blnProcessingError)
            {
                if (!ConvertOMSSA2PepXmlFile())
                {
                    blnProcessingError = true;
                }
            }

            //Add the current job data to the summary file
            UpdateSummaryFile();

            //Make sure objects are released
            Thread.Sleep(500);        // 500 msec delay
            PRISM.clsProgRunner.GarbageCollectNow();

            if (!blnProcessingError)
            {
                //Zip the output file
                result = ZipMainOutputFile();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    blnProcessingError = true;
                }
            }

            result = MakeResultsFolder();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            result = MoveResultFiles();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder";
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            if (blnProcessingError | eReturnCode == CloseOutType.CLOSEOUT_FAILED)
            {
                // Try to save whatever files were moved into the results folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            result = CopyResultsFolderToServer();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            //If we get to here, everything worked so exit happily
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Zips OMSSA XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ZipMainOutputFile()
        {
            //Zip the output file
            string strOMSSAResultsFilePath = null;
            bool blnSuccess = false;

            strOMSSAResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + "_om.omx");

            blnSuccess = base.ZipFile(strOMSSAResultsFilePath, true);
            if (!blnSuccess)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_OMSSA_RUNNING);

            LogProgress("OMSSA");
        }

        protected bool ConvertOMSSA2PepXmlFile()
        {
            string CmdStr = null;
            var result = true;

            try
            {
                // set up formatdb.exe to reference the organsim DB file (fasta)

                LogMessage("Running OMSSA2PepXml");

                var cmdRunner = new clsRunDosProgram(m_WorkDir);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (m_DebugLevel > 4)
                {
                    LogDebug(
                        "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile(): Enter");
                }

                // verify that program formatdb.exe file exists
                string progLoc = m_mgrParams.GetParam("omssa2pepprogloc");
                if (!File.Exists(progLoc))
                {
                    if (progLoc.Length == 0)
                        progLoc = "Parameter 'omssa2pepprogloc' not defined for this manager";
                    LogError(
                        "Cannot find OMSSA2PepXml program file: " + progLoc);
                    return false;
                }

                string outputFilename = Path.Combine(m_WorkDir, m_Dataset + "_pepxml.xml");
                string inputFilename = Path.Combine(m_WorkDir, m_Dataset + "_om_large.omx");

                //Set up and execute a program runner to run Omssa2PepXml.exe
                //omssa2pepxml.exe -xml -o C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_pepxml.xml C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_omx_large.omx
                CmdStr = "-xml -o " + outputFilename + " " + inputFilename;

                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        "Starting OMSSA2PepXml: " + progLoc + " " + CmdStr);
                }

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = true;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

                if (!cmdRunner.RunProgram(progLoc, CmdStr, "OMSSA2PepXml", true))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running OMSSA2PepXml, job " + m_JobNum);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError(
                    "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile, exception, " + ex.Message);
            }

            return result;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(m_mgrParams.GetParam("OMSSAprogloc")));
            ioToolFiles.Add(new FileInfo(m_mgrParams.GetParam("omssa2pepprogloc")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        //--------------------------------------------------------------------------------------------
        //Future section to monitor OMSSA log file for progress determination
        //--------------------------------------------------------------------------------------------
        //	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

        //'Watches the OMSSA status file and reports changes

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
