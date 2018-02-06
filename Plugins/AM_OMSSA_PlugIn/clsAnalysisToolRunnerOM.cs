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

        private const float PROGRESS_PCT_OMSSA_RUNNING = 5;
        private const float PROGRESS_PCT_PEPTIDEHIT_START = 95;
        private const float PROGRESS_PCT_PEPTIDEHIT_COMPLETE = 99;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs OMSSA tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the OMSSA version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining OMSSA version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            LogMessage("Running OMSSA");

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter");
            }

            // verify that program file exists
            var progLoc = m_mgrParams.GetParam("OMSSAprogloc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'OMSSAprogloc' not defined for this manager";
                LogError("Cannot find OMSSA program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var inputFilename = Path.Combine(m_WorkDir, "OMSSA_Input.xml");

            // Set up and execute a program runner to run OMSSA
            var cmdStr = " -pm " + inputFilename;

            if (m_DebugLevel >= 1)
            {
                LogDebug("Starting OMSSA: " + progLoc + " " + cmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

            var processingSuccess = cmdRunner.RunProgram(progLoc, cmdStr, "OMSSA", true);

            if (!processingSuccess)
            {
                LogError("Error running OMSSA");
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            if (processingSuccess)
            {
                var pepXmlSuccess = ConvertOMSSA2PepXmlFile();
                if (!pepXmlSuccess)
                {
                    processingSuccess = false;
                }
            }

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            Thread.Sleep(500);
            PRISM.clsProgRunner.GarbageCollectNow();

            if (processingSuccess)
            {
                // Zip the output file
                var zipSuccess = ZipMainOutputFile();
                if (!zipSuccess)
                {
                    processingSuccess = false;
                }
            }

            if (!processingSuccess)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Zips OMSSA XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private bool ZipMainOutputFile()
        {
            var strOMSSAResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + "_om.omx");

            var blnSuccess = ZipFile(strOMSSAResultsFilePath, true);
            return blnSuccess;
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

        private bool ConvertOMSSA2PepXmlFile()
        {

            try
            {
                // set up formatdb.exe to reference the organsim DB file (fasta)

                LogMessage("Running OMSSA2PepXml");

                var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile(): Enter");
                }

                // verify that program formatdb.exe file exists
                var progLoc = m_mgrParams.GetParam("omssa2pepprogloc");
                if (!File.Exists(progLoc))
                {
                    if (progLoc.Length == 0)
                        progLoc = "Parameter 'omssa2pepprogloc' not defined for this manager";
                    LogError("Cannot find OMSSA2PepXml program file: " + progLoc);
                    return false;
                }

                var outputFilename = Path.Combine(m_WorkDir, m_Dataset + "_pepxml.xml");
                var inputFilename = Path.Combine(m_WorkDir, m_Dataset + "_om_large.omx");

                // Set up and execute a program runner to run Omssa2PepXml.exe
                // omssa2pepxml.exe -xml -o C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_pepxml.xml C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_omx_large.omx
                var cmdStr = "-xml -o " + outputFilename + " " + inputFilename;

                if (m_DebugLevel >= 1)
                {
                    LogDebug("Starting OMSSA2PepXml: " + progLoc + " " + cmdStr);
                }

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = true;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

                if (!cmdRunner.RunProgram(progLoc, cmdStr, "OMSSA2PepXml", true))
                {
                    LogError("Error running OMSSA2PepXml");
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile, exception, " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(m_mgrParams.GetParam("OMSSAprogloc")),
                new FileInfo(m_mgrParams.GetParam("omssa2pepprogloc"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
