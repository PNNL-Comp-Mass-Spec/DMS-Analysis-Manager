//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerOMSSAPlugIn
{
    /// <summary>
    /// Class for running OMSSA analysis
    /// </summary>
    public class AnalysisToolRunnerOM : AnalysisToolRunnerBase
    {
        // Ignore Spelling: omx

        #region "Module Variables"

        private const float PROGRESS_PCT_OMSSA_RUNNING = 5;
        private const float PROGRESS_PCT_PEPTIDE_HIT_START = 95;
        private const float PROGRESS_PCT_PEPTIDE_HIT_COMPLETE = 99;

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
                mMessage = "Error determining OMSSA version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            LogMessage("Running OMSSA");

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerOM.OperateAnalysisTool(): Enter");
            }

            // verify that program file exists
            var progLoc = mMgrParams.GetParam("OMSSAprogloc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'OMSSAprogloc' not defined for this manager";
                LogError("Cannot find OMSSA program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var inputFilename = Path.Combine(mWorkDir, "OMSSA_Input.xml");

            // Set up and execute a program runner to run OMSSA
            var arguments = " -pm " + inputFilename;

            if (mDebugLevel >= 1)
            {
                LogDebug("Starting OMSSA: " + progLoc + " " + arguments);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

            var processingSuccess = cmdRunner.RunProgram(progLoc, arguments, "OMSSA", true);

            if (!processingSuccess)
            {
                LogError("Error running OMSSA");
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

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
            PRISM.ProgRunner.GarbageCollectNow();

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
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Zips OMSSA XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private bool ZipMainOutputFile()
        {
            var strOMSSAResultsFilePath = Path.Combine(mWorkDir, mDatasetName + "_om.omx");

            var blnSuccess = ZipFile(strOMSSAResultsFilePath, true);
            return blnSuccess;
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_OMSSA_RUNNING);

            LogProgress("OMSSA");
        }

        private bool ConvertOMSSA2PepXmlFile()
        {
            try
            {
                // set up formatdb.exe to reference the organism DB file (fasta)

                LogMessage("Running OMSSA2PepXml");

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile(): Enter");
                }

                // verify that program formatdb.exe file exists
                var progLoc = mMgrParams.GetParam("omssa2pepprogloc");
                if (!File.Exists(progLoc))
                {
                    if (progLoc.Length == 0)
                        progLoc = "Parameter 'omssa2pepprogloc' not defined for this manager";
                    LogError("Cannot find OMSSA2PepXml program file: " + progLoc);
                    return false;
                }

                var outputFilename = Path.Combine(mWorkDir, mDatasetName + "_pepxml.xml");
                var inputFilename = Path.Combine(mWorkDir, mDatasetName + "_om_large.omx");

                // Set up and execute a program runner to run Omssa2PepXml.exe
                // omssa2pepxml.exe -xml -o C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_pepxml.xml C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_omx_large.omx
                var arguments = "-xml" +
                                " -o " + outputFilename +
                                " " + inputFilename;

                if (mDebugLevel >= 1)
                {
                    LogDebug("Starting OMSSA2PepXml: " + progLoc + " " + arguments);
                }

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = true;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(progLoc) + "_ConsoleOutput.txt");

                if (!cmdRunner.RunProgram(progLoc, arguments, "OMSSA2PepXml", true))
                {
                    LogError("Error running OMSSA2PepXml");
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile, exception, " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(mMgrParams.GetParam("OMSSAprogloc")),
                new(mMgrParams.GetParam("omssa2pepprogloc"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles);
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
