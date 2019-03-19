//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Class for performing MASIC analysis
    /// </summary>
    public abstract class clsAnalysisToolRunnerMASICBase : clsAnalysisToolRunnerBase
    {
        #region "Module variables"

        private const string SICS_XML_FILE_SUFFIX = "_SICs.xml";

        // Job running status variable
        private bool mJobRunning;

        protected string mErrorMessage = string.Empty;

        private string mProcessStep = string.Empty;
        private string mMASICStatusFileName = string.Empty;

        #endregion

        #region "Methods"

        private void ExtractErrorsFromMASICLogFile(FileSystemInfo logFile)
        {
            // Read the most recent MASIC_Log file and look for any lines with the text "Error"

            try
            {
                if (!logFile.Exists)
                    return;

                using (var reader = new StreamReader(new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var errorCount = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (dataLine.ToLower().Contains("error"))
                        {
                            if (errorCount == 0)
                            {
                                LogError("Errors found in the MASIC Log File");
                            }

                            if (errorCount <= 10)
                                LogWarning(" ... " + dataLine);

                            errorCount += 1;
                        }
                    }

                    if (errorCount > 10)
                    {
                        LogWarning(string.Format(" ... {0} total errors", errorCount));
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error reading MASIC Log File at '" + logFile.FullName + "'; " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            // Call base class for initial setup
            base.RunTool();

            // Store the MASIC version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining MASIC version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Start the job timer
            mStartTime = DateTime.UtcNow;
            mMessage = string.Empty;

            // Make the SIC's
            LogMessage("Calling MASIC to create the SIC files, job " + mJob);
            try
            {
                // Note that RunMASIC will populate the File Path variables, then will call
                //  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
                var eProcessingResult = RunMASIC();
                if (eProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eProcessingResult;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mProgress = 100;
            UpdateStatusFile();

            // Run the cleanup routine from the base class
            var ePostProcessingResult = PerfPostAnalysisTasks();
            if (ePostProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make the results folder
            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        protected CloseOutType StartMASICAndWait(string inputFilePath, string outputFolderPath, string parameterFilePath)
        {
            // Note that this function is normally called by RunMasic() in the subclass

            var masicExePath = string.Empty;

            mErrorMessage = string.Empty;
            mProcessStep = "NewTask";

            try
            {
                mMASICStatusFileName = "MasicStatus_" + mMgrName + ".xml";
            }
            catch (Exception)
            {
                mMASICStatusFileName = "MasicStatus.xml";
            }

            // Make sure the MASIC.Exe file exists
            try
            {
                masicExePath = mMgrParams.GetParam("MasicProgLoc");
                if (!File.Exists(masicExePath))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " + masicExePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC .Exe at " + masicExePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Call MASIC using the Program Runner class

            var logFile = new FileInfo(Path.Combine(mWorkDir, "MASIC_Log_Job" + mJob + ".txt"));

            // Define the parameters to send to Masic.exe
            var arguments =
                " /I:" + inputFilePath +
                " /O:" + outputFolderPath +
                " /P:" + parameterFilePath +
                " /Q /SF:" + mMASICStatusFileName +
                " /L:" + PathUtils.PossiblyQuotePath(logFile.FullName);

            if (mDebugLevel >= 1)
            {
                LogDebug(masicExePath + arguments);
            }

            var objMasicProgRunner = new ProgRunner
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                Name = "MASIC",
                Program = masicExePath,
                Arguments = arguments,
                WorkDir = mWorkDir
            };

            ResetProgRunnerCpuUsage();

            objMasicProgRunner.StartAndMonitorProgram();

            // Wait for the job to complete
            var blnSuccess = WaitForJobToFinish(objMasicProgRunner);

            // Delay for 3 seconds to make sure program exits
            clsGlobal.IdleLoop(3);

            // Read the most recent MASIC_Log file and look for any lines with the text "Error"
            ExtractErrorsFromMASICLogFile(logFile);

            // Verify MASIC exited due to job completion
            if (blnSuccess)
            {
                mJobParams.AddResultFileToSkip(logFile.Name);
            }
            else
            {
                if (mDebugLevel > 1)
                {
                    LogError("WaitForJobToFinish returned False");
                }

                if (!string.IsNullOrEmpty(mErrorMessage))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message: " + mErrorMessage);
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = mErrorMessage;
                }
                else
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message is blank");
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "Unknown error running MASIC";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 0)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); mProcessStep=" + mProcessStep);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected abstract CloseOutType RunMASIC();

        protected abstract CloseOutType DeleteDataFile();

        protected virtual void CalculateNewStatus(string masicProgLoc)
        {
            // Calculates status information for progress file
            // Does this by reading the MasicStatus.xml file

            var progress = string.Empty;

            try
            {
                var masicExe = new FileInfo(masicProgLoc);

                if (masicExe.DirectoryName == null)
                    return;

                var statusFile = new FileInfo(Path.Combine(masicExe.DirectoryName, mMASICStatusFileName));

                if (!statusFile.Exists)
                    return;

                using (var fsInFile = new FileStream(statusFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var objXmlReader = new XmlTextReader(fsInFile))
                    {
                        objXmlReader.WhitespaceHandling = WhitespaceHandling.None;

                        while (objXmlReader.Read())
                        {
                            if (objXmlReader.NodeType != XmlNodeType.Element)
                                continue;

                            switch (objXmlReader.Name)
                            {
                                case "ProcessingStep":
                                    if (!objXmlReader.IsEmptyElement)
                                    {
                                        if (objXmlReader.Read())
                                            mProcessStep = objXmlReader.Value;
                                    }
                                    break;
                                case "Progress":
                                    if (!objXmlReader.IsEmptyElement)
                                    {
                                        if (objXmlReader.Read())
                                            progress = objXmlReader.Value;
                                    }
                                    break;
                                case "Error":
                                    if (!objXmlReader.IsEmptyElement)
                                    {
                                        if (objXmlReader.Read())
                                            mErrorMessage = objXmlReader.Value;
                                    }
                                    break;
                            }
                        }
                    }
                }


                if (string.IsNullOrEmpty(progress))
                    return;

                try
                {
                    mProgress = float.Parse(progress);
                }
                catch (Exception)
                {
                    // Ignore errors
                }
            }
            catch (Exception)
            {
                // Ignore errors
            }
        }

        protected virtual CloseOutType PerfPostAnalysisTasks()
        {
            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Get rid of raw data file
            var stepResult = DeleteDataFile();
            if (stepResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return stepResult;
            }

            // Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
            var FoundFiles = Directory.GetFiles(mWorkDir, "*" + SICS_XML_FILE_SUFFIX);

            if (FoundFiles.Length > 0)
            {
                // Setup zipper

                var zipFileName = mDatasetName + "_SICs.zip";

                if (!ZipFile(FoundFiles[0], true, Path.Combine(mWorkDir, zipFileName)))
                {
                    LogErrorToDatabase("Error zipping " + Path.GetFileName(FoundFiles[0]) + ", job " + mJob);
                    UpdateStatusMessage("Error zipping " + SICS_XML_FILE_SUFFIX + " file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip(SICS_XML_FILE_SUFFIX); // Unzipped, concatenated DTA

            // Add the current job data to the summary file
            UpdateSummaryFile();

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var masicExecutablePath = mMgrParams.GetParam("MasicProgLoc");
            var success = StoreDotNETToolVersionInfo(masicExecutablePath);

            return success;
        }

        /// <summary>
        /// Validate that required options are defined in the MASIC parameter file
        /// </summary>
        /// <param name="parameterFilePath"></param>
        /// <remarks></remarks>
        protected bool ValidateParameterFile(string parameterFilePath)
        {
            if (string.IsNullOrWhiteSpace(parameterFilePath))
            {
                LogWarning("The MASIC Parameter File path is empty; nothing to validate");
                return true;
            }

            var objSettingsFile = new XmlSettingsFileAccessor();

            if (!objSettingsFile.LoadSettings(parameterFilePath))
            {
                LogError("Error loading parameter file " + parameterFilePath);
                return false;
            }

            if (!objSettingsFile.SectionPresent("MasicExportOptions"))
            {
                LogWarning("MasicExportOptions section not found in " + parameterFilePath);
                objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                objSettingsFile.SaveSettings();
                return true;
            }

            var includeHeaders = objSettingsFile.GetParam("MasicExportOptions", "IncludeHeaders", false, out _);

            if (!includeHeaders)
            {
                // File needs to be updated
                objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                objSettingsFile.SaveSettings();
            }

            return true;
        }

        private bool WaitForJobToFinish(ProgRunner objMasicProgRunner)
        {
            const int MAX_RUNTIME_HOURS = 24;
            const int SECONDS_BETWEEN_UPDATE = 30;

            var blnSICsXMLFileExists = false;
            var dtStartTime = DateTime.UtcNow;
            var blnAbortedProgram = false;

            // Wait for completion
            mJobRunning = true;

            while (mJobRunning)
            {
                // Wait for 30 seconds
                clsGlobal.IdleLoop(SECONDS_BETWEEN_UPDATE);

                if (objMasicProgRunner.State == ProgRunner.States.NotMonitoring)
                {
                    mJobRunning = false;
                }
                else
                {
                    // Update the status
                    CalculateNewStatus(objMasicProgRunner.Program);
                    UpdateStatusFile();

                    var processID = 0;

                    try
                    {
                        // Note that the call to GetCoreUsage() will take at least 1 second
                        processID = objMasicProgRunner.PID;
                        var coreUsage = clsGlobal.ProcessInfo.GetCoreUsageByProcessID(processID);

                        UpdateProgRunnerCpuUsage(objMasicProgRunner.PID, coreUsage, SECONDS_BETWEEN_UPDATE);
                    }
                    catch (Exception ex)
                    {
                        // Sometimes we get exception "Performance counter not found for processID 4896" if the process ends before we can check its core usage
                        // Log a warning since this is not a fatal error
                        LogWarning("Exception getting core usage for MASIC, process ID " + processID + ": " + ex.Message);
                    }

                    LogProgress("MASIC");
                }

                if (DateTime.UtcNow.Subtract(dtStartTime).TotalHours >= MAX_RUNTIME_HOURS)
                {
                    // Abort processing
                    objMasicProgRunner.StopMonitoringProgram(kill: true);
                    blnAbortedProgram = true;
                }
            }

            if (mDebugLevel > 0)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended");
            }

            if (blnAbortedProgram)
            {
                mErrorMessage = "Aborted MASIC processing since over " + MAX_RUNTIME_HOURS + " hours have elapsed";
                LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + mErrorMessage);
                return false;
            }

            if ((int)objMasicProgRunner.State == 10)
            {
                LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.State = 10");
                return false;
            }

            if (objMasicProgRunner.ExitCode == 0)
                return true;

            LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); objMasicProgRunner.ExitCode is nonzero: " + objMasicProgRunner.ExitCode);

            // See if a _SICs.XML file was created
            if (Directory.GetFiles(mWorkDir, "*" + SICS_XML_FILE_SUFFIX).Length > 0)
            {
                blnSICsXMLFileExists = true;
            }

            if (objMasicProgRunner.ExitCode != 32)
                return false;

            // FindSICPeaksError
            // As long as the _SICs.xml file was created, we can safely ignore this error
            if (blnSICsXMLFileExists)
            {
                LogWarning(
                    "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX +
                    " file found, so ignoring non-zero exit code");
                return true;
            }

            LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX + " file not found");
            return false;

            // Return False for any other exit codes
        }

        #endregion
    }
}
