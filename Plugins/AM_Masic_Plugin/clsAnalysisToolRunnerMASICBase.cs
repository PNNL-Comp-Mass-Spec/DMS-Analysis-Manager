//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Base class for performing MASIC analysis
    /// </summary>
    /// <remarks></remarks>
    public abstract class clsAnalysisToolRunnerMASICBase : clsAnalysisToolRunnerBase
    {
        #region "Module variables"

        protected const string SICS_XML_FILE_SUFFIX = "_SICs.xml";

        // Job running status variable
        protected bool m_JobRunning;

        protected string m_ErrorMessage = string.Empty;
        protected string m_ProcessStep = string.Empty;
        protected string m_MASICStatusFileName = string.Empty;
        protected string m_MASICLogFileName = string.Empty;

        #endregion

        #region "Methods"

        protected void ExtractErrorsFromMASICLogFile(string strLogFilePath)
        {
            // Read the most recent MASIC_Log file and look for any lines with the text "Error"

            try
            {
                // Fix the case of the MASIC LogFile
                var ioFileInfo = new FileInfo(strLogFilePath);

                var strLogFileNameCorrectCase = Path.GetFileName(strLogFilePath);

                if (m_DebugLevel >= 1)
                {
                    LogDebug("Checking capitalization of the the MASIC Log File: should be " + strLogFileNameCorrectCase +
                             "; is currently " + ioFileInfo.Name);
                }

                if (ioFileInfo.Name != strLogFileNameCorrectCase && ioFileInfo.DirectoryName != null)
                {
                    // Need to fix the case
                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Fixing capitalization of the MASIC Log File: " + strLogFileNameCorrectCase + " instead of " + ioFileInfo.Name);
                    }
                    ioFileInfo.MoveTo(Path.Combine(ioFileInfo.DirectoryName, strLogFileNameCorrectCase));
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                LogError("Error fixing capitalization of the MASIC Log File at " + strLogFilePath + ": " + ex.Message, ex);
            }

            try
            {
                if (string.IsNullOrEmpty(strLogFilePath))
                {
                    return;
                }

                using (var srInFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intErrorCount = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                            continue;

                        if (strLineIn.ToLower().Contains("error"))
                        {
                            if (intErrorCount == 0)
                            {
                                LogError("Errors found in the MASIC Log File");
                            }

                            LogWarning(" ... " + strLineIn);

                            intErrorCount += 1;
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error reading MASIC Log File at '" + strLogFilePath + "'; " + ex.Message, ex);
            }
        }

        public override CloseOutType RunTool()
        {
            // Call base class for initial setup
            base.RunTool();

            // Store the MASIC version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining MASIC version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Start the job timer
            m_StartTime = DateTime.UtcNow;
            m_message = string.Empty;

            // Make the SIC's
            LogMessage("Calling MASIC to create the SIC files, job " + m_JobNum);
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

            m_progress = 100;
            UpdateStatusFile();

            // Run the cleanup routine from the base class
            var ePostProcessingResult = PerfPostAnalysisTasks("SIC");
            if (ePostProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make the results folder
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        protected CloseOutType StartMASICAndWait(string strInputFilePath, string strOutputFolderPath, string strParameterFilePath)
        {
            // Note that this function is normally called by RunMasic() in the subclass

            var strMASICExePath = string.Empty;

            m_ErrorMessage = string.Empty;
            m_ProcessStep = "NewTask";

            try
            {
                m_MASICStatusFileName = "MasicStatus_" + m_MachName + ".xml";
            }
            catch (Exception)
            {
                m_MASICStatusFileName = "MasicStatus.xml";
            }

            // Make sure the MASIC.Exe file exists
            try
            {
                strMASICExePath = m_mgrParams.GetParam("masicprogloc");
                if (!File.Exists(strMASICExePath))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " + strMASICExePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC .Exe at " + strMASICExePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Call MASIC using the Program Runner class

            // Define the parameters to send to Masic.exe
            var cmdStr = "/I:" + strInputFilePath + " /O:" + strOutputFolderPath + " /P:" + strParameterFilePath + " /Q /SF:" + m_MASICStatusFileName;

            if (m_DebugLevel >= 2)
            {
                // Create a MASIC Log File
                cmdStr += " /L";
                m_MASICLogFileName = "MASIC_Log_Job" + m_JobNum + ".txt";

                cmdStr += ":" + Path.Combine(m_WorkDir, m_MASICLogFileName);
            }
            else
            {
                m_MASICLogFileName = string.Empty;
            }

            if (m_DebugLevel >= 1)
            {
                LogDebug(strMASICExePath + " " + cmdStr);
            }

            var objMasicProgRunner = new PRISM.clsProgRunner
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                Name = "MASIC",
                Program = strMASICExePath,
                Arguments = cmdStr,
                WorkDir = m_WorkDir
            };

            ResetProgRunnerCpuUsage();

            objMasicProgRunner.StartAndMonitorProgram();

            // Wait for the job to complete
            var blnSuccess = WaitForJobToFinish(objMasicProgRunner);

            Thread.Sleep(3000);                // Delay for 3 seconds to make sure program exits

            if (!string.IsNullOrEmpty(m_MASICLogFileName))
            {
                // Read the most recent MASIC_Log file and look for any lines with the text "Error"
                ExtractErrorsFromMASICLogFile(Path.Combine(m_WorkDir, m_MASICLogFileName));
            }

            // Verify MASIC exited due to job completion
            if (!blnSuccess)
            {
                if (m_DebugLevel > 1)
                {
                    LogError("WaitForJobToFinish returned False");
                }

                if (!string.IsNullOrEmpty(m_ErrorMessage))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message: " + m_ErrorMessage);
                    if (string.IsNullOrEmpty(m_message))
                        m_message = m_ErrorMessage;
                }
                else
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message is blank");
                    if (string.IsNullOrEmpty(m_message))
                        m_message = "Unknown error running MASIC";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel > 0)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); m_ProcessStep=" + m_ProcessStep);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected abstract CloseOutType RunMASIC();

        protected abstract CloseOutType DeleteDataFile();

        protected virtual void CalculateNewStatus(string strMasicProgLoc)
        {
            // Calculates status information for progress file
            // Does this by reading the MasicStatus.xml file

            var strProgress = string.Empty;

            try
            {
                var masicExe = new FileInfo(strMasicProgLoc);
                if (masicExe.DirectoryName == null)
                    return;

                var strPath = Path.Combine(masicExe.DirectoryName, m_MASICStatusFileName);

                if (!File.Exists(strPath))
                    return;

                using (var fsInFile = new FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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
                                            m_ProcessStep = objXmlReader.Value;
                                    }
                                    break;
                                case "Progress":
                                    if (!objXmlReader.IsEmptyElement)
                                    {
                                        if (objXmlReader.Read())
                                            strProgress = objXmlReader.Value;
                                    }
                                    break;
                                case "Error":
                                    if (!objXmlReader.IsEmptyElement)
                                    {
                                        if (objXmlReader.Read())
                                            m_ErrorMessage = objXmlReader.Value;
                                    }
                                    break;
                            }
                        }
                    }
                }


                if (string.IsNullOrEmpty(strProgress))
                    return;

                try
                {
                    m_progress = float.Parse(strProgress);
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

        protected virtual CloseOutType PerfPostAnalysisTasks(string ResType)
        {
            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Get rid of raw data file
            var StepResult = DeleteDataFile();
            if (StepResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return StepResult;
            }

            // Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
            var FoundFiles = Directory.GetFiles(m_WorkDir, "*" + SICS_XML_FILE_SUFFIX);

            if (FoundFiles.Length > 0)
            {
                // Setup zipper

                var zipFileName = m_Dataset + "_SICs.zip";

                if (!ZipFile(FoundFiles[0], true, Path.Combine(m_WorkDir, zipFileName)))
                {
                    LogErrorToDatabase("Error zipping " + Path.GetFileName(FoundFiles[0]) + ", job " + m_JobNum);
                    UpdateStatusMessage("Error zipping " + SICS_XML_FILE_SUFFIX + " file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip(SICS_XML_FILE_SUFFIX); // Unzipped, concatenated DTA

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
            var masicExecutablePath = m_mgrParams.GetParam("masicprogloc");
            var success = StoreDotNETToolVersionInfo(masicExecutablePath);

            return success;
        }

        /// <summary>
        /// Validate that required options are defined in the MASIC parameter file
        /// </summary>
        /// <param name="strParameterFilePath"></param>
        /// <remarks></remarks>
        protected bool ValidateParameterFile(string strParameterFilePath)
        {
            if (string.IsNullOrWhiteSpace(strParameterFilePath))
            {
                LogWarning("The MASIC Parameter File path is empty; nothing to validate");
                return true;
            }

            var objSettingsFile = new PRISM.XmlSettingsFileAccessor();

            if (!objSettingsFile.LoadSettings(strParameterFilePath))
            {
                LogError("Error loading parameter file " + strParameterFilePath);
                return false;
            }

            if (!objSettingsFile.SectionPresent("MasicExportOptions"))
            {
                LogWarning("MasicExportOptions section not found in " + strParameterFilePath);
                objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                objSettingsFile.SaveSettings();
                return true;
            }

            bool bad;
            var includeHeaders = objSettingsFile.GetParam("MasicExportOptions", "IncludeHeaders", false, out bad);

            if (!includeHeaders)
            {
                // File needs to be updated
                objSettingsFile.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                objSettingsFile.SaveSettings();
            }

            return true;
        }

        protected bool WaitForJobToFinish(PRISM.clsProgRunner objMasicProgRunner)
        {
            const int MAX_RUNTIME_HOURS = 24;
            const int SECONDS_BETWEEN_UPDATE = 30;

            var blnSICsXMLFileExists = false;
            var dtStartTime = DateTime.UtcNow;
            var blnAbortedProgram = false;

            // Wait for completion
            m_JobRunning = true;

            var dtLastUpdate = DateTime.UtcNow;

            while (m_JobRunning)
            {
                // Wait for 15 seconds
                while (DateTime.UtcNow.Subtract(dtLastUpdate).TotalSeconds < SECONDS_BETWEEN_UPDATE)
                {
                    Thread.Sleep(250);
                }
                dtLastUpdate = DateTime.UtcNow;

                if (objMasicProgRunner.State == PRISM.clsProgRunner.States.NotMonitoring)
                {
                    m_JobRunning = false;
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

            if (m_DebugLevel > 0)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended");
            }

            if (blnAbortedProgram)
            {
                m_ErrorMessage = "Aborted MASIC processing since over " + MAX_RUNTIME_HOURS + " hours have elapsed";
                LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + m_ErrorMessage);
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
            if (Directory.GetFiles(m_WorkDir, "*" + SICS_XML_FILE_SUFFIX).Length > 0)
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
