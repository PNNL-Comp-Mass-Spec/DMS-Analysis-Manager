//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/24/2013
//
// Uses DeconConsole.exe to create a .MGF file from a .Raw file or .mzXML file or .mzML file
// Next, converts the .MGF file to a _DTA.txt file
//
// Note that DeconConsole was the re-implementation of the legacy DeconMSn program (using C#)
// DeconConsole was superseded by the C#-based DeconMSn developed by Bryson Gibbons in December 2016
// We replaced the C++ based DeconMSn.exe with the C# based version after showing that results were identical
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    [Obsolete("This class is longer used")]
    public class clsDtaGenDeconConsole : clsDtaGenThermoRaw
    {
        #region "Constants"

        private const int PROGRESS_DECON_CONSOLE_START = 5;
        private const int PROGRESS_MGF_TO_CDTA_START = 85;
        private const int PROGRESS_CDTA_CREATED = 95;

        #endregion

        #region "Structures"

        private struct udtDeconToolsStatusType
        {
            public int CurrentLCScan;       // LC Scan number or IMS Frame Number
            public float PercentComplete;

            public void Clear()
            {
                CurrentLCScan = 0;
                PercentComplete = 0;
            }
        }

        #endregion

        #region "Classwide variables"

        private string mInputFilePath;
        private bool mDeconConsoleExceptionThrown;
        private bool mDeconConsoleFinishedDespiteProgRunnerError;

        #endregion

        private udtDeconToolsStatusType mDeconConsoleStatus;

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            string deconToolsDir = m_MgrParams.GetParam("DeconToolsProgLoc");         // DeconConsole.exe is stored in the DeconTools folder

            var strDTAToolPath = Path.Combine(deconToolsDir, DECON_CONSOLE_FILENAME);

            return strDTAToolPath;
        }

        protected override void MakeDTAFilesThreaded()
        {
            m_Status = ProcessStatus.SF_RUNNING;
            m_ErrMsg = string.Empty;

            m_Progress = PROGRESS_DECON_CONSOLE_START;

            if (!ConvertRawToMGF(m_RawDataType))
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
                return;
            }

            m_Progress = PROGRESS_MGF_TO_CDTA_START;

            if (!ConvertMGFtoDTA())
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
                return;
            }

            m_Progress = PROGRESS_CDTA_CREATED;

            m_Results = ProcessResults.SF_SUCCESS;
            m_Status = ProcessStatus.SF_COMPLETE;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This functon is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertMGFtoDTA()
        {
            bool blnSuccess = false;

            string strRawDataType = m_JobParams.GetJobParameter("RawDataType", "");

            var oMGFConverter = new clsMGFConverter(m_DebugLevel, m_WorkDir)
            {
                IncludeExtraInfoOnParentIonLine = true,
                MinimumIonsPerSpectrum = 0
            };

            RegisterEvents(oMGFConverter);

            var eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);
            blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, m_Dataset);

            if (!blnSuccess)
            {
                m_ErrMsg = oMGFConverter.ErrorMessage;
            }

            m_SpectraFileCount = oMGFConverter.SpectraCountWritten;

            return blnSuccess;
        }

        /// <summary>
        /// Create .mgf file using MSConvert
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <param name="eRawDataType">Raw data file type</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertRawToMGF(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            string RawFilePath = null;

            if (m_DebugLevel > 0)
            {
                OnStatusEvent("Creating .MGF file using DeconConsole");
            }

            m_ErrMsg = string.Empty;

            // Construct the path to the .raw file
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    RawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    break;
                default:
                    m_ErrMsg = "Data file type not supported by the DeconMSn workflow in DeconConsole: " + eRawDataType.ToString();
                    return false;
            }

            m_InstrumentFileName = Path.GetFileName(RawFilePath);
            mInputFilePath = RawFilePath;
            m_JobParams.AddResultFileToSkip(m_InstrumentFileName);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                //Get the maximum number of scans in the file
                m_MaxScanInFile = GetMaxScan(RawFilePath);
            }
            else
            {
                m_MaxScanInFile = DEFAULT_SCAN_STOP;
            }

            //Determine max number of scans to be performed
            m_NumScans = m_MaxScanInFile;

            // Reset the state variables
            mDeconConsoleExceptionThrown = false;
            mDeconConsoleFinishedDespiteProgRunnerError = false;
            mDeconConsoleStatus.Clear();

            var strParamFilePath = m_JobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", string.Empty);

            if (string.IsNullOrEmpty(strParamFilePath))
            {
                m_ErrMsg = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_JobParams, "DeconMSn_ParamFile");
                return false;
            }
            else
            {
                strParamFilePath = Path.Combine(m_WorkDir, strParamFilePath);
            }

            //Set up command
            var cmdStr = " " + RawFilePath + " " + strParamFilePath;

            if (m_DebugLevel > 0)
            {
                OnStatusEvent(m_DtaToolNameLoc + " " + cmdStr);
            }

            //Setup a program runner tool to make the spectra files
            mCmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false   // Disable since the DeconConsole log file has very similar information
            };

            mCmdRunner.ErrorEvent += base.CmdRunner_ErrorEvent;
            mCmdRunner.LoopWaiting += base.CmdRunner_LoopWaiting;

            var blnSuccess = mCmdRunner.RunProgram(m_DtaToolNameLoc, cmdStr, "DeconConsole", true);

            // Parse the DeconTools .Log file to see whether it contains message "Finished file processing"

            DateTime dtFinishTime = DateTime.Now;
            bool blnFinishedProcessing = false;

            ParseDeconToolsLogFile(out blnFinishedProcessing, out dtFinishTime);

            if (mDeconConsoleExceptionThrown)
            {
                blnSuccess = false;
            }

            if (blnFinishedProcessing && !blnSuccess)
            {
                mDeconConsoleFinishedDespiteProgRunnerError = true;
            }

            // Look for file Dataset*BAD_ERROR_log.txt
            // If it exists, an exception occurred
            var diWorkdir = new DirectoryInfo(Path.Combine(m_WorkDir));

            foreach (FileInfo fiFile in diWorkdir.GetFiles(m_Dataset + "*BAD_ERROR_log.txt"))
            {
                m_ErrMsg = "Error running DeconTools; Bad_Error_log file exists";
                OnErrorEvent(m_ErrMsg + ": " + fiFile.Name);
                blnSuccess = false;
                mDeconConsoleFinishedDespiteProgRunnerError = false;
                break;
            }

            if (mDeconConsoleFinishedDespiteProgRunnerError && !mDeconConsoleExceptionThrown)
            {
                // ProgRunner reported an error code
                // However, the log file says things completed successfully
                // We'll trust the log file
                blnSuccess = true;
            }

            if (!blnSuccess)
            {
                // .RunProgram returned False
                LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False");

                if (!string.IsNullOrEmpty(m_ErrMsg))
                {
                    m_ErrMsg = "Error running " + Path.GetFileNameWithoutExtension(m_DtaToolNameLoc);
                }

                return false;
            }

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent(" ... MGF file created using DeconConsole");
            }

            return true;
        }

        protected override void MonitorProgress()
        {
            DateTime dtFinishTime = DateTime.Now;
            bool blnFinishedProcessing = false;

            ParseDeconToolsLogFile(out blnFinishedProcessing, out dtFinishTime);

            if (m_DebugLevel >= 2)
            {
                var strProgressMessage = "Scan=" + mDeconConsoleStatus.CurrentLCScan;
                OnProgressUpdate("... " + strProgressMessage + ", " + m_Progress.ToString("0.0") + "% complete", m_Progress);
            }

            const int MAX_LOGFINISHED_WAITTIME_SECONDS = 120;
            if (blnFinishedProcessing)
            {
                // The DeconConsole Log File reports that the task is complete
                // If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, then send an abort to the CmdRunner

                if (DateTime.Now.Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS)
                {
                    OnWarningEvent("Note: Log file reports finished over " + MAX_LOGFINISHED_WAITTIME_SECONDS + " seconds ago, " +
                                   "but the DeconConsole CmdRunner is still active");

                    mDeconConsoleFinishedDespiteProgRunnerError = true;

                    // Abort processing
                    mCmdRunner.AbortProgramNow();

                    Thread.Sleep(3000);
                }
            }
        }

        private void ParseDeconToolsLogFile(out bool blnFinishedProcessing, out DateTime dtFinishTime)
        {
            string strScanLine = string.Empty;

            blnFinishedProcessing = false;
            dtFinishTime = System.DateTime.MinValue;

            try
            {
                string strLogFilePath = null;

                switch (m_RawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the _Log.txt file is created inside the .D folder
                        strLogFilePath = Path.Combine(mInputFilePath, m_Dataset) + "_log.txt";
                        break;
                    default:
                        strLogFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) + "_log.txt");
                        break;
                }

                //
                if (!File.Exists(strLogFilePath))
                {
                    return;
                }

                using (var srInFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var intCharIndex = strLineIn.IndexOf("finished file processing", StringComparison.InvariantCultureIgnoreCase);

                        if (intCharIndex >= 0)
                        {
                            var blnDateValid = false;
                            if (intCharIndex > 1)
                            {
                                // Parse out the date from strLineIn
                                if (DateTime.TryParse(strLineIn.Substring(0, intCharIndex).Trim(), out dtFinishTime))
                                {
                                    blnDateValid = true;
                                }
                                else
                                {
                                    // Unable to parse out the date
                                    OnErrorEvent("Unable to parse date from string '" + strLineIn.Substring(0, intCharIndex).Trim() +
                                                 "'; will use file modification date as the processing finish time");
                                }
                            }

                            if (!blnDateValid)
                            {
                                var fiFileInfo = new FileInfo(strLogFilePath);
                                dtFinishTime = fiFileInfo.LastWriteTime;
                            }

                            if (m_DebugLevel >= 3)
                            {
                                OnStatusEvent("DeconConsole log file reports 'finished file processing' at " + dtFinishTime.ToString());
                            }

                            //'If intWorkFlowStep < TOTAL_WORKFLOW_STEPS Then
                            //'	intWorkFlowStep += 1
                            //'End If

                            blnFinishedProcessing = true;
                        }

                        if (intCharIndex < 0)
                        {
                            intCharIndex = strLineIn.IndexOf("DeconTools.Backend.dll", StringComparison.Ordinal);
                            if (intCharIndex > 0)
                            {
                                // DeconConsole reports "Finished file processing" at the end of each step in the workflow
                                // Reset blnFinishedProcessing back to false
                                blnFinishedProcessing = false;
                            }
                        }

                        if (intCharIndex < 0)
                        {
                            intCharIndex = strLineIn.ToLower().IndexOf("scan/frame", StringComparison.Ordinal);
                            if (intCharIndex > 0)
                            {
                                strScanLine = strLineIn.Substring(intCharIndex);
                            }
                        }

                        if (intCharIndex < 0)
                        {
                            intCharIndex = strLineIn.ToLower().IndexOf("scan=", StringComparison.Ordinal);
                            if (intCharIndex > 0)
                            {
                                strScanLine = strLineIn.Substring(intCharIndex);
                            }
                        }

                        if (intCharIndex < 0)
                        {
                            intCharIndex = strLineIn.IndexOf("ERROR THROWN", StringComparison.Ordinal);
                            if (intCharIndex >= 0)
                            {
                                // An exception was reported in the log file; treat this as a fatal error
                                m_ErrMsg = "Error thrown by DeconConsole";

                                OnErrorEvent("DeconConsole reports " + strLineIn.Substring(intCharIndex));
                                mDeconConsoleExceptionThrown = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 4)
                {
                    OnWarningEvent("Exception in ParseDeconToolsLogFile: " + ex.Message);
                }
            }

            if (!string.IsNullOrWhiteSpace(strScanLine))
            {
                // Parse strScanFrameLine
                // It will look like:
                // Scan= 16500; PercentComplete= 89.2

                string[] strProgressStats = null;

                strProgressStats = strScanLine.Split(';');

                for (int i = 0; i <= strProgressStats.Length - 1; i++)
                {
                    var kvStat = ParseKeyValue(strProgressStats[i]);
                    if (!string.IsNullOrWhiteSpace(kvStat.Key))
                    {
                        switch (kvStat.Key)
                        {
                            case "Scan":
                                int.TryParse(kvStat.Value, out mDeconConsoleStatus.CurrentLCScan);
                                break;
                            case "Scan/Frame":
                                int.TryParse(kvStat.Value, out mDeconConsoleStatus.CurrentLCScan);
                                break;
                            case "PercentComplete":
                                float.TryParse(kvStat.Value, out mDeconConsoleStatus.PercentComplete);
                                break;
                        }
                    }
                }

                m_Progress = PROGRESS_DECON_CONSOLE_START + mDeconConsoleStatus.PercentComplete * (PROGRESS_MGF_TO_CDTA_START - PROGRESS_DECON_CONSOLE_START) / 100f;
            }
        }

        /// <summary>
        /// Looks for an equals sign in strData
        /// Returns a KeyValuePair object with the text before the equals sign and the text after the equals sign
        /// </summary>
        /// <param name="strData"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private KeyValuePair<string, string> ParseKeyValue(string strData)
        {
            int intCharIndex = 0;
            intCharIndex = strData.IndexOf('=');

            if (intCharIndex > 0)
            {
                try
                {
                    return new KeyValuePair<string, string>(strData.Substring(0, intCharIndex).Trim(), strData.Substring(intCharIndex + 1).Trim());
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }

            return new KeyValuePair<string, string>(string.Empty, string.Empty);
        }
    }
}
