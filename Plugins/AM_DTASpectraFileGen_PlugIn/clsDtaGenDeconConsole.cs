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

        private struct DeconToolsStatusType
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

        private DeconToolsStatusType mDeconConsoleStatus;

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            var deconToolsDir = mMgrParams.GetParam("DeconToolsProgLoc");         // DeconConsole.exe is stored in the DeconTools folder

            var strDTAToolPath = Path.Combine(deconToolsDir, DECON_CONSOLE_FILENAME);

            return strDTAToolPath;
        }

        protected override void MakeDTAFilesThreaded()
        {
            mStatus = ProcessStatus.SF_RUNNING;
            mErrMsg = string.Empty;

            mProgress = PROGRESS_DECON_CONSOLE_START;

            if (!ConvertRawToMGF(mRawDataType))
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
                return;
            }

            mProgress = PROGRESS_MGF_TO_CDTA_START;

            if (!ConvertMGFtoDTA())
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
                return;
            }

            mProgress = PROGRESS_CDTA_CREATED;

            mResults = ProcessResults.SF_SUCCESS;
            mStatus = ProcessStatus.SF_COMPLETE;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertMGFtoDTA()
        {
            var strRawDataType = mJobParams.GetJobParameter("RawDataType", "");

            var oMGFConverter = new clsMGFConverter(mDebugLevel, mWorkDir)
            {
                IncludeExtraInfoOnParentIonLine = true,
                MinimumIonsPerSpectrum = 0
            };

            RegisterEvents(oMGFConverter);

            var eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);
            var blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, mDatasetName);

            if (!blnSuccess)
            {
                mErrMsg = oMGFConverter.ErrorMessage;
            }

            mSpectraFileCount = oMGFConverter.SpectraCountWritten;

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
            string rawFilePath;

            if (mDebugLevel > 0)
            {
                OnStatusEvent("Creating .MGF file using DeconConsole");
            }

            mErrMsg = string.Empty;

            // Construct the path to the .raw file
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                    break;
                default:
                    mErrMsg = "Data file type not supported by the DeconMSn workflow in DeconConsole: " + eRawDataType;
                    return false;
            }

            mInstrumentFileName = Path.GetFileName(rawFilePath);
            mInputFilePath = rawFilePath;
            mJobParams.AddResultFileToSkip(mInstrumentFileName);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                // Get the maximum number of scans in the file
                mMaxScanInFile = GetMaxScan(rawFilePath);
            }
            else
            {
                mMaxScanInFile = DEFAULT_SCAN_STOP;
            }

            // Determine max number of scans to be performed
            mNumScans = mMaxScanInFile;

            // Reset the state variables
            mDeconConsoleExceptionThrown = false;
            mDeconConsoleFinishedDespiteProgRunnerError = false;
            mDeconConsoleStatus.Clear();

            var strParamFilePath = mJobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", string.Empty);

            if (string.IsNullOrEmpty(strParamFilePath))
            {
                mErrMsg = clsAnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "DeconMSn_ParamFile");
                return false;
            }

            strParamFilePath = Path.Combine(mWorkDir, strParamFilePath);

            // Set up command
            var arguments = " " + rawFilePath +
                            " " + strParamFilePath;

            if (mDebugLevel > 0)
            {
                OnStatusEvent(mDtaToolNameLoc + " " + arguments);
            }

            // Setup a program runner tool to make the spectra files
            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false   // Disable since the DeconConsole log file has very similar information
            };

            mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            var blnSuccess = mCmdRunner.RunProgram(mDtaToolNameLoc, arguments, "DeconConsole", true);

            // Parse the DeconTools .Log file to see whether it contains message "Finished file processing"

            ParseDeconToolsLogFile(out var blnFinishedProcessing, out _);

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
            var workDirInfo = new DirectoryInfo(Path.Combine(mWorkDir));

            foreach (var badErrorLogFile in workDirInfo.GetFiles(mDatasetName + "*BAD_ERROR_log.txt"))
            {
                mErrMsg = "Error running DeconTools; Bad_Error_log file exists";
                OnErrorEvent(mErrMsg + ": " + badErrorLogFile.Name);
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
                LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(mDtaToolNameLoc), "m_RunProgTool.RunProgram returned False");

                if (!string.IsNullOrEmpty(mErrMsg))
                {
                    mErrMsg = "Error running " + Path.GetFileNameWithoutExtension(mDtaToolNameLoc);
                }

                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent(" ... MGF file created using DeconConsole");
            }

            return true;
        }

        protected override void MonitorProgress()
        {
            ParseDeconToolsLogFile(out var blnFinishedProcessing, out var dtFinishTime);

            if (mDebugLevel >= 2)
            {
                var strProgressMessage = "Scan=" + mDeconConsoleStatus.CurrentLCScan;
                OnProgressUpdate("... " + strProgressMessage + ", " + mProgress.ToString("0.0") + "% complete", mProgress);
            }

            const int MAX_LOG_FINISHED_WAIT_TIME_SECONDS = 120;
            if (blnFinishedProcessing)
            {
                // The DeconConsole Log File reports that the task is complete
                // If it finished over MAX_LOG_FINISHED_WAIT_TIME_SECONDS seconds ago, send an abort to the CmdRunner

                if (DateTime.Now.Subtract(dtFinishTime).TotalSeconds >= MAX_LOG_FINISHED_WAIT_TIME_SECONDS)
                {
                    OnWarningEvent("Note: Log file reports finished over " + MAX_LOG_FINISHED_WAIT_TIME_SECONDS + " seconds ago, " +
                                   "but the DeconConsole CmdRunner is still active");

                    mDeconConsoleFinishedDespiteProgRunnerError = true;

                    // Abort processing
                    mCmdRunner.AbortProgramNow();

                    clsGlobal.IdleLoop(3);
                }
            }
        }

        private void ParseDeconToolsLogFile(out bool finishedProcessing, out DateTime finishTime)
        {
            var scanLine = string.Empty;

            finishedProcessing = false;
            finishTime = DateTime.MinValue;

            try
            {
                string logFilePath;

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the _Log.txt file is created inside the .D folder
                        logFilePath = Path.Combine(mInputFilePath, mDatasetName) + "_log.txt";
                        break;
                    default:
                        logFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) + "_log.txt");
                        break;
                }

                //
                if (!File.Exists(logFilePath))
                {
                    return;
                }

                using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var charIndex = dataLine.IndexOf("finished file processing", StringComparison.InvariantCultureIgnoreCase);

                        if (charIndex >= 0)
                        {
                            var dateValid = false;
                            if (charIndex > 1)
                            {
                                // Parse out the date from strLineIn
                                if (DateTime.TryParse(dataLine.Substring(0, charIndex).Trim(), out finishTime))
                                {
                                    dateValid = true;
                                }
                                else
                                {
                                    // Unable to parse out the date
                                    OnErrorEvent("Unable to parse date from string '" + dataLine.Substring(0, charIndex).Trim() +
                                                 "'; will use file modification date as the processing finish time");
                                }
                            }

                            if (!dateValid)
                            {
                                var logFile = new FileInfo(logFilePath);
                                finishTime = logFile.LastWriteTime;
                            }

                            if (mDebugLevel >= 3)
                            {
                                OnStatusEvent("DeconConsole log file reports 'finished file processing' at " + finishTime);
                            }

                            //'If intWorkFlowStep < TOTAL_WORKFLOW_STEPS Then
                            //'	intWorkFlowStep += 1
                            //'End If

                            finishedProcessing = true;
                        }

                        if (charIndex < 0)
                        {
                            charIndex = dataLine.IndexOf("DeconTools.Backend.dll", StringComparison.Ordinal);
                            if (charIndex > 0)
                            {
                                // DeconConsole reports "Finished file processing" at the end of each step in the workflow
                                // Reset finishedProcessing back to false
                                finishedProcessing = false;
                            }
                        }

                        if (charIndex < 0)
                        {
                            charIndex = dataLine.IndexOf("scan/frame", StringComparison.OrdinalIgnoreCase);
                            if (charIndex > 0)
                            {
                                scanLine = dataLine.Substring(charIndex);
                            }
                        }

                        if (charIndex < 0)
                        {
                            charIndex = dataLine.IndexOf("scan=", StringComparison.OrdinalIgnoreCase);
                            if (charIndex > 0)
                            {
                                scanLine = dataLine.Substring(charIndex);
                            }
                        }

                        if (charIndex < 0)
                        {
                            charIndex = dataLine.IndexOf("ERROR THROWN", StringComparison.Ordinal);
                            if (charIndex >= 0)
                            {
                                // An exception was reported in the log file; treat this as a fatal error
                                mErrMsg = "Error thrown by DeconConsole";

                                OnErrorEvent("DeconConsole reports " + dataLine.Substring(charIndex));
                                mDeconConsoleExceptionThrown = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 4)
                {
                    OnWarningEvent("Exception in ParseDeconToolsLogFile: " + ex.Message);
                }
            }

            if (!string.IsNullOrWhiteSpace(scanLine))
            {
                // Parse strScanFrameLine
                // It will look like:
                // Scan= 16500; PercentComplete= 89.2

                var progressStats = scanLine.Split(';');

                for (var i = 0; i <= progressStats.Length - 1; i++)
                {
                    var kvStat = ParseKeyValue(progressStats[i]);
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

                mProgress = PROGRESS_DECON_CONSOLE_START + mDeconConsoleStatus.PercentComplete * (PROGRESS_MGF_TO_CDTA_START - PROGRESS_DECON_CONSOLE_START) / 100f;
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
            var intCharIndex = strData.IndexOf('=');

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
