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
            var cmdStr = " " + rawFilePath + " " + strParamFilePath;

            if (mDebugLevel > 0)
            {
                OnStatusEvent(mDtaToolNameLoc + " " + cmdStr);
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

            var blnSuccess = mCmdRunner.RunProgram(mDtaToolNameLoc, cmdStr, "DeconConsole", true);

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
            var diWorkdir = new DirectoryInfo(Path.Combine(mWorkDir));

            foreach (var fiFile in diWorkdir.GetFiles(mDatasetName + "*BAD_ERROR_log.txt"))
            {
                mErrMsg = "Error running DeconTools; Bad_Error_log file exists";
                OnErrorEvent(mErrMsg + ": " + fiFile.Name);
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

            const int MAX_LOGFINISHED_WAITTIME_SECONDS = 120;
            if (blnFinishedProcessing)
            {
                // The DeconConsole Log File reports that the task is complete
                // If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, send an abort to the CmdRunner

                if (DateTime.Now.Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS)
                {
                    OnWarningEvent("Note: Log file reports finished over " + MAX_LOGFINISHED_WAITTIME_SECONDS + " seconds ago, " +
                                   "but the DeconConsole CmdRunner is still active");

                    mDeconConsoleFinishedDespiteProgRunnerError = true;

                    // Abort processing
                    mCmdRunner.AbortProgramNow();

                    clsGlobal.IdleLoop(3);
                }
            }
        }

        private void ParseDeconToolsLogFile(out bool blnFinishedProcessing, out DateTime dtFinishTime)
        {
            var strScanLine = string.Empty;

            blnFinishedProcessing = false;
            dtFinishTime = DateTime.MinValue;

            try
            {
                string strLogFilePath;

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the _Log.txt file is created inside the .D folder
                        strLogFilePath = Path.Combine(mInputFilePath, mDatasetName) + "_log.txt";
                        break;
                    default:
                        strLogFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) + "_log.txt");
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

                            if (mDebugLevel >= 3)
                            {
                                OnStatusEvent("DeconConsole log file reports 'finished file processing' at " + dtFinishTime);
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
                                mErrMsg = "Error thrown by DeconConsole";

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
                if (mDebugLevel >= 4)
                {
                    OnWarningEvent("Exception in ParseDeconToolsLogFile: " + ex.Message);
                }
            }

            if (!string.IsNullOrWhiteSpace(strScanLine))
            {
                // Parse strScanFrameLine
                // It will look like:
                // Scan= 16500; PercentComplete= 89.2

                var strProgressStats = strScanLine.Split(';');

                for (var i = 0; i <= strProgressStats.Length - 1; i++)
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
