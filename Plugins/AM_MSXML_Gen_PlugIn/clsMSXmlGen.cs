using AnalysisManagerBase;
using PRISM;
using System;
using System.IO;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public abstract class clsMSXmlGen : EventNotifier
    {
        #region "Constants"

        // Define a maximum runtime of 36 hours
        const int MAX_RUNTIME_SECONDS = 36 * 60 * 60;

        #endregion

        #region "Module Variables"

        protected readonly string mWorkDir;
        protected readonly string mProgramPath;
        private readonly string mDatasetName;
        protected readonly clsAnalysisResources.eRawDataTypeConstants mRawDataType;
        private string mSourceFilePath = string.Empty;
        protected string mOutputFileName = string.Empty;

        private readonly clsAnalysisResources.MSXMLOutputTypeConstants mOutputType;

        protected readonly bool mCentroidMS1;
        protected readonly bool mCentroidMS2;

        // When true, then return an error if the ProgRunner returns a non-zero exit code
        protected bool mUseProgRunnerResultCode;

        private string mErrorMessage;

        public event ProgRunnerStartingEventHandler ProgRunnerStarting;

        public delegate void ProgRunnerStartingEventHandler(string commandLine);

        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        #endregion

        #region "Properties"

        public string ConsoleOutputFileName { get; set; } = string.Empty;

        public string ConsoleOutputSuffix { get; set; } = string.Empty;

        public int DebugLevel { get; set; } = 1;

        public string ErrorMessage
        {
            get
            {
                if (mErrorMessage == null)
                {
                    return string.Empty;
                }

                return mErrorMessage;
            }
        }

        public string OutputFileName => mOutputFileName;

        protected abstract string ProgramName { get; }

        public string SourceFilePath => mSourceFilePath;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="programPath"></param>
        /// <param name="datasetName"></param>
        /// <param name="rawDataType"></param>
        /// <param name="eOutputType"></param>
        /// <param name="centroidMSXML"></param>
        public clsMSXmlGen(string workDir, string programPath, string datasetName, clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, bool centroidMSXML)
        {
            mWorkDir = workDir;
            mProgramPath = programPath;
            mDatasetName = datasetName;
            mRawDataType = rawDataType;
            mOutputType = eOutputType;
            mCentroidMS1 = centroidMSXML;
            mCentroidMS2 = centroidMSXML;

            mErrorMessage = string.Empty;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="programPath"></param>
        /// <param name="datasetName"></param>
        /// <param name="rawDataType"></param>
        /// <param name="eOutputType"></param>
        /// <param name="centroidMS1"></param>
        /// <param name="centroidMS2"></param>
        public clsMSXmlGen(string workDir, string programPath, string datasetName, clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, bool centroidMS1, bool centroidMS2)
        {
            mWorkDir = workDir;
            mProgramPath = programPath;
            mDatasetName = datasetName;
            mRawDataType = rawDataType;
            mOutputType = eOutputType;
            mCentroidMS1 = centroidMS1;
            mCentroidMS2 = centroidMS2;

            mErrorMessage = string.Empty;
        }

        protected abstract string CreateArguments(string msXmlFormat, string rawFilePath);

        /// <summary>
        /// Generate the mzXML or mzML file
        /// </summary>
        /// <returns>True if success; false if a failure</returns>
        /// <remarks></remarks>
        public bool CreateMSXMLFile()
        {
            var msXmlFormat = "mzXML";

            switch (mRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    mSourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    mSourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_D_EXTENSION);
                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    mSourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);
                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    mSourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unsupported raw data type: " + mRawDataType);
            }

            mErrorMessage = string.Empty;

            switch (mOutputType)
            {
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML:
                    msXmlFormat = "mzXML";
                    break;
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzML:
                    msXmlFormat = "mzML";
                    break;
            }

            var cmdRunner = new clsRunDosProgram(Path.GetDirectoryName(mProgramPath));
            cmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // Verify that program file exists
            if (!File.Exists(mProgramPath))
            {
                mErrorMessage = "Cannot find MSXmlGenerator exe program file: " + mProgramPath;
                return false;
            }

            // Set up and execute a program runner to run MS XML executable

            var arguments = CreateArguments(msXmlFormat, mSourceFilePath);

            var blnSuccess = SetupTool();
            if (!blnSuccess)
            {
                if (string.IsNullOrEmpty(mErrorMessage))
                {
                    mErrorMessage = "SetupTool returned false";
                }
                return false;
            }

            ProgRunnerStarting?.Invoke(mProgramPath + arguments);

            if (ConsoleOutputSuffix == null)
                ConsoleOutputSuffix = string.Empty;
            ConsoleOutputFileName = Path.GetFileNameWithoutExtension(mProgramPath) + "_ConsoleOutput" + ConsoleOutputSuffix + ".txt";

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;

            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, ConsoleOutputFileName);

            cmdRunner.WorkDir = mWorkDir;

            var dtStartTime = DateTime.UtcNow;
            blnSuccess = cmdRunner.RunProgram(mProgramPath, arguments, Path.GetFileNameWithoutExtension(mProgramPath), mUseProgRunnerResultCode,
                MAX_RUNTIME_SECONDS);

            if (!string.IsNullOrWhiteSpace(cmdRunner.CachedConsoleErrors))
            {
                // Append the console errors to the log file
                // Note that ProgRunner will have already included them in the ConsoleOutput.txt file

                var consoleError = "Console error: " + cmdRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ");
                if (string.IsNullOrWhiteSpace(mErrorMessage))
                {
                    mErrorMessage = consoleError;
                }
                else
                {
                    OnErrorEvent(consoleError);
                }
                blnSuccess = false;
            }

            if (!blnSuccess)
            {
                if (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds >= MAX_RUNTIME_SECONDS)
                {
                    mErrorMessage = ProgramName + " has run for over " + DateTime.UtcNow.Subtract(dtStartTime).TotalHours.ToString("0") +
                                    " hours and has thus been aborted";
                    return false;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    mErrorMessage = Path.GetFileNameWithoutExtension(mProgramPath) + " returned a non-zero exit code: " +
                                    cmdRunner.ExitCode;
                    return false;
                }

                mErrorMessage = "Call to " + Path.GetFileNameWithoutExtension(mProgramPath) + " failed (but exit code is 0)";
                return true;
            }

            // Make sure the output file was created and is not empty
            var sourceFile = new FileInfo(mSourceFilePath);
            var outputFilePath = Path.Combine(sourceFile.Directory.FullName, GetOutputFileName(msXmlFormat, mSourceFilePath, mRawDataType));

            if (!File.Exists(outputFilePath))
            {
                mErrorMessage = "Output file not found: " + outputFilePath;
                return false;
            }

            // Validate that the output file is complete
            if (!ValidateMsXmlFile(mOutputType, outputFilePath))
            {
                return false;
            }

            return true;
        }

        protected abstract string GetOutputFileName(string msXmlFormat, string rawFilePath, clsAnalysisResources.eRawDataTypeConstants rawDataType);

        public void LogCreationStatsSourceToMsXml(DateTime dtStartTimeUTC, string strSourceFilePath, string strMsXmlFilePath)
        {
            try
            {
                // Save some stats to the log
                double dblSourceFileSizeMB = 0;
                double dblMsXmlSizeMB = 0;

                var strSourceFileExtension = Path.GetExtension(strSourceFilePath);
                var strTargetFileExtension = Path.GetExtension(strMsXmlFilePath);

                var dblTotalMinutes = DateTime.UtcNow.Subtract(dtStartTimeUTC).TotalMinutes;

                var sourceFile = new FileInfo(strSourceFilePath);
                if (sourceFile.Exists)
                {
                    dblSourceFileSizeMB = clsGlobal.BytesToMB(sourceFile.Length);
                }

                var msXmlFile = new FileInfo(strMsXmlFilePath);
                if (msXmlFile.Exists)
                {
                    dblMsXmlSizeMB = clsGlobal.BytesToMB(msXmlFile.Length);
                }

                var strMessage = "MsXml creation time = " + dblTotalMinutes.ToString("0.00") + " minutes";

                if (dblTotalMinutes > 0)
                {
                    strMessage += "; Processing rate = " + (dblSourceFileSizeMB / dblTotalMinutes / 60).ToString("0.0") + " MB/second";
                }

                strMessage += "; " + strSourceFileExtension + " file size = " + dblSourceFileSizeMB.ToString("0.0") + " MB";
                strMessage += "; " + strTargetFileExtension + " file size = " + dblMsXmlSizeMB.ToString("0.0") + " MB";

                if (dblMsXmlSizeMB > 0)
                {
                    strMessage += "; Filesize Ratio = " + (dblMsXmlSizeMB / dblSourceFileSizeMB).ToString("0.00");
                }

                OnStatusEvent(strMessage);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception saving msXML stats", ex);
            }
        }

        protected abstract bool SetupTool();

        private bool ValidateMsXmlFile(clsAnalysisResources.MSXMLOutputTypeConstants eOutputType, string outputFilePath)
        {
            // Open the .mzXML or .mzML file and look for </mzXML> or </indexedmzML> at the end of the file

            try
            {
                var mostRecentLine = string.Empty;

                var fiOutputFile = new FileInfo(outputFilePath);

                if (!fiOutputFile.Exists)
                {
                    mErrorMessage = "Output file not found: " + fiOutputFile.FullName;
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(outputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            mostRecentLine = dataLine;
                        }
                    }
                }

                mostRecentLine = mostRecentLine.Trim();
                if (mostRecentLine.Length > 250)
                {
                    mostRecentLine = mostRecentLine.Substring(0, 250);
                }

                switch (eOutputType)
                {
                    case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML:
                        if (mostRecentLine != "</mzXML>")
                        {
                            mErrorMessage = "File " + fiOutputFile.Name + " is corrupt; it does not end in </mzXML>";
                            if (string.IsNullOrWhiteSpace(mostRecentLine))
                            {
                                OnErrorEvent("mzXML file is corrupt; file is empty or only contains whitespace");
                            }
                            else
                            {
                                OnErrorEvent("mzXML file is corrupt; final line is: " + mostRecentLine);
                            }
                            return false;
                        }

                        break;
                    case clsAnalysisResources.MSXMLOutputTypeConstants.mzML:
                        if (mostRecentLine != "</indexedmzML>")
                        {
                            mErrorMessage = "File " + fiOutputFile.Name + " is corrupt; it does not end in </indexedmzML>";
                            if (string.IsNullOrWhiteSpace(mostRecentLine))
                            {
                                OnErrorEvent("mzML file is corrupt; file is empty or only contains whitespace");
                            }
                            else
                            {
                                OnErrorEvent("mzML file is corrupt; final line is: " + mostRecentLine);
                            }
                            return false;
                        }

                        break;
                    default:
                        mErrorMessage = "Unrecognized output type: " + eOutputType;

                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception validating the .mzXML or .mzML file";
                OnErrorEvent(mErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            mErrorMessage = strMessage;
            OnErrorEvent(strMessage, ex);
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            LoopWaiting?.Invoke();
        }
    }
}
