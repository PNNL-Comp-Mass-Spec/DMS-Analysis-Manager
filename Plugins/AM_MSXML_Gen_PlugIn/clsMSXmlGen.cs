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

        public const string MZXML_FILE_FORMAT = "mzXML";
        public const string MZML_FILE_FORMAT = "mzML";
        public const string MGF_FILE_FORMAT = "mgf";

        #endregion

        #region "Module Variables"

        protected readonly string mWorkDir;
        protected readonly string mProgramPath;
        private readonly string mDatasetName;
        protected readonly clsAnalysisResources.eRawDataTypeConstants mRawDataType;
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

        /// <summary>
        /// Job parameters
        /// </summary>
        protected IJobParams JobParams { get; }

        public string OutputFileName => mOutputFileName;

        protected abstract string ProgramName { get; }

        public string SourceFilePath { get; private set; }

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
        /// <param name="jobParams"></param>
        protected clsMSXmlGen(
            string workDir,
            string programPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            bool centroidMSXML,
            IJobParams jobParams)
        {
            mWorkDir = workDir;
            mProgramPath = programPath;
            mDatasetName = datasetName;
            mRawDataType = rawDataType;
            mOutputType = eOutputType;
            mCentroidMS1 = centroidMSXML;
            mCentroidMS2 = centroidMSXML;
            JobParams = jobParams;

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
        /// <param name="jobParams"></param>
        protected clsMSXmlGen(
            string workDir,
            string programPath,
            string datasetName,
            clsAnalysisResources.eRawDataTypeConstants rawDataType,
            clsAnalysisResources.MSXMLOutputTypeConstants eOutputType,
            bool centroidMS1,
            bool centroidMS2,
            IJobParams jobParams)
        {
            mWorkDir = workDir;
            mProgramPath = programPath;
            mDatasetName = datasetName;
            mRawDataType = rawDataType;
            mOutputType = eOutputType;
            mCentroidMS1 = centroidMS1;
            mCentroidMS2 = centroidMS2;
            JobParams = jobParams;

            mErrorMessage = string.Empty;
        }

        protected abstract string CreateArguments(string msXmlFormat, string rawFilePath);

        /// <summary>
        /// Generate the mzXML, mzML, or .mgf file file
        /// </summary>
        /// <returns>True if success; false if a failure</returns>
        /// <remarks></remarks>
        public bool CreateMSXMLFile()
        {
            string msXmlFormat;

            switch (mRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                    break;

                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_D_EXTENSION);
                    break;

                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);
                    break;

                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);
                    break;

                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    var processingAgilentDotD = JobParams.GetJobParameter("MSXMLGenerator", "ProcessingAgilentDotD", false);

                    if (processingAgilentDotD)
                    {
                        SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_D_EXTENSION);
                    }
                    else
                    {
                        SourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_UIMF_EXTENSION);
                    }

                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(mRawDataType), "Unsupported raw data type: " + mRawDataType);
            }

            mErrorMessage = string.Empty;

            switch (mOutputType)
            {
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML:
                    msXmlFormat = MZXML_FILE_FORMAT;
                    break;
                case clsAnalysisResources.MSXMLOutputTypeConstants.mzML:
                    msXmlFormat = MZML_FILE_FORMAT;
                    break;
                case clsAnalysisResources.MSXMLOutputTypeConstants.mgf:
                    msXmlFormat = MGF_FILE_FORMAT;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(mOutputType), "Unsupported output type: " + mRawDataType);
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

            var arguments = CreateArguments(msXmlFormat, SourceFilePath);

            var success = SetupTool();
            if (!success)
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

            var startTime = DateTime.UtcNow;
            success = cmdRunner.RunProgram(mProgramPath, arguments, Path.GetFileNameWithoutExtension(mProgramPath), mUseProgRunnerResultCode,
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
                success = false;
            }

            if (!success)
            {
                if (DateTime.UtcNow.Subtract(startTime).TotalSeconds >= MAX_RUNTIME_SECONDS)
                {
                    mErrorMessage = ProgramName + " has run for over " + DateTime.UtcNow.Subtract(startTime).TotalHours.ToString("0") +
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
            var sourceFile = new FileInfo(SourceFilePath);
            if (sourceFile.Directory == null)
            {
                mErrorMessage = "Unable to determine the parent directory of " + sourceFile.FullName;
                return false;
            }

            var outputFilePath = Path.Combine(sourceFile.Directory.FullName, GetOutputFileName(msXmlFormat, SourceFilePath, mRawDataType));

            if (!File.Exists(outputFilePath))
            {
                mErrorMessage = "Output file not found: " + outputFilePath;
                return false;
            }


            if (mOutputType == clsAnalysisResources.MSXMLOutputTypeConstants.mgf)
            {
                // Do not try to validate it
                return true;
            }

            // Validate that the output file is complete
            if (!ValidateMsXmlFile(mOutputType, outputFilePath))
            {
                return false;
            }

            return true;
        }

        protected abstract string GetOutputFileName(string msXmlFormat, string rawFilePath, clsAnalysisResources.eRawDataTypeConstants rawDataType);

        public void LogCreationStatsSourceToMsXml(DateTime startTimeUTC, string sourceFilePath, string msXmlFilePath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(sourceFilePath))
                {
                    OnErrorEvent("LogCreationStatsSourceToMsXml: sourceFilePath is an empty string");
                    return;
                }

                if (string.IsNullOrWhiteSpace(msXmlFilePath))
                {
                    OnErrorEvent("LogCreationStatsSourceToMsXml: msXmlFilePath is an empty string");
                    return;
                }

                // Save some stats to the log
                double sourceFileSizeMB = 0;
                double msXmlSizeMB = 0;

                var sourceFileExtension = Path.GetExtension(sourceFilePath);
                var targetFileExtension = Path.GetExtension(msXmlFilePath);

                var totalMinutes = DateTime.UtcNow.Subtract(startTimeUTC).TotalMinutes;

                var sourceFile = new FileInfo(sourceFilePath);
                if (sourceFile.Exists)
                {
                    sourceFileSizeMB = clsGlobal.BytesToMB(sourceFile.Length);
                }

                var msXmlFile = new FileInfo(msXmlFilePath);
                if (msXmlFile.Exists)
                {
                    msXmlSizeMB = clsGlobal.BytesToMB(msXmlFile.Length);
                }

                var message = "MsXml creation time = " + totalMinutes.ToString("0.00") + " minutes";

                if (totalMinutes > 0)
                {
                    message += "; Processing rate = " + (sourceFileSizeMB / totalMinutes / 60).ToString("0.0") + " MB/second";
                }

                message += "; " + sourceFileExtension + " file size = " + sourceFileSizeMB.ToString("0.0") + " MB";
                message += "; " + targetFileExtension + " file size = " + msXmlSizeMB.ToString("0.0") + " MB";

                if (msXmlSizeMB > 0)
                {
                    message += "; FileSize Ratio = " + (msXmlSizeMB / sourceFileSizeMB).ToString("0.00");
                }

                OnStatusEvent(message);
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

                var outputFile = new FileInfo(outputFilePath);

                if (!outputFile.Exists)
                {
                    mErrorMessage = "Output file not found: " + outputFile.FullName;
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
                            mErrorMessage = "File " + outputFile.Name + " is corrupt; it does not end in </mzXML>";
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
                            mErrorMessage = "File " + outputFile.Name + " is corrupt; it does not end in </indexedmzML>";
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
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            mErrorMessage = message;
            OnErrorEvent(message, ex);
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
