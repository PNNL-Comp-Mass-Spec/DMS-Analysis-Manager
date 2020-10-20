﻿//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// This class is intended to be instantiated by other Analysis Manager plugins
// For example, see AM_MSGF_PlugIn
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.IO;

// ReSharper disable UnusedMember.Global
namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// This class is used by plugins to create a .mzML or .mzXML file for a dataset
    /// </summary>
    public class clsMSXMLCreator : EventNotifier
    {
        // Ignore Spelling: Centroiding

        #region "Class wide variables"

        private readonly string mMSXmlGeneratorAppPath;
        private readonly IJobParams mJobParams;
        private readonly string mWorkDir;
        private string mDatasetName;

        private readonly short mDebugLevel;

        private clsMSXmlGen mMSXmlGen;

        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        #endregion

        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="msXmlGeneratorAppPath"></param>
        /// <param name="workDir"></param>
        /// <param name="dataset"></param>
        /// <param name="debugLevel"></param>
        /// <param name="jobParams"></param>
        public clsMSXMLCreator(string msXmlGeneratorAppPath, string workDir, string dataset, short debugLevel, IJobParams jobParams)
        {
            mMSXmlGeneratorAppPath = msXmlGeneratorAppPath;
            mWorkDir = workDir;
            mDatasetName = dataset;
            mDebugLevel = debugLevel;
            mJobParams = jobParams;

            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Convert a .mzML file to .mzXML
        /// </summary>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>This is used by class clsMSGFRunner in the MSGF Plugin</remarks>
        public bool ConvertMzMLToMzXML()
        {
            // mzXML filename is dataset plus .mzXML
            var mzXmlFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(mzXmlFilePath) || File.Exists(mzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            var sourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);

            var progLoc = mMSXmlGeneratorAppPath;
            if (!File.Exists(progLoc))
            {
                ErrorMessage = "MSXmlGenerator not found; unable to convert .mzML file to .mzXML";
                OnErrorEvent(ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + mDatasetName + " using " + Path.GetFileName(sourceFilePath));
            }

            // Setup a program runner tool to call MSConvert
            var progRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(progRunner);

            // Set up command
            var arguments = clsAnalysisToolRunnerBase.PossiblyQuotePath(sourceFilePath) +
                            " --32 " +
                            "--mzXML " +
                            "-o " + mWorkDir;

            if (mDebugLevel > 0)
            {
                OnStatusEvent(progLoc + " " + arguments);
            }

            progRunner.CreateNoWindow = true;
            progRunner.CacheStandardOutput = true;
            progRunner.EchoOutputToConsole = true;

            progRunner.WriteConsoleOutputToFile = false;
            progRunner.ConsoleOutputFilePath = string.Empty;
            // Allow the console output filename to be auto-generated

            var startTimeUTC = DateTime.UtcNow;

            if (!progRunner.RunProgram(progLoc, arguments, "MSConvert", true))
            {
                // .RunProgram returned False
                ErrorMessage = "Error running " + Path.GetFileNameWithoutExtension(progLoc) + " to convert the .mzML file to a .mzXML file";
                OnErrorEvent(ErrorMessage);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent(" ... mzXML file created");
            }

            // Validate that the .mzXML file was actually created
            if (!File.Exists(mzXmlFilePath))
            {
                ErrorMessage = ".mzXML file was not created by MSConvert";
                OnErrorEvent(ErrorMessage + ": " + mzXmlFilePath);
                return false;
            }

            if (mDebugLevel >= 1)
            {
                mMSXmlGen.LogCreationStatsSourceToMsXml(startTimeUTC, sourceFilePath, mzXmlFilePath);
            }

            return true;
        }

        /// <summary>
        /// Generate the mzXML
        /// </summary>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>This is used by class clsMSGFRunner in the MSGF Plugin</remarks>
        public bool CreateMZXMLFile()
        {
            // Turn on Centroiding, which will result in faster mzXML file generation time and smaller .mzXML files
            var centroidMSXML = true;

            // mzXML filename is dataset plus .mzXML
            var mzXmlFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(mzXmlFilePath) || File.Exists(mzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            var outputType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML;

            // Instantiate the processing class
            // Note that mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo() by an Analysis Manager plugin using clsAnalysisToolRunnerBase.GetMSXmlGeneratorAppPath()
            var msXmlGeneratorExe = Path.GetFileName(mMSXmlGeneratorAppPath);

            if (!File.Exists(mMSXmlGeneratorAppPath))
            {
                ErrorMessage = "MSXmlGenerator not found; unable to create .mzXML file";
                OnErrorEvent(ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (string.IsNullOrEmpty(msXmlGeneratorExe))
            {
                ErrorMessage = "Path.GetFileName returned an empty string for MSXmlGenerator path " + mMSXmlGeneratorAppPath;
                OnErrorEvent(ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + mDatasetName);
            }

            var rawDataType = mJobParams.GetParam("RawDataType");
            var rawDataTypeEnum = clsAnalysisResources.GetRawDataType(rawDataType);

            if (msXmlGeneratorExe.IndexOf("readw", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // ReAdW
                // mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                mMSXmlGen = new clsMSXMLGenReadW(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                                                 rawDataTypeEnum, outputType,
                                                 centroidMSXML, mJobParams);

                if (rawDataType != clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                {
                    ErrorMessage = "ReAdW can only be used with .Raw files, not with " + rawDataType;
                    OnErrorEvent(ErrorMessage);
                    return false;
                }
            }
            else if (msXmlGeneratorExe.IndexOf("msconvert", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // MSConvert

                // Lookup Centroid Settings
                centroidMSXML = mJobParams.GetJobParameter("CentroidMSXML", true);

                // Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
                var centroidPeakCountToRetain = mJobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = mJobParams.GetJobParameter("CentroidPeakCountToRetain",
                                                                           clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                // Look for custom processing arguments
                var customMSConvertArguments = mJobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "");

                if (string.IsNullOrWhiteSpace(customMSConvertArguments))
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                                                         rawDataTypeEnum, outputType,
                                                         centroidMSXML, centroidPeakCountToRetain, mJobParams);
                }
                else
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName,
                                                         rawDataTypeEnum, outputType,
                                                         customMSConvertArguments, mJobParams);
                }
            }
            else
            {
                ErrorMessage = "Unsupported XmlGenerator: " + msXmlGeneratorExe;
                OnErrorEvent(ErrorMessage);
                return false;
            }

            // Register the events in mMSXMLGen
            RegisterMsXmlGenEventHandlers(mMSXmlGen);

            var startTimeUTC = DateTime.UtcNow;

            // Create the file
            var success = mMSXmlGen.CreateMSXMLFile();

            if (!success)
            {
                ErrorMessage = mMSXmlGen.ErrorMessage;
                OnErrorEvent(mMSXmlGen.ErrorMessage);
                return false;
            }

            if (mMSXmlGen.ErrorMessage.Length > 0)
            {
                OnWarningEvent(mMSXmlGen.ErrorMessage);
            }

            // Validate that the .mzXML file was actually created
            if (!File.Exists(mzXmlFilePath))
            {
                ErrorMessage = ".mzXML file was not created by " + msXmlGeneratorExe;
                OnErrorEvent(ErrorMessage + ": " + mzXmlFilePath);
                return false;
            }

            if (mDebugLevel >= 1)
            {
                mMSXmlGen.LogCreationStatsSourceToMsXml(startTimeUTC, mMSXmlGen.SourceFilePath, mzXmlFilePath);
            }

            return true;
        }

        /// <summary>
        /// Update the current dataset name
        /// </summary>
        /// <param name="datasetName"></param>
        /// <remarks>Used by clsAnalysisToolRunnerPRIDEConverter.vb and clsAnalysisToolRunnerRepoPkgr.cs</remarks>
        public void UpdateDatasetName(string datasetName)
        {
            mDatasetName = datasetName;
        }

        #region "Event Handlers"

        private void RegisterMsXmlGenEventHandlers(clsMSXmlGen msXmlGen)
        {
            RegisterEvents(msXmlGen);
            msXmlGen.LoopWaiting += MSXmlGenReadW_LoopWaiting;
            msXmlGen.ProgRunnerStarting += MSXmlGenReadW_ProgRunnerStarting;
        }

        /// <summary>
        /// Event handler for MSXmlGenReadW.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void MSXmlGenReadW_LoopWaiting()
        {
            LoopWaiting?.Invoke();
        }

        /// <summary>
        /// Event handler for mMSXmlGen.ProgRunnerStarting event
        /// </summary>
        /// <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
        /// <remarks></remarks>
        private void MSXmlGenReadW_ProgRunnerStarting(string CommandLine)
        {
            if (mDebugLevel >= 1)
            {
                OnStatusEvent(CommandLine);
            }
        }

        #endregion

    }
}
