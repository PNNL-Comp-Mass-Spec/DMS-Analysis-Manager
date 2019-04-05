//*********************************************************************************************************
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

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsMSXMLCreator : EventNotifier
    {
        #region "Classwide variables"

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

        public clsMSXMLCreator(string MSXmlGeneratorAppPath, string WorkDir, string Dataset, short DebugLevel, IJobParams JobParams)
        {
            mMSXmlGeneratorAppPath = MSXmlGeneratorAppPath;
            mWorkDir = WorkDir;
            mDatasetName = Dataset;
            mDebugLevel = DebugLevel;
            mJobParams = JobParams;

            ErrorMessage = string.Empty;
        }

        public bool ConvertMzMLToMzXML()
        {
            // mzXML filename is dataset plus .mzXML
            var strMzXmlFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(strMzXmlFilePath) || File.Exists(strMzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            var strSourceFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);

            var progLoc = mMSXmlGeneratorAppPath;
            if (!File.Exists(progLoc))
            {
                ErrorMessage = "MSXmlGenerator not found; unable to convert .mzML file to .mzXML";
                OnErrorEvent(ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + mDatasetName + " using " + Path.GetFileName(strSourceFilePath));
            }

            // Setup a program runner tool to call MSConvert
            var oProgRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(oProgRunner);

            // Set up command
            var arguments = clsAnalysisToolRunnerBase.PossiblyQuotePath(strSourceFilePath) +
                            " --32 " +
                            "--mzXML " +
                            "-o " + mWorkDir;

            if (mDebugLevel > 0)
            {
                OnStatusEvent(progLoc + " " + arguments);
            }

            oProgRunner.CreateNoWindow = true;
            oProgRunner.CacheStandardOutput = true;
            oProgRunner.EchoOutputToConsole = true;

            oProgRunner.WriteConsoleOutputToFile = false;
            oProgRunner.ConsoleOutputFilePath = string.Empty;
            // Allow the console output filename to be auto-generated

            var dtStartTimeUTC = DateTime.UtcNow;

            if (!oProgRunner.RunProgram(progLoc, arguments, "MSConvert", true))
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
            if (!File.Exists(strMzXmlFilePath))
            {
                ErrorMessage = ".mzXML file was not created by MSConvert";
                OnErrorEvent(ErrorMessage + ": " + strMzXmlFilePath);
                return false;
            }

            if (mDebugLevel >= 1)
            {
                mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, strSourceFilePath, strMzXmlFilePath);
            }

            return true;
        }

        /// <summary>
        /// Generate the mzXML
        /// </summary>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool CreateMZXMLFile()
        {
            // Turn on Centroiding, which will result in faster mzXML file generation time and smaller .mzXML files
            var CentroidMSXML = true;

            var blnSuccess = false;

            // mzXML filename is dataset plus .mzXML
            string strMzXmlFilePath = null;
            strMzXmlFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(strMzXmlFilePath) || File.Exists(strMzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            var eOutputType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML;

            // Instantiate the processing class
            // Note that mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo() by an Analysis Manager plugin using clsAnalysisToolRunnerBase.GetMSXmlGeneratorAppPath()
            var strMSXmlGeneratorExe = Path.GetFileName(mMSXmlGeneratorAppPath);

            if (!File.Exists(mMSXmlGeneratorAppPath))
            {
                ErrorMessage = "MSXmlGenerator not found; unable to create .mzXML file";
                OnErrorEvent(ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + mDatasetName);
            }

            var rawDataType = mJobParams.GetParam("RawDataType");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            if (strMSXmlGeneratorExe.ToLower().Contains("readw"))
            {
                // ReAdW
                // mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()
                mMSXmlGen = new clsMSXMLGenReadW(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName, eRawDataType, eOutputType, CentroidMSXML);

                if (rawDataType != clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                {
                    ErrorMessage = "ReAdW can only be used with .Raw files, not with " + rawDataType;
                    OnErrorEvent(ErrorMessage);
                    return false;
                }
            }
            else if (strMSXmlGeneratorExe.ToLower().Contains("msconvert"))
            {
                // MSConvert

                // Lookup Centroid Settings
                CentroidMSXML = mJobParams.GetJobParameter("CentroidMSXML", true);
                var CentroidPeakCountToRetain = 0;

                // Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
                CentroidPeakCountToRetain = mJobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0);

                if (CentroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    CentroidPeakCountToRetain = mJobParams.GetJobParameter("CentroidPeakCountToRetain",
                        clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                // Look for custom processing arguments
                var CustomMSConvertArguments = mJobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "");

                if (string.IsNullOrWhiteSpace(CustomMSConvertArguments))
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName, eRawDataType, eOutputType, CentroidMSXML,
                        CentroidPeakCountToRetain);
                }
                else
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(mWorkDir, mMSXmlGeneratorAppPath, mDatasetName, eRawDataType, eOutputType,
                        CustomMSConvertArguments);
                }
            }
            else
            {
                ErrorMessage = "Unsupported XmlGenerator: " + strMSXmlGeneratorExe;
                OnErrorEvent(ErrorMessage);
                return false;
            }

            // Register the events in mMSXMLGen
            RegisterMsXmlGenEventHandlers(mMSXmlGen);

            var dtStartTimeUTC = DateTime.UtcNow;

            // Create the file
            blnSuccess = mMSXmlGen.CreateMSXMLFile();

            if (!blnSuccess)
            {
                ErrorMessage = mMSXmlGen.ErrorMessage;
                OnErrorEvent(mMSXmlGen.ErrorMessage);
                return false;
            }
            else if (mMSXmlGen.ErrorMessage.Length > 0)
            {
                OnWarningEvent(mMSXmlGen.ErrorMessage);
            }

            // Validate that the .mzXML file was actually created
            if (!File.Exists(strMzXmlFilePath))
            {
                ErrorMessage = ".mzXML file was not created by " + strMSXmlGeneratorExe;
                OnErrorEvent(ErrorMessage + ": " + strMzXmlFilePath);
                return false;
            }

            if (mDebugLevel >= 1)
            {
                mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, mMSXmlGen.SourceFilePath, strMzXmlFilePath);
            }

            return true;
        }

        // ReSharper disable once UnusedMember.Global
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
