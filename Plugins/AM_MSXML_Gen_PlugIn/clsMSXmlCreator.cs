//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// This class is intended to be instantiated by other Analysis Manager plugins
// For example, see AM_MSGF_PlugIn
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsMSXMLCreator : clsEventNotifier
    {
        #region "Classwide variables"

        private readonly string mMSXmlGeneratorAppPath;
        private readonly IJobParams m_jobParams;
        private readonly string m_WorkDir;
        private string m_Dataset;

        private readonly short m_DebugLevel;

        private string m_ErrorMessage;

        private clsMSXmlGen withEventsField_mMSXmlGen;

        private clsMSXmlGen mMSXmlGen
        {
            get { return withEventsField_mMSXmlGen; }
            set
            {
                if (withEventsField_mMSXmlGen != null)
                {
                    withEventsField_mMSXmlGen.LoopWaiting -= MSXmlGenReadW_LoopWaiting;
                    withEventsField_mMSXmlGen.ProgRunnerStarting -= mMSXmlGenReadW_ProgRunnerStarting;
                }
                withEventsField_mMSXmlGen = value;
                if (withEventsField_mMSXmlGen != null)
                {
                    withEventsField_mMSXmlGen.LoopWaiting += MSXmlGenReadW_LoopWaiting;
                    withEventsField_mMSXmlGen.ProgRunnerStarting += mMSXmlGenReadW_ProgRunnerStarting;
                }
            }
        }

        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        #endregion

        public string ErrorMessage
        {
            get { return m_ErrorMessage; }
        }

        public clsMSXMLCreator(string MSXmlGeneratorAppPath, string WorkDir, string Dataset, short DebugLevel, IJobParams JobParams)
        {
            mMSXmlGeneratorAppPath = MSXmlGeneratorAppPath;
            m_WorkDir = WorkDir;
            m_Dataset = Dataset;
            m_DebugLevel = DebugLevel;
            m_jobParams = JobParams;

            m_ErrorMessage = string.Empty;
        }

        public bool ConvertMzMLToMzXML()
        {
            string ProgLoc = null;
            string CmdStr = null;

            string strSourceFilePath = null;

            // mzXML filename is dataset plus .mzXML
            string strMzXmlFilePath = null;
            strMzXmlFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(strMzXmlFilePath) || File.Exists(strMzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            strSourceFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            ProgLoc = mMSXmlGeneratorAppPath;
            if (!File.Exists(ProgLoc))
            {
                m_ErrorMessage = "MSXmlGenerator not found; unable to convert .mzML file to .mzXML";
                OnErrorEvent(m_ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + m_Dataset + " using " + Path.GetFileName(strSourceFilePath));
            }

            //Setup a program runner tool to call MSConvert
            var oProgRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(oProgRunner);

            //Set up command
            CmdStr = " " + clsAnalysisToolRunnerBase.PossiblyQuotePath(strSourceFilePath) + " --32 --mzXML -o " + m_WorkDir;

            if (m_DebugLevel > 0)
            {
                OnStatusEvent(ProgLoc + " " + CmdStr);
            }

            oProgRunner.CreateNoWindow = true;
            oProgRunner.CacheStandardOutput = true;
            oProgRunner.EchoOutputToConsole = true;

            oProgRunner.WriteConsoleOutputToFile = false;
            oProgRunner.ConsoleOutputFilePath = string.Empty;
            // Allow the console output filename to be auto-generated

            var dtStartTimeUTC = DateTime.UtcNow;

            if (!oProgRunner.RunProgram(ProgLoc, CmdStr, "MSConvert", true))
            {
                // .RunProgram returned False
                m_ErrorMessage = "Error running " + Path.GetFileNameWithoutExtension(ProgLoc) + " to convert the .mzML file to a .mzXML file";
                OnErrorEvent(m_ErrorMessage);
                return false;
            }

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent(" ... mzXML file created");
            }

            // Validate that the .mzXML file was actually created
            if (!File.Exists(strMzXmlFilePath))
            {
                m_ErrorMessage = ".mzXML file was not created by MSConvert";
                OnErrorEvent(m_ErrorMessage + ": " + strMzXmlFilePath);
                return false;
            }

            if (m_DebugLevel >= 1)
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

            bool blnSuccess = false;

            // mzXML filename is dataset plus .mzXML
            string strMzXmlFilePath = null;
            strMzXmlFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(strMzXmlFilePath) || File.Exists(strMzXmlFilePath + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
            {
                // File already exists; nothing to do
                return true;
            }

            var eOutputType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML;

            // Instantiate the processing class
            // Note that mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo() by an Analysis Manager plugin using clsAnalysisToolRunnerBase.GetMSXmlGeneratorAppPath()
            string strMSXmlGeneratorExe = null;
            strMSXmlGeneratorExe = Path.GetFileName(mMSXmlGeneratorAppPath);

            if (!File.Exists(mMSXmlGeneratorAppPath))
            {
                m_ErrorMessage = "MSXmlGenerator not found; unable to create .mzXML file";
                OnErrorEvent(m_ErrorMessage + ": " + mMSXmlGeneratorAppPath);
                return false;
            }

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent("Creating the .mzXML file for " + m_Dataset);
            }

            string rawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            if (strMSXmlGeneratorExe.ToLower().Contains("readw"))
            {
                // ReAdW
                // mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

                mMSXmlGen = new clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, eOutputType, CentroidMSXML);
                RegisterEvents(mMSXmlGen);

                if (rawDataType != clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES)
                {
                    m_ErrorMessage = "ReAdW can only be used with .Raw files, not with " + rawDataType;
                    OnErrorEvent(m_ErrorMessage);
                    return false;
                }
            }
            else if (strMSXmlGeneratorExe.ToLower().Contains("msconvert"))
            {
                // MSConvert

                // Lookup Centroid Settings
                CentroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", true);
                int CentroidPeakCountToRetain = 0;

                // Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
                CentroidPeakCountToRetain = m_jobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0);

                if (CentroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    CentroidPeakCountToRetain = m_jobParams.GetJobParameter("CentroidPeakCountToRetain",
                        clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                // Look for custom processing arguments
                var CustomMSConvertArguments = m_jobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "");

                if (string.IsNullOrWhiteSpace(CustomMSConvertArguments))
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, eOutputType, CentroidMSXML,
                        CentroidPeakCountToRetain);
                }
                else
                {
                    mMSXmlGen = new clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, eOutputType,
                        CustomMSConvertArguments);
                }
            }
            else
            {
                m_ErrorMessage = "Unsupported XmlGenerator: " + strMSXmlGeneratorExe;
                OnErrorEvent(m_ErrorMessage);
                return false;
            }

            var dtStartTimeUTC = DateTime.UtcNow;

            // Create the file
            blnSuccess = mMSXmlGen.CreateMSXMLFile();

            if (!blnSuccess)
            {
                m_ErrorMessage = mMSXmlGen.ErrorMessage;
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
                m_ErrorMessage = ".mzXML file was not created by " + strMSXmlGeneratorExe;
                OnErrorEvent(m_ErrorMessage + ": " + strMzXmlFilePath);
                return false;
            }

            if (m_DebugLevel >= 1)
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
            m_Dataset = datasetName;
        }

        #region "Event Handlers"

        /// <summary>
        /// Event handler for MSXmlGenReadW.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void MSXmlGenReadW_LoopWaiting()
        {
            if (LoopWaiting != null)
            {
                LoopWaiting();
            }
        }

        /// <summary>
        /// Event handler for mMSXmlGen.ProgRunnerStarting event
        /// </summary>
        /// <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
        /// <remarks></remarks>
        private void mMSXmlGenReadW_ProgRunnerStarting(string CommandLine)
        {
            if (m_DebugLevel >= 1)
            {
                OnStatusEvent(CommandLine);
            }
        }

        #endregion

        #region "clsEventNotifier events"

        protected void RegisterEvents(clsEventNotifier oProcessingClass)
        {
            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void StatusEventHandler(string statusMessage)
        {
            OnStatusEvent(statusMessage);
        }

        private void ErrorEventHandler(string strMessage, Exception ex)
        {
            OnErrorEvent(strMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            OnWarningEvent(warningMessage);
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            OnProgressUpdate(progressMessage, percentComplete);
        }

        #endregion
    }
}
