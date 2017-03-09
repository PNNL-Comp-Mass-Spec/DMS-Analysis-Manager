//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;

using AnalysisManagerBase;
using AnalysisManagerMsXmlGenPlugIn;
using PHRPReader;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using MSGFResultsSummarizer;
using PRISM;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// Primary class for running MSGF
    /// </summary>
    public class clsMSGFRunner : clsAnalysisToolRunnerBase
    {
        #region "Constants and enums"

        private const float PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD = 2;
        private const float PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED = 3;
        private const float PROGRESS_PCT_MSXML_GEN_RUNNING = 6;
        private const float PROGRESS_PCT_MZXML_CREATED = 10;
        private const float PROGRESS_PCT_MSGF_START = PROGRESS_PCT_MZXML_CREATED;
        private const float PROGRESS_PCT_MSGF_COMPLETE = 95;
        private const float PROGRESS_PCT_MSGF_POST_PROCESSING = 97;

        public const string MSGF_RESULT_COLUMN_SpectrumFile = "#SpectrumFile";
        public const string MSGF_RESULT_COLUMN_Title = "Title";
        public const string MSGF_RESULT_COLUMN_ScanNumber = "Scan#";
        public const string MSGF_RESULT_COLUMN_Annotation = "Annotation";
        public const string MSGF_RESULT_COLUMN_Charge = "Charge";
        public const string MSGF_RESULT_COLUMN_Protein_First = "Protein_First";
        public const string MSGF_RESULT_COLUMN_Result_ID = "Result_ID";
        public const string MSGF_RESULT_COLUMN_SpecProb = "SpecProb";
        public const string MSGF_RESULT_COLUMN_Data_Source = "Data_Source";
        public const string MSGF_RESULT_COLUMN_Collision_Mode = "Collision_Mode";

        public const string MSGF_PHRP_DATA_SOURCE_SYN = "Syn";
        public const string MSGF_PHRP_DATA_SOURCE_FHT = "FHT";

        public const int MSGF_SEGMENT_ENTRY_COUNT = 25000;

        // If the final segment is less than 5% of MSGF_SEGMENT_ENTRY_COUNT then combine the data with the previous segment
        public const float MSGF_SEGMENT_OVERFLOW_MARGIN = 0.05f;

        private const string MSGF_CONSOLE_OUTPUT = "MSGF_ConsoleOutput.txt";
        private const string MSGF_JAR_NAME = "MSGF.jar";
        private const string MSGFDB_JAR_NAME = "MSGFDB.jar";

        [Obsolete("Old, unsupported tool")]
        private const string MODa_JAR_NAME = "moda.jar";

        [Obsolete("Old, unsupported tool")]
        private const string MODPlus_JAR_NAME = "modp_pnnl.jar";

        private struct udtSegmentFileInfoType
        {
            // Segment number
            public int Segment;
            // Full path to the file
            public string FilePath;
            // Number of entries in this segment
            public int Entries;
        }

        #endregion

        #region "Module variables"

        private bool mETDMode;

        private string mMSGFInputFilePath = string.Empty;
        private string mMSGFResultsFilePath = string.Empty;
        private string mCurrentMSGFResultsFilePath = string.Empty;

        private int mMSGFInputFileLineCount;
        private int mMSGFLineCountPreviousSegments;

        private bool mProcessingMSGFDBCollisionModeData;
        private int mCollisionModeIteration;

        private bool mKeepMSGFInputFiles;

        private bool mToolVersionWritten;
        private string mMSGFVersion = string.Empty;
        private string mMSGFProgLoc = string.Empty;

        private string mMSXmlGeneratorAppPath = string.Empty;

        private clsMSXMLCreator mMSXmlCreator;

        private bool mUsingMSGFDB = true;
        private string mMSGFDBVersion = "Unknown";

        private string mJavaProgLoc = string.Empty;

        private string mConsoleOutputErrorMsg;

        private clsMSGFInputCreator mMSGFInputCreator;

        private clsRunDosProgram mMSGFRunner;

        private int mMSGFInputCreatorErrorCount;
        private int mMSGFInputCreatorWarningCount;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSGF
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            // Set this to success for now
            var eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            //Call base class for initial setup
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", false);

            // Determine the raw data type
            var eRawDataType = clsAnalysisResources.GetRawDataType(m_jobParams.GetParam("RawDataType"));

            // Resolve eResultType
            var eResultType = clsPHRPReader.GetPeptideHitResultType(m_jobParams.GetParam("ResultType"));

            if (eResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
            {
                // Result type is not supported
                var msg = "ResultType is not supported by MSGF: " + m_jobParams.GetParam("ResultType");
                m_message = clsGlobal.AppendToComment(m_message, msg);
                LogError("clsMSGFToolRunner.RunTool(); " + msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that program files exist
            if (!DefineProgramPaths())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the MSGF version info in the database after the program version is written to file MSGF_ConsoleOutput.txt
            mToolVersionWritten = false;
            mMSGFVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;

            mKeepMSGFInputFiles = m_jobParams.GetJobParameter("KeepMSGFInputFile", false);
            var blnDoNotFilterPeptides = m_jobParams.GetJobParameter("MSGFIgnoreFilters", false);
            var blnPostProcessingError = false;

            try
            {
                var blnProcessingError = false;

                if (mUsingMSGFDB & eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
                {
                    // Analysis tool is MSGF+ so we don't actually need to run the MSGF re-scorer
                    // Simply copy the values from the MSGFDB result file

                    StoreToolVersionInfoPrecomputedProbabilities(eResultType);

                    if (!CreateMSGFResultsFromMSGFDBResults())
                    {
                        blnProcessingError = true;
                    }
                }
                else if (eResultType == clsPHRPReader.ePeptideHitResultType.MODa)
                {
                    // Analysis tool is MODa, which MSGF does not support
                    // Instead, summarize the MODa results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(eResultType);

                    if (!SummarizeMODaResults())
                    {
                        blnProcessingError = true;
                    }
                }
                else if (eResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
                {
                    // Analysis tool is MODPlus, which MSGF does not support
                    // Instead, summarize the MODPlus results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(eResultType);

                    if (!SummarizeMODPlusResults())
                    {
                        blnProcessingError = true;
                    }
                }
                else if (eResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                {
                    // Analysis tool is MSPathFinder, which MSGF does not support
                    // Instead, summarize the MSPathFinder results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(eResultType);

                    if (!SummarizeMSPathFinderResults())
                    {
                        blnProcessingError = true;
                    }
                }
                else
                {
                    if (!ProcessFilesWrapper(eRawDataType, eResultType, blnDoNotFilterPeptides, blnMGFInstrumentData))
                    {
                        blnProcessingError = true;
                    }

                    if (!blnProcessingError)
                    {
                        // Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
                        // Will also make sure that all of the peptides have numeric SpecProb values
                        // For peptides where MSGF reported an error, the MSGF SpecProb will be set to 1
                        // If the Instrument Data was a .MGF file, then we need to update the scan numbers using mMSGFInputCreator.GetScanByMGFSpectrumIndex()

                        // Sleep for 1 second to give the MSGF results file a chance to finalize
                        Thread.Sleep(1000);

                        var blnSuccess = PostProcessMSGFResults(eResultType, mMSGFResultsFilePath, blnMGFInstrumentData);

                        if (!blnSuccess)
                        {
                            m_message = clsGlobal.AppendToComment(m_message, "MSGF results file post-processing error");
                            blnPostProcessingError = true;
                        }
                    }
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                if (blnProcessingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                Thread.Sleep(500);
                // 500 msec delay
                clsProgRunner.GarbageCollectNow();

                var eResult = MakeResultsFolder();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                eResult = MoveResultFiles();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MoveResultFiles moves the result files to the result folder
                    m_message = "Error moving files into results folder";
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }

                if (blnProcessingError | eReturnCode == CloseOutType.CLOSEOUT_FAILED)
                {
                    // Try to save whatever files were moved into the results folder
                    var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                eResult = CopyResultsFolderToServer();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return eResult;
                }

                if (blnPostProcessingError)
                {
                    // When a post-processing error occurs, we copy the files to the server, but return CLOSEOUT_FAILED
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                var errMsg = "clsMSGFToolRunner.RunTool(); Exception running MSGF: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(errMsg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running MSGF");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //If we get to here, everything worked so exit happily
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private string AddFileNameSuffix(string strFilePath, int intSuffix)
        {
            return AddFileNameSuffix(strFilePath, intSuffix.ToString());
        }

        private string AddFileNameSuffix(string strFilePath, string strSuffix)
        {
            var fiFile = new FileInfo(strFilePath);
            var strFilePathNew = Path.Combine(fiFile.DirectoryName, Path.GetFileNameWithoutExtension(fiFile.Name) + "_" + strSuffix + fiFile.Extension);

            return strFilePathNew;
        }

        /// <summary>
        /// Examines the Sequest, X!Tandem, Inspect, or MSGFDB param file to determine if ETD mode is enabled
        /// </summary>
        /// <param name="eResultType"></param>
        /// <param name="strSearchToolParamFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        private bool CheckETDModeEnabled(clsPHRPReader.ePeptideHitResultType eResultType, string strSearchToolParamFilePath)
        {
            mETDMode = false;
            var blnSuccess = false;

            if (string.IsNullOrEmpty(strSearchToolParamFilePath))
            {
                LogError("PeptideHit param file path is empty; unable to continue");
                return false;
            }

            m_StatusTools.CurrentOperation = "Checking whether ETD mode is enabled";

            switch (eResultType)
            {
                case clsPHRPReader.ePeptideHitResultType.Sequest:
                    blnSuccess = CheckETDModeEnabledSequest(strSearchToolParamFilePath);

                    break;
                case clsPHRPReader.ePeptideHitResultType.XTandem:
                    blnSuccess = CheckETDModeEnabledXTandem(strSearchToolParamFilePath);

                    break;
                case clsPHRPReader.ePeptideHitResultType.Inspect:
                    LogDebug("Inspect does not support ETD data processing; will set mETDMode to False");
                    blnSuccess = true;

                    break;
                case clsPHRPReader.ePeptideHitResultType.MSGFDB:
                    blnSuccess = CheckETDModeEnabledMSGFDB(strSearchToolParamFilePath);

                    break;
                case clsPHRPReader.ePeptideHitResultType.MODa:
                    LogDebug("MODa does not support ETD data processing; will set mETDMode to False");
                    blnSuccess = true;

                    break;
                case clsPHRPReader.ePeptideHitResultType.MODPlus:
                    LogDebug("MODPlus does not support ETD data processing; will set mETDMode to False");
                    blnSuccess = true;

                    break;
                default:
                    // Unknown result type
                    break;
            }

            if (mETDMode)
            {
                LogDebug("ETD search mode has been enabled since c and z ions were used for the peptide search");
            }

            return blnSuccess;
        }

        private bool CheckETDModeEnabledMSGFDB(string strSearchToolParamFilePath)
        {
            const string MSGFDB_FRAG_METHOD_TAG = "FragmentationMethodID";

            try
            {
                mETDMode = false;

                if (m_DebugLevel >= 2)
                {
                    LogDebug("Reading the MSGF-DB parameter file: " + strSearchToolParamFilePath);
                }

                // Read the data from the MSGF-DB Param file
                using (var srParamFile = new StreamReader(new FileStream(strSearchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn) && strLineIn.StartsWith(MSGFDB_FRAG_METHOD_TAG))
                        {
                            // Check whether this line is FragmentationMethodID=2
                            // Note that FragmentationMethodID=4 means Merge spectra from the same precursor (e.g. CID/ETD pairs, CID/HCD/ETD triplets)
                            // This mode is not yet supported

                            if (m_DebugLevel >= 3)
                            {
                                LogDebug("MSGFDB " + MSGFDB_FRAG_METHOD_TAG + " line found: " + strLineIn);
                            }

                            // Look for the equals sign
                            var intCharIndex = strLineIn.IndexOf('=');
                            if (intCharIndex > 0)
                            {
                                var strFragMode = strLineIn.Substring(intCharIndex + 1).Trim();

                                int intFragMode;
                                if (int.TryParse(strFragMode, out intFragMode))
                                {
                                    if (intFragMode == 2)
                                    {
                                        mETDMode = true;
                                    }
                                    else if (intFragMode == 4)
                                    {
                                        // ToDo: Figure out how to handle this mode
                                        mETDMode = false;
                                    }
                                    else
                                    {
                                        mETDMode = false;
                                    }
                                }
                            }
                            else
                            {
                                LogWarning(
                                    "MSGFDB " + MSGFDB_FRAG_METHOD_TAG +
                                     " line does not have an equals sign; will assume not using ETD ions: " +
                                    strLineIn);
                            }

                            // No point in checking any further since we've parsed the ion_series line
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var msg = "Error reading the MSGFDB param file: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(msg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception reading MSGFDB parameter file");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Examines the Sequest param file to determine if ETD mode is enabled
        /// If it is, then sets mETDMode to True
        /// </summary>
        /// <param name="strSearchToolParamFilePath">Sequest parameter file to read</param>
        /// <returns>True if success; false if an error</returns>
        private bool CheckETDModeEnabledSequest(string strSearchToolParamFilePath)
        {
            const string SEQUEST_ION_SERIES_TAG = "ion_series";

            try
            {
                mETDMode = false;

                if (m_DebugLevel >= 2)
                {
                    LogDebug("Reading the Sequest parameter file: " + strSearchToolParamFilePath);
                }

                // Read the data from the Sequest Param file
                using (var srParamFile = new StreamReader(new FileStream(strSearchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn) && strLineIn.StartsWith(SEQUEST_ION_SERIES_TAG))
                        {
                            // This is the ion_series line
                            // If ETD mode is enabled, then c and z ions will have a 1 in this series of numbers:
                            // ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0
                            //
                            // The key to parsing this data is:
                            // ion_series = - - -  a   b   c  --- --- ---  x   y   z
                            // ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0

                            if (m_DebugLevel >= 3)
                            {
                                LogDebug("Sequest " + SEQUEST_ION_SERIES_TAG + " line found: " + strLineIn);
                            }

                            // Look for the equals sign
                            var intCharIndex = strLineIn.IndexOf('=');
                            if (intCharIndex > 0)
                            {
                                var strIonWeightText = strLineIn.Substring(intCharIndex + 1).Trim();

                                // Split strIonWeightText on spaces
                                var strIonWeights = strIonWeightText.Split(' ');

                                if (strIonWeights.Length >= 12)
                                {
                                    double dblCWeight;
                                    double dblZWeight;

                                    double.TryParse(strIonWeights[5], out dblCWeight);
                                    double.TryParse(strIonWeights[11], out dblZWeight);

                                    if (m_DebugLevel >= 3)
                                    {
                                        LogDebug(
                                            "Sequest " + SEQUEST_ION_SERIES_TAG + " line has c-ion weighting = " + dblCWeight +
                                            " and z-ion weighting = " + dblZWeight);
                                    }

                                    if (dblCWeight > 0 || dblZWeight > 0)
                                    {
                                        mETDMode = true;
                                    }
                                }
                                else
                                {
                                    LogWarning(
                                        "Sequest " + SEQUEST_ION_SERIES_TAG + " line does not have 11 numbers; will assume not using ETD ions: " +
                                        strLineIn);
                                }
                            }
                            else
                            {
                                LogWarning(
                                    "Sequest " + SEQUEST_ION_SERIES_TAG + " line does not have an equals sign; will assume not using ETD ions: " +
                                    strLineIn);
                            }

                            // No point in checking any further since we've parsed the ion_series line
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                var msg = "Error reading the Sequest param file: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(msg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception reading Sequest parameter file");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Examines the X!Tandem param file to determine if ETD mode is enabled
        /// If it is, then sets mETDMode to True
        /// </summary>
        /// <param name="strSearchToolParamFilePath">X!Tandem XML parameter file to read</param>
        /// <returns>True if success; false if an error</returns>
        private bool CheckETDModeEnabledXTandem(string strSearchToolParamFilePath)
        {
            XmlNodeList objSelectedNodes = null;

            try
            {
                mETDMode = false;

                if (m_DebugLevel >= 2)
                {
                    LogDebug("Reading the X!Tandem parameter file: " + strSearchToolParamFilePath);
                }

                // Open the parameter file
                // Look for either of these lines:
                //   <note type="input" label="scoring, c ions">yes</note>
                //   <note type="input" label="scoring, z ions">yes</note>

                var objParamFile = new XmlDocument {
                    PreserveWhitespace = true
                };
                objParamFile.Load(strSearchToolParamFilePath);

                for (var intSettingIndex = 0; intSettingIndex <= 1; intSettingIndex++)
                {
                    switch (intSettingIndex)
                    {
                        case 0:
                            objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']");
                            break;
                        case 1:
                            objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']");
                            break;
                    }

                    if (objSelectedNodes != null)
                    {
                        for (var intMatchIndex = 0; intMatchIndex <= objSelectedNodes.Count - 1; intMatchIndex++)
                        {
                            // Make sure this node has an attribute named type with value "input"
                            var objAttributeNode = objSelectedNodes.Item(intMatchIndex).Attributes.GetNamedItem("type");

                            if (objAttributeNode == null)
                            {
                                // Node does not have an attribute named "type"
                            }
                            else
                            {
                                if (objAttributeNode.Value.ToLower() == "input")
                                {
                                    // Valid node; examine its InnerText value
                                    if (objSelectedNodes.Item(intMatchIndex).InnerText.ToLower() == "yes")
                                    {
                                        mETDMode = true;
                                    }
                                }
                            }
                        }
                    }

                    if (mETDMode)
                        break;
                }
            }
            catch (Exception ex)
            {
                var msg = "Error reading the X!Tandem param file: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(msg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception reading X!Tandem parameter file");

                return false;
            }

            return true;
        }

        private bool ConvertMzMLToMzXML()
        {
            m_StatusTools.CurrentOperation = "Creating the .mzXML file";

            mMSXmlCreator = new clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

            var blnSuccess = mMSXmlCreator.ConvertMzMLToMzXML();

            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = mMSXmlCreator.ErrorMessage;
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Unknown error creating the mzXML file";
                }
            }

            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZML_EXTENSION);

            return blnSuccess;
        }

        /// <summary>
        /// Creates the MSGF Input file by reading Sequest, X!Tandem, or Inspect PHRP result file and extracting the relevant information
        /// Uses the ModSummary.txt file to determine the dynamic and static mods used
        /// </summary>
        /// <param name="eResultType"></param>
        /// <param name="blnDoNotFilterPeptides"></param>
        /// <param name="blnMGFInstrumentData"></param>
        /// <param name="intMSGFInputFileLineCount"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CreateMSGFInputFile(clsPHRPReader.ePeptideHitResultType eResultType, bool blnDoNotFilterPeptides, bool blnMGFInstrumentData, out int intMSGFInputFileLineCount)
        {

            var blnSuccess = true;

            intMSGFInputFileLineCount = 0;
            mMSGFInputCreatorErrorCount = 0;
            mMSGFInputCreatorWarningCount = 0;

            // Convert the peptide-hit result file (from PHRP) to a tab-delimited input file to be read by MSGF
            switch (eResultType)
            {
                case clsPHRPReader.ePeptideHitResultType.Sequest:

                    // Convert Sequest results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorSequest(m_Dataset, m_WorkDir);

                    break;
                case clsPHRPReader.ePeptideHitResultType.XTandem:

                    // Convert X!Tandem results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorXTandem(m_Dataset, m_WorkDir);

                    break;

                case clsPHRPReader.ePeptideHitResultType.Inspect:

                    // Convert Inspect results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorInspect(m_Dataset, m_WorkDir);

                    break;
                case clsPHRPReader.ePeptideHitResultType.MSGFDB:

                    // Convert MSGFDB results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorMSGFDB(m_Dataset, m_WorkDir);

                    break;
                case clsPHRPReader.ePeptideHitResultType.MODa:

                    // Convert MODa results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorMODa(m_Dataset, m_WorkDir);

                    break;
                case clsPHRPReader.ePeptideHitResultType.MODPlus:

                    // Convert MODPlus results to input format required for MSGF
                    mMSGFInputCreator = new clsMSGFInputCreatorMODPlus(m_Dataset, m_WorkDir);

                    break;
                default:
                    // Should never get here; invalid result type specified
                    var msg = "Invalid PeptideHit ResultType specified: " + eResultType;
                    m_message = clsGlobal.AppendToComment(m_message, msg);
                    LogError("clsMSGFToolRunner.CreateMSGFInputFile(); " + msg);

                    blnSuccess = false;
                    break;
            }

            if (blnSuccess)
            {
                // Register events
                mMSGFInputCreator.ErrorEvent += mMSGFInputCreator_ErrorEvent;
                mMSGFInputCreator.WarningEvent += mMSGFInputCreator_WarningEvent;

                mMSGFInputFilePath = mMSGFInputCreator.MSGFInputFilePath;
                mMSGFResultsFilePath = mMSGFInputCreator.MSGFResultsFilePath;

                mMSGFInputCreator.DoNotFilterPeptides = blnDoNotFilterPeptides;
                mMSGFInputCreator.MgfInstrumentData = blnMGFInstrumentData;

                m_StatusTools.CurrentOperation = "Creating the MSGF Input file";

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Creating the MSGF Input file");
                }

                blnSuccess = mMSGFInputCreator.CreateMSGFInputFileUsingPHRPResultFiles();

                intMSGFInputFileLineCount = mMSGFInputCreator.MSGFInputFileLineCount;

                if (!blnSuccess)
                {
                    LogError("mMSGFInputCreator.MSGFDataFileLineCount returned False");
                }
                else
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogDebug("CreateMSGFInputFileUsingPHRPResultFile complete; " + intMSGFInputFileLineCount + " lines of data");
                    }
                }
            }

            return blnSuccess;
        }

        private bool SummarizeMODaResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Any results based on a MSGF SpecProb will be meaningless because we didn't run MSGF on the MODa results
            // Post the results to the database
            var blnSuccess = SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType.MODa);

            if (blnSuccess)
            {
                // We didn't actually run MSGF, so these files aren't needed
                m_jobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                m_jobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return blnSuccess;
        }

        private bool SummarizeMODPlusResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Any results based on a MSGF SpecProb will be meaningless because we didn't run MSGF on the MODPlus results
            // Post the results to the database
            var blnSuccess = SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType.MODPlus);

            if (blnSuccess)
            {
                // We didn't actually run MSGF, so these files aren't needed
                m_jobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                m_jobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return blnSuccess;
        }

        private bool SummarizeMSPathFinderResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Will use SpecEValue in place of MSGF SpecProb
            // Post the results to the database
            var blnSuccess = SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType.MSPathFinder);

            if (blnSuccess)
            {
                // We didn't actually run MSGF, so these files aren't needed
                m_jobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                m_jobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return blnSuccess;
        }

        private bool CreateMSGFResultsFromMSGFDBResults()
        {
            var objMSGFInputCreator = new clsMSGFInputCreatorMSGFDB(m_Dataset, m_WorkDir);

            if (!CreateMSGFResultsFromMSGFPlusResults(objMSGFInputCreator, MSGF_PHRP_DATA_SOURCE_SYN.ToLower()))
            {
                return false;
            }

            if (!CreateMSGFResultsFromMSGFPlusResults(objMSGFInputCreator, MSGF_PHRP_DATA_SOURCE_FHT.ToLower()))
            {
                return false;
            }

            // Summarize the results in the _syn_MSGF.txt file
            // Post the results to the database
            var blnSuccess = SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType.MSGFDB);

            return blnSuccess;
        }

        private bool CreateMSGFResultsFromMSGFPlusResults(clsMSGFInputCreatorMSGFDB objMSGFInputCreator, string strSynOrFHT)
        {
            var sourceFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_" + strSynOrFHT + ".txt");

            if (!File.Exists(sourceFilePath))
            {
                var sourceFilePathAlternate = Path.Combine(m_WorkDir, m_Dataset + "_msgfdb_" + strSynOrFHT + ".txt");
                if (!File.Exists(sourceFilePathAlternate))
                {
                    m_message = "Input file not found: " + Path.GetFileName(sourceFilePath);
                    return false;
                }
                sourceFilePath = sourceFilePathAlternate;
            }

            var success = objMSGFInputCreator.CreateMSGFFileUsingMSGFDBSpecProb(sourceFilePath, strSynOrFHT);

            if (!success)
            {
                m_message = "Error creating MSGF file for " + Path.GetFileName(sourceFilePath);
                if (!string.IsNullOrEmpty(objMSGFInputCreator.ErrorMessage))
                {
                    m_message += ": " + objMSGFInputCreator.ErrorMessage;
                }
                return false;
            }

            return true;

        }

        private bool CreateMzXMLFile()
        {
            m_StatusTools.CurrentOperation = "Creating the .mzXML file";

            var strMzXmlFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(strMzXmlFilePath))
            {
                // File already exists; nothing to do
                return true;
            }

            mMSXmlCreator = new clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

            var blnSuccess = mMSXmlCreator.CreateMZXMLFile();

            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = mMSXmlCreator.ErrorMessage;
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Unknown error creating the mzXML file";
                }
            }

            CopyMzXMLFileToServerCache(strMzXmlFilePath, string.Empty, Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath), blnPurgeOldFilesIfNeeded: true);

            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);

            return blnSuccess;
        }

        private bool DefineProgramPaths()
        {
            // mJavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
            // Note that we need to run MSGF with a 64-bit version of Java since it prefers to use 2 or more GB of ram
            mJavaProgLoc = GetJavaProgLoc();
            if (string.IsNullOrEmpty(mJavaProgLoc))
            {
                return false;
            }

            // Determine the path to the MSGFDB program (which contains the MSGF class); we also allow for the possibility of calling the legacy version of MSGF
            mMSGFProgLoc = DetermineMSGFProgramLocation();

            if (string.IsNullOrEmpty(mMSGFProgLoc))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error determining MSGF program location";
                }
                LogError(m_message);
                return false;
            }

            mMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            return true;
        }

        private string DetermineMSGFProgramLocation()
        {
            var strStepToolName = "MSGFDB";
            var strProgLocManagerParamName = "MSGFDbProgLoc";
            var strExeName = MSGFDB_JAR_NAME;

            mUsingMSGFDB = true;

            // Note that as of 12/20/2011 we are using MSGFDB.jar to access the MSGF class
            // In order to allow the old version of MSGF to be run, we must look for parameter MSGF_Version

            // Check whether the settings file specifies that a specific version of the step tool be used
            var strMSGFStepToolVersion = m_jobParams.GetParam("MSGF_Version");

            if (!string.IsNullOrWhiteSpace(strMSGFStepToolVersion))
            {
                // Specific version is defined
                // Check whether the version is one of the known versions for the old MSGF

                if (IsLegacyMSGFVersion(strMSGFStepToolVersion))
                {
                    // Use MSGF

                    strStepToolName = "MSGF";
                    strProgLocManagerParamName = "MSGFLoc";
                    strExeName = MSGF_JAR_NAME;

                    mUsingMSGFDB = false;
                }
                else
                {
                    // Use MSGFDB
                    mUsingMSGFDB = true;
                    mMSGFDBVersion = string.Copy(strMSGFStepToolVersion);
                }
            }
            else
            {
                // Use MSGFDB
                mUsingMSGFDB = true;
                mMSGFDBVersion = "Production_Release";
            }

            return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strMSGFStepToolVersion);
        }

        public static bool IsLegacyMSGFVersion(string strStepToolVersion)
        {
            switch (strStepToolVersion.ToLower())
            {
                case "v2010-11-16":
                case "v2011-09-02":
                case "v6393":
                case "v6432":
                    // Legacy MSGF

                    return true;
                default:
                    // Using MSGF inside MSGFDB

                    return false;
            }
        }

        /// <summary>
        /// Compare intPrecursorMassErrorCount to intLinesRead
        /// </summary>
        /// <param name="intLinesRead"></param>
        /// <param name="intPrecursorMassErrorCount"></param>
        /// <returns>True if more than 10% of the results have a precursor mass error</returns>
        /// <remarks></remarks>
        private bool PostProcessMSGFCheckPrecursorMassErrorCount(int intLinesRead, int intPrecursorMassErrorCount)
        {
            const int MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT = 10;

            var blnTooManyPrecursorMassMismatches = false;

            try
            {
                // If 10% or more of the data has a message like "N/A: precursor mass != peptide mass (3571.8857 vs 3581.9849)"
                // then set blnTooManyPrecursorMassMismatches to True

                blnTooManyPrecursorMassMismatches = false;

                if (intLinesRead >= 2 && intPrecursorMassErrorCount > 0)
                {
                    var sngPercentDataPrecursorMassError = intPrecursorMassErrorCount / (float)intLinesRead * 100f;

                    var msg = sngPercentDataPrecursorMassError.ToString("0.0") +
                                 "% of the data processed by MSGF has a precursor mass 10 or more Da away from the computed peptide mass";

                    if (sngPercentDataPrecursorMassError >= MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT)
                    {
                        msg += "; this likely indicates a static or dynamic mod definition is missing from the PHRP _ModSummary.txt file";
                        LogError(msg);
                        blnTooManyPrecursorMassMismatches = true;
                    }
                    else
                    {
                        msg += "; this is below the error threshold of " + MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT +
                               "% and thus is only a warning (note that static and dynamic mod info is loaded from the PHRP _ModSummary.txt file)";
                        LogWarning(msg);
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return blnTooManyPrecursorMassMismatches;
        }

        /// <summary>
        /// Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
        /// Will also look for non-numeric values in the SpecProb column
        /// Examples:
        ///   N/A: unrecognizable annotation
        ///   N/A: precursor mass != peptide mass (4089.068 vs 4078.069)
        /// the new MSGF result files will guarantee that the SpecProb column has a number,
        ///   but will have an additional column called SpecProbNotes with any notes or warnings
        /// The synopsis-based MSGF results will be extended to include any entries skipped when
        ///  creating the MSGF input file (to aid in linking up files later)
        /// </summary>
        /// <param name="eResultType">PHRP result type</param>
        /// <param name="strMSGFResultsFilePath">MSGF results file to examine</param>
        /// <param name="blnMGFInstrumentData">True when the instrument data file is a .mgf file</param>
        /// <returns>True if success; false if one or more errors</returns>
        /// <remarks></remarks>
        private bool PostProcessMSGFResults(clsPHRPReader.ePeptideHitResultType eResultType, string strMSGFResultsFilePath, bool blnMGFInstrumentData)
        {
            FileInfo fiInputFile;

            string strMSGFSynopsisResults;

            bool blnSuccess;
            bool blnFirstHitsDataPresent;
            bool blnTooManyErrors;

            try
            {
                if (string.IsNullOrEmpty(strMSGFResultsFilePath))
                {
                    LogError("MSGF Results File path is empty; unable to continue");
                    return false;
                }

                m_StatusTools.CurrentOperation = "MSGF complete; post-processing the results";

                if (m_DebugLevel >= 2)
                {
                    LogDebug("MSGF complete; post-processing the results");
                }

                fiInputFile = new FileInfo(strMSGFResultsFilePath);

                // Define the path to write the synopsis MSGF results to
                strMSGFSynopsisResults = Path.Combine(fiInputFile.DirectoryName, Path.GetFileNameWithoutExtension(fiInputFile.Name) + "_PostProcess.txt");

                m_progress = PROGRESS_PCT_MSGF_POST_PROCESSING;
                m_StatusTools.UpdateAndWrite(m_progress);

                blnSuccess = PostProcessMSGFResultsWork(strMSGFResultsFilePath, strMSGFSynopsisResults, blnMGFInstrumentData, out blnFirstHitsDataPresent, out blnTooManyErrors);
            }
            catch (Exception ex)
            {
                var errMsg = "Error post-processing the MSGF Results file: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(errMsg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file");

                return false;
            }

            try
            {
                // Now replace the _MSGF.txt file with the _MSGF_PostProcess.txt file
                // For example, replace:
                //   QC_Shew_Dataset_syn_MSGF.txt
                // With the contents of:
                //   QC_Shew_Dataset_syn_MSGF_PostProcess.txt

                Thread.Sleep(500);

                // Delete the original file
                fiInputFile.Delete();
                Thread.Sleep(500);

                // Rename the _PostProcess.txt file
                var fiMSGFSynFile = new FileInfo(strMSGFSynopsisResults);

                fiMSGFSynFile.MoveTo(strMSGFResultsFilePath);
            }
            catch (Exception ex)
            {
                var errMsg = "Error replacing the original MSGF Results file with the post-processed one: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                LogError(errMsg, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Exception post-processing the MSGF Results file");

                return false;
            }

            if (blnSuccess)
            {
                // Summarize the results in the _syn_MSGF.txt file
                // Post the results to the database
                LogDebug("Call SummarizeMSGFResults for eResultType " + eResultType, 3);

                var summarizeSuccess = SummarizeMSGFResults(eResultType);

                LogDebug("SummarizeMSGFResults returned " + summarizeSuccess, 3);
            }

            if (blnSuccess && blnFirstHitsDataPresent)
            {
                // Write out the First-Hits file results
                LogDebug("Call mMSGFInputCreator.CreateMSGFFirstHitsFile", 3);

                blnSuccess = mMSGFInputCreator.CreateMSGFFirstHitsFile();

                LogDebug("CreateMSGFFirstHitsFile returned " + blnSuccess, 3);
            }

            if (blnSuccess & eResultType != clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                LogDebug("Call UpdateProteinModsFile for eResultType " + eResultType, 3);

                blnSuccess = UpdateProteinModsFile(eResultType, strMSGFResultsFilePath);

                LogDebug("UpdateProteinModsFile returned " + blnSuccess, 3);
            }

            if (blnTooManyErrors)
            {
                return false;
            }

            return blnSuccess;

        }

        /// <summary>
        /// Process the data in strMSGFResultsFilePath to create strMSGFSynopsisResults
        /// </summary>
        /// <param name="strMSGFResultsFilePath">MSGF Results file path</param>
        /// <param name="strMSGFSynopsisResults">MSGF synopsis file path</param>
        /// <param name="blnMGFInstrumentData">True when the instrument data file is a .mgf file</param>
        /// <param name="blnFirstHitsDataPresent">Will be set to True if First-hits data is present</param>
        /// <param name="blnTooManyErrors">Will be set to True if too many errors occur</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool PostProcessMSGFResultsWork(string strMSGFResultsFilePath, string strMSGFSynopsisResults, bool blnMGFInstrumentData,
            out bool blnFirstHitsDataPresent, out bool blnTooManyErrors)
        {
            const int MAX_ERRORS_TO_LOG = 5;

            var chSepChars = new[] { '\t' };

            var intLinesRead = 0;
            var intSpecProbErrorCount = 0;
            var intPrecursorMassErrorCount = 0;
            var intMGFLookupErrorCount = 0;

            ///////////////////////////////////////////////////////
            // Note: Do not put a Try/Catch block in this function
            // Allow the calling function to catch any errors
            ///////////////////////////////////////////////////////

            // Initialize the column mapping
            // Using a case-insensitive comparer
            var objColumnHeaders = new SortedDictionary<string, int>(StringComparer.CurrentCultureIgnoreCase)
            {
                // Define the default column mapping
                {MSGF_RESULT_COLUMN_SpectrumFile, 0},
                {MSGF_RESULT_COLUMN_Title, 1},
                {MSGF_RESULT_COLUMN_ScanNumber, 2},
                {MSGF_RESULT_COLUMN_Annotation, 3},
                {MSGF_RESULT_COLUMN_Charge, 4},
                {MSGF_RESULT_COLUMN_Protein_First, 5},
                {MSGF_RESULT_COLUMN_Result_ID, 6},
                {MSGF_RESULT_COLUMN_Data_Source, 7},
                {MSGF_RESULT_COLUMN_Collision_Mode, 8},
                {MSGF_RESULT_COLUMN_SpecProb, 9}
            };



            // Read the data from the MSGF Result file and
            // write the Synopsis MSGF Results to a new file
            using (var srMSGFResults = new StreamReader(new FileStream(strMSGFResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            using (var swMSGFSynFile = new StreamWriter(new FileStream(strMSGFSynopsisResults, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                // Write out the headers to swMSGFSynFile
                mMSGFInputCreator.WriteMSGFResultsHeaders(swMSGFSynFile);

                var blnHeaderLineParsed = false;
                blnFirstHitsDataPresent = false;
                blnTooManyErrors = false;

                while (!srMSGFResults.EndOfStream)
                {
                    var strLineIn = srMSGFResults.ReadLine();
                    intLinesRead += 1;
                    var blnSkipLine = false;

                    if (string.IsNullOrEmpty(strLineIn))
                        continue;

                    var strSplitLine = strLineIn.Split('\t');

                    if (!blnHeaderLineParsed)
                    {
                        if (strSplitLine[0].ToLower() == MSGF_RESULT_COLUMN_SpectrumFile.ToLower())
                        {
                            // Parse the header line to confirm the column ordering
                            clsPHRPReader.ParseColumnHeaders(strSplitLine, objColumnHeaders);
                            blnSkipLine = true;
                        }

                        blnHeaderLineParsed = true;
                    }

                    if (blnSkipLine || strSplitLine.Length < 4)
                        continue;

                    var strOriginalPeptide = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Title, objColumnHeaders);
                    var strScan = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_ScanNumber, objColumnHeaders);
                    var strCharge = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Charge, objColumnHeaders);
                    var strProtein = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Protein_First, objColumnHeaders);
                    var strPeptide = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Annotation, objColumnHeaders);
                    var strResultID = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Result_ID, objColumnHeaders);
                    var strSpecProb = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_SpecProb, objColumnHeaders);
                    var strDataSource = clsPHRPReader.LookupColumnValue(strSplitLine, MSGF_RESULT_COLUMN_Data_Source, objColumnHeaders);
                    var strNotes = string.Empty;

                    if (blnMGFInstrumentData)
                    {
                        // Update the scan number
                        int intMGFScanIndex;
                        var intActualScanNumber = 0;
                        if (int.TryParse(strScan, out intMGFScanIndex))
                        {
                            intActualScanNumber = mMSGFInputCreator.GetScanByMGFSpectrumIndex(intMGFScanIndex);
                        }

                        if (intActualScanNumber == 0)
                        {
                            intMGFLookupErrorCount += 1;

                            // Log the first 5 instances to the log file as warnings
                            LogWarning(
                                "Unable to determine the scan number for MGF spectrum index " + strScan + " on line  " + intLinesRead +
                                " in the result file");
                        }
                        strScan = intActualScanNumber.ToString();
                    }

                    double dblSpecProb;
                    if (double.TryParse(strSpecProb, out dblSpecProb))
                    {
                        if (strOriginalPeptide != strPeptide)
                        {
                            strNotes = string.Copy(strPeptide);
                        }

                        // Update strSpecProb to reduce the number of significant figures
                        strSpecProb = dblSpecProb.ToString("0.000000E+00");
                    }
                    else
                    {
                        // The specProb column does not contain a number
                        intSpecProbErrorCount += 1;

                        if (intSpecProbErrorCount <= MAX_ERRORS_TO_LOG)
                        {
                            // Log the first 5 instances to the log file as warnings

                            string strOriginalPeptideInfo;
                            if (strOriginalPeptide != strPeptide)
                            {
                                strOriginalPeptideInfo = ", original peptide sequence " + strOriginalPeptide;
                            }
                            else
                            {
                                strOriginalPeptideInfo = string.Empty;
                            }

                            LogWarning(
                                "MSGF SpecProb is not numeric on line " + intLinesRead + " in the result file: " + strSpecProb +
                                " (parent peptide " + strPeptide + ", Scan " + strScan + ", Result_ID " + strResultID + strOriginalPeptideInfo +
                                ")");
                        }

                        if (strSpecProb.Contains("precursor mass"))
                        {
                            intPrecursorMassErrorCount += 1;
                        }

                        if (strOriginalPeptide != strPeptide)
                        {
                            strNotes = strPeptide + "; " + strSpecProb;
                        }
                        else
                        {
                            strNotes = string.Copy(strSpecProb);
                        }

                        // Change the spectrum probability to 1
                        strSpecProb = "1";
                    }

                    var strMSGFResultData = strScan + "\t" + strCharge + "\t" + strProtein + "\t" + strOriginalPeptide + "\t" + strSpecProb + "\t" + strNotes;

                    // Add this result to the cached string dictionary
                    mMSGFInputCreator.AddUpdateMSGFResult(strScan, strCharge, strOriginalPeptide, strMSGFResultData);

                    if (strDataSource == MSGF_PHRP_DATA_SOURCE_FHT)
                    {
                        // First-hits file
                        blnFirstHitsDataPresent = true;
                    }
                    else
                    {
                        // Synopsis file

                        // Add this entry to the MSGF synopsis results
                        // Note that strOriginalPeptide has the original peptide sequence
                        swMSGFSynFile.WriteLine(strResultID + "\t" + strMSGFResultData);

                        // See if any entries were skipped when reading the synopsis file used to create the MSGF input file
                        // If they were, add them to the validated MSGF file (to aid in linking up files later)

                        int intResultID;
                        if (!int.TryParse(strResultID, out intResultID))
                            continue;

                        var objSkipList = mMSGFInputCreator.GetSkippedInfoByResultId(intResultID);

                        for (var intIndex = 0; intIndex <= objSkipList.Count - 1; intIndex++)
                        {
                            // Split the entry on the tab character
                            // The item left of the tab is the skipped result id
                            // the item right of the tab is the protein corresponding to the skipped result id

                            var strSkipInfo = objSkipList[intIndex].Split(chSepChars, 2);

                            swMSGFSynFile.WriteLine(strSkipInfo[0] + "\t" + strScan + "\t" + strCharge + "\t" + strSkipInfo[1] + "\t" +
                                                    strOriginalPeptide + "\t" + strSpecProb + "\t" + strNotes);
                        }
                    }
                }
            }

            if (intSpecProbErrorCount > 1)
            {
                LogWarning("MSGF SpecProb was not numeric for " + intSpecProbErrorCount + " entries in the MSGF result file");
            }

            if (intMGFLookupErrorCount > 1)
            {
                LogError("MGF Index-to-scan lookup failed for " + intMGFLookupErrorCount + " entries in the MSGF result file");
                if (intLinesRead > 0 && intMGFLookupErrorCount / (float)intLinesRead > 0.1)
                {
                    blnTooManyErrors = true;
                }
            }

            // Check whether more than 10% of the results have a precursor mass error
            if (PostProcessMSGFCheckPrecursorMassErrorCount(intLinesRead, intPrecursorMassErrorCount))
            {
                blnTooManyErrors = true;
            }

            // If we get here, return True
            return true;
        }

        private bool ProcessFileWithMSGF(clsPHRPReader.ePeptideHitResultType eResultType, int intMSGFInputFileLineCount, string strMSGFInputFilePath, string strMSGFResultsFilePath)
        {
            bool blnSuccess;

            if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                // Input file may contain a mix of scan types (CID, ETD, and/or HCD)
                // If this is the case, then need to call MSGF twice: first for the CID and HCD spectra, then again for the ETD spectra
                blnSuccess = RunMSGFonMSGFDB(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath);
            }
            else
            {
                // Run MSGF
                blnSuccess = RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath);
            }

            return blnSuccess;
        }

        private bool ProcessFilesWrapper(clsAnalysisResources.eRawDataTypeConstants eRawDataType, clsPHRPReader.ePeptideHitResultType eResultType,
            bool blnDoNotFilterPeptides, bool blnMGFInstrumentData)
        {
            string msg;
            int intMSGFInputFileLineCount;

            // Parse the Sequest, X!Tandem, Inspect, or MODa parameter file to determine if ETD mode was used
            var strSearchToolParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"));

            var blnSuccess = CheckETDModeEnabled(eResultType, strSearchToolParamFilePath);
            if (!blnSuccess)
            {
                msg = "Error examining param file to determine if ETD mode was enabled)";
                m_message = clsGlobal.AppendToComment(m_message, msg);
                LogError("clsMSGFToolRunner.RunTool(); " + msg);
                return false;
            }

            m_progress = PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD;
            m_StatusTools.UpdateAndWrite(m_progress);

            // Create the _MSGF_input.txt file
            blnSuccess = CreateMSGFInputFile(eResultType, blnDoNotFilterPeptides, blnMGFInstrumentData, out intMSGFInputFileLineCount);

            if (!blnSuccess)
            {
                msg = "Error creating MSGF input file";
                m_message = clsGlobal.AppendToComment(m_message, msg);
            }
            else
            {
                m_progress = PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED;
                m_StatusTools.UpdateAndWrite(m_progress);
            }

            if (blnSuccess)
            {
                if (blnMGFInstrumentData)
                {
                    blnSuccess = true;
                }
                else if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzXML)
                {
                    blnSuccess = true;
                }
                else if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzML)
                {
                    blnSuccess = ConvertMzMLToMzXML();
                }
                else
                {
                    // Possibly create the .mzXML file
                    // We're waiting to do this until now just in case the above steps fail (since they should all run quickly)
                    blnSuccess = CreateMzXMLFile();
                }

                if (!blnSuccess)
                {
                    msg = "Error creating .mzXML file";
                    m_message = clsGlobal.AppendToComment(m_message, msg);
                }
                else
                {
                    m_progress = PROGRESS_PCT_MZXML_CREATED;
                    m_StatusTools.UpdateAndWrite(m_progress);
                }
            }

            if (blnSuccess)
            {
                var blnUseExistingMSGFResults = m_jobParams.GetJobParameter("UseExistingMSGFResults", false);

                if (blnUseExistingMSGFResults)
                {
                    // Look for a file named Dataset_syn_MSGF.txt in the job's transfer folder
                    // If that file exists, use it as the official MSGF results file
                    // The assumption is that this file will have been created by manually running MSGF on another computer

                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("UseExistingMSGFResults = True; will look for pre-generated MSGF results file in the transfer folder");
                    }

                    if (RetrievePreGeneratedDataFile(Path.GetFileName(mMSGFResultsFilePath)))
                    {
                        LogDebug("Pre-generated MSGF results file successfully copied to the work directory");
                        blnSuccess = true;
                    }
                    else
                    {
                        LogDebug("Pre-generated MSGF results file not found");
                        blnSuccess = false;
                    }
                }
                else
                {
                    // Run MSGF
                    // Note that mMSGFInputFilePath and mMSGFResultsFilePath get populated by CreateMSGFInputFile
                    blnSuccess = ProcessFileWithMSGF(eResultType, intMSGFInputFileLineCount, mMSGFInputFilePath, mMSGFResultsFilePath);
                }

                if (!blnSuccess)
                {
                    msg = "Error running MSGF";
                    m_message = clsGlobal.AppendToComment(m_message, msg);
                }
                else
                {
                    // MSGF successfully completed
                    if (!mKeepMSGFInputFiles)
                    {
                        // Add the _MSGF_input.txt file to the list of files to delete (i.e., do not move it into the results folder)
                        m_jobParams.AddResultFileToSkip(Path.GetFileName(mMSGFInputFilePath));
                    }

                    m_progress = PROGRESS_PCT_MSGF_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                }
            }

            // Make sure the MSGF Input Creator log file is closed
            mMSGFInputCreator.CloseLogFileNow();

            return blnSuccess;
        }

        /// <summary>
        /// Looks for file strFileNameToFind in the transfer folder for this job
        /// If found, copies the file to the work directory
        /// </summary>
        /// <param name="strFileNameToFind"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        private bool RetrievePreGeneratedDataFile(string strFileNameToFind)
        {
            var strFolderToCheck = "??";

            try
            {
                var strTransferFolderPath = m_jobParams.GetParam("transferFolderPath");
                var strInputFolderName = m_jobParams.GetParam("inputFolderName");

                strFolderToCheck = Path.Combine(Path.Combine(strTransferFolderPath, m_Dataset), strInputFolderName);

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Looking for folder " + strFolderToCheck);
                }

                // Look for strFileNameToFind in strFolderToCheck
                if (Directory.Exists(strFolderToCheck))
                {
                    var strFilePathSource = Path.Combine(strFolderToCheck, strFileNameToFind);

                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Looking for file " + strFilePathSource);
                    }

                    if (File.Exists(strFilePathSource))
                    {
                        var strFilePathTarget = Path.Combine(m_WorkDir, strFileNameToFind);
                        LogDebug("Copying file " + strFilePathSource + " to " + strFilePathTarget);

                        File.Copy(strFilePathSource, strFilePathTarget, true);

                        // File found and successfully copied; return true
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LogWarning("Exception finding file " + strFileNameToFind + " in folder " + strFolderToCheck + ": " + ex);
                return false;
            }

            // File not found
            return false;
        }

        private bool RunMSGFonMSGFDB(int intMSGFInputFileLineCount, string strMSGFInputFilePath, string strMSGFResultsFilePath)
        {
            var intCollisionModeColIndex = -1;

            try
            {
                var lstCIDData = new List<string>();
                var lstETDData = new List<string>();

                using (var srSourceFile = new StreamReader(new FileStream(strMSGFInputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intLinesRead = 0;
                    while (!srSourceFile.EndOfStream)
                    {
                        var strLineIn = srSourceFile.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                            continue;

                        intLinesRead += 1;
                        var strSplitLine = strLineIn.Split('\t').ToList();

                        if (intLinesRead == 1)
                        {
                            // Cache the header line
                            lstCIDData.Add(strLineIn);
                            lstETDData.Add(strLineIn);

                            // Confirm the column index of the Collision_Mode column
                            for (var intIndex = 0; intIndex <= strSplitLine.Count - 1; intIndex++)
                            {
                                if (string.Equals(strSplitLine[intIndex], MSGF_RESULT_COLUMN_Collision_Mode, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    intCollisionModeColIndex = intIndex;
                                }
                            }

                            if (intCollisionModeColIndex < 0)
                            {
                                // Collision_Mode column not found; this is unexpected
                                LogError("Collision_Mode column not found in the MSGF input file for MSGFDB data; unable to continue");
                                srSourceFile.Close();
                                return false;
                            }
                        }
                        else
                        {
                            // Read the collision mode

                            if (strSplitLine.Count > intCollisionModeColIndex)
                            {
                                if (strSplitLine[intCollisionModeColIndex].ToUpper() == "ETD")
                                {
                                    lstETDData.Add(strLineIn);
                                }
                                else
                                {
                                    lstCIDData.Add(strLineIn);
                                }
                            }
                            else
                            {
                                lstCIDData.Add(strLineIn);
                            }
                        }
                    }
                }

                mProcessingMSGFDBCollisionModeData = false;

                if (lstCIDData.Count <= 1 & lstETDData.Count > 1)
                {
                    // Only ETD data is present
                    mETDMode = true;
                    return RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath);
                }

                if (lstCIDData.Count > 1 & lstETDData.Count > 1)
                {
                    // Mix of both CID and ETD data found

                    mProcessingMSGFDBCollisionModeData = true;

                    // Make sure the final results file does not exist
                    if (File.Exists(strMSGFResultsFilePath))
                    {
                        File.Delete(strMSGFResultsFilePath);
                    }

                    // Process the CID data
                    mETDMode = false;
                    mCollisionModeIteration = 1;
                    var blnSuccess = RunMSGFonMSGFDBCachedData(lstCIDData, strMSGFInputFilePath, strMSGFResultsFilePath, "CID");
                    if (!blnSuccess)
                        return false;

                    // Process the ETD data
                    mETDMode = true;
                    mCollisionModeIteration = 2;
                    blnSuccess = RunMSGFonMSGFDBCachedData(lstETDData, strMSGFInputFilePath, strMSGFResultsFilePath, "ETD");
                    if (!blnSuccess)
                        return false;

                    return true;
                }
                
                // Only CID or HCD data is present (or no data is present)
                mETDMode = false;
                return RunMSGF(intMSGFInputFileLineCount, strMSGFInputFilePath, strMSGFResultsFilePath);
            }
            catch (Exception ex)
            {
                LogError("Exception in RunMSGFonMSGFDB", ex);
                return false;
            }
        }

        private bool RunMSGFonMSGFDBCachedData(List<string> lstData, string strMSGFInputFilePath, string strMSGFResultsFilePathFinal, string strCollisionMode)
        {

            try
            {
                var strInputFileTempPath = AddFileNameSuffix(strMSGFInputFilePath, strCollisionMode);
                var strResultFileTempPath = AddFileNameSuffix(strMSGFResultsFilePathFinal, strCollisionMode);

                using (var swInputFileTemp = new StreamWriter(new FileStream(strInputFileTempPath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var strData in lstData)
                    {
                        swInputFileTemp.WriteLine(strData);
                    }
                }

                var blnSuccess = RunMSGF(lstData.Count - 1, strInputFileTempPath, strResultFileTempPath);

                if (!blnSuccess)
                {
                    return false;
                }

                Thread.Sleep(500);

                // Append the results of strResultFileTempPath to strMSGFResultsFilePath
                if (!File.Exists(strMSGFResultsFilePathFinal))
                {
                    File.Move(strResultFileTempPath, strMSGFResultsFilePathFinal);
                }
                else
                {
                    using (var srTempResults = new StreamReader(new FileStream(strResultFileTempPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    using (var swFinalResults = new StreamWriter(new FileStream(strMSGFResultsFilePathFinal, FileMode.Append, FileAccess.Write, FileShare.Read)))
                    {
                        // Read and skip the first line of srTempResults (it's a header)
                        srTempResults.ReadLine();

                        // Append the remaining lines to swFinalResults
                        while (!srTempResults.EndOfStream)
                        {
                            swFinalResults.WriteLine(srTempResults.ReadLine());
                        }
                    }
                }

                Thread.Sleep(500);

                if (!mKeepMSGFInputFiles)
                {
                    // Delete the temporary files
                    DeleteTemporaryfile(strInputFileTempPath);
                    DeleteTemporaryfile(strResultFileTempPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in RunMSGFonMSGFDBCachedData", ex);
                return false;
            }

            return true;
        }

        private bool RunMSGF(int intMSGFInputFileLineCount, string strMSGFInputFilePath, string strMSGFResultsFilePath)
        {
            bool blnSuccess;
            bool blnUseSegments;
            string strSegmentUsageMessage;

            var intMSGFEntriesPerSegment = m_jobParams.GetJobParameter("MSGFEntriesPerSegment", MSGF_SEGMENT_ENTRY_COUNT);
            if (m_DebugLevel >= 2)
            {
                LogDebug("MSGFInputFileLineCount = " + intMSGFInputFileLineCount + "; MSGFEntriesPerSegment = " + intMSGFEntriesPerSegment);
            }

            if (intMSGFEntriesPerSegment <= 1)
            {
                blnUseSegments = false;
                strSegmentUsageMessage = "Not using MSGF segments since MSGFEntriesPerSegment is <= 1";
            }
            else if (intMSGFInputFileLineCount <= intMSGFEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN)
            {
                blnUseSegments = false;
                strSegmentUsageMessage = "Not using MSGF segments since MSGFInputFileLineCount is <= " + intMSGFEntriesPerSegment + " * " +
                                         (int)(MSGF_SEGMENT_OVERFLOW_MARGIN * 100) + "%";
            }
            else
            {
                blnUseSegments = true;
                strSegmentUsageMessage = "Using MSGF segments";
            }

            mMSGFLineCountPreviousSegments = 0;
            mMSGFInputFileLineCount = intMSGFInputFileLineCount;
            m_progress = PROGRESS_PCT_MSGF_START;

            if (!blnUseSegments)
            {
                if (m_DebugLevel >= 2)
                {
                    LogMessage(strSegmentUsageMessage);
                }

                // Do not use segments
                blnSuccess = RunMSGFWork(strMSGFInputFilePath, strMSGFResultsFilePath);
            }
            else
            {
                var lstSegmentFileInfo = new List<udtSegmentFileInfoType>();
                var lstResultFiles = new List<string>();

                // Split strMSGFInputFilePath into chunks with intMSGFEntriesPerSegment each
                blnSuccess = SplitMSGFInputFile(intMSGFInputFileLineCount, strMSGFInputFilePath, intMSGFEntriesPerSegment, lstSegmentFileInfo);

                if (m_DebugLevel >= 2)
                {
                    LogMessage(
                        strSegmentUsageMessage + "; segment count = " + lstSegmentFileInfo.Count);
                }

                if (blnSuccess)
                {
                    // Call MSGF for each segment
                    foreach (var udtSegmentFile in lstSegmentFileInfo)
                    {
                        var strResultFile = AddFileNameSuffix(strMSGFResultsFilePath, udtSegmentFile.Segment);

                        blnSuccess = RunMSGFWork(udtSegmentFile.FilePath, strResultFile);

                        if (!blnSuccess)
                            break;

                        lstResultFiles.Add(strResultFile);
                        mMSGFLineCountPreviousSegments += udtSegmentFile.Entries;
                    }
                }

                if (blnSuccess)
                {
                    // Combine the results
                    blnSuccess = CombineMSGFResultFiles(strMSGFResultsFilePath, lstResultFiles);
                }

                if (blnSuccess)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogDebug("Deleting MSGF segment files");
                    }

                    // Delete the segment files
                    foreach (var udtSegmentFile in lstSegmentFileInfo)
                    {
                        DeleteTemporaryfile(udtSegmentFile.FilePath);
                    }

                    // Delete the result files
                    foreach (var strResultFile in lstResultFiles)
                    {
                        DeleteTemporaryfile(strResultFile);
                    }
                }
            }

            try
            {
                // Delete the Console_Output.txt file if it is empty
                var fiConsoleOutputFile = new FileInfo(Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT));
                if (fiConsoleOutputFile.Exists && fiConsoleOutputFile.Length == 0)
                {
                    fiConsoleOutputFile.Delete();
                }
            }
            catch (Exception ex)
            {
                LogWarning("Unable to delete the " + MSGF_CONSOLE_OUTPUT + " file: " + ex);
            }

            return blnSuccess;
        }

        private bool RunMSGFWork(string strInputFilePath, string strResultsFilePath)
        {
            if (string.IsNullOrEmpty(strInputFilePath))
            {
                LogError("strInputFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(strResultsFilePath))
            {
                LogError("strResultsFilePath has not been defined; unable to continue");
                return false;
            }

            // Delete the output file if it already exists (MSGFDB will not overwrite it)
            if (File.Exists(strResultsFilePath))
            {
                File.Delete(strResultsFilePath);
            }

            // If an MSGF analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java
            // Customize this on a per-job basis using the MSGFJavaMemorySize setting in the settings file
            // (job 611216 succeeded with a value of 5000)
            var intJavaMemorySize = m_jobParams.GetJobParameter("MSGFJavaMemorySize", 2000);
            if (intJavaMemorySize < 512)
                intJavaMemorySize = 512;

            if (m_DebugLevel >= 1)
            {
                LogMessage("Running MSGF on " + Path.GetFileName(strInputFilePath));
            }

            mCurrentMSGFResultsFilePath = string.Copy(strResultsFilePath);

            m_StatusTools.CurrentOperation = "Running MSGF";
            m_StatusTools.UpdateAndWrite(m_progress);

            var CmdStr = " -Xmx" + intJavaMemorySize.ToString() + "M ";

            if (mUsingMSGFDB)
            {
                CmdStr += "-cp " + PossiblyQuotePath(mMSGFProgLoc) + " ui.MSGF";
            }
            else
            {
                CmdStr += "-jar " + PossiblyQuotePath(mMSGFProgLoc);
            }

            CmdStr += " -i " + PossiblyQuotePath(strInputFilePath);
            // Input file
            CmdStr += " -d " + PossiblyQuotePath(m_WorkDir);
            // Folder containing .mzXML, .mzML, or .mgf file
            CmdStr += " -o " + PossiblyQuotePath(strResultsFilePath);
            // Output file

            // MSGF v6432 and earlier use -m 0 for CID and -m 1 for ETD
            // MSGFDB v7097 and later use:
            //   -m 0 means as written in the spectrum or CID if no info
            //   -m 1 means CID
            //   -m 2 means ETD
            //   -m 3 means HCD

            var intMSGFDBVersion = int.MaxValue;

            if (mUsingMSGFDB)
            {
                if (!string.IsNullOrEmpty(mMSGFDBVersion) && mMSGFDBVersion.StartsWith("v"))
                {
                    if (int.TryParse(mMSGFDBVersion.Substring(1), out intMSGFDBVersion))
                    {
                        // Using a specific version of MSGFDB
                        // intMSGFDBVersion should now be something like 6434, 6841, 6964, 7097 etc.
                    }
                    else
                    {
                        // Unable to parse out an integer from mMSGFDBVersion
                        intMSGFDBVersion = int.MaxValue;
                    }
                }
            }

            if (mUsingMSGFDB && intMSGFDBVersion >= 7097)
            {
                // Always use -m 0 (assuming we're sending an mzXML file to MSGFDB)
                CmdStr += " -m 0";
                // as-written in the input file
            }
            else
            {
                if (mETDMode)
                {
                    CmdStr += " -m 1";
                    // ETD fragmentation
                }
                else
                {
                    CmdStr += " -m 0";
                    // CID fragmentation
                }
            }

            CmdStr += " -e 1";
            // Enzyme is Trypsin; other supported enzymes are 2: Chymotrypsin, 3: Lys-C, 4: Lys-N, 5: Glu-C, 6: Arg-C, 7: Asp-N, and 8: aLP
            CmdStr += " -fixMod 0";
            // No fixed mods on cysteine
            CmdStr += " -x 0";
            // Write out all matches for each spectrum
            CmdStr += " -p 1";
            // SpecProbThreshold threshold of 1, i.e., do not filter results by the computed SpecProb value

            LogDebug(mJavaProgLoc + " " + CmdStr);

            mMSGFRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = false,
                CacheStandardOutput = false,
                EchoOutputToConsole = false,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT)
            };
            RegisterEvents(mMSGFRunner);
            mMSGFRunner.LoopWaiting += MSGFRunner_LoopWaiting;

            var blnSuccess = mMSGFRunner.RunProgram(mJavaProgLoc, CmdStr, "MSGF", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFVersion))
                {
                    var fiConsoleOutputfile = new FileInfo(Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT));
                    if (fiConsoleOutputfile.Length == 0)
                    {
                        // File is 0-bytes; delete it
                        DeleteTemporaryfile(fiConsoleOutputfile.FullName);
                    }
                    else
                    {
                        ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT));
                    }
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                LogError("Error running MSGF, job " + m_JobNum);
            }

            return blnSuccess;
        }

        private bool CombineMSGFResultFiles(string strMSGFOutputFilePath, List<string> lstResultFiles)
        {
            try
            {

                // Create the output file
                using (var swOutFile = new StreamWriter(new FileStream(strMSGFOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Step through the input files and append the results
                    var blnHeaderWritten = false;
                    foreach (var strResultFile in lstResultFiles)
                    {
                        using (var srInFile = new StreamReader(new FileStream(strResultFile, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            var intLinesRead = 0;
                            while (!srInFile.EndOfStream)
                            {
                                var strLineIn = srInFile.ReadLine();
                                intLinesRead += 1;

                                if (!blnHeaderWritten)
                                {
                                    blnHeaderWritten = true;
                                    swOutFile.WriteLine(strLineIn);
                                }
                                else
                                {
                                    if (intLinesRead > 1)
                                    {
                                        swOutFile.WriteLine(strLineIn);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception combining MSGF result files", ex);
                return false;
            }

            return true;
        }

        private bool LoadMSGFResults(string strMSGFResultsFilePath, out Dictionary<int, string> lstMSGFResults)
        {

            lstMSGFResults = new Dictionary<int, string>();

            try
            {
                var blnSuccess = true;

                var intMSGFSpecProbColIndex = -1;
                using (var srInFile = new StreamReader(new FileStream(strMSGFResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                            continue;

                        var strSplitLine = strLineIn.Split();

                        if (strSplitLine.Length <= 0)
                            continue;

                        if (intMSGFSpecProbColIndex < 0)
                        {
                            // Assume this is the headerline, look for SpecProb
                            for (var intIndex = 0; intIndex <= strSplitLine.Length - 1; intIndex++)
                            {
                                if (String.Equals(strSplitLine[intIndex], "SpecProb", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    intMSGFSpecProbColIndex = intIndex;
                                    break;
                                }
                            }

                            if (intMSGFSpecProbColIndex < 0)
                            {
                                // Match not found; abort
                                LogError("SpecProb column not found in file " + strMSGFResultsFilePath);
                                blnSuccess = false;
                                break;
                            }
                        }
                        else
                        {
                            // Data line
                            int intResultID;
                            if (int.TryParse(strSplitLine[0], out intResultID))
                            {
                                if (intMSGFSpecProbColIndex < strSplitLine.Length)
                                {
                                    try
                                    {
                                        lstMSGFResults.Add(intResultID, strSplitLine[intMSGFSpecProbColIndex]);
                                    }
                                    catch (Exception)
                                    {
                                        // Ignore errors here
                                        // Possibly a key violation or a column index issue
                                    }
                                }
                            }
                        }
                    }
                }

                return blnSuccess;
            }
            catch (Exception ex)
            {
                LogError("Exception in LoadMSGFResults: " + ex.Message, ex);
                return false;
            }
        }

        /// <summary>
        /// Parse the MSGF console output file to determine the MSGF version
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example console output
            // MSGF v7097 (12/29/2011)
            // MS-GF complete (total elapsed time: 507.68 sec)

            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        if (intLinesRead <= 3 && string.IsNullOrWhiteSpace(mMSGFVersion) && strLineIn.StartsWith("MSGF v"))
                        {
                            // Originally the first line was the MSGF version
                            // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                            // The third line is the MSGF version

                            if (m_DebugLevel >= 2)
                            {
                                LogDebug("MSGF version: " + strLineIn);
                            }

                            mMSGFVersion = string.Copy(strLineIn);
                        }
                        else
                        {
                            if (strLineIn.ToLower().Contains("error"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running MSGF:";
                                }
                                mConsoleOutputErrorMsg += "; " + strLineIn;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message, ex);
                }
            }
        }

        private bool SplitMSGFInputFile(int intMSGFinputFileLineCount, string strMSGFInputFilePath, int intMSGFEntriesPerSegment,
            List<udtSegmentFileInfoType> lstSegmentFileInfo)
        {
            var intLinesRead = 0;
            var strHeaderLine = string.Empty;

            var intLineCountAllSegments = 0;

            try
            {
                lstSegmentFileInfo.Clear();
                if (intMSGFEntriesPerSegment < 100)
                    intMSGFEntriesPerSegment = 100;

                using (var srInFile = new StreamReader(new FileStream(strMSGFInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    StreamWriter swOutFile = null;

                    udtSegmentFileInfoType udtThisSegment;
                    udtThisSegment.FilePath = string.Empty;
                    udtThisSegment.Entries = 0;
                    udtThisSegment.Segment = 0;

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (intLinesRead == 1)
                        {
                            // This is the header line; cache it so that we can write it out to the top of each input file
                            strHeaderLine = string.Copy(strLineIn);
                        }

                        if (udtThisSegment.Segment == 0 || udtThisSegment.Entries >= intMSGFEntriesPerSegment)
                        {
                            // Need to create a new segment
                            // However, if the number of lines remaining to be written is less than 5% of intMSGFEntriesPerSegment then keep writing to this segment

                            var intLineCountRemaining = intMSGFinputFileLineCount - intLineCountAllSegments;

                            if (udtThisSegment.Segment == 0 || intLineCountRemaining > intMSGFEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN)
                            {
                                if (udtThisSegment.Segment > 0)
                                {
                                    // Close the current segment
                                    swOutFile.Close();
                                    lstSegmentFileInfo.Add(udtThisSegment);
                                }

                                // Initialize a new segment
                                udtThisSegment.Segment += 1;
                                udtThisSegment.Entries = 0;
                                udtThisSegment.FilePath = AddFileNameSuffix(strMSGFInputFilePath, udtThisSegment.Segment);

                                swOutFile = new StreamWriter(new FileStream(udtThisSegment.FilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                                // Write the header line to the new segment
                                swOutFile.WriteLine(strHeaderLine);
                            }
                        }

                        if (intLinesRead > 1)
                        {
                            swOutFile.WriteLine(strLineIn);
                            udtThisSegment.Entries += 1;
                            intLineCountAllSegments += 1;
                        }
                    }

                    // Close the the output files
                    swOutFile.Close();
                    lstSegmentFileInfo.Add(udtThisSegment);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception splitting MSGF input file", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSGFVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(mMSGFProgLoc),
                new FileInfo(mMSXmlGeneratorAppPath)
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database when using MODa or MSGF+ probabilities to create the MSGF files
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfoPrecomputedProbabilities(clsPHRPReader.ePeptideHitResultType eResultType)
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of AnalysisManagerMSGFPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerMSGFPlugin"))
            {
                return false;
            }

            var ioToolFiles = new List<FileInfo>();

            if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                // Store the path to MSGFDB.jar
                ioToolFiles.Add(new FileInfo(mMSGFProgLoc));
            }

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        private bool SummarizeMSGFResults(clsPHRPReader.ePeptideHitResultType eResultType)
        {
            bool blnSuccess;

            try
            {
                // Gigasax.DMS5
                var strConnectionString = m_mgrParams.GetParam("connectionstring");
                int intJobNumber;
                bool blnPostResultsToDB;

                if (int.TryParse(m_JobNum, out intJobNumber))
                {
                    blnPostResultsToDB = true;
                }
                else
                {
                    blnPostResultsToDB = false;
                    m_message = "Job number is not numeric: " + m_JobNum + "; will not be able to post PSM results to the database";
                    LogError(m_message);
                }

                var objSummarizer = new clsMSGFResultsSummarizer(eResultType, m_Dataset, intJobNumber, m_WorkDir, strConnectionString, m_DebugLevel);
                RegisterEvents(objSummarizer, writeDebugEventsToLog: true);

                objSummarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

                objSummarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD;

                objSummarizer.ContactDatabase = true;
                objSummarizer.PostJobPSMResultsToDB = blnPostResultsToDB;
                objSummarizer.SaveResultsToTextFile = false;
                objSummarizer.DatasetName = m_Dataset;

                blnSuccess = objSummarizer.ProcessMSGFResults();

                if (!blnSuccess)
                {
                    var errMsg = "Error calling ProcessMSGFResults";
                    m_message = clsGlobal.AppendToComment(m_message, errMsg);
                    if (objSummarizer.ErrorMessage.Length > 0)
                    {
                        errMsg += ": " + objSummarizer.ErrorMessage;
                    }

                    errMsg += "; input file name: " + objSummarizer.MSGFSynopsisFileName;

                    LogError(errMsg);
                }
                else
                {
                    if (objSummarizer.DatasetScanStatsLookupError)
                    {
                        m_message = clsGlobal.AppendToComment(m_message, objSummarizer.ErrorMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                var errMsg = "Exception summarizing the MSGF results";
                m_message = clsGlobal.AppendToComment(m_message, errMsg);
                LogError(errMsg + ": " + ex.Message, ex);
                return false;
            }

            return blnSuccess;
        }

        private int intErrorCount;

        private void UpdateMSGFProgress(string strMSGFResultsFilePath)
        {
            try
            {
                if (mMSGFInputFileLineCount <= 0)
                    return;
                if (!File.Exists(strMSGFResultsFilePath))
                    return;

                // Read the data from the results file
                int intLineCount;
                using (var srMSGFResultsFile = new StreamReader(new FileStream(strMSGFResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLineCount = 0;

                    while (!srMSGFResultsFile.EndOfStream)
                    {
                        srMSGFResultsFile.ReadLine();
                        intLineCount += 1;
                    }
                }

                // Update the overall progress
                double dblFraction = (intLineCount + mMSGFLineCountPreviousSegments) / (float)mMSGFInputFileLineCount;

                if (mProcessingMSGFDBCollisionModeData)
                {
                    // Running MSGF twice; first for CID spectra and then for ETD spectra
                    // Divide the progress by 2, then add 0.5 if we're on the second iteration

                    dblFraction = dblFraction / 2.0;
                    if (mCollisionModeIteration > 1)
                    {
                        dblFraction = dblFraction + 0.5;
                    }
                }

                m_progress = (float)(PROGRESS_PCT_MSGF_START + (PROGRESS_PCT_MSGF_COMPLETE - PROGRESS_PCT_MSGF_START) * dblFraction);
                m_StatusTools.UpdateAndWrite(m_progress);
            }
            catch (Exception ex)
            {
                // Log errors the first 3 times they occur
                intErrorCount += 1;
                if (intErrorCount <= 3)
                {
                    LogError("Error counting the number of lines in the MSGF results file, " + strMSGFResultsFilePath, ex);
                }
            }
        }

        private bool UpdateProteinModsFile(clsPHRPReader.ePeptideHitResultType eResultType, string strMSGFResultsFilePath)
        {
            bool blnSuccess;

            try
            {
                LogDebug("Contact clsPHRPReader.GetPHRPProteinModsFileName for eResultType " + eResultType, 3);

                var fiProteinModsFile = new FileInfo(Path.Combine(m_WorkDir, clsPHRPReader.GetPHRPProteinModsFileName(eResultType, m_Dataset)));
                var fiProteinModsFileNew = new FileInfo(fiProteinModsFile.FullName + ".tmp");

                if (!fiProteinModsFile.Exists)
                {
                    LogWarning("PHRP ProteinMods.txt file not found: " + fiProteinModsFile.Name);
                    return true;
                }

                LogDebug("Load MSGFResults from " + strMSGFResultsFilePath, 3);

                Dictionary <int, string> lstMSGFResults;
                blnSuccess = LoadMSGFResults(strMSGFResultsFilePath, out lstMSGFResults);
                if (!blnSuccess)
                {
                    return false;
                }

                var intMSGFSpecProbColIndex = -1;
                blnSuccess = true;

                LogDebug("Read " + fiProteinModsFile.FullName + " and create " + fiProteinModsFileNew.FullName, 3);

                using (var srSource = new StreamReader(new FileStream(fiProteinModsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var swTarget = new StreamWriter(new FileStream(fiProteinModsFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!srSource.EndOfStream)
                    {
                        var strLineIn = srSource.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                        {
                            swTarget.WriteLine();
                            continue;
                        }

                        var strSplitLine = strLineIn.Split().ToList();

                        if (strSplitLine.Count <= 0)
                        {
                            swTarget.WriteLine();
                            continue;
                        }

                        if (intMSGFSpecProbColIndex < 0)
                        {
                            // Assume this is the header line, look for MSGF_SpecProb
                            for (var intIndex = 0; intIndex <= strSplitLine.Count - 1; intIndex++)
                            {
                                if (string.Equals(strSplitLine[intIndex], "MSGF_SpecProb", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    intMSGFSpecProbColIndex = intIndex;
                                    break;
                                }
                            }

                            if (intMSGFSpecProbColIndex < 0)
                            {
                                // Match not found; abort
                                blnSuccess = false;
                                break;
                            }
                        }
                        else
                        {
                            // Data line; determine the ResultID
                            int intResultID;
                            if (int.TryParse(strSplitLine[0], out intResultID))
                            {
                                // Lookup the MSGFSpecProb value for this ResultID
                                string strMSGFSpecProb;
                                if (lstMSGFResults.TryGetValue(intResultID, out strMSGFSpecProb))
                                {
                                    // Only update the value if strMSGFSpecProb is a number
                                    double dblValue;
                                    if (double.TryParse(strMSGFSpecProb, out dblValue))
                                    {
                                        strSplitLine[intMSGFSpecProbColIndex] = strMSGFSpecProb;
                                    }
                                }
                            }
                        }

                        swTarget.WriteLine(clsGlobal.CollapseList(strSplitLine));
                    }
                }

                if (blnSuccess)
                {
                    // Replace the original file with the new one
                    Thread.Sleep(200);
                    clsProgRunner.GarbageCollectNow();

                    try
                    {
                        LogDebug("Replace " + fiProteinModsFile.FullName + " with " + fiProteinModsFileNew.Name, 3);

                        fiProteinModsFile.Delete();

                        try
                        {
                            fiProteinModsFileNew.MoveTo(fiProteinModsFile.FullName);
                            if (m_DebugLevel >= 2)
                            {
                                LogMessage("Updated MSGF_SpecProb values in the ProteinMods.txt file");
                            }

                            blnSuccess = true;
                        }
                        catch (Exception ex)
                        {
                            var msg = "Error updating the ProteinMods.txt file; cannot rename new version";
                            m_message = clsGlobal.AppendToComment(m_message, msg);
                            LogError(msg + ": " + ex.Message, ex);
                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        var msg = "Error updating the ProteinMods.txt file; cannot delete old version";
                        m_message = clsGlobal.AppendToComment(m_message, msg);
                        LogError(msg + ": " + ex.Message, ex);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception updating the ProteinMods.txt file";
                m_message = clsGlobal.AppendToComment(m_message, msg);
                LogError(msg + ": " + ex.Message, ex);
                return false;
            }

            return blnSuccess;
        }

        #endregion

        #region "Event Handlers"

        private void mMSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);

            LogProgress("MSGF");
        }

        /// <summary>
        /// Event handler for Error Events reported by the MSGF Input Creator
        /// </summary>
        /// <param name="strErrorMessage"></param>
        /// <remarks></remarks>
        private void mMSGFInputCreator_ErrorEvent(string strErrorMessage)
        {
            mMSGFInputCreatorErrorCount += 1;
            if (mMSGFInputCreatorErrorCount < 10 || mMSGFInputCreatorErrorCount % 1000 == 0)
            {
                LogError("Error reported by MSGFInputCreator; " + strErrorMessage + " (ErrorCount=" + mMSGFInputCreatorErrorCount + ")");
            }
        }

        /// <summary>
        /// Event handler for Warning Events reported by the MSGF Input Creator
        /// </summary>
        /// <param name="strWarningMessage"></param>
        /// <remarks></remarks>
        private void mMSGFInputCreator_WarningEvent(string strWarningMessage)
        {
            mMSGFInputCreatorWarningCount += 1;
            if (mMSGFInputCreatorWarningCount < 10 || mMSGFInputCreatorWarningCount % 1000 == 0)
            {
                LogWarning("Warning reported by MSGFInputCreator; " + strWarningMessage + " (WarnCount=" + mMSGFInputCreatorWarningCount + ")");
            }
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        private void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            if (Message.ToLower().Contains("permission was denied"))
            {
                LogErrorToDatabase(errorMessage);
            }
        }

        private DateTime dtLastUpdateTime = DateTime.MinValue;
        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler that fires while MSGF is processing
        /// </summary>
        /// <remarks></remarks>
        private void MSGFRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20)
            {
                // Update the MSGF progress by counting the number of lines in the _MSGF.txt file
                UpdateMSGFProgress(mCurrentMSGFResultsFilePath);

                dtLastUpdateTime = DateTime.UtcNow;
            }

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSGF_CONSOLE_OUTPUT));
                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }
            }
        }

        #endregion
    }
}
