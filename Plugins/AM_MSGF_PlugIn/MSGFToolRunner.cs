//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
//*********************************************************************************************************

using AnalysisManagerBase;
using AnalysisManagerMsXmlGenPlugIn;
using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using MSGFResultsSummarizer;
using PRISM.Logging;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// Primary class for running MSGF
    /// </summary>
    public class MSGFRunner : AnalysisToolRunnerBase
    {
        // ReSharper disable once CommentTypo

        // Ignore Spelling: Arg, bioml, Chymotrypsin, cp, cysteine, Da, FHT, Glu, Lys
        // Ignore Spelling: MODa, modp, MSGF< msgfdb, Parm, PHRP, Pre, Xmx

        private const int PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD = 2;
        private const int PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED = 3;
        private const int PROGRESS_PCT_MSXML_GEN_RUNNING = 6;
        private const int PROGRESS_PCT_MZXML_CREATED = 10;
        private const int PROGRESS_PCT_MSGF_START = PROGRESS_PCT_MZXML_CREATED;
        private const int PROGRESS_PCT_MSGF_COMPLETE = 95;
        private const int PROGRESS_PCT_MSGF_POST_PROCESSING = 97;

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

        private struct SegmentFileInfo
        {
            /// <summary>
            /// Segment number
            /// </summary>
            public int Segment;

            /// <summary>
            /// Full path to the file
            /// </summary>
            public string FilePath;

            /// <summary>
            /// Number of entries in this segment
            /// </summary>
            public int Entries;
        }

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

        private MSXMLCreator mMSXmlCreator;

        private bool mUsingMSGFDB = true;
        private string mMSGFDBVersion = "Unknown";

        private string mJavaProgLoc = string.Empty;

        private string mConsoleOutputErrorMsg;

        private MSGFInputCreator mMSGFInputCreator;

        private RunDosProgram mMSGFRunner;

        private int mMSGFInputCreatorErrorCount;
        private int mMSGFInputCreatorWarningCount;

        private bool mPostProcessingError;

        /// <summary>
        /// Runs MSGF
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Call base class for initial setup
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var mgfInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            // Determine the raw data type
            var rawDataType = AnalysisResources.GetRawDataType(mJobParams.GetParam("RawDataType"));

            // Resolve resultType
            var resultTypeName = AnalysisResources.GetResultType(mJobParams);

            var resultType = ReaderFactory.GetPeptideHitResultType(resultTypeName);

            if (resultType == PeptideHitResultTypes.Unknown)
            {
                // Result type is not supported
                LogError("ResultType is not supported by MSGF in MSGFToolRunner: " + resultTypeName);
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

            mKeepMSGFInputFiles = mJobParams.GetJobParameter("KeepMSGFInputFile", false);
            var doNotFilterPeptides = mJobParams.GetJobParameter("MSGFIgnoreFilters", false);

            mPostProcessingError = false;

            try
            {
                var processingError = false;

                if (mUsingMSGFDB && resultType == PeptideHitResultTypes.MSGFPlus)
                {
                    // Analysis tool is MS-GF+ so we don't actually need to run the MSGF re-scorer
                    // Simply copy the values from the MSGFDB result file

                    StoreToolVersionInfoPrecomputedProbabilities(resultType);

                    if (!CreateMSGFResultsFromMSGFPlusResults())
                    {
                        processingError = true;
                    }
                }
                else if (resultType == PeptideHitResultTypes.MODa)
                {
                    // Analysis tool is MODa, which MSGF does not support
                    // Instead, summarize the MODa results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(resultType);

                    if (!SummarizeMODaResults())
                    {
                        processingError = true;
                    }
                }
                else if (resultType == PeptideHitResultTypes.MODPlus)
                {
                    // Analysis tool is MODPlus, which MSGF does not support
                    // Instead, summarize the MODPlus results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(resultType);

                    if (!SummarizeMODPlusResults())
                    {
                        processingError = true;
                    }
                }
                else if (resultType == PeptideHitResultTypes.MSPathFinder)
                {
                    // Analysis tool is MSPathFinder, which MSGF does not support
                    // Instead, summarize the MSPathFinder results using FDR alone

                    StoreToolVersionInfoPrecomputedProbabilities(resultType);

                    if (!SummarizeMSPathFinderResults())
                    {
                        processingError = true;
                    }
                }
                else if (resultType == PeptideHitResultTypes.MaxQuant)
                {
                    // Analysis tool is MaxQuant, which could have multiple datasets
                    // This plugin does not include the required logic for running MSGF against a multi-dataset result file

                    mMessage = "The MSGF step tool does not support MaxQuant";
                    processingError = true;
                }
                else
                {
                    if (!ProcessFilesWrapper(rawDataType, resultType, doNotFilterPeptides, mgfInstrumentData))
                    {
                        processingError = true;
                    }

                    if (!processingError)
                    {
                        // Post-process the MSGF output file to create two new MSGF result files, one for the synopsis file and one for the first-hits file
                        // Will also make sure that every peptide has a numeric SpecProb value
                        // For peptides where MSGF reported an error, the MSGF SpecProb will be set to 1
                        // If the Instrument Data was a .MGF file, we need to update the scan numbers using mMSGFInputCreator.GetScanByMGFSpectrumIndex()

                        // Sleep for 1 second to give the MSGF results file a chance to finalize
                        Global.IdleLoop(1);

                        var success = PostProcessMSGFResults(resultType, mMSGFResultsFilePath, mgfInstrumentData);

                        if (!success)
                        {
                            if (string.IsNullOrWhiteSpace(mMessage))
                                mMessage = "MSGF results file post-processing error";
                            mPostProcessingError = true;
                        }
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Global.IdleLoop(0.5);
                AppUtils.GarbageCollectNow();

                if (processingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var copySuccess = CopyResultsToTransferDirectory();

                if (!copySuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (mPostProcessingError)
                {
                    // When a post-processing error occurs, we copy the files to the server, but return CLOSEOUT_FAILED
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running MSGF", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we get to here, everything worked so exit happily
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private string AddFileNameSuffix(string filePath, int suffix)
        {
            return AddFileNameSuffix(filePath, suffix.ToString());
        }

        private string AddFileNameSuffix(string filePath, string suffix)
        {
            var file = new FileInfo(filePath);
            var newFileName = Path.GetFileNameWithoutExtension(file.Name) + "_" + suffix + file.Extension;

            return file.DirectoryName == null
                       ? newFileName
                       : Path.Combine(file.DirectoryName, newFileName);
        }

        /// <summary>
        /// Examines the SEQUEST, X!Tandem, Inspect, or MS-GF+ param file to determine if ETD mode is enabled
        /// </summary>
        /// <param name="resultType"></param>
        /// <param name="searchToolParamFilePath"></param>
        /// <returns>True if success, false if an error</returns>
        private bool CheckETDModeEnabled(PeptideHitResultTypes resultType, string searchToolParamFilePath)
        {
            mETDMode = false;
            var success = false;

            if (string.IsNullOrEmpty(searchToolParamFilePath))
            {
                LogError("PeptideHit param file path is empty; unable to continue");
                return false;
            }

            mStatusTools.CurrentOperation = "Checking whether ETD mode is enabled";

            switch (resultType)
            {
                case PeptideHitResultTypes.Sequest:
                    success = CheckETDModeEnabledSequest(searchToolParamFilePath);
                    break;

                case PeptideHitResultTypes.XTandem:
                    success = CheckETDModeEnabledXTandem(searchToolParamFilePath);
                    break;

                case PeptideHitResultTypes.Inspect:
                    LogDebug("Inspect does not support ETD data processing; will set mETDMode to false");
                    success = true;
                    break;

                case PeptideHitResultTypes.MSGFPlus:
                    success = CheckETDModeEnabledMSGFPlus(searchToolParamFilePath);
                    break;

                case PeptideHitResultTypes.MODa:
                    LogDebug("MODa does not support ETD data processing; will set mETDMode to false");
                    success = true;
                    break;

                case PeptideHitResultTypes.MODPlus:
                    LogDebug("MODPlus does not support ETD data processing; will set mETDMode to false");
                    success = true;
                    break;

                case PeptideHitResultTypes.MSPathFinder:
                    LogDebug("MSPathFinder does not support ETD data processing; will set mETDMode to false");
                    success = true;
                    break;
            }

            if (mETDMode)
            {
                LogDebug("ETD search mode has been enabled since c and z ions were used for the peptide search");
            }

            return success;
        }

        /// <summary>
        /// Examines the MS-GF+ param file to determine if ETD mode is enabled
        /// If it is, sets mETDMode to true
        /// </summary>
        /// <param name="searchToolParamFilePath">MS-GF+ parameter file to read</param>
        /// <returns>True if success, false if an error</returns>
        private bool CheckETDModeEnabledMSGFPlus(string searchToolParamFilePath)
        {
            const string MSGFPLUS_FRAG_METHOD_TAG = "FragmentationMethodID";

            try
            {
                mETDMode = false;

                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the MS-GF+ parameter file: " + searchToolParamFilePath);
                }

                // Read the data from the MS-GF+ Param file
                using var reader = new StreamReader(new FileStream(searchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine) || !dataLine.StartsWith(MSGFPLUS_FRAG_METHOD_TAG))
                        continue;

                    // Check whether this line is FragmentationMethodID=2
                    // Note that FragmentationMethodID=4 means Merge spectra from the same precursor (e.g. CID/ETD pairs, CID/HCD/ETD triplets)
                    // This mode is not yet supported

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("MS-GF+ " + MSGFPLUS_FRAG_METHOD_TAG + " line found: " + dataLine);
                    }

                    // Look for the equals sign
                    var charIndex = dataLine.IndexOf('=');

                    if (charIndex > 0)
                    {
                        var fragModeText = dataLine.Substring(charIndex + 1).Trim();

                        if (int.TryParse(fragModeText, out var fragMode))
                        {
                            if (fragMode == 2)
                            {
                                mETDMode = true;
                            }
                            else if (fragMode == 4)
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
                        LogWarning("MS-GF+ " + MSGFPLUS_FRAG_METHOD_TAG + " line does not have an equals sign; " +
                                   "will assume not using ETD ions: " + dataLine);
                    }

                    // No point in checking any further since we've parsed the FragmentationMethodID line
                    break;
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading the MS-GF+ param file", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Examines the SEQUEST param file to determine if ETD mode is enabled
        /// If it is, sets mETDMode to true
        /// </summary>
        /// <param name="searchToolParamFilePath">SEQUEST parameter file to read</param>
        /// <returns>True if success, false if an error</returns>
        private bool CheckETDModeEnabledSequest(string searchToolParamFilePath)
        {
            const string SEQUEST_ION_SERIES_TAG = "ion_series";

            try
            {
                mETDMode = false;

                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the SEQUEST parameter file: " + searchToolParamFilePath);
                }

                // Read the data from the SEQUEST Param file
                using var reader = new StreamReader(new FileStream(searchToolParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine) || !dataLine.StartsWith(SEQUEST_ION_SERIES_TAG))
                        continue;

                    // This is the ion_series line
                    // If ETD mode is enabled, c and z ions will have a 1 in this series of numbers:
                    // ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0

                    // The key to parsing this data is:
                    // ion_series = - - -  a   b   c  --- --- ---  x   y   z
                    // ion_series = 0 1 1 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 1.0

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("SEQUEST " + SEQUEST_ION_SERIES_TAG + " line found: " + dataLine);
                    }

                    // Look for the equals sign
                    var charIndex = dataLine.IndexOf('=');

                    if (charIndex > 0)
                    {
                        var ionWeightText = dataLine.Substring(charIndex + 1).Trim();

                        // Split ionWeightText on spaces
                        var ionWeights = ionWeightText.Split(' ');

                        if (ionWeights.Length >= 12)
                        {
                            double.TryParse(ionWeights[5], out var cWeight);
                            double.TryParse(ionWeights[11], out var zWeight);

                            if (mDebugLevel >= 3)
                            {
                                LogDebug("SEQUEST " + SEQUEST_ION_SERIES_TAG + " line" +
                                         " has c-ion weighting = " + cWeight +
                                         " and z-ion weighting = " + zWeight);
                            }

                            if (cWeight > 0 || zWeight > 0)
                            {
                                mETDMode = true;
                            }
                        }
                        else
                        {
                            LogWarning("SEQUEST " + SEQUEST_ION_SERIES_TAG + " line does not have 11 numbers; " +
                                       "will assume not using ETD ions: " + dataLine);
                        }
                    }
                    else
                    {
                        LogWarning("SEQUEST " + SEQUEST_ION_SERIES_TAG + " line does not have an equals sign; " +
                                   "will assume not using ETD ions: " + dataLine);
                    }

                    // No point in checking any further since we've parsed the ion_series line
                    break;
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading the SEQUEST param file", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Examines the X!Tandem param file to determine if ETD mode is enabled
        /// If it is, sets mETDMode to true
        /// </summary>
        /// <param name="searchToolParamFilePath">X!Tandem XML parameter file to read</param>
        /// <returns>True if success, false if an error</returns>
        private bool CheckETDModeEnabledXTandem(string searchToolParamFilePath)
        {
            try
            {
                mETDMode = false;

                if (mDebugLevel >= 2)
                {
                    LogDebug("Reading the X!Tandem parameter file: " + searchToolParamFilePath);
                }

                // Open the parameter file
                // Look for either of these lines:
                //   <note type="input" label="scoring, c ions">yes</note>
                //   <note type="input" label="scoring, z ions">yes</note>

                var paramFile = new XmlDocument
                {
                    PreserveWhitespace = true
                };
                paramFile.Load(searchToolParamFilePath);

                if (paramFile.DocumentElement == null)
                {
                    LogError("Error reading the X!Tandem param file: DocumentElement is null");
                    return false;
                }

                for (var settingIndex = 0; settingIndex <= 1; settingIndex++)
                {
                    var selectedNodes = settingIndex switch
                    {
                        0 => paramFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']"),
                        1 => paramFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']"),
                        _ => null
                    };

                    if (selectedNodes == null)
                    {
                        continue;
                    }

                    for (var matchIndex = 0; matchIndex <= selectedNodes.Count - 1; matchIndex++)
                    {
                        var xmlAttributes = selectedNodes.Item(matchIndex)?.Attributes;

                        // Make sure this node has an attribute named type with value "input"
                        var attributeNode = xmlAttributes?.GetNamedItem("type");

                        if (attributeNode == null)
                        {
                            // Node does not have an attribute named "type"
                            continue;
                        }

                        if (!string.Equals(attributeNode.Value, "input", StringComparison.OrdinalIgnoreCase))
                            continue;

                        // Valid node; examine its InnerText value
                        if (selectedNodes.Item(matchIndex)?.InnerText.ToLower() == "yes")
                        {
                            mETDMode = true;
                        }
                    }

                    if (mETDMode)
                        break;
                }
            }
            catch (Exception ex)
            {
                LogError("Error reading the X!Tandem param file", ex);

                return false;
            }

            return true;
        }

        private bool ConvertMzMLToMzXML()
        {
            mStatusTools.CurrentOperation = "Creating the .mzXML file";

            mMSXmlCreator = new MSXMLCreator(mMSXmlGeneratorAppPath, mWorkDir, mDatasetName, mDebugLevel, mJobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += MSXmlCreator_LoopWaiting;

            var success = mMSXmlCreator.ConvertMzMLToMzXML();

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                mMessage = mMSXmlCreator.ErrorMessage;

                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Unknown error creating the mzXML file";
                }
            }

            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);

            return success;
        }

        /// <summary>
        /// Creates the MSGF Input file by reading SEQUEST, X!Tandem, or Inspect PHRP result file and extracting the relevant information
        /// Uses the ModSummary.txt file to determine the dynamic and static mods used
        /// </summary>
        /// <param name="resultType"></param>
        /// <param name="doNotFilterPeptides"></param>
        /// <param name="mgfInstrumentData"></param>
        /// <param name="msgfInputFileLineCount"></param>
        private bool CreateMSGFInputFile(PeptideHitResultTypes resultType, bool doNotFilterPeptides, bool mgfInstrumentData, out int msgfInputFileLineCount)
        {
            var success = true;

            msgfInputFileLineCount = 0;
            mMSGFInputCreatorErrorCount = 0;
            mMSGFInputCreatorWarningCount = 0;

            // Convert the peptide-hit result file (from PHRP) to a tab-delimited input file to be read by MSGF
            switch (resultType)
            {
                case PeptideHitResultTypes.Sequest:
                    // Convert SEQUEST results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorSequest(mDatasetName, mWorkDir);
                    break;

                case PeptideHitResultTypes.XTandem:
                    // Convert X!Tandem results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorXTandem(mDatasetName, mWorkDir);
                    break;

                case PeptideHitResultTypes.Inspect:
                    // Convert Inspect results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorInspect(mDatasetName, mWorkDir);
                    break;

                case PeptideHitResultTypes.MSGFPlus:
                    // Convert MS-GF+ results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorMSGFDB(mDatasetName, mWorkDir);
                    break;

                case PeptideHitResultTypes.MODa:
                    // Convert MODa results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorMODa(mDatasetName, mWorkDir);
                    break;

                case PeptideHitResultTypes.MODPlus:
                    // Convert MODPlus results to input format required for MSGF
                    mMSGFInputCreator = new MSGFInputCreatorMODPlus(mDatasetName, mWorkDir);
                    break;

                default:
                    // Should never get here; invalid result type specified
                    LogError("Invalid PeptideHit ResultType specified: " + resultType);

                    success = false;
                    break;
            }

            if (!success)
                return false;

            // Register events (do not use RegisterEvents since we only log the first 10 errors/warnings, then every 1000th one after that)
            mMSGFInputCreator.StatusEvent += MSGFInputCreator_StatusEvent;
            mMSGFInputCreator.ErrorEvent += MSGFInputCreator_ErrorEvent;
            mMSGFInputCreator.WarningEvent += MSGFInputCreator_WarningEvent;

            mMSGFInputFilePath = mMSGFInputCreator.MSGFInputFilePath;
            mMSGFResultsFilePath = mMSGFInputCreator.MSGFResultsFilePath;

            mMSGFInputCreator.DoNotFilterPeptides = doNotFilterPeptides;
            mMSGFInputCreator.MgfInstrumentData = mgfInstrumentData;

            mStatusTools.CurrentOperation = "Creating the MSGF Input file";

            if (mDebugLevel >= 3)
            {
                LogDebug("Creating the MSGF Input file");
            }

            success = mMSGFInputCreator.CreateMSGFInputFileUsingPHRPResultFiles();

            msgfInputFileLineCount = mMSGFInputCreator.MSGFInputFileLineCount;

            if (!success)
            {
                LogError("mMSGFInputCreator.MSGFDataFileLineCount returned false");
                return false;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("CreateMSGFInputFileUsingPHRPResultFile complete; " + msgfInputFileLineCount + " lines of data");
            }

            return true;
        }

        private bool SummarizeMODaResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Any results based on a MSGF SpecProb will be meaningless because we didn't run MSGF on the MODa results
            // Post the results to the database
            var success = SummarizeMSGFResults(PeptideHitResultTypes.MODa);

            if (success)
            {
                // We didn't actually run MSGF, so these files aren't needed
                mJobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                mJobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return success;
        }

        private bool SummarizeMODPlusResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Any results based on a MSGF SpecProb will be meaningless because we didn't run MSGF on the MODPlus results
            // Post the results to the database
            var success = SummarizeMSGFResults(PeptideHitResultTypes.MODPlus);

            if (success)
            {
                // We didn't actually run MSGF, so these files aren't needed
                mJobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                mJobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return success;
        }

        private bool SummarizeMSPathFinderResults()
        {
            // Summarize the results to determine the number of peptides and proteins at a given FDR threshold
            // Will use SpecEValue in place of MSGF SpecProb
            // Post the results to the database
            var success = SummarizeMSGFResults(PeptideHitResultTypes.MSPathFinder);

            if (success)
            {
                // We didn't actually run MSGF, so these files aren't needed
                mJobParams.AddResultFileToSkip("MSGF_AnalysisSummary.txt");
                mJobParams.AddResultFileToSkip("Tool_Version_Info_MSGF.txt");
            }

            return success;
        }

        private bool CreateMSGFResultsFromMSGFPlusResults()
        {
            var msgfInputCreator = new MSGFInputCreatorMSGFDB(mDatasetName, mWorkDir);

            if (!CreateMSGFResultsFromMSGFPlusResults(msgfInputCreator, MSGF_PHRP_DATA_SOURCE_SYN.ToLower()))
            {
                return false;
            }

            if (!CreateMSGFResultsFromMSGFPlusResults(msgfInputCreator, MSGF_PHRP_DATA_SOURCE_FHT.ToLower()))
            {
                return false;
            }

            // Summarize the results in the _syn_MSGF.txt file
            // Post the results to the database
            return SummarizeMSGFResults(PeptideHitResultTypes.MSGFPlus);
        }

        private bool CreateMSGFResultsFromMSGFPlusResults(MSGFInputCreatorMSGFDB msgfInputCreator, string synOrFHT)
        {
            var sourceFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus_" + synOrFHT + ".txt");

            if (!File.Exists(sourceFilePath))
            {
                var sourceFilePathAlternate = Path.Combine(mWorkDir, mDatasetName + "_msgfdb_" + synOrFHT + ".txt");

                if (!File.Exists(sourceFilePathAlternate))
                {
                    mMessage = "Input file not found: " + Path.GetFileName(sourceFilePath);
                    return false;
                }
                sourceFilePath = sourceFilePathAlternate;
            }

            var success = msgfInputCreator.CreateMSGFFileUsingMSGFDBSpecProb(sourceFilePath);

            if (!success)
            {
                mMessage = "Error creating MSGF file for " + Path.GetFileName(sourceFilePath);

                if (!string.IsNullOrEmpty(msgfInputCreator.ErrorMessage))
                {
                    mMessage += ": " + msgfInputCreator.ErrorMessage;
                }
                return false;
            }

            return true;
        }

        private bool CreateMzXMLFile()
        {
            mStatusTools.CurrentOperation = "Creating the .mzXML file";

            var mzXmlFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MZXML_EXTENSION);

            if (File.Exists(mzXmlFilePath))
            {
                // File already exists; nothing to do
                return true;
            }

            mMSXmlCreator = new MSXMLCreator(mMSXmlGeneratorAppPath, mWorkDir, mDatasetName, mDebugLevel, mJobParams);
            RegisterEvents(mMSXmlCreator);
            mMSXmlCreator.LoopWaiting += MSXmlCreator_LoopWaiting;

            var success = mMSXmlCreator.CreateMZXMLFile();

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                mMessage = mMSXmlCreator.ErrorMessage;

                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Unknown error creating the mzXML file";
                }
            }

            CopyMzXMLFileToServerCache(mzXmlFilePath, string.Empty, Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath), purgeOldFilesIfNeeded: true);

            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);

            return success;
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
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Error determining MSGF program location");
                }
                return false;
            }

            mMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            return true;
        }

        private string DetermineMSGFProgramLocation()
        {
            var stepToolName = "MSGFDB";
            var progLocManagerParamName = "MSGFDbProgLoc";
            var exeName = MSGFDB_JAR_NAME;

            mUsingMSGFDB = true;

            // Note that as of 12/20/2011 we are using MSGFDB.jar to access the MSGF class
            // In order to allow the old version of MSGF to be run, we must look for parameter MSGF_Version

            // Check whether the settings file specifies that a specific version of the step tool be used
            var msgfStepToolVersion = mJobParams.GetParam("MSGF_Version");

            if (!string.IsNullOrWhiteSpace(msgfStepToolVersion))
            {
                // Specific version is defined
                // Check whether the version is one of the known versions for the old MSGF

                if (IsLegacyMSGFVersion(msgfStepToolVersion))
                {
                    // Use MSGF

                    stepToolName = "MSGF";
                    progLocManagerParamName = "MSGFLoc";
                    exeName = MSGF_JAR_NAME;

                    mUsingMSGFDB = false;
                }
                else
                {
                    // Use MSGFDB
                    mUsingMSGFDB = true;
                    mMSGFDBVersion = msgfStepToolVersion;
                }
            }
            else
            {
                // Use MSGFDB
                mUsingMSGFDB = true;
                mMSGFDBVersion = "Production_Release";
            }

            return DetermineProgramLocation(stepToolName, progLocManagerParamName, exeName,
                msgfStepToolVersion, mMgrParams, out mMessage);
        }

        public static bool IsLegacyMSGFVersion(string stepToolVersion)
        {
            switch (stepToolVersion.ToLower())
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
        /// Compare precursorMassErrorCount to linesRead
        /// </summary>
        /// <param name="linesRead"></param>
        /// <param name="precursorMassErrorCount"></param>
        /// <returns>True if more than 10% of the results have a precursor mass error</returns>
        private bool PostProcessMSGFCheckPrecursorMassErrorCount(int linesRead, int precursorMassErrorCount)
        {
            const int MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT = 10;

            var tooManyPrecursorMassMismatches = false;

            try
            {
                // If 10% or more of the data has a message like "N/A: precursor mass != peptide mass (3571.8857 vs 3581.9849)",
                // set tooManyPrecursorMassMismatches to true

                if (linesRead >= 2 && precursorMassErrorCount > 0)
                {
                    var percentDataPrecursorMassError = precursorMassErrorCount / (float)linesRead * 100f;

                    var msg = string.Format(
                        "{0:F1}% of the data processed by MSGF has a precursor mass 10 or more Da away from the computed peptide mass",
                        percentDataPrecursorMassError);

                    if (percentDataPrecursorMassError >= MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT)
                    {
                        LogError(msg + "; this likely indicates a static or dynamic mod definition is missing from the PHRP _ModSummary.txt file");
                        tooManyPrecursorMassMismatches = true;
                    }
                    else
                    {
                        LogWarning(
                            "{0}; this is below the error threshold of {1}% and thus is only a warning " +
                            "(note that static and dynamic mod info is loaded from the PHRP _ModSummary.txt file)",
                            msg, MAX_ALLOWABLE_PRECURSOR_MASS_ERRORS_PERCENT);
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return tooManyPrecursorMassMismatches;
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
        /// <param name="resultType">PHRP result type</param>
        /// <param name="msgfResultsFilePath">MSGF results file to examine</param>
        /// <param name="mgfInstrumentData">True when the instrument data file is a .mgf file</param>
        /// <returns>True if success; false if one or more errors</returns>
        private bool PostProcessMSGFResults(PeptideHitResultTypes resultType, string msgfResultsFilePath, bool mgfInstrumentData)
        {
            FileInfo inputFile;

            string msgfSynopsisResults;

            bool success;
            bool firstHitsDataPresent;
            bool tooManyErrors;

            try
            {
                if (string.IsNullOrEmpty(msgfResultsFilePath))
                {
                    LogError("MSGF Results File path is empty; unable to continue");
                    return false;
                }

                mStatusTools.CurrentOperation = "MSGF complete; post-processing the results";

                if (mDebugLevel >= 2)
                {
                    LogDebug("MSGF complete; post-processing the results");
                }

                inputFile = new FileInfo(msgfResultsFilePath);

                if (inputFile.Directory == null)
                {
                    LogError("Unable to determine the parent directory of the MSGF results file: " + msgfResultsFilePath);
                    return false;
                }

                // Define the path to write the synopsis MSGF results to
                msgfSynopsisResults = Path.Combine(inputFile.Directory.FullName, Path.GetFileNameWithoutExtension(inputFile.Name) + "_PostProcess.txt");

                mProgress = PROGRESS_PCT_MSGF_POST_PROCESSING;
                mStatusTools.UpdateAndWrite(mProgress);

                success = PostProcessMSGFResultsWork(msgfResultsFilePath, msgfSynopsisResults, mgfInstrumentData, out firstHitsDataPresent, out tooManyErrors);
            }
            catch (Exception ex)
            {
                LogError("Error post-processing the MSGF Results file", ex);
                return false;
            }

            try
            {
                // Now replace the _MSGF.txt file with the _MSGF_PostProcess.txt file
                // For example, replace:
                //   QC_Shew_Dataset_syn_MSGF.txt
                // With the contents of:
                //   QC_Shew_Dataset_syn_MSGF_PostProcess.txt

                Global.IdleLoop(0.5);

                // Delete the original file
                inputFile.Delete();
                Global.IdleLoop(0.5);

                // Rename the _PostProcess.txt file
                var msgfSynFile = new FileInfo(msgfSynopsisResults);

                msgfSynFile.MoveTo(msgfResultsFilePath);
            }
            catch (Exception ex)
            {
                LogError("Error post-processing the MSGF Results file", "Exception replacing the original MSGF Results file with the post-processed one", ex);
                return false;
            }

            if (success)
            {
                // Summarize the results in the _syn_MSGF.txt file
                // Post the results to the database
                LogDebug("Call SummarizeMSGFResults for resultType " + resultType, 3);

                var summarizeSuccess = SummarizeMSGFResults(resultType);

                LogDebug("SummarizeMSGFResults returned " + summarizeSuccess, 3);
            }

            if (success && firstHitsDataPresent)
            {
                // Write out the First-Hits file results
                LogDebug("Call mMSGFInputCreator.CreateMSGFFirstHitsFile", 3);

                success = mMSGFInputCreator.CreateMSGFFirstHitsFile();

                LogDebug("CreateMSGFFirstHitsFile returned " + success, 3);
            }

            if (success && resultType != PeptideHitResultTypes.MSGFPlus)
            {
                LogDebug("Call UpdateProteinModsFile for resultType " + resultType, 3);

                success = UpdateProteinModsFile(resultType, msgfResultsFilePath);

                LogDebug("UpdateProteinModsFile returned " + success, 3);
            }

            if (tooManyErrors)
            {
                return false;
            }

            return success;
        }

        /// <summary>
        /// Process the data in msgfResultsFilePath to create msgfSynopsisResults
        /// </summary>
        /// <param name="msgfResultsFilePath">MSGF Results file path</param>
        /// <param name="msgfSynopsisResults">MSGF synopsis file path</param>
        /// <param name="mgfInstrumentData">True when the instrument data file is a .mgf file</param>
        /// <param name="firstHitsDataPresent">Will be set to true if First-hits data is present</param>
        /// <param name="tooManyErrors">Will be set to true if too many errors occur</param>
        private bool PostProcessMSGFResultsWork(string msgfResultsFilePath, string msgfSynopsisResults, bool mgfInstrumentData,
            out bool firstHitsDataPresent, out bool tooManyErrors)
        {
            const int MAX_ERRORS_TO_LOG = 5;

            var sepChars = new[] { '\t' };

            var linesRead = 0;
            var specProbErrorCount = 0;
            var precursorMassErrorCount = 0;
            var mgfLookupErrorCount = 0;

            // Note: Do not put a Try/Catch block in this method
            // Allow the calling method to catch any errors

            // Initialize the column mapping
            // Using a case-insensitive comparer
            var columnHeaders = new SortedDictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
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
            using (var reader = new StreamReader(new FileStream(msgfResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            using (var writer = new StreamWriter(new FileStream(msgfSynopsisResults, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                // Write out the headers
                mMSGFInputCreator.WriteMSGFResultsHeaders(writer);

                var headerLineParsed = false;
                firstHitsDataPresent = false;
                tooManyErrors = false;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;
                    var skipLine = false;

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    var dataCols = dataLine.Split('\t');

                    if (!headerLineParsed)
                    {
                        if (string.Equals(dataCols[0], MSGF_RESULT_COLUMN_SpectrumFile, StringComparison.OrdinalIgnoreCase))
                        {
                            // Parse the header line to confirm the column ordering
                            ReaderFactory.ParseColumnHeaders(dataCols, columnHeaders);
                            skipLine = true;
                        }

                        headerLineParsed = true;
                    }

                    if (skipLine || dataCols.Length < 4)
                        continue;

                    var originalPeptide = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Title, columnHeaders);
                    var scan = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_ScanNumber, columnHeaders);
                    var charge = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Charge, columnHeaders);
                    var protein = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Protein_First, columnHeaders);
                    var peptide = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Annotation, columnHeaders);
                    var resultID = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Result_ID, columnHeaders);
                    var specProb = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_SpecProb, columnHeaders);
                    var dataSource = ReaderFactory.LookupColumnValue(dataCols, MSGF_RESULT_COLUMN_Data_Source, columnHeaders);
                    var notes = string.Empty;

                    if (mgfInstrumentData)
                    {
                        // Update the scan number
                        var actualScanNumber = 0;

                        if (int.TryParse(scan, out var mgfScanIndex))
                        {
                            actualScanNumber = mMSGFInputCreator.GetScanByMGFSpectrumIndex(mgfScanIndex);
                        }

                        if (actualScanNumber == 0)
                        {
                            mgfLookupErrorCount++;

                            // Log the first 5 instances to the log file as warnings
                            LogWarning(
                                "Unable to determine the scan number for MGF spectrum index " + scan + " on line  " + linesRead +
                                " in the result file");
                        }
                        scan = actualScanNumber.ToString();
                    }

                    if (double.TryParse(specProb, out var specProbValue))
                    {
                        if (originalPeptide != peptide)
                        {
                            notes = peptide;
                        }

                        // Update specProb to reduce the number of significant figures
                        specProb = specProbValue.ToString("0.000000E+00");
                    }
                    else
                    {
                        // The specProb column does not contain a number
                        specProbErrorCount++;

                        if (specProbErrorCount <= MAX_ERRORS_TO_LOG)
                        {
                            // Log the first 5 instances to the log file as warnings

                            string originalPeptideInfo;

                            if (originalPeptide != peptide)
                            {
                                originalPeptideInfo = ", original peptide sequence " + originalPeptide;
                            }
                            else
                            {
                                originalPeptideInfo = string.Empty;
                            }

                            LogWarning(
                                "MSGF SpecProb is not numeric on line " + linesRead + " in the result file: " + specProb +
                                " (parent peptide " + peptide + ", Scan " + scan + ", Result_ID " + resultID + originalPeptideInfo +
                                ")");
                        }

                        if (specProb.Contains("precursor mass"))
                        {
                            precursorMassErrorCount++;
                        }

                        if (originalPeptide != peptide)
                        {
                            notes = peptide + "; " + specProb;
                        }
                        else
                        {
                            notes = specProb;
                        }

                        // Change the spectrum probability to 1
                        specProb = "1";
                    }

                    var msgfResultData = scan + "\t" + charge + "\t" + protein + "\t" + originalPeptide + "\t" + specProb + "\t" + notes;

                    // Add this result to the cached string dictionary
                    mMSGFInputCreator.AddUpdateMSGFResult(scan, charge, originalPeptide, msgfResultData);

                    if (dataSource == MSGF_PHRP_DATA_SOURCE_FHT)
                    {
                        // First-hits file
                        firstHitsDataPresent = true;
                    }
                    else
                    {
                        // Synopsis file

                        // Add this entry to the MSGF synopsis results
                        // Note that originalPeptide has the original peptide sequence
                        writer.WriteLine(resultID + "\t" + msgfResultData);

                        // See if any entries were skipped when reading the synopsis file used to create the MSGF input file
                        // If they were, add them to the validated MSGF file (to aid in linking up files later)

                        if (!int.TryParse(resultID, out var idValue))
                            continue;

                        var skipList = mMSGFInputCreator.GetSkippedInfoByResultId(idValue);

                        for (var index = 0; index <= skipList.Count - 1; index++)
                        {
                            // Split the entry on the tab character
                            // The item left of the tab is the skipped result id
                            // the item right of the tab is the protein corresponding to the skipped result id

                            var skipInfo = skipList[index].Split(sepChars, 2);

                            writer.WriteLine(skipInfo[0] + "\t" + scan + "\t" + charge + "\t" + skipInfo[1] + "\t" +
                                                    originalPeptide + "\t" + specProb + "\t" + notes);
                        }
                    }
                }
            }

            if (specProbErrorCount > 1)
            {
                LogWarning("MSGF SpecProb was not numeric for " + specProbErrorCount + " entries in the MSGF result file");
            }

            if (mgfLookupErrorCount > 1)
            {
                LogError("MGF Index-to-scan lookup failed for " + mgfLookupErrorCount + " entries in the MSGF result file");

                if (linesRead > 0 && mgfLookupErrorCount / (float)linesRead > 0.1)
                {
                    tooManyErrors = true;
                }
            }

            // Check whether more than 10% of the results have a precursor mass error
            if (PostProcessMSGFCheckPrecursorMassErrorCount(linesRead, precursorMassErrorCount))
            {
                tooManyErrors = true;
            }

            // If we get here, return true
            return true;
        }

        private bool ProcessFileWithMSGF(PeptideHitResultTypes resultType, int msgfInputFileLineCount, string msgfInputFilePath, string msgfResultsFilePath)
        {
            bool success;

            if (resultType == PeptideHitResultTypes.MSGFPlus)
            {
                // Input file may contain a mix of scan types (CID, ETD, and/or HCD)
                // If this is the case, call MSGF twice: first for the CID and HCD spectra, then again for the ETD spectra
                success = RunMSGFonMSGFDB(msgfInputFileLineCount, msgfInputFilePath, msgfResultsFilePath);
            }
            else
            {
                // Run MSGF
                success = RunMSGF(msgfInputFileLineCount, msgfInputFilePath, msgfResultsFilePath);
            }

            return success;
        }

        private bool ProcessFilesWrapper(
            AnalysisResources.RawDataTypeConstants rawDataType,
            PeptideHitResultTypes resultType,
            bool doNotFilterPeptides,
            bool mgfInstrumentData)
        {
            // Parse the SEQUEST, X!Tandem, Inspect, or MODa parameter file to determine if ETD mode was used
            var searchToolParamFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParamFileName"));

            var success = CheckETDModeEnabled(resultType, searchToolParamFilePath);

            if (!success)
            {
                LogError("Error examining param file to determine if ETD mode was enabled");
                return false;
            }

            mProgress = PROGRESS_PCT_PARAM_FILE_EXAMINED_FOR_ETD;
            mStatusTools.UpdateAndWrite(mProgress);

            // Create the _MSGF_input.txt file
            success = CreateMSGFInputFile(resultType, doNotFilterPeptides, mgfInstrumentData, out var msgfInputFileLineCount);

            if (!success)
            {
                if (string.IsNullOrWhiteSpace(mMessage))
                    mMessage = "Error creating MSGF input file";
            }
            else
            {
                mProgress = PROGRESS_PCT_MSGF_INPUT_FILE_GENERATED;
                mStatusTools.UpdateAndWrite(mProgress);
            }

            if (success)
            {
                if (mgfInstrumentData)
                {
                    success = true;
                }
                else if (rawDataType == AnalysisResources.RawDataTypeConstants.mzXML)
                {
                    success = true;
                }
                else if (rawDataType == AnalysisResources.RawDataTypeConstants.mzML)
                {
                    success = ConvertMzMLToMzXML();
                }
                else
                {
                    // Possibly create the .mzXML file
                    // We're waiting to do this until now just in case the above steps fail (since they should all run quickly)
                    success = CreateMzXMLFile();
                }

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Error creating .mzXML file";
                }
                else
                {
                    mProgress = PROGRESS_PCT_MZXML_CREATED;
                    mStatusTools.UpdateAndWrite(mProgress);
                }
            }

            if (success)
            {
                var useExistingMSGFResults = mJobParams.GetJobParameter("UseExistingMSGFResults", false);

                if (useExistingMSGFResults)
                {
                    // Look for a file named Dataset_syn_MSGF.txt in the job's transfer folder
                    // If that file exists, use it as the official MSGF results file
                    // The assumption is that this file will have been created by manually running MSGF on another computer

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("UseExistingMSGFResults is true; will look for pre-generated MSGF results file in the transfer folder");
                    }

                    if (RetrievePreGeneratedDataFile(Path.GetFileName(mMSGFResultsFilePath)))
                    {
                        LogDebug("Pre-generated MSGF results file successfully copied to the work directory");
                        success = true;
                    }
                    else
                    {
                        LogDebug("Pre-generated MSGF results file not found");
                        success = false;
                    }
                }
                else
                {
                    // Run MSGF
                    // Note that mMSGFInputFilePath and mMSGFResultsFilePath get populated by CreateMSGFInputFile
                    success = ProcessFileWithMSGF(resultType, msgfInputFileLineCount, mMSGFInputFilePath, mMSGFResultsFilePath);
                }

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Error running MSGF";
                }
                else
                {
                    // MSGF successfully completed
                    if (!mKeepMSGFInputFiles)
                    {
                        // Add the _MSGF_input.txt file to the list of files to delete (i.e., do not move it into the results folder)
                        mJobParams.AddResultFileToSkip(Path.GetFileName(mMSGFInputFilePath));
                    }

                    mProgress = PROGRESS_PCT_MSGF_COMPLETE;
                    mStatusTools.UpdateAndWrite(mProgress);
                }
            }

            // Make sure the MSGF Input Creator log file is closed
            mMSGFInputCreator.CloseLogFileNow();

            return success;
        }

        /// <summary>
        /// Looks for fileNameToFind in the transfer folder for this job
        /// If found, copies the file to the work directory
        /// </summary>
        /// <param name="fileNameToFind"></param>
        /// <returns>True if success, false if an error</returns>
        private bool RetrievePreGeneratedDataFile(string fileNameToFind)
        {
            var folderToCheck = "??";

            try
            {
                var transferDirectoryPath = mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);
                var inputFolderName = mJobParams.GetParam("inputFolderName");

                folderToCheck = Path.Combine(Path.Combine(transferDirectoryPath, mDatasetName), inputFolderName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Looking for folder " + folderToCheck);
                }

                // Look for fileNameToFind in folderToCheck
                if (Directory.Exists(folderToCheck))
                {
                    var filePathSource = Path.Combine(folderToCheck, fileNameToFind);

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Looking for file " + filePathSource);
                    }

                    if (File.Exists(filePathSource))
                    {
                        var filePathTarget = Path.Combine(mWorkDir, fileNameToFind);
                        LogDebug("Copying file " + filePathSource + " to " + filePathTarget);

                        File.Copy(filePathSource, filePathTarget, true);

                        // File found and successfully copied; return true
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LogWarning("Exception finding file " + fileNameToFind + " in folder " + folderToCheck + ": " + ex);
                return false;
            }

            // File not found
            return false;
        }

        private bool RunMSGFonMSGFDB(int msgfInputFileLineCount, string msgfInputFilePath, string msgfResultsFilePath)
        {
            var collisionModeColIndex = -1;

            try
            {
                var cidData = new List<string>();
                var etdData = new List<string>();

                using (var reader = new StreamReader(new FileStream(msgfInputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        linesRead++;
                        var dataCols = dataLine.Split('\t').ToList();

                        if (linesRead == 1)
                        {
                            // Cache the header line
                            cidData.Add(dataLine);
                            etdData.Add(dataLine);

                            // Confirm the column index of the Collision_Mode column
                            for (var index = 0; index <= dataCols.Count - 1; index++)
                            {
                                if (string.Equals(dataCols[index], MSGF_RESULT_COLUMN_Collision_Mode, StringComparison.OrdinalIgnoreCase))
                                {
                                    collisionModeColIndex = index;
                                }
                            }

                            if (collisionModeColIndex < 0)
                            {
                                // Collision_Mode column not found; this is unexpected
                                LogError("Collision_Mode column not found in the MSGF input file for MSGFDB data; unable to continue");
                                return false;
                            }
                        }
                        else
                        {
                            // Read the collision mode

                            if (dataCols.Count > collisionModeColIndex)
                            {
                                if (string.Equals(dataCols[collisionModeColIndex], "ETD", StringComparison.OrdinalIgnoreCase))
                                {
                                    etdData.Add(dataLine);
                                }
                                else
                                {
                                    cidData.Add(dataLine);
                                }
                            }
                            else
                            {
                                cidData.Add(dataLine);
                            }
                        }
                    }
                }

                mProcessingMSGFDBCollisionModeData = false;

                if (cidData.Count <= 1 && etdData.Count > 1)
                {
                    // Only ETD data is present
                    mETDMode = true;
                    return RunMSGF(msgfInputFileLineCount, msgfInputFilePath, msgfResultsFilePath);
                }

                if (cidData.Count > 1 && etdData.Count > 1)
                {
                    // Mix of both CID and ETD data found

                    mProcessingMSGFDBCollisionModeData = true;

                    // Make sure the final results file does not exist
                    if (File.Exists(msgfResultsFilePath))
                    {
                        File.Delete(msgfResultsFilePath);
                    }

                    // Process the CID data
                    mETDMode = false;
                    mCollisionModeIteration = 1;
                    var success = RunMSGFonMSGFDBCachedData(cidData, msgfInputFilePath, msgfResultsFilePath, "CID");

                    if (!success)
                        return false;

                    // Process the ETD data
                    mETDMode = true;
                    mCollisionModeIteration = 2;
                    success = RunMSGFonMSGFDBCachedData(etdData, msgfInputFilePath, msgfResultsFilePath, "ETD");
                    return success;
                }

                // Only CID or HCD data is present (or no data is present)
                mETDMode = false;
                return RunMSGF(msgfInputFileLineCount, msgfInputFilePath, msgfResultsFilePath);
            }
            catch (Exception ex)
            {
                LogError("Exception in RunMSGF on MSGFDB", ex);
                return false;
            }
        }

        private bool RunMSGFonMSGFDBCachedData(IReadOnlyCollection<string> data, string msgfInputFilePath, string msgfResultsFilePathFinal, string collisionMode)
        {
            try
            {
                var inputFileTempPath = AddFileNameSuffix(msgfInputFilePath, collisionMode);
                var resultFileTempPath = AddFileNameSuffix(msgfResultsFilePathFinal, collisionMode);

                using (var writer = new StreamWriter(new FileStream(inputFileTempPath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var item in data)
                    {
                        writer.WriteLine(item);
                    }
                }

                var success = RunMSGF(data.Count - 1, inputFileTempPath, resultFileTempPath);

                if (!success)
                {
                    return false;
                }

                Global.IdleLoop(0.5);

                // Append the results of resultFileTempPath to msgfResultsFilePath
                if (!File.Exists(msgfResultsFilePathFinal))
                {
                    File.Move(resultFileTempPath, msgfResultsFilePathFinal);
                }
                else
                {
                    using var reader = new StreamReader(new FileStream(resultFileTempPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                    using var writer = new StreamWriter(new FileStream(msgfResultsFilePathFinal, FileMode.Append, FileAccess.Write, FileShare.Read));

                    // Read and skip the first line of resultFileTempPath (it's a header)
                    reader.ReadLine();

                    // Append the remaining lines to msgfResultsFilePathFinal
                    while (!reader.EndOfStream)
                    {
                        writer.WriteLine(reader.ReadLine());
                    }
                }

                Global.IdleLoop(0.5);

                if (!mKeepMSGFInputFiles)
                {
                    // Delete the temporary files
                    DeleteTemporaryFile(inputFileTempPath);
                    DeleteTemporaryFile(resultFileTempPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in RunMSGF on MSGFDBCachedData", ex);
                return false;
            }

            return true;
        }

        private bool RunMSGF(int msgfInputFileLineCount, string msgfInputFilePath, string msgfResultsFilePath)
        {
            bool success;
            bool useSegments;
            string segmentUsageMessage;

            var msgfEntriesPerSegment = mJobParams.GetJobParameter("MSGFEntriesPerSegment", MSGF_SEGMENT_ENTRY_COUNT);

            if (mDebugLevel >= 2)
            {
                LogDebug("MSGFInputFileLineCount = " + msgfInputFileLineCount + "; MSGFEntriesPerSegment = " + msgfEntriesPerSegment);
            }

            if (msgfEntriesPerSegment <= 1)
            {
                useSegments = false;
                segmentUsageMessage = "Not using MSGF segments since MSGFEntriesPerSegment is <= 1";
            }
            else if (msgfInputFileLineCount <= msgfEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN)
            {
                useSegments = false;
                segmentUsageMessage = "Not using MSGF segments since MSGFInputFileLineCount is <= " + msgfEntriesPerSegment + " * " +
                                         (int)(MSGF_SEGMENT_OVERFLOW_MARGIN * 100) + "%";
            }
            else
            {
                useSegments = true;
                segmentUsageMessage = "Using MSGF segments";
            }

            mMSGFLineCountPreviousSegments = 0;
            mMSGFInputFileLineCount = msgfInputFileLineCount;
            mProgress = PROGRESS_PCT_MSGF_START;

            if (!useSegments)
            {
                if (mDebugLevel >= 2)
                {
                    LogMessage(segmentUsageMessage);
                }

                // Do not use segments
                success = RunMSGFWork(msgfInputFilePath, msgfResultsFilePath);
            }
            else
            {
                var segmentFileInfo = new List<SegmentFileInfo>();
                var resultFiles = new List<string>();

                // Split msgfInputFilePath into chunks with msgfEntriesPerSegment each
                success = SplitMSGFInputFile(msgfInputFileLineCount, msgfInputFilePath, msgfEntriesPerSegment, segmentFileInfo);

                if (mDebugLevel >= 2)
                {
                    LogMessage(
                        segmentUsageMessage + "; segment count = " + segmentFileInfo.Count);
                }

                if (success)
                {
                    // Call MSGF for each segment
                    foreach (var segmentFile in segmentFileInfo)
                    {
                        var resultFile = AddFileNameSuffix(msgfResultsFilePath, segmentFile.Segment);

                        success = RunMSGFWork(segmentFile.FilePath, resultFile);

                        if (!success)
                            break;

                        resultFiles.Add(resultFile);
                        mMSGFLineCountPreviousSegments += segmentFile.Entries;
                    }
                }

                if (success)
                {
                    // Combine the results
                    success = CombineMSGFResultFiles(msgfResultsFilePath, resultFiles);
                }

                if (success)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Deleting MSGF segment files");
                    }

                    // Delete the segment files
                    foreach (var segmentFile in segmentFileInfo)
                    {
                        DeleteTemporaryFile(segmentFile.FilePath);
                    }

                    // Delete the result files
                    foreach (var resultFile in resultFiles)
                    {
                        DeleteTemporaryFile(resultFile);
                    }
                }
            }

            try
            {
                // Delete the Console_Output.txt file if it is empty
                var consoleOutputFile = new FileInfo(Path.Combine(mWorkDir, MSGF_CONSOLE_OUTPUT));

                if (consoleOutputFile.Exists && consoleOutputFile.Length == 0)
                {
                    consoleOutputFile.Delete();
                }
            }
            catch (Exception ex)
            {
                LogWarning("Unable to delete the " + MSGF_CONSOLE_OUTPUT + " file: " + ex);
            }

            return success;
        }

        private bool RunMSGFWork(string inputFilePath, string resultsFilePath)
        {
            if (string.IsNullOrEmpty(inputFilePath))
            {
                LogError("inputFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(resultsFilePath))
            {
                LogError("resultsFilePath has not been defined; unable to continue");
                return false;
            }

            // Delete the output file if it already exists (MSGFDB will not overwrite it)
            if (File.Exists(resultsFilePath))
            {
                File.Delete(resultsFilePath);
            }

            // If an MSGF analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java
            // Customize this on a per-job basis using the MSGFJavaMemorySize setting in the settings file
            // (job 611216 succeeded with a value of 5000)
            var javaMemorySize = mJobParams.GetJobParameter("MSGFJavaMemorySize", 2000);

            if (javaMemorySize < 512)
                javaMemorySize = 512;

            if (mDebugLevel >= 1)
            {
                LogMessage("Running MSGF on " + Path.GetFileName(inputFilePath));
            }

            mCurrentMSGFResultsFilePath = resultsFilePath;

            mStatusTools.CurrentOperation = "Running MSGF";
            mStatusTools.UpdateAndWrite(mProgress);

            var arguments = " -Xmx" + javaMemorySize + "M ";

            if (mUsingMSGFDB)
            {
                arguments += "-cp " + PossiblyQuotePath(mMSGFProgLoc) + " ui.MSGF";
            }
            else
            {
                arguments += "-jar " + PossiblyQuotePath(mMSGFProgLoc);
            }

            // Input file
            arguments += " -i " + PossiblyQuotePath(inputFilePath);

            // Folder containing .mzXML or .mgf file
            // Note that this class converts .mzML files to .mzXML
            arguments += " -d " + PossiblyQuotePath(mWorkDir);

            // Output file
            arguments += " -o " + PossiblyQuotePath(resultsFilePath);

            // MSGF v6432 and earlier use -m 0 for CID and -m 1 for ETD
            // MSGFDB v7097 and later use:
            //   -m 0 means as written in the spectrum or CID if no info
            //   -m 1 means CID
            //   -m 2 means ETD
            //   -m 3 means HCD

            var msgfDBVersion = int.MaxValue;

            if (mUsingMSGFDB)
            {
                if (!string.IsNullOrEmpty(mMSGFDBVersion) && mMSGFDBVersion.StartsWith("v"))
                {
                    if (int.TryParse(mMSGFDBVersion.Substring(1), out msgfDBVersion))
                    {
                        // Using a specific version of MSGFDB
                        // msgfDBVersion should now be something like 6434, 6841, 6964, 7097 etc.
                    }
                    else
                    {
                        // Unable to parse out an integer from mMSGFDBVersion
                        msgfDBVersion = int.MaxValue;
                    }
                }
            }

            if (mUsingMSGFDB && msgfDBVersion >= 7097)
            {
                // Always use -m 0 (assuming we're sending an mzXML file to MSGFDB)
                // -m 0 means as-written in the input file
                arguments += " -m 0";
            }
            else
            {
                if (mETDMode)
                {
                    // ETD fragmentation
                    arguments += " -m 1";
                }
                else
                {
                    // CID fragmentation
                    arguments += " -m 0";
                }
            }

            // Enzyme is Trypsin; other supported enzymes are 2: Chymotrypsin, 3: Lys-C, 4: Lys-N, 5: Glu-C, 6: Arg-C, 7: Asp-N, and 8: aLP
            arguments += " -e 1";

            // No fixed mods on cysteine
            arguments += " -fixMod 0";

            // Write out all matches for each spectrum
            arguments += " -x 0";

            // SpecProbThreshold of 1, i.e., do not filter results by the computed SpecProb value
            arguments += " -p 1";

            LogDebug(mJavaProgLoc + " " + arguments);

            mMSGFRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = false,
                CacheStandardOutput = false,
                EchoOutputToConsole = false,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, MSGF_CONSOLE_OUTPUT)
            };
            RegisterEvents(mMSGFRunner);
            mMSGFRunner.LoopWaiting += MSGFRunner_LoopWaiting;

            var success = mMSGFRunner.RunProgram(mJavaProgLoc, arguments, "MSGF", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFVersion))
                {
                    var consoleOutputFile = new FileInfo(Path.Combine(mWorkDir, MSGF_CONSOLE_OUTPUT));

                    if (consoleOutputFile.Length == 0)
                    {
                        // File is 0-bytes; delete it
                        DeleteTemporaryFile(consoleOutputFile.FullName);
                    }
                    else
                    {
                        ParseConsoleOutputFile(Path.Combine(mWorkDir, MSGF_CONSOLE_OUTPUT));
                    }
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
            {
                LogError("Error running MSGF, job " + mJob);
            }

            return success;
        }

        private bool CombineMSGFResultFiles(string msgfOutputFilePath, IEnumerable<string> resultFiles)
        {
            try
            {
                // Create the output file
                using var writer = new StreamWriter(new FileStream(msgfOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Step through the input files and append the results
                var headerWritten = false;

                foreach (var resultFile in resultFiles)
                {
                    using var reader = new StreamReader(new FileStream(resultFile, FileMode.Open, FileAccess.Read, FileShare.Read));

                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (!headerWritten)
                        {
                            headerWritten = true;
                            writer.WriteLine(dataLine);
                        }
                        else
                        {
                            if (linesRead > 1)
                            {
                                writer.WriteLine(dataLine);
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

        private bool LoadMSGFResults(string msgfResultsFilePath, out Dictionary<int, string> msgfResults)
        {
            msgfResults = new Dictionary<int, string>();

            try
            {
                var success = true;

                var msgfSpecProbColIndex = -1;
                using var reader = new StreamReader(new FileStream(msgfResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    var dataCols = dataLine.Split();

                    if (dataCols.Length == 0)
                        continue;

                    if (msgfSpecProbColIndex < 0)
                    {
                        // Assume this is the header line, look for SpecProb
                        for (var index = 0; index <= dataCols.Length - 1; index++)
                        {
                            if (string.Equals(dataCols[index], "SpecProb", StringComparison.InvariantCultureIgnoreCase))
                            {
                                msgfSpecProbColIndex = index;
                                break;
                            }
                        }

                        if (msgfSpecProbColIndex < 0)
                        {
                            // Match not found; abort
                            LogError("SpecProb column not found in file " + msgfResultsFilePath);
                            success = false;
                            break;
                        }
                    }
                    else
                    {
                        // Data line
                        if (int.TryParse(dataCols[0], out var resultID))
                        {
                            if (msgfSpecProbColIndex < dataCols.Length)
                            {
                                try
                                {
                                    msgfResults.Add(resultID, dataCols[msgfSpecProbColIndex]);
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

                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception in LoadMSGFResults", ex);
                return false;
            }
        }

        /// <summary>
        /// Parse the MSGF console output file to determine the MSGF version
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example console output
            // MSGF v7097 (12/29/2011)
            // MS-GF complete (total elapsed time: 507.68 sec)

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead <= 3 && string.IsNullOrWhiteSpace(mMSGFVersion) && dataLine.StartsWith("MSGF v"))
                    {
                        // Originally the first line was the MSGF version
                        // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                        // The third line is the MSGF version

                        if (mDebugLevel >= 2)
                        {
                            LogDebug("MSGF version: " + dataLine);
                        }

                        mMSGFVersion = dataLine;
                    }
                    else
                    {
                        if (dataLine.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MSGF:";
                            }
                            mConsoleOutputErrorMsg += "; " + dataLine;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        private bool SplitMSGFInputFile(int msgfInputFileLineCount, string msgfInputFilePath, int msgfEntriesPerSegment,
            ICollection<SegmentFileInfo> segmentFileInfo)
        {
            var linesRead = 0;
            var headerLine = string.Empty;

            var lineCountAllSegments = 0;

            try
            {
                segmentFileInfo.Clear();

                if (msgfEntriesPerSegment < 100)
                    msgfEntriesPerSegment = 100;

                using var reader = new StreamReader(new FileStream(msgfInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                StreamWriter writer = null;

                SegmentFileInfo thisSegment;
                thisSegment.FilePath = string.Empty;
                thisSegment.Entries = 0;
                thisSegment.Segment = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    linesRead++;

                    if (linesRead == 1)
                    {
                        // This is the header line; cache it so that we can write it out to the top of each input file
                        headerLine = dataLine;
                    }

                    if (thisSegment.Segment == 0 || thisSegment.Entries >= msgfEntriesPerSegment)
                    {
                        // Need to create a new segment
                        // However, if the number of lines remaining to be written is less than 5% of msgfEntriesPerSegment then keep writing to this segment

                        var lineCountRemaining = msgfInputFileLineCount - lineCountAllSegments;

                        if (thisSegment.Segment == 0 || lineCountRemaining > msgfEntriesPerSegment * MSGF_SEGMENT_OVERFLOW_MARGIN)
                        {
                            if (thisSegment.Segment > 0)
                            {
                                // Close the current segment
                                writer?.Flush();
                                writer?.Dispose();
                                segmentFileInfo.Add(thisSegment);
                            }

                            // Initialize a new segment
                            thisSegment.Segment++;
                            thisSegment.Entries = 0;
                            thisSegment.FilePath = AddFileNameSuffix(msgfInputFilePath, thisSegment.Segment);

                            writer = new StreamWriter(new FileStream(thisSegment.FilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                            // Write the header line to the new segment
                            writer.WriteLine(headerLine);
                        }
                    }

                    if (linesRead > 1)
                    {
                        if (writer == null)
                            throw new Exception("writer has not been initialized; this indicates a bug in SplitMSGFInputFile");

                        writer.WriteLine(dataLine);
                        thisSegment.Entries++;
                        lineCountAllSegments++;
                    }
                }

                // Close the output files
                writer?.Flush();
                writer?.Dispose();

                segmentFileInfo.Add(thisSegment);
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
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mMSGFVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMSGFProgLoc),
                new(mMSXmlGeneratorAppPath)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database when using MODa or MS-GF+ probabilities to create the MSGF files
        /// </summary>
        private bool StoreToolVersionInfoPrecomputedProbabilities(PeptideHitResultTypes resultType)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of AnalysisManagerMSGFPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerMSGFPlugin"))
            {
                return false;
            }

            var toolFiles = new List<FileInfo>();

            if (resultType == PeptideHitResultTypes.MSGFPlus)
            {
                // Store the path to MSGFDB.jar
                toolFiles.Add(new FileInfo(mMSGFProgLoc));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool SummarizeMSGFResults(PeptideHitResultTypes resultType)
        {
            bool success;

            try
            {
                // Gigasax.DMS5
                var connectionString = mMgrParams.GetParam("ConnectionString");

                var summarizer = new ResultsSummarizer(
                    resultType, mDatasetName, mJob,
                    mWorkDir,
                    connectionString,
                    mDebugLevel,
                    TraceMode);

                RegisterEvents(summarizer);

                UnregisterEventHandler(summarizer, BaseLogger.LogLevels.ERROR);
                summarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

                summarizer.MSGFSpecEValueOrPEPThreshold = ResultsSummarizer.DEFAULT_MSGF_SPEC_EVALUE_THRESHOLD;

                summarizer.ContactDatabase = true;
                summarizer.PostJobPSMResultsToDB = true;
                summarizer.SaveResultsToTextFile = false;
                summarizer.DatasetName = mDatasetName;

                success = summarizer.ProcessPSMResults();

                if (!success)
                {
                    const string msg = "Error calling ProcessPSMResults";

                    var detailedMsg = msg;

                    if (summarizer.ErrorMessage.Length > 0)
                    {
                        detailedMsg += ": " + summarizer.ErrorMessage;
                    }

                    detailedMsg += "; input file name: " + summarizer.MSGFSynopsisFileName;

                    LogError(msg, detailedMsg);
                }
                else
                {
                    if (summarizer.DatasetScanStatsLookupError)
                    {
                        if (string.IsNullOrWhiteSpace(mMessage) && !string.IsNullOrWhiteSpace(summarizer.ErrorMessage))
                            LogError(summarizer.ErrorMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception summarizing the MSGF results", ex);
                return false;
            }

            return success;
        }

        private int errorCount;

        private void UpdateMSGFProgress(string msgfResultsFilePath)
        {
            try
            {
                if (mMSGFInputFileLineCount <= 0)
                    return;

                if (!File.Exists(msgfResultsFilePath))
                    return;

                // Read the data from the results file
                using var reader = new StreamReader(new FileStream(msgfResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var lineCount = 0;

                while (!reader.EndOfStream)
                {
                    reader.ReadLine();
                    lineCount++;
                }

                // Update the overall progress
                double fraction = (lineCount + mMSGFLineCountPreviousSegments) / (float)mMSGFInputFileLineCount;

                if (mProcessingMSGFDBCollisionModeData)
                {
                    // Running MSGF twice; first for CID spectra and then for ETD spectra
                    // Divide the progress by 2, then add 0.5 if we're on the second iteration

                    fraction /= 2.0;

                    if (mCollisionModeIteration > 1)
                    {
                        fraction += 0.5;
                    }
                }

                mProgress = (float)(PROGRESS_PCT_MSGF_START + (PROGRESS_PCT_MSGF_COMPLETE - PROGRESS_PCT_MSGF_START) * fraction);
                mStatusTools.UpdateAndWrite(mProgress);
            }
            catch (Exception ex)
            {
                // Log errors the first 3 times they occur
                errorCount++;

                if (errorCount <= 3)
                {
                    LogError("Error counting the number of lines in the MSGF results file, " + msgfResultsFilePath, ex);
                }
            }
        }

        private bool UpdateProteinModsFile(PeptideHitResultTypes resultType, string msgfResultsFilePath)
        {
            try
            {
                LogDebug("Contact PHRPReader.GetPHRPProteinModsFileName for resultType " + resultType, 3);

                var proteinModsFile = new FileInfo(Path.Combine(mWorkDir, ReaderFactory.GetPHRPProteinModsFileName(resultType, mDatasetName)));
                var proteinModsFileNew = new FileInfo(proteinModsFile.FullName + ".tmp");

                if (!proteinModsFile.Exists)
                {
                    LogWarning("PHRP ProteinMods.txt file not found: " + proteinModsFile.Name);
                    return true;
                }

                LogDebug("Load MSGFResults from " + msgfResultsFilePath, 3);

                var success = LoadMSGFResults(msgfResultsFilePath, out var msgfResults);

                if (!success)
                {
                    return false;
                }

                var msgfSpecProbColIndex = -1;

                LogDebug("Read " + proteinModsFile.FullName + " and create " + proteinModsFileNew.FullName, 3);

                using (var reader = new StreamReader(new FileStream(proteinModsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(proteinModsFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var splitLine = dataLine.Split().ToList();

                        if (splitLine.Count == 0)
                        {
                            writer.WriteLine();
                            continue;
                        }

                        if (msgfSpecProbColIndex < 0)
                        {
                            // Assume this is the header line, look for MSGF_SpecProb
                            for (var index = 0; index <= splitLine.Count - 1; index++)
                            {
                                if (string.Equals(splitLine[index], "MSGF_SpecProb", StringComparison.OrdinalIgnoreCase))
                                {
                                    msgfSpecProbColIndex = index;
                                    break;
                                }
                            }

                            if (msgfSpecProbColIndex < 0)
                            {
                                // Match not found; abort
                                success = false;
                                break;
                            }
                        }
                        else
                        {
                            // Data line; determine the ResultID
                            if (int.TryParse(splitLine[0], out var resultID))
                            {
                                // Lookup the MSGFSpecProb value for this ResultID
                                if (msgfResults.TryGetValue(resultID, out var msgfSpecProb))
                                {
                                    // Only update the value if msgfSpecProb is a number
                                    if (double.TryParse(msgfSpecProb, out _))
                                    {
                                        splitLine[msgfSpecProbColIndex] = msgfSpecProb;
                                    }
                                }
                            }
                        }

                        writer.WriteLine(Global.CollapseList(splitLine));
                    }
                }

                if (!success)
                {
                    return false;
                }

                // Replace the original file with the new one
                Global.IdleLoop(0.2);
                AppUtils.GarbageCollectNow();

                try
                {
                    LogDebug("Replace " + proteinModsFile.FullName + " with " + proteinModsFileNew.Name, 3);

                    proteinModsFile.Delete();

                    try
                    {
                        proteinModsFileNew.MoveTo(proteinModsFile.FullName);

                        if (mDebugLevel >= 2)
                        {
                            LogMessage("Updated MSGF_SpecProb values in the ProteinMods.txt file");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Error updating the ProteinMods.txt file; cannot rename new version", ex);
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error updating the ProteinMods.txt file; cannot delete old version", ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception updating the ProteinMods.txt file", ex);
                return false;
            }
        }

        private void MSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING);

            LogProgress("MSGF");
        }

        /// <summary>
        /// Event handler for Error Events reported by the MSGF Input Creator
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void MSGFInputCreator_ErrorEvent(string message, Exception ex)
        {
            mMSGFInputCreatorErrorCount++;

            if (mMSGFInputCreatorErrorCount < 10 || mMSGFInputCreatorErrorCount % 1000 == 0)
            {
                LogError("Error reported by MSGFInputCreator; " + message + " (ErrorCount=" + mMSGFInputCreatorErrorCount + ")");
            }
        }

        /// <summary>
        /// Event handler for status events reported by the MSGF Input Creator
        /// </summary>
        /// <param name="message"></param>
        private void MSGFInputCreator_StatusEvent(string message)
        {
            LogMessage(message, 10);
        }

        /// <summary>
        /// Event handler for Warning Events reported by the MSGF Input Creator
        /// </summary>
        /// <param name="warningMessage"></param>
        private void MSGFInputCreator_WarningEvent(string warningMessage)
        {
            mMSGFInputCreatorWarningCount++;

            if (mMSGFInputCreatorWarningCount < 10 || mMSGFInputCreatorWarningCount % 1000 == 0)
            {
                LogWarning("Warning reported by MSGFInputCreator; " + warningMessage + " (WarnCount=" + mMSGFInputCreatorWarningCount + ")");
            }
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        private void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            if (errorMessage.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                errorMessage.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                LogErrorToDatabase(errorMessage);
            }
            else
            {
                LogError(errorMessage, ex);
            }
        }

        private DateTime mLastUpdateTime = DateTime.MinValue;
        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler that fires while MSGF is processing
        /// </summary>
        private void MSGFRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastUpdateTime).TotalSeconds >= 20)
            {
                // Update the MSGF progress by counting the number of lines in the _MSGF.txt file
                UpdateMSGFProgress(mCurrentMSGFResultsFilePath);

                mLastUpdateTime = DateTime.UtcNow;
            }

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MSGF_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }
            }
        }
    }
}
