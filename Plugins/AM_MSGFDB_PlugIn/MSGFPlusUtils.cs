using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PeptideToProteinMapEngine;
using PRISM;
using PRISM.AppSettings;
using PRISMDatabaseUtils;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// MS-GF+ Utilities
    /// </summary>
    public class MSGFPlusUtils : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Arg, Bruker, ccm, centroided, Chymotrypsin, cid, Conc, cp, defs, dto, endopeptidase, etd, Exactive, Exploris, FASTA, FDRs, frag
        // Ignore Spelling: Glu, glutamyl, hashcheck, hcd, hmsn, Hydroxyproline, hyperthreading, ident, iso, loc, Lumos, Lys, msn, mzid, MSGF, MSGFPlus, MZIDto, prog
        // Ignore Spelling: na, nnet, novo, ntt, Orbitrap, phosphorylated, phospho, Phosphorylation, pre, prepended, prot
        // Ignore Spelling: tda, tims, Tryp, tryptic, Tsv, utils, Xmx

        // ReSharper restore CommentTypo

        /// <summary>
        /// Progress value for MS-GF+ starting
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_STARTING = 1;

        /// <summary>
        /// Progress value for MS-GF+ loading the FASTA file
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE = 2;

        /// <summary>
        /// Progress value for MS-GF+ reading the spectra file
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_READING_SPECTRA = 3;

        /// <summary>
        /// Progress value for MS-GF+ spawning worker threads
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED = 4;

        /// <summary>
        /// Progress value for MS-GF+ computing FDRs
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS = 95;

        /// <summary>
        /// Progress value for MS-GF+ having completed
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_COMPLETE = 96;

        /// <summary>
        /// Progress value for conversion of the .mzid file to .tsv
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_CONVERT_MZID_TO_TSV = 97;

        /// <summary>
        /// Progress value for mapping peptides to proteins
        /// </summary>
        public const int PROGRESS_PCT_MSGFPLUS_MAPPING_PEPTIDES_TO_PROTEINS = 98;

        /// <summary>
        /// Progress value for all processing being completed
        /// </summary>
        public const int PROGRESS_PCT_COMPLETE = 99;

        private const string MZIDToTSV_CONSOLE_OUTPUT_FILE = "MzIDToTsv_ConsoleOutput.txt";

        private enum ModDefinitionParts
        {
            EmpiricalFormulaOrMass = 0,
            Residues = 1,
            ModType = 2,
            Position = 3,            // For CustomAA definitions this field is essentially ignored
            Name = 4
        }

        /// <summary>
        /// AddFeatures parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_ADD_FEATURES = "AddFeatures";

        /// <summary>
        /// Legacy MSGFDB parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_C13 = "c13";

        /// <summary>
        /// Custom amino acid definition
        /// </summary>
        private const string MSGFPLUS_OPTION_CUSTOM_AA = "CustomAA";

        /// <summary>
        /// Dynamic modification definition
        /// </summary>
        private const string MSGFPLUS_OPTION_DYNAMIC_MOD = "DynamicMod";

        /// <summary>
        /// Isotope error range parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_ISOTOPE_ERROR_RANGE = "IsotopeErrorRange";

        /// <summary>
        /// Fragmentation Method ID parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_FRAGMENTATION_METHOD = "FragmentationMethodID";

        /// <summary>
        /// Instrument ID parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_INSTRUMENT_ID = "InstrumentID";

        /// <summary>
        /// Parameter name changed to MinNumPeaksPerSpectrum in 2019
        /// </summary>
        private const string MSGFPLUS_OPTION_MIN_NUM_PEAKS_LEGACY = "MinNumPeaks";

        /// <summary>
        /// Legacy MSGFDB parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_MIN_NUM_PEAKS = "MinNumPeaksPerSpectrum";

        /// <summary>
        /// Legacy MSGFDB parameter
        /// </summary>
        private const string MSGFPLUS_OPTION_NNET = "nnet";

        /// <summary>
        /// Number of tolerable termini parameter (used by MS-GF+)
        /// </summary>
        private const string MSGFPLUS_OPTION_NTT = "NTT";

        /// <summary>
        /// Number of threads to use
        /// </summary>
        private const string MSGFPLUS_OPTION_NUM_THREADS = "NumThreads";

        /// <summary>
        /// Static modification definition
        /// </summary>
        private const string MSGFPLUS_OPTION_STATIC_MOD = "StaticMod";

        /// <summary>
        /// TDA parameter
        /// </summary>
        public const string MSGFPLUS_OPTION_TDA = "TDA";

        /// <summary>
        /// MS-GF+ TSV file suffix
        /// </summary>
        public const string MSGFPLUS_TSV_SUFFIX = "_msgfplus.tsv";

        /// <summary>
        /// MS-GF+ jar file name
        /// </summary>
        /// <remarks>Previously MSGFDB.jar</remarks>
        public const string MSGFPLUS_JAR_NAME = "MSGFPlus.jar";

        /// <summary>
        /// MS-GF+ console output file name
        /// </summary>
        public const string MSGFPLUS_CONSOLE_OUTPUT_FILE = "MSGFPlus_ConsoleOutput.txt";

        /// <summary>
        /// Custom enzyme definition file
        /// </summary>
        private const string ENZYMES_FILE_NAME = "enzymes.txt";

        /// <summary>
        /// MS-GF+ mods file name
        /// </summary>
        public const string MOD_FILE_NAME = "MSGFPlus_Mods.txt";

        public const string SCAN_COUNT_LOW_RES_MSN = "ScanCountLowResMSn";
        public const string SCAN_COUNT_HIGH_RES_MSN = "ScanCountHighResMSn";
        public const string SCAN_COUNT_LOW_RES_HCD = "ScanCountLowResHCD";
        public const string SCAN_COUNT_HIGH_RES_HCD = "ScanCountHighResHCD";

        /// <summary>
        /// Event raised when a peptide to protein mapping error has been ignored
        /// </summary>
        public event IgnorePreviousErrorEventEventHandler IgnorePreviousErrorEvent;

        /// <summary>
        /// Delegate for IgnorePreviousErrorEvent
        /// </summary>
        public delegate void IgnorePreviousErrorEventEventHandler(string messageToIgnore);

        private readonly Regex mCommentExtractor;

        private readonly IMgrParams mMgrParams;
        private readonly IJobParams mJobParams;

        private readonly string mWorkDir;
        private readonly short mDebugLevel;

        // Note that PeptideToProteinMapEngine utilizes System.Data.SQLite.dll
        private clsPeptideToProteinMapEngine mPeptideToProteinMapper;

        /// <summary>
        /// Number of skipped continuum spectra
        /// </summary>
        public int ContinuumSpectraSkipped { get; private set; }

        /// <summary>
        /// Console output error message
        /// </summary>
        public string ConsoleOutputErrorMsg { get; private set; }

        /// <summary>
        /// Total search time, in hours
        /// </summary>
        public float ElapsedTimeHours { get; private set; }

        /// <summary>
        /// Path to the enzymes.txt file (if created)
        /// </summary>
        public string EnzymeDefinitionFilePath { get; private set; } = string.Empty;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; } = string.Empty;

        /// <summary>
        /// This will be set to true if the job cannot be run due to not enough free memory
        /// </summary>
        public bool InsufficientFreeMemory { get; private set; }

        /// <summary>
        /// MS-GF+ version
        /// </summary>
        public string MSGFPlusVersion { get; private set; }

        /// <summary>
        /// True if searching for phosphorylated S, T, or Y
        /// </summary>
        public bool PhosphorylationSearch { get; private set; }

        /// <summary>
        /// True if the results include auto-added decoy peptides
        /// </summary>
        public bool ResultsIncludeAutoAddedDecoyPeptides { get; private set; }

        /// <summary>
        /// Number of spectra searched
        /// </summary>
        public int SpectraSearched { get; private set; }

        /// <summary>
        /// Actual thread count in use
        /// </summary>
        public int ThreadCountActual { get; private set; }

        /// <summary>
        /// Number of processing tasks to be run by MS-GF+
        /// </summary>
        public int TaskCountTotal { get; private set; }

        /// <summary>
        /// Number of completed processing tasks
        /// </summary>
        public int TaskCountCompleted { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="workDir">Working directory</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public MSGFPlusUtils(IMgrParams mgrParams, IJobParams jobParams, string workDir, short debugLevel)
        {
            mMgrParams = mgrParams;
            mJobParams = jobParams;

            mWorkDir = workDir;

            mDebugLevel = debugLevel;

            MSGFPlusVersion = string.Empty;
            ConsoleOutputErrorMsg = string.Empty;
            ContinuumSpectraSkipped = 0;
            SpectraSearched = 0;

            ElapsedTimeHours = 0;

            ThreadCountActual = 0;
            TaskCountTotal = 0;
            TaskCountCompleted = 0;

            // This RegEx is used to extract comments in lines of the form
            // DynamicMod=O1,          M,  opt, any,       Oxidation             # Oxidized methionine
            mCommentExtractor = new Regex(@"^(?<ParamInfo>[^#]+?)(?<WhiteSpace>\s*#\s*)(?<Comment>.*)");
        }

        private void AddUpdateParamFileLineMapping(
            IDictionary<string, List<MSGFPlusKeyValueParamFileLine>> paramFileParamToLineMapping,
            string paramName,
            MSGFPlusKeyValueParamFileLine newParamLine)
        {
            if (paramFileParamToLineMapping.TryGetValue(paramName, out var linesForParamName))
            {
                linesForParamName.Add(newParamLine);
            }
            else
            {
                paramFileParamToLineMapping.Add(paramName, new List<MSGFPlusKeyValueParamFileLine> { newParamLine });
            }
        }

        /// <summary>
        /// Update the parameter if using the MSGFDB syntax yet should be using the MS-GF+ syntax
        /// Also make updates from older parameter names to newer names (e.g. MinNumPeaksPerSpectrum instead of MinNumPeaks)
        /// </summary>
        /// <remarks>
        /// If the parameter does need to be replaced, the value in paramInfo will be changed to an empty string
        /// and the new parameter will be returned via replacementParameter
        /// </remarks>
        /// <param name="msgfPlusParameters">Standard MS-GF+ parameters</param>
        /// <param name="paramFileLine">MS-GF+ parameter file line</param>
        /// <param name="replacementParameter">New MS-GF+ parameter</param>
        /// <returns>True if a replacement parameter is defined, otherwise false</returns>
        private bool AdjustParametersForMSGFPlus(
            IEnumerable<MSGFPlusParameter> msgfPlusParameters,
            MSGFPlusKeyValueParamFileLine paramFileLine,
            out MSGFPlusParameter replacementParameter)
        {
            if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_NNET))
            {
                // Auto-switch to NTT
                replacementParameter = GetMSGFPlusParameter(msgfPlusParameters, MSGFPLUS_OPTION_NTT, "0");

                if (!int.TryParse(paramFileLine.ParamInfo.Value, out var value))
                {
                    throw new Exception(string.Format("Parameter {0} does not contain an integer in the MS-GF+ parameter file: {1}",
                                                      MSGFPLUS_OPTION_NNET, paramFileLine.ParamInfo.Value));
                }

                switch (value)
                {
                    case 0:
                        replacementParameter.UpdateValue("2");         // Fully-tryptic
                        break;
                    case 1:
                        replacementParameter.UpdateValue("1");         // Partially tryptic
                        break;
                    case 2:
                        replacementParameter.UpdateValue("0");         // No-enzyme search
                        break;
                    default:
                        // Assume partially tryptic
                        replacementParameter.UpdateValue("1");
                        OnWarningEvent("Unrecognized value for {0} ({1}); assuming {2}={3}", MSGFPLUS_OPTION_NNET, paramFileLine.ParamInfo.Value, MSGFPLUS_OPTION_NTT, replacementParameter.Value);
                        break;
                }

                return true;
            }

            if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_C13))
            {
                // Auto-switch to ti
                replacementParameter = GetMSGFPlusParameter(msgfPlusParameters, MSGFPLUS_OPTION_ISOTOPE_ERROR_RANGE, "0");

                if (int.TryParse(paramFileLine.ParamInfo.Value, out var value))
                {
                    if (value == 0)
                    {
                        replacementParameter.UpdateValue("0,0");
                    }
                    else if (value == 1)
                    {
                        replacementParameter.UpdateValue("-1,1");
                    }
                    else if (value == 2)
                    {
                        replacementParameter.UpdateValue("-1,2");
                    }
                    else
                    {
                        replacementParameter.UpdateValue("0,1");
                    }
                }
                else
                {
                    replacementParameter.UpdateValue("0,1");
                    OnWarningEvent("Unrecognized value for {0} ({1}); assuming {2}={3}", MSGFPLUS_OPTION_C13, paramFileLine.ParamInfo.Value, MSGFPLUS_OPTION_NTT, replacementParameter.Value);
                }

                return true;
            }

            if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_MIN_NUM_PEAKS_LEGACY))
            {
                // Auto-switch to MinNumPeaksPerSpectrum
                replacementParameter = GetReplacementParameter(msgfPlusParameters, paramFileLine.ParamInfo, MSGFPLUS_OPTION_MIN_NUM_PEAKS);
                return true;
            }

            if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, "showDecoy"))
            {
                // Not valid for MS-GF+; skip it
                paramFileLine.ChangeLineToComment("Obsolete");
            }

            replacementParameter = null;
            return false;
        }

        /// <summary>
        /// Append one or more lines from the start of sourceFile to the end of targetFile
        /// </summary>
        /// <param name="workDir">Working directory</param>
        /// <param name="sourceFile">Source file path</param>
        /// <param name="targetFile">Target file path</param>
        /// <param name="headerLinesToAppend">Number of lines to append</param>
        private void AppendConsoleOutputHeader(string workDir, string sourceFile, string targetFile, int headerLinesToAppend)
        {
            try
            {
                var sourceFilePath = Path.Combine(workDir, sourceFile);
                var targetFilePath = Path.Combine(workDir, targetFile);

                if (!File.Exists(sourceFilePath))
                {
                    OnWarningEvent("Source file not found in AppendConsoleOutputHeader: " + sourceFilePath);
                    return;
                }

                if (!File.Exists(targetFilePath))
                {
                    OnWarningEvent("Target file not found in AppendConsoleOutputHeader: " + targetFilePath);
                    return;
                }

                using var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

                var linesRead = 0;
                var separatorAdded = false;

                while (linesRead < headerLinesToAppend && !reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (!separatorAdded)
                    {
                        writer.WriteLine(new string('-', 80));
                        separatorAdded = true;
                    }

                    writer.WriteLine(dataLine);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in MSGFPlusUtils->AppendConsoleOutputHeader", ex);
            }
        }

        private void AppendParameter(
            ICollection<MSGFPlusKeyValueParamFileLine> msgfPlusParamFileLines,
            IDictionary<string, List<MSGFPlusKeyValueParamFileLine>> paramFileParamToLineMapping,
            MSGFPlusParameter newParam)
        {
            var newBlankLine = new KeyValueParamFileLine(0, string.Empty);
            msgfPlusParamFileLines.Add(new MSGFPlusKeyValueParamFileLine(newBlankLine, true));

            var newParamLine = new MSGFPlusKeyValueParamFileLine(newParam);
            msgfPlusParamFileLines.Add(newParamLine);

            AddUpdateParamFileLineMapping(paramFileParamToLineMapping, newParam.ParameterName, newParamLine);
        }

        private bool CanDetermineInstIdFromInstGroup(string instrumentGroup, out string instrumentIDNew, out string autoSwitchReason)
        {
            if (Global.IsMatch(instrumentGroup, "QExactive") ||
                Global.IsMatch(instrumentGroup, "QEHFX") ||
                Global.IsMatch(instrumentGroup, "Exploris"))
            {
                // Thermo Q Exactive, Q Exactive HFX, or Exploris
                instrumentIDNew = "3";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (Global.IsMatch(instrumentGroup, "Bruker_Amazon_Ion_Trap"))
            {
                // Non-Thermo Instrument, low-res MS/MS
                instrumentIDNew = "0";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (Global.IsMatch(instrumentGroup, "IMS"))
            {
                // Non-Thermo Instrument, high-res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (Global.IsMatch(instrumentGroup, "Sciex_TripleTOF"))
            {
                // Non-Thermo Instrument, high-res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (Global.IsMatch(instrumentGroup, "timsTOF") ||
                Global.IsMatch(instrumentGroup, "timsTOF_SCP") ||
                Global.IsMatch(instrumentGroup, "timsTOF_Flex"))
            {
                // Bruker TOF with high-res MS/MS
                // Use instrument type 2 (TOF)
                instrumentIDNew = "2";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            instrumentIDNew = string.Empty;
            autoSwitchReason = string.Empty;
            return false;
        }

        /// <summary>
        /// Update the instrument ID if needed
        /// </summary>
        /// <param name="paramFileLine">MS-GF+ parameter file line tracking instrument ID; its value may get updated by this method</param>
        /// <param name="instrumentIDNew">New instrument ID</param>
        /// <param name="autoSwitchReason">Reason for updating the instrument ID</param>
        /// <param name="callerName">Calling method name</param>
        private void AutoUpdateInstrumentIDIfChanged(
            MSGFPlusKeyValueParamFileLine paramFileLine,
            string instrumentIDNew,
            string autoSwitchReason,
            [CallerMemberName] string callerName = "")
        {
            if (string.IsNullOrEmpty(instrumentIDNew) || string.Equals(instrumentIDNew, paramFileLine.ParamInfo.Value))
            {
                // Nothing to do
                return;
            }

            if (mDebugLevel >= 1)
            {
                var instrumentDescription = instrumentIDNew switch
                {
                    "0" => "Low-res MSn",
                    "1" => "High-res MSn",
                    "2" => "TOF",
                    "3" => "Q-Exactive",
                    _ => "??"
                };

                if (paramFileLine.ParamInfo.ValueLocked)
                {
                    OnStatusEvent("Although code logic suggests to use InstrumentID {0} ({1}), " +
                                  "the existing value will be left as {2} since it is locked in the parameter file (via an exclamation mark)", instrumentIDNew, instrumentDescription, paramFileLine.ParamInfo.Value);
                }
                else
                {
                    OnStatusEvent("Auto-updating instrument ID from {0} to {1} ({2}) {3}; called by {4}", paramFileLine.ParamInfo.Value, instrumentIDNew, instrumentDescription, autoSwitchReason, callerName);
                }
            }

            if (paramFileLine.ParamInfo.ValueLocked)
                return;

            paramFileLine.UpdateParamValue(instrumentIDNew);
        }

        /// <summary>
        /// Convert a .mzid file to a tab-delimited text file (.tsv) using MzidToTsvConverter.exe
        /// </summary>
        /// <param name="mzidToTsvConverterProgLoc">Full path to MzidToTsvConverter.exe</param>
        /// <param name="datasetName">Dataset name (output file will be named DatasetName_msgfdb.tsv)</param>
        /// <param name="mzidFileName">.mzid file name (assumed to be in the work directory)</param>
        /// <returns>TSV file path, or an empty string if an error</returns>
        public string ConvertMZIDToTSV(string mzidToTsvConverterProgLoc, string datasetName, string mzidFileName)
        {
            try
            {
                // In November 2016, this file was renamed from Dataset_msgfdb.tsv to Dataset_msgfplus.tsv
                var tsvFileName = datasetName + MSGFPLUS_TSV_SUFFIX;
                var tsvFilePath = Path.Combine(mWorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var mzidFile = new FileInfo(Path.Combine(mWorkDir, mzidFileName));

                if (!mzidFile.Exists)
                {
                    OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV; Mzid file not found: " + mzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
                if (!MSGFPlusResultsFileHasClosingTag(mzidFile))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Set up and execute a program runner to run MzidToTsvConverter.exe
                var arguments = GetMZIDtoTSVCommandLine(mzidFileName, tsvFileName, mWorkDir);

                var mzidToTsvRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE)
                };

                RegisterEvents(mzidToTsvRunner);

                if (Global.LinuxOS)
                {
                    // Need to run MzidToTsvConverter.exe using mono
                    var updated = mzidToTsvRunner.UpdateToUseMono(mMgrParams, ref mzidToTsvConverterProgLoc, ref arguments);

                    if (!updated)
                    {
                        OnWarningEvent("Unable to run MzidToTsvConverter.exe with mono");
                        return string.Empty;
                    }
                }

                if (mDebugLevel >= 1)
                {
                    OnStatusEvent(mzidToTsvConverterProgLoc + " " + arguments);
                }

                // This process is typically quite fast, so we do not track CPU usage
                var success = mzidToTsvRunner.RunProgram(mzidToTsvConverterProgLoc, arguments, "MzIDToTsv", true);

                if (!success)
                {
                    OnErrorEvent("MzidToTsvConverter.exe returned an error code converting the .mzid file To a .tsv file: " + mzidToTsvRunner.ExitCode);
                    return string.Empty;
                }

                // The conversion succeeded

                // Update MSGFPlus_ConsoleOutput with the contents of MzIDToTsv_ConsoleOutput.txt

                var targetFile = new FileInfo(Path.Combine(mWorkDir, MSGFPLUS_CONSOLE_OUTPUT_FILE));

                if (!targetFile.Exists && mJobParams.GetJobParameter("NumberOfClonedSteps", 0) > 1)
                {
                    // We're likely rerunning data extraction on an old job; this step is not necessary
                    // Skip the call to AppendConsoleOutputHeader to avoid repeated warnings
                }
                else
                {
                    // Append the first line from the console output file to the end of the MSGFPlus console output file
                    AppendConsoleOutputHeader(mWorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);
                }

                try
                {
                    // The MzIDToTsv console output file doesn't contain any log messages we need to save, so delete it
                    File.Delete(mzidToTsvRunner.ConsoleOutputFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                return tsvFilePath;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV", ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Convert a .mzid file to a tab-delimited text file (.tsv) using MSGFPlus.jar
        /// </summary>
        /// <param name="javaProgLoc">Full path to Java</param>
        /// <param name="msgfPlusProgLoc">Folder with MSGFPlus.jar</param>
        /// <param name="datasetName">Dataset name (output file will be named DatasetName_msgfdb.tsv)</param>
        /// <param name="mzidFileName">.mzid file name (assumed to be in the work directory)</param>
        /// <returns>TSV file path, or an empty string if an error</returns>
        [Obsolete("Use the version of ConvertMzidToTsv that simply accepts a dataset name and .mzid file path and uses MzidToTsvConverter.exe")]
        public string ConvertMZIDToTSV(string javaProgLoc, string msgfPlusProgLoc, string datasetName, string mzidFileName)
        {
            string tsvFilePath;

            try
            {
                // In November 2016, this file was renamed from Dataset_msgfdb.tsv to Dataset_msgfplus.tsv
                var tsvFileName = datasetName + MSGFPLUS_TSV_SUFFIX;
                tsvFilePath = Path.Combine(mWorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var mzidFile = new FileInfo(Path.Combine(mWorkDir, mzidFileName));

                if (!mzidFile.Exists)
                {
                    OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV; Mzid file not found: " + mzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
                if (!MSGFPlusResultsFileHasClosingTag(mzidFile))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Dynamically set the amount of required memory based on the size of the .mzid file
                var fileSizeMB = Global.BytesToMB(mzidFile.Length);
                int javaMemorySizeMB;

                if (fileSizeMB < 100)
                    javaMemorySizeMB = 2000;
                else if (fileSizeMB < 200)
                    javaMemorySizeMB = 3000;
                else if (fileSizeMB < 300)
                    javaMemorySizeMB = 4000;
                else if (fileSizeMB < 400)
                    javaMemorySizeMB = 5000;
                else if (fileSizeMB < 600)
                    javaMemorySizeMB = 6000;
                else if (fileSizeMB < 800)
                    javaMemorySizeMB = 7000;
                else if (fileSizeMB < 1000)
                    javaMemorySizeMB = 8000;
                else
                    javaMemorySizeMB = 10000;

                // Set up and execute a program runner to run the MzIDToTsv module of MSGFPlus
                var arguments = GetMZIDtoTSVCommandLine(mzidFileName, tsvFileName, mWorkDir, msgfPlusProgLoc, javaMemorySizeMB);

                // Make sure the machine has enough free memory to run MSGFPlus
                const bool LOG_FREE_MEMORY_ON_SUCCESS = false;

                if (!AnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "MzIDToTsv", LOG_FREE_MEMORY_ON_SUCCESS))
                {
                    InsufficientFreeMemory = true;
                    OnErrorEvent("Not enough free memory to run the MzIDToTsv module in MSGFPlus");
                    return string.Empty;
                }

                if (mDebugLevel >= 1)
                {
                    OnStatusEvent(javaProgLoc + " " + arguments);
                }

                var mzidToTsvRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE)
                };

                RegisterEvents(mzidToTsvRunner);

                // This process is typically quite fast, so we do not track CPU usage
                var success = mzidToTsvRunner.RunProgram(javaProgLoc, arguments, "MzIDToTsv", true);

                if (!success)
                {
                    OnErrorEvent("MSGFPlus returned an error code converting the .mzid file to a .tsv file: " + mzidToTsvRunner.ExitCode);
                    return string.Empty;
                }

                // The conversion succeeded

                // Append the first line from the console output file to the end of the MSGFPlus console output file
                AppendConsoleOutputHeader(mWorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);

                try
                {
                    // Delete the console output file
                    File.Delete(mzidToTsvRunner.ConsoleOutputFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV", ex);
                return string.Empty;
            }

            return tsvFilePath;
        }

        /// <summary>
        /// Construct the path for converting a .mzid file to .tsv using MzidToTsvConverter.exe
        /// </summary>
        /// <param name="mzidFileName">.mzid file name</param>
        /// <param name="tsvFileName">.tsv file name</param>
        /// <param name="workingDirectory">Working directory</param>
        public static string GetMZIDtoTSVCommandLine(string mzidFileName, string tsvFileName, string workingDirectory)
        {
            return " -mzid:" + Global.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName)) +
                   " -tsv:" + Global.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName)) +
                   " -unroll" +
                   " -showDecoy";
        }

        /// <summary>
        /// Obtain the MzidToTsv command line arguments
        /// </summary>
        /// <param name="mzidFileName">.mzid file name</param>
        /// <param name="tsvFileName">.tsv file name</param>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="msgfPlusProgLoc">MS-GF+ program location</param>
        /// <param name="javaMemorySizeMB">Java memory size, in MB</param>
        [Obsolete("Use GetMZIDtoTSVCommandLine for MzidToTsvConverter.exe")]
        public static string GetMZIDtoTSVCommandLine(string mzidFileName, string tsvFileName, string workingDirectory, string msgfPlusProgLoc, int javaMemorySizeMB)
        {
            // We're using "-XX:+UseConcMarkSweepGC" as directed at https://stackoverflow.com/questions/5839359/java-lang-outofmemoryerror-gc-overhead-limit-exceeded
            // due to seeing error "java.lang.OutOfMemoryError: GC overhead limit exceeded" with a 353 MB .mzid file

            return " -Xmx" + javaMemorySizeMB + "M -XX:+UseConcMarkSweepGC -cp " + msgfPlusProgLoc +
                   " edu.ucsd.msjava.ui.MzIDToTsv" +
                   " -i " + Global.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName)) +
                   " -o " + Global.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName)) +
                   " -showQValue 1" +
                   " -showDecoy 1" +
                   " -unroll 1";
        }

        /// <summary>
        /// Create file params\enzymes.txt using enzymeDefs
        /// </summary>
        /// <param name="outputDirectory">Output directory</param>
        /// <param name="enzymeDefs">List of enzyme definitions</param>
        /// <returns>True if successful, or if enzymeDefs is empty</returns>
        private bool CreateEnzymeDefinitionsFile(FileSystemInfo outputDirectory, IEnumerable<string> enzymeDefs)
        {
            EnzymeDefinitionFilePath = string.Empty;

            try
            {
                var createFile = false;

                var enzymeBuilder = new StringBuilder();
                enzymeBuilder.AppendLine("# This file specifies additional enzymes considered for MS-GF+");
                enzymeBuilder.AppendLine("#");
                enzymeBuilder.AppendLine("# To be loaded, this file must reside in a directory named params below the working directory");
                enzymeBuilder.AppendLine("# For example, create file C:\\Work\\params\\enzymes.txt when the working directory is C:\\Work");
                enzymeBuilder.AppendLine("# Or, on Linux, create file /home/user/work/params/enzymes.txt when the working directory is /home/user/work/");
                enzymeBuilder.AppendLine("#");
                enzymeBuilder.AppendLine("# Format: ShortName,CleaveAt,Terminus,Description");
                enzymeBuilder.AppendLine("# - ShortName: A unique short name of the enzyme (e.g. Tryp). No space is allowed.");
                enzymeBuilder.AppendLine("# - CleaveAt: The residues cleaved by the enzyme (e.g. KR). Put \"null\" in case of no specificity.");
                enzymeBuilder.AppendLine("# - Terminus: Whether the enzyme cleaves C-terminal (C) or N-terminal (N)");
                enzymeBuilder.AppendLine("# - Description: Description of the enzyme");
                enzymeBuilder.AppendLine("#");

                // ReSharper disable StringLiteralTypo
                enzymeBuilder.AppendLine("# The following enzymes are pre-configured, numbered 1 through 9 when using the -e argument at the command line");
                enzymeBuilder.AppendLine("# Tryp,KR,C,Trypsin                         # 1");
                enzymeBuilder.AppendLine("# Chymotrypsin,FYWL,C,Chymotrypsin          # 2");
                enzymeBuilder.AppendLine("# LysC,K,C,Lys-C                            # 3");
                enzymeBuilder.AppendLine("# LysN,K,N,Lys-N                            # 4");
                enzymeBuilder.AppendLine("# GluC,E,C,Glu-C                            # 5: glutamyl endopeptidase");
                enzymeBuilder.AppendLine("# ArgC,R,C,Arg-C                            # 6");
                enzymeBuilder.AppendLine("# AspN,D,N,Asp-N                            # 7");
                enzymeBuilder.AppendLine("# aLP,null,C,alphaLP                        # 8");
                enzymeBuilder.AppendLine("# NoCleavage,null,C,no cleavage             # 9: Endogenous peptides");
                // ReSharper restore StringLiteralTypo

                enzymeBuilder.AppendLine("#");
                enzymeBuilder.AppendLine("# If you want to redefine a pre-configured enzyme (e.g. change CleaveAt of Asp-N to \"DE\"), specify the enzyme again.");
                enzymeBuilder.AppendLine("# Specify one enzyme per line.");
                enzymeBuilder.AppendLine("# New enzymes will continue the numbering at 10");
                enzymeBuilder.AppendLine("#");
                enzymeBuilder.AppendLine("# Examples:");
                enzymeBuilder.AppendLine("# CNBr,M,C,CNBr");
                enzymeBuilder.AppendLine("# AspN,DE,N,Asp-N");
                enzymeBuilder.AppendLine();

                foreach (var enzymeDef in enzymeDefs)
                {
                    // At least one definition is defined; create the file
                    createFile = true;

                    // Split on commas, change tabs to spaces, and remove whitespace
                    var enzymeDefParts = enzymeDef.Split(',');

                    if (enzymeDefParts.Length < 4)
                    {
                        ErrorMessage = "Invalid enzyme definition in the MS-GF+ parameter file: " + enzymeDef;
                        OnErrorEvent(ErrorMessage);
                        return false;
                    }

                    var enzymeDefClean = enzymeDefParts[0];

                    for (var i = 0; i < 4; i++)
                    {
                        enzymeDefParts[i] = enzymeDefParts[i].Replace("\t", " ").Trim();

                        if (i > 0)
                            enzymeDefClean += "," + enzymeDefParts[i];
                    }

                    enzymeBuilder.AppendLine(enzymeDefClean);
                }

                if (!createFile)
                    return true;

                var enzymesFile = new FileInfo(Path.Combine(outputDirectory.FullName, "params", ENZYMES_FILE_NAME));

                if (enzymesFile.Directory == null)
                {
                    ErrorMessage = "Unable to determine the parent directory of " + enzymesFile.FullName;
                    OnErrorEvent(ErrorMessage);
                    return false;
                }

                if (!enzymesFile.Directory.Exists)
                    enzymesFile.Directory.Create();

                using var writer = new StreamWriter(new FileStream(enzymesFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.Write(enzymeBuilder.ToString());

                EnzymeDefinitionFilePath = enzymesFile.FullName;

                return true;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception creating MS-GF+ enzymes.txt file";
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Create the peptide to protein mapping file, Dataset_msgfplus_PepToProtMap.txt
        /// </summary>
        /// <param name="resultsFileName">Results file name</param>
        /// <param name="resultsIncludeAutoAddedDecoyPeptides">True if the results include auto-added decoy peptides</param>
        /// <param name="localOrgDbFolder">Local FASTA file database directory</param>
        public CloseOutType CreatePeptideToProteinMapping(string resultsFileName, bool resultsIncludeAutoAddedDecoyPeptides, string localOrgDbFolder)
        {
            return CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder,
                clsPeptideToProteinMapEngine.PeptideInputFileFormatConstants.MSGFPlusResultsFile);
        }

        /// <summary>
        /// Create the peptide to protein mapping file, Dataset_msgfplus_PepToProtMap.txt
        /// </summary>
        /// <param name="resultsFileName">Results file name</param>
        /// <param name="resultsIncludeAutoAddedDecoyPeptides">True if the results include auto-added decoy peptides</param>
        /// <param name="localOrgDbFolder">Local FASTA file database directory</param>
        /// <param name="peptideInputFileFormat">Peptide input file format enum</param>
        public CloseOutType CreatePeptideToProteinMapping(
            string resultsFileName,
            bool resultsIncludeAutoAddedDecoyPeptides,
            string localOrgDbFolder,
            clsPeptideToProteinMapEngine.PeptideInputFileFormatConstants peptideInputFileFormat)
        {
            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var dbFilename = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME);

            string msg;

            var ignorePeptideToProteinMapperErrors = false;

            var inputFilePath = Path.Combine(mWorkDir, resultsFileName);
            var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

            try
            {
                // Validate that the input file has at least one entry; if not, no point in continuing

                var inputFile = new FileInfo(inputFilePath);

                if (!inputFile.Exists)
                {
                    msg = "MS-GF+ TSV results file not found: " + inputFilePath;
                    OnErrorEvent(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (inputFile.Length == 0)
                {
                    msg = "MS-GF+ TSV results file is empty";
                    OnErrorEvent(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                var linesRead = 0;

                while (!reader.EndOfStream && linesRead < 10)
                {
                    var dataLine = reader.ReadLine();

                    if (!string.IsNullOrEmpty(dataLine))
                    {
                        linesRead++;
                    }
                }

                if (linesRead <= 1)
                {
                    // File is empty or only contains a header line
                    msg = "No results above threshold";
                    OnErrorEvent(msg);

                    return CloseOutType.CLOSEOUT_NO_DATA;
                }
            }
            catch (Exception ex)
            {
                msg = "Error validating MS-GF+ results file contents in CreatePeptideToProteinMapping";
                OnErrorEvent(msg, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            string fastaFileToSearch;

            if (!resultsIncludeAutoAddedDecoyPeptides)
            {
                fastaFileToSearch = fastaFilePath;
            }
            else
            {
                // Read the original FASTA file to create a decoy FASTA file
                var decoyFastaFilePath = GenerateDecoyFastaFile(fastaFilePath, mWorkDir);
                fastaFileToSearch = decoyFastaFilePath;

                if (string.IsNullOrEmpty(decoyFastaFilePath))
                {
                    // Problem creating the decoy FASTA file
                    if (string.IsNullOrEmpty(ErrorMessage))
                    {
                        ErrorMessage = "Error creating a decoy version of the FASTA file";
                    }
                    OnErrorEvent(ErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileToSkip(Path.GetFileName(decoyFastaFilePath));
            }

            try
            {
                if (mDebugLevel >= 1)
                {
                    OnStatusEvent("Creating peptide to protein map file");
                }

                ignorePeptideToProteinMapperErrors = mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false);

                var options = new ProteinCoverageSummarizer.ProteinCoverageSummarizerOptions
                {
                    IgnoreILDifferences = false,
                    MatchPeptidePrefixAndSuffixToProtein = false,
                    OutputProteinSequence = false,
                    PeptideFileSkipFirstLine = false,
                    RemoveSymbolCharacters = true,
                    ProteinInputFilePath = fastaFileToSearch,
                    SaveProteinToPeptideMappingFile = true,
                    SearchAllProteinsForPeptideSequence = true,
                    SearchAllProteinsSkipCoverageComputationSteps = true
                };

                mPeptideToProteinMapper = new clsPeptideToProteinMapEngine(options)
                {
                    DeleteTempFiles = true,
                    InspectParameterFilePath = string.Empty,
                    PeptideInputFileFormat = peptideInputFileFormat
                };

                RegisterEvents(mPeptideToProteinMapper);
                mPeptideToProteinMapper.ProgressUpdate -= OnProgressUpdate;
                mPeptideToProteinMapper.ProgressUpdate += PeptideToProteinMapper_ProgressChanged;

                if (mDebugLevel > 2)
                {
                    mPeptideToProteinMapper.LogMessagesToFile = true;
                    mPeptideToProteinMapper.LogDirectoryPath = mWorkDir;
                }
                else
                {
                    mPeptideToProteinMapper.LogMessagesToFile = false;
                }

                // Note that PeptideToProteinMapEngine utilizes System.Data.SQLite.dll
                var success = mPeptideToProteinMapper.ProcessFile(inputFilePath, mWorkDir, string.Empty, true);

                mPeptideToProteinMapper.CloseLogFileNow();

                var pepToProtMapFileName = Path.GetFileNameWithoutExtension(inputFilePath) +
                                           clsPeptideToProteinMapEngine.FILENAME_SUFFIX_PEP_TO_PROTEIN_MAPPING;

                var pepToProtMapFilePath = Path.Combine(mWorkDir, pepToProtMapFileName);

                if (success)
                {
                    if (!File.Exists(pepToProtMapFilePath))
                    {
                        OnErrorEvent("Peptide to protein mapping file was not created");
                        success = false;
                    }
                    else
                    {
                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent("Peptide to protein mapping complete");
                        }

                        success = ValidatePeptideToProteinMapResults(pepToProtMapFilePath, ignorePeptideToProteinMapperErrors);
                    }
                }
                else
                {
                    if (mPeptideToProteinMapper.GetErrorMessage().Length == 0 && mPeptideToProteinMapper.StatusMessage.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        OnErrorEvent("Error running the PeptideToProteinMapEngine: " + mPeptideToProteinMapper.StatusMessage);
                    }
                    else
                    {
                        OnErrorEvent("Error running the PeptideToProteinMapEngine: " + mPeptideToProteinMapper.GetErrorMessage());

                        if (mPeptideToProteinMapper.StatusMessage.Length > 0)
                        {
                            OnErrorEvent("PeptideToProteinMapEngine status: " + mPeptideToProteinMapper.StatusMessage);
                        }
                    }

                    if (ignorePeptideToProteinMapperErrors)
                    {
                        OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' is true");

                        if (File.Exists(pepToProtMapFilePath))
                        {
                            success = ValidatePeptideToProteinMapResults(pepToProtMapFilePath, ignorePeptideToProteinMapperErrors: true);
                        }
                        else
                        {
                            success = true;
                        }
                    }
                    else
                    {
                        OnErrorEvent("Error in CreatePeptideToProteinMapping");
                    }
                }

                if (success)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreatePeptideToProteinMapping", ex);

                if (ignorePeptideToProteinMapperErrors)
                {
                    OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' is true");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Create a trimmed version of fastaFilePath, with max size maxFastaFileSizeMB
        /// </summary>
        /// <param name="fastaFilePath">FASTA file to trim</param>
        /// <param name="maxFastaFileSizeMB">Maximum file size</param>
        /// <returns>Full path to the trimmed FASTA; empty string if a problem</returns>
        private string CreateTrimmedFasta(string fastaFilePath, int maxFastaFileSizeMB)
        {
            try
            {
                var fastaFile = new FileInfo(fastaFilePath);

                if (fastaFile.DirectoryName == null)
                {
                    ErrorMessage = "Unable to determine the parent directory of " + fastaFilePath;
                    OnErrorEvent(ErrorMessage);
                    return string.Empty;
                }

                var trimmedFasta = new FileInfo(Path.Combine(
                    fastaFile.DirectoryName,
                    Path.GetFileNameWithoutExtension(fastaFile.Name) + "_Trim" + maxFastaFileSizeMB + "MB.fasta"));

                if (trimmedFasta.Exists)
                {
                    // Verify that the file matches the .hashcheck value
                    var hashcheckFilePath = trimmedFasta.FullName + Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

                    if (FileSyncUtils.ValidateFileVsHashcheck(trimmedFasta.FullName, hashcheckFilePath, out _))
                    {
                        // The trimmed FASTA file is valid
                        OnStatusEvent("Using existing trimmed FASTA: " + trimmedFasta.Name);
                        return trimmedFasta.FullName;
                    }
                }

                OnStatusEvent("Creating trimmed FASTA: " + trimmedFasta.Name);

                // Construct the list of required contaminant proteins
                var contaminantUtility = new FastaContaminantUtility();

                var requiredContaminants = new Dictionary<string, bool>();

                foreach (var proteinName in contaminantUtility.ProteinNames)
                {
                    requiredContaminants.Add(proteinName, false);
                }

                long maxSizeBytes = maxFastaFileSizeMB * 1024 * 1024;
                long bytesWritten = 0;
                var proteinCount = 0;

                using (var sourceFastaReader = new StreamReader(new FileStream(fastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var trimmedFastaWriter = new StreamWriter(new FileStream(trimmedFasta.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!sourceFastaReader.EndOfStream)
                    {
                        var dataLine = sourceFastaReader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.StartsWith(">"))
                        {
                            // Protein header
                            if (bytesWritten > maxSizeBytes)
                            {
                                // Do not write out any more proteins
                                break;
                            }

                            var spaceIndex = dataLine.IndexOf(' ', 1);

                            if (spaceIndex < 0)
                                spaceIndex = dataLine.Length - 1;
                            var proteinName = dataLine.Substring(1, spaceIndex - 1);

                            if (requiredContaminants.ContainsKey(proteinName))
                            {
                                requiredContaminants[proteinName] = true;
                            }

                            proteinCount++;
                        }

                        trimmedFastaWriter.WriteLine(dataLine);
                        bytesWritten += dataLine.Length + 2;
                    }

                    // Add any missing contaminants
                    foreach (var protein in requiredContaminants)
                    {
                        if (!protein.Value)
                        {
                            contaminantUtility.WriteProteinToFasta(trimmedFastaWriter, protein.Key);
                        }
                    }
                }

                OnStatusEvent("Trimmed FASTA created using " + proteinCount + " proteins; creating the hashcheck file");

                Global.CreateHashcheckFile(trimmedFasta.FullName, true);

                return trimmedFasta.FullName;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception trimming FASTA file to " + maxFastaFileSizeMB + " MB";
                OnErrorEvent(ErrorMessage, ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Creates a decoy version of the FASTA file specified by inputFilePath
        /// This new file will include the original proteins plus reversed versions of the original proteins
        /// Protein names will be prepended with REV_ or XXX_
        /// </summary>
        /// <param name="inputFilePath">FASTA file to process</param>
        /// <param name="outputDirectoryPath">Output folder to create decoy file in</param>
        /// <returns>Full path to the decoy FASTA file</returns>
        private string GenerateDecoyFastaFile(string inputFilePath, string outputDirectoryPath)
        {
            const char PROTEIN_LINE_START_CHAR = '>';
            const char PROTEIN_LINE_ACCESSION_END_CHAR = ' ';

            try
            {
                var sourceFile = new FileInfo(inputFilePath);

                if (!sourceFile.Exists)
                {
                    ErrorMessage = "FASTA file not found: " + sourceFile.FullName;
                    return string.Empty;
                }

                var decoyFastaFilePath = Path.Combine(outputDirectoryPath, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_decoy.fasta");

                if (mDebugLevel >= 2)
                {
                    OnStatusEvent("Creating decoy FASTA file at " + decoyFastaFilePath);
                }

                var fastaFileReader = new ProteinFileReader.FastaFileReader();

                if (!fastaFileReader.OpenFile(inputFilePath))
                {
                    OnErrorEvent("Error reading FASTA file with ProteinFileReader to create decoy file");
                    return string.Empty;
                }

                const string NAME_PREFIX = "XXX_";

                using (var writer = new StreamWriter(new FileStream(decoyFastaFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    bool inputProteinFound;

                    do
                    {
                        inputProteinFound = fastaFileReader.ReadNextProteinEntry();

                        if (inputProteinFound)
                        {
                            // Write the forward protein
                            writer.WriteLine(PROTEIN_LINE_START_CHAR + fastaFileReader.ProteinName +
                                             PROTEIN_LINE_ACCESSION_END_CHAR + fastaFileReader.ProteinDescription);
                            WriteProteinSequence(writer, fastaFileReader.ProteinSequence);

                            // Write the decoy protein
                            writer.WriteLine(PROTEIN_LINE_START_CHAR + NAME_PREFIX + fastaFileReader.ProteinName +
                                             PROTEIN_LINE_ACCESSION_END_CHAR + fastaFileReader.ProteinDescription);
                            WriteProteinSequence(writer, ReverseString(fastaFileReader.ProteinSequence));
                        }
                    } while (inputProteinFound);
                }

                fastaFileReader.CloseFile();

                return decoyFastaFilePath;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception creating decoy FASTA file", ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        /// <returns>The number of cores on this computer</returns>
        public int GetCoreCount()
        {
            return Global.GetCoreCount();
        }

        /// <summary>
        /// Get the given MS-GF+ parameter by name
        /// Throws an exception if an invalid name
        /// </summary>
        /// <param name="msgfPlusParameters">MS-GF+ parameters</param>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterValue">New value to assign to the parameter</param>
        /// <returns>Parameter, if found</returns>
        private MSGFPlusParameter GetMSGFPlusParameter(IEnumerable<MSGFPlusParameter> msgfPlusParameters, string parameterName, string parameterValue)
        {
            if (TryGetParameter(msgfPlusParameters, parameterName, parameterValue, out var paramInfo))
            {
                return paramInfo;
            }

            throw new Exception("Required parameter not found in msgfPlusParameters: " + parameterName);
        }

        private List<MSGFPlusParameter> GetMSGFPlusParameters()
        {
            var msgfPlusParameters = new List<MSGFPlusParameter>
            {
                new("SpectrumFile", "s"),
                new("DatabaseFile", "d"),
                new("DecoyPrefix", "decoy"),
                new("PrecursorMassTolerance", "t", "PMTolerance"),
                new("PrecursorMassToleranceUnits", "u"),

                // When using a _dta.txt file, this setting is set to 0 since we create a _ScanType.txt file that specifies the type of each scan
                // (thus, the value in the parameter file is ignored)
                // One exception: when it is UVPD (mode 4), we use -m 4
                new(MSGFPLUS_OPTION_FRAGMENTATION_METHOD, "m"),

                // This setting is auto-updated based on the instrument group for this dataset,
                // plus also the scan types listed In the _ScanType.txt file
                // (thus, the value in the parameter file is typically ignored)
                new(MSGFPLUS_OPTION_INSTRUMENT_ID, "inst"),

                new("EnzymeID", "e"),
                new("ProtocolID", "protocol", "Protocol"),
                new(MSGFPLUS_OPTION_NUM_THREADS, "thread"),
                new("NumTasks", "tasks"),
                new(MSGFPLUS_OPTION_ISOTOPE_ERROR_RANGE, "ti", "IsotopeError"),
                new("NTT", "ntt"),

                // C13 was a MSGFDB parameter name; old parameter files may still have it. This class will auto-change it to IsotopeError
                new(MSGFPLUS_OPTION_C13, ""),

                // NNET was a MSGFDB parameter name; old parameter files may still have it. This class will auto-change it to "NTT"
                new(MSGFPLUS_OPTION_NNET, ""),

                new("MinPepLength", "minLength", "minLength"),
                new("MaxPepLength", "maxLength", "maxLength"),

                // MinCharge and MaxCharge are only used if the spectrum file doesn't have charge information
                new("MinCharge", "minCharge"),
                new("MaxCharge", "maxCharge"),

                new("NumMatchesPerSpec", "n"),
                new("ChargeCarrierMass", "ccm"),

                // Auto-added by this code if not defined
                new("MinNumPeaksPerSpectrum", "minNumPeaks", "minNumPeaks"),
                new("NumIsoforms", "iso"),
                new("IgnoreMetCleavage", "ignoreMetCleavage"),
                new("MinDeNovoScore", "minDeNovoScore"),

                // Spec index range
                new("SpecIndex", "index"),

                new("MaxMissedCleavages", "maxMissedCleavages"),
                new(MSGFPLUS_OPTION_TDA, "tda"),
                new(MSGFPLUS_OPTION_ADD_FEATURES, "addFeatures"),

                // Future parameter; unused in February 2019
                new("FragTolerance", "f"),

                // These settings were previously used to create a Mods.txt file
                // Starting in February 2019, the settings are read by MS-GF+ from the parameter file
                new("NumMods", ""),
                new(MSGFPLUS_OPTION_STATIC_MOD, ""),
                new(MSGFPLUS_OPTION_DYNAMIC_MOD, ""),
                new(MSGFPLUS_OPTION_CUSTOM_AA, "")
            };

            // The following is a special case; do not add it
            //   "EnzymeDef"

            foreach (var parameter in msgfPlusParameters)
            {
                RegisterEvents(parameter);
            }

            return msgfPlusParameters;
        }

        /// <summary>
        /// Get an MS-GF+ parameter to replace the given parameter
        /// </summary>
        /// <param name="msgfPlusParameters">MS-GF+ parameters</param>
        /// <param name="paramInfo">Parameter info</param>
        /// <param name="replacementParameterName">Replacement parameter name</param>
        private MSGFPlusParameter GetReplacementParameter(
            IEnumerable<MSGFPlusParameter> msgfPlusParameters,
            MSGFPlusParameter paramInfo,
            string replacementParameterName)
        {
            return GetMSGFPlusParameter(msgfPlusParameters, replacementParameterName, paramInfo.Value);
        }

        private string GetSettingFromMSGFPlusParamFile(string sourceParameterFilePath, string settingToFind)
        {
            return GetSettingFromMSGFPlusParamFile(sourceParameterFilePath, settingToFind, string.Empty);
        }

        private string GetSettingFromMSGFPlusParamFile(string sourceParameterFilePath, string settingToFind, string valueIfNotFound)
        {
            if (!File.Exists(sourceParameterFilePath))
            {
                OnErrorEvent("Parameter file not found: " + sourceParameterFilePath);
                return valueIfNotFound;
            }

            try
            {
                var paramFileReader = new KeyValueParamFileReader("MS-GF+", sourceParameterFilePath);
                RegisterEvents(paramFileReader);

                var success = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);

                if (!success)
                {
                    ErrorMessage = Global.AppendToComment(
                            "Error reading MS-GF+ parameter file in GetSettingFromMSGFPlusParamFile",
                            paramFileReader.ErrorMessage);
                    return valueIfNotFound;
                }

                return KeyValueParamFileReader.GetParameterValue(paramFileEntries, settingToFind, valueIfNotFound);
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception examining parameters loaded from the MS-GF+ parameter file";
                OnErrorEvent(ErrorMessage, ex);
            }

            return valueIfNotFound;
        }

        /// <summary>
        /// Initialize the FASTA file
        /// </summary>
        /// <param name="javaProgLoc">Path to java.exe</param>
        /// <param name="msgfPlusProgLoc">MS-GF+ program location</param>
        /// <param name="fastaFileSizeKB">Output: FASTA file size (in KB)</param>
        /// <param name="fastaFileIsDecoy">Output: True if the FASTA file is a decoy FASTA</param>
        /// <param name="fastaFilePath">Output: FASTA file path</param>
        /// <param name="msgfPlusParameterFilePath">MS-GF+ parameter file path</param>
        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfPlusProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string msgfPlusParameterFilePath)
        {
            return InitializeFastaFile(javaProgLoc, msgfPlusProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy,
                                       out fastaFilePath, msgfPlusParameterFilePath, 0);
        }

        /// <summary>
        /// Initialize the FASTA file
        /// </summary>
        /// <param name="javaProgLoc">Path to java.exe</param>
        /// <param name="msgfPlusProgLoc">MS-GF+ program location</param>
        /// <param name="fastaFileSizeKB">Output: FASTA file size (in KB)</param>
        /// <param name="fastaFileIsDecoy">Output: True if the FASTA file is a decoy FASTA</param>
        /// <param name="fastaFilePath">Output: FASTA file path</param>
        /// <param name="msgfPlusParameterFilePath">MS-GF+ parameter file path</param>
        /// <param name="maxFastaFileSizeMB">Maximum FASTA file size (in MB); MzRefinery sets this to 50 MB to give faster search times</param>
        public CloseOutType InitializeFastaFile(
            string javaProgLoc, string msgfPlusProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string msgfPlusParameterFilePath, int maxFastaFileSizeMB)
        {
            var randomGenerator = new Random();

            var mgrName = mMgrParams.ManagerName;

            InsufficientFreeMemory = false;

            var indexedDBCreator = new CreateMSGFDBSuffixArrayFiles(mgrName);
            RegisterEvents(indexedDBCreator);

            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
            fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            fastaFileSizeKB = 0;
            fastaFileIsDecoy = false;

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                OnErrorEvent("FASTA file not found: " + fastaFile.FullName);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            fastaFileSizeKB = (float)(fastaFile.Length / 1024.0);

            var proteinOptions = mJobParams.GetParam("ProteinOptions");

            if (string.IsNullOrEmpty(proteinOptions) || proteinOptions.Equals("na", StringComparison.OrdinalIgnoreCase))
            {
                // Determine the fraction of the proteins that start with Reversed_ or XXX_ or XXX.
                var decoyPrefixes = AnalysisResources.GetDefaultDecoyPrefixes();

                foreach (var decoyPrefix in decoyPrefixes)
                {
                    var fractionDecoy = AnalysisResources.GetDecoyFastaCompositionStats(fastaFile, decoyPrefix, out _);

                    if (fractionDecoy >= 0.25)
                    {
                        fastaFileIsDecoy = true;
                        break;
                    }
                }
            }
            else
            {
                if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    fastaFileIsDecoy = true;
                }
            }

            if (!string.IsNullOrEmpty(msgfPlusParameterFilePath))
            {
                var tdaSetting = GetSettingFromMSGFPlusParamFile(msgfPlusParameterFilePath, MSGFPLUS_OPTION_TDA);

                if (!int.TryParse(tdaSetting, out var tdaValue))
                {
                    OnErrorEvent("TDA value is not numeric: " + tdaSetting);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (tdaValue == 0)
                {
                    if (!fastaFileIsDecoy && Global.BytesToGB(fastaFile.Length) > 1)
                    {
                        // Large FASTA file (over 1 GB in size)
                        // TDA is 0, so we're performing a forward-only search
                        // Auto-change fastaFileIsDecoy to true to prevent the reverse indices from being created

                        fastaFileIsDecoy = true;

                        if (mDebugLevel >= 1)
                        {
                            OnStatusEvent("Processing large FASTA file with forward-only search; auto switching to -tda 0");
                        }
                    }
                    else if (msgfPlusParameterFilePath.EndsWith("_NoDecoy.txt", StringComparison.OrdinalIgnoreCase))
                    {
                        // Parameter file ends in _NoDecoy.txt and TDA = 0, thus we're performing a forward-only search
                        // Auto-change fastaFileIsDecoy to true to prevent the reverse indices from being created

                        fastaFileIsDecoy = true;

                        if (mDebugLevel >= 1)
                        {
                            OnStatusEvent("Using NoDecoy parameter file with TDA=0; auto switching to -tda 0");
                        }
                    }
                }
            }

            if (maxFastaFileSizeMB > 0 && fastaFileSizeKB / 1024.0 > maxFastaFileSizeMB)
            {
                // Create a trimmed version of the FASTA file
                OnStatusEvent("FASTA file is over " + maxFastaFileSizeMB + " MB; creating a trimmed version of the FASTA file");

                var fastaFilePathTrimmed = string.Empty;

                // Allow for up to 3 attempts since multiple processes might potentially try to do this at the same time
                var trimIteration = 0;

                while (trimIteration <= 2)
                {
                    trimIteration++;
                    fastaFilePathTrimmed = CreateTrimmedFasta(fastaFilePath, maxFastaFileSizeMB);

                    if (!string.IsNullOrEmpty(fastaFilePathTrimmed))
                    {
                        break;
                    }

                    if (trimIteration <= 2)
                    {
                        var sleepTimeSec = randomGenerator.Next(10, 19);

                        OnStatusEvent("FASTA file trimming failed; waiting " + sleepTimeSec + " seconds then trying again");
                        Global.IdleLoop(sleepTimeSec);
                    }
                }

                if (string.IsNullOrEmpty(fastaFilePathTrimmed))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Update fastaFilePath to use the path to the trimmed version
                fastaFilePath = fastaFilePathTrimmed;

                fastaFile.Refresh();
                fastaFileSizeKB = (float)(fastaFile.Length / 1024.0);
            }

            if (mDebugLevel >= 3 || (mDebugLevel >= 1 && fastaFileSizeKB > 500))
            {
                OnStatusEvent("Indexing FASTA file to create Suffix Array files");
            }

            // Look for the suffix array files that should exist for the FASTA file
            // Either copy them from Gigasax (or Proto-7) or re-create them

            var indexIteration = 0;
            var msgfPlusIndexFilesFolderPath = mMgrParams.GetParam("MSGFPlusIndexFilesFolderPath", @"\\gigasax\MSGFPlus_Index_Files");
            var msgfPlusIndexFilesFolderPathLegacyDB = mMgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", @"\\proto-7\MSGFPlus_Index_Files");

            while (true)
            {
                indexIteration++;

                var result = indexedDBCreator.CreateSuffixArrayFiles(mWorkDir, mDebugLevel, javaProgLoc, msgfPlusProgLoc, fastaFilePath,
                    fastaFileIsDecoy, msgfPlusIndexFilesFolderPath, msgfPlusIndexFilesFolderPathLegacyDB);

                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    break;
                }

                if (indexedDBCreator.InsufficientFreeMemory)
                {
                    InsufficientFreeMemory = true;
                    ErrorMessage = "Not enough free memory to create suffix array files";
                    OnErrorEvent(indexedDBCreator.ErrorMessage);
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
                }

                if (result == CloseOutType.CLOSEOUT_FAILED || indexIteration > 2)
                {
                    if (!string.IsNullOrEmpty(indexedDBCreator.ErrorMessage))
                    {
                        ErrorMessage = indexedDBCreator.ErrorMessage;
                    }
                    else
                    {
                        ErrorMessage = "Error creating Suffix Array files";
                    }
                    OnErrorEvent(indexedDBCreator.ErrorMessage);

                    return result;
                }

                var sleepTimeSec = randomGenerator.Next(10, 19);

                OnStatusEvent("FASTA file indexing failed; waiting " + sleepTimeSec + " seconds then trying again");
                Global.IdleLoop(sleepTimeSec);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Reads the contents of a _ScanType.txt file, returning the scan info using three generic dictionary objects
        /// </summary>
        /// <param name="scanTypeFilePath">Scan type file path</param>
        /// <param name="lowResMSn">Low-res MSn spectra</param>
        /// <param name="highResMSn">High-res MSn spectra (but not HCD)</param>
        /// <param name="hcdMSn">HCD Spectra</param>
        /// <param name="other">Spectra that are not MSn</param>
        public bool LoadScanTypeFile(string scanTypeFilePath, out Dictionary<int, string> lowResMSn, out Dictionary<int, string> highResMSn,
            out Dictionary<int, string> hcdMSn, out Dictionary<int, string> other)
        {
            var scanNumberColIndex = -1;
            var scanTypeNameColIndex = -1;

            lowResMSn = new Dictionary<int, string>();
            highResMSn = new Dictionary<int, string>();
            hcdMSn = new Dictionary<int, string>();
            other = new Dictionary<int, string>();

            try
            {
                if (!File.Exists(scanTypeFilePath))
                {
                    ErrorMessage = "ScanType file not found: " + scanTypeFilePath;
                    return false;
                }

                using var reader = new StreamReader(new FileStream(scanTypeFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var columns = dataLine.Split('\t').ToList();

                    if (scanNumberColIndex < 0)
                    {
                        // Parse the header line to define the mapping
                        // Expected headers are ScanNumber   ScanTypeName   ScanType
                        scanNumberColIndex = columns.IndexOf("ScanNumber");
                        scanTypeNameColIndex = columns.IndexOf("ScanTypeName");
                        continue;
                    }

                    if (!int.TryParse(columns[scanNumberColIndex], out var scanNumber))
                        continue;

                    if (scanTypeNameColIndex < 0)
                        continue;

                    var scanType = columns[scanTypeNameColIndex];
                    var scanTypeLCase = scanType.ToLower();

                    if (scanTypeLCase.Contains("hcd"))
                    {
                        hcdMSn.Add(scanNumber, scanType);
                    }
                    else if (scanTypeLCase.Contains("hmsn"))
                    {
                        highResMSn.Add(scanNumber, scanType);
                    }
                    else if (scanTypeLCase.Contains("msn"))
                    {
                        // Not HCD and doesn't contain HMSn; assume low-res
                        lowResMSn.Add(scanNumber, scanType);
                    }
                    else if (scanTypeLCase.Contains("cid") || scanTypeLCase.Contains("etd"))
                    {
                        // The ScanTypeName likely came from the "Collision Mode" column of a MASIC ScanStatsEx file; we don't know if it is high-res MSn or low-res MSn
                        // This will be the case for MASIC results from prior to February 1, 2010, since those results did not have the ScanTypeName column in the _ScanStats.txt file
                        // We'll assume low-res
                        lowResMSn.Add(scanNumber, scanType);
                    }
                    else
                    {
                        // Does not contain MSn or HCD
                        // Likely SRM or MS1
                        other.Add(scanNumber, scanType);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in LoadScanTypeFile";
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Check for a modification definition that has an incorrect tag type
        /// </summary>
        /// <param name="definitionDataClean">Modification definition, in a standardized form</param>
        /// <param name="definitionType">Modification definition type</param>
        /// <param name="expectedTag">Expected modification type tag</param>
        /// <param name="invalidTag">Invalid tag to look for</param>
        /// <returns>True if an incorrect tag is present, false if no errors</returns>
        private bool MisleadingModDef(string definitionDataClean, string definitionType, string expectedTag, string invalidTag)
        {
            if (!definitionDataClean.Contains("," + invalidTag + ","))
                return false;

            // ReSharper disable GrammarMistakeInComment

            // One of the following is true:
            //  Static (fixed) mod is listed as dynamic or custom
            //  Dynamic (optional) mod is listed as static or custom
            //  Custom amino acid def is listed as a dynamic or static

            // ReSharper restore GrammarMistakeInComment

            var verboseTag = invalidTag switch
            {
                "opt" => MSGFPLUS_OPTION_DYNAMIC_MOD,
                "fix" => MSGFPLUS_OPTION_STATIC_MOD,
                "custom" => MSGFPLUS_OPTION_CUSTOM_AA,
                _ => "??"
            };

            // Abort the analysis since the parameter file is misleading and needs to be fixed
            // Example messages:
            //  Dynamic mod definition contains ,fix, -- update the param file to have ,opt, or change to StaticMod=
            //  Static mod definition contains ,opt,  -- update the param file to have ,fix, or change to DynamicMod=
            ErrorMessage = string.Format("{0} definition contains ,{1}, -- update the param file to have ,{2}, or change to {3}=", definitionType, invalidTag, expectedTag, verboseTag);
            OnErrorEvent(ErrorMessage);

            return true;
        }

        /// <summary>
        /// Verify that the MS-GF+ .mzid file ends with XML tag MzIdentML
        /// </summary>
        /// <param name="mzidFile">.mzid file</param>
        public static bool MSGFPlusResultsFileHasClosingTag(FileSystemInfo mzidFile)
        {
            // Check whether the mzid file ends with XML tag </MzIdentML>
            var lastLine = string.Empty;

            using var reader = new StreamReader(new FileStream(mzidFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    lastLine = dataLine;
                }
            }

            var validClosingTag = lastLine.Trim().EndsWith("</MzIdentML>", StringComparison.OrdinalIgnoreCase);
            return validClosingTag;
        }

        // Example Console output (verbose mode used by an old version of MS-GF+):

        // MS-GF+ Release (v2016.01.20) (1/20/2016)
        // Loading database files...
        // Loading database finished (elapsed time: 4.93 sec)
        // Reading spectra...
        // Ignoring 0 profile spectra.
        // Ignoring 0 spectra having less than 5 peaks.
        // Reading spectra finished (elapsed time: 113.00 sec)
        // Using 7 threads.
        // Search Parameters:
        // 	PrecursorMassTolerance: 20.0ppm
        // 	IsotopeError: -1,2
        // 	TargetDecoyAnalysis: true
        // 	FragmentationMethod: As written in the spectrum or CID if no info
        // 	Instrument: LowRes
        // 	Enzyme: Tryp
        // 	Protocol: Phosphorylation
        // 	NumTolerableTermini: 2
        // 	MinPeptideLength: 6
        // 	MaxPeptideLength: 50
        // 	NumMatchesPerSpec: 2
        // Spectrum 0-138840 (total: 138841)
        // Splitting work into 128 tasks.
        // pool-1-thread-1: Starting task 1
        // pool-1-thread-2: Starting task 2
        // pool-1-thread-4: Starting task 4
        // pool-1-thread-7: Starting task 7
        // Search progress: 0 / 128 tasks, 0.0%
        // pool-1-thread-4: Preprocessing spectra...
        // Loading built-in param file: HCD_QExactive_Tryp_Phosphorylation.param
        // Loading built-in param file: CID_LowRes_Tryp_Phosphorylation.param
        // pool-1-thread-3: Preprocessing spectra...
        // Loading built-in param file: ETD_LowRes_Tryp_Phosphorylation.param
        // pool-1-thread-6: Preprocessing spectra...
        // pool-1-thread-1: Preprocessing spectra...
        // pool-1-thread-6: Preprocessing spectra finished (elapsed time: 16.00 sec)
        // pool-1-thread-6: Database search...
        // pool-1-thread-6: Database search progress... 0.0% complete
        // pool-1-thread-7: Preprocessing spectra finished (elapsed time: 16.00 sec)
        // pool-1-thread-7: Database search...
        // pool-1-thread-7: Database search progress... 0.0% complete
        // pool-1-thread-4: Database search progress... 8.8% complete
        // pool-1-thread-7: Computing spectral E-values... 92.2% complete
        // pool-1-thread-7: Computing spectral E-values finished (elapsed time: 77.00 sec)
        // Search progress: 0 / 128 tasks, 0.0%
        // pool-1-thread-7: Task 7 completed.
        // pool-1-thread-7: Starting task 8
        // pool-1-thread-6: Database search progress... 35.3% complete
        // pool-1-thread-2: Database search finished (elapsed time: 498.00 sec)
        // pool-1-thread-2: Computing spectral E-values...
        // pool-1-thread-2: Database search finished (elapsed time: 500.00 sec)
        // pool-1-thread-2: Computing spectral E-values...
        // pool-1-thread-5: Computing spectral E-values finished (elapsed time: 63.00 sec)
        // pool-1-thread-5: Task 18 completed.
        // pool-1-thread-5: Starting task 25
        // pool-1-thread-5: Preprocessing spectra...
        // Search progress: 18 / 128 tasks, 14.1%
        // pool-1-thread-5: Preprocessing spectra finished (elapsed time: 8.00 sec)
        // pool-1-thread-5: Database search...
        // pool-1-thread-5: Database search progress... 0.0% complete
        // pool-1-thread-3: Computing spectral E-values... 92.2% complete
        // pool-1-thread-5: Database search progress... 8.8% complete
        // Computing q-values...
        // Computing q-values finished (elapsed time: 0.13 sec)
        // Writing results...
        // Writing results finished (elapsed time: 11.50 sec)
        // MS-GF+ complete (total elapsed time: 3730.61 sec)

        // Example Console output (compact mode, default starting 2017 January 30):
        // MS-GF+ Release (v2017.01.27) (27 Jan 2017)
        // Loading database files...
        // Loading database finished (elapsed time: 0.61 sec)
        // Reading spectra...
        // Ignoring 0 profile spectra.
        // Ignoring 0 spectra having less than 5 peaks.
        // Reading spectra finished (elapsed time: 15.54 sec)
        // Using 7 threads.
        // Search Parameters:
        // 	PrecursorMassTolerance: 20.0 ppm
        // 	IsotopeError: -1,2
        // Spectrum 0-27672 (total: 27673)
        // Splitting work into 21 tasks.
        // Search progress: 0 / 21 tasks, 0.00%		0.02 seconds elapsed
        // Loading built-in param file: HCD_HighRes_Tryp.param
        // Search progress: 0 / 21 tasks, 13.99%		1.00 minutes elapsed
        // Search progress: 0 / 21 tasks, 27.11%		2.00 minutes elapsed
        // Search progress: 0 / 21 tasks, 29.41%		3.00 minutes elapsed
        // Search progress: 0 / 21 tasks, 30.38%		3.38 minutes elapsed
        // Search progress: 1 / 21 tasks, 31.66%		3.65 minutes elapsed
        // Search progress: 2 / 21 tasks, 32.87%		3.81 minutes elapsed
        // Search progress: 3 / 21 tasks, 34.45%		4.00 minutes elapsed
        // Search progress: 3 / 21 tasks, 34.89%		4.02 minutes elapsed
        // Search progress: 20 / 21 tasks, 100.00%		17.25 minutes elapsed
        // Search progress: 21 / 21 tasks, 100.00%		17.25 minutes elapsed
        // Computing q-values...
        // Computing q-values finished (elapsed time: 0.16 sec)
        // Writing results...
        // Writing results finished (elapsed time: 22.71 sec)
        // MS-GF+ complete (total elapsed time: 1073.62 sec)

        private readonly Regex reExtractThreadCount = new(@"Using (?<ThreadCount>\d+) threads", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reExtractTaskCount = new(@"Splitting work into +(?<TaskCount>\d+) +tasks", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reSpectraSearched = new(@"Spectrum.+\(total: *(?<SpectrumCount>\d+)\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reTaskComplete = new(@"pool-\d+-thread-\d+: Task +(?<TaskNumber>\d+) +completed", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex rePercentComplete = new(@"Search progress: (?<TasksComplete>\d+) / \d+ tasks?, (?<PercentComplete>[0-9.]+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reElapsedTime = new("(?<ElapsedTime>[0-9.]+) (?<Units>seconds|minutes|hours) elapsed", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSGFPlus console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        /// <remarks>MSGFPlus version is available via the MSGFPlusVersion property</remarks>
        /// <returns>Percent Complete (value between 0 and 96)</returns>
        public float ParseMSGFPlusConsoleOutputFile(string workingDirectory)
        {
            var consoleOutputFilePath = "??";

            float effectiveProgress = 0;
            float percentCompleteAllTasks = 0;
            var tasksCompleteViaSearchProgress = 0;
            float totalElapsedTimeHours = 0;

            try
            {
                consoleOutputFilePath = Path.Combine(workingDirectory, MSGFPLUS_CONSOLE_OUTPUT_FILE);

                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        OnStatusEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return 0;
                }

                if (mDebugLevel >= 4)
                {
                    OnStatusEvent("Parsing file " + consoleOutputFilePath);
                }

                // This is the total threads that MS-GF+ reports that it is using
                short totalThreadCount = 0;

                var totalTasks = 0;

                // List of completed task numbers
                var completedTasks = new SortedSet<int>();

                ConsoleOutputErrorMsg = string.Empty;

                effectiveProgress = PROGRESS_PCT_MSGFPLUS_STARTING;
                ContinuumSpectraSkipped = 0;
                SpectraSearched = 0;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var dataLineLCase = dataLine.ToLower();

                    if (linesRead <= 3)
                    {
                        // Originally the first line was the MS-GF+ version
                        // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                        // The third line is the MS-GF+ version
                        if (string.IsNullOrWhiteSpace(MSGFPlusVersion) && dataLine.StartsWith("MS-GF+ Release", StringComparison.OrdinalIgnoreCase))
                        {
                            if (mDebugLevel >= 2 && string.IsNullOrWhiteSpace(MSGFPlusVersion))
                            {
                                OnStatusEvent("MS-GF+ version: " + dataLine);
                            }

                            MSGFPlusVersion = dataLine;
                        }
                        else
                        {
                            if (dataLineLCase.Contains("error"))
                            {
                                if (string.IsNullOrEmpty(ConsoleOutputErrorMsg))
                                {
                                    ConsoleOutputErrorMsg = "Error running MS-GF+: ";
                                }
                                if (!ConsoleOutputErrorMsg.Contains(dataLine))
                                {
                                    ConsoleOutputErrorMsg += "; " + dataLine;
                                }
                            }
                        }
                    }

                    // Look for warning messages
                    // Additionally, update progress if the line starts with one of the expected phrases
                    if (dataLine.StartsWith("Ignoring spectrum", StringComparison.OrdinalIgnoreCase))
                    {
                        // Spectra are typically ignored either because they have too few ions, or because the data is not centroided
                        if (dataLine.IndexOf("spectrum is not centroided", StringComparison.OrdinalIgnoreCase) > 0)
                        {
                            ContinuumSpectraSkipped++;
                        }
                    }
                    else if (dataLine.StartsWith("Loading database files", StringComparison.OrdinalIgnoreCase))
                    {
                        if (effectiveProgress < PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE)
                        {
                            effectiveProgress = PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE;
                        }
                    }
                    else if (dataLine.StartsWith("Reading spectra", StringComparison.OrdinalIgnoreCase))
                    {
                        if (effectiveProgress < PROGRESS_PCT_MSGFPLUS_READING_SPECTRA)
                        {
                            effectiveProgress = PROGRESS_PCT_MSGFPLUS_READING_SPECTRA;
                        }
                    }
                    else if (dataLine.StartsWith("Using", StringComparison.OrdinalIgnoreCase))
                    {
                        // Extract out the thread or task count
                        var threadMatch = reExtractThreadCount.Match(dataLine);

                        if (threadMatch.Success)
                        {
                            short.TryParse(threadMatch.Groups["ThreadCount"].Value, out totalThreadCount);

                            if (effectiveProgress < PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED)
                            {
                                effectiveProgress = PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED;
                            }
                        }
                    }
                    else if (dataLine.StartsWith("Splitting", StringComparison.OrdinalIgnoreCase))
                    {
                        var taskMatch = reExtractTaskCount.Match(dataLine);

                        if (taskMatch.Success)
                        {
                            int.TryParse(taskMatch.Groups["TaskCount"].Value, out totalTasks);
                        }
                    }
                    else if (dataLine.StartsWith("Spectrum", StringComparison.OrdinalIgnoreCase))
                    {
                        // Extract out the number of spectra that MS-GF+ will actually search
                        var spectraSearchedMatch = reSpectraSearched.Match(dataLine);

                        if (spectraSearchedMatch.Success && int.TryParse(spectraSearchedMatch.Groups["SpectrumCount"].Value, out var spectraSearched))
                        {
                            SpectraSearched = spectraSearched;
                        }
                    }
                    else if (dataLine.StartsWith("Computing EFDRs", StringComparison.OrdinalIgnoreCase) ||
                             dataLine.StartsWith("Computing q-values", StringComparison.OrdinalIgnoreCase))
                    {
                        if (effectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS)
                        {
                            effectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS;
                        }
                    }
                    else if (dataLine.StartsWith("MS-GF+ complete", StringComparison.OrdinalIgnoreCase))
                    {
                        if (effectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPLETE)
                        {
                            effectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPLETE;
                        }
                    }
                    else if (string.IsNullOrEmpty(ConsoleOutputErrorMsg))
                    {
                        if (dataLineLCase.Contains("error") && !dataLineLCase.Contains("IsotopeError:".ToLower()))
                        {
                            ConsoleOutputErrorMsg += "; " + dataLine;
                        }
                    }

                    UpdateCompletedTasks(dataLine, completedTasks);

                    UpdatePercentComplete(dataLine, ref percentCompleteAllTasks, ref tasksCompleteViaSearchProgress);

                    UpdateElapsedTime(dataLine, ref totalElapsedTimeHours);
                }

                ThreadCountActual = totalThreadCount;

                TaskCountTotal = totalTasks;
                TaskCountCompleted = completedTasks.Count;

                if (TaskCountCompleted == 0 && tasksCompleteViaSearchProgress > 0)
                {
                    TaskCountCompleted = tasksCompleteViaSearchProgress;
                }

                if (percentCompleteAllTasks > 0)
                {
                    effectiveProgress = percentCompleteAllTasks * PROGRESS_PCT_MSGFPLUS_COMPLETE / 100f;

                    if (effectiveProgress > PROGRESS_PCT_MSGFPLUS_COMPLETE)
                    {
                        effectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPLETE;
                    }
                }

                ElapsedTimeHours = totalElapsedTimeHours;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    OnWarningEvent("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }

            return effectiveProgress;
        }

        /// <summary>
        /// Parses the static modifications, dynamic modifications, and custom amino acid information to create the MS-GF+ Mods file
        /// </summary>
        /// <param name="sourceParameterFilePath">Full path to the MS-GF+ parameter file; will create file MSGFPlus_Mods.txt in the same folder</param>
        /// <param name="options">String builder of command line arguments to pass to MS-GF+</param>
        /// <param name="numMods">Max Number of Modifications per peptide</param>
        /// <param name="staticMods">List of Static Mods</param>
        /// <param name="dynamicMods">List of Dynamic Mods</param>
        /// <param name="customAminoAcids">List of Custom Amino Acids</param>
        /// <returns>True if success, false if an error</returns>
        [Obsolete("Deprecated in February 2019")]
        private bool ParseMSGFDBModifications(string sourceParameterFilePath, StringBuilder options, int numMods,
            IReadOnlyCollection<string> staticMods, IReadOnlyCollection<string> dynamicMods, IReadOnlyCollection<string> customAminoAcids)
        {
            try
            {
                var parameterFile = new FileInfo(sourceParameterFilePath);

                if (string.IsNullOrWhiteSpace(parameterFile.DirectoryName))
                {
                    OnErrorEvent("Unable to determine the parent directory of " + parameterFile.FullName);
                    return false;
                }

                var modFilePath = Path.Combine(parameterFile.DirectoryName, MOD_FILE_NAME);

                // Note that ParseMSGFPlusValidateMod will set this to true if a dynamic or static mod is STY phosphorylation
                PhosphorylationSearch = false;

                options.Append(" -mod " + MOD_FILE_NAME);

                using var writer = new StreamWriter(new FileStream(modFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("# This file is used to specify modifications for MS-GF+");
                writer.WriteLine();
                writer.WriteLine("# Max Number of Modifications per peptide");
                writer.WriteLine("# If this value is large, the search will be slow");
                writer.WriteLine("NumMods=" + numMods);

                if (customAminoAcids.Count > 0)
                {
                    // Custom Amino Acid definitions need to be listed before static or dynamic modifications
                    writer.WriteLine();
                    writer.WriteLine("# Custom Amino Acids");

                    foreach (var customAADef in customAminoAcids)
                    {
                        if (ParseMSGFPlusValidateMod(customAADef, out var customAADefClean))
                        {
                            if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "opt"))
                                return false;

                            if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "fix"))
                                return false;
                            writer.WriteLine(customAADefClean);
                        }
                        else
                        {
                            return false;
                        }
                    }
                }

                writer.WriteLine();
                writer.WriteLine("# Static mods");

                if (staticMods.Count == 0)
                {
                    writer.WriteLine("# None");
                }
                else
                {
                    foreach (var staticMod in staticMods)
                    {
                        if (ParseMSGFPlusValidateMod(staticMod, out var modClean))
                        {
                            if (MisleadingModDef(modClean, "Static mod", "fix", "opt"))
                                return false;

                            if (MisleadingModDef(modClean, "Static mod", "fix", "custom"))
                                return false;
                            writer.WriteLine(modClean);
                        }
                        else
                        {
                            return false;
                        }
                    }
                }

                writer.WriteLine();
                writer.WriteLine("# Dynamic mods");

                if (dynamicMods.Count == 0)
                {
                    writer.WriteLine("# None");
                }
                else
                {
                    foreach (var dynamicMod in dynamicMods)
                    {
                        if (ParseMSGFPlusValidateMod(dynamicMod, out var modClean))
                        {
                            if (MisleadingModDef(modClean, "Dynamic mod", "opt", "fix"))
                                return false;

                            if (MisleadingModDef(modClean, "Dynamic mod", "opt", "custom"))
                                return false;
                            writer.WriteLine(modClean);
                        }
                        else
                        {
                            return false;
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception creating MS-GF+ Mods file";
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Read the MS-GF+ options file and optionally create a new, customized version
        /// </summary>
        /// <param name="fastaFileIsDecoy">True if the FASTA file has had forward and reverse index files created</param>
        /// <param name="assumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="scanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="sourceParameterFilePath">Full path to the MS-GF+ parameter file to use</param>
        /// <param name="sourceParamFile">FileInfo object to the source parameter file. If a new parameter file was created, this will now have extension .original</param>
        /// <param name="finalParamFile">FileInfo object to the parameter file to use; will have path sourceParameterFilePath</param>
        /// <returns>Options string if success; empty string if an error</returns>
        public CloseOutType ParseMSGFPlusParameterFile(
            bool fastaFileIsDecoy, string assumedScanType,
            string scanTypeFilePath, string instrumentGroup, string sourceParameterFilePath,
            out FileInfo sourceParamFile, out FileInfo finalParamFile)
        {
            var overrideParams = new Dictionary<string, string>();

            return ParseMSGFPlusParameterFile(
                fastaFileIsDecoy, assumedScanType,
                scanTypeFilePath, instrumentGroup, sourceParameterFilePath,
                overrideParams, out sourceParamFile, out finalParamFile);
        }

        /// <summary>
        /// Read the MS-GF+ options file and create a new, customized version
        /// </summary>
        /// <param name="fastaFileIsDecoy">True if the FASTA file has had forward and reverse index files created</param>
        /// <param name="assumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="scanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="sourceParameterFilePath">Full path to the MS-GF+ parameter file to read</param>
        /// <param name="overrideParams">Parameters to override settings in the MS-GF+ parameter file (Keys are parameter name, value is the override value)</param>
        /// <param name="sourceParamFile">FileInfo object to the source parameter file. If a new parameter file was created, this will now have extension .original</param>
        /// <param name="finalParamFile">FileInfo object to the parameter file to use; will have path sourceParameterFilePath</param>
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS, otherwise an error code</returns>
        public CloseOutType ParseMSGFPlusParameterFile(
            bool fastaFileIsDecoy, string assumedScanType,
            string scanTypeFilePath, string instrumentGroup, string sourceParameterFilePath,
            Dictionary<string, string> overrideParams,
            out FileInfo sourceParamFile, out FileInfo finalParamFile)
        {
            var paramFileThreadCount = 0;

            var staticMods = new List<string>();
            var dynamicMods = new List<string>();
            var customAminoAcids = new List<string>();
            var enzymeDefs = new List<string>();

            var isTDA = false;

            sourceParamFile = new FileInfo(sourceParameterFilePath);
            finalParamFile = new FileInfo(sourceParameterFilePath);

            if (!sourceParamFile.Exists)
            {
                OnErrorEvent("Parameter file Not found:  " + sourceParameterFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var outputDirectory = sourceParamFile.Directory;

            if (outputDirectory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of " + sourceParamFile.FullName);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var paramFileReader = new KeyValueParamFileReader("MS-GF+", sourceParamFile.FullName);
            RegisterEvents(paramFileReader);

            var paramFileSuccess = paramFileReader.ParseKeyValueParameterFileGetAllLines(out var sourceParamFileLines);

            if (!paramFileSuccess)
            {
                ErrorMessage = Global.AppendToComment(
                    "Error reading MS-GF+ parameter file in ParseMSGFPlusParameterFile",
                    paramFileReader.ErrorMessage);

                return paramFileReader.ParamFileNotFound ? CloseOutType.CLOSEOUT_NO_PARAM_FILE : CloseOutType.CLOSEOUT_FAILED;
            }

            // Keys are the parameter name, values are the parameter line(s) and associated MSGFPlusParameter(s)
            var paramFileParamToLineMapping = new Dictionary<string, List<MSGFPlusKeyValueParamFileLine>>(StringComparer.OrdinalIgnoreCase);

            // This will be set to true if the parameter file has TDA=1, meaning MS-GF+ will auto-added decoy proteins to its list of candidate proteins
            // When TDA is 1, the FASTA must only contain normal (forward) protein sequences
            ResultsIncludeAutoAddedDecoyPeptides = false;

            // Initialize the list of MS-GF+ parameters
            var msgfPlusParameters = GetMSGFPlusParameters();

            if (msgfPlusParameters.Count == 0)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                {
                    ErrorMessage = "GetMSGFPlusParameters returned an empty dictionary";
                    OnErrorEvent(ErrorMessage);
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var msgfPlusParamFileLines = new List<MSGFPlusKeyValueParamFileLine>();

            try
            {
                foreach (var sourceParamFileLine in sourceParamFileLines)
                {
                    var paramFileLine = new MSGFPlusKeyValueParamFileLine(sourceParamFileLine);
                    msgfPlusParamFileLines.Add(paramFileLine);

                    if (!paramFileLine.HasParameter)
                    {
                        continue;
                    }

                    // Remove the comment, if present
                    var valueText = ExtractComment(paramFileLine.ParamValue, out var comment, out var whiteSpaceBeforeComment);

                    // Check whether paramFileLine.ParamName is one of the standard parameter names defined in msgfPlusParameters
                    if (TryGetParameter(msgfPlusParameters, paramFileLine.ParamName, valueText, out var paramInfo))
                    {
                        if (!string.IsNullOrWhiteSpace(comment))
                        {
                            paramInfo.UpdateComment(comment, whiteSpaceBeforeComment);
                        }

                        paramFileLine.StoreParameter(paramInfo);

                        if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_FRAGMENTATION_METHOD))
                        {
                            if (string.IsNullOrWhiteSpace(paramFileLine.ParamInfo.Value) && !string.IsNullOrWhiteSpace(scanTypeFilePath))
                            {
                                // No setting for FragmentationMethodID, and a ScanType file was created
                                // Use FragmentationMethodID 0 (as written in the spectrum, or CID)
                                paramFileLine.UpdateParamValue("0");

                                OnStatusEvent("Using Fragmentation method ID " + paramFileLine.ParamInfo.Value + " because a ScanType file was created");
                            }
                            else if (!string.IsNullOrWhiteSpace(assumedScanType))
                            {
                                // ReSharper disable GrammarMistakeInComment

                                // Override FragmentationMethodID using assumedScanType
                                // AssumedScanType is an optional job setting; see for example:
                                //  IonTrapDefSettings_AssumeHCD.xml with <item key="AssumedScanType" value="HCD"/>

                                // ReSharper restore GrammarMistakeInComment

                                switch (assumedScanType.ToUpper())
                                {
                                    case "CID":
                                        paramFileLine.UpdateParamValue("1");
                                        break;
                                    case "ETD":
                                        paramFileLine.UpdateParamValue("2");
                                        break;
                                    case "HCD":
                                        paramFileLine.UpdateParamValue("3");
                                        break;
                                    case "UVPD":
                                        // Previously, with MSGFDB, fragmentationType 4 meant Merge ETD and CID
                                        // Now with MS-GF+, fragmentationType 4 means UVPD
                                        paramFileLine.UpdateParamValue("4");
                                        break;
                                    default:
                                        // Invalid string
                                        ErrorMessage = string.Format("Invalid assumed scan type '{0}'; must be CID, ETD, HCD, or UVPD",
                                                                     assumedScanType);

                                        OnErrorEvent(ErrorMessage);
                                        WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                                        return CloseOutType.CLOSEOUT_FAILED;
                                }

                                OnStatusEvent("Using Fragmentation method ID {0} because of assumed scan type {1}", paramFileLine.ParamInfo.Value, assumedScanType);
                            }
                            else
                            {
                                OnStatusEvent("Using Fragmentation method ID " + paramFileLine.ParamInfo.Value);
                            }
                        }
                        else if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_INSTRUMENT_ID))
                        {
                            if (!string.IsNullOrWhiteSpace(scanTypeFilePath))
                            {
                                var instrumentLookupResult = DetermineInstrumentID(paramFileLine, scanTypeFilePath, instrumentGroup);

                                if (instrumentLookupResult != CloseOutType.CLOSEOUT_SUCCESS)
                                {
                                    WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                                    return instrumentLookupResult;
                                }
                            }
                            else if (!string.IsNullOrWhiteSpace(instrumentGroup))
                            {
                                if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out var instrumentIDNew, out var autoSwitchReason))
                                {
                                    var datasetName = AnalysisResources.GetDatasetName(mJobParams);

                                    bool scanTypeLookupSuccess;
                                    int countLowResMSn;
                                    int countHighResMSn;
                                    int countLowResHCD;
                                    int countHighResHCD;

                                    if (Global.OfflineMode)
                                    {
                                        countLowResMSn = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SCAN_COUNT_LOW_RES_MSN, 0);
                                        countHighResMSn = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SCAN_COUNT_HIGH_RES_MSN, 0);
                                        countLowResHCD = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SCAN_COUNT_LOW_RES_HCD, 0);
                                        countHighResHCD = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, SCAN_COUNT_HIGH_RES_HCD, 0);

                                        scanTypeLookupSuccess = (countLowResMSn + countHighResMSn + countLowResHCD + countHighResHCD) > 0;
                                    }
                                    else
                                    {
                                        scanTypeLookupSuccess = LookupScanTypesForDataset(datasetName, out countLowResMSn, out countHighResMSn, out countLowResHCD, out countHighResHCD);
                                    }

                                    if (scanTypeLookupSuccess)
                                    {
                                        var success = ExamineScanTypes(countLowResMSn, countHighResMSn, countLowResHCD, countHighResHCD, out instrumentIDNew, out autoSwitchReason);

                                        if (!success)
                                        {
                                            return CloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }

                                AutoUpdateInstrumentIDIfChanged(paramFileLine, instrumentIDNew, autoSwitchReason);
                            }
                            else
                            {
                                ErrorMessage = "Instrument group is empty and a scan type file was not provided; unable to determine the value to use for InstrumentID";
                                OnErrorEvent(ErrorMessage);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                        else if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_STATIC_MOD))
                        {
                            if (!EmptyOrNone(paramFileLine.ParamInfo.Value))
                            {
                                staticMods.Add(paramFileLine.ParamInfo.Value);
                            }
                        }
                        else if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_DYNAMIC_MOD))
                        {
                            if (!EmptyOrNone(paramFileLine.ParamInfo.Value))
                            {
                                dynamicMods.Add(paramFileLine.ParamInfo.Value);
                            }
                        }
                        else if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_CUSTOM_AA))
                        {
                            customAminoAcids.Add(paramFileLine.ParamInfo.Value);
                        }

                        if (AdjustParametersForMSGFPlus(msgfPlusParameters, paramFileLine, out var replacementParameter))
                        {
                            OnStatusEvent("Replacing parameter {0} with {1}={2}", paramFileLine.ParamInfo.ParameterName, replacementParameter.ParameterName, replacementParameter.Value);

                            paramFileLine.ReplaceParameter(replacementParameter);
                        }

                        PossiblyOverrideParameter(overrideParams, paramFileLine);

                        if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_NUM_THREADS))
                        {
                            if (string.IsNullOrWhiteSpace(paramFileLine.ParamInfo.Value))
                            {
                                // NumThreads parameter is specified but does not have a value; change it to "all"
                                paramFileLine.UpdateParamValue("All");
                            }
                            else if (Global.IsMatch(paramFileLine.ParamInfo.Value, "All"))
                            {
                                // As of February 2019, MS-GF+ supports NumThreads=All
                            }
                            else
                            {
                                if (int.TryParse(paramFileLine.ParamInfo.Value, out paramFileThreadCount))
                                {
                                    // paramFileThreadCount now has the thread count
                                }
                                else
                                {
                                    OnWarningEvent("Invalid value for NumThreads in MS-GF+ parameter file: {0}={1}", paramFileLine.ParamInfo.ParameterName, paramFileLine.ParamInfo.Value);
                                    OnStatusEvent("Changing to: {0}={1}", paramFileLine.ParamInfo.ParameterName, "All");
                                }
                            }
                        }
                        else if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, "NumMods"))
                        {
                            if (!int.TryParse(paramFileLine.ParamInfo.Value, out _))
                            {
                                ErrorMessage = string.Format("Invalid value for NumMods in MS-GF+ parameter file: {0}={1}",
                                                             paramFileLine.ParamInfo.ParameterName, paramFileLine.ParamInfo.Value);
                                OnErrorEvent(ErrorMessage);
                                WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                        else if (string.IsNullOrEmpty(paramFileLine.ParamInfo.Value))
                        {
                            if (mDebugLevel >= 1)
                            {
                                OnWarningEvent("Commenting out parameter {0} since the value is empty", paramFileLine.ParamInfo.ParameterName);
                                paramFileLine.ChangeLineToComment();
                            }
                        }

                        if (Global.IsMatch(paramFileLine.ParamInfo.ParameterName, MSGFPLUS_OPTION_TDA))
                        {
                            if (int.TryParse(paramFileLine.ParamInfo.Value, out var value))
                            {
                                if (value > 0)
                                {
                                    isTDA = true;
                                }
                            }
                        }

                        AddUpdateParamFileLineMapping(paramFileParamToLineMapping, paramFileLine.ParamInfo.ParameterName, paramFileLine);
                    }
                    else if (Global.IsMatch(paramFileLine.ParamName, "UniformAAProb") ||
                             Global.IsMatch(paramFileLine.ParamName, "ShowDecoy"))
                    {
                        // Not valid for MS-GF+; comment out this line
                        paramFileLine.ChangeLineToComment("Obsolete");

                        if (mDebugLevel >= 1)
                        {
                            OnWarningEvent("Commenting out parameter {0} since it is not valid for this version of MS-GF+", paramFileLine.ParamName);
                        }
                    }
                    else if (Global.IsMatch(paramFileLine.ParamName, "SkipMzRefinery"))
                    {
                        // Used by MZRefinery; comment out this line so that MS-GF+ does not complain
                        paramFileLine.ChangeLineToComment();
                    }
                    else if (Global.IsMatch(paramFileLine.ParamName, "EnzymeDef"))
                    {
                        if (!EmptyOrNone(valueText))
                        {
                            enzymeDefs.Add(valueText);
                        }
                    }
                }

                if (isTDA)
                {
                    // Parameter file contains TDA=1, and we're running MS-GF+
                    ResultsIncludeAutoAddedDecoyPeptides = true;
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception examining parameters defined in the MS-GF+ parameter file";
                OnErrorEvent(ErrorMessage, ex);
                WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Define the thread count; note that MSGFPlusThreads (previously MSGFDBThreads) could be "all"
            var dmsDefinedThreadCountText = mJobParams.GetJobParameter("MSGFPlusThreads", string.Empty);

            if (string.IsNullOrWhiteSpace(dmsDefinedThreadCountText) ||
                string.Equals(dmsDefinedThreadCountText, "all", StringComparison.OrdinalIgnoreCase) ||
                !int.TryParse(dmsDefinedThreadCountText, out var dmsDefinedThreadCount))
            {
                dmsDefinedThreadCount = 0;
            }

            if (dmsDefinedThreadCount > 0)
            {
                paramFileThreadCount = dmsDefinedThreadCount;
            }

            // If running on a Proto storage server (e.g. Proto-4, Proto-5, or Proto-11),
            // we will limit the number of cores used to 75% of the total core count
            var limitCoreUsage = Dns.GetHostName().StartsWith("Proto-", StringComparison.OrdinalIgnoreCase) ||
                                 Dns.GetHostName().StartsWith("PrismWeb", StringComparison.OrdinalIgnoreCase);

            if (paramFileThreadCount <= 0 || limitCoreUsage)
            {
                // Set paramFileThreadCount to the number of cores on this computer
                // However, do not exceed 8 cores on machines with less than 16 cores, since this can actually slow down MS-GF+ due to context switching
                // Furthermore, Java will restrict the threads to a single NUMA node, and we don't want too many threads on a single node
                // For machines with 16 or more cores, use 75% of the cores

                var coreCount = GetCoreCount();

                if (limitCoreUsage)
                {
                    int maxAllowedCores;

                    if (Dns.GetHostName().StartsWith("PrismWeb3", StringComparison.OrdinalIgnoreCase))
                    {
                        // Use fewer cores on the web server
                        maxAllowedCores = (int)Math.Floor(coreCount * 0.5);
                    }
                    else
                    {
                        maxAllowedCores = (int)Math.Floor(coreCount * 0.75);
                    }

                    if (paramFileThreadCount > 0 && paramFileThreadCount < maxAllowedCores)
                    {
                        // Leave paramFileThreadCount unchanged
                    }
                    else
                    {
                        paramFileThreadCount = maxAllowedCores;
                    }
                }
                else
                {
                    // Prior to July 2014 we would use "coreCount - 1" when the computer had more than 4 cores because MS-GF+ would actually use paramFileThreadCount+1 cores
                    // Starting with version v10072, MS-GF+ actually uses all the cores, so we started using paramFileThreadCount = coreCount

                    // Then, in April 2015, we started running two copies of MS-GF+ simultaneously on machines with > 4 cores
                    //  because even if we tell MS-GF+ to use all the cores, we saw a lot of idle time
                    // When two simultaneous copies of MS-GF+ are running the CPUs get a bit overtaxed, so we're now using this logic:

                    if (coreCount > 4)
                    {
                        paramFileThreadCount = coreCount - 1;
                    }
                    else
                    {
                        paramFileThreadCount = coreCount;
                    }
                }

                if (paramFileThreadCount > 8)
                {
                    if (coreCount >= 16)
                    {
                        // There are enough spare cores that we can use 75% of the cores
                        var maxAllowedCores = (int)Math.Floor(coreCount * 0.75);

                        if (paramFileThreadCount > maxAllowedCores)
                        {
                            OnStatusEvent("The system has " + coreCount + " cores; MS-GF+ will use " + maxAllowedCores + " cores " +
                                          "(bumped down from " + paramFileThreadCount + " to avoid overloading a single NUMA node)");
                            paramFileThreadCount = maxAllowedCores;
                        }
                    }
                    else
                    {
                        // Example message: The system has 12 cores; MS-GF+ will use 8 cores (bumped down from 9 to avoid overloading a single NUMA node)
                        OnStatusEvent("The system has " + coreCount + " cores; MS-GF+ will use 8 cores " +
                                      "(bumped down from " + paramFileThreadCount + " to avoid overloading a single NUMA node)");
                        paramFileThreadCount = 8;
                    }
                }
                else
                {
                    // Example message: The system has 8 cores; MS-GF+ will use 7 cores
                    OnStatusEvent("The system has " + coreCount + " cores; MS-GF+ will use " + paramFileThreadCount + " cores");
                }
            }

            if (paramFileThreadCount > 0)
            {
                if (paramFileParamToLineMapping.TryGetValue(MSGFPLUS_OPTION_NUM_THREADS, out var threadParams) && threadParams.Count > 0)
                {
                    threadParams.First().UpdateParamValue(paramFileThreadCount.ToString(), true);
                }
                else
                {
                    var newThreadParam = GetMSGFPlusParameter(msgfPlusParameters, MSGFPLUS_OPTION_NUM_THREADS, paramFileThreadCount.ToString());
                    AppendParameter(msgfPlusParamFileLines, paramFileParamToLineMapping, newThreadParam);
                }
            }

            // Validate the modifications
            // Also set PhosphorylationSearch to true if a dynamic or static mod is STY phosphorylation
            if (!ValidateMSGFPlusModifications(staticMods, dynamicMods, customAminoAcids))
            {
                WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Look for a custom enzyme definition in the parameter file
            // If defined, create file enzymes.txt in the params directory below the working directory
            if (enzymeDefs.Count > 0 && !CreateEnzymeDefinitionsFile(outputDirectory, enzymeDefs))
            {
                WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Prior to MS-GF+ version v9284 we used " -protocol 1" at the command line when performing an HCD-based phosphorylation search
            // However, v9284 now auto-selects the correct protocol based on the spectrum type and the dynamic modifications
            // Options for -protocol are 0=NoProtocol (Default), 1=Phosphorylation, 2=iTRAQ, 3=iTRAQPhospho

            // As of March 23, 2015, if the user is searching for Phospho mods with TMT labeling enabled,
            // MS-GF+ will use a model trained for TMT peptides (without phospho)
            // In this case, the user should probably use a parameter file with Protocol=1 defined (which leads to Options having "-protocol 1")

            // By default, MS-GF+ filters out spectra with fewer than 10 data points
            // Override this threshold to 5 data points (if not yet defined)
            if (!paramFileParamToLineMapping.TryGetValue(MSGFPLUS_OPTION_MIN_NUM_PEAKS, out _))
            {
                var newParam = GetMSGFPlusParameter(msgfPlusParameters, MSGFPLUS_OPTION_MIN_NUM_PEAKS, "5");
                AppendParameter(msgfPlusParamFileLines, paramFileParamToLineMapping, newParam);
            }

            // Auto-add the "AddFeatures" parameter if not present
            // This is required to post-process the results with Percolator
            if (!paramFileParamToLineMapping.TryGetValue(MSGFPLUS_OPTION_ADD_FEATURES, out _))
            {
                var newParam = GetMSGFPlusParameter(msgfPlusParameters, MSGFPLUS_OPTION_ADD_FEATURES, "1");
                AppendParameter(msgfPlusParamFileLines, paramFileParamToLineMapping, newParam);
            }

            if (paramFileParamToLineMapping.TryGetValue(MSGFPLUS_OPTION_TDA, out var tdaParams) && tdaParams.Count > 0)
            {
                var tdaParam = tdaParams.First();

                if (!int.TryParse(tdaParam.ParamInfo.Value, out var tdaSetting))
                {
                    OnErrorEvent("TDA parameter is not numeric in the parameter file; it should be 0 or 1, see " + tdaParam.Text);
                    WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (tdaSetting > 0)
                {
                    // Make sure the FASTA file is not a Decoy FASTA
                    if (fastaFileIsDecoy)
                    {
                        OnErrorEvent("Parameter file / decoy protein collection conflict: " +
                                     "do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)");
                        WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, true, out finalParamFile);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            return WriteMSGFPlusParameterFile(sourceParamFile, msgfPlusParamFileLines, false, out finalParamFile);
        }

        /// <summary>
        /// Override Instrument ID based on the instrument class and scan types in the _ScanType file
        /// </summary>
        /// <param name="paramFileLine">MS-GF+ parameter file line tracking instrument ID; its value may get updated by this method</param>
        /// <param name="scanTypeFilePath">Scan type file path</param>
        /// <param name="instrumentGroup">Instrument group</param>
        private CloseOutType DetermineInstrumentID(MSGFPlusKeyValueParamFileLine paramFileLine, string scanTypeFilePath, string instrumentGroup)
        {
            // InstrumentID values:
            // #  0 means Low-res LCQ/LTQ (Default for CID and ETD); use InstrumentID=0 if analyzing a dataset with low-res CID and high-res HCD spectra
            // #  1 means High-res LTQ (Default for HCD; also appropriate for high-res CID); use InstrumentID=1 for Orbitrap, Lumos, Eclipse, and Ascend instruments with high-res MS2 spectra
            // #  2 means TOF
            // #  3 means Q-Exactive; use InstrumentID=3 for Q Exactive, QEHFX, and Exploris instruments

            // The logic for determining InstrumentID is:
            // If the instrument is a QExactive, QEHFX, or Exploris, use InstrumentID 3
            // If the instrument has HCD spectra (which are typically high-res MS2, but sometimes low-res), use InstrumentID 1
            // If the instrument has high-res MS2 spectra, use InstrumentID 1
            // If the instrument has low-res MS2 spectra, use InstrumentID 0

            if (string.IsNullOrEmpty(instrumentGroup))
                instrumentGroup = "#Undefined#";

            if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out var instrumentIDNew, out var autoSwitchReason))
            {
                // Instrument ID is not obvious from the instrument group
                // Examine the scan types in scanTypeFilePath

                // If low-res MS1,  Instrument Group is typically LCQ, LTQ, LTQ-ETD, LTQ-Prep, VelosPro

                // If high-res MS2, Instrument Group is typically VelosOrbi, or LTQ_FT

                // Count the number of high-res CID or ETD spectra
                // Count HCD spectra separately since MS-GF+ has a special scoring model for HCD spectra

                var scanTypeFileLoaded = LoadScanTypeFile(scanTypeFilePath, out var lowResMSn, out var highResMSn, out var hcdMSn, out _);

                if (!scanTypeFileLoaded)
                {
                    if (string.IsNullOrEmpty(ErrorMessage))
                    {
                        ErrorMessage = "LoadScanTypeFile returned false for " + Path.GetFileName(scanTypeFilePath);
                        OnErrorEvent(ErrorMessage);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (lowResMSn.Count + highResMSn.Count + hcdMSn.Count == 0)
                {
                    ErrorMessage = "LoadScanTypeFile could not find any MSn spectra " + Path.GetFileName(scanTypeFilePath);
                    OnErrorEvent(ErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = ExamineScanTypes(lowResMSn.Count, highResMSn.Count, hcdMSn.Count, 0, out instrumentIDNew, out autoSwitchReason);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            AutoUpdateInstrumentIDIfChanged(paramFileLine, instrumentIDNew, autoSwitchReason);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Return true if paramValue is an empty string or "None"
        /// </summary>
        /// <param name="paramValue">Parameter value to examine</param>
        private bool EmptyOrNone(string paramValue)
        {
            return string.IsNullOrWhiteSpace(paramValue) || Global.IsMatch(paramValue, "None");
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Determine the instrument mode based on the number of low-res and high-res MS2 spectra
        /// This method is used when the dataset's instrument group does not have a preferred Instrument ID value
        /// </summary>
        /// <param name="countLowResMSn">Total number of spectra that are MSn (does not include HCD-MSn)</param>
        /// <param name="countHighResMSn">Total number of spectra that are HMSn (does not include HCD-HMSn or SA_HCD-HMSn)</param>
        /// <param name="countLowResHCD">Total number of spectra that are HCD-MSn</param>
        /// <param name="countHighResHCD">Total number of spectra that are HCD-HMSn or SA_HCD-HMSn</param>
        /// <param name="instrumentIDNew">Output: new instrument ID to use (empty string if it should not be changed)</param>
        /// <param name="autoSwitchReason">Output: auto-switch reason</param>
        /// <returns>True if successful, false if countLowResMSn, countHighResMSn, countLowResHCD, and countHighResHCD are all 0</returns>
        private bool ExamineScanTypes(
            int countLowResMSn,
            int countHighResMSn,
            int countLowResHCD,
            int countHighResHCD,
            out string instrumentIDNew,
            out string autoSwitchReason)
        {
            instrumentIDNew = string.Empty;
            autoSwitchReason = string.Empty;

            if (countLowResMSn + countHighResMSn + countLowResHCD + countHighResHCD == 0)
            {
                // Scan counts are all 0; assume this is a critical error
                ErrorMessage = "Scan counts provided to ExamineScanTypes are all 0; cannot auto-update InstrumentID";
                OnErrorEvent(ErrorMessage);
                return false;
            }

            double fractionHiRes = 0;

            if (countHighResMSn > 0)
            {
                fractionHiRes = countHighResMSn / (double)(countLowResMSn + countHighResMSn);
            }

            double fractionLowResHCD = 0;

            if (countLowResHCD > 0)
            {
                fractionLowResHCD = countLowResHCD / (double)(countLowResHCD + countHighResHCD);
            }

            if (fractionHiRes > 0.1 && fractionLowResHCD < 0.5)
            {
                // At least 10% of the spectra are HMSn and no more than 50% of the spectra are HCD-MSn
                instrumentIDNew = "1";
                autoSwitchReason = "since " + (fractionHiRes * 100).ToString("0") + "% of the spectra are HMSn";
                return true;
            }

            if (countLowResMSn == 0 && countLowResHCD == 0 && countHighResHCD > 0)
            {
                // All of the spectra are HCD-HMSn
                instrumentIDNew = "1";
                autoSwitchReason = "since all of the spectra are high resolution HCD";
                return true;
            }

            instrumentIDNew = "0";

            if (countHighResHCD == 0 && countHighResMSn == 0)
            {
                autoSwitchReason = "since all of the spectra are low-res MSn";
            }
            else
            {
                autoSwitchReason = "since there is a mix of low-res and high-res spectra";
            }

            return true;
        }

        /// <summary>
        /// Look for the # comment character in paramLine
        /// If found, extract the comment
        /// </summary>
        /// <param name="paramLine">Parameter file line</param>
        /// <param name="comment">Comment if present, or empty string (does not include the # sign)</param>
        /// <param name="whiteSpaceBeforeComment">Whitespace before the comment, including the # sign</param>
        /// <returns>Parameter line, without the comment</returns>
        private string ExtractComment(string paramLine, out string comment, out string whiteSpaceBeforeComment)
        {
            var poundIndex = paramLine.IndexOf(MSGFPlusKeyValueParamFileLine.COMMENT_CHAR);

            if (poundIndex <= 0)
            {
                comment = string.Empty;
                whiteSpaceBeforeComment = string.Empty;
                return paramLine;
            }

            if (poundIndex < paramLine.Length - 1)
            {
                comment = paramLine.Substring(poundIndex + 1).Trim();
            }
            else
            {
                // Empty comment
                comment = string.Empty;
            }

            var match = mCommentExtractor.Match(paramLine);

            if (!match.Success)
            {
                whiteSpaceBeforeComment = string.Empty;
            }
            else
            {
                whiteSpaceBeforeComment = match.Groups["WhiteSpace"].Value;

                var commentFromRegEx = match.Groups["Comment"].Value;

                if (!string.IsNullOrWhiteSpace(commentFromRegEx) && !string.Equals(comment, commentFromRegEx))
                {
                    comment = commentFromRegEx;
                }
            }

            return paramLine.Substring(0, poundIndex - 1).Trim();
        }

        /// <summary>
        /// Contact the database to determine the number of MSn scans of various type for the given dataset
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="countLowResMSn">Output: Total number of spectra that are MSn (does not include HCD-MSn)</param>
        /// <param name="countHighResMSn">Output: Total number of spectra that are HMSn (does not include HCD-HMSn or SA_HCD-HMSn)</param>
        /// <param name="countLowResHCD">Output: Total number of spectra that are HCD-MSn</param>
        /// <param name="countHighResHCD">Output: Total number of spectra that are HCD-HMSn or SA_HCD-HMSn</param>
        public bool LookupScanTypesForDataset(string datasetName, out int countLowResMSn, out int countHighResMSn, out int countLowResHCD, out int countHighResHCD)
        {
            countLowResMSn = 0;
            countHighResMSn = 0;
            countLowResHCD = 0;
            countHighResHCD = 0;

            try
            {
                if (string.IsNullOrEmpty(datasetName))
                {
                    OnWarningEvent("LookupScanTypesForDataset called with empty dataset name");
                    return false;
                }

                var connectionString = mMgrParams.GetParam("ConnectionString");

                var sqlStr = new StringBuilder();

                // This query runs quickly on SQL Server but takes ~4 seconds on Postgres

                // SELECT "HMS", "MS", "CID-HMSn", "CID-MSn",
                //        "ETD-HMSn", "SA_CID-HMSn",
                //        "SA_ETD-HMSn", "EThcD-HMSn",
                //        "HCD-HMSn", "HCD-MSn", "SA_HCD-HMSn",
                //        "ETD-MSn", "SA_ETD-MSn",
                //        "HMSn", "MSn",
                //        "PQD-HMSn", "PQD-MSn",
                //        "UVPD-HMSn", "UVPD-MSn"
                // FROM V_Dataset_Scan_Type_CrossTab
                // WHERE dataset = 'QC_Mam_19_01_b_17Jan22_Rage_Rep-21-12-20'

                // Query v_dataset_scan_types instead
                sqlStr.Append("SELECT scan_type, scan_count ");
                sqlStr.Append("FROM v_dataset_scan_types ");
                sqlStr.AppendFormat("WHERE dataset = '{0}'", datasetName);

                const int retryCount = 2;

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrParams.ManagerName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
                RegisterEvents(dbTools);

                var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var results, retryCount);

                if (!success)
                {
                    OnErrorEvent("Excessive failures attempting to retrieve dataset scan types in LookupScanTypesForDataset");
                    results.Dispose();
                    return false;
                }

                // Verify at least one row returned
                if (results.Rows.Count < 1)
                {
                    // No data was returned
                    OnStatusEvent("No rows were returned for dataset " + datasetName + " from v_dataset_scan_types in DMS");
                    return false;
                }

                foreach (DataRow curRow in results.Rows)
                {
                    var scanType = curRow["scan_type"].CastDBVal<string>();
                    var scanCount = curRow["scan_count"].CastDBVal<int>();

                    switch (scanType)
                    {
                        case "CID-MSn":
                        case "ETD-MSn":
                        case "SA_ETD-MSn":
                        case "MSn":
                        case "PQD-MSn":
                        case "UVPD-MSn":
                            countLowResMSn += scanCount;
                            break;

                        case "CID-HMSn":
                        case "ETD-HMSn":
                        case "SA_CID-HMSn":
                        case "SA_ETD-HMSn":
                        case "EThcD-HMSn":
                        case "HMSn":
                        case "PQD-HMSn":
                        case "UVPD-HMSn":
                            countHighResMSn += scanCount;
                            break;

                        case "HCD-MSn":
                            countLowResHCD += scanCount;
                            break;

                        case "HCD-HMSn":
                        case "SA_HCD-HMSn":
                            countHighResHCD += scanCount;
                            break;

                        default:
                            if (scanType.EndsWith("-HMSn", StringComparison.OrdinalIgnoreCase))
                            {
                                countHighResMSn += scanCount;
                            }
                            else if (scanType.EndsWith("-MSn", StringComparison.OrdinalIgnoreCase))
                            {
                                countLowResMSn += scanCount;
                            }

                            break;
                    }
                }

                results.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LookupScanTypesForDataset", ex);
                return false;
            }
        }

        /// <summary>
        /// Validates that the modification definition text
        /// </summary>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        /// <param name="modDefLine">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        private bool ParseMSGFPlusValidateMod(string modDefLine, out string modClean)
        {
            modClean = string.Empty;

            var modDef = ExtractComment(modDefLine, out var comment, out _);

            // Split on commas, change tabs to spaces, and remove whitespace
            var modParts = modDef.Split(',');

            for (var i = 0; i < modParts.Length; i++)
            {
                modParts[i] = modParts[i].Replace("\t", " ").Trim();
            }

            // Check whether this is a custom AA definition
            var query = (from item in modParts where string.Equals(item, "custom", StringComparison.OrdinalIgnoreCase) select item).ToList();
            var customAminoAcidDef = query.Count > 0;

            if (modParts.Length < 5)
            {
                // Invalid definition

                if (customAminoAcidDef)
                {
                    // Invalid custom AA definition; must have 5 sections, for example:
                    // C5H7N1O2S0,J,custom,P,Hydroxylation     # Hydroxyproline
                    ErrorMessage = "Invalid custom AA string; must have 5 sections: " + modDef;
                }
                else
                {
                    // Invalid dynamic or static mod definition; must have 5 sections, for example:
                    // O1, M, opt, any, Oxidation
                    ErrorMessage = "Invalid modification string; must have 5 sections: " + modDef;
                }

                OnErrorEvent(ErrorMessage);
                return false;
            }

            // Reconstruct the mod (or custom AA) definition, making sure there is no whitespace
            modClean = string.Copy(modParts[0]);

            if (customAminoAcidDef)
            {
                // Make sure that the custom amino acid definition does not have any invalid characters
                var reInvalidCharacters = new Regex("[^CHNOS0-9]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
                var invalidCharacters = reInvalidCharacters.Matches(modClean);

                if (invalidCharacters.Count > 0)
                {
                    ErrorMessage = "Custom amino acid empirical formula " + modClean + " has invalid characters. " +
                                    "It must only contain C, H, N, O, and S, and optionally an integer after each element, for example: C6H7N3O";
                    OnErrorEvent(ErrorMessage);
                    return false;
                }

                // Make sure that the elements in modClean have a number after them
                // For example, auto-change C6H7N3O to C6H7N3O1

                var reElementSplitter = new Regex(@"(?<Atom>[A-Z])(?<Count>\d*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

                var elements = reElementSplitter.Matches(modClean);
                var reconstructedFormula = string.Empty;

                foreach (Match subPart in elements)
                {
                    var elementSymbol = subPart.Groups["Atom"].ToString();
                    var elementCount = subPart.Groups["Count"].ToString();

                    if (elementSymbol != "C" && elementSymbol != "H" && elementSymbol != "N" && elementSymbol != "O" && elementSymbol != "S")
                    {
                        ErrorMessage = "Invalid element " + elementSymbol + " in the custom amino acid empirical formula " + modClean + "; " +
                                        "MS-GF+ only supports C, H, N, O, and S";
                        OnErrorEvent(ErrorMessage);
                        return false;
                    }

                    if (string.IsNullOrWhiteSpace(elementCount))
                    {
                        reconstructedFormula += elementSymbol + "1";
                    }
                    else
                    {
                        reconstructedFormula += elementSymbol + elementCount;
                    }
                }

                if (!string.Equals(modClean, reconstructedFormula))
                {
                    OnStatusEvent("Auto updated the custom amino acid empirical formula to include a 1 " +
                                  "after elements that did not have an element count listed: " + modClean + " --> " + reconstructedFormula);
                    modClean = reconstructedFormula;
                }
            }

            for (var index = 1; index < modParts.Length; index++)
            {
                modClean += "," + modParts[index];
            }

            // Possibly append the comment (which will start with a # sign)
            if (!string.IsNullOrWhiteSpace(comment))
            {
                modClean += "     " + comment;
            }

            if (customAminoAcidDef)
                return true;

            // Check whether this is a phosphorylation mod
            // Note that MS-GF+ recognizes phosphorylated, phosphorylation, or Phospho for STY phosphorylation
            if (modParts[(int)ModDefinitionParts.Name].StartsWith("Phospho", StringComparison.OrdinalIgnoreCase) ||
                modParts[(int)ModDefinitionParts.EmpiricalFormulaOrMass].StartsWith("HO3P", StringComparison.OrdinalIgnoreCase))
            {
                if (modParts[(int)ModDefinitionParts.Residues].ToUpper().IndexOfAny(new[]
                {
                    'S',
                    'T',
                    'Y'
                }) >= 0)
                {
                    PhosphorylationSearch = true;
                }
            }

            return true;
        }

        /// <summary>
        /// Override the parameter value if defined in overrideParams
        /// </summary>
        /// <param name="overrideParams">Parameters to override</param>
        /// <param name="paramFileLine">MS-GF+ parameter file line</param>
        private void PossiblyOverrideParameter(IReadOnlyDictionary<string, string> overrideParams, MSGFPlusKeyValueParamFileLine paramFileLine)
        {
            if (overrideParams.TryGetValue(paramFileLine.ParamInfo.ParameterName, out var valueOverride))
            {
                OnStatusEvent("Overriding parameter {0} to be {1} instead of {2}", paramFileLine.ParamInfo.ParameterName, valueOverride, paramFileLine.ParamInfo.Value);

                paramFileLine.UpdateParamValue(valueOverride);
            }
        }

        private string ReverseString(string text)
        {
            var reversed = text.ToCharArray();
            Array.Reverse(reversed);
            return new string(reversed);
        }

        private static bool TryGetParameter(IEnumerable<MSGFPlusParameter> msgfPlusParameters,
                                            string parameterName, string parameterValue,
                                            out MSGFPlusParameter msgfPlusParameter)
        {
            foreach (var parameter in msgfPlusParameters)
            {
                // ReSharper disable once InvertIf
                if (string.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase) ||
                    parameter.HasSynonym(parameterName))
                {
                    msgfPlusParameter = parameter.Clone(parameterValue);
                    return true;
                }
            }

            msgfPlusParameter = null;
            return false;
        }

        /// <summary>
        /// If the data line is of the form "pool-1-thread-7: Task 7 completed",
        /// extract out the task number that completed
        /// </summary>
        /// <remarks>This type of status line was removed in January 2017</remarks>
        /// <param name="dataLine">Data line</param>
        /// <param name="completedTasks">List of completed tasks</param>
        private void UpdateCompletedTasks(string dataLine, ISet<int> completedTasks)
        {
            var reMatch = reTaskComplete.Match(dataLine);

            if (reMatch.Success)
            {
                var taskNumber = int.Parse(reMatch.Groups["TaskNumber"].Value);

                // ReSharper disable once CanSimplifySetAddingWithSingleCall

                if (completedTasks.Contains(taskNumber))
                {
                    OnWarningEvent("MS-GF+ reported that task " + taskNumber + " completed more than once");
                }
                else
                {
                    completedTasks.Add(taskNumber);
                }
            }
        }

        /// <summary>
        /// Look for seconds elapsed, minutes elapsed, or hours elapsed in dataLine
        /// If found, and if larger than totalElapsedTimeHours, update totalElapsedTimeHours
        /// </summary>
        /// <param name="dataLine">Data line</param>
        /// <param name="totalElapsedTimeHours">Total elapsed time, in hours</param>
        private void UpdateElapsedTime(string dataLine, ref float totalElapsedTimeHours)
        {
            var reElapsedTimeMatch = reElapsedTime.Match(dataLine);

            if (!reElapsedTimeMatch.Success)
                return;

            var elapsedTimeValue = float.Parse(reElapsedTimeMatch.Groups["ElapsedTime"].Value);
            var elapsedTimeUnits = reElapsedTimeMatch.Groups["Units"].Value;

            var elapsedTimeHours = elapsedTimeUnits switch
            {
                "seconds" => elapsedTimeValue / 3600,
                "minutes" => elapsedTimeValue / 60,
                "hours" => elapsedTimeValue,
                _ => 0
            };

            if (elapsedTimeHours > totalElapsedTimeHours)
            {
                totalElapsedTimeHours = elapsedTimeHours;
            }
        }

        /// <summary>
        /// If the data line is of the form "Search progress: 27 / 36 tasks, 92.33%	"
        /// extract out the number of completed tasks and the percent complete
        /// </summary>
        /// <param name="dataLine">Data line</param>
        /// <param name="percentCompleteAllTasks">Processing percent complete, across all tasks</param>
        /// <param name="tasksCompleteViaSearchProgress">Number of completed tasks</param>
        private void UpdatePercentComplete(string dataLine, ref float percentCompleteAllTasks, ref int tasksCompleteViaSearchProgress)
        {
            var reProgressMatch = rePercentComplete.Match(dataLine);

            if (reProgressMatch.Success)
            {
                var newTasksComplete = int.Parse(reProgressMatch.Groups["TasksComplete"].Value);

                if (newTasksComplete > tasksCompleteViaSearchProgress)
                {
                    tasksCompleteViaSearchProgress = newTasksComplete;
                }

                var newPercentComplete = float.Parse(reProgressMatch.Groups["PercentComplete"].Value);

                if (newPercentComplete > percentCompleteAllTasks)
                {
                    percentCompleteAllTasks = newPercentComplete;
                }
            }
        }

        /// <summary>
        /// Verify that the static mods, dynamic mods, and/or custom amino acid definitions are valid
        /// </summary>
        /// <param name="staticMods">List of static mods</param>
        /// <param name="dynamicMods">List of dynamic mods</param>
        /// <param name="customAminoAcids">List of custom amino acids</param>
        private bool ValidateMSGFPlusModifications(
            IEnumerable<string> staticMods,
            IEnumerable<string> dynamicMods,
            IEnumerable<string> customAminoAcids)
        {
            try
            {
                // Note that ParseMSGFPlusValidateMod will set this to true if a dynamic or static mod is STY phosphorylation
                PhosphorylationSearch = false;

                // Examine custom amino acid definitions
                foreach (var customAADef in customAminoAcids)
                {
                    if (!ParseMSGFPlusValidateMod(customAADef, out var customAADefClean))
                    {
                        return false;
                    }

                    if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "opt"))
                        return false;

                    if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "fix"))
                        return false;
                }

                // Examine static mods
                foreach (var staticMod in staticMods)
                {
                    // Examine the definition to update it to a standard form (minimal whitespace)
                    // modClean will still include the comment, if any
                    if (!ParseMSGFPlusValidateMod(staticMod, out var modClean))
                    {
                        return false;
                    }

                    if (MisleadingModDef(modClean, "Static mod", "fix", "opt"))
                        return false;

                    if (MisleadingModDef(modClean, "Static mod", "fix", "custom"))
                        return false;
                }

                // Examine dynamic mods
                foreach (var dynamicMod in dynamicMods)
                {
                    if (!ParseMSGFPlusValidateMod(dynamicMod, out var modClean))
                    {
                        return false;
                    }

                    if (MisleadingModDef(modClean, "Dynamic mod", "opt", "fix"))
                        return false;

                    if (MisleadingModDef(modClean, "Dynamic mod", "opt", "custom"))
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in ValidateMSGFPlusModifications";
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }
        }

        private bool ValidatePeptideToProteinMapResults(string pepToProtMapFilePath, bool ignorePeptideToProteinMapperErrors)
        {
            const string PROTEIN_NAME_NO_MATCH = "__NoMatch__";

            var peptideCount = 0;
            var peptideCountNoMatch = 0;
            var linesRead = 0;

            var unmatchedPeptides = new List<string>();

            try
            {
                // Validate that none of the results in pepToProtMapFilePath has protein name PROTEIN_NAME_NO_MATCH

                if (mDebugLevel >= 2)
                {
                    OnStatusEvent("Validating peptide to protein mapping, file " + Path.GetFileName(pepToProtMapFilePath));
                }

                using var reader = new StreamReader(new FileStream(pepToProtMapFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (linesRead <= 1 || string.IsNullOrEmpty(dataLine))
                        continue;

                    peptideCount++;

                    if (dataLine.Contains(PROTEIN_NAME_NO_MATCH))
                    {
                        peptideCountNoMatch++;

                        if (unmatchedPeptides.Count < 5)
                        {
                            unmatchedPeptides.Add(dataLine);
                        }
                    }
                }

                if (peptideCount == 0)
                {
                    ErrorMessage = "Peptide to protein mapping file is empty";
                    OnErrorEvent(ErrorMessage + ", file " + Path.GetFileName(pepToProtMapFilePath));
                    return false;
                }

                if (peptideCountNoMatch == 0)
                {
                    if (mDebugLevel >= 2)
                    {
                        OnStatusEvent("Peptide to protein mapping validation complete; processed " + peptideCount + " peptides");
                    }

                    return true;
                }

                // Value between 0 and 100
                var errorPercent = peptideCountNoMatch / (double)peptideCount * 100.0;

                ErrorMessage = string.Format("{0:F1}% of the entries in the peptide to protein map file did not match to a protein in the FASTA file ({1:N0} / {2:N0})",
                                             errorPercent, peptideCountNoMatch, peptideCount);

                if (ignorePeptideToProteinMapperErrors)
                    OnWarningEvent(ErrorMessage);
                else
                    OnErrorEvent(ErrorMessage);

                Console.WriteLine();
                OnDebugEvent("First {0} unmatched peptides", unmatchedPeptides.Count);

                foreach (var unmatchedPeptide in unmatchedPeptides)
                {
                    Console.WriteLine(unmatchedPeptide);
                }
                Console.WriteLine();

                if (ignorePeptideToProteinMapperErrors)
                {
                    OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' is true");
                    return true;
                }

                OnWarningEvent("To ignore this error, create job parameter 'IgnorePeptideToProteinMapError' with value 'True'");

                IgnorePreviousErrorEvent?.Invoke(ErrorMessage);
                return false;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error validating peptide to protein map file";
                OnErrorEvent(ErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Optionally replace the source parameter file with a new one based on the data in msgfPlusParamFileLines
        /// If a new file is created, the source file will be renamed to have extension .original
        /// </summary>
        /// <param name="sourceParamFile">FileInfo object to the source parameter file. If a new parameter file was created, this will now have extension .original</param>
        /// <param name="msgfPlusParamFileLines">List of lines read from the MS-GF+ parameter file</param>
        /// <param name="alwaysCreate">If false, only replace the original file if at least one parameter has been updated; if true, always replace it</param>
        /// <param name="finalParamFile">FileInfo object to the parameter file to use; will have path sourceParameterFilePath</param>
        private CloseOutType WriteMSGFPlusParameterFile(
            FileInfo sourceParamFile,
            IReadOnlyCollection<MSGFPlusKeyValueParamFileLine> msgfPlusParamFileLines,
            bool alwaysCreate,
            out FileInfo finalParamFile)
        {
            try
            {
                // Count the number of new or updated lines
                var updatedLineCount = 0;

                foreach (var paramFileLine in msgfPlusParamFileLines)
                {
                    if (paramFileLine.LineUpdated)
                        updatedLineCount++;
                }

                if (updatedLineCount == 0 && !alwaysCreate)
                {
                    OnDebugEvent("No parameters were customized in " + sourceParamFile.FullName + "; not creating a new file");
                    finalParamFile = sourceParamFile;
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                if (sourceParamFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of " + sourceParamFile.FullName);
                    finalParamFile = sourceParamFile;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var renamedSourceParamFilePath = Path.Combine(sourceParamFile.Directory.FullName,
                                                              Path.ChangeExtension(sourceParamFile.Name, ".original"));

                finalParamFile = new FileInfo(sourceParamFile.FullName);

                if (File.Exists(renamedSourceParamFilePath))
                    File.Delete(renamedSourceParamFilePath);

                sourceParamFile.MoveTo(renamedSourceParamFilePath);

                using (var writer = new StreamWriter(new FileStream(finalParamFile.FullName, FileMode.Create, FileAccess.Write)))
                {
                    foreach (var paramFileLine in msgfPlusParamFileLines)
                    {
                        writer.WriteLine(paramFileLine.Text);
                    }
                }

                // Set the date of the new file to the date of the old file, plus 5 minutes added on for each updated parameter
                var newFileDate = sourceParamFile.LastWriteTimeUtc.AddMinutes(5 * updatedLineCount);
                finalParamFile.LastWriteTimeUtc = newFileDate;

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception creating the customized MS-GF+ parameter file";
                OnErrorEvent(ErrorMessage, ex);
                finalParamFile = sourceParamFile;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void WriteProteinSequence(TextWriter writer, string sequence)
        {
            var index = 0;

            while (index < sequence.Length)
            {
                var length = Math.Min(60, sequence.Length - index);
                writer.WriteLine(sequence.Substring(index, length));
                index += 60;
            }
        }

        /// <summary>
        /// Zips MS-GF+ Output File (creating a .gz file)
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public CloseOutType ZipOutputFile(AnalysisToolRunnerBase toolRunner, string fileName)
        {
            try
            {
                var tmpFilePath = Path.Combine(mWorkDir, fileName);

                if (!File.Exists(tmpFilePath))
                {
                    OnErrorEvent("MS-GF+ results file not found: " + fileName);
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                if (!toolRunner.GZipFile(tmpFilePath, false))
                {
                    OnErrorEvent("Error zipping output files: toolRunner.GZipFile returned false");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
                mJobParams.AddResultFileToSkip(fileName);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error zipping output files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime mLastLogTime = DateTime.MinValue;

        private void PeptideToProteinMapper_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int MAPPER_PROGRESS_LOG_INTERVAL_SECONDS = 120;

            if (mDebugLevel < 1) return;

            if (DateTime.UtcNow.Subtract(mLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS)
            {
                mLastLogTime = DateTime.UtcNow;
                OnStatusEvent("Mapping peptides to proteins: " + percentComplete.ToString("0.0") + "% complete");
            }
        }
    }
}
