using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;
using PeptideToProteinMapEngine;
using PRISM;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// MSGF+ Utilities
    /// </summary>
    public class MSGFPlusUtils : clsEventNotifier
    {
        #region "Constants"

        /// <summary>
        /// Progress value for MSGF+ starting
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_STARTING = 1;

        /// <summary>
        /// Progress value for MSGF+ loading the FASTA file
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE = 2;

        /// <summary>
        /// Progress value for MSGF+ reading the spectra file
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_READING_SPECTRA = 3;

        /// <summary>
        /// Progress value for MSGF+ spawning worker threads
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED = 4;

        /// <summary>
        /// Progress value for MSGF+ computing FDRs
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS = 95;

        /// <summary>
        /// Progress value for MSGF+ having completed
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_COMPLETE = 96;

        /// <summary>
        /// Progress value for conversion of the .mzid file to .tsv
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_CONVERT_MZID_TO_TSV = 97;

        /// <summary>
        /// Progress value for mapping peptides to proteins
        /// </summary>
        public const float PROGRESS_PCT_MSGFPLUS_MAPPING_PEPTIDES_TO_PROTEINS = 98;

        /// <summary>
        /// Progress value for all processing beingcompleted
        /// </summary>
        public const float PROGRESS_PCT_COMPLETE = 99;

        private const string MZIDToTSV_CONSOLE_OUTPUT_FILE = "MzIDToTsv_ConsoleOutput.txt";

        private enum ModDefinitionParts
        {
            EmpiricalFormulaOrMass = 0,
            Residues = 1,
            ModType = 2,
            Position = 3,            // For CustomAA definitions this field is essentially ignored
            Name = 4
        }

        private const string MSGFPLUS_OPTION_TDA = "TDA";
        private const string MSGFPLUS_OPTION_SHOWDECOY = "showDecoy";
        private const string MSGFPLUS_OPTION_FRAGMENTATION_METHOD = "FragmentationMethodID";
        private const string MSGFPLUS_OPTION_INSTRUMENT_ID = "InstrumentID";

        /// <summary>
        /// MSGF+ TSV file suffix
        /// </summary>
        public const string MSGFPLUS_TSV_SUFFIX = "_msgfplus.tsv";

        /// <summary>
        /// MSGF+ jar file name
        /// </summary>
        /// <remarks>Previously MSGFDB.jar</remarks>
        public const string MSGFPLUS_JAR_NAME = "MSGFPlus.jar";

        /// <summary>
        /// MSGF+ console output file anme
        /// </summary>
        public const string MSGFPLUS_CONSOLE_OUTPUT_FILE = "MSGFPlus_ConsoleOutput.txt";

        /// <summary>
        /// MSGF+ mods file name
        /// </summary>
        public const string MOD_FILE_NAME = "MSGFPlus_Mods.txt";

        #endregion

        #region "Events"

        /// <summary>
        /// Even raised when a peptide to protein mapping error has been ignored
        /// </summary>
        public event IgnorePreviousErrorEventEventHandler IgnorePreviousErrorEvent;

        /// <summary>
        /// Delegate for IgnorePreviousErrorEvent
        /// </summary>
        public delegate void IgnorePreviousErrorEventEventHandler();

        #endregion

        #region "Module Variables"

        private readonly IMgrParams m_mgrParams;
        private readonly IJobParams m_jobParams;

        private readonly string m_WorkDir;
        private readonly short m_DebugLevel;

        private string mMSGFPlusVersion;
        private string mErrorMessage = string.Empty;
        private string mConsoleOutputErrorMsg;

        private int mContinuumSpectraSkipped;
        private int mSpectraSearched;

        private int mThreadCountActual;
        private int mTaskCountTotal;
        private int mTaskCountCompleted;

        private bool mPhosphorylationSearch;
        private bool mResultsIncludeAutoAddedDecoyPeptides;

        // Note that clsPeptideToProteinMapEngine utilizes System.Data.SQLite.dll
        private clsPeptideToProteinMapEngine mPeptideToProteinMapper;

        #endregion

        #region "Properties"

        /// <summary>
        /// Number of skipped continuum spectra
        /// </summary>
        public int ContinuumSpectraSkipped => mContinuumSpectraSkipped;

        /// <summary>
        /// Console output error message
        /// </summary>
        public string ConsoleOutputErrorMsg => mConsoleOutputErrorMsg;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// MSGF+ version
        /// </summary>
        public string MSGFPlusVersion => mMSGFPlusVersion;

        /// <summary>
        /// True if searching for phosphorylated S, T, or Y
        /// </summary>
        public bool PhosphorylationSearch => mPhosphorylationSearch;

        /// <summary>
        /// True if the results include auto-added decoy peptides
        /// </summary>
        public bool ResultsIncludeAutoAddedDecoyPeptides => mResultsIncludeAutoAddedDecoyPeptides;

        /// <summary>
        /// Number of spectra searched
        /// </summary>
        public int SpectraSearched => mSpectraSearched;

        /// <summary>
        /// Actual thread count in use
        /// </summary>
        public int ThreadCountActual => mThreadCountActual;

        /// <summary>
        /// Number of processing tasks to be run by MSGF+
        /// </summary>
        public int TaskCountTotal => mTaskCountTotal;

        /// <summary>
        /// Number of completed processing tasks
        /// </summary>
        public int TaskCountCompleted => mTaskCountCompleted;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="oMgrParams"></param>
        /// <param name="oJobParams"></param>
        /// <param name="workDir"></param>
        /// <param name="debugLevel"></param>
        public MSGFPlusUtils(IMgrParams oMgrParams, IJobParams oJobParams, string workDir, short debugLevel)
        {
            m_mgrParams = oMgrParams;
            m_jobParams = oJobParams;

            m_WorkDir = workDir;

            m_DebugLevel = debugLevel;

            mMSGFPlusVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;
            mContinuumSpectraSkipped = 0;
            mSpectraSearched = 0;

            mThreadCountActual = 0;
            mTaskCountTotal = 0;
            mTaskCountCompleted = 0;
        }

        /// <summary>
        /// Update argumentSwitch and argumentValue if using the MS-GFDB syntax yet should be using the MS-GF+ syntax
        /// </summary>
        /// <param name="argumentSwitch"></param>
        /// <param name="argumentValue"></param>
        /// <remarks></remarks>
        private void AdjustSwitchesForMSGFPlus(ref string argumentSwitch, ref string argumentValue)
        {
            if (clsGlobal.IsMatch(argumentSwitch, "nnet"))
            {
                // Auto-switch to ntt
                argumentSwitch = "ntt";
                if (int.TryParse(argumentValue, out var value))
                {
                    switch (value)
                    {
                        case 0:
                            argumentValue = "2";         // Fully-tryptic
                            break;
                        case 1:
                            argumentValue = "1";         // Partially tryptic
                            break;
                        case 2:
                            argumentValue = "0";         // No-enzyme search
                            break;
                        default:
                            // Assume partially tryptic
                            argumentValue = "1";
                            break;
                    }
                }
            }
            else if (clsGlobal.IsMatch(argumentSwitch, "c13"))
            {
                // Auto-switch to ti
                argumentSwitch = "ti";
                if (int.TryParse(argumentValue, out var value))
                {
                    if (value == 0)
                    {
                        argumentValue = "0,0";
                    }
                    else if (value == 1)
                    {
                        argumentValue = "-1,1";
                    }
                    else if (value == 2)
                    {
                        argumentValue = "-1,2";
                    }
                    else
                    {
                        argumentValue = "0,1";
                    }
                }
                else
                {
                    argumentValue = "0,1";
                }
            }
            else if (clsGlobal.IsMatch(argumentSwitch, "showDecoy"))
            {
                // Not valid for MS-GF+; skip it
                argumentSwitch = string.Empty;
            }
        }

        /// <summary>
        /// Append one or more lines from the start of sourceFile to the end of targetFile
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="sourceFile"></param>
        /// <param name="targetFile"></param>
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

                using (var srReader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swWriter = new StreamWriter(new FileStream(targetFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    var separatorAdded = false;

                    while (linesRead < headerLinesToAppend && !srReader.EndOfStream)
                    {
                        var dataLine = srReader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (!separatorAdded)
                        {
                            swWriter.WriteLine(new string('-', 80));
                            separatorAdded = true;
                        }

                        swWriter.WriteLine(dataLine);
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in MSGFPlusUtils->AppendConsoleOutputHeader", ex);
            }
        }

        private bool CanDetermineInstIdFromInstGroup(string instrumentGroup, out string instrumentIDNew, out string autoSwitchReason)
        {
            if (clsGlobal.IsMatch(instrumentGroup, "QExactive"))
            {
                // Thermo Instruments
                instrumentIDNew = "3";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (clsGlobal.IsMatch(instrumentGroup, "Bruker_Amazon_Ion_Trap"))
            {
                // Non-Thermo Instrument, low res MS/MS
                instrumentIDNew = "0";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (clsGlobal.IsMatch(instrumentGroup, "IMS"))
            {
                // Non-Thermo Instrument, high res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            if (clsGlobal.IsMatch(instrumentGroup, "Sciex_TripleTOF"))
            {
                // Non-Thermo Instrument, high res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }

            instrumentIDNew = string.Empty;
            autoSwitchReason = string.Empty;
            return false;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="instrumentIDCurrent">Current instrument ID; may get updated by this method</param>
        /// <param name="instrumentIDNew"></param>
        /// <param name="autoSwitchReason"></param>
        /// <remarks></remarks>
        private void AutoUpdateInstrumentIDIfChanged(ref string instrumentIDCurrent, string instrumentIDNew, string autoSwitchReason)
        {
            if (!string.IsNullOrEmpty(instrumentIDNew) && instrumentIDNew != instrumentIDCurrent)
            {
                if (m_DebugLevel >= 1)
                {
                    string instrumentDescription;

                    switch (instrumentIDNew)
                    {
                        case "0":
                            instrumentDescription = "Low-res MSn";
                            break;
                        case "1":
                            instrumentDescription = "High-res MSn";
                            break;
                        case "2":
                            instrumentDescription = "TOF";
                            break;
                        case "3":
                            instrumentDescription = "Q-Exactive";
                            break;
                        default:
                            instrumentDescription = "??";
                            break;
                    }

                    OnStatusEvent("Auto-updating instrument ID " +
                                  "from " + instrumentIDCurrent + " to " + instrumentIDNew +
                                  " (" + instrumentDescription + ") " + autoSwitchReason);
                }

                instrumentIDCurrent = instrumentIDNew;
            }
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
                var tsvFilePath = Path.Combine(m_WorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var fiMzidFile = new FileInfo(Path.Combine(m_WorkDir, mzidFileName));
                if (!fiMzidFile.Exists)
                {
                    OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV; Mzid file not found: " + fiMzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
                if (!MSGFPlusResultsFileHasClosingTag(fiMzidFile))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Set up and execute a program runner to run MzidToTsvConverter.exe
                var cmdStr = GetMZIDtoTSVCommandLine(mzidFileName, tsvFileName, m_WorkDir, mzidToTsvConverterProgLoc);

                var objCreateTSV = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE)
                };
                RegisterEvents(objCreateTSV);

                if (clsGlobal.LinuxOS)
                {
                    // Need to run MzidToTsvConverter.exe using mono
                    var updated = objCreateTSV.UpdateToUseMono(m_mgrParams, ref mzidToTsvConverterProgLoc, ref cmdStr);
                    if (!updated)
                    {
                        OnWarningEvent("Unable to run MzidToTsvConverter.exe with mono");
                        return string.Empty;
                    }

                }

                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent(mzidToTsvConverterProgLoc + " " + cmdStr);
                }

                // This process is typically quite fast, so we do not track CPU usage
                var success = objCreateTSV.RunProgram(mzidToTsvConverterProgLoc, cmdStr, "MzIDToTsv", true);

                if (!success)
                {
                    OnErrorEvent("MzidToTsvConverter.exe returned an error code converting the .mzid file To a .tsv file: " + objCreateTSV.ExitCode);
                    return string.Empty;
                }

                // The conversion succeeded

                // Update MSGFPlus_ConsoleOutput with the contents of MzIDToTsv_ConsoleOutput.txt

                var targetFile = new FileInfo(Path.Combine(m_WorkDir, MSGFPLUS_CONSOLE_OUTPUT_FILE));

                if (!targetFile.Exists && m_jobParams.GetJobParameter("NumberOfClonedSteps", 0) > 1)
                {
                    // We're likely rerunning data extraction on an old job; this step is not necessary
                    // Skip the call to AppendConsoleOutputHeader to avoid repeated warnings
                }
                else
                {
                    // Append the first line from the console output file to the end of the MSGFPlus console output file
                    AppendConsoleOutputHeader(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);
                }


                try
                {
                    // The MzIDToTsv console output file doesn't contain any log messsages we need to save, so delete it
                    File.Delete(objCreateTSV.ConsoleOutputFilePath);
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
        /// <param name="msgfDbProgLoc">Folder with MSGFPlusjar</param>
        /// <param name="datasetName">Dataset name (output file will be named DatasetName_msgfdb.tsv)</param>
        /// <param name="mzidFileName">.mzid file name (assumed to be in the work directory)</param>
        /// <returns>TSV file path, or an empty string if an error</returns>
        /// <remarks></remarks>
        [Obsolete("Use the version of ConvertMzidToTsv that simply accepts a dataset name and .mzid file path and uses MzidToTsvConverter.exe")]
        public string ConvertMZIDToTSV(string javaProgLoc, string msgfDbProgLoc, string datasetName, string mzidFileName)
        {
            string tsvFilePath;

            try
            {
                // In November 2016, this file was renamed from Dataset_msgfdb.tsv to Dataset_msgfplus.tsv
                var tsvFileName = datasetName + MSGFPLUS_TSV_SUFFIX;
                tsvFilePath = Path.Combine(m_WorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var fiMzidFile = new FileInfo(Path.Combine(m_WorkDir, mzidFileName));
                if (!fiMzidFile.Exists)
                {
                    OnErrorEvent("Error in MSGFPlusUtils->ConvertMZIDToTSV; Mzid file not found: " + fiMzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
                if (!MSGFPlusResultsFileHasClosingTag(fiMzidFile))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Dynamically set the amount of required memory based on the size of the .mzid file
                var fileSizeMB = fiMzidFile.Length / 1024.0 / 1024.0;
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
                var cmdStr = GetMZIDtoTSVCommandLine(mzidFileName, tsvFileName, m_WorkDir, msgfDbProgLoc, javaMemorySizeMB);

                // Make sure the machine has enough free memory to run MSGFPlus
                const bool LOG_FREE_MEMORY_ON_SUCCESS = false;

                if (!clsAnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "MzIDToTsv", LOG_FREE_MEMORY_ON_SUCCESS))
                {
                    OnErrorEvent("Not enough free memory to run the MzIDToTsv module in MSGFPlus");
                    return string.Empty;
                }

                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent(javaProgLoc + " " + cmdStr);
                }

                var objCreateTSV = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE)
                };
                RegisterEvents(objCreateTSV);

                // This process is typically quite fast, so we do not track CPU usage
                var success = objCreateTSV.RunProgram(javaProgLoc, cmdStr, "MzIDToTsv", true);

                if (!success)
                {
                    OnErrorEvent("MSGFPlus returned an error code converting the .mzid file to a .tsv file: " + objCreateTSV.ExitCode);
                    return string.Empty;
                }

                // The conversion succeeded

                // Append the first line from the console output file to the end of the MSGFPlus console output file
                AppendConsoleOutputHeader(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);

                try
                {
                    // Delete the console output file
                    File.Delete(objCreateTSV.ConsoleOutputFilePath);
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
        /// <param name="mzidFileName"></param>
        /// <param name="tsvFileName"></param>
        /// <param name="workingDirectory"></param>
        /// <param name="mzidToTsvConverterProgLoc"></param>
        /// <returns></returns>
        public static string GetMZIDtoTSVCommandLine(string mzidFileName, string tsvFileName, string workingDirectory, string mzidToTsvConverterProgLoc)
        {
            var cmdStr =
                " -mzid:" + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName)) +
                " -tsv:" + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName)) +
                " -unroll" +
                " -showDecoy";

            return cmdStr;
        }

        /// <summary>
        /// Obtain the MzidToTsv command line arguments
        /// </summary>
        /// <param name="mzidFileName"></param>
        /// <param name="tsvFileName"></param>
        /// <param name="workingDirectory"></param>
        /// <param name="msgfDbProgLoc"></param>
        /// <param name="javaMemorySizeMB"></param>
        /// <returns></returns>
        [Obsolete("Use GetMZIDtoTSVCommandLine for MzidToTsvConverter.exe")]
        public static string GetMZIDtoTSVCommandLine(string mzidFileName, string tsvFileName, string workingDirectory, string msgfDbProgLoc, int javaMemorySizeMB)
        {
            // We're using "-XX:+UseConcMarkSweepGC" as directed at http://stackoverflow.com/questions/5839359/java-lang-outofmemoryerror-gc-overhead-limit-exceeded
            // due to seeing error "java.lang.OutOfMemoryError: GC overhead limit exceeded" with a 353 MB .mzid file

            var cmdStr = " -Xmx" + javaMemorySizeMB + "M -XX:+UseConcMarkSweepGC -cp " + msgfDbProgLoc;
            cmdStr += " edu.ucsd.msjava.ui.MzIDToTsv";

            cmdStr += " -i " + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName));
            cmdStr += " -o " + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName));
            cmdStr += " -showQValue 1";
            cmdStr += " -showDecoy 1";
            cmdStr += " -unroll 1";

            return cmdStr;
        }

        /// <summary>
        /// Create the peptide to protein map file
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <param name="ePeptideInputFileFormat"></param>
        /// <returns></returns>
        public CloseOutType CreatePeptideToProteinMapping(string resultsFileName,
            clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants ePeptideInputFileFormat)
        {
            const bool resultsIncludeAutoAddedDecoyPeptides = false;
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            return CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder, ePeptideInputFileFormat);
        }

        /// <summary>
        /// Create the peptide to protein mapping file
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <param name="resultsIncludeAutoAddedDecoyPeptides"></param>
        /// <returns></returns>
        public CloseOutType CreatePeptideToProteinMapping(string resultsFileName, bool resultsIncludeAutoAddedDecoyPeptides)
        {
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            return CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);
        }

        /// <summary>
        /// Create the peptide to protein mapping file
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <param name="resultsIncludeAutoAddedDecoyPeptides"></param>
        /// <param name="localOrgDbFolder"></param>
        /// <returns></returns>
        public CloseOutType CreatePeptideToProteinMapping(string resultsFileName, bool resultsIncludeAutoAddedDecoyPeptides, string localOrgDbFolder)
        {
            return CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder,
                clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.MSGFDBResultsFile);
        }

        /// <summary>
        /// Create file Dataset_msgfplus_PepToProtMap.txt
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <param name="resultsIncludeAutoAddedDecoyPeptides"></param>
        /// <param name="localOrgDbFolder"></param>
        /// <param name="ePeptideInputFileFormat"></param>
        /// <returns></returns>
        public CloseOutType CreatePeptideToProteinMapping(string resultsFileName, bool resultsIncludeAutoAddedDecoyPeptides,
            string localOrgDbFolder, clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants ePeptideInputFileFormat)
        {
            // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
            var dbFilename = m_jobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME);

            string msg;

            var ignorePeptideToProteinMapperErrors = false;

            var inputFilePath = Path.Combine(m_WorkDir, resultsFileName);
            var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

            try
            {
                // Validate that the input file has at least one entry; if not, no point in continuing
                int linesRead;

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

                using (var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    linesRead = 0;
                    while (!reader.EndOfStream && linesRead < 10)
                    {
                        var dataLine = reader.ReadLine();
                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            linesRead += 1;
                        }
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
                // Read the original fasta file to create a decoy fasta file
                var decoyFastaFilePath = GenerateDecoyFastaFile(fastaFilePath, m_WorkDir);
                fastaFileToSearch = decoyFastaFilePath;

                if (string.IsNullOrEmpty(decoyFastaFilePath))
                {
                    // Problem creating the decoy fasta file
                    if (string.IsNullOrEmpty(mErrorMessage))
                    {
                        mErrorMessage = "Error creating a decoy version of the fasta file";
                    }
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(Path.GetFileName(decoyFastaFilePath));
            }

            try
            {
                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent("Creating peptide to protein map file");
                }

                ignorePeptideToProteinMapperErrors = m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", false);

                mPeptideToProteinMapper = new clsPeptideToProteinMapEngine
                {
                    DeleteTempFiles = true,
                    IgnoreILDifferences = false,
                    InspectParameterFilePath = string.Empty,
                    MatchPeptidePrefixAndSuffixToProtein = false,
                    OutputProteinSequence = false,
                    PeptideInputFileFormat = ePeptideInputFileFormat,
                    PeptideFileSkipFirstLine = false,
                    ProteinDataRemoveSymbolCharacters = true,
                    ProteinInputFilePath = fastaFileToSearch,
                    SaveProteinToPeptideMappingFile = true,
                    SearchAllProteinsForPeptideSequence = true,
                    SearchAllProteinsSkipCoverageComputationSteps = true,
                    ShowMessages = false
                };

                RegisterEvents(mPeptideToProteinMapper);
                mPeptideToProteinMapper.ProgressUpdate -= base.OnProgressUpdate;
                mPeptideToProteinMapper.ProgressUpdate += PeptideToProteinMapper_ProgressChanged;

                if (m_DebugLevel > 2)
                {
                    mPeptideToProteinMapper.LogMessagesToFile = true;
                    mPeptideToProteinMapper.LogFolderPath = m_WorkDir;
                }
                else
                {
                    mPeptideToProteinMapper.LogMessagesToFile = false;
                }

                // Note that clsPeptideToProteinMapEngine utilizes System.Data.SQLite.dll
                var success = mPeptideToProteinMapper.ProcessFile(inputFilePath, m_WorkDir, string.Empty, true);

                mPeptideToProteinMapper.CloseLogFileNow();

                var pepToProtMapFileName = Path.GetFileNameWithoutExtension(inputFilePath) +
                    clsPeptideToProteinMapEngine.FILENAME_SUFFIX_PEP_TO_PROTEIN_MAPPING;

                var pepToProtMapFilePath = Path.Combine(m_WorkDir, pepToProtMapFileName);

                if (success)
                {
                    if (!File.Exists(pepToProtMapFilePath))
                    {
                        OnErrorEvent("Peptide to protein mapping file was not created");
                        success = false;
                    }
                    else
                    {
                        if (m_DebugLevel >= 2)
                        {
                            OnStatusEvent("Peptide to protein mapping complete");
                        }

                        success = ValidatePeptideToProteinMapResults(pepToProtMapFilePath, ignorePeptideToProteinMapperErrors);
                    }
                }
                else
                {
                    if (mPeptideToProteinMapper.GetErrorMessage().Length == 0 && mPeptideToProteinMapper.StatusMessage.ToLower().Contains("error"))
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
                        OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");

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
                OnErrorEvent("Exception in CreatePeptideToProteinMapping", ex);

                if (ignorePeptideToProteinMapperErrors)
                {
                    OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                return CloseOutType.CLOSEOUT_FAILED;

            }

        }

        /// <summary>
        /// Create a trimmed version of fastaFilePath, with max size maxFastaFileSizeMB
        /// </summary>
        /// <param name="fastaFilePath">Fasta file to trim</param>
        /// <param name="maxFastaFileSizeMB">Maximum file size</param>
        /// <returns>Full path to the trimmed fasta; empty string if a problem</returns>
        /// <remarks></remarks>
        private string CreateTrimmedFasta(string fastaFilePath, int maxFastaFileSizeMB)
        {
            try
            {
                var fiFastaFile = new FileInfo(fastaFilePath);

                if (fiFastaFile.DirectoryName == null)
                {
                    mErrorMessage = "Unable to determine the parent directory of " + fastaFilePath;
                    OnErrorEvent(mErrorMessage);
                    return string.Empty;
                }

                var fiTrimmedFasta = new FileInfo(Path.Combine(
                    fiFastaFile.DirectoryName,
                    Path.GetFileNameWithoutExtension(fiFastaFile.Name) + "_Trim" + maxFastaFileSizeMB + "MB.fasta"));

                if (fiTrimmedFasta.Exists)
                {
                    // Verify that the file matches the .hashcheck value
                    var hashcheckFilePath = fiTrimmedFasta.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

                    if (clsGlobal.ValidateFileVsHashcheck(fiTrimmedFasta.FullName, hashcheckFilePath, out _))
                    {
                        // The trimmed fasta file is valid
                        OnStatusEvent("Using existing trimmed fasta: " + fiTrimmedFasta.Name);
                        return fiTrimmedFasta.FullName;
                    }
                }

                OnStatusEvent("Creating trimmed fasta: " + fiTrimmedFasta.Name);

                // Construct the list of required contaminant proteins
                var contaminantUtility = new clsFastaContaminantUtility();

                var dctRequiredContaminants = new Dictionary<string, bool>();
                foreach (var proteinName in contaminantUtility.ProteinNames)
                {
                    dctRequiredContaminants.Add(proteinName, false);
                }

                long maxSizeBytes = maxFastaFileSizeMB * 1024 * 1024;
                long bytesWritten = 0;
                var proteinCount = 0;

                using (var sourceFastaReader = new StreamReader(new FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var trimmedFastaWriter = new StreamWriter(new FileStream(fiTrimmedFasta.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
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

                            if (dctRequiredContaminants.ContainsKey(proteinName))
                            {
                                dctRequiredContaminants[proteinName] = true;
                            }

                            proteinCount += 1;
                        }

                        trimmedFastaWriter.WriteLine(dataLine);
                        bytesWritten += dataLine.Length + 2;
                    }

                    // Add any missing contaminants
                    foreach (var protein in dctRequiredContaminants)
                    {
                        if (!protein.Value)
                        {
                            contaminantUtility.WriteProteinToFasta(trimmedFastaWriter, protein.Key);
                        }
                    }
                }

                OnStatusEvent("Trimmed fasta created using " + proteinCount + " proteins; creating the hashcheck file");

                clsGlobal.CreateHashcheckFile(fiTrimmedFasta.FullName, true);
                var trimmedFastaFilePath = fiTrimmedFasta.FullName;
                return trimmedFastaFilePath;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception trimming fasta file to " + maxFastaFileSizeMB + " MB";
                OnErrorEvent(mErrorMessage, ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Delete a file in the working directory
        /// </summary>
        /// <param name="filename"></param>
        public void DeleteFileInWorkDir(string filename)
        {
            try
            {
                var targetFile = new FileInfo(Path.Combine(m_WorkDir, filename));

                if (targetFile.Exists)
                {
                    targetFile.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// Read the original fasta file to create a decoy fasta file

        /// <summary>
        /// Creates a decoy version of the fasta file specified by inputFilePath
        /// This new file will include the original proteins plus reversed versions of the original proteins
        /// Protein names will be prepended with REV_ or XXX_
        /// </summary>
        /// <param name="inputFilePath">Fasta file to process</param>
        /// <param name="outputDirectoryPath">Output folder to create decoy file in</param>
        /// <returns>Full path to the decoy fasta file</returns>
        /// <remarks></remarks>
        private string GenerateDecoyFastaFile(string inputFilePath, string outputDirectoryPath)
        {
            const char PROTEIN_LINE_START_CHAR = '>';
            const char PROTEIN_LINE_ACCESSION_END_CHAR = ' ';

            try
            {
                var sourceFile = new FileInfo(inputFilePath);
                if (!sourceFile.Exists)
                {
                    mErrorMessage = "Fasta file not found: " + sourceFile.FullName;
                    return string.Empty;
                }

                var decoyFastaFilePath = Path.Combine(outputDirectoryPath, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_decoy.fasta");

                if (m_DebugLevel >= 2)
                {
                    OnStatusEvent("Creating decoy fasta file at " + decoyFastaFilePath);
                }

                var fastaFileReader = new ProteinFileReader.FastaFileReader
                {
                    ProteinLineStartChar = PROTEIN_LINE_START_CHAR,
                    ProteinLineAccessionEndChar = PROTEIN_LINE_ACCESSION_END_CHAR
                };

                if (!fastaFileReader.OpenFile(inputFilePath))
                {
                    OnErrorEvent("Error reading fasta file with ProteinFileReader to create decoy file");
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
                            writer.WriteLine(PROTEIN_LINE_START_CHAR + fastaFileReader.ProteinName + " " +
                                                          fastaFileReader.ProteinDescription);
                            WriteProteinSequence(writer, fastaFileReader.ProteinSequence);

                            // Write the decoy protein
                            writer.WriteLine(PROTEIN_LINE_START_CHAR + NAME_PREFIX + fastaFileReader.ProteinName + " " +
                                                          fastaFileReader.ProteinDescription);
                            WriteProteinSequence(writer, ReverseString(fastaFileReader.ProteinSequence));
                        }
                    } while (inputProteinFound);
                }

                fastaFileReader.CloseFile();

                return decoyFastaFilePath;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception creating decoy fasta file", ex);
                return string.Empty;
            }

        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns>The number of cores on this computer</returns>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        public int GetCoreCount()
        {
            return clsGlobal.GetCoreCount();
        }

        private Dictionary<string, string> GetMSFGDBParameterNames()
        {
            // Keys are the parameter name in the MS-GF+ parameter file
            // Values are the command line switch name
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"PMTolerance", "t"},
                {MSGFPLUS_OPTION_TDA, "tda"},
                {MSGFPLUS_OPTION_SHOWDECOY, "showDecoy"},
                // This setting is nearly always set to 0 since we create a _ScanType.txt file that specifies the type of each scan
                // (thus, the value in the parameter file is ignored); the exception, when it is UVPD (mode 4)
                {MSGFPLUS_OPTION_FRAGMENTATION_METHOD, "m"},
                // This setting is auto-updated based on the instrument class for this dataset,
                // plus also the scan types listed In the _ScanType.txt file
                // (thus, the value in the parameter file Is typically ignored)
                {MSGFPLUS_OPTION_INSTRUMENT_ID, "inst"},
                {"EnzymeID", "e"},
                // Used by MS-GFDB
                {"C13", "c13"},
                // Used by MS-GF+
                {"IsotopeError", "ti"},
                // Used by MS-GFDB
                {"NNET", "nnet"},
                // Used by MS-GF+
                {"NTT", "ntt"},
                {"minLength", "minLength"},
                {"maxLength", "maxLength"},
                // Only used if the spectrum file doesn't have charge information
                {"minCharge", "minCharge"},
                // Only used if the spectrum file doesn't have charge information
                {"maxCharge", "maxCharge"},
                {"NumMatchesPerSpec", "n"},
                // Auto-added by this code if not defined
                {"minNumPeaks", "minNumPeaks"},
                {"Protocol", "protocol"},
                {"ChargeCarrierMass", "ccm"}
            };

            // The following are special cases;
            // Do not add them to dctParamNames
            //   uniformAAProb
            //   NumThreads
            //   NumMods
            //   StaticMod
            //   DynamicMod
            //   CustomAA

            return dctParamNames;
        }

        private string GetSearchEngineName()
        {
            return "MS-GF+";
        }

        private string GetSettingFromMSGFPlusParamFile(string parameterFilePath, string settingToFind)
        {
            return GetSettingFromMSGFPlusParamFile(parameterFilePath, settingToFind, string.Empty);
        }

        private string GetSettingFromMSGFPlusParamFile(string parameterFilePath, string settingToFind, string valueIfNotFound)
        {
            if (!File.Exists(parameterFilePath))
            {
                OnErrorEvent("Parameter file not found: " + parameterFilePath);
                return valueIfNotFound;
            }

            try
            {
                using (var srParamFile = new StreamReader(new FileStream(parameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var dataLine = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(dataLine);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key) && clsGlobal.IsMatch(kvSetting.Key, settingToFind))
                        {
                            return kvSetting.Value;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception reading MSGFDB parameter file";
                OnErrorEvent(mErrorMessage, ex);
            }

            return valueIfNotFound;
        }

        /// <summary>
        /// Initialize the FASTA file
        /// </summary>
        /// <param name="javaProgLoc"></param>
        /// <param name="msgfPlusProgLoc"></param>
        /// <param name="fastaFileSizeKB"></param>
        /// <param name="fastaFileIsDecoy"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="msgfPlusParameterFilePath"></param>
        /// <returns></returns>
        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfPlusProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string msgfPlusParameterFilePath)
        {
            return InitializeFastaFile(javaProgLoc, msgfPlusProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath,
                                       msgfPlusParameterFilePath, 0);
        }

        /// <summary>
        /// Initialize the FASTA file
        /// </summary>
        /// <param name="javaProgLoc"></param>
        /// <param name="msgfPlusProgLoc"></param>
        /// <param name="fastaFileSizeKB"></param>
        /// <param name="fastaFileIsDecoy"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="msgfPlusParameterFilePath"></param>
        /// <param name="maxFastaFileSizeMB"></param>
        /// <returns></returns>
        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfPlusProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string msgfPlusParameterFilePath, int maxFastaFileSizeMB)
        {
            var oRand = new Random();

            var mgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager");

            var objIndexedDBCreator = new clsCreateMSGFDBSuffixArrayFiles(mgrName);
            RegisterEvents(objIndexedDBCreator);

            // Define the path to the fasta file
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            fastaFileSizeKB = 0;
            fastaFileIsDecoy = false;

            var fiFastaFile = new FileInfo(fastaFilePath);

            if (!fiFastaFile.Exists)
            {
                // Fasta file not found
                OnErrorEvent("Fasta file not found: " + fiFastaFile.FullName);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            fastaFileSizeKB = (float)(fiFastaFile.Length / 1024.0);

            var proteinOptions = m_jobParams.GetParam("ProteinOptions");

            if (string.IsNullOrEmpty(proteinOptions) || proteinOptions == "na")
            {
                // Determine the fraction of the proteins that start with Reversed_ or XXX_ or XXX.
                var decoyPrefixes = clsAnalysisResources.GetDefaultDecoyPrefixes();
                foreach (var decoyPrefix in decoyPrefixes)
                {
                    var fractionDecoy = clsAnalysisResources.GetDecoyFastaCompositionStats(fiFastaFile, decoyPrefix, out _);
                    if (fractionDecoy >= 0.25)
                    {
                        fastaFileIsDecoy = true;
                        break;
                    }
                }
            }
            else
            {
                if (proteinOptions.ToLower().Contains("seq_direction=decoy"))
                {
                    fastaFileIsDecoy = true;
                }
            }

            if (!string.IsNullOrEmpty(msgfPlusParameterFilePath))
            {
                var tdaSetting = GetSettingFromMSGFPlusParamFile(msgfPlusParameterFilePath, "TDA");

                if (!int.TryParse(tdaSetting, out var tdaValue))
                {
                    OnErrorEvent("TDA value is not numeric: " + tdaSetting);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (tdaValue == 0)
                {
                    if (!fastaFileIsDecoy && fastaFileSizeKB / 1024.0 / 1024.0 > 1)
                    {
                        // Large Fasta file (over 1 GB in size)
                        // TDA is 0, so we're performing a forward-only search
                        // Auto-change fastaFileIsDecoy to True to prevent the reverse indices from being created

                        fastaFileIsDecoy = true;
                        if (m_DebugLevel >= 1)
                        {
                            OnStatusEvent("Processing large FASTA file with forward-only search; auto switching to -tda 0");
                        }
                    }
                    else if (msgfPlusParameterFilePath.EndsWith("_NoDecoy.txt", StringComparison.OrdinalIgnoreCase))
                    {
                        // Parameter file ends in _NoDecoy.txt and TDA = 0, thus we're performing a forward-only search
                        // Auto-change fastaFileIsDecoy to True to prevent the reverse indices from being created

                        fastaFileIsDecoy = true;
                        if (m_DebugLevel >= 1)
                        {
                            OnStatusEvent("Using NoDecoy parameter file with TDA=0; auto switching to -tda 0");
                        }
                    }
                }
            }

            if (maxFastaFileSizeMB > 0 && fastaFileSizeKB / 1024.0 > maxFastaFileSizeMB)
            {
                // Create a trimmed version of the fasta file
                OnStatusEvent("Fasta file is over " + maxFastaFileSizeMB + " MB; creating a trimmed version of the fasta file");

                var fastaFilePathTrimmed = string.Empty;

                // Allow for up to 3 attempts since multiple processes might potentially try to do this at the same time
                var trimIteration = 0;

                while (trimIteration <= 2)
                {
                    trimIteration += 1;
                    fastaFilePathTrimmed = CreateTrimmedFasta(fastaFilePath, maxFastaFileSizeMB);

                    if (!string.IsNullOrEmpty(fastaFilePathTrimmed))
                    {
                        break;
                    }

                    if (trimIteration <= 2)
                    {
                        var sleepTimeSec = oRand.Next(10, 19);

                        OnStatusEvent("Fasta file trimming failed; waiting " + sleepTimeSec + " seconds then trying again");
                        Thread.Sleep(sleepTimeSec * 1000);
                    }
                }

                if (string.IsNullOrEmpty(fastaFilePathTrimmed))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Update fastaFilePath to use the path to the trimmed version
                fastaFilePath = fastaFilePathTrimmed;

                fiFastaFile.Refresh();
                fastaFileSizeKB = (float)(fiFastaFile.Length / 1024.0);
            }

            if (m_DebugLevel >= 3 || (m_DebugLevel >= 1 && fastaFileSizeKB > 500))
            {
                OnStatusEvent("Indexing Fasta file to create Suffix Array files");
            }

            // Look for the suffix array files that should exist for the fasta file
            // Either copy them from Gigasax (or Proto-7) or re-create them
            //
            var indexIteration = 0;
            var msgfPlusIndexFilesFolderPath = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPath", @"\\gigasax\MSGFPlus_Index_Files");
            var msgfPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", @"\\proto-7\MSGFPlus_Index_Files");

            while (indexIteration <= 2)
            {
                indexIteration += 1;

                var result = objIndexedDBCreator.CreateSuffixArrayFiles(m_WorkDir, m_DebugLevel, javaProgLoc, msgfPlusProgLoc, fastaFilePath,
                    fastaFileIsDecoy, msgfPlusIndexFilesFolderPath, msgfPlusIndexFilesFolderPathLegacyDB);

                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    break;
                }

                if (result == CloseOutType.CLOSEOUT_FAILED || (result != CloseOutType.CLOSEOUT_FAILED && indexIteration > 2))
                {
                    if (!string.IsNullOrEmpty(objIndexedDBCreator.ErrorMessage))
                    {
                        OnErrorEvent(objIndexedDBCreator.ErrorMessage);
                    }
                    else
                    {
                        OnErrorEvent("Error creating Suffix Array files");
                    }
                    return result;
                }

                var sleepTimeSec = oRand.Next(10, 19);

                OnStatusEvent("Fasta file indexing failed; waiting " + sleepTimeSec + " seconds then trying again");
                Thread.Sleep(sleepTimeSec * 1000);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Reads the contents of a _ScanType.txt file, returning the scan info using three generic dictionary objects
        /// </summary>
        /// <param name="scanTypeFilePath"></param>
        /// <param name="lstLowResMSn">Low Res MSn spectra</param>
        /// <param name="lstHighResMSn">High Res MSn spectra (but not HCD)</param>
        /// <param name="lstHCDMSn">HCD Spectra</param>
        /// <param name="lstOther">Spectra that are not MSn</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool LoadScanTypeFile(string scanTypeFilePath, out Dictionary<int, string> lstLowResMSn, out Dictionary<int, string> lstHighResMSn,
            out Dictionary<int, string> lstHCDMSn, out Dictionary<int, string> lstOther)
        {
            var scanNumberColIndex = -1;
            var scanTypeNameColIndex = -1;

            lstLowResMSn = new Dictionary<int, string>();
            lstHighResMSn = new Dictionary<int, string>();
            lstHCDMSn = new Dictionary<int, string>();
            lstOther = new Dictionary<int, string>();

            try
            {
                if (!File.Exists(scanTypeFilePath))
                {
                    mErrorMessage = "ScanType file not found: " + scanTypeFilePath;
                    return false;
                }

                using (var srScanTypeFile = new StreamReader(new FileStream(scanTypeFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srScanTypeFile.EndOfStream)
                    {
                        var dataLine = srScanTypeFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var lstColumns = dataLine.Split('\t').ToList();

                        if (scanNumberColIndex < 0)
                        {
                            // Parse the header line to define the mapping
                            // Expected headers are ScanNumber   ScanTypeName   ScanType
                            scanNumberColIndex = lstColumns.IndexOf("ScanNumber");
                            scanTypeNameColIndex = lstColumns.IndexOf("ScanTypeName");
                        }
                        else if (scanNumberColIndex >= 0)
                        {

                            if (!int.TryParse(lstColumns[scanNumberColIndex], out var scanNumber))
                                continue;

                            if (scanTypeNameColIndex < 0)
                                continue;

                            var scanType = lstColumns[scanTypeNameColIndex];
                            var scanTypeLCase = scanType.ToLower();

                            if (scanTypeLCase.Contains("hcd"))
                            {
                                lstHCDMSn.Add(scanNumber, scanType);
                            }
                            else if (scanTypeLCase.Contains("hmsn"))
                            {
                                lstHighResMSn.Add(scanNumber, scanType);
                            }
                            else if (scanTypeLCase.Contains("msn"))
                            {
                                // Not HCD and doesn't contain HMSn; assume low-res
                                lstLowResMSn.Add(scanNumber, scanType);
                            }
                            else if (scanTypeLCase.Contains("cid") || scanTypeLCase.Contains("etd"))
                            {
                                // The ScanTypeName likely came from the "Collision Mode" column of a MASIC ScanStatsEx file; we don't know if it is high res MSn or low res MSn
                                // This will be the case for MASIC results from prior to February 1, 2010, since those results did not have the ScanTypeName column in the _ScanStats.txt file
                                // We'll assume low res
                                lstLowResMSn.Add(scanNumber, scanType);
                            }
                            else
                            {
                                // Does not contain MSn or HCD
                                // Likely SRM or MS1
                                lstOther.Add(scanNumber, scanType);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in LoadScanTypeFile";
                OnErrorEvent(mErrorMessage, ex);
                return false;
            }

            return true;
        }

        private bool MisleadingModDef(string definitionDataClean, string definitionType, string expectedTag, string invalidTag)
        {
            if (definitionDataClean.Contains("," + invalidTag + ","))
            {
                // One of the following is true:
                //  Static (fixed) mod is listed as dynamic or custom
                //  Dynamic (optional) mod is listed as static or custom
                //  Custom amino acid def is listed as a dynamic or static

                var verboseTag = "??";
                switch (invalidTag)
                {
                    case "opt":
                        verboseTag = "DynamicMod";
                        break;
                    case "fix":
                        verboseTag = "StaticMod";
                        break;
                    case "custom":
                        verboseTag = "CustomAA";
                        break;
                }

                // Abort the analysis since the parameter file is misleading and needs to be fixed
                // Example messages:
                //  Dynamic mod definition contains ,fix, -- update the param file to have ,opt, or change to StaticMod="
                //  Static mod definition contains ,opt, -- update the param file to have ,fix, or change to DynamicMod="
                mErrorMessage = definitionType + " definition contains ," + invalidTag + ", -- update the param file to have ," + expectedTag +
                                ", or change to " + verboseTag + "=";
                OnErrorEvent(mErrorMessage);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Verify that the MSGF+ .mzid file ends with XML tag MzIdentML
        /// </summary>
        /// <param name="fiMzidFile"></param>
        /// <returns></returns>
        public static bool MSGFPlusResultsFileHasClosingTag(FileSystemInfo fiMzidFile)
        {

            // Check whether the mzid file ends with XML tag </MzIdentML>
            var lastLine = string.Empty;
            using (var reader = new StreamReader(new FileStream(fiMzidFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(dataLine))
                    {
                        lastLine = dataLine;
                    }
                }
            }

            var validClosingTag = lastLine.Trim().EndsWith("</MzIdentML>", StringComparison.OrdinalIgnoreCase);
            return validClosingTag;
        }

        /// <summary>
        /// Parse the MSGFPlus console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        /// <returns>Percent Complete (value between 0 and 100)</returns>
        /// <remarks>MSGFPlus version is available via the MSGFPlusVersion property</remarks>
        public float ParseMSGFPlusConsoleOutputFile()
        {
            return ParseMSGFPlusConsoleOutputFile(m_WorkDir);
        }

        // Example Console output (verbose mode):
        //
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
        // 	PrecursorMassTolerance: 20.0ppm
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

        // ReSharper disable once UseImplicitlyTypedVariableEvident
        private readonly Regex reExtractThreadCount = new Regex(@"Using (?<ThreadCount>\d+) threads", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reExtractTaskCount = new Regex(@"Splitting work into +(?<TaskCount>\d+) +tasks", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reSpectraSearched = new Regex(@"Spectrum.+\(total: *(?<SpectrumCount>\d+)\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex reTaskComplete = new Regex(@"pool-\d+-thread-\d+: Task +(?<TaskNumber>\d+) +completed", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private readonly Regex rePercentComplete = new Regex(@"Search progress: (?<TasksComplete>\d+) / \d+ tasks?, (?<PercentComplete>[0-9.]+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSGFPlus console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        /// <returns>Percent Complete (value between 0 and 96)</returns>
        /// <remarks>MSGFPlus version is available via the MSGFPlusVersion property</remarks>
        public float ParseMSGFPlusConsoleOutputFile(string workingDirectory)
        {
            var consoleOutputFilePath = "??";

            float sngEffectiveProgress = 0;
            float percentCompleteAllTasks = 0;
            var tasksCompleteViaSearchProgress = 0;

            try
            {
                consoleOutputFilePath = Path.Combine(workingDirectory, MSGFPLUS_CONSOLE_OUTPUT_FILE);
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        OnStatusEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return 0;
                }

                if (m_DebugLevel >= 4)
                {
                    OnStatusEvent("Parsing file " + consoleOutputFilePath);
                }

                // This is the total threads that MS-GF+ reports that it is using
                short totalThreadCount = 0;

                var totalTasks = 0;

                // List of completed task numbers
                var completedTasks = new SortedSet<int>();

                mConsoleOutputErrorMsg = string.Empty;

                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_STARTING;
                mContinuumSpectraSkipped = 0;
                mSpectraSearched = 0;

                using (var srInFile = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var dataLine = srInFile.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataLineLcase = dataLine.ToLower();

                        if (linesRead <= 3)
                        {
                            // Originally the first line was the MS-GF+ version
                            // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                            // The third line is the MS-GF+ version
                            if (string.IsNullOrWhiteSpace(mMSGFPlusVersion) && dataLine.StartsWith("MS-GF+ Release", StringComparison.OrdinalIgnoreCase))
                            {
                                if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSGFPlusVersion))
                                {
                                    OnStatusEvent("MS-GF+ version: " + dataLine);
                                }

                                mMSGFPlusVersion = string.Copy(dataLine);
                            }
                            else
                            {
                                if (dataLineLcase.Contains("error"))
                                {
                                    if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                    {
                                        mConsoleOutputErrorMsg = "Error running MS-GF+: ";
                                    }
                                    if (!mConsoleOutputErrorMsg.Contains(dataLine))
                                    {
                                        mConsoleOutputErrorMsg += "; " + dataLine;
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
                                mContinuumSpectraSkipped += 1;
                            }
                        }
                        else if (dataLine.StartsWith("Loading database files", StringComparison.OrdinalIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE;
                            }
                        }
                        else if (dataLine.StartsWith("Reading spectra", StringComparison.OrdinalIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_READING_SPECTRA)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_READING_SPECTRA;
                            }
                        }
                        else if (dataLine.StartsWith("Using", StringComparison.OrdinalIgnoreCase))
                        {
                            // Extract out the thread or task count
                            var oThreadMatch = reExtractThreadCount.Match(dataLine);

                            if (oThreadMatch.Success)
                            {
                                short.TryParse(oThreadMatch.Groups["ThreadCount"].Value, out totalThreadCount);

                                if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED)
                                {
                                    sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED;
                                }
                            }
                        }
                        else if (dataLine.StartsWith("Splitting", StringComparison.OrdinalIgnoreCase))
                        {
                            var oTaskMatch = reExtractTaskCount.Match(dataLine);

                            if (oTaskMatch.Success)
                            {
                                int.TryParse(oTaskMatch.Groups["TaskCount"].Value, out totalTasks);
                            }
                        }
                        else if (dataLine.StartsWith("Spectrum", StringComparison.OrdinalIgnoreCase))
                        {
                            // Extract out the number of spectra that MS-GF+ will actually search

                            var oMatch = reSpectraSearched.Match(dataLine);

                            if (oMatch.Success)
                            {
                                int.TryParse(oMatch.Groups["SpectrumCount"].Value, out mSpectraSearched);
                            }
                        }
                        else if (dataLine.StartsWith("Computing EFDRs", StringComparison.OrdinalIgnoreCase) ||
                                 dataLine.StartsWith("Computing q-values", StringComparison.OrdinalIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS;
                            }
                        }
                        else if (dataLine.StartsWith("MS-GF+ complete", StringComparison.OrdinalIgnoreCase) ||
                                 dataLine.StartsWith("MS-GF+ complete", StringComparison.OrdinalIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPLETE)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPLETE;
                            }
                        }
                        else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            if (dataLineLcase.Contains("error") && !dataLineLcase.Contains("isotopeerror:"))
                            {
                                mConsoleOutputErrorMsg += "; " + dataLine;
                            }
                        }

                        var reMatch = reTaskComplete.Match(dataLine);
                        if (reMatch.Success)
                        {
                            var taskNumber = int.Parse(reMatch.Groups["TaskNumber"].Value);

                            if (completedTasks.Contains(taskNumber))
                            {
                                OnWarningEvent("MS-GF+ reported that task " + taskNumber + " completed more than once");
                            }
                            else
                            {
                                completedTasks.Add(taskNumber);
                            }
                        }

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
                }

                mThreadCountActual = totalThreadCount;

                mTaskCountTotal = totalTasks;
                mTaskCountCompleted = completedTasks.Count;
                if (mTaskCountCompleted == 0 && tasksCompleteViaSearchProgress > 0)
                {
                    mTaskCountCompleted = tasksCompleteViaSearchProgress;
                }

                if (percentCompleteAllTasks > 0)
                {
                    sngEffectiveProgress = percentCompleteAllTasks * PROGRESS_PCT_MSGFPLUS_COMPLETE / 100f;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    OnWarningEvent("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }

            return sngEffectiveProgress;
        }

        /// <summary>
        /// Parses the static modifications, dynamic modifications, and custom amino acid information to create the MS-GF+ Mods file
        /// </summary>
        /// <param name="parameterFilePath">Full path to the MSGF parameter file; will create file MSGFPlus_Mods.txt in the same folder</param>
        /// <param name="sbOptions">String builder of command line arguments to pass to MS-GF+</param>
        /// <param name="numMods">Max Number of Modifications per peptide</param>
        /// <param name="lstStaticMods">List of Static Mods</param>
        /// <param name="lstDynamicMods">List of Dynamic Mods</param>
        /// <param name="lstCustomAminoAcids">List of Custom Amino Acids</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ParseMSGFDBModifications(string parameterFilePath, StringBuilder sbOptions, int numMods,
            IReadOnlyCollection<string> lstStaticMods, IReadOnlyCollection<string> lstDynamicMods, IReadOnlyCollection<string> lstCustomAminoAcids)
        {
            bool success;

            try
            {
                var fiParameterFile = new FileInfo(parameterFilePath);

                if (string.IsNullOrWhiteSpace(fiParameterFile.DirectoryName))
                {
                    OnErrorEvent("Unable to determine the parent directory of " + fiParameterFile.FullName);
                    return false;
                }

                var modFilePath = Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME);

                // Note that ParseMSGFDbValidateMod will set this to True if a dynamic or static mod is STY phosphorylation
                mPhosphorylationSearch = false;

                sbOptions.Append(" -mod " + MOD_FILE_NAME);

                using (var swModFile = new StreamWriter(new FileStream(modFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swModFile.WriteLine("# This file is used to specify modifications for MS-GF+");
                    swModFile.WriteLine();
                    swModFile.WriteLine("# Max Number of Modifications per peptide");
                    swModFile.WriteLine("# If this value is large, the search will be slow");
                    swModFile.WriteLine("NumMods=" + numMods);

                    if (lstCustomAminoAcids.Count > 0)
                    {
                        // Custom Amino Acid definitions need to be listed before static or dynamic modifications
                        swModFile.WriteLine();
                        swModFile.WriteLine("# Custom Amino Acids");

                        foreach (var customAADef in lstCustomAminoAcids)
                        {

                            if (ParseMSGFDbValidateMod(customAADef, out var customAADefClean))
                            {
                                if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "opt"))
                                    return false;
                                if (MisleadingModDef(customAADefClean, "Custom AA", "custom", "fix"))
                                    return false;
                                swModFile.WriteLine(customAADefClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }

                    swModFile.WriteLine();
                    swModFile.WriteLine("# Static mods");
                    if (lstStaticMods.Count == 0)
                    {
                        swModFile.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var staticMod in lstStaticMods)
                        {

                            if (ParseMSGFDbValidateMod(staticMod, out var modClean))
                            {
                                if (MisleadingModDef(modClean, "Static mod", "fix", "opt"))
                                    return false;
                                if (MisleadingModDef(modClean, "Static mod", "fix", "custom"))
                                    return false;
                                swModFile.WriteLine(modClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }

                    swModFile.WriteLine();
                    swModFile.WriteLine("# Dynamic mods");
                    if (lstDynamicMods.Count == 0)
                    {
                        swModFile.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var dynamicMod in lstDynamicMods)
                        {

                            if (ParseMSGFDbValidateMod(dynamicMod, out var modClean))
                            {
                                if (MisleadingModDef(modClean, "Dynamic mod", "opt", "fix"))
                                    return false;
                                if (MisleadingModDef(modClean, "Dynamic mod", "opt", "custom"))
                                    return false;
                                swModFile.WriteLine(modClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                }

                success = true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception creating MS-GF+ Mods file";
                OnErrorEvent(mErrorMessage, ex);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Read the MS-GF+ options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="assumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="scanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="parameterFilePath">Full path to the MS-GF+ parameter file to use</param>
        /// <param name="msgfPlusCmdLineOptions">Output: MS-GF+ command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSGFPlusParameterFile(float fastaFileSizeKB, bool fastaFileIsDecoy, string assumedScanType,
            string scanTypeFilePath, string instrumentGroup, string parameterFilePath,
            out string msgfPlusCmdLineOptions)
        {
            var overrideParams = new Dictionary<string, string>();

            return ParseMSGFPlusParameterFile(fastaFileSizeKB, fastaFileIsDecoy, assumedScanType, scanTypeFilePath, instrumentGroup,
                parameterFilePath, overrideParams, out msgfPlusCmdLineOptions);
        }

        /// <summary>
        /// Read the MS-GF+ options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="assumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="scanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="parameterFilePath">Full path to the MS-GF+ parameter file to use</param>
        /// <param name="overrideParams">Parameters to override settings in the MS-GF+ parameter file</param>
        /// <param name="msgfPlusCmdLineOptions">Output: MS-GF+ command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSGFPlusParameterFile(float fastaFileSizeKB, bool fastaFileIsDecoy, string assumedScanType,
            string scanTypeFilePath, string instrumentGroup, string parameterFilePath,
            Dictionary<string, string> overrideParams, out string msgfPlusCmdLineOptions)
        {
            var paramFileThreadCount = 0;

            var numMods = 0;
            var lstStaticMods = new List<string>();
            var lstDynamicMods = new List<string>();
            var lstCustomAminoAcids = new List<string>();

            var isTDA = false;

            msgfPlusCmdLineOptions = string.Empty;

            if (!File.Exists(parameterFilePath))
            {
                OnErrorEvent("Parameter file Not found:  " + parameterFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var searchEngineName = GetSearchEngineName();

            var sbOptions = new StringBuilder(500);

            // This will be set to True if the parameter file has TDA=1, meaning MSGF+ will auto-added decoy proteins to its list of candidate proteins
            // When TDA is 1, the FASTA must only contain normal (forward) protein sequences
            mResultsIncludeAutoAddedDecoyPeptides = false;

            try
            {
                // Initialize the Param Name dictionary
                var dctParamNames = GetMSFGDBParameterNames();

                using (var srParamFile = new StreamReader(new FileStream(parameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var dataLine = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(dataLine);

                        if (string.IsNullOrWhiteSpace(kvSetting.Key))
                            continue;

                        var valueText = kvSetting.Value;


                        // Check whether kvSetting.key is one of the standard keys defined in dctParamNames
                        int value;
                        if (dctParamNames.TryGetValue(kvSetting.Key, out var argumentSwitch))
                        {
                            if (clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_FRAGMENTATION_METHOD))
                            {
                                if (string.IsNullOrWhiteSpace(valueText) && !string.IsNullOrWhiteSpace(scanTypeFilePath))
                                {
                                    // No setting for FragmentationMethodID, and a ScanType file was created
                                    // Use FragmentationMethodID 0 (as written in the spectrum, or CID)
                                    valueText = "0";

                                    OnStatusEvent("Using Fragmentation method -m " + valueText + " because a ScanType file was created");
                                }
                                else if (!string.IsNullOrWhiteSpace(assumedScanType))
                                {
                                    // Override FragmentationMethodID using assumedScanType
                                    // AssumedScanType is an optional job setting; see for example:
                                    //  IonTrapDefSettings_AssumeHCD.xml with <item key="AssumedScanType" value="HCD"/>
                                    switch (assumedScanType.ToUpper())
                                    {
                                        case "CID":
                                            valueText = "1";
                                            break;
                                        case "ETD":
                                            valueText = "2";
                                            break;
                                        case "HCD":
                                            valueText = "3";
                                            break;
                                        case "UVPD":
                                            // Previously, with MS-GFDB, fragmentationType 4 meant Merge ETD and CID
                                            // Now with MS-GF+, fragmentationType 4 means UVPD
                                            valueText = "4";
                                            break;
                                        default:
                                            // Invalid string
                                            mErrorMessage = "Invalid assumed scan type '" + assumedScanType +
                                                            "'; must be CID, ETD, HCD, or UVPD";
                                            OnErrorEvent(mErrorMessage);
                                            return CloseOutType.CLOSEOUT_FAILED;
                                    }

                                    OnStatusEvent("Using Fragmentation method -m " + valueText + " because of Assumed scan type " + assumedScanType);
                                }
                                else
                                {
                                    OnStatusEvent("Using Fragmentation method -m " + valueText);
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_INSTRUMENT_ID))
                            {
                                if (!string.IsNullOrWhiteSpace(scanTypeFilePath))
                                {
                                    var eResult = DetermineInstrumentID(ref valueText, scanTypeFilePath, instrumentGroup);
                                    if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                                    {
                                        return eResult;
                                    }
                                }
                                else if (!string.IsNullOrWhiteSpace(instrumentGroup))
                                {

                                    if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out var instrumentIDNew, out var autoSwitchReason))
                                    {
                                        var datasetName = m_jobParams.GetParam("JobParameters", "DatasetNum");

                                        if (LookupScanTypesForDataset(datasetName, out var countLowResMSn, out var countHighResMSn, out var countHCDMSn))
                                        {
                                            ExamineScanTypes(countLowResMSn, countHighResMSn, countHCDMSn, out instrumentIDNew, out autoSwitchReason);
                                        }
                                    }

                                    AutoUpdateInstrumentIDIfChanged(ref valueText, instrumentIDNew, autoSwitchReason);
                                }
                            }

                            var argumentSwitchOriginal = string.Copy(argumentSwitch);

                            AdjustSwitchesForMSGFPlus(ref argumentSwitch, ref valueText);

                            if (overrideParams.TryGetValue(argumentSwitch, out var valueOverride))
                            {
                                OnStatusEvent("Overriding switch " + argumentSwitch + " to use -" + argumentSwitch + " " + valueOverride +
                                              " instead of -" + argumentSwitch + " " + valueText);
                                valueText = string.Copy(valueOverride);
                            }

                            if (string.IsNullOrEmpty(argumentSwitch))
                            {
                                if (m_DebugLevel >= 1 && !clsGlobal.IsMatch(argumentSwitchOriginal, MSGFPLUS_OPTION_SHOWDECOY))
                                {
                                    OnWarningEvent("Skipping switch " + argumentSwitchOriginal + " since it is not valid for this version of " + searchEngineName);
                                }
                            }
                            else if (string.IsNullOrEmpty(valueText))
                            {
                                if (m_DebugLevel >= 1)
                                {
                                    OnWarningEvent("Skipping switch " + argumentSwitch + " since the value is empty");
                                }
                            }
                            else
                            {
                                sbOptions.Append(" -" + argumentSwitch + " " + valueText);
                            }

                            if (clsGlobal.IsMatch(argumentSwitch, "showDecoy"))
                            {
                                if (int.TryParse(valueText, out value))
                                {
                                    if (value > 0)
                                    {
                                    }
                                }
                            }
                            else if (clsGlobal.IsMatch(argumentSwitch, "tda"))
                            {
                                if (int.TryParse(valueText, out value))
                                {
                                    if (value > 0)
                                    {
                                        isTDA = true;
                                    }
                                }
                            }
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "uniformAAProb"))
                        {
                            // Not valid for MS-GF+; skip it
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "NumThreads"))
                        {
                            if (string.IsNullOrWhiteSpace(valueText) || clsGlobal.IsMatch(valueText, "all"))
                            {
                                // Do not append -thread to the command line; MS-GF+ will use all available cores by default
                            }
                            else
                            {
                                if (int.TryParse(valueText, out paramFileThreadCount))
                                {
                                    // paramFileThreadCount now has the thread count
                                }
                                else
                                {
                                    OnWarningEvent("Invalid value for NumThreads in MS-GF+ parameter file: " + dataLine);
                                }
                            }
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "NumMods"))
                        {
                            if (int.TryParse(valueText, out value))
                            {
                                numMods = value;
                            }
                            else
                            {
                                mErrorMessage = "Invalid value for NumMods in MS-GF+ parameter file";
                                OnErrorEvent(mErrorMessage + ": " + dataLine);
                                srParamFile.Dispose();
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "StaticMod"))
                        {
                            if (!string.IsNullOrWhiteSpace(valueText) && !clsGlobal.IsMatch(valueText, "none"))
                            {
                                lstStaticMods.Add(valueText);
                            }
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "DynamicMod"))
                        {
                            if (!string.IsNullOrWhiteSpace(valueText) && !clsGlobal.IsMatch(valueText, "none"))
                            {
                                lstDynamicMods.Add(valueText);
                            }
                        }
                        else if (clsGlobal.IsMatch(kvSetting.Key, "CustomAA"))
                        {
                            if (!string.IsNullOrWhiteSpace(valueText) && !clsGlobal.IsMatch(valueText, "none"))
                            {
                                lstCustomAminoAcids.Add(valueText);
                            }
                        }

                        // if (clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_FRAGMENTATION_METHOD)) {
                        //	 if (int.TryParse(valueText, out value)) {
                        //		if (value == 3) {
                        //			isHCD = True;
                        //      }
                        //	 }
                        // }
                    }
                }

                if (isTDA)
                {
                    // Parameter file contains TDA=1 and we're running MS-GF+
                    mResultsIncludeAutoAddedDecoyPeptides = true;
                }

            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception reading MS-GF+ parameter file";
                OnErrorEvent(mErrorMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Define the thread count; note that MSGFDBThreads could be "all"
            var dmsDefinedThreadCountText = m_jobParams.GetJobParameter("MSGFDBThreads", string.Empty);
            if (string.IsNullOrWhiteSpace(dmsDefinedThreadCountText) || dmsDefinedThreadCountText.ToLower() == "all" ||
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
            var limitCoreUsage = Dns.GetHostName().StartsWith("proto-", StringComparison.OrdinalIgnoreCase);

            if (paramFileThreadCount <= 0 || limitCoreUsage)
            {
                // Set paramFileThreadCount to the number of cores on this computer
                // However, do not exceed 8 cores because this can actually slow down MS-GF+ due to context switching
                // Furthermore, Java will restrict all of the threads to a single NUMA node, and we don't want too many threads on a single node

                var coreCount = GetCoreCount();

                if (limitCoreUsage)
                {
                    var maxAllowedCores = (int)Math.Floor(coreCount * 0.75);
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
                        // There are enough spare cores that we can use 75% of all of the cores
                        var maxAllowedCores = (int)Math.Floor(coreCount * 0.75);
                        if (paramFileThreadCount > maxAllowedCores)
                        {
                            OnStatusEvent("The system has " + coreCount + " cores; " + searchEngineName + " will use " + maxAllowedCores + " cores " +
                                          "(bumped down from " + paramFileThreadCount + " to avoid overloading a single NUMA node)");
                            paramFileThreadCount = maxAllowedCores;
                        }
                    }
                    else
                    {
                        // Example message: The system has 12 cores; MS-GF+ will use 8 cores (bumped down from 9 to avoid overloading a single NUMA node)
                        OnStatusEvent("The system has " + coreCount + " cores; " + searchEngineName + " will use 8 cores " +
                                      "(bumped down from " + paramFileThreadCount + " to avoid overloading a single NUMA node)");
                        paramFileThreadCount = 8;
                    }
                }
                else
                {
                    // Example message: The system has 8 cores; MS-GF+ will use 7 cores")
                    OnStatusEvent("The system has " + coreCount + " cores; " + searchEngineName + " will use " + paramFileThreadCount + " cores");
                }
            }

            if (paramFileThreadCount > 0)
            {
                sbOptions.Append(" -thread " + paramFileThreadCount);
            }

            // Create the modification file and append the -mod switch
            // We'll also set mPhosphorylationSearch to True if a dynamic or static mod is STY phosphorylation
            if (!ParseMSGFDBModifications(parameterFilePath, sbOptions, numMods, lstStaticMods, lstDynamicMods, lstCustomAminoAcids))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Prior to MS-GF+ version v9284 we used " -protocol 1" at the command line when performing an HCD-based phosphorylation search
            // However, v9284 now auto-selects the correct protocol based on the spectrum type and the dynamic modifications
            // Options for -protocol are 0=NoProtocol (Default), 1=Phosphorylation, 2=iTRAQ, 3=iTRAQPhospho
            //
            // As of March 23, 2015, if the user is searching for Phospho mods with TMT labeling enabled,
            // then MS-GF+ will use a model trained for TMT peptides (without phospho)
            // In this case, the user should probably use a parameter file with Protocol=1 defined (which leads to sbOptions having "-protocol 1")

            msgfPlusCmdLineOptions = sbOptions.ToString();

            // By default, MS-GF+ filters out spectra with fewer than 20 data points
            // Override this threshold to 5 data points
            if (msgfPlusCmdLineOptions.IndexOf("-minNumPeaks", StringComparison.OrdinalIgnoreCase) < 0)
            {
                msgfPlusCmdLineOptions += " -minNumPeaks 5";
            }

            // Auto-add the "addFeatures" switch if not present
            // This is required to post-process the results with Percolator
            if (msgfPlusCmdLineOptions.IndexOf("-addFeatures", StringComparison.OrdinalIgnoreCase) < 0)
            {
                msgfPlusCmdLineOptions += " -addFeatures 1";
            }

            if (msgfPlusCmdLineOptions.Contains("-tda 1"))
            {
                // Make sure the .Fasta file is not a Decoy fasta
                if (fastaFileIsDecoy)
                {
                    OnErrorEvent("Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Override Instrument ID based on the instrument class and scan types in the _ScanType file
        /// </summary>
        /// <param name="instrumentIDCurrent">Current instrument ID; may get updated by this method</param>
        /// <param name="scanTypeFilePath"></param>
        /// <param name="instrumentGroup"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType DetermineInstrumentID(ref string instrumentIDCurrent, string scanTypeFilePath, string instrumentGroup)
        {
            // InstrumentID values:
            // #  0 means Low-res LCQ/LTQ (Default for CID and ETD); use InstrumentID=0 if analyzing a dataset with low-res CID and high-res HCD spectra
            // #  1 means High-res LTQ (Default for HCD; also appropriate for high res CID).  Do not merge spectra (FragMethod=4) when InstrumentID is 1; scores will degrade
            // #  2 means TOF
            // #  3 means Q-Exactive

            if (string.IsNullOrEmpty(instrumentGroup))
                instrumentGroup = "#Undefined#";


            if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out var instrumentIDNew, out var autoSwitchReason))
            {
                // Instrument ID is not obvious from the instrument group
                // Examine the scan types in scanTypeFilePath

                // If low res MS1,  Instrument Group is typically LCQ, LTQ, LTQ-ETD, LTQ-Prep, VelosPro

                // If high res MS2, Instrument Group is typically VelosOrbi, or LTQ_FT

                // Count the number of High res CID or ETD spectra
                // Count HCD spectra separately since MS-GF+ has a special scoring model for HCD spectra


                var success = LoadScanTypeFile(scanTypeFilePath, out var lstLowResMSn, out var lstHighResMSn, out var lstHCDMSn, out _);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mErrorMessage))
                    {
                        mErrorMessage = "LoadScanTypeFile returned false for " + Path.GetFileName(scanTypeFilePath);
                        OnErrorEvent(mErrorMessage);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (lstLowResMSn.Count + lstHighResMSn.Count + lstHCDMSn.Count == 0)
                {
                    mErrorMessage = "LoadScanTypeFile could not find any MSn spectra " + Path.GetFileName(scanTypeFilePath);
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ExamineScanTypes(lstLowResMSn.Count, lstHighResMSn.Count, lstHCDMSn.Count, out instrumentIDNew, out autoSwitchReason);
            }

            AutoUpdateInstrumentIDIfChanged(ref instrumentIDCurrent, instrumentIDNew, autoSwitchReason);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void ExamineScanTypes(int countLowResMSn, int countHighResMSn, int countHCDMSn, out string instrumentIDNew, out string autoSwitchReason)
        {
            instrumentIDNew = string.Empty;
            autoSwitchReason = string.Empty;

            if (countLowResMSn + countHighResMSn + countHCDMSn == 0)
            {
                // Scan counts are all 0; leave instrumentIDNew untouched
                OnStatusEvent("Scan counts provided to ExamineScanTypes are all 0; cannot auto-update InstrumentID");
            }
            else
            {
                double dblFractionHiRes = 0;

                if (countHighResMSn > 0)
                {
                    dblFractionHiRes = countHighResMSn / (double)(countLowResMSn + countHighResMSn);
                }

                if (dblFractionHiRes > 0.1)
                {
                    // At least 10% of the spectra are HMSn
                    instrumentIDNew = "1";
                    autoSwitchReason = "since " + (dblFractionHiRes * 100).ToString("0") + "% of the spectra are HMSn";
                }
                else
                {
                    if (countLowResMSn == 0 && countHCDMSn > 0)
                    {
                        // All of the spectra are HCD
                        instrumentIDNew = "1";
                        autoSwitchReason = "since all of the spectra are HCD";
                    }
                    else
                    {
                        instrumentIDNew = "0";
                        if (countHCDMSn == 0 && countHighResMSn == 0)
                        {
                            autoSwitchReason = "since all of the spectra are low res MSn";
                        }
                        else
                        {
                            autoSwitchReason = "since there is a mix of low res and high res spectra";
                        }
                    }
                }
            }
        }

        private bool LookupScanTypesForDataset(string datasetName, out int countLowResMSn, out int countHighResMSn, out int countHCDMSn)
        {
            countLowResMSn = 0;
            countHighResMSn = 0;
            countHCDMSn = 0;

            try
            {
                if (string.IsNullOrEmpty(datasetName))
                {
                    return false;
                }

                var connectionString = m_mgrParams.GetParam("connectionstring");

                var sqlStr = new StringBuilder();

                sqlStr.Append(" SELECT HMS, MS, [CID-HMSn], [CID-MSn], ");
                sqlStr.Append("   [HCD-HMSn], [ETD-HMSn], [ETD-MSn], ");
                sqlStr.Append("   [SA_ETD-HMSn], [SA_ETD-MSn], ");
                sqlStr.Append("   HMSn, MSn, ");
                sqlStr.Append("   [PQD-HMSn], [PQD-MSn]");
                sqlStr.Append(" FROM V_Dataset_ScanType_CrossTab");
                sqlStr.Append(" WHERE Dataset = '" + datasetName + "'");

                const int retryCount = 2;

                // Get a table to hold the results of the query
                var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LookupScanTypesForDataset", retryCount, out var dtResults);

                if (!success)
                {
                    OnErrorEvent("Excessive failures attempting to retrieve dataset scan types in LookupScanTypesForDataset");
                    dtResults.Dispose();
                    return false;
                }

                // Verify at least one row returned
                if (dtResults.Rows.Count < 1)
                {
                    // No data was returned
                    OnStatusEvent("No rows were returned for dataset " + datasetName + " from V_Dataset_ScanType_CrossTab in DMS");
                    return false;
                }

                foreach (DataRow curRow in dtResults.Rows)
                {
                    countLowResMSn += clsGlobal.DbCInt(curRow["CID-MSn"]);
                    countHighResMSn += clsGlobal.DbCInt(curRow["CID-HMSn"]);
                    countHCDMSn += clsGlobal.DbCInt(curRow["HCD-HMSn"]);

                    countHighResMSn += clsGlobal.DbCInt(curRow["ETD-HMSn"]);
                    countLowResMSn += clsGlobal.DbCInt(curRow["ETD-MSn"]);

                    countHighResMSn += clsGlobal.DbCInt(curRow["SA_ETD-HMSn"]);
                    countLowResMSn += clsGlobal.DbCInt(curRow["SA_ETD-MSn"]);

                    countHighResMSn += clsGlobal.DbCInt(curRow["HMSn"]);
                    countLowResMSn += clsGlobal.DbCInt(curRow["MSn"]);

                    countHighResMSn += clsGlobal.DbCInt(curRow["PQD-HMSn"]);
                    countLowResMSn += clsGlobal.DbCInt(curRow["PQD-MSn"]);
                }

                dtResults.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                const string msg = "Exception in LookupScanTypersForDataset";
                OnErrorEvent(msg, ex);
                return false;
            }
        }

        /// <summary>
        /// Validates that the modification definition text
        /// </summary>
        /// <param name="modDef">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        private bool ParseMSGFDbValidateMod(string modDef, out string modClean)
        {
            var comment = string.Empty;

            modClean = string.Empty;

            var poundIndex = modDef.IndexOf('#');
            if (poundIndex > 0)
            {
                comment = modDef.Substring(poundIndex);
                modDef = modDef.Substring(0, poundIndex - 1).Trim();
            }

            // Split on commas, change tabs to spaces, and remove whitespace
            var modParts = modDef.Split(',');
            for (var i = 0; i <= modParts.Length - 1; i++)
            {
                modParts[i] = modParts[i].Replace("\t", " ").Trim();
            }

            // Check whether this is a custom AA definition
            var query = (from item in modParts where item.ToLower() == "custom" select item).ToList();
            var customAminoAcidDef = query.Count > 0;

            if (modParts.Length < 5)
            {
                // Invalid definition

                if (customAminoAcidDef)
                {
                    // Invalid custom AA definition; must have 5 sections, for example:
                    // C5H7N1O2S0,J,custom,P,Hydroxylation     # Hydroxyproline
                    mErrorMessage = "Invalid custom AA string; must have 5 sections: " + modDef;
                }
                else
                {
                    // Invalid dynamic or static mod definition; must have 5 sections, for example:
                    // O1, M, opt, any, Oxidation
                    mErrorMessage = "Invalid modification string; must have 5 sections: " + modDef;
                }

                OnErrorEvent(mErrorMessage);
                return false;
            }

            // Reconstruct the mod (or custom AA) definition, making sure there is no whitespace
            modClean = string.Copy(modParts[0]);

            if (customAminoAcidDef)
            {
                // Make sure that the custom amino acid definition does not have any invalid characters
                var reInvalidCharacters = new Regex(@"[^CHNOS0-9]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
                var lstInvalidCharacters = reInvalidCharacters.Matches(modClean);

                if (lstInvalidCharacters.Count > 0)
                {
                    mErrorMessage = "Custom amino acid empirical formula " + modClean + " has invalid characters. " +
                                    "It must only contain C, H, N, O, and S, and optionally an integer after each element, for example: C6H7N3O";
                    OnErrorEvent(mErrorMessage);
                    return false;
                }

                // Make sure that all of the elements in modClean have a number after them
                // For example, auto-change C6H7N3O to C6H7N3O1

                var reElementSplitter = new Regex(@"(?<Atom>[A-Z])(?<Count>\d*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

                var lstElements = reElementSplitter.Matches(modClean);
                var reconstructedFormula = string.Empty;

                foreach (Match subPart in lstElements)
                {
                    var elementSymbol = subPart.Groups["Atom"].ToString();
                    var elementCount = subPart.Groups["Count"].ToString();

                    if (elementSymbol != "C" && elementSymbol != "H" && elementSymbol != "N" && elementSymbol != "O" && elementSymbol != "S")
                    {
                        mErrorMessage = "Invalid element " + elementSymbol + " in the custom amino acid empirical formula " + modClean + "; " +
                                        "MS-GF+ only supports C, H, N, O, and S";
                        OnErrorEvent(mErrorMessage);
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

            for (var index = 1; index <= modParts.Length - 1; index++)
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
            if (modParts[(int)ModDefinitionParts.Name].StartsWith("PHOSPH", StringComparison.OrdinalIgnoreCase) ||
                modParts[(int)ModDefinitionParts.EmpiricalFormulaOrMass].StartsWith("HO3P", StringComparison.OrdinalIgnoreCase))
            {
                if (modParts[(int)ModDefinitionParts.Residues].ToUpper().IndexOfAny(new[]
                {
                    'S',
                    'T',
                    'Y'
                }) >= 0)
                {
                    mPhosphorylationSearch = true;
                }
            }

            return true;
        }

        private string ReverseString(string text)
        {
            var chReversed = text.ToCharArray();
            Array.Reverse(chReversed);
            return new string(chReversed);
        }

        /// <summary>
        /// Previously returned true if legacy MSGFDB should have been used
        /// Now always returns false
        /// </summary>
        /// <param name="jobParams"></param>
        /// <returns></returns>
        public static bool UseLegacyMSGFDB(IJobParams jobParams)
        {
            return false;
        }

        private bool ValidatePeptideToProteinMapResults(string pepToProtMapFilePath, bool ignorePeptideToProteinMapperErrors)
        {
            const string PROTEIN_NAME_NO_MATCH = "__NoMatch__";

            bool success;

            var peptideCount = 0;
            var peptideCountNoMatch = 0;
            var linesRead = 0;

            try
            {
                // Validate that none of the results in pepToProtMapFilePath has protein name PROTEIN_NAME_NO_MATCH

                if (m_DebugLevel >= 2)
                {
                    OnStatusEvent("Validating peptide to protein mapping, file " + Path.GetFileName(pepToProtMapFilePath));
                }

                using (var srInFile = new StreamReader(new FileStream(pepToProtMapFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var dataLine = srInFile.ReadLine();
                        linesRead += 1;

                        if (linesRead <= 1 || string.IsNullOrEmpty(dataLine))
                            continue;

                        peptideCount += 1;
                        if (dataLine.Contains(PROTEIN_NAME_NO_MATCH))
                        {
                            peptideCountNoMatch += 1;
                        }
                    }
                }

                if (peptideCount == 0)
                {
                    mErrorMessage = "Peptide to protein mapping file is empty";
                    OnErrorEvent(mErrorMessage + ", file " + Path.GetFileName(pepToProtMapFilePath));
                    success = false;
                }
                else if (peptideCountNoMatch == 0)
                {
                    if (m_DebugLevel >= 2)
                    {
                        OnStatusEvent("Peptide to protein mapping validation complete; processed " + peptideCount + " peptides");
                    }

                    success = true;
                }
                else
                {
                    // Value between 0 and 100
                    var dblErrorPercent = peptideCountNoMatch / (double)peptideCount * 100.0;

                    mErrorMessage = dblErrorPercent.ToString("0.0") + "% of the entries in the peptide to protein map file did not match to a protein in the FASTA file";
                    OnErrorEvent(mErrorMessage);

                    if (ignorePeptideToProteinMapperErrors)
                    {
                        OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                        success = true;
                    }
                    else
                    {
                        IgnorePreviousErrorEvent?.Invoke();
                        success = false;
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error validating peptide to protein map file";
                OnErrorEvent(mErrorMessage, ex);
                success = false;
            }

            return success;
        }

        private void WriteProteinSequence(TextWriter swOutFile, string sequence)
        {
            var index = 0;

            while (index < sequence.Length)
            {
                var length = Math.Min(60, sequence.Length - index);
                swOutFile.WriteLine(sequence.Substring(index, length));
                index += 60;
            }
        }

        /// <summary>
        /// Zips MS-GF+ Output File (creating a .gz file)
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ZipOutputFile(clsAnalysisToolRunnerBase oToolRunner, string fileName)
        {
            try
            {
                var tmpFilePath = Path.Combine(m_WorkDir, fileName);
                if (!File.Exists(tmpFilePath))
                {
                    OnErrorEvent("MS-GF+ results file not found: " + fileName);
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                if (!oToolRunner.GZipFile(tmpFilePath, false))
                {
                    OnErrorEvent("Error zipping output files: oToolRunner.GZipFile returned false");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
                m_jobParams.AddResultFileToSkip(fileName);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error zipping output files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion

        #region "Event Methods"

        private DateTime dtLastLogTime = DateTime.MinValue;

        private void PeptideToProteinMapper_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int MAPPER_PROGRESS_LOG_INTERVAL_SECONDS = 120;

            if (m_DebugLevel < 1) return;

            if (DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS)
            {
                dtLastLogTime = DateTime.UtcNow;
                OnStatusEvent("Mapping peptides to proteins: " + percentComplete.ToString("0.0") + "% complete");
            }
        }

        #endregion

    }
}
