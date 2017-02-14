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

namespace AnalysisManagerMSGFDBPlugIn
{
    public class clsMSGFDBUtils : clsEventNotifier
    {
        #region "Constants"

        public const float PROGRESS_PCT_MSGFPLUS_STARTING = 1;
        public const float PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE = 2;
        public const float PROGRESS_PCT_MSGFPLUS_READING_SPECTRA = 3;
        public const float PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED = 4;
        public const float PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS = 95;
        public const float PROGRESS_PCT_MSGFPLUS_COMPLETE = 96;
        public const float PROGRESS_PCT_MSGFPLUS_CONVERT_MZID_TO_TSV = 97;
        public const float PROGRESS_PCT_MSGFPLUS_MAPPING_PEPTIDES_TO_PROTEINS = 98;

        public const float PROGRESS_PCT_COMPLETE = 99;

        private const string MZIDToTSV_CONSOLE_OUTPUT_FILE = "MzIDToTsv_ConsoleOutput.txt";

        private enum eModDefinitionParts : int
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

        public const string MSGFPLUS_TSV_SUFFIX = "_msgfplus.tsv";

        // Obsolete setting: Old MS-GFDB program
        //Public Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

        public const string MSGFPLUS_JAR_NAME = "MSGFPlus.jar";
        public const string MSGFPLUS_CONSOLE_OUTPUT_FILE = "MSGFPlus_ConsoleOutput.txt";

        public const string MOD_FILE_NAME = "MSGFPlus_Mods.txt";

        #endregion

        #region "Events"

        public event IgnorePreviousErrorEventEventHandler IgnorePreviousErrorEvent;

        public delegate void IgnorePreviousErrorEventEventHandler();

        #endregion

        #region "Module Variables"

        private readonly IMgrParams m_mgrParams;
        private readonly IJobParams m_jobParams;

        private readonly string m_WorkDir;
        private readonly string m_JobNum;
        private readonly short m_DebugLevel;

        private readonly bool mMSGFPlus;
        private string mMSGFPlusVersion = string.Empty;
        private string mErrorMessage = string.Empty;
        private string mConsoleOutputErrorMsg = string.Empty;

        private int mContinuumSpectraSkipped;
        private int mSpectraSearched;

        private int mThreadCountActual;
        private int mTaskCountTotal;
        private int mTaskCountCompleted;

        private bool mPhosphorylationSearch;
        private bool mResultsIncludeAutoAddedDecoyPeptides;

        // Note that clsPeptideToProteinMapEngine utilizes System.Data.SQLite.dll
        private PeptideToProteinMapEngine.clsPeptideToProteinMapEngine mPeptideToProteinMapper;

        #endregion

        #region "Properties"

        public int ContinuumSpectraSkipped
        {
            get { return mContinuumSpectraSkipped; }
        }

        public string ConsoleOutputErrorMsg
        {
            get { return mConsoleOutputErrorMsg; }
        }

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        public string MSGFPlusVersion
        {
            get { return mMSGFPlusVersion; }
        }

        public bool PhosphorylationSearch
        {
            get { return mPhosphorylationSearch; }
        }

        public bool ResultsIncludeAutoAddedDecoyPeptides
        {
            get { return mResultsIncludeAutoAddedDecoyPeptides; }
        }

        public int SpectraSearched
        {
            get { return mSpectraSearched; }
        }

        public int ThreadCountActual
        {
            get { return mThreadCountActual; }
        }

        public int TaskCountTotal
        {
            get { return mTaskCountTotal; }
        }

        public int TaskCountCompleted
        {
            get { return mTaskCountCompleted; }
        }

        #endregion

        #region "Methods"

        public clsMSGFDBUtils(IMgrParams oMgrParams, IJobParams oJobParams, string JobNum, string strWorkDir, short intDebugLevel, bool blnMSGFPlus)
        {
            m_mgrParams = oMgrParams;
            m_jobParams = oJobParams;

            m_WorkDir = strWorkDir;

            m_JobNum = JobNum;
            m_DebugLevel = intDebugLevel;

            mMSGFPlus = blnMSGFPlus;
            mMSGFPlusVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;
            mContinuumSpectraSkipped = 0;
            mSpectraSearched = 0;

            mThreadCountActual = 0;
            mTaskCountTotal = 0;
            mTaskCountCompleted = 0;
        }

        /// <summary>
        /// Update strArgumentSwitch and strValue if using the MS-GFDB syntax yet should be using the MS-GF+ syntax (or vice versa)
        /// </summary>
        /// <param name="blnMSGFPlus"></param>
        /// <param name="strArgumentSwitch"></param>
        /// <param name="strValue"></param>
        /// <remarks></remarks>
        private void AdjustSwitchesForMSGFPlus(bool blnMSGFPlus, ref string strArgumentSwitch, ref string strValue)
        {
            int intValue = 0;
            int intCharIndex = 0;

            if (blnMSGFPlus)
            {
                // MS-GF+

                if (clsGlobal.IsMatch(strArgumentSwitch, "nnet"))
                {
                    // Auto-switch to ntt
                    strArgumentSwitch = "ntt";
                    if (int.TryParse(strValue, out intValue))
                    {
                        switch (intValue)
                        {
                            case 0:
                                strValue = "2";         // Fully-tryptic
                                break;
                            case 1:
                                strValue = "1";         // Partially tryptic
                                break;
                            case 2:
                                strValue = "0";         // No-enzyme search
                                break;
                            default:
                                // Assume partially tryptic
                                strValue = "1";
                                break;
                        }
                    }
                }
                else if (clsGlobal.IsMatch(strArgumentSwitch, "c13"))
                {
                    // Auto-switch to ti
                    strArgumentSwitch = "ti";
                    if (int.TryParse(strValue, out intValue))
                    {
                        if (intValue == 0)
                        {
                            strValue = "0,0";
                        }
                        else if (intValue == 1)
                        {
                            strValue = "-1,1";
                        }
                        else if (intValue == 2)
                        {
                            strValue = "-1,2";
                        }
                        else
                        {
                            strValue = "0,1";
                        }
                    }
                    else
                    {
                        strValue = "0,1";
                    }
                }
                else if (clsGlobal.IsMatch(strArgumentSwitch, "showDecoy"))
                {
                    // Not valid for MS-GF+; skip it
                    strArgumentSwitch = string.Empty;
                }
            }
            else
            {
                // MS-GFDB

                if (clsGlobal.IsMatch(strArgumentSwitch, "ntt"))
                {
                    // Auto-switch to nnet
                    strArgumentSwitch = "nnet";
                    if (int.TryParse(strValue, out intValue))
                    {
                        switch (intValue)
                        {
                            case 2:
                                strValue = "0";         // Fully-tryptic
                                break;
                            case 1:
                                strValue = "1";         // Partially tryptic
                                break;
                            case 0:
                                strValue = "2";         // No-enzyme search
                                break;
                            default:
                                // Assume partially tryptic
                                strValue = "1";
                                break;
                        }
                    }
                }
                else if (clsGlobal.IsMatch(strArgumentSwitch, "ti"))
                {
                    // Auto-switch to c13
                    // Use the digit after the comma in the "ti" specification
                    strArgumentSwitch = "c13";
                    intCharIndex = strValue.IndexOf(",", StringComparison.Ordinal);
                    if (intCharIndex >= 0)
                    {
                        strValue = strValue.Substring(intCharIndex + 1);
                    }
                    else
                    {
                        // Comma not found
                        if (int.TryParse(strValue, out intValue))
                        {
                            strValue = intValue.ToString();
                        }
                        else
                        {
                            strValue = "1";
                        }
                    }
                }
                else if (clsGlobal.IsMatch(strArgumentSwitch, "addFeatures"))
                {
                    // Not valid for MS-GFDB; skip it
                    strArgumentSwitch = string.Empty;
                }
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
                OnErrorEvent("Error in clsMSGFDBUtils->AppendConsoleOutputHeader", ex);
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
            else if (clsGlobal.IsMatch(instrumentGroup, "Bruker_Amazon_Ion_Trap"))
            {
                // Non-Thermo Instrument, low res MS/MS
                instrumentIDNew = "0";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }
            else if (clsGlobal.IsMatch(instrumentGroup, "IMS"))
            {
                // Non-Thermo Instrument, high res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }
            else if (clsGlobal.IsMatch(instrumentGroup, "Sciex_TripleTOF"))
            {
                // Non-Thermo Instrument, high res MS/MS
                instrumentIDNew = "1";
                autoSwitchReason = "based on instrument group " + instrumentGroup;
                return true;
            }
            else
            {
                instrumentIDNew = string.Empty;
                autoSwitchReason = string.Empty;
                return false;
            }
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
                    var strInstIDDescription = "??";
                    switch (instrumentIDNew)
                    {
                        case "0":
                            strInstIDDescription = "Low-res MSn";
                            break;
                        case "1":
                            strInstIDDescription = "High-res MSn";
                            break;
                        case "2":
                            strInstIDDescription = "TOF";
                            break;
                        case "3":
                            strInstIDDescription = "Q-Exactive";
                            break;
                    }

                    OnStatusEvent("Auto-updating instrument ID from " + instrumentIDCurrent + " to " + instrumentIDNew + " (" + strInstIDDescription +
                                  ") " + autoSwitchReason);
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
        /// <remarks></remarks>
        public string ConvertMZIDToTSV(string mzidToTsvConverterProgLoc, string datasetName, string mzidFileName)
        {
            try
            {
                // In November 2016, this file was renamed from Dataset_msgfdb.tsv to Dataset_msgfplus.tsv
                var tsvFileName = datasetName + MSGFPLUS_TSV_SUFFIX;
                var strTSVFilePath = Path.Combine(m_WorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var fiMzidFile = new FileInfo(Path.Combine(m_WorkDir, mzidFileName));
                if (!fiMzidFile.Exists)
                {
                    OnErrorEvent("Error in clsMSGFDBUtils->ConvertMZIDToTSV; Mzid file not found: " + fiMzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
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

                if (!lastLine.Trim().EndsWith("</MzIdentML>", StringComparison.InvariantCulture))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Set up and execute a program runner to run MzidToTsvConverter.exe
                var cmdStr = GetMZIDtoTSVCommandLine(mzidFileName, tsvFileName, m_WorkDir, mzidToTsvConverterProgLoc);

                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent(mzidToTsvConverterProgLoc + " " + cmdStr);
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
                var blnSuccess = objCreateTSV.RunProgram(mzidToTsvConverterProgLoc, cmdStr, "MzIDToTsv", true);

                if (!blnSuccess)
                {
                    OnErrorEvent("MzidToTsvConverter.exe returned an error code converting the .mzid file To a .tsv file: " + objCreateTSV.ExitCode);
                    return string.Empty;
                }
                else
                {
                    // The conversion succeeded

                    // Append the first line from the console output file to the end of the MSGFPlus console output file
                    AppendConsoleOutputHeader(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);

                    try
                    {
                        // Delete the console output file
                        File.Delete(objCreateTSV.ConsoleOutputFilePath);
                    }
                    catch (Exception ex)
                    {
                        // Ignore errors here
                    }
                }

                return strTSVFilePath;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in clsMSGFDBUtils->ConvertMZIDToTSV", ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Convert a .mzid file to a tab-delimited text file (.tsv) using MSGFPlus.jar
        /// </summary>
        /// <param name="javaProgLoc">Full path to Java</param>
        /// <param name="msgfDbProgLoc">Folder with MSGFPlusjar</param>
        /// <param name="strDatasetName">Dataset name (output file will be named DatasetName_msgfdb.tsv)</param>
        /// <param name="strMZIDFileName">.mzid file name (assumed to be in the work directory)</param>
        /// <returns>TSV file path, or an empty string if an error</returns>
        /// <remarks></remarks>
        [Obsolete("Use the version of ConvertMzidToTsv that simply accepts a dataset name and .mzid file path and uses MzidToTsvConverter.exe")]
        public string ConvertMZIDToTSV(string javaProgLoc, string msgfDbProgLoc, string strDatasetName, string strMZIDFileName)
        {
            string strTSVFilePath = null;

            try
            {
                // In November 2016, this file was renamed from Dataset_msgfdb.tsv to Dataset_msgfplus.tsv
                var tsvFileName = strDatasetName + MSGFPLUS_TSV_SUFFIX;
                strTSVFilePath = Path.Combine(m_WorkDir, tsvFileName);

                // Examine the size of the .mzid file
                var fiMzidFile = new FileInfo(Path.Combine(m_WorkDir, strMZIDFileName));
                if (!fiMzidFile.Exists)
                {
                    OnErrorEvent("Error in clsMSGFDBUtils->ConvertMZIDToTSV; Mzid file not found: " + fiMzidFile.FullName);
                    return string.Empty;
                }

                // Make sure the mzid file ends with XML tag </MzIdentML>
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

                if (!string.Equals(lastLine.Trim(), "</MzIdentML>", StringComparison.InvariantCulture))
                {
                    OnErrorEvent("The .mzid file created by MS-GF+ does not end with XML tag MzIdentML");
                    return string.Empty;
                }

                // Dynamically set the amount of required memory based on the size of the .mzid file
                var fileSizeMB = fiMzidFile.Length / 1024.0 / 1024.0;
                var javaMemorySizeMB = 10000;

                if (fileSizeMB < 1000)
                    javaMemorySizeMB = 8000;
                if (fileSizeMB < 800)
                    javaMemorySizeMB = 7000;
                if (fileSizeMB < 600)
                    javaMemorySizeMB = 6000;
                if (fileSizeMB < 400)
                    javaMemorySizeMB = 5000;
                if (fileSizeMB < 300)
                    javaMemorySizeMB = 4000;
                if (fileSizeMB < 200)
                    javaMemorySizeMB = 3000;
                if (fileSizeMB < 100)
                    javaMemorySizeMB = 2000;

                // Set up and execute a program runner to run the MzIDToTsv module of MSGFPlus
                var cmdStr = GetMZIDtoTSVCommandLine(strMZIDFileName, tsvFileName, m_WorkDir, msgfDbProgLoc, javaMemorySizeMB);

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
                var blnSuccess = objCreateTSV.RunProgram(javaProgLoc, cmdStr, "MzIDToTsv", true);

                if (!blnSuccess)
                {
                    OnErrorEvent("MSGFPlus returned an error code converting the .mzid file to a .tsv file: " + objCreateTSV.ExitCode);
                    return string.Empty;
                }
                else
                {
                    // The conversion succeeded

                    // Append the first line from the console output file to the end of the MSGFPlus console output file
                    AppendConsoleOutputHeader(m_WorkDir, MZIDToTSV_CONSOLE_OUTPUT_FILE, MSGFPLUS_CONSOLE_OUTPUT_FILE, 1);

                    try
                    {
                        // Delete the console output file
                        File.Delete(objCreateTSV.ConsoleOutputFilePath);
                    }
                    catch (Exception ex)
                    {
                        // Ignore errors here
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in clsMSGFDBUtils->ConvertMZIDToTSV", ex);
                return string.Empty;
            }

            return strTSVFilePath;
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
            var cmdStr = " -mzid:" + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName)) + " -tsv:" +
                         clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName)) + " -unroll" + " -showDecoy";

            return cmdStr;
        }

        [Obsolete("Use GetMZIDtoTSVCommandLine for MzidToTsvConverter.exe")]
        public static string GetMZIDtoTSVCommandLine(string mzidFileName, string tsvFileName, string workingDirectory, string msgfDbProgLoc, int javaMemorySizeMB)
        {
            string cmdStr = null;

            // We're using "-XX:+UseConcMarkSweepGC" as directed at http://stackoverflow.com/questions/5839359/java-lang-outofmemoryerror-gc-overhead-limit-exceeded
            // due to seeing error "java.lang.OutOfMemoryError: GC overhead limit exceeded" with a 353 MB .mzid file

            cmdStr = " -Xmx" + javaMemorySizeMB + "M -XX:+UseConcMarkSweepGC -cp " + msgfDbProgLoc;
            cmdStr += " edu.ucsd.msjava.ui.MzIDToTsv";

            cmdStr += " -i " + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, mzidFileName));
            cmdStr += " -o " + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(workingDirectory, tsvFileName));
            cmdStr += " -showQValue 1";
            cmdStr += " -showDecoy 1";
            cmdStr += " -unroll 1";

            return cmdStr;
        }

        public CloseOutType CreatePeptideToProteinMapping(string ResultsFileName,
            PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants ePeptideInputFileFormat)
        {
            const bool blnResultsIncludeAutoAddedDecoyPeptides = false;
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder, ePeptideInputFileFormat);
        }

        public CloseOutType CreatePeptideToProteinMapping(string ResultsFileName, bool blnResultsIncludeAutoAddedDecoyPeptides)
        {
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);
        }

        public CloseOutType CreatePeptideToProteinMapping(string ResultsFileName, bool blnResultsIncludeAutoAddedDecoyPeptides, string localOrgDbFolder)
        {
            return CreatePeptideToProteinMapping(ResultsFileName, blnResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder,
                PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.MSGFDBResultsFile);
        }

        /// <summary>
        /// Create file Dataset_msgfplus_PepToProtMap.txt
        /// </summary>
        /// <param name="ResultsFileName"></param>
        /// <param name="blnResultsIncludeAutoAddedDecoyPeptides"></param>
        /// <param name="localOrgDbFolder"></param>
        /// <param name="ePeptideInputFileFormat"></param>
        /// <returns></returns>
        public CloseOutType CreatePeptideToProteinMapping(string ResultsFileName, bool blnResultsIncludeAutoAddedDecoyPeptides,
            string localOrgDbFolder, PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants ePeptideInputFileFormat)
        {
            // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
            var dbFilename = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
            string strInputFilePath = null;
            string strFastaFilePath = null;

            string msg = null;

            bool blnIgnorePeptideToProteinMapperErrors = false;
            bool blnSuccess = false;

            strInputFilePath = Path.Combine(m_WorkDir, ResultsFileName);
            strFastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

            try
            {
                // Validate that the input file has at least one entry; if not, then no point in continuing
                string strLineIn = null;
                int intLinesRead = 0;

                var fiInputFile = new FileInfo(strInputFilePath);
                if (!fiInputFile.Exists)
                {
                    msg = "MS-GF+ TSV results file not found: " + strInputFilePath;
                    OnErrorEvent(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (fiInputFile.Length == 0)
                {
                    msg = "MS-GF+ TSV results file is empty";
                    OnErrorEvent(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                using (var srInFile = new StreamReader(new FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream && intLinesRead < 10)
                    {
                        strLineIn = srInFile.ReadLine();
                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            intLinesRead += 1;
                        }
                    }
                }

                if (intLinesRead <= 1)
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

            if (blnResultsIncludeAutoAddedDecoyPeptides)
            {
                // Read the original fasta file to create a decoy fasta file
                strFastaFilePath = GenerateDecoyFastaFile(strFastaFilePath, m_WorkDir);

                if (string.IsNullOrEmpty(strFastaFilePath))
                {
                    // Problem creating the decoy fasta file
                    if (string.IsNullOrEmpty(mErrorMessage))
                    {
                        mErrorMessage = "Error creating a decoy version of the fasta file";
                    }
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(Path.GetFileName(strFastaFilePath));
            }

            try
            {
                if (m_DebugLevel >= 1)
                {
                    OnStatusEvent("Creating peptide to protein map file");
                }

                blnIgnorePeptideToProteinMapperErrors = m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", false);

                mPeptideToProteinMapper = new PeptideToProteinMapEngine.clsPeptideToProteinMapEngine
                {
                    DeleteTempFiles = true,
                    IgnoreILDifferences = false,
                    InspectParameterFilePath = string.Empty,
                    MatchPeptidePrefixAndSuffixToProtein = false,
                    OutputProteinSequence = false,
                    PeptideInputFileFormat = ePeptideInputFileFormat,
                    PeptideFileSkipFirstLine = false,
                    ProteinDataRemoveSymbolCharacters = true,
                    ProteinInputFilePath = strFastaFilePath,
                    SaveProteinToPeptideMappingFile = true,
                    SearchAllProteinsForPeptideSequence = true,
                    SearchAllProteinsSkipCoverageComputationSteps = true,
                    ShowMessages = false
                };
                mPeptideToProteinMapper.ProgressChanged += mPeptideToProteinMapper_ProgressChanged;

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
                blnSuccess = mPeptideToProteinMapper.ProcessFile(strInputFilePath, m_WorkDir, string.Empty, true);

                mPeptideToProteinMapper.CloseLogFileNow();

                string strResultsFilePath = null;
                strResultsFilePath = Path.GetFileNameWithoutExtension(strInputFilePath) +
                                     PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.FILENAME_SUFFIX_PEP_TO_PROTEIN_MAPPING;
                strResultsFilePath = Path.Combine(m_WorkDir, strResultsFilePath);

                if (blnSuccess)
                {
                    if (!File.Exists(strResultsFilePath))
                    {
                        OnErrorEvent("Peptide to protein mapping file was not created");
                        blnSuccess = false;
                    }
                    else
                    {
                        if (m_DebugLevel >= 2)
                        {
                            OnStatusEvent("Peptide to protein mapping complete");
                        }

                        blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors);
                    }
                }
                else
                {
                    if (mPeptideToProteinMapper.GetErrorMessage().Length == 0 && mPeptideToProteinMapper.StatusMessage.ToLower().Contains("error"))
                    {
                        OnErrorEvent("Error running clsPeptideToProteinMapEngine: " + mPeptideToProteinMapper.StatusMessage);
                    }
                    else
                    {
                        OnErrorEvent("Error running clsPeptideToProteinMapEngine: " + mPeptideToProteinMapper.GetErrorMessage());
                        if (mPeptideToProteinMapper.StatusMessage.Length > 0)
                        {
                            OnErrorEvent("clsPeptideToProteinMapEngine status: " + mPeptideToProteinMapper.StatusMessage);
                        }
                    }

                    if (blnIgnorePeptideToProteinMapperErrors)
                    {
                        OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");

                        if (File.Exists(strResultsFilePath))
                        {
                            blnSuccess = ValidatePeptideToProteinMapResults(strResultsFilePath, blnIgnorePeptideToProteinMapperErrors);
                        }
                        else
                        {
                            blnSuccess = true;
                        }
                    }
                    else
                    {
                        OnErrorEvent("Error in CreatePeptideToProteinMapping");
                        blnSuccess = false;
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in CreatePeptideToProteinMapping", ex);

                if (blnIgnorePeptideToProteinMapperErrors)
                {
                    OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
                else
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
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

                var fiTrimmedFasta = new FileInfo(Path.Combine(fiFastaFile.DirectoryName, Path.GetFileNameWithoutExtension(fiFastaFile.Name) + "_Trim" + maxFastaFileSizeMB + "MB.fasta"));

                if (fiTrimmedFasta.Exists)
                {
                    // Verify that the file matches the .hashcheck value
                    var hashcheckFilePath = fiTrimmedFasta.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;

                    var hashCheckError = string.Empty;
                    if (clsGlobal.ValidateFileVsHashcheck(fiTrimmedFasta.FullName, hashcheckFilePath, out hashCheckError))
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

                using (var srSourceFasta = new StreamReader(new FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var swTrimmedFasta = new StreamWriter(new FileStream(fiTrimmedFasta.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!srSourceFasta.EndOfStream)
                    {
                        var dataLine = srSourceFasta.ReadLine();

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

                        swTrimmedFasta.WriteLine(dataLine);
                        bytesWritten += dataLine.Length + 2;
                    }

                    // Add any missing contaminants
                    foreach (var protein in dctRequiredContaminants)
                    {
                        if (!protein.Value)
                        {
                            contaminantUtility.WriteProteinToFasta(swTrimmedFasta, protein.Key);
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

        public void DeleteFileInWorkDir(string strFilename)
        {
            try
            {
                var fiFile = new FileInfo(Path.Combine(m_WorkDir, strFilename));

                if (fiFile.Exists)
                {
                    fiFile.Delete();
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }
        }

        /// Read the original fasta file to create a decoy fasta file
        /// <summary>
        /// Creates a decoy version of the fasta file specified by strInputFilePath
        /// This new file will include the original proteins plus reversed versions of the original proteins
        /// Protein names will be prepended with REV_ or XXX_
        /// </summary>
        /// <param name="strInputFilePath">Fasta file to process</param>
        /// <param name="strOutputDirectoryPath">Output folder to create decoy file in</param>
        /// <returns>Full path to the decoy fasta file</returns>
        /// <remarks></remarks>
        private string GenerateDecoyFastaFile(string strInputFilePath, string strOutputDirectoryPath)
        {
            const char PROTEIN_LINE_START_CHAR = '>';
            const char PROTEIN_LINE_ACCESSION_END_CHAR = ' ';

            string strDecoyFastaFilePath = null;

            bool blnInputProteinFound = false;
            string strPrefix = null;

            try
            {
                var ioSourceFile = new FileInfo(strInputFilePath);
                if (!ioSourceFile.Exists)
                {
                    mErrorMessage = "Fasta file not found: " + ioSourceFile.FullName;
                    return string.Empty;
                }

                strDecoyFastaFilePath = Path.Combine(strOutputDirectoryPath, Path.GetFileNameWithoutExtension(ioSourceFile.Name) + "_decoy.fasta");

                if (m_DebugLevel >= 2)
                {
                    OnStatusEvent("Creating decoy fasta file at " + strDecoyFastaFilePath);
                }

                var objFastaFileReader = new ProteinFileReader.FastaFileReader
                {
                    ProteinLineStartChar = PROTEIN_LINE_START_CHAR,
                    ProteinLineAccessionEndChar = PROTEIN_LINE_ACCESSION_END_CHAR
                };

                if (!objFastaFileReader.OpenFile(strInputFilePath))
                {
                    OnErrorEvent("Error reading fasta file with ProteinFileReader to create decoy file");
                    return string.Empty;
                }

                if (mMSGFPlus)
                {
                    strPrefix = "XXX_";
                }
                else
                {
                    strPrefix = "REV_";
                }

                using (var swProteinOutputFile = new StreamWriter(new FileStream(strDecoyFastaFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    do
                    {
                        blnInputProteinFound = objFastaFileReader.ReadNextProteinEntry();

                        if (blnInputProteinFound)
                        {
                            // Write the forward protein
                            swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR + objFastaFileReader.ProteinName + " " +
                                                          objFastaFileReader.ProteinDescription);
                            WriteProteinSequence(swProteinOutputFile, objFastaFileReader.ProteinSequence);

                            // Write the decoy protein
                            swProteinOutputFile.WriteLine(PROTEIN_LINE_START_CHAR + strPrefix + objFastaFileReader.ProteinName + " " +
                                                          objFastaFileReader.ProteinDescription);
                            WriteProteinSequence(swProteinOutputFile, ReverseString(objFastaFileReader.ProteinSequence));
                        }
                    } while (blnInputProteinFound);
                }

                objFastaFileReader.CloseFile();
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception creating decoy fasta file", ex);
                return string.Empty;
            }

            return strDecoyFastaFilePath;
        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns>The number of cores on this computer</returns>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        public int GetCoreCount()
        {
            return PRISM.Processes.clsProgRunner.GetCoreCount();
        }

        private Dictionary<string, string> GetMSFGDBParameterNames()
        {
            // Keys are the parameter name in the MS-GF+ parameter file
            // Values are the command line switch name
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.CurrentCultureIgnoreCase);

            dctParamNames.Add("PMTolerance", "t");
            dctParamNames.Add(MSGFPLUS_OPTION_TDA, "tda");
            dctParamNames.Add(MSGFPLUS_OPTION_SHOWDECOY, "showDecoy");

            // This setting is nearly always set to 0 since we create a _ScanType.txt file that specifies the type of each scan
            // (thus, the value in the parameter file is ignored); the exception, when it is UVPD (mode 4)
            dctParamNames.Add(MSGFPLUS_OPTION_FRAGMENTATION_METHOD, "m");

            // This setting is auto-updated based on the instrument class for this dataset,
            // plus also the scan types listed In the _ScanType.txt file
            // (thus, the value in the parameter file Is typically ignored)
            dctParamNames.Add(MSGFPLUS_OPTION_INSTRUMENT_ID, "inst");

            dctParamNames.Add("EnzymeID", "e");
            dctParamNames.Add("C13", "c13");                 // Used by MS-GFDB
            dctParamNames.Add("IsotopeError", "ti");         // Used by MS-GF+
            dctParamNames.Add("NNET", "nnet");               // Used by MS-GFDB
            dctParamNames.Add("NTT", "ntt");                 // Used by MS-GF+
            dctParamNames.Add("minLength", "minLength");
            dctParamNames.Add("maxLength", "maxLength");
            dctParamNames.Add("minCharge", "minCharge");     // Only used if the spectrum file doesn't have charge information
            dctParamNames.Add("maxCharge", "maxCharge");     // Only used if the spectrum file doesn't have charge information
            dctParamNames.Add("NumMatchesPerSpec", "n");
            dctParamNames.Add("minNumPeaks", "minNumPeaks"); // Auto-added by this code if not defined
            dctParamNames.Add("Protocol", "protocol");
            dctParamNames.Add("ChargeCarrierMass", "ccm");

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
            return GetSearchEngineName(mMSGFPlus);
        }

        public static string GetSearchEngineName(bool blnMSGFPlus)
        {
            if (blnMSGFPlus)
            {
                return "MS-GF+";
            }
            else
            {
                return "MS-GFDB";
            }
        }

        public string GetSettingFromMSGFDbParamFile(string strParameterFilePath, string strSettingToFind)
        {
            return GetSettingFromMSGFDbParamFile(strParameterFilePath, strSettingToFind, string.Empty);
        }

        public string GetSettingFromMSGFDbParamFile(string strParameterFilePath, string strSettingToFind, string strValueIfNotFound)
        {
            string strLineIn = null;

            if (!File.Exists(strParameterFilePath))
            {
                OnErrorEvent("Parameter file not found: " + strParameterFilePath);
                return strValueIfNotFound;
            }

            try
            {
                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        strLineIn = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(strLineIn);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key) && clsGlobal.IsMatch(kvSetting.Key, strSettingToFind))
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

            return strValueIfNotFound;
        }

        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfDbProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy, out string fastaFilePath)
        {
            var udtHPCOptions = new clsAnalysisResources.udtHPCOptionsType();

            return InitializeFastaFile(javaProgLoc, msgfDbProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath, string.Empty, udtHPCOptions);
        }

        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfDbProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string strMSGFDBParameterFilePath, clsAnalysisResources.udtHPCOptionsType udtHPCOptions)
        {
            return InitializeFastaFile(javaProgLoc, msgfDbProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath,
                strMSGFDBParameterFilePath, udtHPCOptions, 0);
        }

        public CloseOutType InitializeFastaFile(string javaProgLoc, string msgfDbProgLoc, out float fastaFileSizeKB, out bool fastaFileIsDecoy,
            out string fastaFilePath, string strMSGFDBParameterFilePath, clsAnalysisResources.udtHPCOptionsType udtHPCOptions, int maxFastaFileSizeMB)
        {
            var oRand = new Random();

            var strMgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager");
            var sPICHPCUsername = m_mgrParams.GetParam("PICHPCUser", "");
            var sPICHPCPassword = m_mgrParams.GetParam("PICHPCPassword", "");

            var objIndexedDBCreator = new clsCreateMSGFDBSuffixArrayFiles(strMgrName, sPICHPCUsername, sPICHPCPassword);
            RegisterEvents(objIndexedDBCreator);

            // Define the path to the fasta file
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            if (udtHPCOptions.UsingHPC)
            {
                // Override the OrgDB folder to point to Picfs, specifically \\winhpcfs\projects\DMS\DMS_Temp_Org
                localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org");
            }
            fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

            fastaFileSizeKB = 0;
            fastaFileIsDecoy = false;

            var fiFastaFile = new FileInfo(fastaFilePath);

            if (!fiFastaFile.Exists)
            {
                // Fasta file not found
                OnErrorEvent("Fasta file not found: " + fiFastaFile.FullName);
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            fastaFileSizeKB = Convert.ToSingle(fiFastaFile.Length / 1024.0);

            var strProteinOptions = m_jobParams.GetParam("ProteinOptions");

            if (string.IsNullOrEmpty(strProteinOptions) || strProteinOptions == "na")
            {
                // Determine the fraction of the proteins that start with Reversed_ or XXX_ or XXX.
                var decoyPrefixes = clsAnalysisResources.GetDefaultDecoyPrefixes();
                foreach (var decoyPrefix in decoyPrefixes)
                {
                    int proteinCount = 0;
                    var fractionDecoy = clsAnalysisResources.GetDecoyFastaCompositionStats(fiFastaFile, decoyPrefix, out proteinCount);
                    if (fractionDecoy >= 0.25)
                    {
                        fastaFileIsDecoy = true;
                        break;
                    }
                }
            }
            else
            {
                if (strProteinOptions.ToLower().Contains("seq_direction=decoy"))
                {
                    fastaFileIsDecoy = true;
                }
            }

            if (!string.IsNullOrEmpty(strMSGFDBParameterFilePath))
            {
                string strTDASetting = null;
                strTDASetting = GetSettingFromMSGFDbParamFile(strMSGFDBParameterFilePath, "TDA");

                int tdaValue = 0;
                if (!int.TryParse(strTDASetting, out tdaValue))
                {
                    OnErrorEvent("TDA value is not numeric: " + strTDASetting);
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
                    else if (strMSGFDBParameterFilePath.ToLower().EndsWith("_NoDecoy.txt".ToLower()))
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
                fastaFileSizeKB = Convert.ToSingle(fiFastaFile.Length / 1024.0);
            }

            if (m_DebugLevel >= 3 || (m_DebugLevel >= 1 & fastaFileSizeKB > 500))
            {
                OnStatusEvent("Indexing Fasta file to create Suffix Array files");
            }

            // Look for the suffix array files that should exist for the fasta file
            // Either copy them from Gigasax (or Proto-7) or re-create them
            //
            var indexIteration = 0;
            var strMSGFPlusIndexFilesFolderPath = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPath", "\\\\gigasax\\MSGFPlus_Index_Files");
            var strMSGFPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", "\\\\proto-7\\MSGFPlus_Index_Files");

            while (indexIteration <= 2)
            {
                indexIteration += 1;

                // Note that fastaFilePath will get updated by the IndexedDBCreator if we're running Legacy MSGFDB
                var result = objIndexedDBCreator.CreateSuffixArrayFiles(m_WorkDir, m_DebugLevel, m_JobNum, javaProgLoc, msgfDbProgLoc, fastaFilePath,
                    fastaFileIsDecoy, strMSGFPlusIndexFilesFolderPath, strMSGFPlusIndexFilesFolderPathLegacyDB, udtHPCOptions);

                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    break;
                }
                else if (result == CloseOutType.CLOSEOUT_FAILED || (result != CloseOutType.CLOSEOUT_FAILED & indexIteration > 2))
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
                else
                {
                    var sleepTimeSec = oRand.Next(10, 19);

                    OnStatusEvent("Fasta file indexing failed; waiting " + sleepTimeSec + " seconds then trying again");
                    Thread.Sleep(sleepTimeSec * 1000);
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Reads the contents of a _ScanType.txt file, returning the scan info using three generic dictionary objects
        /// </summary>
        /// <param name="strScanTypeFilePath"></param>
        /// <param name="lstLowResMSn">Low Res MSn spectra</param>
        /// <param name="lstHighResMSn">High Res MSn spectra (but not HCD)</param>
        /// <param name="lstHCDMSn">HCD Spectra</param>
        /// <param name="lstOther">Spectra that are not MSn</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool LoadScanTypeFile(string strScanTypeFilePath, out Dictionary<int, string> lstLowResMSn, out Dictionary<int, string> lstHighResMSn,
            out Dictionary<int, string> lstHCDMSn, out Dictionary<int, string> lstOther)
        {
            string strLineIn = null;
            var intScanNumberColIndex = -1;
            var intScanTypeNameColIndex = -1;

            lstLowResMSn = new Dictionary<int, string>();
            lstHighResMSn = new Dictionary<int, string>();
            lstHCDMSn = new Dictionary<int, string>();
            lstOther = new Dictionary<int, string>();

            try
            {
                if (!File.Exists(strScanTypeFilePath))
                {
                    mErrorMessage = "ScanType file not found: " + strScanTypeFilePath;
                    return false;
                }

                using (var srScanTypeFile = new StreamReader(new FileStream(strScanTypeFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srScanTypeFile.EndOfStream)
                    {
                        strLineIn = srScanTypeFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            var lstColumns = strLineIn.Split('\t').ToList();

                            if (intScanNumberColIndex < 0)
                            {
                                // Parse the header line to define the mapping
                                // Expected headers are ScanNumber   ScanTypeName   ScanType
                                intScanNumberColIndex = lstColumns.IndexOf("ScanNumber");
                                intScanTypeNameColIndex = lstColumns.IndexOf("ScanTypeName");
                            }
                            else if (intScanNumberColIndex >= 0)
                            {
                                int intScanNumber = 0;
                                string strScanType = null;
                                string strScanTypeLCase = null;

                                if (int.TryParse(lstColumns[intScanNumberColIndex], out intScanNumber))
                                {
                                    if (intScanTypeNameColIndex >= 0)
                                    {
                                        strScanType = lstColumns[intScanTypeNameColIndex];
                                        strScanTypeLCase = strScanType.ToLower();

                                        if (strScanTypeLCase.Contains("hcd"))
                                        {
                                            lstHCDMSn.Add(intScanNumber, strScanType);
                                        }
                                        else if (strScanTypeLCase.Contains("hmsn"))
                                        {
                                            lstHighResMSn.Add(intScanNumber, strScanType);
                                        }
                                        else if (strScanTypeLCase.Contains("msn"))
                                        {
                                            // Not HCD and doesn't contain HMSn; assume low-res
                                            lstLowResMSn.Add(intScanNumber, strScanType);
                                        }
                                        else if (strScanTypeLCase.Contains("cid") || strScanTypeLCase.Contains("etd"))
                                        {
                                            // The ScanTypeName likely came from the "Collision Mode" column of a MASIC ScanStatsEx file; we don't know if it is high res MSn or low res MSn
                                            // This will be the case for MASIC results from prior to February 1, 2010, since those results did not have the ScanTypeName column in the _ScanStats.txt file
                                            // We'll assume low res
                                            lstLowResMSn.Add(intScanNumber, strScanType);
                                        }
                                        else
                                        {
                                            // Does not contain MSn or HCD
                                            // Likely SRM or MS1
                                            lstOther.Add(intScanNumber, strScanType);
                                        }
                                    }
                                }
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

        private bool MisleadingModDef(string definitionData, string definitionDataClean, string definitionType, string expectedTag, string invalidTag)
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
        /// Parse the MSGFPlus console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        /// <returns>Percent Complete (value between 0 and 100)</returns>
        /// <remarks>MSGFPlus version is available via the MSGFDbVersion property</remarks>
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
        private Regex reExtractThreadCount = new Regex(@"Using (?<ThreadCount>\d+) threads", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex reExtractTaskCount = new Regex(@"Splitting work into +(?<TaskCount>\d+) +tasks", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex reSpectraSearched = new Regex(@"Spectrum.+\(total: *(?<SpectrumCount>\d+)\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex reTaskComplete = new Regex(@"pool-\d+-thread-\d+: Task +(?<TaskNumber>\d+) +completed", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex rePercentComplete = new Regex(@"Search progress: (?<TasksComplete>\d+) / \d+ tasks?, (?<PercentComplete>[0-9.]+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSGFPlus console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        /// <returns>Percent Complete (value between 0 and 96)</returns>
        /// <remarks>MSGFPlus version is available via the MSGFDbVersion property</remarks>
        public float ParseMSGFPlusConsoleOutputFile(string workingDirectory)
        {
            var strConsoleOutputFilePath = "??";

            float sngEffectiveProgress = 0;
            float percentCompleteAllTasks = 0;
            var tasksCompleteViaSearchProgress = 0;

            try
            {
                strConsoleOutputFilePath = Path.Combine(workingDirectory, MSGFPLUS_CONSOLE_OUTPUT_FILE);
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        OnStatusEvent("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return 0;
                }

                if (m_DebugLevel >= 4)
                {
                    OnStatusEvent("Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                int intLinesRead = 0;

                // This is the total threads that MS-GF+ reports that it is using
                short totalThreadCount = 0;

                var totalTasks = 0;

                // List of completed task numbers
                var completedTasks = new SortedSet<int>();

                mConsoleOutputErrorMsg = string.Empty;

                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_STARTING;
                mContinuumSpectraSkipped = 0;
                mSpectraSearched = 0;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var strLineInLcase = strLineIn.ToLower();

                        if (intLinesRead <= 3)
                        {
                            // Originally the first line was the MS-GF+ version
                            // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                            // The third line is the MS-GF+ version
                            if (string.IsNullOrWhiteSpace(mMSGFPlusVersion) && (strLineIn.StartsWith("MS-GF+ Release")))
                            {
                                if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSGFPlusVersion))
                                {
                                    OnStatusEvent("MS-GF+ version: " + strLineIn);
                                }

                                mMSGFPlusVersion = string.Copy(strLineIn);
                            }
                            else
                            {
                                if (strLineInLcase.Contains("error"))
                                {
                                    if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                    {
                                        mConsoleOutputErrorMsg = "Error running MS-GF+: ";
                                    }
                                    if (!mConsoleOutputErrorMsg.Contains(strLineIn))
                                    {
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }
                        }

                        // Look for warning messages
                        // Additionally, update progress if the line starts with one of the expected phrases
                        if (strLineIn.StartsWith("Ignoring spectrum"))
                        {
                            // Spectra are typically ignored either because they have too few ions, or because the data is not centroided
                            if (strLineIn.IndexOf("spectrum is not centroided", StringComparison.CurrentCultureIgnoreCase) > 0)
                            {
                                mContinuumSpectraSkipped += 1;
                            }
                        }
                        else if (strLineIn.StartsWith("Loading database files"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_LOADING_DATABASE;
                            }
                        }
                        else if (strLineIn.StartsWith("Reading spectra"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_READING_SPECTRA)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_READING_SPECTRA;
                            }
                        }
                        else if (strLineIn.StartsWith("Using"))
                        {
                            // Extract out the thread or task count
                            var oThreadMatch = reExtractThreadCount.Match(strLineIn);

                            if (oThreadMatch.Success)
                            {
                                short.TryParse(oThreadMatch.Groups["ThreadCount"].Value, out totalThreadCount);

                                if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED)
                                {
                                    sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_THREADS_SPAWNED;
                                }
                            }
                        }
                        else if (strLineIn.StartsWith("Splitting"))
                        {
                            var oTaskMatch = reExtractTaskCount.Match(strLineIn);

                            if (oTaskMatch.Success)
                            {
                                int.TryParse(oTaskMatch.Groups["TaskCount"].Value, out totalTasks);
                            }
                        }
                        else if (strLineIn.StartsWith("Spectrum"))
                        {
                            // Extract out the number of spectra that MS-GF+ will actually search

                            var oMatch = reSpectraSearched.Match(strLineIn);

                            if (oMatch.Success)
                            {
                                int.TryParse(oMatch.Groups["SpectrumCount"].Value, out mSpectraSearched);
                            }
                        }
                        else if (strLineIn.StartsWith("Computing EFDRs") || strLineIn.StartsWith("Computing q-values"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPUTING_FDRS;
                            }
                        }
                        else if (strLineIn.StartsWith("MS-GF+ complete") || strLineIn.StartsWith("MS-GF+ complete"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_MSGFPLUS_COMPLETE)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_MSGFPLUS_COMPLETE;
                            }
                        }
                        else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            if (strLineInLcase.Contains("error") & !strLineInLcase.Contains("isotopeerror:"))
                            {
                                mConsoleOutputErrorMsg += "; " + strLineIn;
                            }
                        }

                        Match reMatch = reTaskComplete.Match(strLineIn);
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

                        var reProgressMatch = rePercentComplete.Match(strLineIn);
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
                if (mTaskCountCompleted == 0 & tasksCompleteViaSearchProgress > 0)
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
                    OnWarningEvent("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

            return sngEffectiveProgress;
        }

        /// <summary>
        /// Parses the static modifications, dynamic modifications, and custom amino acid information to create the MS-GF+ Mods file
        /// </summary>
        /// <param name="strParameterFilePath">Full path to the MSGF parameter file; will create file MSGFPlus_Mods.txt in the same folder</param>
        /// <param name="sbOptions">String builder of command line arguments to pass to MS-GF+</param>
        /// <param name="intNumMods">Max Number of Modifications per peptide</param>
        /// <param name="lstStaticMods">List of Static Mods</param>
        /// <param name="lstDynamicMods">List of Dynamic Mods</param>
        /// <param name="lstCustomAminoAcids">List of Custom Amino Acids</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ParseMSGFDBModifications(string strParameterFilePath, StringBuilder sbOptions, int intNumMods,
            IReadOnlyCollection<string> lstStaticMods, IReadOnlyCollection<string> lstDynamicMods, IReadOnlyCollection<string> lstCustomAminoAcids)
        {
            bool blnSuccess = false;
            string strModFilePath = null;

            try
            {
                var fiParameterFile = new FileInfo(strParameterFilePath);

                strModFilePath = Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME);

                // Note that ParseMSGFDbValidateMod will set this to True if a dynamic or static mod is STY phosphorylation
                mPhosphorylationSearch = false;

                sbOptions.Append(" -mod " + MOD_FILE_NAME);

                using (var swModFile = new StreamWriter(new FileStream(strModFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swModFile.WriteLine("# This file is used to specify modifications for MS-GF+");
                    swModFile.WriteLine();
                    swModFile.WriteLine("# Max Number of Modifications per peptide");
                    swModFile.WriteLine("# If this value is large, the search will be slow");
                    swModFile.WriteLine("NumMods=" + intNumMods);

                    if (lstCustomAminoAcids.Count > 0)
                    {
                        // Custom Amino Acid definitions need to be listed before static or dynamic modifications
                        swModFile.WriteLine();
                        swModFile.WriteLine("# Custom Amino Acids");

                        foreach (var strCustomAADef in lstCustomAminoAcids)
                        {
                            var strCustomAADefClean = string.Empty;

                            if (ParseMSGFDbValidateMod(strCustomAADef, out strCustomAADefClean))
                            {
                                if (MisleadingModDef(strCustomAADef, strCustomAADefClean, "Custom AA", "custom", "opt"))
                                    return false;
                                if (MisleadingModDef(strCustomAADef, strCustomAADefClean, "Custom AA", "custom", "fix"))
                                    return false;
                                swModFile.WriteLine(strCustomAADefClean);
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
                        foreach (var strStaticMod in lstStaticMods)
                        {
                            var strModClean = string.Empty;

                            if (ParseMSGFDbValidateMod(strStaticMod, out strModClean))
                            {
                                if (MisleadingModDef(strStaticMod, strModClean, "Static mod", "fix", "opt"))
                                    return false;
                                if (MisleadingModDef(strStaticMod, strModClean, "Static mod", "fix", "custom"))
                                    return false;
                                swModFile.WriteLine(strModClean);
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
                        foreach (var strDynamicMod in lstDynamicMods)
                        {
                            var strModClean = string.Empty;

                            if (ParseMSGFDbValidateMod(strDynamicMod, out strModClean))
                            {
                                if (MisleadingModDef(strDynamicMod, strModClean, "Dynamic mod", "opt", "fix"))
                                    return false;
                                if (MisleadingModDef(strDynamicMod, strModClean, "Dynamic mod", "opt", "custom"))
                                    return false;
                                swModFile.WriteLine(strModClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception creating MS-GF+ Mods file";
                OnErrorEvent(mErrorMessage, ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Read the MSGFDB options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="strScanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="strInstrumentGroup">DMS Instrument Group name</param>
        /// <param name="strMSGFDbCmdLineOptions">Output: MSGFDb command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSGFPlusParameterFile(float fastaFileSizeKB, bool fastaFileIsDecoy, string strAssumedScanType,
            string strScanTypeFilePath, string strInstrumentGroup, clsAnalysisResources.udtHPCOptionsType udtHPCOptions,
            out string strMSGFDbCmdLineOptions)
        {
            var strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));

            return ParseMSGFPlusParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup,
                strParameterFilePath, udtHPCOptions, out strMSGFDbCmdLineOptions);
        }

        /// <summary>
        /// Read the MS-GF+ options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="strScanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="strParameterFilePath">Full path to the MS-GF+ parameter file to use</param>
        /// <param name="strMSGFDbCmdLineOptions">Output: MS-GF+ command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSGFPlusParameterFile(float fastaFileSizeKB, bool fastaFileIsDecoy, string strAssumedScanType,
            string strScanTypeFilePath, string instrumentGroup, string strParameterFilePath, clsAnalysisResources.udtHPCOptionsType udtHPCOptions,
            out string strMSGFDbCmdLineOptions)
        {
            var overrideParams = new Dictionary<string, string>();

            return ParseMSGFPlusParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, instrumentGroup,
                strParameterFilePath, udtHPCOptions, overrideParams, out strMSGFDbCmdLineOptions);
        }

        /// <summary>
        /// Read the MS-GF+ options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileSizeKB">Size of the .Fasta file, in KB</param>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="strAssumedScanType">Empty string if no assumed scan type; otherwise CID, ETD, or HCD</param>
        /// <param name="strScanTypeFilePath">The path to the ScanType file (which lists the scan type for each scan); should be empty string if no ScanType file</param>
        /// <param name="instrumentGroup">DMS Instrument Group name</param>
        /// <param name="strParameterFilePath">Full path to the MS-GF+ parameter file to use</param>
        /// <param name="overrideParams">Parameters to override settings in the MS-GF+ parameter file</param>
        /// <param name="strMSGFDbCmdLineOptions">Output: MS-GF+ command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSGFPlusParameterFile(float fastaFileSizeKB, bool fastaFileIsDecoy, string strAssumedScanType,
            string strScanTypeFilePath, string instrumentGroup, string strParameterFilePath, clsAnalysisResources.udtHPCOptionsType udtHPCOptions,
            Dictionary<string, string> overrideParams, out string strMSGFDbCmdLineOptions)
        {
            const int SMALL_FASTA_FILE_THRESHOLD_KB = 20;

            string strLineIn = null;

            int intValue = 0;

            var intParamFileThreadCount = 0;
            string strDMSDefinedThreadCount = null;
            var intDMSDefinedThreadCount = 0;

            var intNumMods = 0;
            var lstStaticMods = new List<string>();
            var lstDynamicMods = new List<string>();
            var lstCustomAminoAcids = new List<string>();

            var blnShowDecoyParamPresent = false;
            var blnShowDecoy = false;
            var blnTDA = false;

            string strSearchEngineName = null;

            strMSGFDbCmdLineOptions = string.Empty;

            if (!File.Exists(strParameterFilePath))
            {
                OnErrorEvent("Parameter file Not found:  " + strParameterFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            //Dim strDatasetType As String
            //Dim blnHCD As Boolean = False
            //strDatasetType = m_jobParams.GetParam("JobParameters", "DatasetType")
            //If strDatasetType.ToUpper().Contains("HCD") Then
            //	blnHCD = True
            //End If

            strSearchEngineName = GetSearchEngineName();

            var sbOptions = new StringBuilder(500);

            // This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
            // Alternatively, if running MS-GF+, this is set to true if TDA=1
            mResultsIncludeAutoAddedDecoyPeptides = false;

            try
            {
                // Initialize the Param Name dictionary
                var dctParamNames = GetMSFGDBParameterNames();

                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        strLineIn = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(strLineIn);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key))
                        {
                            var strValue = kvSetting.Value;

                            var strArgumentSwitch = string.Empty;
                            string strArgumentSwitchOriginal = null;

                            // Check whether kvSetting.key is one of the standard keys defined in dctParamNames
                            if (dctParamNames.TryGetValue(kvSetting.Key, out strArgumentSwitch))
                            {
                                if (clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_FRAGMENTATION_METHOD))
                                {
                                    if (string.IsNullOrWhiteSpace(strValue) && !string.IsNullOrWhiteSpace(strScanTypeFilePath))
                                    {
                                        // No setting for FragmentationMethodID, and a ScanType file was created
                                        // Use FragmentationMethodID 0 (as written in the spectrum, or CID)
                                        strValue = "0";

                                        OnStatusEvent("Using Fragmentation method -m " + strValue + " because a ScanType file was created");
                                    }
                                    else if (!string.IsNullOrWhiteSpace(strAssumedScanType))
                                    {
                                        // Override FragmentationMethodID using strAssumedScanType
                                        // AssumedScanType is an optional job setting; see for example:
                                        //  IonTrapDefSettings_AssumeHCD.xml with <item key="AssumedScanType" value="HCD"/>
                                        switch (strAssumedScanType.ToUpper())
                                        {
                                            case "CID":
                                                strValue = "1";
                                                break;
                                            case "ETD":
                                                strValue = "2";
                                                break;
                                            case "HCD":
                                                strValue = "3";
                                                break;
                                            case "UVPD":
                                                // Previously, with MS-GFDB, fragmentationType 4 meant Merge ETD and CID
                                                // Now with MS-GF+, fragmentationType 4 means UVPD
                                                strValue = "4";
                                                break;
                                            default:
                                                // Invalid string
                                                mErrorMessage = "Invalid assumed scan type '" + strAssumedScanType +
                                                                "'; must be CID, ETD, HCD, or UVPD";
                                                OnErrorEvent(mErrorMessage);
                                                return CloseOutType.CLOSEOUT_FAILED;
                                        }

                                        OnStatusEvent("Using Fragmentation method -m " + strValue + " because of Assumed scan type " + strAssumedScanType);
                                    }
                                    else
                                    {
                                        OnStatusEvent("Using Fragmentation method -m " + strValue);
                                    }
                                }
                                else if (clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_INSTRUMENT_ID))
                                {
                                    if (!string.IsNullOrWhiteSpace(strScanTypeFilePath))
                                    {
                                        var eResult = DetermineInstrumentID(ref strValue, strScanTypeFilePath, instrumentGroup);
                                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                                        {
                                            return eResult;
                                        }
                                    }
                                    else if (!string.IsNullOrWhiteSpace(instrumentGroup))
                                    {
                                        var instrumentIDNew = string.Empty;
                                        var autoSwitchReason = string.Empty;

                                        if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out instrumentIDNew, out autoSwitchReason))
                                        {
                                            var datasetName = m_jobParams.GetParam("JobParameters", "DatasetNum");
                                            int countLowResMSn = 0;
                                            int countHighResMSn = 0;
                                            int countHCDMSn = 0;

                                            if (LookupScanTypesForDataset(datasetName, out countLowResMSn, out countHighResMSn, out countHCDMSn))
                                            {
                                                ExamineScanTypes(countLowResMSn, countHighResMSn, countHCDMSn, out instrumentIDNew, out autoSwitchReason);
                                            }
                                        }

                                        AutoUpdateInstrumentIDIfChanged(ref strValue, instrumentIDNew, autoSwitchReason);
                                    }
                                }

                                strArgumentSwitchOriginal = string.Copy(strArgumentSwitch);

                                AdjustSwitchesForMSGFPlus(mMSGFPlus, ref strArgumentSwitch, ref strValue);

                                var valueOverride = string.Empty;
                                if (overrideParams.TryGetValue(strArgumentSwitch, out valueOverride))
                                {
                                    OnStatusEvent("Overriding switch " + strArgumentSwitch + " to use -" + strArgumentSwitch + " " + valueOverride +
                                                  " instead of -" + strArgumentSwitch + " " + strValue);
                                    strValue = string.Copy(valueOverride);
                                }

                                if (string.IsNullOrEmpty(strArgumentSwitch))
                                {
                                    if (m_DebugLevel >= 1 & !clsGlobal.IsMatch(strArgumentSwitchOriginal, MSGFPLUS_OPTION_SHOWDECOY))
                                    {
                                        OnWarningEvent("Skipping switch " + strArgumentSwitchOriginal + " since it is not valid for this version of " + strSearchEngineName);
                                    }
                                }
                                else if (string.IsNullOrEmpty(strValue))
                                {
                                    if (m_DebugLevel >= 1)
                                    {
                                        OnWarningEvent("Skipping switch " + strArgumentSwitch + " since the value is empty");
                                    }
                                }
                                else
                                {
                                    sbOptions.Append(" -" + strArgumentSwitch + " " + strValue);
                                }

                                if (clsGlobal.IsMatch(strArgumentSwitch, "showDecoy"))
                                {
                                    blnShowDecoyParamPresent = true;
                                    if (int.TryParse(strValue, out intValue))
                                    {
                                        if (intValue > 0)
                                        {
                                            blnShowDecoy = true;
                                        }
                                    }
                                }
                                else if (clsGlobal.IsMatch(strArgumentSwitch, "tda"))
                                {
                                    if (int.TryParse(strValue, out intValue))
                                    {
                                        if (intValue > 0)
                                        {
                                            blnTDA = true;
                                        }
                                    }
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "uniformAAProb"))
                            {
                                if (mMSGFPlus)
                                {
                                    // Not valid for MS-GF+; skip it
                                }
                                else
                                {
                                    if (string.IsNullOrWhiteSpace(strValue) || clsGlobal.IsMatch(strValue, "auto"))
                                    {
                                        if (fastaFileSizeKB < SMALL_FASTA_FILE_THRESHOLD_KB)
                                        {
                                            sbOptions.Append(" -uniformAAProb 1");
                                        }
                                        else
                                        {
                                            sbOptions.Append(" -uniformAAProb 0");
                                        }
                                    }
                                    else
                                    {
                                        if (int.TryParse(strValue, out intValue))
                                        {
                                            sbOptions.Append(" -uniformAAProb " + intValue);
                                        }
                                        else
                                        {
                                            mErrorMessage = "Invalid value for uniformAAProb in MS-GF+ parameter file";
                                            OnErrorEvent(mErrorMessage + ": " + strLineIn);
                                            srParamFile.Close();
                                            return CloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "NumThreads"))
                            {
                                if (string.IsNullOrWhiteSpace(strValue) || clsGlobal.IsMatch(strValue, "all"))
                                {
                                    // Do not append -thread to the command line; MS-GF+ will use all available cores by default
                                }
                                else
                                {
                                    if (int.TryParse(strValue, out intParamFileThreadCount))
                                    {
                                        // intParamFileThreadCount now has the thread count
                                    }
                                    else
                                    {
                                        OnWarningEvent("Invalid value for NumThreads in MS-GF+ parameter file: " + strLineIn);
                                    }
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "NumMods"))
                            {
                                if (int.TryParse(strValue, out intValue))
                                {
                                    intNumMods = intValue;
                                }
                                else
                                {
                                    mErrorMessage = "Invalid value for NumMods in MS-GF+ parameter file";
                                    OnErrorEvent(mErrorMessage + ": " + strLineIn);
                                    srParamFile.Close();
                                    return CloseOutType.CLOSEOUT_FAILED;
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "StaticMod"))
                            {
                                if (!string.IsNullOrWhiteSpace(strValue) && !clsGlobal.IsMatch(strValue, "none"))
                                {
                                    lstStaticMods.Add(strValue);
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "DynamicMod"))
                            {
                                if (!string.IsNullOrWhiteSpace(strValue) && !clsGlobal.IsMatch(strValue, "none"))
                                {
                                    lstDynamicMods.Add(strValue);
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "CustomAA"))
                            {
                                if (!string.IsNullOrWhiteSpace(strValue) && !clsGlobal.IsMatch(strValue, "none"))
                                {
                                    lstCustomAminoAcids.Add(strValue);
                                }
                            }

                            //If clsGlobal.IsMatch(kvSetting.Key, MSGFPLUS_OPTION_FRAGMENTATION_METHOD) Then
                            //	If Integer.TryParse(strValue, out intValue) Then
                            //		If intValue = 3 Then
                            //			blnHCD = True
                            //		End If
                            //	End If
                            //End If
                        }
                    }
                }

                if (blnTDA)
                {
                    if (mMSGFPlus)
                    {
                        // Parameter file contains TDA=1 and we're running MS-GF+
                        mResultsIncludeAutoAddedDecoyPeptides = true;
                    }
                    else if (blnShowDecoy)
                    {
                        // Parameter file contains both TDA=1 and showDecoy=1
                        mResultsIncludeAutoAddedDecoyPeptides = true;
                    }
                }

                if (!blnShowDecoyParamPresent & !mMSGFPlus)
                {
                    // Add showDecoy to sbOptions
                    sbOptions.Append(" -showDecoy 0");
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception reading MS-GF+ parameter file";
                OnErrorEvent(mErrorMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Define the thread count; note that MSGFDBThreads could be "all"
            strDMSDefinedThreadCount = m_jobParams.GetJobParameter("MSGFDBThreads", string.Empty);
            if (string.IsNullOrWhiteSpace(strDMSDefinedThreadCount) || strDMSDefinedThreadCount.ToLower() == "all" ||
                !int.TryParse(strDMSDefinedThreadCount, out intDMSDefinedThreadCount))
            {
                intDMSDefinedThreadCount = 0;
            }

            if (intDMSDefinedThreadCount > 0)
            {
                intParamFileThreadCount = intDMSDefinedThreadCount;
            }

            var limitCoreUsage = false;

            if (Dns.GetHostName().ToLower().StartsWith("proto-"))
            {
                // Running on a Proto storage server (e.g. Proto-4, Proto-5, or Proto-11)
                // Limit the number of cores used to 75% of the total core count
                limitCoreUsage = true;
            }

            if (udtHPCOptions.UsingHPC)
            {
                // Do not define the thread count when running on HPC; MS-GF+ should use all 16 cores (or all 32 cores)
                if (intParamFileThreadCount > 0)
                    intParamFileThreadCount = 0;

                OnStatusEvent("Running on HPC; " + strSearchEngineName + " will use all available cores");
            }
            else if (intParamFileThreadCount <= 0 || limitCoreUsage)
            {
                // Set intParamFileThreadCount to the number of cores on this computer
                // However, do not exceed 8 cores because this can actually slow down MS-GF+ due to context switching
                // Furthermore, Java will restrict all of the threads to a single NUMA node, and we don't want too many threads on a single node

                var coreCount = GetCoreCount();

                if (limitCoreUsage)
                {
                    var maxAllowedCores = Convert.ToInt32(Math.Floor(coreCount * 0.75));
                    if (intParamFileThreadCount > 0 && intParamFileThreadCount < maxAllowedCores)
                    {
                        // Leave intParamFileThreadCount unchanged
                    }
                    else
                    {
                        intParamFileThreadCount = maxAllowedCores;
                    }
                }
                else
                {
                    // Prior to July 2014 we would use "coreCount - 1" when the computer had more than 4 cores because MS-GF+ would actually use intParamFileThreadCount+1 cores
                    // Starting with version v10072, MS-GF+ actually uses all the cores, so we started using intParamFileThreadCount = coreCount

                    // Then, in April 2015, we started running two copies of MS-GF+ simultaneously on machines with > 4 cores because even if we tell MS-GF+ to use all the cores, we saw a lot of idle time
                    // When two simultaneous copies of MS-GF+ are running the CPUs get a bit overtaxed, so we're now using this logic:

                    if (coreCount > 4)
                    {
                        intParamFileThreadCount = coreCount - 1;
                    }
                    else
                    {
                        intParamFileThreadCount = coreCount;
                    }
                }

                if (intParamFileThreadCount > 8)
                {
                    OnStatusEvent("The system has " + coreCount + " cores; " + strSearchEngineName + " will use 8 cores (bumped down from " +
                                  intParamFileThreadCount + " to avoid overloading a single NUMA node)");
                    intParamFileThreadCount = 8;
                }
                else
                {
                    // Example message: The system has 8 cores; MS-GF+ will use 7 cores")
                    OnStatusEvent("The system has " + coreCount + " cores; " + strSearchEngineName + " will use " + intParamFileThreadCount + " cores");
                }
            }

            if (intParamFileThreadCount > 0)
            {
                sbOptions.Append(" -thread " + intParamFileThreadCount);
            }

            // Create the modification file and append the -mod switch
            // We'll also set mPhosphorylationSearch to True if a dynamic or static mod is STY phosphorylation
            if (!ParseMSGFDBModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods, lstCustomAminoAcids))
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

            strMSGFDbCmdLineOptions = sbOptions.ToString();

            // By default, MS-GF+ filters out spectra with fewer than 20 data points
            // Override this threshold to 5 data points
            if (strMSGFDbCmdLineOptions.IndexOf("-minNumPeaks", StringComparison.CurrentCultureIgnoreCase) < 0)
            {
                strMSGFDbCmdLineOptions += " -minNumPeaks 5";
            }

            // Auto-add the "addFeatures" switch if not present
            // This is required to post-process the results with Percolator
            if (mMSGFPlus && strMSGFDbCmdLineOptions.IndexOf("-addFeatures", StringComparison.CurrentCultureIgnoreCase) < 0)
            {
                strMSGFDbCmdLineOptions += " -addFeatures 1";
            }

            if (strMSGFDbCmdLineOptions.Contains("-tda 1"))
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

            var instrumentIDNew = string.Empty;
            var autoSwitchReason = string.Empty;

            if (!CanDetermineInstIdFromInstGroup(instrumentGroup, out instrumentIDNew, out autoSwitchReason))
            {
                // Instrument ID is not obvious from the instrument group
                // Examine the scan types in scanTypeFilePath

                // If low res MS1,  then Instrument Group is typically LCQ, LTQ, LTQ-ETD, LTQ-Prep, VelosPro

                // If high res MS2, then Instrument Group is typically VelosOrbi, or LTQ_FT

                // Count the number of High res CID or ETD spectra
                // Count HCD spectra separately since MS-GF+ has a special scoring model for HCD spectra

                Dictionary<int, string> lstLowResMSn = null;
                Dictionary<int, string> lstHighResMSn = null;
                Dictionary<int, string> lstHCDMSn = null;
                Dictionary<int, string> lstOther = null;
                bool blnSuccess = false;

                blnSuccess = LoadScanTypeFile(scanTypeFilePath, out lstLowResMSn, out lstHighResMSn, out lstHCDMSn, out lstOther);

                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(mErrorMessage))
                    {
                        mErrorMessage = "LoadScanTypeFile returned false for " + Path.GetFileName(scanTypeFilePath);
                        OnErrorEvent(mErrorMessage);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else if (lstLowResMSn.Count + lstHighResMSn.Count + lstHCDMSn.Count == 0)
                {
                    mErrorMessage = "LoadScanTypeFile could not find any MSn spectra " + Path.GetFileName(scanTypeFilePath);
                    OnErrorEvent(mErrorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    ExamineScanTypes(lstLowResMSn.Count, lstHighResMSn.Count, lstHCDMSn.Count, out instrumentIDNew, out autoSwitchReason);
                }
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
                    dblFractionHiRes = countHighResMSn / (countLowResMSn + countHighResMSn);
                }

                if (dblFractionHiRes > 0.1)
                {
                    // At least 10% of the spectra are HMSn
                    instrumentIDNew = "1";
                    autoSwitchReason = "since " + (dblFractionHiRes * 100).ToString("0") + "% of the spectra are HMSn";
                }
                else
                {
                    if (countLowResMSn == 0 & countHCDMSn > 0)
                    {
                        // All of the spectra are HCD
                        instrumentIDNew = "1";
                        autoSwitchReason = "since all of the spectra are HCD";
                    }
                    else
                    {
                        instrumentIDNew = "0";
                        if (countHCDMSn == 0 & countHighResMSn == 0)
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

                DataTable dtResults = null;
                const int retryCount = 2;

                //Get a table to hold the results of the query
                var blnSuccess = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LookupScanTypesForDataset", retryCount, out dtResults);

                if (!blnSuccess)
                {
                    OnErrorEvent("Excessive failures attempting to retrieve dataset scan types in LookupScanTypesForDataset");
                    dtResults.Dispose();
                    return false;
                }

                //Verify at least one row returned
                if (dtResults.Rows.Count < 1)
                {
                    // No data was returned
                    OnStatusEvent("No rows were returned for dataset " + datasetName + " from V_Dataset_ScanType_CrossTab in DMS");
                    return false;
                }
                else
                {
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
        /// <param name="strMod">Modification definition</param>
        /// <param name="strModClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        private bool ParseMSGFDbValidateMod(string strMod, out string strModClean)
        {
            int intPoundIndex = 0;
            string[] strSplitMod = null;

            var strComment = string.Empty;

            strModClean = string.Empty;

            intPoundIndex = strMod.IndexOf('#');
            if (intPoundIndex > 0)
            {
                strComment = strMod.Substring(intPoundIndex);
                strMod = strMod.Substring(0, intPoundIndex - 1).Trim();
            }

            // Split on commas and remove whitespace
            strSplitMod = strMod.Split(',');
            for (var i = 0; i <= strSplitMod.Length - 1; i++)
            {
                strSplitMod[i] = strSplitMod[i].Trim();
            }

            // Check whether this is a custom AA definition
            var query = (from item in strSplitMod where item.ToLower() == "custom" select item).ToList();
            var customAminoAcidDef = query.Count > 0;

            if (strSplitMod.Length < 5)
            {
                // Invalid definition

                if (customAminoAcidDef)
                {
                    // Invalid custom AA definition; must have 5 sections, for example:
                    // C5H7N1O2S0,J,custom,P,Hydroxylation     # Hydroxyproline
                    mErrorMessage = "Invalid custom AA string; must have 5 sections: " + strMod;
                }
                else
                {
                    // Invalid dynamic or static mod definition; must have 5 sections, for example:
                    // O1, M, opt, any, Oxidation
                    mErrorMessage = "Invalid modification string; must have 5 sections: " + strMod;
                }

                OnErrorEvent(mErrorMessage);
                return false;
            }

            // Reconstruct the mod (or custom AA) definition, making sure there is no whitespace
            strModClean = string.Copy(strSplitMod[0]);

            if (customAminoAcidDef)
            {
                // Make sure that the custom amino acid definition does not have any invalid characters
                var reInvalidCharacters = new Regex(@"[^CHNOS0-9]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
                var lstInvalidCharacters = reInvalidCharacters.Matches(strModClean);

                if (lstInvalidCharacters.Count > 0)
                {
                    mErrorMessage = "Custom amino acid empirical formula " + strModClean + " has invalid characters. " +
                                    "It must only contain C, H, N, O, and S, and optionally an integer after each element, for example: C6H7N3O";
                    OnErrorEvent(mErrorMessage);
                    return false;
                }

                // Make sure that all of the elements in strModClean have a number after them
                // For example, auto-change C6H7N3O to C6H7N3O1

                var reElementSplitter = new Regex(@"(?<Atom>[A-Z])(?<Count>\d*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

                var lstElements = reElementSplitter.Matches(strModClean);
                var reconstructedFormula = string.Empty;

                foreach (Match subPart in lstElements)
                {
                    var elementSymbol = subPart.Groups["Atom"].ToString();
                    var elementCount = subPart.Groups["Count"].ToString();

                    if (elementSymbol != "C" && elementSymbol != "H" && elementSymbol != "N" && elementSymbol != "O" && elementSymbol != "S")
                    {
                        mErrorMessage = "Invalid element " + elementSymbol + " in the custom amino acid empirical formula " + strModClean + "; " +
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

                if (!string.Equals(strModClean, reconstructedFormula))
                {
                    OnStatusEvent("Auto updated the custom amino acid empirical formula to include a 1 " +
                                  "after elements that did not have an element count listed: " + strModClean + " --> " + reconstructedFormula);
                    strModClean = reconstructedFormula;
                }
            }

            for (var intIndex = 1; intIndex <= strSplitMod.Length - 1; intIndex++)
            {
                strModClean += "," + strSplitMod[intIndex];
            }

            // Possibly append the comment (which will start with a # sign)
            if (!string.IsNullOrWhiteSpace(strComment))
            {
                strModClean += "     " + strComment;
            }

            // Check whether this is a phosphorylation mod
            if (!customAminoAcidDef)
            {
                if (strSplitMod[(int) eModDefinitionParts.Name].ToUpper().StartsWith("PHOSPH") ||
                    strSplitMod[(int) eModDefinitionParts.EmpiricalFormulaOrMass].ToUpper() == "HO3P")
                {
                    if (strSplitMod[(int) eModDefinitionParts.Residues].ToUpper().IndexOfAny(new char[]
                    {
                        'S',
                        'T',
                        'Y'
                    }) >= 0)
                    {
                        mPhosphorylationSearch = true;
                    }
                }
            }

            return true;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="sbOptions"></param>
        /// <param name="strKeyName"></param>
        /// <param name="strValue"></param>
        /// <param name="strParameterName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ParseMSFDBParamLine(StringBuilder sbOptions, string strKeyName, string strValue, string strParameterName)
        {
            var strCommandLineSwitchName = strParameterName;

            return ParseMSFDBParamLine(sbOptions, strKeyName, strValue, strParameterName, strCommandLineSwitchName);
        }

        private bool ParseMSFDBParamLine(StringBuilder sbOptions, string strKeyName, string strValue, string strParameterName, string strCommandLineSwitchName)
        {
            if (clsGlobal.IsMatch(strKeyName, strParameterName))
            {
                sbOptions.Append(" -" + strCommandLineSwitchName + " " + strValue);
                return true;
            }
            else
            {
                return false;
            }
        }

        private string ReverseString(string strText)
        {
            char[] chReversed = strText.ToCharArray();
            Array.Reverse(chReversed);
            return new string(chReversed);
        }

        public static bool UseLegacyMSGFDB(IJobParams jobParams)
        {
            return false;

            //Dim strValue As String

            //' Default to using MS-GF+
            //Dim blnUseLegacyMSGFDB As Boolean = False

            //strValue = jobParams.GetJobParameter("UseLegacyMSGFDB", String.Empty)
            //If Not String.IsNullOrEmpty(strValue) Then
            //	If Not Boolean.TryParse(strValue, out blnUseLegacyMSGFDB) Then
            //		' Error parsing strValue; not boolean
            //		strValue = String.Empty
            //	End If
            //End If

            //If String.IsNullOrEmpty(strValue) Then
            //	strValue = jobParams.GetJobParameter("UseMSGFPlus", String.Empty)

            //	If Not String.IsNullOrEmpty(strValue) Then
            //		Dim blnUseMSGFPlus As Boolean
            //		If Boolean.TryParse(strValue, out blnUseMSGFPlus) Then
            //			strValue = "False"
            //			blnUseLegacyMSGFDB = False
            //		Else
            //			strValue = String.Empty
            //		End If
            //	End If

            //	If String.IsNullOrEmpty(strValue) Then
            //		' Default to using MS-GF+
            //		blnUseLegacyMSGFDB = False
            //	End If
            //End If

            //Return blnUseLegacyMSGFDB
        }

        private bool ValidatePeptideToProteinMapResults(string strPeptideToProteinMapFilePath, bool blnIgnorePeptideToProteinMapperErrors)
        {
            const string PROTEIN_NAME_NO_MATCH = "__NoMatch__";

            bool blnSuccess = false;

            var intPeptideCount = 0;
            var intPeptideCountNoMatch = 0;
            var intLinesRead = 0;

            try
            {
                // Validate that none of the results in strPeptideToProteinMapFilePath has protein name PROTEIN_NAME_NO_MATCH

                string strLineIn = null;

                if (m_DebugLevel >= 2)
                {
                    OnStatusEvent("Validating peptide to protein mapping, file " + Path.GetFileName(strPeptideToProteinMapFilePath));
                }

                using (var srInFile = new StreamReader(new FileStream(strPeptideToProteinMapFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (intLinesRead > 1 && !string.IsNullOrEmpty(strLineIn))
                        {
                            intPeptideCount += 1;
                            if (strLineIn.Contains(PROTEIN_NAME_NO_MATCH))
                            {
                                intPeptideCountNoMatch += 1;
                            }
                        }
                    }
                }

                if (intPeptideCount == 0)
                {
                    mErrorMessage = "Peptide to protein mapping file is empty";
                    OnErrorEvent(mErrorMessage + ", file " + Path.GetFileName(strPeptideToProteinMapFilePath));
                    blnSuccess = false;
                }
                else if (intPeptideCountNoMatch == 0)
                {
                    if (m_DebugLevel >= 2)
                    {
                        OnStatusEvent("Peptide to protein mapping validation complete; processed " + intPeptideCount + " peptides");
                    }

                    blnSuccess = true;
                }
                else
                {
                    double dblErrorPercent = 0;    // Value between 0 and 100
                    dblErrorPercent = intPeptideCountNoMatch / intPeptideCount * 100.0;

                    mErrorMessage = dblErrorPercent.ToString("0.0") + "% of the entries in the peptide to protein map file did not match to a protein in the FASTA file";
                    OnErrorEvent(mErrorMessage);

                    if (blnIgnorePeptideToProteinMapperErrors)
                    {
                        OnWarningEvent("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                        blnSuccess = true;
                    }
                    else
                    {
                        if (IgnorePreviousErrorEvent != null)
                        {
                            IgnorePreviousErrorEvent();
                        }
                        blnSuccess = false;
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error validating peptide to protein map file";
                OnErrorEvent(mErrorMessage, ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        private void WriteProteinSequence(StreamWriter swOutFile, string strSequence)
        {
            var intIndex = 0;
            int intLength = 0;

            while (intIndex < strSequence.Length)
            {
                intLength = Math.Min(60, strSequence.Length - intIndex);
                swOutFile.WriteLine(strSequence.Substring(intIndex, intLength));
                intIndex += 60;
            }
        }

        /// <summary>
        /// Zips MS-GF+ Output File (creating a .gz file)
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ZipOutputFile(clsAnalysisToolRunnerBase oToolRunner, string fileName)
        {
            string tmpFilePath = null;

            try
            {
                tmpFilePath = Path.Combine(m_WorkDir, fileName);
                if (!File.Exists(tmpFilePath))
                {
                    OnErrorEvent("MS-GF+ results file not found: " + fileName);
                    return CloseOutType.CLOSEOUT_NO_OUT_FILES;
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

        private void mPeptideToProteinMapper_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int MAPPER_PROGRESS_LOG_INTERVAL_SECONDS = 120;

            if (m_DebugLevel >= 1)
            {
                if (System.DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS)
                {
                    dtLastLogTime = System.DateTime.UtcNow;
                    OnStatusEvent("Mapping peptides to proteins: " + percentComplete.ToString("0.0") + "% complete");
                }
            }
        }

        #endregion
        
    }
}
