using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Class for running IDPicker
    /// </summary>
    public class clsAnalysisToolRunnerIDPicker : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        /// <summary>
        /// If True, always skip IDPicker
        /// </summary>
        public const bool ALWAYS_SKIP_IDPICKER = true;

        private const string PEPXML_CONSOLE_OUTPUT = "PepXML_ConsoleOutput.txt";

        private const string IPD_Qonvert_CONSOLE_OUTPUT = "IDPicker_Qonvert_ConsoleOutput.txt";
        private const string IPD_Assemble_CONSOLE_OUTPUT = "IDPicker_Assemble_ConsoleOutput.txt";
        private const string IPD_Report_CONSOLE_OUTPUT = "IDPicker_Report_ConsoleOutput.txt";

        private const string IDPicker_Qonvert = "idpQonvert.exe";
        private const string IDPicker_Assemble = "idpAssemble.exe";
        private const string IDPicker_Report = "idpReport.exe";
        private const string IDPicker_GUI = "IdPickerGui.exe";

        private const string ASSEMBLE_GROUPING_FILENAME = "Assemble.txt";
        private const string ASSEMBLE_OUTPUT_FILENAME = "IDPicker_AssembledResults.xml";

        private const string MSGFDB_DECOY_PROTEIN_PREFIX = "REV_";
        private const string MSGFPLUS_DECOY_PROTEIN_PREFIX = "XXX_";

        private const string PEPTIDE_LIST_TO_XML_EXE = "PeptideListToXML.exe";

        private const float PROGRESS_PCT_IDPicker_STARTING = 1;
        private const float PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES = 5;
        private const float PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE = 10;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert = 20;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble = 60;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPReport = 70;
        private const float PROGRESS_PCT_IDPicker_COMPLETE = 95;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private string mIDPickerProgramFolder = string.Empty;
        private string mIDPickerParamFileNameLocal = string.Empty;

        private string mPeptideListToXMLExePath = string.Empty;
        private string mPepXMLFilePath = string.Empty;
        private string mIdpXMLFilePath = string.Empty;
        private string mIdpAssembleFilePath = string.Empty;

        private Dictionary<string, string> mIDPickerOptions;

        // This variable holds the name of the program that is currently running via CmdRunner
        private string mCmdRunnerDescription = string.Empty;

        // This list tracks the error messages reported by CmdRunner
        private ConcurrentBag<string> mCmdRunnerErrors;

        // This list tracks error message text that we look for when considering whether or not to ignore an error message
        private ConcurrentBag<string> mCmdRunnerErrorsToIgnore;

        // This list tracks files that we want to include in the zipped up IDPicker report folder
        private List<string> mFilenamesToAddToReportFolder;

        private bool mBatchFilesMoved;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs PepXML converter and IDPicker tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            // As of January 21, 2015 we are now always skipping IDPicker (and thus simply creating the .pepXML file)
            var skipIDPicker = ALWAYS_SKIP_IDPICKER;

            var processingSuccess = true;

            mIDPickerOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mCmdRunnerErrors = new ConcurrentBag<string>();
            mCmdRunnerErrorsToIgnore = new ConcurrentBag<string>();
            mFilenamesToAddToReportFolder = new List<string>();

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerIDPicker.RunTool(): Enter");
                }

                m_progress = PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES;

                // Determine the path to the IDPicker program (idpQonvert); folder will also contain idpAssemble.exe and idpReport.exe
                var progLocQonvert = string.Empty;

                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                if (!skipIDPicker)
                {
                    progLocQonvert = DetermineProgramLocation("IDPickerProgLoc", IDPicker_Qonvert);

                    if (string.IsNullOrWhiteSpace(progLocQonvert))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Determine the result type
                var strResultType = m_jobParams.GetParam("ResultType");

                var ePHRPResultType = clsPHRPReader.GetPeptideHitResultType(strResultType);
                if (ePHRPResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
                {
                    m_message = "Invalid tool result type (not supported by IDPicker): " + strResultType;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Define the path to the synopsis file
                var strSynFilePath = Path.Combine(m_WorkDir, clsPHRPReader.GetPHRPSynopsisFileName(ePHRPResultType, m_Dataset));
                if (!File.Exists(strSynFilePath))
                {
                    var alternateFilePath = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynFilePath, "Dataset_msgfdb.txt");
                    if (File.Exists(alternateFilePath))
                    {
                        strSynFilePath = alternateFilePath;
                    }
                }

                if (!clsAnalysisResources.ValidateFileHasData(strSynFilePath, "Synopsis file", out var strErrorMessage))
                {
                    // The synopsis file is empty
                    m_message = strErrorMessage;
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                // Define the path to the fasta file
                var orgDbDir = m_mgrParams.GetParam("orgdbdir");
                var strFASTAFilePath = Path.Combine(orgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

                var fiFastaFile = new FileInfo(strFASTAFilePath);

                if (!skipIDPicker && !fiFastaFile.Exists)
                {
                    // Fasta file not found
                    m_message = "Fasta file not found: " + fiFastaFile.Name;
                    LogError("Fasta file not found: " + fiFastaFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var splitFasta = m_jobParams.GetJobParameter("SplitFasta", false);

                if (!skipIDPicker && splitFasta)
                {
                    skipIDPicker = true;
                    LogWarning("SplitFasta jobs typically have fasta files too large for IDPQonvert; skipping IDPicker", true);
                }

                // Store the version of IDPicker and PeptideListToXML in the database
                // Alternatively, if skipIDPicker is true, just store the version of PeptideListToXML

                // This function updates mPeptideListToXMLExePath and mIDPickerProgramFolder
                if (!StoreToolVersionInfo(progLocQonvert, skipIDPicker))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining IDPicker version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the PepXML file
                var pepXmlSuccess = CreatePepXMLFile(fiFastaFile.FullName, strSynFilePath, ePHRPResultType);
                if (!pepXmlSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error creating PepXML file";
                    }
                    LogError("Error creating PepXML file for job " + m_JobNum);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (skipIDPicker)
                {
                    // Don't keep this file since we're skipping IDPicker
                    m_jobParams.AddResultFileToSkip("Tool_Version_Info_IDPicker.txt");

                    var strParamFileNameLocal = m_jobParams.GetParam(clsAnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL);
                    if (string.IsNullOrEmpty(strParamFileNameLocal))
                    {
                        m_jobParams.AddResultFileToSkip(clsAnalysisResourcesIDPicker.DEFAULT_IDPICKER_PARAM_FILE_NAME);
                    }
                    else
                    {
                        m_jobParams.AddResultFileToSkip(strParamFileNameLocal);
                    }
                }
                else
                {
                    var idPickerSuccess = RunIDPickerWrapper(ePHRPResultType, strSynFilePath, fiFastaFile.FullName, out var processingError, out var blnCriticalError);

                    if (blnCriticalError)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!idPickerSuccess || processingError)
                        processingSuccess = false;
                }

                if (processingSuccess)
                {
                    // Zip the PepXML file
                    var zipSuccess = ZipPepXMLFile();
                    if (!zipSuccess)
                        processingSuccess = false;
                }

                m_jobParams.AddResultFileExtensionToSkip(".bat");

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, return CloseOutType.CLOSEOUT_FAILED

                    m_jobParams.RemoveResultFileToSkip(ASSEMBLE_GROUPING_FILENAME);
                    m_jobParams.RemoveResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME);

                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var folderCreated = MakeResultsFolder();
                if (!folderCreated)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!skipIDPicker)
                {
                    var moveResult = MoveFilesIntoIDPickerSubfolder();
                    if (moveResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                        m_message = "Error moving files into IDPicker subfolder";
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // ReSharper restore ConditionIsAlwaysTrueOrFalse

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private bool RunIDPickerWrapper(clsPHRPReader.ePeptideHitResultType ePHRPResultType, string strSynFilePath, string fastaFilePath,
            out bool blnProcessingError, out bool blnCriticalError)
        {
            bool blnSuccess;
            blnProcessingError = false;
            blnCriticalError = false;

            // Determine the prefix used by decoy proteins
            var strDecoyPrefix = string.Empty;

            if (ePHRPResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                // If we run MSGF+ with target/decoy mode and showDecoy=1, the _syn.txt file will have decoy proteins that start with REV_ or XXX_
                // Check for this
                blnSuccess = LookForDecoyProteinsInMSGFPlusResults(strSynFilePath, ePHRPResultType, ref strDecoyPrefix);
                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error looking for decoy proteins in the MSGF+ synopsis file";
                    }
                    blnCriticalError = true;
                    return false;
                }
            }

            if (string.IsNullOrEmpty(strDecoyPrefix))
            {
                // Look for decoy proteins in the Fasta file
                blnSuccess = DetermineDecoyProteinPrefix(fastaFilePath, out strDecoyPrefix);
                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error looking for decoy proteins in the Fasta file";
                    }
                    blnCriticalError = true;
                    return false;
                }
            }

            if (string.IsNullOrEmpty(strDecoyPrefix))
            {
                LogWarning("No decoy proteins; skipping IDPicker", true);
                return true;
            }

            // Load the IDPicker options
            blnSuccess = LoadIDPickerOptions();
            if (!blnSuccess)
            {
                blnProcessingError = true;
                return false;
            }

            // Convert the search scores in the pepXML file to q-values
            blnSuccess = RunQonvert(fastaFilePath, strDecoyPrefix, ePHRPResultType);
            if (!blnSuccess)
            {
                blnProcessingError = true;
                return false;
            }

            // Organizes the search results into a hierarchy
            blnSuccess = RunAssemble();
            if (!blnSuccess)
            {
                blnProcessingError = true;
                return false;
            }

            // Apply parsimony in protein assembly and generate reports
            blnSuccess = RunReport();
            if (!blnSuccess)
            {
                blnProcessingError = true;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Append a new command line argument (appends using ValueIfMissing if not defined in mIDPickerOptions)
        /// </summary>
        /// <param name="CmdArgs">Current arguments</param>
        /// <param name="ArgumentName">Argument Name</param>
        /// <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <returns>The new argument list</returns>
        /// <remarks></remarks>
        private string AppendArgument(string CmdArgs, string ArgumentName, string ValueIfMissing)
        {
            const bool AppendIfMissing = true;
            return AppendArgument(CmdArgs, ArgumentName, ArgumentName, ValueIfMissing, AppendIfMissing);
        }

        /// <summary>
        /// Append a new command line argument (appends using ValueIfMissing if not defined in mIDPickerOptions)
        /// </summary>
        /// <param name="CmdArgs">Current arguments</param>
        /// <param name="OptionName">Key name to lookup in mIDPickerOptions</param>
        /// <param name="ArgumentName">Argument Name</param>
        /// <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <returns>The new argument list</returns>
        /// <remarks></remarks>
        private string AppendArgument(string CmdArgs, string OptionName, string ArgumentName, string ValueIfMissing)
        {
            const bool AppendIfMissing = true;
            return AppendArgument(CmdArgs, OptionName, ArgumentName, ValueIfMissing, AppendIfMissing);
        }

        /// <summary>
        /// Append a new command line argument
        /// </summary>
        /// <param name="CmdArgs">Current arguments</param>
        /// <param name="OptionName">Key name to lookup in mIDPickerOptions</param>
        /// <param name="ArgumentName">Argument Name</param>
        /// <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <param name="AppendIfMissing">If True, append the argument using ValueIfMissing if not found in mIDPickerOptions; if false, and not found, does not append the argument</param>
        /// <returns>The new argument list</returns>
        /// <remarks></remarks>
        private string AppendArgument(string CmdArgs, string OptionName, string ArgumentName, string ValueIfMissing, bool AppendIfMissing)
        {
            bool blnIsMissing;
            bool blnAppendParam;

            if (mIDPickerOptions.TryGetValue(OptionName, out var strValue))
            {
                blnIsMissing = false;
            }
            else
            {
                blnIsMissing = true;
                strValue = ValueIfMissing;
            }

            if (blnIsMissing)
            {
                blnAppendParam = AppendIfMissing;
            }
            else
            {
                blnAppendParam = true;
            }

            if (string.IsNullOrEmpty(CmdArgs))
            {
                CmdArgs = string.Empty;
            }

            if (blnAppendParam)
            {
                return CmdArgs + " -" + ArgumentName + " " + PossiblyQuotePath(strValue);
            }

            return CmdArgs;
        }

        private bool CreateAssembleFile(string strAssembleFilePath)
        {
            try
            {
                // Prepend strExperiment with PNNL/
                // Also make sure it doesn't contain any spaces
                var strDatasetLabel = "PNNL/" + m_Dataset.Replace(" ", "_");

                // Create the Assemble.txt file
                using (var swOutfile = new StreamWriter(new FileStream(strAssembleFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutfile.WriteLine(strDatasetLabel + " " + Path.GetFileName(mIdpXMLFilePath));
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->CreateAssembleFile";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        private void ClearConcurrentBag(ref ConcurrentBag<string> oBag)
        {
            while (!oBag.IsEmpty)
            {
                string item;
                oBag.TryTake(out item);
            }
        }

        /// <summary>
        /// Copies a file into folder strReportFolderPath then adds it to m_jobParams.AddResultFileToSkip
        /// </summary>
        /// <param name="strFileName"></param>
        /// <param name="strReportFolderPath"></param>
        /// <remarks></remarks>
        private void CopyFileIntoReportFolder(string strFileName, string strReportFolderPath)
        {
            try
            {
                var ioSourceFile = new FileInfo(Path.Combine(m_WorkDir, strFileName));

                if (ioSourceFile.Exists)
                {
                    ioSourceFile.CopyTo(Path.Combine(strReportFolderPath, strFileName), true);
                    m_jobParams.AddResultFileToSkip(strFileName);
                }
            }
            catch (Exception ex)
            {
                LogWarning("Error copying ConsoleOutput file into the IDPicker Report folder: " + ex.Message);
            }
        }

        private bool CreatePepXMLFile(string strFastaFilePath, string strSynFilePath, clsPHRPReader.ePeptideHitResultType ePHRPResultType)
        {
            // PepXML file creation should generally be done in less than 10 minutes
            // However, for huge fasta files, conversion could take several hours
            const int intMaxRuntimeMinutes = 480;

            bool blnSuccess;

            try
            {
                // Set up and execute a program runner to run PeptideListToXML
                var strParamFileName = m_jobParams.GetParam("ParmFileName");

                mPepXMLFilePath = Path.Combine(m_WorkDir, m_Dataset + ".pepXML");
                var iHitsPerSpectrum = m_jobParams.GetJobParameter("PepXMLHitsPerSpectrum", 3);

                var cmdStr = PossiblyQuotePath(strSynFilePath) + " /E:" + PossiblyQuotePath(strParamFileName) + " /F:" +
                             PossiblyQuotePath(strFastaFilePath) + " /H:" + iHitsPerSpectrum;

                if (ePHRPResultType == clsPHRPReader.ePeptideHitResultType.MODa | ePHRPResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
                {
                    // The SpecProb values listed in the _syn_MSGF.txt file are not true spectral probabilities
                    // Instead, they're just 1 - Probability  (where Probability is a value between 0 and 1 assigned by MODa)
                    // Therefore, don't include them in the PepXML file
                    cmdStr += " /NoMSGF";
                }

                if (m_jobParams.GetJobParameter("PepXMLNoScanStats", false))
                {
                    cmdStr += " /NoScanStats";
                }

                ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);

                m_progress = PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE;

                blnSuccess = RunProgramWork("PeptideListToXML", mPeptideListToXMLExePath, cmdStr, PEPXML_CONSOLE_OUTPUT, false, intMaxRuntimeMinutes);

                if (blnSuccess)
                {
                    // Make sure the .pepXML file was created
                    if (!File.Exists(mPepXMLFilePath))
                    {
                        m_message = "Error creating PepXML file";
                        LogError(m_message + ", job " + m_JobNum);
                        blnSuccess = false;
                    }
                    else
                    {
                        m_jobParams.AddResultFileToSkip(PEPXML_CONSOLE_OUTPUT);
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->CreatePepXMLFile";
                LogError(m_message, ex);
                return false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Determine the prefix used by to denote decoy (reversed) proteins
        /// </summary>
        /// <param name="strFastaFilePath"></param>
        /// <param name="strDecoyPrefix"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        private bool DetermineDecoyProteinPrefix(string strFastaFilePath, out string strDecoyPrefix)
        {
            strDecoyPrefix = string.Empty;

            try
            {
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Looking for decoy proteins in the fasta file");
                }

                var lstReversedProteinPrefixes = new SortedSet<string>
                {
                    "reversed_",                                   // MTS reversed proteins                 // reversed[_]%'
                    "scrambled_",                                  // MTS scrambled proteins                // scrambled[_]%'
                    "xxx.",                                        // Inspect reversed/scrambled proteins   // xxx.%'
                    MSGFDB_DECOY_PROTEIN_PREFIX.ToLower(),         // MSGFDB reversed proteins              // rev[_]%'
                    MSGFPLUS_DECOY_PROTEIN_PREFIX.ToLower()        // MSGF+ reversed proteins               // xxx[_]%'
                };


                // Note that X!Tandem decoy proteins end with ":reversed"
                // IDPicker doesn't support decoy protein name suffixes, only prefixes

                var lstPrefixStats = new Dictionary<string, int>();

                var objFastaFileReader = new ProteinFileReader.FastaFileReader();

                if (!objFastaFileReader.OpenFile(strFastaFilePath))
                {
                    m_message = "Error reading fasta file with ProteinFileReader";
                    return false;
                }

                while (objFastaFileReader.ReadNextProteinEntry())
                {
                    var strProtein = objFastaFileReader.ProteinName;

                    foreach (var strPrefix in lstReversedProteinPrefixes)
                    {
                        if (strProtein.ToLower().StartsWith(strPrefix.ToLower()))
                        {
                            var strProteinPrefix = strProtein.Substring(0, strPrefix.Length);

                            if (lstPrefixStats.TryGetValue(strProteinPrefix, out var intCount))
                            {
                                lstPrefixStats[strProteinPrefix] = intCount + 1;
                            }
                            else
                            {
                                lstPrefixStats.Add(strProteinPrefix, 1);
                            }
                        }
                    }
                }

                objFastaFileReader.CloseFile();

                if (lstPrefixStats.Count == 1)
                {
                    strDecoyPrefix = lstPrefixStats.First().Key;
                }
                else if (lstPrefixStats.Count > 1)
                {
                    // Find the prefix (key) in lstPrefixStats with the highest occurrence count
                    var intMaxCount = -1;
                    foreach (var kvEntry in lstPrefixStats)
                    {
                        if (kvEntry.Value > intMaxCount)
                        {
                            intMaxCount = kvEntry.Value;
                            strDecoyPrefix = kvEntry.Key;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->DetermineDecoyProteinPrefix";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        private bool IgnoreError(string strErrorMessage)
        {
            var blnIgnore = false;

            foreach (var strIgnoreText in mCmdRunnerErrorsToIgnore)
            {
                if (strErrorMessage.Contains(strIgnoreText))
                {
                    blnIgnore = true;
                    break;
                }
            }

            return blnIgnore;
        }

        private bool LoadIDPickerOptions()
        {
            try
            {
                mIDPickerParamFileNameLocal = m_jobParams.GetParam(clsAnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL);
                if (string.IsNullOrEmpty(mIDPickerParamFileNameLocal))
                {
                    m_message = "IDPicker parameter file not defined";
                    return false;
                }

                var strParameterFilePath = Path.Combine(m_WorkDir, mIDPickerParamFileNameLocal);

                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        strLineIn = strLineIn.Trim();

                        if (strLineIn.StartsWith("#") || !strLineIn.Contains('='))
                        {
                            continue;
                        }

                        var strKey = string.Empty;
                        var strValue = string.Empty;

                        var intCharIndex = strLineIn.IndexOf('=');
                        if (intCharIndex > 0)
                        {
                            strKey = strLineIn.Substring(0, intCharIndex).Trim();
                            if (intCharIndex < strLineIn.Length - 1)
                            {
                                strValue = strLineIn.Substring(intCharIndex + 1).Trim();
                            }
                            else
                            {
                                strValue = string.Empty;
                            }
                        }

                        intCharIndex = strValue.IndexOf('#');
                        if (intCharIndex >= 0)
                        {
                            strValue = strValue.Substring(0, intCharIndex);
                        }

                        if (!string.IsNullOrWhiteSpace(strKey))
                        {
                            if (mIDPickerOptions.ContainsKey(strKey))
                            {
                                LogWarning("Ignoring duplicate parameter file option '" + strKey + "' in file " + mIDPickerParamFileNameLocal);
                            }
                            else
                            {
                                mIDPickerOptions.Add(strKey, strValue.Trim());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->LoadIDPickerOptions";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        private bool LookForDecoyProteinsInMSGFPlusResults(string strSynFilePath, clsPHRPReader.ePeptideHitResultType eResultType, ref string strDecoyPrefix)
        {
            try
            {
                strDecoyPrefix = string.Empty;
                var lstPrefixesToCheck = new List<string> {
                    MSGFDB_DECOY_PROTEIN_PREFIX.ToUpper(),
                    MSGFPLUS_DECOY_PROTEIN_PREFIX.ToUpper()
                };

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Looking for decoy proteins in the MSGF+ synopsis file");
                }

                using (var reader = new clsPHRPReader(strSynFilePath, eResultType, false, false, false))
                {
                    RegisterEvents(reader);

                    while (reader.MoveNext())
                    {
                        var found = false;
                        foreach (var strPrefixToCheck in lstPrefixesToCheck)
                        {
                            if (reader.CurrentPSM.ProteinFirst.ToUpper().StartsWith(strPrefixToCheck))
                            {
                                strDecoyPrefix = reader.CurrentPSM.ProteinFirst.Substring(0, strPrefixToCheck.Length);

                                if (m_DebugLevel >= 4)
                                {
                                    LogDebug("Decoy protein prefix found: " + strDecoyPrefix);
                                }

                                found = true;
                                break;
                            }
                        }
                        if (found)
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in IDPickerPlugin->LookForDecoyProteinsInMSGFPlusResults";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        private CloseOutType MoveFilesIntoIDPickerSubfolder()
        {
            var blnErrorEncountered = false;

            try
            {
                var ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName);

                var diSourceFolder = new DirectoryInfo(ResFolderNamePath);
                var diTargetFolder = diSourceFolder.CreateSubdirectory("IDPicker");

                var lstFileSpecs = new List<string>();
                var fiFilesToMove = new List<FileInfo>();

                lstFileSpecs.Add("*.idpXML");
                lstFileSpecs.Add("IDPicker*.*");
                lstFileSpecs.Add("Tool_Version_Info_IDPicker.txt");
                lstFileSpecs.Add(mIDPickerParamFileNameLocal);

                if (!mBatchFilesMoved)
                {
                    lstFileSpecs.Add("Run*.bat");
                }

                foreach (var strFileSpec in lstFileSpecs)
                {
                    fiFilesToMove.AddRange(diSourceFolder.GetFiles(strFileSpec));
                }

                foreach (var fiFile in fiFilesToMove)
                {
                    var intAttempts = 0;
                    var blnSuccess = false;

                    do
                    {
                        try
                        {
                            // Note that the file may have been moved already; confirm that it still exists
                            fiFile.Refresh();
                            if (fiFile.Exists)
                            {
                                fiFile.MoveTo(Path.Combine(diTargetFolder.FullName, fiFile.Name));
                            }
                            blnSuccess = true;
                        }
                        catch (Exception)
                        {
                            intAttempts += 1;
                            clsGlobal.IdleLoop(2);
                        }
                    } while (!blnSuccess && intAttempts <= 3);

                    if (!blnSuccess)
                    {
                        blnErrorEncountered = true;
                        LogError("Unable to move " + fiFile.Name + " into the IDPicker subfolder; tried " + (intAttempts - 1) + " times");
                    }
                }
            }
            catch (Exception)
            {
                blnErrorEncountered = true;
            }

            if (blnErrorEncountered)
            {
                // Try to save whatever files were moved into the results folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void ParseConsoleOutputFileForErrors(string strConsoleOutputFilePath)
        {
            var blnUnhandledException = false;
            var strExceptionText = string.Empty;

            try
            {
                if (File.Exists(strConsoleOutputFilePath))
                {
                    using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();

                            if (string.IsNullOrEmpty(strLineIn))
                                continue;

                            if (blnUnhandledException)
                            {
                                if (string.IsNullOrEmpty(strExceptionText))
                                {
                                    strExceptionText = string.Copy(strLineIn);
                                }
                                else
                                {
                                    strExceptionText = ";" + strLineIn;
                                }
                            }
                            else if (strLineIn.StartsWith("Error:"))
                            {
                                if (!IgnoreError(strLineIn))
                                {
                                    mCmdRunnerErrors.Add(strLineIn);
                                }
                            }
                            else if (strLineIn.StartsWith("Unhandled Exception"))
                            {
                                mCmdRunnerErrors.Add(strLineIn);
                                blnUnhandledException = true;
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(strExceptionText))
                    {
                        mCmdRunnerErrors.Add(strExceptionText);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseConsoleOutputFileForErrors: " + ex.Message);
            }
        }

        /// <summary>
        /// Run idpAssemble to organizes the search results into a hierarchy
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RunAssemble()
        {
            const int intMaxRuntimeMinutes = 90;

            // Create the Assemble.txt file
            // Since we're only processing one dataset, the file will only have one line
            var strAssembleFilePath = Path.Combine(m_WorkDir, ASSEMBLE_GROUPING_FILENAME);

            var blnSuccess = CreateAssembleFile(strAssembleFilePath);
            if (!blnSuccess)
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error running idpAssemble";
                }
                return false;
            }

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Assemble);

            // Build the command string, for example:
            //  Assemble.xml -MaxFDR 0.1 -b Assemble.txt

            var cmdStr = ASSEMBLE_OUTPUT_FILENAME;
            cmdStr = AppendArgument(cmdStr, "AssemblyMaxFDR", "MaxFDR", "0.1");
            cmdStr += " -b Assemble.txt -dump";

            m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble;

            blnSuccess = RunProgramWork("IDPAssemble", progLoc, cmdStr, IPD_Assemble_CONSOLE_OUTPUT, true, intMaxRuntimeMinutes);

            mIdpAssembleFilePath = Path.Combine(m_WorkDir, ASSEMBLE_OUTPUT_FILENAME);

            if (blnSuccess)
            {
                // Make sure the output file was created
                if (!File.Exists(mIdpAssembleFilePath))
                {
                    m_message = "IDPicker Assemble results file not found";
                    LogError(m_message + " at " + mIdpAssembleFilePath);
                    blnSuccess = false;
                }
                else
                {
                    // Do not keep the assemble input or output files
                    m_jobParams.AddResultFileToSkip(ASSEMBLE_GROUPING_FILENAME);
                    m_jobParams.AddResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME);
                }
            }

            return blnSuccess;
        }

        /// <summary>
        /// Run idpQonvert to convert the search scores in the pepXML file to q-values
        /// </summary>
        /// <param name="strFASTAFilePath"></param>
        /// <param name="strDecoyPrefix"></param>
        /// <param name="ePHRPResultType"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RunQonvert(string strFASTAFilePath, string strDecoyPrefix, clsPHRPReader.ePeptideHitResultType ePHRPResultType)
        {
            const int intMaxRuntimeMinutes = 90;

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("could not find the default configuration file");
            mCmdRunnerErrorsToIgnore.Add("could not find the default residue masses file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Qonvert);

            // Possibly override some options
            if (ePHRPResultType == clsPHRPReader.ePeptideHitResultType.MODa | ePHRPResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
            {
                // Higher MODa probability scores are better
                mIDPickerOptions["SearchScoreWeights"] = "Probability 1";
                mIDPickerOptions["NormalizedSearchScores"] = "Probability";
            }

            // Build the command string, for example:
            //   -MaxFDR 0.1 -ProteinDatabase c:\DMS_Temp_Org\ID_002339_125D2B84.fasta
            //   -SearchScoreWeights "msgfspecprob -1" -OptimizeScoreWeights 1
            //   -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_
            //   -dump QC_Shew_11_06_pt5_3_13Feb12_Doc_11-12-07.pepXML
            var cmdStr = string.Empty;

            cmdStr = AppendArgument(cmdStr, "QonvertMaxFDR", "MaxFDR", "0.1");
            cmdStr += " -ProteinDatabase " + PossiblyQuotePath(strFASTAFilePath);
            cmdStr = AppendArgument(cmdStr, "SearchScoreWeights", "msgfspecprob -1");
            cmdStr = AppendArgument(cmdStr, "OptimizeScoreWeights", "1");
            cmdStr = AppendArgument(cmdStr, "NormalizedSearchScores", "msgfspecprob");

            cmdStr += " -DecoyPrefix " + PossiblyQuotePath(strDecoyPrefix);
            cmdStr += " -dump";              // This tells IDPQonvert to display the processing options that the program is using
            cmdStr += " " + mPepXMLFilePath;

            m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert;

            var blnSuccess = RunProgramWork("IDPQonvert", progLoc, cmdStr, IPD_Qonvert_CONSOLE_OUTPUT, true, intMaxRuntimeMinutes);

            mIdpXMLFilePath = Path.Combine(m_WorkDir, m_Dataset + ".idpXML");

            if (blnSuccess)
            {
                // Make sure the output file was created
                if (!File.Exists(mIdpXMLFilePath))
                {
                    m_message = "IDPicker Qonvert results file not found";
                    LogError(m_message + " at " + mIdpXMLFilePath);
                    blnSuccess = false;
                }
            }

            return blnSuccess;
        }

        /// <summary>
        /// Run idpReport to apply parsimony in protein assembly and generate reports
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RunReport()
        {
            const int intMaxRuntimeMinutes = 60;

            var strOutputFolderName = "IDPicker";

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Report);

            // Build the command string, for example:
            //  report Assemble.xml -MaxFDR 0.05 -MinDistinctPeptides 2 -MinAdditionalPeptides 2 -ModsAreDistinctByDefault true -MaxAmbiguousIds 2 -MinSpectraPerProtein 2 -OutputTextReport true

            var cmdStr = strOutputFolderName + " " + mIdpAssembleFilePath;
            cmdStr = AppendArgument(cmdStr, "ReportMaxFDR", "MaxFDR", "0.05");
            cmdStr = AppendArgument(cmdStr, "MinDistinctPeptides", "2");
            cmdStr = AppendArgument(cmdStr, "MinAdditionalPeptides", "2");
            cmdStr = AppendArgument(cmdStr, "ModsAreDistinctByDefault", "true");
            cmdStr = AppendArgument(cmdStr, "MaxAmbiguousIds", "2");
            cmdStr = AppendArgument(cmdStr, "MinSpectraPerProtein", "2");

            cmdStr += " -OutputTextReport true -dump";

            m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPReport;

            var blnSuccess = RunProgramWork("IDPReport", progLoc, cmdStr, IPD_Report_CONSOLE_OUTPUT, true, intMaxRuntimeMinutes);

            if (blnSuccess)
            {
                var diReportFolder = new DirectoryInfo(Path.Combine(m_WorkDir, strOutputFolderName));

                // Make sure the output folder was created
                if (!diReportFolder.Exists)
                {
                    m_message = "IDPicker report folder file not found";
                    LogError(m_message + " at " + diReportFolder.FullName);
                    blnSuccess = false;
                }

                if (blnSuccess)
                {
                    var blnTSVFileFound = false;

                    // Move the .tsv files from the Report folder up one level
                    foreach (var fiFile in diReportFolder.GetFiles("*.tsv"))
                    {
                        fiFile.MoveTo(Path.Combine(m_WorkDir, fiFile.Name));
                        blnTSVFileFound = true;
                    }

                    if (!blnTSVFileFound)
                    {
                        m_message = "IDPicker report folder does not contain any TSV files";
                        LogError(m_message + "; " + diReportFolder.FullName);
                        blnSuccess = false;
                    }
                }

                if (blnSuccess)
                {
                    // Copy the ConsoleOutput and RunProgram batch files into the Report folder (and add them to the files to Skip)
                    // mFilenamesToAddToReportFolder will already contain the batch file names

                    mFilenamesToAddToReportFolder.Add(IPD_Qonvert_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportFolder.Add(IPD_Assemble_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportFolder.Add(IPD_Report_CONSOLE_OUTPUT);

                    foreach (var strFileName in mFilenamesToAddToReportFolder)
                    {
                        CopyFileIntoReportFolder(strFileName, diReportFolder.FullName);
                    }

                    mBatchFilesMoved = true;

                    // Zip the report folder
                    var strZippedResultsFilePath = Path.Combine(m_WorkDir, "IDPicker_HTML_Results.zip");
                    m_DotNetZipTools.DebugLevel = m_DebugLevel;
                    blnSuccess = m_DotNetZipTools.ZipDirectory(diReportFolder.FullName, strZippedResultsFilePath, true);

                    if (!blnSuccess && m_DotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
                    {
                        m_NeedToAbortProcessing = true;
                    }
                }
            }
            else
            {
                // Check whether mCmdRunnerErrors contains a known error message
                foreach (var strError in mCmdRunnerErrors)
                {
                    if (strError.Contains("no spectra in workspace"))
                    {
                        // All of the proteins were filtered out; we'll treat this as a successful completion of IDPicker
                        m_message = string.Empty;
                        m_EvalMessage = "IDPicker Report filtered out all of the proteins";
                        LogWarning(m_EvalMessage + "; this indicates there are not enough filter-passing peptides.");
                        blnSuccess = true;
                        break;
                    }
                }
            }

            return blnSuccess;
        }

        /// <summary>
        /// Run IDPicker
        /// </summary>
        /// <param name="strProgramDescription"></param>
        /// <param name="strExePath"></param>
        /// <param name="cmdStr"></param>
        /// <param name="strConsoleOutputFileName">If empty, does not create a console output file</param>
        /// <param name="blnCaptureConsoleOutputViaDosRedirection"></param>
        /// <param name="intMaxRuntimeMinutes"></param>
        /// <returns></returns>
        ///  <remarks></remarks>
        private bool RunProgramWork(string strProgramDescription, string strExePath, string cmdStr, string strConsoleOutputFileName,
            bool blnCaptureConsoleOutputViaDosRedirection, int intMaxRuntimeMinutes)
        {
            if (m_DebugLevel >= 1)
            {
                LogMessage(strExePath + " " + cmdStr.TrimStart(' '));
            }

            mCmdRunnerDescription = string.Copy(strProgramDescription);
            ClearConcurrentBag(ref mCmdRunnerErrors);

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.ErrorEvent += CmdRunner_ConsoleErrorEvent;
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
            cmdRunner.Timeout += CmdRunner_Timeout;

            if (blnCaptureConsoleOutputViaDosRedirection)
            {
                // Create a batch file to run the command
                // Capture the console output (including output to the error stream) via redirection symbols:
                //    strExePath cmdStr > ConsoleOutputFile.txt 2>&1

                var strExePathOriginal = string.Copy(strExePath);
                var CmdStrOriginal = string.Copy(cmdStr);

                strProgramDescription = strProgramDescription.Replace(" ", "_");

                var strBatchFileName = "Run_" + strProgramDescription + ".bat";
                mFilenamesToAddToReportFolder.Add(strBatchFileName);

                // Update the Exe path to point to the RunProgram batch file; update cmdStr to be empty
                strExePath = Path.Combine(m_WorkDir, strBatchFileName);
                cmdStr = string.Empty;

                if (string.IsNullOrEmpty(strConsoleOutputFileName))
                {
                    strConsoleOutputFileName = strProgramDescription + "_Console_Output.txt";
                }

                // Create the batch file
                using (var swBatchFile = new StreamWriter(new FileStream(strExePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swBatchFile.WriteLine(strExePathOriginal + " " + CmdStrOriginal + " > " + strConsoleOutputFileName + " 2>&1");
                }

            }

            if (blnCaptureConsoleOutputViaDosRedirection || string.IsNullOrEmpty(strConsoleOutputFileName))
            {
                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = false;
            }
            else
            {
                cmdRunner.CreateNoWindow = true;
                cmdRunner.EchoOutputToConsole = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, strConsoleOutputFileName);
            }

            var intMaxRuntimeSeconds = intMaxRuntimeMinutes * 60;

            var blnSuccess = cmdRunner.RunProgram(strExePath, cmdStr, strProgramDescription, true, intMaxRuntimeSeconds);

            if (mCmdRunnerErrors.Count == 0 && !string.IsNullOrEmpty(cmdRunner.CachedConsoleError))
            {
                LogWarning("Cached console error is not empty, but mCmdRunnerErrors is empty; need to add code to parse CmdRunner.CachedConsoleError");
            }

            if (blnCaptureConsoleOutputViaDosRedirection)
            {
                ParseConsoleOutputFileForErrors(Path.Combine(m_WorkDir, strConsoleOutputFileName));
            }
            else if (mCmdRunnerErrors.Count > 0)
            {
                // Append the error messages to the log
                // Note that ProgRunner will have already included them in the ConsoleOutput.txt file
                foreach (var strError in mCmdRunnerErrors)
                {
                    if (!strError.ToLower().StartsWith("warning"))
                    {
                        LogError("... " + strError);
                    }
                }
            }

            if (!blnSuccess)
            {
                m_message = "Error running " + strProgramDescription;
                if (mCmdRunnerErrors.Count > 0)
                {
                    m_message += ": " + mCmdRunnerErrors.First();
                }

                LogError(m_message);

                if (cmdRunner.ExitCode != 0)
                {
                    LogWarning(strProgramDescription + " returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to " + strProgramDescription + " failed (but exit code is 0)");
                }
            }
            else
            {
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug(strProgramDescription + " Complete");
                }
            }

            return blnSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strIDPickerProgLoc, bool blnSkipIDPicker)
        {
            var strToolVersionInfo = string.Empty;

            var blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // We will store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            // Determine the path to the PeptideListToXML.exe
            mPeptideListToXMLExePath = DetermineProgramLocation("PeptideListToXMLProgLoc", PEPTIDE_LIST_TO_XML_EXE);

            if (blnSkipIDPicker)
            {
                // Only store the version of PeptideListToXML.exe in the database
                blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, mPeptideListToXMLExePath);
                toolFiles.Add(new FileInfo(mPeptideListToXMLExePath));
            }
            else
            {
                var ioIDPicker = new FileInfo(strIDPickerProgLoc);
                if (!ioIDPicker.Exists)
                {
                    try
                    {
                        strToolVersionInfo = "Unknown";
                        return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>());
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                        return false;
                    }
                }

                mIDPickerProgramFolder = ioIDPicker.DirectoryName;

                if (ioIDPicker.Directory == null)
                {
                    LogError("Cannot determine the prent directory of " + ioIDPicker.FullName);
                    return false;
                }

                // Lookup the version of idpAssemble.exe (which is a .NET app; cannot use idpQonvert.exe since it is a C++ app)
                var strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Assemble);
                StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strExePath);
                toolFiles.Add(new FileInfo(strExePath));

                // Lookup the version of idpReport.exe
                strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Report);
                StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strExePath);
                toolFiles.Add(new FileInfo(strExePath));

                // Also include idpQonvert.exe in toolFiles (version determination does not work)
                strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Qonvert);
                toolFiles.Add(new FileInfo(strExePath));

                // Lookup the version of PeptideListToXML.exe
                StoreToolVersionInfoOneFile(ref strToolVersionInfo, mPeptideListToXMLExePath);
                toolFiles.Add(new FileInfo(mPeptideListToXMLExePath));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private bool ZipPepXMLFile()
        {
            try
            {
                var strZippedPepXMLFilePath = Path.Combine(m_WorkDir, m_Dataset + "_pepXML.zip");

                if (!ZipFile(mPepXMLFilePath, false, strZippedPepXMLFilePath))
                {
                    LogError("Error zipping PepXML file");
                    return false;
                }

                // Add the .pepXML file to .FilesToDelete since we only want to keep the Zipped version
                m_jobParams.AddResultFileToSkip(Path.GetFileName(mPepXMLFilePath));
            }
            catch (Exception ex)
            {
                LogError("Exception zipping PepXML output file", ex);
                return false;
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private void CmdRunner_ConsoleErrorEvent(string NewText, Exception ex)
        {
            if (mCmdRunnerErrors == null)
                return;

            // Split NewText on newline characters
            var chNewLineChars = new[] { '\r', '\n' };

            var strSplitLine = NewText.Split(chNewLineChars, StringSplitOptions.RemoveEmptyEntries);

            foreach (var strItem in strSplitLine)
            {
                var strItem2 = strItem.Trim(chNewLineChars);

                if (!string.IsNullOrEmpty(strItem2))
                {
                    // Confirm that strItem does not contain any text in mCmdRunnerErrorsToIgnore
                    if (!IgnoreError(strItem2))
                    {
                        mCmdRunnerErrors.Add(strItem2);
                    }
                }
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("IDPicker");
        }

        private void CmdRunner_Timeout()
        {
            if (m_DebugLevel >= 2)
            {
                LogError("Aborted " + mCmdRunnerDescription);
            }
        }

        #endregion
    }
}
