//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Class for running TopPIC analysis
    /// </summary>
    public class clsAnalysisToolRunnerTopPIC : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const string TopPIC_CONSOLE_OUTPUT = "TopPIC_ConsoleOutput.txt";
        private const string TopPIC_EXE_NAME = "toppic.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string PRSM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2.OUTPUT_TABLE";
        private const string PRSM_RESULT_TABLE_NAME_SUFFIX_FINAL = "_TopPIC_PrSMs.txt";

        private const string PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2.FORM_OUTPUT_TABLE";
        private const string PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_FINAL = "_TopPIC_Proteoforms.txt";

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mTopPICVersion;

        private string mTopPICProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mValidatedFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs TopPIC tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerTopPIC.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to TopPIC
                mTopPICProgLoc = DetermineProgramLocation("TopPICProgLoc", TopPIC_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mTopPICProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TopPIC version info in the database after the first line is written to file TopPIC_ConsoleOutput.txt
                mToolVersionWritten = false;
                mTopPICVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Validate the FASTA file (to remove invalid residues)
                // Create the static mods file
                // Optionally create the dynamic mods file
                if (!CreateInputFiles())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!InitializeFastaFile(out var fastaFileIsDecoy))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using TopPIC
                var processingResult = StartTopPIC(fastaFileIsDecoy, mTopPICProgLoc);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsGlobal.IdleLoop(0.5);
                PRISM.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the "processing" messages
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                m_message = "Error in TopPICPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private bool CreateInputFiles()
        {

            // ToDo
            //if (!CreateModsFile(mTopPICWorkFolderPath))
            //{
            //    return false;
            //}

            // ToDo: Maybe call this
            // CopyFastaCheckResidues();

            return false;

        }

        private bool CopyFastaCheckResidues(string strSourceFilePath, string strTargetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            var intWarningCount = 0;

            try
            {
                var reInvalidResidues = new Regex(@"[BJOUXZ]", RegexOptions.Compiled);

                var oReader = new ProteinFileReader.FastaFileReader();
                if (!oReader.OpenFile(strSourceFilePath))
                {
                    m_message = "Error opening fasta file in CopyFastaCheckResidues";
                    return false;
                }

                using (var swNewFasta = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (oReader.ReadNextProteinEntry())
                    {
                        swNewFasta.WriteLine(oReader.ProteinLineStartChar + oReader.HeaderLine);
                        var strProteinResidues = reInvalidResidues.Replace(oReader.ProteinSequence, "-");

                        if (intWarningCount < 5 && strProteinResidues.GetHashCode() != oReader.ProteinSequence.GetHashCode())
                        {
                            LogWarning("Changed invalid residues to '-' in protein " + oReader.ProteinName);
                            intWarningCount += 1;
                        }

                        var intIndex = 0;
                        var intResidueCount = strProteinResidues.Length;
                        while (intIndex < strProteinResidues.Length)
                        {
                            var intLength = Math.Min(RESIDUES_PER_LINE, intResidueCount - intIndex);
                            swNewFasta.WriteLine(strProteinResidues.Substring(intIndex, intLength));
                            intIndex += RESIDUES_PER_LINE;
                        }
                    }
                }

                oReader.CloseFile();
            }
            catch (Exception ex)
            {
                m_message = "Exception in CopyFastaCheckResidues";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Returns a dictionary mapping parameter names to argument names
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, string> GetTopPICParameterNames()
        {
            var paramToArgMapping = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"ErrorTolerance", "error-tolerance"},
                {"MaxShift", "max-shift"},
                {"NumShift", "num-shift"},
                {"SpectrumCutoffType", "spectrum-cutoff-type"},
                {"SpectrumCutoffValue", "spectrum-cutoff-value"},
                {"ProteoformCutoffType", "proteoform-cutoff-type"},
                {"ProteoformCutoffValue", "proteoform-cutoff-value"},
                {"Decoy", "decoy"},
                {"NTerminalProteinForms", "n-terminal-form"}
            };

            return paramToArgMapping;
        }

        private bool InitializeFastaFile(out bool fastaFileIsDecoy)
        {
            fastaFileIsDecoy = false;

            // Define the path to the fasta file
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // Fasta file not found
                LogError("Fasta file not found: " + fastaFile.Name, "Fasta file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = m_jobParams.GetParam("ProteinOptions");
            if (!string.IsNullOrEmpty(proteinOptions))
            {
                if (proteinOptions.ToLower().Contains("seq_direction=decoy"))
                {
                    fastaFileIsDecoy = true;
                }
            }

            return true;
        }

        /// <summary>
        /// Parse the TopPIC console output file to determine the TopPIC version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output
            //
            // Zero PTM filtering - started.
            // Zero PTM filtering - block 1 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 1 finished.
            // Zero PTM filtering - block 2 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 2 finished.
            // Zero PTM filtering - block 3 out of 3 started.
            // Zero PTM filtering - processing 1504 of 1504 spectra.
            // Zero PTM filtering - block 3 finished.
            // Zero PTM filtering - combining blocks started.
            // Zero PTM filtering - combining blocks finished.
            // Zero PTM filtering - finished.
            // Zero PTM search - started.
            // Zero PTM search - processing 1504 of 1504 spectra.
            // Zero PTM search - finished.
            // One PTM filtering - started.
            // One PTM filtering - block 1 out of 3 started.
            // One PTM filtering - processing 1504 of 1504 spectra.
            // One PTM filtering - block 1 finished.
            // ...
            // One PTM filtering - finished.
            // One PTM search - started.
            // One PTM search - processing 1504 of 1504 spectra.
            // One PTM search - finished.
            // Diagonal PTM filtering - started.
            // Diagonal filtering - block 1 out of 3 started.
            // ...
            // Diagonal filtering - finished.
            // Two PTM search - started.
            // PTM search - processing 1504 of 1504 spectra.
            // Two PTM search - finished.
            // Combining PRSMs - started.
            // Combining PRSMs - finished.
            // E-value computation - started.
            // E-value computation - processing 1504 of 1504 spectra.
            // E-value computation - finished.
            // Finding protein species - started.
            // Finding protein species - finished.
            // Top PRSM selecting - started
            // Top PRSM selecting - finished.
            // FDR computation - started.
            // FDR computation - finished.
            // PRSM selecting by cutoff - started.
            // PRSM selecting by cutoff - finished.
            // Outputting the PRSM result table - started.
            // Outputting the PRSM result table - finished.
            // Generating the PRSM xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // Generating xml files - preprocessing 466 Proteoforms.
            // Generating xml files - processing 466 Proteoforms.
            // Generating xml files - preprocessing 110 Proteins.
            // Generating xml files - processing 110 Proteins.
            // Generating the PRSM xml files - finished.
            // Converting the PRSM xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting the PRSM xml files to html files - finished.
            // Proteoform selecting by cutoff - started.
            // Proteoform selecting by cutoff - finished.
            // Proteoform filtering - started.
            // Proteoform filtering - finished.
            // Outputting the proteoform result table - started.
            // Outputting the proteoform result table - finished.
            // Generating the proteoform xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // ...
            // Generating the proteoform xml files - finished.
            // Converting the proteoform xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting the proteoform xml files to html files - finished.
            // Deleting temporary files - started.
            // Deleting temporary files - finished.
            // TopPIC finished.

            var processingSteps = new SortedList<string, int>
            {
                {"Zero PTM filtering", 0},
                {"Zero PTM search", 10},
                {"One PTM filtering", 15},
                {"One PTM search", 20},
                {"Diagonal filtering", 25},
                {"Two PTM search", 45},
                {"Combining PRSMs", 80},
                {"E-value computation", 88},
                {"Finding protein species", 89},
                {"Generating the PRSM xml files", 90},
                {"Converting the PRSM xml files to html files", 93},
                {"Generating the proteoform xml files", 95},
                {"Converting the proteoform xml files to html files", 98},
                {"Deleting temporary files", 99},
                {"TopPIC finished", 100}
            };

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var actualProgress = 0;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataLineLcase = dataLine.ToLower();

                        if (linesRead <= 3)
                        {
                            // The first line has the TopPIC version
                            if (string.IsNullOrEmpty(mTopPICVersion) && dataLineLcase.Contains("toppic"))
                            {
                                if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mTopPICVersion))
                                {
                                    LogDebug("TopPIC version: " + dataLine);
                                }

                                mTopPICVersion = string.Copy(dataLine);
                            }
                        }

                        foreach (var processingStep in processingSteps)
                        {
                            if (!dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase))
                                continue;

                            if (actualProgress < processingStep.Value)
                                actualProgress = processingStep.Value;
                        }

                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg) &&
                            dataLineLcase.Contains("error") && !dataLineLcase.StartsWith("error tolerance:"))
                        {
                            mConsoleOutputErrorMsg += "Error running TopPIC: " + dataLine;
                        }

                    }
                }

                if (m_progress < actualProgress)
                {
                    m_progress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }


        /// <summary>
        /// Validate the static or dynamic mods defined in modList
        /// If valid mods are defined, write them to a text file and update cmdLineOptions
        /// </summary>
        /// <param name="cmdLineOptions">Command line arguments to pass to TopPIC</param>
        /// <param name="modList">List of static or dynamic mods</param>
        /// <param name="modDescription">Either "static" or "dynamic"</param>
        /// <param name="modsFileName">Filename that mods are written to</param>
        /// <param name="modArgumentSwitch">Argument name to append to cmdLineOptions along with the mod file name</param>
        /// <returns></returns>
        private bool ParseTopPICModifications(
            ref string cmdLineOptions,
            IReadOnlyCollection<string> modList,
            string modDescription,
            string modsFileName,
            string modArgumentSwitch)
        {
            try
            {
                var validatedMods = ValidateTopPICMods(modList, out var invalidMods);

                if (validatedMods.Count != modList.Count)
                {
                    LogError(string.Format("One or more {0} mods failed validation: {1}", modDescription, string.Join(", ", invalidMods)));
                    return false;
                }

                if (validatedMods.Count > 0)
                {
                    var modsFilePath = Path.Combine(m_WorkDir, modsFileName);
                    var success = WriteModsFile(modsFilePath, validatedMods);
                    if (!success)
                        return false;

                    // Append --fixed-mod ModsFilePath
                    // or     --mod-file-name ModsFilePath
                    cmdLineOptions += string.Format(" --{0} {1} ", modArgumentSwitch, modsFilePath);
                }

                return true;

            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception creating {0} mods file for TopPIC", modDescription), ex);
                return false;
            }
        }

        /// <summary>
        /// Read the TopPIC options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileIsDecoy">The plugin will set this to true if hte FASTA file is a forward+reverse FASTA file</param>
        /// <param name="cmdLineOptions">Output: TopPIC command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseTopPICParameterFile(bool fastaFileIsDecoy, out string cmdLineOptions)
        {
            const string STATIC_MODS_FILE_NAME = "TopPIC_Static_Mods.txt";
            const string DYNAMIC_MODS_FILE_NAME = "TopPIC_Dynamic_Mods.txt";

            cmdLineOptions = string.Empty;

            var paramFileName = m_jobParams.GetParam("parmFileName");

            var paramFileReader = new clsKeyValueParamFileReader("TopPIC", m_WorkDir, paramFileName);
            RegisterEvents(paramFileReader);

            var eResult = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                m_message = paramFileReader.ErrorMessage;
                return eResult;
            }

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetTopPICParameterNames();
            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase) {
                "NumMods",
                "StaticMod",
                "DynamicMod",
                "NTerminalProteinForms"
            };

            cmdLineOptions = paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "--");
            if (string.IsNullOrWhiteSpace(cmdLineOptions))
            {
                m_message = paramFileReader.ErrorMessage;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Instruct TopPIC to use the fragmentation method info tracked in the .mzML file
            // Other options for activation are CID, HCDCID, ETDCID, or UVPDCID
            cmdLineOptions += " --activation=FILE";

            if (paramFileReader.ParamIsEnabled(paramFileEntries, "Decoy"))
            {
                if (paramToArgMapping.TryGetValue("Decoy", out var argumentName))
                    cmdLineOptions += " --" + argumentName;
                else
                {
                    LogError("Parameter to argument mapping dictionary does not have Decoy");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            var staticMods = new List<string>();
            var dynamicMods = new List<string>();

            try
            {
                foreach (var kvSetting in paramFileEntries)
                {
                    var paramValue = kvSetting.Value;

                    if (clsGlobal.IsMatch(kvSetting.Key, "StaticMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !clsGlobal.IsMatch(paramValue, "none"))
                        {
                            staticMods.Add(paramValue);
                        }
                    }
                    else if (clsGlobal.IsMatch(kvSetting.Key, "DynamicMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !clsGlobal.IsMatch(paramValue, "none") && !clsGlobal.IsMatch(paramValue, "defaults"))
                        {
                            dynamicMods.Add(paramValue);
                        }
                    }
                    else if (clsGlobal.IsMatch(kvSetting.Key, "NTerminalProteinForms"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue))
                        {
                            // Assure the N-terminal protein forms list has no spaces
                            if (paramToArgMapping.TryGetValue(kvSetting.Key, out var argumentName))
                            {
                                cmdLineOptions += " --" + argumentName + kvSetting.Value.Replace(" ", "");
                            }
                            else
                            {
                                LogError("Parameter to argument mapping dictionary does not have NTerminalProteinForms");
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                    }

                } // for
            }
            catch (Exception ex)
            {
                LogError("Exception extracting dynamic and static mod information from the TopPIC parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create the static and dynamic modification file(s) if any static or dynamic mods are defined
            // Will also update cmdLineOptions to have --fixed-mod and/or --mod-file-name
            if (!ParseTopPICModifications(ref cmdLineOptions, staticMods, "static", STATIC_MODS_FILE_NAME, "--fixed-mod"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ParseTopPICModifications(ref cmdLineOptions, dynamicMods, "dynamic", DYNAMIC_MODS_FILE_NAME, "--mod-file-name"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once InvertIf
            if (paramToArgMapping.ContainsKey("Decoy") && fastaFileIsDecoy)
            {
                // TopPIC should be run with a forward=only protein collection; allow TopPIC to add the decoy proteins
                LogError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection " +
                         "when using a parameter file with setting Decoy=1");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType StartTopPIC(bool fastaFileIsDecoy, string progLoc)
        {

            LogMessage("Running TopPIC");

            // Set up and execute a program runner to run TopPIC
            // By default uses all cores; limit the number of cores to 4 with "--thread-number 4"

            var eResult = ParseTopPICParameterFile(fastaFileIsDecoy, out var cmdLineOptions);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return eResult;
            }

            var cmdStr = cmdLineOptions + " " +
                         mValidatedFASTAFilePath + " " +
                         Dataset + clsAnalysisResourcesTopPIC.MSALIGN_FILE_SUFFIX;

            LogDebug(progLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "TopPIC", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopPICVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running TopPIC");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopPIC returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopPIC failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate the results files and zip the html subdirectories
            var processingError = !ValidateAndZipResults(out var noValidResults);

            if (processingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("TopPIC Search Complete");
            }

            return noValidResults ? CloseOutType.CLOSEOUT_NO_DATA : CloseOutType.CLOSEOUT_SUCCESS;
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

            var strToolVersionInfo = string.Copy(mTopPICVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mTopPICProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Reads the console output file and removes the majority of "Processing" messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            var reExtractScan = new Regex(@"processing +(?<Scan>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + consoleOutputFilePath);
                }

                var trimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var scanNumberOutputThreshold = 0;
                    var lastScanNumber = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var keepLine = true;

                        var match = reExtractScan.Match(dataLine);
                        if (match.Success)
                        {
                            if (int.TryParse(match.Groups["Scan"].Value, out var scanNumber))
                            {
                                if (scanNumber < lastScanNumber)
                                {
                                    // We have entered a new processing mode; reset the threshold
                                    scanNumberOutputThreshold = 0;
                                }

                                if (scanNumber < scanNumberOutputThreshold)
                                {
                                    keepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up scanNumberOutputThreshold by 250
                                    scanNumberOutputThreshold += 250;
                                }
                            }
                        }

                        if (keepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(consoleOutputFilePath);
                    File.Move(trimmedFilePath, consoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + consoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        /// <summary>
        /// Validate the results files and zip the html subdirectories
        /// </summary>
        /// <returns></returns>
        private bool ValidateAndZipResults(out bool noValidResults)
        {

            noValidResults = false;

            try
            {
                // Dictionary mapping the original results file name created by TopPIC to the final name for the file
                var resultTableFiles = new Dictionary<string, string>
                {
                    {PRSM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, PRSM_RESULT_TABLE_NAME_SUFFIX_FINAL},
                    {PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_FINAL}
                };

                foreach (var resultFile in resultTableFiles)
                {
                    var sourceFile = Path.Combine(m_WorkDir, m_Dataset + resultFile.Key);
                    var targetFile = Path.Combine(m_WorkDir, m_Dataset + resultFile.Value);

                    var saveParameterfile = string.Equals(resultFile.Key, PRSM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL);
                    var success = ValidateResultTableFile(sourceFile, targetFile, saveParameterfile, out var noValidResultsThisFile);

                    if (string.Equals(resultFile.Key, PRSM_RESULT_TABLE_NAME_SUFFIX_ORIGINAL) && noValidResultsThisFile)
                        noValidResults = true;

                    if (!success)
                        return false;
                }

                // Zip the Html directories
                var directoriesToCompress = new List<string> {
                    "_ms2_proteoform_cutoff_html",
                    "_ms2_prsm_cutoff_html" };

                foreach (var directorySuffix in directoriesToCompress)
                {
                    var success = ZipTopPICResultsDirectory(directorySuffix);
                    if (!success)
                        return false;
                }


            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateAndZipResults", ex);
                return false;
            }


            return true;
        }

        private bool ValidateResultTableFile(string sourceFilePath, string targetFilePath, bool saveParameterfile, out bool noValidResults)
        {
            var reParametersHeader = new Regex(@"\*+ Parameters \*+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            noValidResults = false;

            try
            {
                var validFile = false;
                var foundParamHeaderA = false;
                var foundParamHeaderB = false;

                var parameterInfo = new List<string>();

                var sourceFile = new FileInfo(sourceFilePath);

                if (!sourceFile.Exists)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogWarning("TopPIC results file not found: " + sourceFilePath);
                    }
                    return false;
                }

                if (m_DebugLevel >= 2)
                {
                    LogMessage("Validating that the TopPIC results file is not empty");
                }

                // Open the input file and output file
                // The output file will not include the Parameters block before the header line
                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (!foundParamHeaderB)
                        {
                            // Look for the parameters header: ********************** Parameters **********************
                            var match = reParametersHeader.Match(dataLine);
                            if (match.Success)
                            {
                                if (!foundParamHeaderA)
                                {
                                    foundParamHeaderA = true;
                                }
                                else
                                {
                                    foundParamHeaderB = true;

                                    if (parameterInfo.Count > 0)
                                    {
                                        // This is second instance of the parameters header
                                        // Optionally write the parameter file
                                        if (saveParameterfile)
                                        {
                                            WriteParametersToDisk(parameterInfo);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                parameterInfo.Add(dataLine);
                            }

                            continue;
                        }

                        writer.WriteLine(dataLine);

                        var dataColumns = dataLine.Split('\t');

                        if (dataColumns.Length > 1)
                        {
                            // Look for an integer in the second column (the first column has the data file name)
                            if (int.TryParse(dataColumns[1], out _))
                            {
                                // Integer found; line is valid
                                validFile = true;
                            }
                        }
                    }
                }

                m_jobParams.AddResultFileToSkip(sourceFile.Name);

                if (validFile)
                    return true;

                if (!foundParamHeaderB)
                {
                    LogError("TopPIC results file is empty: " + sourceFile.Name);
                    return false;
                }

                noValidResults = true;

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultTableFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Validates the modification definition text
        /// </summary>
        /// <param name="mod">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>A valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        private bool ValidateMod(string mod, out string modClean)
        {

            modClean = string.Empty;

            var poundIndex = mod.IndexOf('#');
            if (poundIndex > 0)
            {
                // comment = mod.Substring(poundIndex);
                mod = mod.Substring(0, poundIndex - 1).Trim();
            }

            var splitMod = mod.Split(',');

            if (splitMod.Length < 5)
            {
                // Invalid mod definition; must have 5 sections
                LogError("Invalid modification string; must have 5 sections: " + mod);
                return false;
            }

            // Make sure mod does not have both * and any
            if (splitMod[1].Trim() == "*" && splitMod[3].ToLower().Trim() == "any")
            {
                LogError("Modification cannot contain both * and any: " + mod);
                return false;
            }

            // Make sure the Unimod ID is a positive integer or -1
            if (!int.TryParse(splitMod[4], out var unimodId))
            {
                LogError("UnimodID must be an integer: " + splitMod[4]);
                return false;
            }

            if (unimodId < 1 && unimodId != -1)
            {
                LogError(string.Format("Changing UnimodID from {0} to -1", splitMod[4]));
                splitMod[4] = "-1";
            }

            // Reconstruct the mod definition, making sure there is no whitespace
            modClean = splitMod[0].Trim();
            for (var index = 1; index <= splitMod.Length - 1; index++)
            {
                modClean += "," + splitMod[index].Trim();
            }

            return true;
        }

        private List<string> ValidateTopPICMods(IReadOnlyCollection<string> modList, out List<string> invalidMods)
        {
            var validatedMods = new List<string>();
            invalidMods = new List<string>();

            foreach (var modEntry in modList)
            {

                if (ValidateMod(modEntry, out var modClean))
                {
                    validatedMods.Add(modClean);
                }
                else
                {
                    invalidMods.Add(modEntry);
                }
            }

            return validatedMods;
        }

        private bool WriteModsFile(string modsFilePath, IEnumerable<string> validatedMods)
        {
            try
            {
                using (var writer = new StreamWriter(new FileStream(modsFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var modItem in validatedMods)
                    {
                        writer.WriteLine(modItem);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception creating mods file {0} for TopPIC", Path.GetFileName(modsFilePath)), ex);
                return false;
            }
        }

        private void WriteParametersToDisk(IEnumerable<string> parameterInfo)
        {
            try
            {
                var runtimeParamsPath = Path.Combine(m_WorkDir, "TopPIC_RuntimeParameters.txt");
                using (var writer = new StreamWriter(new FileStream(runtimeParamsPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var parameter in parameterInfo)
                    {
                        writer.WriteLine(parameter);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception saving the parameters file for TopPIC (after TopPIC finished running)", ex);
            }
        }

        private bool ZipTopPICResultsDirectory(string directorySuffix)
        {

            try
            {
                var zipFilePath = Path.Combine(m_WorkDir, m_Dataset + directorySuffix + ".zip");

                var sourceDirectoryPath = Path.Combine(m_WorkDir, m_Dataset + directorySuffix);

                // Confirm that the directory has one or more files or subdirectories
                var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                if (sourceDirectory.GetFileSystemInfos().Length == 0)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogWarning("TopPIC results directory is empty; nothing to zip: " + Path.GetFileName(sourceDirectoryPath));
                    }
                    return false;
                }

                if (m_DebugLevel >= 1)
                {
                    var logMessage = "Zipping directory " + sourceDirectoryPath;

                    if (m_DebugLevel >= 2)
                    {
                        logMessage += ": " + zipFilePath;
                    }
                    LogMessage(logMessage);
                }

                var zipper = new Ionic.Zip.ZipFile(zipFilePath);
                zipper.AddDirectory(sourceDirectoryPath);
                zipper.Save();

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception compressing the {0} directory created by TopPIC", directorySuffix), ex);
                return false;
            }

        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopPIC_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopPICVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopPIC");
        }

        #endregion
    }
}
