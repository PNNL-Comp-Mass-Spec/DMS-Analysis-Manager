//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/30/2015
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerProMexPlugIn
{
    /// <summary>
    /// Class for running ProMex to deisotope high resolution spectra
    /// </summary>
    public class clsAnalysisToolRunnerProMex : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const string PROMEX_CONSOLE_OUTPUT = "ProMex_ConsoleOutput.txt";

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        protected string mConsoleOutputErrorMsg;

        protected string mProMexParamFilePath;

        protected string mProMexResultsFilePath;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs ProMex
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
                    LogDebug("clsAnalysisToolRunnerProMex.RunTool(): Enter");
                }

                // Determine the path to the ProMex program
                var progLoc = DetermineProgramLocation("ProMexProgLoc", "ProMex.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the ProMex version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining ProMex version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run ProMex
                var processingSuccess = StartProMex(progLoc);

                if (processingSuccess)
                {
                    // Look for the results file

                    var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MS1FT_EXTENSION));

                    if (fiResultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessProMexResults(fiResultsFile);
                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error post-processing the ProMex results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "ProMex results file not found: " + fiResultsFile.Name;
                        }
                        processingSuccess = false;
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // There is no need to keep the parameter file since it is fairly simple, and the ProMex_ConsoleOutput.txt file displays all of the parameters used
                m_jobParams.AddResultFileToSkip(mProMexParamFilePath);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in ProMexPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        protected Dictionary<string, string> GetProMexParameterNames()
        {
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"MinCharge", "minCharge"},
                {"MaxCharge", "maxCharge"},
                {"MinMass", "minMass"},
                {"MaxMass", "maxMass"},
                {"Score", "score"},
                {"Csv", "csv"},
                {"MaxThreads", "maxThreads"}
            };

            return dctParamNames;
        }

        private const string REGEX_ProMex_PROGRESS = @"Processing ([0-9.]+)\%";
        private readonly Regex reCheckProgress = new Regex(REGEX_ProMex_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the ProMex console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // ****** ProMex   ver. 1.0 (Jan 29, 2014) ************
            // -i      CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
            // -minCharge      2
            // -maxCharge      60
            // -minMass        3000.0
            // -maxMass        50000.0
            // -score  n
            // -csv    y
            // -maxThreads     0
            // Start loading MS1 data from CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
            // Complete loading MS1 data. Elapsed Time = 17.514 sec
            // Start MS1 feature extracting...
            // Mass Range 3000 - 50000
            // Charge Range 2 - 60
            // Output File     CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.ms1ft
            // Csv Output File CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ_ms1ft.csv
            // Processing 2.25 % of mass bins (3187.563 Da); Elapsed Time = 16.035 sec; # of features = 283
            // Processing 4.51 % of mass bins (3375.063 Da); Elapsed Time = 26.839 sec; # of features = 770
            // Processing 6.76 % of mass bins (3562.563 Da); Elapsed Time = 40.169 sec; # of features = 1426
            // Processing 9.02 % of mass bins (3750.063 Da); Elapsed Time = 51.633 sec; # of features = 2154

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

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                // Value between 0 and 100
                float progressComplete = 0;
                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var strLineInLCase = strLineIn.ToLower();

                        if (strLineInLCase.StartsWith("error:") || strLineInLCase.Contains("unhandled exception"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running ProMex:";
                            }

                            if (strLineInLCase.StartsWith("error:"))
                            {
                                mConsoleOutputErrorMsg += " " + strLineIn.Substring("error:".Length).Trim();
                            }
                            else
                            {
                                mConsoleOutputErrorMsg += " " + strLineIn;
                            }

                            continue;
                        }

                        var oMatch = reCheckProgress.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            float.TryParse(oMatch.Groups[1].ToString(), out progressComplete);
                        }
                    }
                }

                if (m_progress < progressComplete)
                {
                    m_progress = progressComplete;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the ProMex options file and convert the options to command line switches
        /// </summary>
        /// <param name="cmdLineOptions">Output: MSGFDb command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseProMexParameterFile(out string cmdLineOptions)
        {
            cmdLineOptions = string.Empty;

            var paramFileName = m_jobParams.GetParam("ProMexParamFile");

            // Although ParseKeyValueParameterFile checks for paramFileName being an empty string,
            // we check for it here since the name comes from the settings file, so we want to customize the error message
            if (string.IsNullOrWhiteSpace(paramFileName))
            {
                LogError("ProMex parameter file not defined in the job settings (param name ProMexParamFile)");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var paramFileReader = new clsKeyValueParamFileReader("ProMex", m_WorkDir, paramFileName);
            RegisterEvents(paramFileReader);

            var eResult = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                m_message = paramFileReader.ErrorMessage;
                return eResult;
            }

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetProMexParameterNames();
            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            cmdLineOptions = paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "-");
            if (string.IsNullOrWhiteSpace(cmdLineOptions))
            {
                m_message = paramFileReader.ErrorMessage;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Validates that the modification definition text
        /// </summary>
        /// <param name="strMod">Modification definition</param>
        /// <param name="strModClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        protected bool ParseProMexValidateMod(string strMod, out string strModClean)
        {
            var strComment = string.Empty;

            strModClean = string.Empty;

            var intPoundIndex = strMod.IndexOf('#');
            if (intPoundIndex > 0)
            {
                strComment = strMod.Substring(intPoundIndex);
                strMod = strMod.Substring(0, intPoundIndex - 1).Trim();
            }

            var strSplitMod = strMod.Split(',');

            if (strSplitMod.Length < 5)
            {
                // Invalid mod definition; must have 5 sections
                LogError("Invalid modification string; must have 5 sections: " + strMod);
                return false;
            }

            // Make sure mod does not have both * and any
            if (strSplitMod[1].Trim() == "*" && strSplitMod[3].ToLower().Trim() == "any")
            {
                LogError("Modification cannot contain both * and any: " + strMod);
                return false;
            }

            // Reconstruct the mod definition, making sure there is no whitespace
            strModClean = strSplitMod[0].Trim();
            for (var intIndex = 1; intIndex <= strSplitMod.Length - 1; intIndex++)
            {
                strModClean += "," + strSplitMod[intIndex].Trim();
            }

            if (!string.IsNullOrWhiteSpace(strComment))
            {
                // As of August 12, 2011, the comment cannot contain a comma
                // Sangtae Kim has promised to fix this, but for now, we'll replace commas with semicolons
                strComment = strComment.Replace(",", ";");
                strModClean += "     " + strComment;
            }

            return true;
        }

        private bool PostProcessProMexResults(FileSystemInfo fiResultsFile)
        {
            // Make sure there are at least two features in the .ms1ft file

            try
            {
                using (var resultsReader = new StreamReader(new FileStream(fiResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var lineCount = 0;
                    while (!resultsReader.EndOfStream)
                    {
                        var lineIn = resultsReader.ReadLine();
                        if (!string.IsNullOrEmpty(lineIn))
                        {
                            lineCount += 1;
                            if (lineCount > 2)
                            {
                                return true;
                            }
                        }
                    }
                }

                m_message = "The ProMex results file has fewer than 2 deisotoped features";

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception examining the ms1ft file: " + ex.Message);
                return false;
            }
        }

        protected bool StartProMex(string progLoc)
        {
            mConsoleOutputErrorMsg = string.Empty;

            // Read the ProMex Parameter File
            // The parameter file name specifies the mass modifications to consider, plus also the analysis parameters

            var eResult = ParseProMexParameterFile(out var cmdLineOptions);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return false;
            }

            if (string.IsNullOrEmpty(cmdLineOptions))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Problem parsing ProMex parameter file";
                }
                return false;
            }

            string msFilePath;

            var proMexBruker = clsAnalysisResourcesProMex.IsProMexBrukerJob(m_jobParams);

            if (proMexBruker)
            {
                msFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
            }
            else
            {
                msFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION);
            }

            LogMessage("Running ProMex");

            // Set up and execute a program runner to run ProMex

            var cmdStr = " -i " + msFilePath;
            cmdStr += " " + cmdLineOptions;

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, PROMEX_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var blnSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "ProMex", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                }

            }

            // Parse the console output file one more time to check for errors
            clsGlobal.IdleLoop(0.25);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {

                LogError("Error running ProMex");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("ProMex returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to ProMex failed (but exit code is 0)");
                }

                return false;
            }

            if (mConsoleOutputErrorMsg.Contains("Data file has no MS1 spectra"))
            {
                m_message = mConsoleOutputErrorMsg;
                return false;
            }

            if (proMexBruker)
            {
                blnSuccess = StorePbfFileInCache();
                if (!blnSuccess)
                {
                    return false;
                }
            }

            m_progress = PROGRESS_PCT_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("ProMex Search Complete");
            }

            return true;
        }

        private bool StorePbfFileInCache()
        {
            var fiAutoGeneratedPbfFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION));

            if (!fiAutoGeneratedPbfFile.Exists)
            {
                LogError("Auto-generated .PBF file not found; this is unexpected: " + fiAutoGeneratedPbfFile.Name);
                return false;
            }

            // Store the PBF file in the spectra cache folder; not in the job result folder

            var msXmlCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
            var msXmlCacheFolder = new DirectoryInfo(msXmlCacheFolderPath);

            if (!msXmlCacheFolder.Exists)
            {
                LogError("MSXmlCache folder not found: " + msXmlCacheFolderPath);
                return false;
            }

            // Temporarily override the result folder name
            var resultFolderNameSaved = string.Copy(m_ResFolderName);

            m_ResFolderName = "PBF_Gen_1_193_000000";

            // Copy the .pbf file to the MSXML cache
            var remoteCachefilePath = CopyFileToServerCache(msXmlCacheFolder.FullName, fiAutoGeneratedPbfFile.FullName, purgeOldFilesIfNeeded: true);

            // Restore the result folder name
            m_ResFolderName = resultFolderNameSaved;

            if (string.IsNullOrEmpty(remoteCachefilePath))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("CopyFileToServerCache returned false for " + fiAutoGeneratedPbfFile.Name);
                }
                return false;
            }

            // Create the _CacheInfo.txt file
            var cacheInfoFilePath = fiAutoGeneratedPbfFile.FullName + "_CacheInfo.txt";
            using (var swOutFile = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swOutFile.WriteLine(remoteCachefilePath);
            }

            m_jobParams.AddResultFileToSkip(fiAutoGeneratedPbfFile.Name);

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the console output file every 15 seconds
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, PROMEX_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("ProMex");
            }
        }

        #endregion
    }
}
