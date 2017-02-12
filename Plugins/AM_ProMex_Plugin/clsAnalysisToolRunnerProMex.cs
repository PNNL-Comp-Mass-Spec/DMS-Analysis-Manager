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
using System.Threading;
using AnalysisManagerBase;
using PRISM.Processes;

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
        /// <remarks></remarks>
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerProMex.RunTool(): Enter");
                }

                // Determine the path to the ProMex program
                string progLoc = null;
                progLoc = DetermineProgramLocation("ProMex", "ProMexProgLoc", "ProMex.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the ProMex version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining ProMex version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run ProMex
                var blnSuccess = StartProMex(progLoc);

                if (blnSuccess)
                {
                    // Look for the results file

                    var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MS1FT_EXTENSION));

                    if (fiResultsFile.Exists)
                    {
                        blnSuccess = PostProcessProMexResults(fiResultsFile);
                        if (!blnSuccess)
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error post-processing the ProMex results";
                            }
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "ProMex results file not found: " + fiResultsFile.Name;
                        }
                        blnSuccess = false;
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                mCmdRunner = null;

                //Make sure objects are released
                Thread.Sleep(500);        // 500 msec delay
                clsProgRunner.GarbageCollectNow();

                if (!blnSuccess)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // There is no need to keep the parameter file since it is fairly simple, and the ProMex_ConsoleOutput.txt file displays all of the parameters used
                m_jobParams.AddResultFileToSkip(mProMexParamFilePath);

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in ProMexPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the .mzXML file first)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + ".mzXML"));
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        protected Dictionary<string, string> GetProMexParameterNames()
        {
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.CurrentCultureIgnoreCase);

            dctParamNames.Add("MinCharge", "minCharge");
            dctParamNames.Add("MaxCharge", "maxCharge");

            dctParamNames.Add("MinMass", "minMass");
            dctParamNames.Add("MaxMass", "maxMass");

            dctParamNames.Add("Score", "score");
            dctParamNames.Add("Csv", "csv");
            dctParamNames.Add("MaxThreads", "maxThreads");

            return dctParamNames;
        }

        private const string REGEX_ProMex_PROGRESS = @"Processing ([0-9.]+)\%";
        private Regex reCheckProgress = new Regex(REGEX_ProMex_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                // Value between 0 and 100
                float progressComplete = 0;
                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
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
                            else
                            {
                                Match oMatch = reCheckProgress.Match(strLineIn);
                                if (oMatch.Success)
                                {
                                    float.TryParse(oMatch.Groups[1].ToString(), out progressComplete);
                                    continue;
                                }
                            }
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the ProMex options file and convert the options to command line switches
        /// </summary>
        /// <param name="strCmdLineOptions">Output: MSGFDb command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseProMexParameterFile(out string strCmdLineOptions)
        {
            strCmdLineOptions = string.Empty;

            mProMexParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("ProMexParamFile"));

            if (!File.Exists(mProMexParamFilePath))
            {
                LogError("Parameter file not found", "Parameter file not found: " + mProMexParamFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var sbOptions = new StringBuilder(500);

            try
            {
                // Initialize the Param Name dictionary
                var dctParamNames = GetProMexParameterNames();

                using (var srParamFile = new StreamReader(new FileStream(mProMexParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(strLineIn);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key))
                        {
                            string strValue = kvSetting.Value;

                            string strArgumentSwitch = string.Empty;

                            // Check whether kvSetting.key is one of the standard keys defined in dctParamNames

                            if (dctParamNames.TryGetValue(kvSetting.Key, out strArgumentSwitch))
                            {
                                sbOptions.Append(" -" + strArgumentSwitch + " " + strValue);
                            }
                            else
                            {
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                    "Ignoring parameter '" + kvSetting.Key + "' since not recognized as a valid ProMex parameter");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception reading ProMex parameter file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            strCmdLineOptions = sbOptions.ToString();

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
            int intPoundIndex = 0;
            string[] strSplitMod = null;

            string strComment = string.Empty;

            strModClean = string.Empty;

            intPoundIndex = strMod.IndexOf('#');
            if (intPoundIndex > 0)
            {
                strComment = strMod.Substring(intPoundIndex);
                strMod = strMod.Substring(0, intPoundIndex - 1).Trim();
            }

            strSplitMod = strMod.Split(',');

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
            for (int intIndex = 1; intIndex <= strSplitMod.Length - 1; intIndex++)
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

        private bool PostProcessProMexResults(FileInfo fiResultsFile)
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception examining the ms1ft file: " + ex.Message);
                return false;
            }
        }

        protected bool StartProMex(string progLoc)
        {
            string CmdStr = null;
            bool blnSuccess = false;

            mConsoleOutputErrorMsg = string.Empty;

            // Read the ProMex Parameter File
            // The parameter file name specifies the mass modifications to consider, plus also the analysis parameters

            string strCmdLineOptions = string.Empty;

            var eResult = ParseProMexParameterFile(out strCmdLineOptions);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return false;
            }
            else if (string.IsNullOrEmpty(strCmdLineOptions))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Problem parsing ProMex parameter file";
                }
                return false;
            }

            string msFilePath = null;

            var proMexBruker = clsAnalysisResourcesProMex.IsProMexBrukerJob(m_jobParams);

            if (proMexBruker)
            {
                msFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
            }
            else
            {
                msFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION);
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running ProMex");

            //Set up and execute a program runner to run ProMex

            CmdStr = " -i " + msFilePath;
            CmdStr += " " + strCmdLineOptions;

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc + CmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
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
            blnSuccess = mCmdRunner.RunProgram(progLoc, CmdStr, "ProMex", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile =
                    new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            // Parse the console output file one more time to check for errors
            Thread.Sleep(250);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                var Msg = "Error running ProMex";
                m_message = clsGlobal.AppendToComment(m_message, Msg);

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);

                if (mCmdRunner.ExitCode != 0)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "ProMex returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                }
                else
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to ProMex failed (but exit code is 0)");
                }

                return false;
            }
            else if (mConsoleOutputErrorMsg.Contains("Data file has no MS1 spectra"))
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ProMex Search Complete");
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

            string msXmlCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
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
        protected bool StoreToolVersionInfo(string strProgLoc)
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }
            }

            // Lookup the version of the .NET application
            blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, fiProgram.FullName);
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(fiProgram);

            ioToolFiles.Add(new FileInfo(Path.Combine(fiProgram.Directory.FullName, "InformedProteomics.Backend.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
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
