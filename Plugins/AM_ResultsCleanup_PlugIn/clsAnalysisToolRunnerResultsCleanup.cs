//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/17/2013
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerResultsCleanupPlugin
{
    public class clsAnalysisToolRunnerResultsCleanup : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        protected const string RESULTS_DB3_FILE = "Results.db3";

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs ResultsCleanup tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AnalysisManager version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Cleanup results in the transfer directory
                var Result = PerformResultsCleanup();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error calling PerformResultsCleanup";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                m_message = "Error in clsAnalysisToolRunnerResultsCleanup->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected CloseOutType PerformResultsCleanup()
        {
            string strTransferDirectoryPath = null;
            string strResultsFolderName = null;
            CloseOutType eResult = CloseOutType.CLOSEOUT_SUCCESS;

            try
            {
                strTransferDirectoryPath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", string.Empty);
                strResultsFolderName = m_jobParams.GetJobParameter("JobParameters", "InputFolderName", string.Empty);

                if (string.IsNullOrWhiteSpace(strTransferDirectoryPath))
                {
                    m_message = "transferFolderPath not defined";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else if (string.IsNullOrWhiteSpace(strResultsFolderName))
                {
                    m_message = "InputFolderName not defined";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DirectoryInfo diTransferFolder = new DirectoryInfo(strTransferDirectoryPath);

                if (!diTransferFolder.Exists)
                {
                    m_message = "transferFolder not found at " + strTransferDirectoryPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var diResultsFolder = new DirectoryInfo(Path.Combine(diTransferFolder.FullName, strResultsFolderName));
                eResult = RemoveOldResultsDb3Files(diResultsFolder);
            }
            catch (Exception ex)
            {
                m_message = "Error in PerformResultsCleanup";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return eResult;
        }

        protected CloseOutType RemoveOldResultsDb3Files(DirectoryInfo diResultsFolder)
        {
            int intStepFolderCount = 0;
            int intStepNumber = 0;

            try
            {
                var reStepNumber = new Regex(@"Step_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var dctResultsFiles = new Dictionary<int, FileInfo>();

                // Look for Results.db3 files in the subfolders of the transfer folder
                // Only process folders that start with the text "Step_"
                foreach (var diSubfolder in diResultsFolder.GetDirectories("Step_*"))
                {
                    intStepFolderCount += 1;

                    // Parse out the step number
                    var reMatch = reStepNumber.Match(diSubfolder.Name);

                    if (reMatch.Success && int.TryParse(reMatch.Groups[1].Value, out intStepNumber))
                    {
                        if (!dctResultsFiles.ContainsKey(intStepNumber))
                        {
                            foreach (FileInfo fiFile in diSubfolder.GetFiles(RESULTS_DB3_FILE))
                            {
                                dctResultsFiles.Add(intStepNumber, fiFile);
                                break;
                            }
                        }
                    }
                }

                if (dctResultsFiles.Count > 1)
                {
                    // Delete the Results.db3 files for the steps prior to intLastStep
                    int intLastStep = dctResultsFiles.Keys.Max();
                    int intFileCountDeleted = 0;

                    var lnqQuery = from item in dctResultsFiles where item.Key < intLastStep select item;
                    foreach (var item in lnqQuery)
                    {
                        try
                        {
                            item.Value.Delete();
                            intFileCountDeleted += 1;
                        }
                        catch (Exception ex)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "Error deleting extra " + RESULTS_DB3_FILE + " file: " + ex.Message);
                        }
                    }

                    m_EvalMessage = "Deleted " + intFileCountDeleted + " extra " + RESULTS_DB3_FILE + " " +
                                    clsGlobal.CheckPlural(intFileCountDeleted, "file", "files");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        m_EvalMessage + " from " + diResultsFolder.FullName);
                }
                else if (dctResultsFiles.Count == 1)
                {
                    m_EvalMessage = "Results folder has just one " + RESULTS_DB3_FILE + " file";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_EvalMessage);
                }
                else
                {
                    if (intStepFolderCount > 0)
                    {
                        m_EvalMessage = "None of the Step_# folders has a " + RESULTS_DB3_FILE + " file";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage);
                    }
                    else
                    {
                        m_message = "Results folder does not have any Step_# folders";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + diResultsFolder.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in RemoveOldResultsDb3Files";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;
            string strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of the Analysis Manager
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerProg"))
            {
                return false;
            }

            // Lookup the version of AnalysisManagerResultsCleanupPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerResultsCleanupPlugin"))
            {
                return false;
            }

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsCleanupPlugin.dll in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerProg.exe")));
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerResultsCleanupPlugin.dll")));

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
    }
}
