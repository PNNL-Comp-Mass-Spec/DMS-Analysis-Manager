using System;
using System.Collections.Generic;
using System.IO;

using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using IDP;
using log4net;
using PRISM.Logging;

namespace AnalysisManager_IDP_PlugIn
{
    /// <summary>
    /// Class for running IDP
    /// </summary>
    public class AnalysisToolRunnerIDP : AnalysisToolRunnerBase
    {
        private const float PROGRESS_PCT_IDP_START = 5;
        private const float PROGRESS_PCT_IDP_DONE = 95;

        public override CloseOutType RunTool()
        {
            try
            {
                var success = false;

                //Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Running IDP");
                mProgress = PROGRESS_PCT_IDP_START;
                UpdateStatusRunning(mProgress);

                if (mDebugLevel > 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "AnalysisToolRunnerIDP.RunTool(): Enter");
                }

                // Store the Cyclops version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining IDP version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }


                Dictionary<string, string> d_Params = new Dictionary<string, string>();
                d_Params.Add("Job", mJobParams.GetParam("Job"));
                d_Params.Add("IDPWorkflowName", mJobParams.GetParam("IDPWorkflowName"));
                d_Params.Add("workDir", mWorkDir);

                //Change the name of the log file for the local log file to the plug in log filename
                String LogFileName = Path.Combine(mWorkDir, "IDP_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                LogTools.ChangeLogFileBaseName(LogFileName, true);

                try
                {
                    var idp = new IDP.clsIDP(d_Params);

                    // if a workflow is not passed to IDPicker, then do not run the program.
                    if (!string.IsNullOrEmpty(d_Params["IDPWorkflowName"]))
                        success = idp.Run();

                    //Change the name of the log file for the local log file to the plug in log filename
                    LogFileName = mMgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    LogTools.ChangeLogFileBaseName(LogFileName, true);
                }
                catch (Exception ex)
                {
                    //Change the name of the log file for the local log file to the plug in log filename
                    LogFileName = mMgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    LogTools.ChangeLogFileBaseName(LogFileName, true);

                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error running IDP: " + ex.Message);
                    success = false;
                }

                //Stop the job timer
                mStopTime = System.DateTime.UtcNow;
                mProgress = PROGRESS_PCT_IDP_DONE;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Error creating summary file, job " + mJob + ", step " + mJobParams.GetParam("Step"));
                }

                //Make sure objects are released
                //2 second delay
                System.Threading.Thread.Sleep(2000);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!success)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging MultiAlign problems
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mResultsDirectoryName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam("OutputFolderName");
                mJobParams.SetParam("StepParameters", "OutputFolderName", mResultsDirectoryName);

                success = MakeResultsDirectory();
                if (!success)
                {
                    // MakeResultsDirectory handles posting to local log, so set database error message and exit
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                success = MoveResultFiles();
                if (!success)
                {
                    // Note that MoveResultFiles should have already called AnalysisResults.CopyFailedResultsToArchiveDirectory
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Move the Plots folder to the result files folder
                System.IO.DirectoryInfo plotsFolder = default(System.IO.DirectoryInfo);
                plotsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(mWorkDir, "IDPickerResults"));

                if (plotsFolder.Exists)
                {
                    string targetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(mWorkDir, mResultsDirectoryName), "IDPickerResults");
                    plotsFolder.MoveTo(targetFolderPath);
                }

                success = CopyResultsFolderToServer();
                if (!success)
                {
                    // Note that CopyResultsFolderToServer should have already called AnalysisResults.CopyFailedResultsToArchiveDirectory
                    return CloseOutType.CLOSEOUT_FAILED;
                }

            }
            catch (Exception ex)
            {
                mMessage = "Error in IDPsPlugin->RunTool";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {

            string toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Determining tool version info");
            }

            try
            {
                System.Reflection.AssemblyName assemblyName = System.Reflection.Assembly.Load("IDP").GetName();

                string nameAndVersion;
                nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version.ToString();
                toolVersionInfo = Global.AppendToComment(toolVersionInfo, nameAndVersion);
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception determining Assembly info for IDP: " + ex.Message);
                return false;
            }

            // Store paths to key DLLs
            System.Collections.Generic.List<System.IO.FileInfo> toolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
            toolFiles.Add(new System.IO.FileInfo("IDP.dll"));

            try
            {
                return base.SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }


        }
    }
}
