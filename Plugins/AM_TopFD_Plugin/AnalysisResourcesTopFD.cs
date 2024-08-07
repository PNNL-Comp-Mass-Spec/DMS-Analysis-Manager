//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopFD plugin
    /// </summary>
    public class AnalysisResourcesTopFD : AnalysisResources
    {
        // Ignore Spelling: desc, TopFD

        public const string JOB_PARAM_EXISTING_TOPFD_RESULTS_DIRECTORY = "ExistingTopFDResultsDirectory";
        public const string JOB_PARAM_EXISTING_TOPFD_TOOL_VERSION = "ExistingTopFDResultsToolVersion";

        private struct TopFDJobInfoType
        {
            public string ToolVersion;
            public string ResultsDirectoryName;
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                var topFdParamFile = mJobParams.GetParam("TopFD_ParamFile");

                if (string.IsNullOrWhiteSpace(topFdParamFile))
                {
                    LogError("TopFD parameter file not defined in the job settings (param name TopFD_ParamFile)");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                // Retrieve the TopFD parameter file
                currentTask = "Retrieve the TopFD parameter file " + topFdParamFile;

                const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "TopFD";

                var topFdParamFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

                if (string.IsNullOrWhiteSpace(topFdParamFileStoragePath))
                {
                    topFdParamFileStoragePath = @"\\gigasax\dms_parameter_Files\TopFD";
                    LogWarning("Parameter '" + paramFileStoragePathKeyName + "' is not defined " +
                               "(obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); " +
                               "will assume: " + topFdParamFileStoragePath);
                }

                if (!FileSearchTool.RetrieveFile(topFdParamFile, topFdParamFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                currentTask = "Find existing results";

                // Look for existing TopFD results for this dataset (using the same parameter file and same version of TopFD)
                // Simulate running TopFD if a match is found
                var existingResultsFound = FindExistingTopFDResults(out var criticalError);

                if (criticalError)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!existingResultsFound)
                {
                    currentTask = "Get Input file";

                    var mzMLResult = GetMzMLFile();

                    if (mzMLResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        if (mzMLResult == CloseOutType.CLOSEOUT_FILE_NOT_IN_CACHE)
                            mMessage = ".mzML file not found";
                        else
                            mMessage = "GetMzMLFile() returned an error code";

                        return mzMLResult;
                    }

                    // Make sure we don't move the .mzML file into the results folder
                    mJobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);
                }

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Look for an existing TopFD results for this dataset (using the same parameter file and same version of TopFD)
        /// If found, creates job step parameters ExistingTopFDResultsDirectory and ExistingTopFDResultsToolVersion
        /// </summary>
        /// <param name="criticalError"></param>
        /// <returns>True if existing results were found, otherwise false</returns>
        private bool FindExistingTopFDResults(out bool criticalError)
        {
            criticalError = false;

            try
            {
                var datasetID = mJobParams.GetJobParameter("DatasetID", 0);

                if (datasetID == 0)
                {
                    LogError("Job parameters do not have DatasetID (or it is 0); unable to look for existing TopFD results");
                    criticalError = true;
                    return false;
                }

                var settingsFileToFind = mJobParams.GetJobParameter("SettingsFileName", string.Empty).Trim();

                if (string.IsNullOrWhiteSpace(settingsFileToFind))
                {
                    LogError("Job parameters do not have SettingsFileName (or it is an empty string); unable to look for existing TopFD results");
                    criticalError = true;
                    return false;
                }

                var useExistingTopFDResults = mJobParams.GetJobParameter("TopFD", "UseExistingTopFDResults", true);

                // Determine the path to the TopFD program
                var topFDProgLoc = AnalysisToolRunnerBase.DetermineProgramLocation(
                    mMgrParams, mJobParams, StepToolName,
                    "TopFDProgLoc",
                    AnalysisToolRunnerTopFD.TOPFD_EXE_NAME,
                    out var errorMessage, out _);

                if (string.IsNullOrWhiteSpace(topFDProgLoc))
                {
                    // The error has already been logged, but we need to update mMessage
                    mMessage = Global.AppendToComment(mMessage, errorMessage);
                    criticalError = true;
                    return false;
                }

                var topFDExe = new FileInfo(topFDProgLoc);

                if (!topFDExe.Exists)
                {
                    mMessage = Global.AppendToComment(mMessage, "File not found: " + topFDExe.FullName);
                    criticalError = true;
                    return false;
                }

                // SQL Server: Data Source=gigasax;Initial Catalog=DMS_Pipeline
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms

                var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");

                var brokerDbConnectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrName);

                if (!useExistingTopFDResults)
                {
                    LogMessage("The settings file for job {0} has 'UseExistingTopFDResults' set to false", mJob);
                    return false;
                }

                // Part 1: Find other TopFD jobs for this dataset
                var jobStepsQuery =
                    "SELECT Job, Tool_Version, Output_Folder " +
                    "FROM V_Job_Steps_History_Export " +
                    "WHERE Tool = 'TopFD' AND " +
                    "      Dataset_ID = " + datasetID + " AND " +
                    "      State = 5 " +
                    "ORDER BY Job Desc";

                var dbToolsDMSPipeline = DbToolsFactory.GetDBTools(brokerDbConnectionStringToUse, debugMode: mMgrParams.TraceMode);
                RegisterEvents(dbToolsDMSPipeline);

                var successForJobs = dbToolsDMSPipeline.GetQueryResults(jobStepsQuery, out var jobStepsResults);

                if (!successForJobs || jobStepsResults.Count == 0)
                    return false;

                var jobCandidates = new Dictionary<int, TopFDJobInfoType>();

                var dateMatcher = new Regex(@"\d+-\d+-\d+ \d+:\d+:\d+ (AM|PM)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var result in jobStepsResults)
                {
                    // Parse out the TopFD.exe date from the tool version
                    // For example:
                    // TopFD 1.3.1; topfd.exe: 2020-01-13 10:55:30 AM
                    // or
                    // topfd.exe: 2023-12-18 07:37:36 AM

                    var job = result[0];

                    if (!int.TryParse(job, out var jobValue))
                    {
                        LogWarning("Dataset {0} has a non-numeric job in V_Job_Steps_History_Export: {1}", datasetID, job);
                        continue;
                    }

                    var toolVersion = result[1];
                    var resultsDirectory = result[2];

                    var charIndex = toolVersion.IndexOf("topfd.exe:", StringComparison.OrdinalIgnoreCase);

                    if (charIndex < 0)
                        continue;

                    // The executable date should be in the form defined by AnalysisToolRunnerBase.DATE_TIME_FORMAT

                    var topFDToolAndDate = toolVersion.Substring(charIndex);

                    var dateMatch = dateMatcher.Match(topFDToolAndDate);

                    if (!dateMatch.Success)
                    {
                        LogWarning("The TopFD tool version for dataset {0}, job {1} does not contain a date: {2}", datasetID, job, topFDToolAndDate);
                        continue;
                    }

                    if (!DateTime.TryParse(dateMatch.Value, out var existingJobToolDate))
                    {
                        LogWarning("The TopFD tool version for dataset {0}, job {1} is not a valid date: {2}", datasetID, job, topFDToolAndDate);
                        continue;
                    }

                    if (Math.Abs(topFDExe.LastWriteTime.Subtract(existingJobToolDate).TotalHours) > 3)
                    {
                        // The local topfd.exe file has a modification time more than 3 hours apart from that in the ToolVersion info
                        continue;
                    }

                    var topFDJobInfo = new TopFDJobInfoType
                    {
                        ToolVersion = toolVersion,
                        ResultsDirectoryName = resultsDirectory
                    };

                    jobCandidates.Add(jobValue, topFDJobInfo);
                }

                if (jobCandidates.Keys.Count == 0)
                {
                    // No jobs were found
                    return false;
                }

                var jobList = string.Join(",", jobCandidates.Keys);

                // SQL Server: Data Source=gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms

                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                // Part 2: Determine the settings files for the jobs in jobCandidates
                var settingsFileQuery = "SELECT Job, Settings_File_Name " +
                                        "FROM V_Analysis_Job_Export_DataPkg " +
                                        "WHERE Job in (" + jobList + ") " +
                                        "ORDER BY Job Desc";

                var dbToolsDMS = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
                RegisterEvents(dbToolsDMS);

                var successForSettingsFiles = dbToolsDMS.GetQueryResults(settingsFileQuery, out var settingsFileResults);

                if (!successForSettingsFiles || settingsFileResults.Count == 0)
                    return false;

                foreach (var result in settingsFileResults)
                {
                    var job = result[0];

                    if (!int.TryParse(job, out var jobValue))
                    {
                        LogWarning("Dataset {0} has a non-numeric job in V_Analysis_Job_Export: {1}", datasetID, job);
                        continue;
                    }

                    var settingsFile = result[1].Trim();

                    if (!settingsFile.Equals(settingsFileToFind))
                        continue;

                    // This job used the same settings file (and was processed using the same version of TopFD as C:\DMS_Programs\TopPIC\topfd.exe; see above)
                    // Use its existing TopFD results

                    var datasetStoragePath = mJobParams.GetParam("DatasetStoragePath");
                    var datasetDirectoryPath = Path.Combine(datasetStoragePath, mJobParams.GetParam(JOB_PARAM_DATASET_FOLDER_NAME));
                    var resultsDirectoryPath = Path.Combine(datasetDirectoryPath, jobCandidates[jobValue].ResultsDirectoryName);

                    var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);

                    if (!resultsDirectory.Exists)
                    {
                        LogWarning("Existing results directory not found for dataset {0}, job {1}: {2}", datasetID, job, resultsDirectoryPath);
                        continue;
                    }

                    if (resultsDirectory.GetFiles().Length == 0)
                    {
                        LogWarning("Existing results directory is empty for dataset {0}, job {1}: {2}", datasetID, job, resultsDirectoryPath);
                        continue;
                    }

                    var existingResultsDirectory = resultsDirectory.FullName;
                    var toolVersionInfo = jobCandidates[jobValue].ToolVersion;

                    mJobParams.AddAdditionalParameter("StepParameters", JOB_PARAM_EXISTING_TOPFD_RESULTS_DIRECTORY, existingResultsDirectory);
                    mJobParams.AddAdditionalParameter("StepParameters", JOB_PARAM_EXISTING_TOPFD_TOOL_VERSION, toolVersionInfo);

                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                LogError("Error looking for existing TopFD results: " + ex.Message);
                return false;
            }
        }
    }
}
