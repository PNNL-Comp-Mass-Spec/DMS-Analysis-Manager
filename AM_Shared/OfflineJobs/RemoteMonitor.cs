using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISM;
using Renci.SshNet.Sftp;

namespace AnalysisManagerBase.OfflineJobs
{
    /// <summary>
    /// Checks the status of a task running remotely
    /// </summary>
    /// <remarks>
    /// If complete, retrieves the results, then calls the appropriate plugin
    /// so it can perform any post-processing tasks that require domain accessible resources
    /// </remarks>
    public class RemoteMonitor : EventNotifier
    {
        // Ignore Spelling: Unstarted

        private const int STALE_LOCK_FILE_AGE_HOURS = 24;

        private const int STALE_JOBSTATUS_FILE_AGE_HOURS = 24;

        /// <summary>
        /// Name of the directory under which completed status files will be archived
        /// Files will be stored in a year-based directory, e.g. /Completed/2018/
        /// </summary>
        public const string ARCHIVED_TASK_QUEUE_DIRECTORY_NAME = "Completed";

        /// <summary>
        /// Remote job status
        /// </summary>
        public enum RemoteJobStatusCodes
        {
            /// <summary>
            /// Undefined
            /// </summary>
            Undefined = 0,

            /// <summary>
            /// Unstarted
            /// </summary>
            Unstarted = 1,

            /// <summary>
            /// Running
            /// </summary>
            Running = 2,

            /// <summary>
            /// Success
            /// </summary>
            Success = 3,

            /// <summary>
            /// Failed
            /// </summary>
            Failed = 4
        }

        /// <summary>
        /// Debug level
        /// </summary>
        public int DebugLevel { get; }

        /// <summary>
        /// Explanation of what happened to last operation this class performed
        /// </summary>
        /// <remarks>Typically used to report error or warning messages</remarks>
        public string Message { get; private set; }

        /// <summary>
        /// Job parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisJob</remarks>
        private IJobParams JobParams { get; }

        /// <summary>
        /// Manager parameters
        /// </summary>
        private IMgrParams MgrParams { get; }

        /// <summary>
        /// Remote processing progress
        /// </summary>
        public float RemoteProgress { get; private set; }

        /// <summary>
        /// Transfer utility
        /// </summary>
        public RemoteTransferUtility TransferUtility { get; }

        private IToolRunner ToolRunner { get; }

        private StatusFile StatusTools { get; }

        /// <summary>
        /// Working directory
        /// </summary>
        public string WorkDir { get; }

        /// <summary>
        /// Dataset Name
        /// </summary>
        public string DatasetName { get; }

        /// <summary>
        /// Job number
        /// </summary>
        public int JobNum { get; }

        /// <summary>
        /// Step number
        /// </summary>
        public int StepNum { get; }

        /// <summary>
        /// Cache of remote status files for this job
        /// Tracks full file paths
        /// </summary>
        private readonly SortedSet<string> mCachedStatusFiles;

        private bool mParametersUpdated;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Instantiation of RemoteTransferUtility will throw an exception if key manager parameters are not defined</remarks>
        public RemoteMonitor(
            IMgrParams mgrParams,
            IJobParams jobParams,
            IToolRunner toolRunner,
            StatusFile statusTools)
        {
            MgrParams = mgrParams;
            JobParams = jobParams;

            WorkDir = mgrParams.GetParam("WorkDir");
            DebugLevel = mgrParams.GetParam("DebugLevel", 2);

            ToolRunner = toolRunner;
            StatusTools = statusTools;

            JobNum = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            StepNum = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            DatasetName = AnalysisResources.GetDatasetName(jobParams);

            TransferUtility = new RemoteTransferUtility(mgrParams, jobParams);
            RegisterEvents(TransferUtility);

            mCachedStatusFiles = new SortedSet<string>();
            mParametersUpdated = false;
        }

        /// <summary>
        /// Delete WorkDir files on the remote host and archive status files
        /// </summary>
        /// <remarks>Will use the cached status file names if GetRemoteJobStatus was previously called</remarks>
        /// <returns>True on success, otherwise failure</returns>
        public bool DeleteRemoteJobFiles()
        {
            try
            {
                if (!mParametersUpdated)
                {
                    TransferUtility.UpdateParameters(false);
                    mParametersUpdated = true;
                }
            }
            catch (Exception ex)
            {
                LogError("Error initializing parameters for the remote transfer utility", ex);
                return false;
            }

            try
            {
                OnDebugEvent("Deleting remote WorkDir files for " + TransferUtility.JobStepDescription);

                TransferUtility.DeleteRemoteWorkDir();
            }
            catch (Exception ex)
            {
                LogError("Error deleting WorkDir files for the remotely running job", ex);
                return false;
            }

            try
            {
                OnDebugEvent("Archiving status files for " + TransferUtility.JobStepDescription);

                SortedSet<string> statusFiles;

                if (mCachedStatusFiles.Count > 0)
                {
                    statusFiles = mCachedStatusFiles;
                }
                else
                {
                    statusFiles = new SortedSet<string>();

                    foreach (var statusFile in TransferUtility.GetStatusFiles())
                    {
                        statusFiles.Add(statusFile.Key);
                    }
                }

                if (statusFiles.Count == 0)
                    return true;

                var archiveDirPathBase = PathUtils.CombineLinuxPaths(TransferUtility.RemoteTaskQueuePath, ARCHIVED_TASK_QUEUE_DIRECTORY_NAME);
                var archiveDirPath = PathUtils.CombineLinuxPaths(archiveDirPathBase, DateTime.Now.Year.ToString());

                TransferUtility.CreateRemoteDirectories(new List<string> { archiveDirPathBase, archiveDirPath });

                // Do not transfer .info, .lock, or .jobstatus files to the archive folder

                var filesToDelete = new List<string>
                {
                    TransferUtility.StatusInfoFile,
                    TransferUtility.StatusLockFile,
                    TransferUtility.JobStatusFile
                };

                TransferUtility.MoveFiles(statusFiles, archiveDirPath, filesToDelete);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error deleting WorkDir files for the remotely running job", ex);
                return false;
            }
        }

        /// <summary>
        /// Determine the status of a remotely running job
        /// </summary>
        public RemoteJobStatusCodes GetRemoteJobStatus()
        {
            try
            {
                if (!mParametersUpdated)
                {
                    TransferUtility.UpdateParameters(false);
                    mParametersUpdated = true;
                }
            }
            catch (Exception ex)
            {
                LogError("Error initializing parameters for the remote transfer utility", ex);

                // Return .Undefined, which will fail out the job step
                return RemoteJobStatusCodes.Undefined;
            }

            try
            {
                OnDebugEvent("Retrieving status files for " + TransferUtility.JobStepDescription);

                var statusFiles = TransferUtility.GetStatusFiles();
                mCachedStatusFiles.Clear();

                foreach (var statusFile in statusFiles)
                {
                    mCachedStatusFiles.Add(statusFile.Key);
                }

                if (statusFiles.Count == 0)
                {
                    // No status files, not even a .info file
                    // Check whether the remote server is reachable
                    var taskFolderItems = TransferUtility.GetRemoteFilesAndDirectories(TransferUtility.RemoteTaskQueuePath);

                    if (taskFolderItems.Count > 0)
                    {
                        LogError("No status files were found for " + TransferUtility.JobStepDescription + ", not even a .info file");
                        return RemoteJobStatusCodes.Failed;
                    }

                    LogWarning("Remote task queue path is empty or not accessible; possible network outage");
                    return RemoteJobStatusCodes.Running;
                }

                // Look for a .lock file
                if (StatusFileExists(TransferUtility.StatusLockFile, statusFiles, out var remoteLockFile))
                {
                    // Check whether the job has finished (success or failure)
                    var jobFinished = StatusFileExists(TransferUtility.ProcessingSuccessFile, statusFiles, out _) ||
                                      StatusFileExists(TransferUtility.ProcessingFailureFile, statusFiles, out _);

                    if (StatusFileExists(TransferUtility.JobStatusFile, statusFiles, out var remoteJobStatusFile))
                    {
                        // .jobstatus file found; check the age
                        // If over 24 hours old, we probably have an issue
                        var statusFileAgeHours = DateTime.UtcNow.Subtract(remoteJobStatusFile.LastWriteTimeUtc).TotalHours;

                        if (statusFileAgeHours > STALE_JOBSTATUS_FILE_AGE_HOURS)
                        {
                            NotifyStaleJobStatusFile(remoteJobStatusFile.Name, (int)Math.Round(statusFileAgeHours, 0));
                        }

                        // Retrieve the .jobstatus file
                        OnDebugEvent("Retrieve status file {0} from {1} ", remoteJobStatusFile.Name, TransferUtility.RemoteHostName);

                        var success = TransferUtility.RetrieveJobStatusFile(out var jobStatusFilePathLocal);

                        if (!success)
                        {
                            // Log a warning, but return RemoteJobStatusCodes.running
                            LogWarning("Error retrieving the .jobstatus file for " + TransferUtility.JobStepDescription + " on " +
                                       TransferUtility.RemoteHostName);
                            return RemoteJobStatusCodes.Running;
                        }

                        var jobStatus = ParseJobStatusFile(jobStatusFilePathLocal);

                        if (!jobFinished)
                        {
                            return jobStatus;
                        }
                    }
                    else if (!jobFinished)
                    {
                        // A lock file exists, but no .jobstatus file exists yet, and a .success or .fail file was not found
                        // If the .lock file is over 24 hours old, we probably have an issue
                        var lockFileAgeHours = DateTime.UtcNow.Subtract(remoteLockFile.LastWriteTimeUtc).TotalHours;

                        if (lockFileAgeHours > STALE_LOCK_FILE_AGE_HOURS)
                        {
                            // Stale lock file; notify the calling class, but still return state "Running"
                            NotifyStaleLockFile(remoteLockFile.Name, (int)Math.Round(lockFileAgeHours, 0));
                        }

                        return RemoteJobStatusCodes.Running;
                    }
                }

                // Look for a .success file
                if (StatusFileExists(TransferUtility.ProcessingSuccessFile, statusFiles, out var remoteSuccessFile))
                {
                    OnStatusEvent(".success file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);

                    // Retrieve the .success file
                    OnDebugEvent("Retrieve status file {0} from {1} ", remoteSuccessFile.Name, TransferUtility.RemoteHostName);

                    TransferUtility.RetrieveStatusFile(remoteSuccessFile.Name, out var successFilePathLocal);

                    ReportRemoteManagerIsIdle(successFilePathLocal);

                    return RemoteJobStatusCodes.Success;
                }

                // Look for a .fail file
                if (StatusFileExists(TransferUtility.ProcessingFailureFile, statusFiles, out var remoteFailureFile))
                {
                    LogWarning(".fail file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);

                    TransferUtility.RetrieveStatusFile(remoteFailureFile.Name, out var failFilePathLocal);

                    ReportRemoteManagerIsIdle(failFilePathLocal);

                    return RemoteJobStatusCodes.Failed;
                }

                // No .lock file, .success file, or .fail file
                // There might be a .jobstatus file, but that would indicate things are in an unstable / unsupported state
                return RemoteJobStatusCodes.Unstarted;
            }
            catch (Exception ex)
            {
                LogError("Error reading the status file for the remotely running job", ex);

                // Return .Undefined, which will fail out the job step
                return RemoteJobStatusCodes.Undefined;
            }
        }

        private void LogError(string message, Exception ex = null)
        {
            Message = message;
            OnErrorEvent(message, ex);
        }

        private void LogMessage(string message)
        {
            OnStatusEvent(message);
        }

        private void LogWarning(string message)
        {
            Message = message;
            OnWarningEvent(message);
        }

        private void NotifyStaleJobStatusFile(string lockFileName, int ageHours)
        {
            Message = string.Format("JobStatus file has not been modified for over {0} hours", STALE_JOBSTATUS_FILE_AGE_HOURS);
            OnStaleJobStatusFileEvent(lockFileName, ageHours);
        }

        private void NotifyStaleLockFile(string lockFileName, int ageHours)
        {
            Message = string.Format("Lock file created over {0} hours ago, but a .jobstatus file has not yet been created", STALE_LOCK_FILE_AGE_HOURS);
            OnStaleLockFileEvent(lockFileName, ageHours);
        }

        private static Queue<KeyValuePair<DateTime, float>> ParseCoreUsageHistory(XContainer doc)
        {
            var coreUsageHistory = new Queue<KeyValuePair<DateTime, float>>();

            var coreUsageInfo = doc.Elements("Root").Elements("ProgRunnerCoreUsage").ToList();

            var coreUsage = coreUsageInfo.Elements("CoreUsageSample").ToList();

            foreach (var coreUsageSample in coreUsage)
            {
                string samplingDateText;

                if (coreUsageSample.HasAttributes)
                {
                    var dateAttribute = coreUsageSample.Attribute("Date");

                    if (dateAttribute == null)
                        continue;

                    samplingDateText = dateAttribute.Value;
                }
                else
                {
                    continue;
                }

                if (!DateTime.TryParse(samplingDateText, out var samplingDate))
                    continue;

                if (float.TryParse(coreUsageSample.Value, out var coresInUse))
                {
                    coreUsageHistory.Enqueue(new KeyValuePair<DateTime, float>(samplingDate, coresInUse));
                }
            }

            return coreUsageHistory;
        }

        /// <summary>
        /// Read the .jobstatus file retrieved from the remote host
        /// </summary>
        /// <param name="jobStatusFilePath">Job status file path</param>
        private RemoteJobStatusCodes ParseJobStatusFile(string jobStatusFilePath)
        {
            RemoteJobStatusCodes jobStatus;

            try
            {
                OnDebugEvent("Parse status file " + Path.GetFileName(jobStatusFilePath));

                using var reader = new StreamReader(new FileStream(jobStatusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and can often be easier to use since XDocument is LINQ-based
                var doc = XDocument.Parse(reader.ReadToEnd());

                var managerInfo = doc.Elements("Root").Elements("Manager").ToList();

                // Note: although we pass localStatusFilePath to the StatusFile constructor, the path doesn't matter because
                // we call UpdateRemoteStatus to push the remote status to the message queue only; a status file is not written
                var localStatusFilePath = Path.Combine(WorkDir, "RemoteStatus.xml");

                var status = new StatusFile(MgrParams, localStatusFilePath, DebugLevel)
                {
                    MgrName = XMLUtils.GetXmlValue(managerInfo, "MgrName")
                };
                RegisterEvents(status);

                // Note: do not configure status to push to the BrokerDB or the MessageQueue
                // Instead, use the .jobstatus file to populate status, then call StatusTools.UpdateRemoteStatus
                // which calls WriteStatusFile with writeToDisk=false, which results in the remote status info getting pushed to the MessageQueue
                // The Status Message DB Updater will then push that info into table T_Processor_Status in the DMS_Pipeline database

                var mgrStatus = XMLUtils.GetXmlValue(managerInfo, "MgrStatus");
                status.MgrStatus = StatusTools.ConvertToMgrStatusFromText(mgrStatus);

                status.TaskStartTime = XMLUtils.GetXmlValue(managerInfo, "LastStartTime", DateTime.MinValue).ToUniversalTime();

                if (status.TaskStartTime > DateTime.MinValue)
                {
                    status.TaskStartTime = status.TaskStartTime.ToUniversalTime();
                }

                var lastUpdate = XMLUtils.GetXmlValue(managerInfo, "LastUpdate", DateTime.MinValue);

                if (lastUpdate > DateTime.MinValue)
                {
                    lastUpdate = lastUpdate.ToUniversalTime();
                }
                else
                {
                    lastUpdate = status.TaskStartTime.ToUniversalTime();
                }

                var cpuUtilization = (int)XMLUtils.GetXmlValue(managerInfo, "CPUUtilization", 0f);
                var freeMemoryMB = XMLUtils.GetXmlValue(managerInfo, "FreeMemoryMB", 0f);
                var processId = XMLUtils.GetXmlValue(managerInfo, "ProcessID", 0);

                status.ProgRunnerProcessID = XMLUtils.GetXmlValue(managerInfo, "ProgRunnerProcessID", 0);

                status.ProgRunnerCoreUsage = XMLUtils.GetXmlValue(managerInfo, "ProgRunnerCoreUsage", 0f);

                var taskInfo = doc.Elements("Root").Elements("Task").ToList();

                status.Tool = XMLUtils.GetXmlValue(taskInfo, "Tool");

                var taskStatus = XMLUtils.GetXmlValue(taskInfo, "Status");
                status.TaskStatus = StatusTools.ConvertToTaskStatusFromText(taskStatus);

                switch (status.TaskStatus)
                {
                    case TaskStatusCodes.STOPPED:
                    case TaskStatusCodes.REQUESTING:
                    case TaskStatusCodes.NO_TASK:
                        // The .jobstatus file in the Task Queue directory should not have these task status values
                        // Return .Undefined, which will fail out the job step
                        jobStatus = RemoteJobStatusCodes.Undefined;
                        break;

                    case TaskStatusCodes.RUNNING:
                    case TaskStatusCodes.CLOSING:
                        jobStatus = RemoteJobStatusCodes.Running;
                        break;

                    case TaskStatusCodes.FAILED:
                        jobStatus = RemoteJobStatusCodes.Failed;
                        break;

                    default:
                        // Unrecognized task status
                        // Return .Undefined, which will fail out the job step
                        jobStatus = RemoteJobStatusCodes.Undefined;
                        break;
                }

                status.Progress = XMLUtils.GetXmlValue(taskInfo, "Progress", 0f);
                status.CurrentOperation = XMLUtils.GetXmlValue(taskInfo, "CurrentOperation");

                var taskDetails = taskInfo.Elements("TaskDetails").ToList();

                var taskStatusDetail = XMLUtils.GetXmlValue(taskDetails, "Status");
                status.TaskStatusDetail = StatusTools.ConvertToTaskDetailStatusFromText(taskStatusDetail);

                status.JobNumber = XMLUtils.GetXmlValue(taskDetails, "Job", 0);
                status.JobStep = XMLUtils.GetXmlValue(taskDetails, "Step", 0);
                status.Dataset = XMLUtils.GetXmlValue(taskDetails, "Dataset");
                status.MostRecentLogMessage = XMLUtils.GetXmlValue(taskDetails, "MostRecentLogMessage");
                status.MostRecentJobInfo = XMLUtils.GetXmlValue(taskDetails, "MostRecentJobInfo");
                status.SpectrumCount = XMLUtils.GetXmlValue(taskDetails, "SpectrumCount", 0);

                var coreUsageHistory = ParseCoreUsageHistory(doc);

                status.StoreCoreUsageHistory(coreUsageHistory);

                // Cache some remote status values so that SetAnalysisJobComplete can use them
                JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                    RemoteTransferUtility.STEP_PARAM_REMOTE_PROGRESS,
                    status.Progress.ToString("0.0###"));

                // Store the remote start time, converting to UTC via format code "{0:O}"
                JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                    RemoteTransferUtility.STEP_PARAM_REMOTE_START,
                    string.Format("{0:O}", status.TaskStartTime));

                if (status.Progress >= 100)
                {
                    // Store the remote finish time, converting to UTC via format code "{0:O}"
                    JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                        RemoteTransferUtility.STEP_PARAM_REMOTE_FINISH,
                        string.Format("{0:O}", lastUpdate));
                }
                else
                {
                    JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                        RemoteTransferUtility.STEP_PARAM_REMOTE_FINISH,
                        string.Empty);
                }

                // Update the cached progress; used by MainProcess
                RemoteProgress = status.Progress;

                StatusTools.UpdateRemoteStatus(status, lastUpdate, processId, cpuUtilization, freeMemoryMB);
            }
            catch (Exception ex)
            {
                // Error parsing the file
                // This can happen if an incomplete XML file was retrieved (since the remote manager was in the middle of writing to it)
                OnErrorEvent("Error reading the .jobstatus file for the remotely running job", ex);
                jobStatus = RemoteJobStatusCodes.Undefined;
            }

            return jobStatus;
        }

        /// <summary>
        /// Parse a .success or .fail file retrieved from the remote host
        /// </summary>
        /// <param name="statusResultFilePath">Job .success or .fail status file path</param>
        /// <param name="toolRunnerResult">ToolRunner result</param>
        /// <param name="completionMessage">Completion message</param>
        /// <param name="remoteStart">Processing start time on the remote host (local time)</param>
        /// <param name="remoteFinish">Processing end time on the remote host (local time)</param>
        public bool ParseStatusResultFile(
            string statusResultFilePath,
            out CloseOutType toolRunnerResult,
            out string completionMessage,
            out DateTime remoteStart,
            out DateTime remoteFinish)
        {
            completionMessage = string.Empty;
            remoteStart = DateTime.MinValue;
            remoteFinish= DateTime.MinValue;

            try
            {
                var statusResultFile = new FileInfo(statusResultFilePath);

                if (!statusResultFile.Exists)
                {
                    LogError("Status result file not found: " + statusResultFile.FullName);
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                var remoteJob = 0;
                var remoteStep = 0;

                var compCodeFound = false;
                var evalCodeFound = false;

                var compCode = 0;

                var evalCode = 0;
                var evalMessage = string.Empty;

                using (var reader = new StreamReader(new FileStream(statusResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    char[] sepChars = { '=' };

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var lineParts = dataLine.Split(sepChars, 2);

                        if (lineParts.Length < 2)
                        {
                            LogWarning(string.Format("Ignoring invalid line in status file {0}: {1}", statusResultFile.Name, dataLine));
                            continue;
                        }

                        if (lineParts[0] == "CompCode")
                            compCodeFound = true;

                        if (lineParts[0] == "EvalCode")
                            evalCodeFound = true;

                        if (string.IsNullOrWhiteSpace(lineParts[1]))
                        {
                            continue;
                        }

                        switch (lineParts[0])
                        {
                            case "Job":
                                int.TryParse(lineParts[1], out remoteJob);
                                break;

                            case "Step":
                                int.TryParse(lineParts[1], out remoteStep);
                                break;

                            case "CompCode":
                                int.TryParse(lineParts[1], out compCode);
                                break;

                            case "CompMsg":
                                completionMessage = lineParts[1];
                                LogWarning(string.Format("Completion message for job {0} run remotely: {1}", JobNum, completionMessage));
                                break;

                            case "EvalCode":
                                int.TryParse(lineParts[1], out evalCode);
                                break;

                            case "EvalMsg":
                                evalMessage = lineParts[1];
                                break;

                            case "Started":
                                if (DateTime.TryParse(lineParts[1], out var startDate))
                                    remoteStart = startDate;
                                break;

                            case "Finished":
                                if (DateTime.TryParse(lineParts[1], out var endDate))
                                    remoteFinish = endDate;
                                break;

                            case "StepTool":
                            case "WorkDir":
                            case "Staged":
                            case "MgrName":
                                // Ignore these lines
                                break;

                            default:
                                LogWarning("Skipping unrecognized line label: " + lineParts[0]);
                                break;
                        }
                    }
                }

                if (remoteJob == 0)
                {
                    LogError("Status file retrieved from remote host does not have Job listed");
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (remoteStep == 0)
                {
                    LogError("Status file retrieved from remote host does not have Step listed");
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (remoteJob != JobNum)
                {
                    LogError(string.Format("Status file retrieved from remote host has the wrong job number: {0} vs. {1}", remoteJob, JobNum));
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (remoteStep != StepNum)
                {
                    LogError(string.Format("Status file retrieved from remote host has the wrong step number for job {0}: {1} vs. {2}", JobNum, remoteStep, StepNum));
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (!(compCodeFound && evalCodeFound))
                {
                    LogError("Status file retrieved from remote host is missing CompCode or EvalCode");
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (!Enum.TryParse(compCode.ToString(), out toolRunnerResult))
                {
                    LogError("Status file retrieved from remote host has an invalid completion code: " + compCode);
                    toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                    return false;
                }

                if (toolRunnerResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    LogWarning(string.Format("Completion code for job {0} run remotely: {1}", JobNum, toolRunnerResult));
                }

                ToolRunner.UpdateEvalCode(evalCode, evalMessage);

                if (evalCode != 0)
                {
                    LogMessage(string.Format("Evaluation code for job {0} run remotely: {1}, {2}", JobNum, evalCode, evalMessage));
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception parsing the remote job status file " + statusResultFilePath, ex);
                toolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }
        }

        /// <summary>
        /// Send a status update to the MessageQueue in order to update the remote manager status in T_Processor_Status
        /// </summary>
        /// <param name="statusResultFilePath">Job .success or .fail status file path</param>
        private void ReportRemoteManagerIsIdle(string statusResultFilePath)
        {
            try
            {
                var statusResultFile = new FileInfo(statusResultFilePath);

                OnDebugEvent("Parse status file " + statusResultFile.Name);

                var remoteMgrName = "";
                var taskStartTime = DateTime.MinValue;
                var taskEndTime = DateTime.MinValue;

                using var reader = new StreamReader(new FileStream(statusResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                char[] sepChars = { '=' };

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split(sepChars, 2);

                    if (lineParts.Length < 2)
                    {
                        LogWarning(string.Format("Ignoring invalid line in status file {0}: {1}", statusResultFile.Name, dataLine));
                        continue;
                    }

                    if (lineParts[0] == "MgrName")
                        remoteMgrName = lineParts[1];

                    // Start time is based on local time
                    if (lineParts[0] == "Started")
                        DateTime.TryParse(lineParts[1], out taskStartTime);

                    // Finish time is based on local time
                    if (lineParts[0] == "Finished")
                        DateTime.TryParse(lineParts[1], out taskEndTime);
                }

                if (string.IsNullOrWhiteSpace(remoteMgrName))
                {
                    OnErrorEvent("File {0} did not contain parameter MgrName; cannot update status for the remote manager", statusResultFile.Name);
                    return;
                }

                // Note: although we pass localStatusFilePath to the StatusFile constructor, the path doesn't matter because
                // we call UpdateRemoteStatus to push the remote status to the message queue only; a status file is not written
                var localStatusFilePath = Path.Combine(WorkDir, "RemoteStatus.xml");

                var status = new StatusFile(MgrParams, localStatusFilePath, DebugLevel)
                {
                    MgrName = remoteMgrName
                };
                RegisterEvents(status);

                // Note: do not configure status to push to the BrokerDB or the MessageQueue
                // Instead, use the .jobstatus file to populate status, then call StatusTools.UpdateRemoteStatus
                // which calls WriteStatusFile with writeToDisk=false, which results in the remote status info getting pushed to the MessageQueue

                status.MgrStatus = MgrStatusCodes.STOPPED;

                if (taskStartTime > DateTime.MinValue)
                    status.TaskStartTime = taskStartTime.ToUniversalTime();
                else
                    status.TaskStartTime = taskStartTime;

                DateTime lastUpdate;

                if (taskEndTime > DateTime.MinValue)
                    lastUpdate = taskEndTime.ToUniversalTime();
                else
                    lastUpdate = statusResultFile.LastWriteTimeUtc;

                const int cpuUtilization = 0;
                const int freeMemoryMB = 0;
                const int processId = 0;

                status.ProgRunnerProcessID = 0;

                status.ProgRunnerCoreUsage = 0;

                status.Tool = string.Empty;

                status.TaskStatus = TaskStatusCodes.NO_TASK;

                if (statusResultFile.Name.EndsWith(".success"))
                    status.Progress = 100;
                else
                    status.Progress = 0;

                status.CurrentOperation = string.Empty;

                status.TaskStatusDetail = TaskStatusDetailCodes.NO_TASK;

                status.JobNumber = 0;
                status.JobStep = 0;
                status.Dataset = string.Empty;

                // Update the cached progress; used by MainProcess
                RemoteProgress = status.Progress;

                StatusTools.UpdateRemoteStatus(status, lastUpdate, processId, cpuUtilization, freeMemoryMB);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReportRemoteManagerIsIdle reading the .success or .fail file for the remotely running job", ex);
            }
        }

        /// <summary>
        /// Look for file statusFileName in dictionary statusFiles
        /// </summary>
        /// <param name="statusFileName">Status file to find</param>
        /// <param name="statusFiles">List of status files</param>
        /// <param name="matchedFile">Output: matched status file, or null if no match</param>
        /// <returns>True if found, otherwise false</returns>
        private bool StatusFileExists(string statusFileName, Dictionary<string, SftpFile> statusFiles, out SftpFile matchedFile)
        {
            foreach (var item in statusFiles)
            {
                if (!string.Equals(item.Value.Name, statusFileName, StringComparison.OrdinalIgnoreCase))
                    continue;

                matchedFile = item.Value;
                return true;
            }

            matchedFile = null;
            return false;
        }

        /// <summary>
        /// Delegate for StaleJobStatusFileEvent and StaleLockFileEvent
        /// </summary>
        /// <param name="fileName">File name</param>
        /// <param name="ageHours">Age, in hours</param>
        public delegate void StaleFileEventHandler(string fileName, int ageHours);

        /// <summary>
        /// Stale job status file event handler
        /// </summary>
        public event StaleFileEventHandler StaleJobStatusFileEvent;

        /// <summary>
        /// Stale lock file event handler
        /// </summary>
        public event StaleFileEventHandler StaleLockFileEvent;

        /// <summary>
        /// Raise an event indicating that the .jobstatus file is stale (modified over 24 hours ago)
        /// </summary>
        /// <param name="fileName">File name</param>
        /// <param name="ageHours">Age, in hours</param>
        private void OnStaleJobStatusFileEvent(string fileName, int ageHours)
        {
            StaleJobStatusFileEvent?.Invoke(fileName, ageHours);
        }

        /// <summary>
        /// Raise an event indicating that the lock file is stale (modified over 24 hours ago)
        /// </summary>
        /// <param name="fileName">File name</param>
        /// <param name="ageHours">Age, in hours</param>
        private void OnStaleLockFileEvent(string fileName, int ageHours)
        {
            StaleLockFileEvent?.Invoke(fileName, ageHours);
        }
    }
}
