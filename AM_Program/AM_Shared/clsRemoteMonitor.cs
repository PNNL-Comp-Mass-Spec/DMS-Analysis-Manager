using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using PRISM;
using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Checks the status of a task running remotely
    /// </summary>
    /// <remarks>
    /// If complete, retrieves the results, then calls the appropriate plugin
    /// so it can perform any post-processing tasks that require domain accessible resources
    /// </remarks>
    public class clsRemoteMonitor : clsEventNotifier
    {

        #region "Constants"

        private int STALE_LOCK_FILE_AGE_HOURS = 24;

        private int STALE_JOBSTATUS_FILE_AGE_HOURS = 24;

        #endregion

        #region "Enums"

        public enum EnumRemoteJobStatus
        {
            Undefined = 0,
            Unstarted = 1,
            Running = 2,
            Success = 3,
            Failed = 4
        }

        #endregion

        #region "Properties"

        public int DebugLevel { get; }

        /// <summary>
        /// Explanation of what happened to last operation this class performed
        /// </summary>
        /// <remarks>Typically used to report error or warning messages</remarks>
        public string Message { get; private set; }

        private IJobParams JobParams { get; }

        private IMgrParams MgrParams { get; }

        public float RemoteProgress { get; private set; }

        public clsRemoteTransferUtility TransferUtility { get; }

        private IToolRunner ToolRunner { get; }

        private clsStatusFile StatusTools { get; }

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

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Instantiation of clsRemoteTransferUtility will throw an exception if key manager parameters are not defined</remarks>
        public clsRemoteMonitor(
            IMgrParams mgrParams,
            IJobParams jobParams,
            IToolRunner toolRunner,
            clsStatusFile statusTools)
        {

            MgrParams = mgrParams;
            JobParams = jobParams;

            WorkDir = mgrParams.GetParam("workdir");
            DebugLevel = mgrParams.GetParam("debuglevel", 2);

            ToolRunner = toolRunner;
            StatusTools = statusTools;

            JobNum = jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            StepNum = jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            DatasetName = jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetNum");

            TransferUtility = new clsRemoteTransferUtility(mgrParams, jobParams);
        }

        /// <summary>
        /// Determine the status of a remotely running job
        /// </summary>
        /// <returns></returns>
        public EnumRemoteJobStatus GetRemoteJobStatus()
        {
            try
            {
                TransferUtility.UpdateParameters(false);
            }
            catch (Exception ex)
            {
                LogError("Error initializing parameters for the remote transfer utility", ex);

                // Return .Undefined, which will fail out the job step
                return EnumRemoteJobStatus.Undefined;
            }

            try
            {

                OnDebugEvent("Retrieving status files for "  + TransferUtility.JobStepDescription);

                var statusFiles = TransferUtility.GetStatusFiles();

                if (statusFiles.Count == 0)
                {
                    // No status files, not even a .info file
                    // This could be due to a network outage issue so we'll return .Running
                    LogWarning("No status files were found for " + TransferUtility.JobStepDescription + ", not even a .info file");

                    return EnumRemoteJobStatus.Running;
                }

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
                        OnDebugEvent(string.Format("Retrieve status file {0} from {1} ", remoteJobStatusFile.Name, TransferUtility.RemoteHostName));

                        var success = TransferUtility.RetrieveJobStatusFile(out var jobStatusFilePathLocal);

                        if (!success)
                        {
                            // Log a warning, but return .running
                            LogWarning("Error retrieving the .jobstatus file for " + TransferUtility.JobStepDescription + " on " +
                                       TransferUtility.RemoteHostName);
                            return EnumRemoteJobStatus.Running;
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

                        return EnumRemoteJobStatus.Running;
                    }
                }

                // Look for a .success file
                if (StatusFileExists(TransferUtility.ProcessingSuccessFile, statusFiles, out var remoteSuccessFile))
                {
                    OnStatusEvent(".success file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);

                    // Retrieve the .success file
                    OnDebugEvent(string.Format("Retrieve status file {0} from {1} ", remoteSuccessFile.Name, TransferUtility.RemoteHostName));

                    TransferUtility.RetrieveStatusFile(remoteSuccessFile.Name, out _);

                    return EnumRemoteJobStatus.Success;
                }

                // Look for a .fail file
                if (StatusFileExists(TransferUtility.ProcessingFailureFile, statusFiles, out var remoteFailureFile))
                {
                    LogWarning(".fail file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);

                    TransferUtility.RetrieveStatusFile(remoteFailureFile.Name, out _);

                    return EnumRemoteJobStatus.Failed;
                }

                // No .lock file, .success file, or .fail file
                // There might be a .jobstatus file, but that would indicate things are in an unstable / unsupported state
                return EnumRemoteJobStatus.Unstarted;
            }
            catch (Exception ex)
            {
                LogError("Error reading the status file for the remotely running job", ex);

                // Return .Undefined, which will fail out the job step
                return EnumRemoteJobStatus.Undefined;
            }

        }

        private void LogError(string message, Exception ex)
        {
            Message = message;
            OnErrorEvent(message, ex);
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
        /// <param name="jobStatusFilePath"></param>
        /// <remarks></remarks>
        private EnumRemoteJobStatus ParseJobStatusFile(string jobStatusFilePath)
        {
            EnumRemoteJobStatus jobStatus;

            try
            {
                OnDebugEvent("Parse status file " + Path.GetFileName(jobStatusFilePath));

                using (var reader = new StreamReader(new FileStream(jobStatusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Note that XDocument supersedes XmlDocument and can often be easier to use since XDocument is LINQ-based
                    var doc = XDocument.Parse(reader.ReadToEnd());

                    var managerInfo = doc.Elements("Root").Elements("Manager").ToList();

                    // Note: although we pass statusFilePath to the clsStatusFile constructor, the path doesn't matter because
                    // we call UpdateRemoteStatus to push the remote status to the message queue only; a status file is not written
                    var statusFilePath = Path.Combine(WorkDir, "RemoteStatus.xml");

                    var status = new clsStatusFile(statusFilePath, DebugLevel)
                    {
                        MgrName = clsXMLUtils.GetXmlValue(managerInfo, "MgrName")
                    };

                    // Note: do not configure status to push to the BrokerDB or the MessageQueue

                    var mgrStatus = clsXMLUtils.GetXmlValue(managerInfo, "MgrStatus");
                    status.MgrStatus = StatusTools.ConvertToMgrStatusFromText(mgrStatus);

                    status.TaskStartTime = clsXMLUtils.GetXmlValue(managerInfo, "LastStartTime", DateTime.MinValue).ToUniversalTime();
                    if (status.TaskStartTime > DateTime.MinValue)
                    {
                        status.TaskStartTime = status.TaskStartTime.ToUniversalTime();
                    }

                    var lastUpdate = clsXMLUtils.GetXmlValue(managerInfo, "LastUpdate", DateTime.MinValue);
                    if (lastUpdate > DateTime.MinValue)
                    {
                        lastUpdate = lastUpdate.ToUniversalTime();
                    }
                    else
                    {
                        lastUpdate = status.TaskStartTime.ToUniversalTime();
                    }

                    var cpuUtilization = (int)clsXMLUtils.GetXmlValue(managerInfo, "CPUUtilization", 0f);
                    var freeMemoryMB = clsXMLUtils.GetXmlValue(managerInfo, "FreeMemoryMB", 0f);
                    var processId = clsXMLUtils.GetXmlValue(managerInfo, "ProcessID", 0);

                    status.ProgRunnerProcessID = clsXMLUtils.GetXmlValue(managerInfo, "ProgRunnerProcessID", 0);

                    status.ProgRunnerCoreUsage = clsXMLUtils.GetXmlValue(managerInfo, "ProgRunnerCoreUsage", 0f);

                    var taskInfo = doc.Elements("Root").Elements("Task").ToList();

                    status.Tool = clsXMLUtils.GetXmlValue(taskInfo, "Tool");

                    var taskStatus = clsXMLUtils.GetXmlValue(taskInfo, "Status");
                    status.TaskStatus = StatusTools.ConvertToTaskStatusFromText(taskStatus);

                    switch (status.TaskStatus)
                    {
                        case EnumTaskStatus.STOPPED:
                        case EnumTaskStatus.REQUESTING:
                        case EnumTaskStatus.NO_TASK:
                            // The .jobstatus file in the Task Queue folder should not have these task status values
                            // Return .Undefined, which will fail out the job step
                            jobStatus = EnumRemoteJobStatus.Undefined;
                            break;

                        case EnumTaskStatus.RUNNING:
                        case EnumTaskStatus.CLOSING:
                            jobStatus = EnumRemoteJobStatus.Running;
                            break;

                        case EnumTaskStatus.FAILED:
                            jobStatus = EnumRemoteJobStatus.Failed;
                            break;

                        default:
                            // Unrecognized task status
                            // Return .Undefined, which will fail out the job step
                            jobStatus = EnumRemoteJobStatus.Undefined;
                            break;
                    }

                    status.Progress = clsXMLUtils.GetXmlValue(taskInfo, "Progress", 0f);
                    status.CurrentOperation = clsXMLUtils.GetXmlValue(taskInfo, "CurrentOperation");

                    var taskDetails = taskInfo.Elements("TaskDetails").ToList();

                    var taskStatusDetail = clsXMLUtils.GetXmlValue(taskDetails, "Status");
                    status.TaskStatusDetail = StatusTools.ConvertToTaskDetailStatusFromText(taskStatusDetail);

                    status.JobNumber = clsXMLUtils.GetXmlValue(taskDetails, "Job", 0);
                    status.JobStep = clsXMLUtils.GetXmlValue(taskDetails, "Step", 0);
                    status.Dataset = clsXMLUtils.GetXmlValue(taskDetails, "Dataset");
                    status.MostRecentLogMessage = clsXMLUtils.GetXmlValue(taskDetails, "MostRecentLogMessage");
                    status.MostRecentJobInfo = clsXMLUtils.GetXmlValue(taskDetails, "MostRecentJobInfo");
                    status.SpectrumCount = clsXMLUtils.GetXmlValue(taskDetails, "SpectrumCount", 0);

                    var coreUsageHistory = ParseCoreUsageHistory(doc);

                    status.StoreCoreUsageHistory(coreUsageHistory);

                    JobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsRemoteTransferUtility.STEP_PARAM_REMOTE_PROGRESS,
                                                     status.Progress.ToString("0.0###"));

                    // Update the cached progress; used by clsMainProcess
                    RemoteProgress = status.Progress;

                    StatusTools.UpdateRemoteStatus(status, lastUpdate, processId, cpuUtilization, freeMemoryMB);

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error reading the .jobstatus file for the remotely running job", ex);
                jobStatus = EnumRemoteJobStatus.Undefined;
            }

            return jobStatus;

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

        #endregion


        #region "Events"

        public delegate void StaleFileEventHandler(string fileName, int ageHours);

        public event StaleFileEventHandler StaleJobStatusFileEvent;

        public event StaleFileEventHandler StaleLockFileEvent;

        private void OnStaleJobStatusFileEvent(string fileName, int ageHours)
        {
            StaleJobStatusFileEvent?.Invoke(fileName, ageHours);
        }

        private void OnStaleLockFileEvent(string fileName, int ageHours)
        {
            StaleLockFileEvent?.Invoke(fileName, ageHours);
        }

        #endregion
    }

}
