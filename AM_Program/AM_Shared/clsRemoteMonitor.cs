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

            JobNum = jobParams.GetJobParameter("StepParameters", "Job", 0);
            StepNum = jobParams.GetJobParameter("StepParameters", "Step", 0);
            DatasetName = jobParams.GetParam("JobParameters", "DatasetNum");

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

                OnDebugEvent("Retrieving status files for "  + TransferUtility.JobStepDescription);

                var statusFiles = TransferUtility.GetStatusFiles();

                if (statusFiles.Count == 0)
                {
                    LogWarning("No status files were found for " + TransferUtility.JobStepDescription);
                    return EnumRemoteJobStatus.Undefined;
                }

                if (StatusFileExists(statusFiles, TransferUtility.ProcessingFailureFile))
                {
                    LogWarning(".fail file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);
                    return EnumRemoteJobStatus.Failed;
                }

                if (StatusFileExists(statusFiles, TransferUtility.ProcessingSuccessFile))
                {
                    OnStatusEvent(".success file found for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);
                    return EnumRemoteJobStatus.Success;
                }

                if (StatusFileExists(statusFiles, TransferUtility.JobStatusFile))
                {
                    // .jobstatus file found; retrieve it

                    OnDebugEvent(string.Format("Retrieve status file {0} from {1} ", TransferUtility.JobStatusFile, TransferUtility.RemoteHostName));

                    var success = TransferUtility.RetrieveJobStatusFile(out var jobStatusFilePathLocal);

                    if (!success)
                    {
                        LogWarning("Error retrieving the .jobstatus file for " + TransferUtility.JobStepDescription + " on " + TransferUtility.RemoteHostName);
                        return EnumRemoteJobStatus.Running;
                    }

                    var jobStatus = ParseJobStatusFile(jobStatusFilePathLocal);

                    return jobStatus;
                }

                if (StatusFileExists(statusFiles, TransferUtility.StatusLockFile))
                {
                    // A lock file exists, but no progress file exists yet
                    return EnumRemoteJobStatus.Running;
                }

                return EnumRemoteJobStatus.Unstarted;
            }
            catch (Exception ex)
            {
                LogError("Error reading the status file for the remotely running job", ex);
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

        /// <summary>
        /// Read the .jobstatus file retrieved from the remote host
        /// </summary>
        /// <param name="jobStatusFilePath"></param>
        /// <param name="logToBrokerDB">When true, log the status to the broker DB (via the messaging queue)</param>
        /// <remarks></remarks>
        private EnumRemoteJobStatus ParseJobStatusFile(string jobStatusFilePath, bool logToBrokerDB = true)
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

                    var status = new clsStatusFile(Path.Combine(WorkDir, "RemoteStatus.xml"), DebugLevel)
                    {
                        MgrName = clsXMLUtils.GetXmlValue(managerInfo, "MgrName")
                    };

                    var mgrStatus = clsXMLUtils.GetXmlValue(managerInfo, "MgrStatus");
                    if (Enum.TryParse(mgrStatus, out EnumMgrStatus eMgrStatus))
                    {
                        status.MgrStatus = eMgrStatus;
                    }

                    status.TaskStartTime = clsXMLUtils.GetXmlValue(managerInfo, "LastStartTime", DateTime.MinValue);

                    var lastUpdate = clsXMLUtils.GetXmlValue(managerInfo, "LastUpdate", DateTime.MinValue);
                    if (lastUpdate == DateTime.MinValue)
                        lastUpdate = status.TaskStartTime;

                    var cpuUtilization = (int)clsXMLUtils.GetXmlValue(managerInfo, "CPUUtilization", 0f);
                    var freeMemoryMB = clsXMLUtils.GetXmlValue(managerInfo, "FreeMemoryMB", 0f);
                    var processId = clsXMLUtils.GetXmlValue(managerInfo, "ProcessID", 0);

                    status.ProgRunnerProcessID = clsXMLUtils.GetXmlValue(managerInfo, "ProgRunnerProcessID", 0);

                    status.ProgRunnerCoreUsage = clsXMLUtils.GetXmlValue(managerInfo, "ProgRunnerCoreUsage", 0f);


                    var taskInfo = doc.Elements("Root").Elements("Task").ToList();

                    status.Tool = clsXMLUtils.GetXmlValue(taskInfo, "Tool");

                    var taskStatus = clsXMLUtils.GetXmlValue(taskInfo, "Status");
                    if (Enum.TryParse(taskStatus, out EnumTaskStatus eTaskStatus))
                    {
                        status.TaskStatus = eTaskStatus;
                    }

                    switch (eTaskStatus)
                    {
                        case EnumTaskStatus.STOPPED:
                        case EnumTaskStatus.REQUESTING:
                        case EnumTaskStatus.NO_TASK:
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
                            jobStatus = EnumRemoteJobStatus.Undefined;
                            break;
                    }

                    status.Progress = clsXMLUtils.GetXmlValue(taskInfo, "Progress", 0f);
                    status.CurrentOperation = clsXMLUtils.GetXmlValue(taskInfo, "CurrentOperation");

                    var taskDetails = taskInfo.Elements("TaskDetails").ToList();

                    var taskStatusDetail = clsXMLUtils.GetXmlValue(taskDetails, "Status");
                    if (Enum.TryParse(taskStatusDetail, out EnumTaskStatusDetail eTaskStatusDetail))
                    {
                        status.TaskStatusDetail = eTaskStatusDetail;
                    }

                    status.JobNumber = clsXMLUtils.GetXmlValue(taskDetails, "Job", 0);
                    status.JobStep = clsXMLUtils.GetXmlValue(taskDetails, "Step", 0);
                    status.Dataset = clsXMLUtils.GetXmlValue(taskDetails, "Dataset");
                    status.MostRecentLogMessage = clsXMLUtils.GetXmlValue(taskDetails, "MostRecentLogMessage");
                    status.MostRecentJobInfo = clsXMLUtils.GetXmlValue(taskDetails, "MostRecentJobInfo");
                    status.SpectrumCount = clsXMLUtils.GetXmlValue(taskDetails, "SpectrumCount", 0);

                    status.WriteStatusFile(lastUpdate, processId, cpuUtilization, freeMemoryMB, forceLogToBrokerDB: logToBrokerDB);
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
        /// Look for file statusFileName in statusFiles
        /// </summary>
        /// <param name="statusFiles"></param>
        /// <param name="statusFileName"></param>
        /// <returns>True if found, otherwise false</returns>
        private bool StatusFileExists(Dictionary<string, SftpFile> statusFiles, string statusFileName)
        {
            return statusFiles.Any(item => string.Equals(item.Value.Name, statusFileName, StringComparison.OrdinalIgnoreCase));
        }

        #endregion

    }

}
