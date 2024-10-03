using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using DMSUpdateManager;
using PRISM;
using Renci.SshNet.Sftp;

namespace AnalysisManagerBase.OfflineJobs
{
    /// <summary>
    /// Methods for transferring files to/from a remote host
    /// Uses sftp for file listings
    /// Uses scp for file transfers
    /// </summary>
    public class RemoteTransferUtility : RemoteUpdateUtility
    {
        // Ignore Spelling: Dirs, passphrase, scp, sftp, SFtpFile, svc-dms, yyyyMMdd_HHmm, wildcards

        /// <summary>
        /// Step parameter RemoteInfo
        /// </summary>
        public const string STEP_PARAM_REMOTE_INFO = "RemoteInfo";

        /// <summary>
        /// Step parameter RemoteTimestamp
        /// </summary>
        public const string STEP_PARAM_REMOTE_TIMESTAMP = "RemoteTimestamp";

        /// <summary>
        /// Step parameter RemoteProgress
        /// </summary>
        /// <remarks>Read from the .info file and sent to stored procedure set_step_task_complete in SetAnalysisJobComplete</remarks>
        public const string STEP_PARAM_REMOTE_PROGRESS = "RemoteProgress";

        /// <summary>
        /// Step parameter RemoteStart
        /// </summary>
        /// <remarks>Read from the .info or .success file and sent to stored procedure set_step_task_complete</remarks>
        public const string STEP_PARAM_REMOTE_START = "RemoteStart";

        /// <summary>
        /// Step parameter RemoteFinish
        /// </summary>
        /// <remarks>Read from the .success file and sent to stored procedure set_step_task_complete</remarks>
        public const string STEP_PARAM_REMOTE_FINISH = "RemoteFinish";

        /// <summary>
        /// Since this constant is true, we will use the RemoteInfo defined for the manager
        /// </summary>
        private const bool USE_MANAGER_REMOTE_INFO = true;

        private bool mUsingManagerRemoteInfo;

        /// <summary>
        /// Dataset Name
        /// </summary>
        public string DatasetName { get; private set; }

        /// <summary>
        /// Debug level
        /// </summary>
        public int DebugLevel { get; private set; }

        /// <summary>
        /// Job number
        /// </summary>
        public int JobNum { get; private set; }

        /// <summary>
        /// Job parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisJob</remarks>
        private IJobParams JobParams { get; }

        /// <summary>
        /// Manager parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisMgrSettings</remarks>
        private IMgrParams MgrParams { get; }

        /// <summary>
        /// Directory with FASTA files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSOrgDBs
        /// </remarks>
        public string RemoteOrgDBPath { get; private set; }

        /// <summary>
        /// Directory with task queue files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSTasks
        /// </remarks>
        public string RemoteTaskQueuePath { get; private set; }

        /// <summary>
        /// Directory with task queue files for the step tool associated with this job
        /// </summary>
        public string RemoteTaskQueuePathForTool
        {
            get
            {
                if (string.IsNullOrWhiteSpace(RemoteTaskQueuePath) || string.IsNullOrWhiteSpace(StepTool))
                    return string.Empty;

                return PathUtils.CombineLinuxPaths(RemoteTaskQueuePath, StepTool);
            }
        }

        /// <summary>
        /// Root directory with job task data files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSWorkDir
        /// </remarks>
        public string RemoteWorkDirPath { get; private set; }

        /// <summary>
        /// Remote working directory for this specific job
        /// </summary>
        public string RemoteJobStepWorkDirPath => PathUtils.CombineLinuxPaths(RemoteWorkDirPath, GetJobStepFileOrFolderName());

        /// <summary>
        /// Step number for the current job
        /// </summary>
        public int StepNum { get; private set; }

        /// <summary>
        /// Step tool, e.g. MSGFPlus
        /// </summary>
        public string StepTool { get; private set; }

        /// <summary>
        /// Local working directory
        /// </summary>
        public string WorkDir { get; private set; }

        /// <summary>
        /// Job number and text, in the form "job x, step y"
        /// </summary>
        public string JobStepDescription => string.Format("job {0}, step {1}", JobNum, StepNum);

        /// <summary>
        /// Filename of the .jobstatus status file
        /// </summary>
        public string JobStatusFile => GetBaseStatusFilename() + ".jobstatus";

        /// <summary>
        /// Filename of the .fail file
        /// </summary>
        public string ProcessingFailureFile => GetBaseStatusFilename() + ".fail";

        /// <summary>
        /// Filename of the .success file
        /// </summary>
        public string ProcessingSuccessFile => GetBaseStatusFilename() + ".success";

        /// <summary>
        /// Filename of the .info file
        /// </summary>
        public string StatusInfoFile => GetBaseStatusFilename() + ".info";

        /// <summary>
        /// Filename of the .lock file
        /// </summary>
        public string StatusLockFile => GetBaseStatusFilename() + ".lock";

        /// <summary>
        /// Return a list of all status file names
        /// </summary>
        /// <remarks>Useful for skipping status files when copying job results</remarks>
        public List<string> StatusFileNames =>
            new()
            {
                JobStatusFile,
                ProcessingFailureFile,
                ProcessingSuccessFile,
                StatusInfoFile,
                StatusLockFile
            };

        /// <summary>
        /// Constructor
        /// </summary>
        public RemoteTransferUtility(IMgrParams mgrParams, IJobParams jobParams) : base(new RemoteHostConnectionInfo())
        {
            MgrParams = mgrParams;
            JobParams = jobParams;

            mUsingManagerRemoteInfo = true;

            // Initialize the properties to have empty strings
            DatasetName = string.Empty;
            RemoteOrgDBPath = string.Empty;
            RemoteTaskQueuePath = string.Empty;
            RemoteWorkDirPath = string.Empty;
            WorkDir = string.Empty;
        }

        /// <summary>
        /// Convert a list of settings to XML
        /// </summary>
        /// <param name="settings">List of settings, as Key/Value pairs</param>
        private static string ConstructRemoteInfoXml(IEnumerable<KeyValuePair<string, string>> settings)
        {
            var xmlText = new StringBuilder();

            foreach (var setting in settings)
            {
                xmlText.Append(string.Format("<{0}>{1}</{0}>", setting.Key, setting.Value));
            }

            // Convert to text, will look like
            // <host>PrismWeb2</host><user>svc-dms</user><dmsPrograms>/opt/DMS_Programs</dmsPrograms><taskQueue>/file1/temp/DMSTasks</taskQueue> ...
            return xmlText.ToString();
        }

        /// <summary>
        /// Copy files from the remote host to a local directory
        /// </summary>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFiles">Dictionary where keys are source file names (no wildcards), and values are true if the file is required, false if optional</param>
        /// <param name="localDirectoryPath">Local target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; false to use RemoteInfo associated with the job (useful if checking on a running job)</param>
        /// <param name="warnIfMissing">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <returns>
        /// True on success, false if an error
        /// Returns false if any files were missing, even if warnIfMissing is false
        /// </returns>
        public bool CopyFilesFromRemote(
            string sourceDirectoryPath,
            IReadOnlyDictionary<string, bool> sourceFiles,
            string localDirectoryPath,
            bool useDefaultManagerRemoteInfo,
            bool warnIfMissing)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            return CopyFilesFromRemote(sourceDirectoryPath, sourceFiles, localDirectoryPath, warnIfMissing);
        }

        /// <summary>
        /// Copy a single file from a local directory to the remote host
        /// </summary>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="useLockFile">True to use a lock file when the destination directory might be accessed via multiple managers simultaneously</param>
        /// <returns>True on success, false if an error</returns>
        public bool CopyFileToRemote(string sourceFilePath, string remoteDirectoryPath, bool useLockFile = false)
        {
            try
            {
                var sourceFile = new FileInfo(sourceFilePath);

                if (!sourceFile.Exists)
                {
                    OnErrorEvent("Cannot copy file to remote; source file not found: " + sourceFilePath);
                    return false;
                }

                var sourceFileNames = new List<string> { sourceFile.Name };

                var success = CopyFilesToRemote(sourceFile.DirectoryName, sourceFileNames, remoteDirectoryPath, USE_MANAGER_REMOTE_INFO, useLockFile);
                return success;
            }
            catch (Exception ex)
            {
                var errMsg = string.Format("Error copying file {0} to {1}: {2}", Path.GetFileName(sourceFilePath), remoteDirectoryPath, ex.Message);
                OnErrorEvent(errMsg, ex);
                return false;
            }
        }

        /// <summary>
        /// Copy files from a local directory to the remote host
        /// </summary>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are allowed</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; false to use RemoteInfo associated with the job (typically should be true)</param>
        /// <param name="useLockFile">True to use a lock file when the destination directory might be accessed via multiple managers simultaneously</param>
        /// <returns>True on success, false if an error</returns>
        public bool CopyFilesToRemote(
            string sourceDirectoryPath,
            IEnumerable<string> sourceFileNames,
            string remoteDirectoryPath,
            bool useDefaultManagerRemoteInfo,
            bool useLockFile)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            return CopyFilesToRemote(sourceDirectoryPath, sourceFileNames, remoteDirectoryPath, useLockFile, MgrParams.ManagerName);
        }

        /// <summary>
        /// Create a new task info file for the current job on the remote host
        /// </summary>
        /// <remarks>Created in RemoteTaskQueuePath</remarks>
        /// <param name="remoteTimestamp">Output: Remote info file path</param>
        /// <param name="infoFilePathRemote">Output: Remote info file path</param>
        /// <returns>True on success, false on an error</returns>
        public bool CreateJobTaskInfoFile(string remoteTimestamp, out string infoFilePathRemote)
        {
            if (IsParameterUpdateRequired(USE_MANAGER_REMOTE_INFO))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(USE_MANAGER_REMOTE_INFO);
            }

            try
            {
                DefineRemoteInfo();

                var infoFileName = GetBaseStatusFilename(JobNum, StepNum, remoteTimestamp) + ".info";

                OnDebugEvent("Creating JobTaskInfo file " + infoFileName);

                var infoFilePathLocal = Path.Combine(WorkDir, infoFileName);

                var remoteDirectoryPath = RemoteTaskQueuePathForTool;

                if (string.IsNullOrWhiteSpace(remoteDirectoryPath))
                {
                    OnErrorEvent("Remote task queue path for this job's step tool is empty; cannot create the task info file");
                    infoFilePathRemote = string.Empty;
                    return false;
                }

                infoFilePathRemote = PathUtils.CombineLinuxPaths(remoteDirectoryPath, infoFileName);

                using (var writer = new StreamWriter(new FileStream(infoFilePathLocal, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("Job=" + JobNum);
                    writer.WriteLine("Step=" + StepNum);
                    writer.WriteLine("StepTool=" + StepTool);
                    writer.WriteLine("WorkDir=" + RemoteJobStepWorkDirPath);
                    writer.WriteLine("Staged=" + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }

                // Assure that the target directory exists
                var targetFolderVerified = CreateRemoteDirectory(remoteDirectoryPath);

                if (!targetFolderVerified)
                {
                    OnErrorEvent("Unable to verify/create directory {0} on host {1}", remoteDirectoryPath, RemoteHostName);
                    infoFilePathRemote = string.Empty;
                    return false;
                }

                CopyFileToRemote(infoFilePathLocal, remoteDirectoryPath);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error creating the remote job task info file", ex);
                infoFilePathRemote = string.Empty;
                return false;
            }
        }

        private void DefineRemoteInfo()
        {
            var remoteInfo = GetRemoteInfoXml(USE_MANAGER_REMOTE_INFO);

            JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_INFO, remoteInfo);
        }

        /// <summary>
        /// Delete the working directory for the current job and step (parameter RemoteJobStepWorkDirPath)
        /// </summary>
        /// <param name="keepEmptyDirectory">When true, delete all files/directories in the remote WorkDir but don't remove WorkDir</param>
        public void DeleteRemoteWorkDir(bool keepEmptyDirectory = false)
        {
            try
            {
                if (string.IsNullOrEmpty(RemoteWorkDirPath))
                    throw new Exception("RemoteWorkDirPath is empty; cannot delete files");

                DeleteDirectoryAndContents(RemoteJobStepWorkDirPath, keepEmptyDirectory);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error deleting remote work directory", ex);
            }
        }

        /// <summary>
        /// Construct the base status file name, of the form JobX_StepY_TimeStamp
        /// </summary>
        /// <remarks>Uses the RemoteTimestamp job parameter</remarks>
        /// <returns>Status filename</returns>
        private string GetBaseStatusFilename()
        {
            var remoteTimestamp = JobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);

            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot properly construct the base tracking file name");
                return string.Empty;
            }

            return GetBaseStatusFilename(JobNum, StepNum, remoteTimestamp);
        }

        /// <summary>
        /// Construct the base status file name, of the form JobX_StepY_TimeStamp
        /// </summary>
        /// <param name="job">Job number</param>
        /// <param name="step">Step number</param>
        /// <param name="remoteTimestamp">Remote timestamp</param>
        /// <returns>Status filename</returns>
        private static string GetBaseStatusFilename(int job, int step, string remoteTimestamp)
        {
            return GetJobStepFileOrFolderName(job, step) + "_" + remoteTimestamp;
        }

        /// <summary>
        /// Return text in the form "Job1234_Step3"
        /// </summary>
        /// <remarks>Intended for use in file and directory names</remarks>
        private string GetJobStepFileOrFolderName()
        {
            return GetJobStepFileOrFolderName(JobNum, StepNum);
        }

        /// <summary>
        /// Return text in the form "Job1234_Step3"
        /// </summary>
        /// <remarks>Intended for use in file and directory names</remarks>
        private static string GetJobStepFileOrFolderName(int job, int step)
        {
            return string.Format("Job{0}_Step{1}", job, step);
        }

        /// <summary>
        /// Construct the path to the .jobstatus file for the running offline job
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="jobParams">Job parameters</param>
        public static string GetOfflineJobStatusFilePath(IMgrParams mgrParams, IJobParams jobParams)
        {
            var job = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var step = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            var stepTool = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "");

            if (job == 0)
            {
                ConsoleMsgUtils.ShowWarning("Job parameter job is missing; " +
                                            "cannot properly construct the .jobstatus file path");
                return string.Empty;
            }

            if (step == 0)
            {
                ConsoleMsgUtils.ShowWarning("Job parameter step is missing; " +
                                            "cannot properly construct the .jobstatus file path for job " + job);
                return string.Empty;
            }

            var jobStepDescription = string.Format("job {0}, step {1}", job, step);

            if (string.IsNullOrWhiteSpace(stepTool))
            {
                ConsoleMsgUtils.ShowWarning("Job parameter StepTool is empty; " +
                                            "cannot properly construct the .jobstatus file path for " + jobStepDescription);
                return string.Empty;
            }

            var remoteTimestamp = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP, "");

            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                ConsoleMsgUtils.ShowWarning("Job parameter RemoteTimestamp is empty; " +
                                            "cannot properly construct the .jobstatus file path for " + jobStepDescription);
            }

            var jobStatusFilename = GetBaseStatusFilename(job, step, remoteTimestamp) + ".jobstatus";

            var taskQueuePath = mgrParams.GetParam("LocalTaskQueuePath");

            if (string.IsNullOrWhiteSpace(taskQueuePath))
            {
                ConsoleMsgUtils.ShowWarning("Manager parameter LocalTaskQueuePath is empty; " +
                                            "cannot properly construct the .jobstatus file path for " + jobStepDescription);
                return string.Empty;
            }

            var taskQueuePathForTool = Path.Combine(taskQueuePath, stepTool);

            var jobStatusFilePath = Path.Combine(taskQueuePathForTool, jobStatusFilename);

            return jobStatusFilePath;
        }

        /// <summary>
        /// Retrieve a listing of files in the remoteDirectoryPath directory on the remote host
        /// </summary>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="fileMatchSpec">Filename to find, or files to find if wildcards are used</param>
        /// <param name="recurse">True to </param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; false to use RemoteInfo associated with the job (typically should be true)</param>
        /// <returns>List of matching files (full paths)</returns>
        // ReSharper disable once UnusedMember.Global
        public Dictionary<string, SftpFile> GetRemoteFileListing(
            string remoteDirectoryPath,
            string fileMatchSpec,
            bool recurse,
            bool useDefaultManagerRemoteInfo)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            return GetRemoteFileListing(remoteDirectoryPath, fileMatchSpec, recurse);
        }

        /// <summary>
        /// Construct the XML string that should be stored as job parameter RemoteInfo
        /// </summary>
        /// <remarks>RemoteInfo is sent to the database via stored procedure set_step_task_complete</remarks>
        /// <param name="useDefaultManagerRemoteInfo">If true, use default remote manager info</param>
        /// <returns>String with XML</returns>
        private string GetRemoteInfoXml(bool useDefaultManagerRemoteInfo)
        {
            if (useDefaultManagerRemoteInfo)
            {
                return GetRemoteInfoXml(MgrParams);
            }

            var settings = new List<KeyValuePair<string, string>>
            {
                new("host", RemoteHostInfo.HostName),
                new("user", RemoteHostInfo.Username),
                new("dmsPrograms", RemoteHostInfo.BaseDirectoryPath),
                new("taskQueue", RemoteTaskQueuePath),
                new("workDir", RemoteWorkDirPath),
                new("orgDB", RemoteOrgDBPath),
                new("privateKey", Path.GetFileName(RemoteHostInfo.PrivateKeyFile)),
                new("passphrase", Path.GetFileName(RemoteHostInfo.PassphraseFile))
            };

            return ConstructRemoteInfoXml(settings);
        }

        /// <summary>
        /// Construct the default XML string that will be used for jobs staged by this manager
        /// </summary>
        /// <remarks>The RemoteInfo generated here is passed to request_step_task_xml when checking for an available job</remarks>
        /// <param name="mgrParams">Manager parameters</param>
        /// <returns>String with XML</returns>
        public static string GetRemoteInfoXml(IMgrParams mgrParams)
        {
            var settings = new List<KeyValuePair<string, string>>
            {
                new("host", mgrParams.GetParam("RemoteHostName")),
                new("user", mgrParams.GetParam("RemoteHostUser")),
                new("dmsPrograms", mgrParams.GetParam("RemoteHostDMSProgramsPath")),
                new("taskQueue", mgrParams.GetParam("RemoteTaskQueuePath")),
                new("workDir", mgrParams.GetParam("RemoteWorkDirPath")),
                new("orgDB", mgrParams.GetParam("RemoteOrgDBPath")),
                new("privateKey",  Path.GetFileName(mgrParams.GetParam("RemoteHostPrivateKeyFile"))),
                new("passphrase", Path.GetFileName(mgrParams.GetParam("RemoteHostPassphraseFile")))
            };

            return ConstructRemoteInfoXml(settings);
        }

        /// <summary>
        /// Find all status files for the current job and job step
        /// </summary>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; false to use RemoteInfo associated with the job (useful if checking on a running job)</param>
        /// <returns>Dictionary of matching files, where keys are full file paths and values are instances of SFtpFile</returns>
        public Dictionary<string, SftpFile> GetStatusFiles(bool useDefaultManagerRemoteInfo = false)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            try
            {
                var remoteTimestamp = JobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);

                if (string.IsNullOrWhiteSpace(remoteTimestamp))
                {
                    OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot list remote status files");
                    return new Dictionary<string, SftpFile>();
                }

                var baseTrackingFilename = GetBaseStatusFilename();

                if (string.IsNullOrWhiteSpace(baseTrackingFilename))
                {
                    return new Dictionary<string, SftpFile>();
                }

                var statusFiles = GetRemoteFileListing(RemoteTaskQueuePathForTool, baseTrackingFilename + "*");

                return statusFiles;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error retrieving remote status files", ex);
                return new Dictionary<string, SftpFile>();
            }
        }

        private bool IsParameterUpdateRequired(bool useDefaultManagerRemoteInfo)
        {
            return !ParametersValidated || mUsingManagerRemoteInfo != useDefaultManagerRemoteInfo;
        }

        /// <summary>
        /// Retrieve the JobX_StepY_RemoteTimestamp.jobstatus file from the remote TaskQueue directory
        /// </summary>
        /// <remarks>Check for the existence of the file by calling GetStatusFiles and looking for a .jobstatus file </remarks>
        /// <param name="jobStatusFilePathLocal">Local job status file path</param>
        /// <returns>True if success, otherwise false</returns>
        public bool RetrieveJobStatusFile(out string jobStatusFilePathLocal)
        {
            if (IsParameterUpdateRequired(false))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(false);
            }

            var remoteTimestamp = JobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);

            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot retrieve the remote .jobstatus file");
                jobStatusFilePathLocal = string.Empty;
                return false;
            }

            return RetrieveStatusFile(JobStatusFile, out jobStatusFilePathLocal);
        }

        /// <summary>
        /// Retrieve the given status file from the remote TaskQueue directory
        /// </summary>
        /// <remarks>Check for the existence of the file by calling GetStatusFiles and looking for desired file </remarks>
        /// <param name="statusFileName">Status file name</param>
        /// <param name="statusFilePathLocal">Output: full path to the status file on the local drive</param>
        /// <returns>True if success, otherwise false</returns>
        public bool RetrieveStatusFile(string statusFileName, out string statusFilePathLocal)
        {
            statusFilePathLocal = string.Empty;

            try
            {
                var sourceFiles = new Dictionary<string, bool> { { statusFileName, true } };

                var success = CopyFilesFromRemote(RemoteTaskQueuePathForTool, sourceFiles, WorkDir);

                if (!success)
                {
                    return false;
                }

                var statusFile = new FileInfo(Path.Combine(WorkDir, statusFileName));

                if (statusFile.Exists)
                {
                    statusFilePathLocal = statusFile.FullName;
                    return true;
                }

                OnWarningEvent(Path.GetExtension(statusFileName) + " file not found despite CopyFilesFromRemote reporting success: " + statusFile.FullName);
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error retrieving status file {0}: {1}", statusFileName, ex.Message), ex);
                return false;
            }
        }

        /// <summary>
        /// Copy new/updated DMS_Programs files to the remote host
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool RunDMSUpdateManager()
        {
            try
            {
                if (IsParameterUpdateRequired(USE_MANAGER_REMOTE_INFO))
                {
                    // Validate that the required parameters are present and load the private key and passphrase from disk
                    // This throws an exception if any parameters are missing
                    UpdateParameters(USE_MANAGER_REMOTE_INFO);
                }

                var dmsUpdateMgrSource = MgrParams.GetParam("DMSUpdateManagerSource");
                var dmsProgramsPath = MgrParams.GetParam("RemoteHostDMSProgramsPath");
                var analysisManagerDirs = MgrParams.GetParam("RemoteHostAnalysisManagerDirs", "AnalysisManager");

                if (string.IsNullOrEmpty(dmsProgramsPath))
                {
                    OnErrorEvent("Manager parameter not found: RemoteHostDMSProgramsPath; cannot run the DMS Update Manager");
                    return false;
                }

                if (string.IsNullOrEmpty(analysisManagerDirs))
                {
                    OnErrorEvent("Manager parameter not found: RemoteHostAnalysisManagerDirs; cannot run the DMS Update Manager");
                    return false;
                }

                var sourceDirectoryPath = string.Empty;

                if (string.IsNullOrEmpty(dmsUpdateMgrSource))
                {
                    var sourceCandidates = new List<string>
                    {
                        @"C:\DMS_Programs"
                    };

                    var appFolder = new DirectoryInfo(Global.GetAppDirectoryPath());
                    var appFolderParent = appFolder.Parent;

                    if (appFolderParent != null)
                    {
                        sourceCandidates.Add(appFolderParent.FullName);
                    }

                    foreach (var candidatePath in sourceCandidates)
                    {
                        if (!Directory.Exists(sourceDirectoryPath))
                            continue;

                        sourceDirectoryPath = candidatePath;
                        break;
                    }

                    if (string.IsNullOrWhiteSpace(sourceDirectoryPath))
                    {
                        OnErrorEvent("Manager parameter not found: DMSUpdateManagerSource; furthermore, unable to determine the local directory path");
                        return false;
                    }
                    OnWarningEvent("Manager parameter not found: DMSUpdateManagerSource; will use " + sourceDirectoryPath);
                }
                else
                {
                    if (!Directory.Exists(dmsUpdateMgrSource))
                    {
                        OnErrorEvent("DMSUpdateManagerSource path not found; cannot run the DMS Update Manager: " + dmsUpdateMgrSource);
                        return false;
                    }

                    sourceDirectoryPath = dmsUpdateMgrSource;
                }

                var filesToIgnore = MgrParams.GetParam("DMSUpdateManagerFilesToIgnore");

                var ignoreList = new List<string>();

                if (!string.IsNullOrWhiteSpace(filesToIgnore))
                {
                    ignoreList.AddRange(filesToIgnore.Split(',').ToList());
                }

                // Also ignore System.Data.SQLite.dll since the Linux version differs from the Windows version
                ignoreList.Add("System.Data.SQLite.dll");

                var dirsProcessed = 0;
                var errorMessages = new List<string>();

                foreach (var analysisManagerDir in analysisManagerDirs.Split(','))
                {
                    if (string.IsNullOrWhiteSpace(analysisManagerDir))
                        continue;

                    var targetDirectoryPath = PathUtils.CombineLinuxPaths(dmsProgramsPath, analysisManagerDir.Trim());

                    OnDebugEvent("Copying new/updated DMS Programs files from {0} to {1} on remote host {2}", sourceDirectoryPath, targetDirectoryPath, RemoteHostInfo.HostName);

                    RemoteHostInfo.BaseDirectoryPath = targetDirectoryPath;

                    // For the first remote analysis manager directory, we will also update the
                    // parent directories of the target directory, e.g. ../MSGFDB and ../MSPathFinder

                    // On subsequent analysis manager directories, we only need to update the analysis manager directory itself
                    CopySubdirectoriesToParentDirectory = dirsProcessed == 0;

                    var success = StartDMSUpdateManager(sourceDirectoryPath, targetDirectoryPath, ignoreList, out var errorMessage);

                    if (success)
                    {
                        dirsProcessed++;
                        continue;
                    }

                    if (!string.IsNullOrWhiteSpace(errorMessage))
                        errorMessages.Add(errorMessage);
                }

                if (dirsProcessed > 0)
                    return true;

                const string msg = "Error pushing DMS Programs files to the remote host; UpdateRemoteHost returns false";

                if (errorMessages.Count == 0)
                {
                    OnErrorEvent(msg);
                }
                else
                {
                    OnErrorEvent(msg + ": " + errorMessages.First());
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error copying new/updated DMS_Programs files to the remote host", ex);
                return false;
            }
        }

        /// <summary>
        /// Update cached parameters using MgrParams and JobParams
        /// In addition, loads the private key information from RemoteHostPrivateKeyFile and RemoteHostPassphraseFile
        /// </summary>
        /// <remarks>
        /// Throws an exception if any parameters are missing or empty
        /// Also throws an exception if there is an error reading the private key information
        /// </remarks>
        /// <param name="useDefaultManagerRemoteInfo">
        /// When true, use default manager remote info params
        /// When false, override remote settings using the RemoteInfo parameter in the StepParameters section for the job
        /// </param>
        public void UpdateParameters(bool useDefaultManagerRemoteInfo)
        {
            WorkDir = MgrParams.GetParam("WorkDir");
            DebugLevel = MgrParams.GetParam("DebugLevel", 2);

            if (useDefaultManagerRemoteInfo)
            {
                // Use settings defined for this manager
                OnDebugEvent("Updating remote transfer settings using manager defaults");

                RemoteHostInfo.HostName = MgrParams.GetParam("RemoteHostName");
                RemoteHostInfo.Username = MgrParams.GetParam("RemoteHostUser");

                RemoteHostInfo.BaseDirectoryPath = MgrParams.GetParam("RemoteHostDMSProgramsPath");

                RemoteHostInfo.PrivateKeyFile = MgrParams.GetParam("RemoteHostPrivateKeyFile");
                RemoteHostInfo.PassphraseFile = MgrParams.GetParam("RemoteHostPassphraseFile");

                RemoteTaskQueuePath = MgrParams.GetParam("RemoteTaskQueuePath");
                RemoteWorkDirPath = MgrParams.GetParam("RemoteWorkDirPath");
                RemoteOrgDBPath = MgrParams.GetParam("RemoteOrgDBPath");

                mUsingManagerRemoteInfo = true;
            }
            else
            {
                // Use settings defined for the running analysis job

                OnDebugEvent("Updating remote transfer settings using job parameter RemoteInfo");

                var remoteInfo = JobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_INFO);

                if (string.IsNullOrWhiteSpace(remoteInfo))
                {
                    throw new Exception("RemoteInfo job step parameter is empty; the RemoteTransferUtility cannot validate remote info");
                }

                try
                {
                    var doc = XDocument.Parse("<root>" + remoteInfo + "</root>");
                    var elements = doc.Elements("root").ToList();

                    RemoteHostInfo.HostName = XMLUtils.GetXmlValue(elements, "host");
                    RemoteHostInfo.Username = XMLUtils.GetXmlValue(elements, "user");

                    RemoteHostInfo.BaseDirectoryPath = XMLUtils.GetXmlValue(elements, "dmsPrograms");

                    var mgrPrivateKeyFilePath = MgrParams.GetParam("RemoteHostPrivateKeyFile");
                    var mgrPassphraseFilePath = MgrParams.GetParam("RemoteHostPassphraseFile");

                    var jobPrivateKeyFileName = XMLUtils.GetXmlValue(elements, "privateKey");
                    var jobPassphraseFileName = XMLUtils.GetXmlValue(elements, "passphrase");

                    RemoteHostInfo.PrivateKeyFile = PathUtils.ReplaceFilenameInPath(mgrPrivateKeyFilePath, jobPrivateKeyFileName);
                    RemoteHostInfo.PassphraseFile = PathUtils.ReplaceFilenameInPath(mgrPassphraseFilePath, jobPassphraseFileName);

                    RemoteTaskQueuePath = XMLUtils.GetXmlValue(elements, "taskQueue");
                    RemoteWorkDirPath = XMLUtils.GetXmlValue(elements, "workDir");
                    RemoteOrgDBPath = XMLUtils.GetXmlValue(elements, "orgDB");
                }
                catch (Exception ex)
                {
                    throw new Exception("Error parsing XML in the RemoteInfo job step parameter: " + ex.Message, ex);
                }

                mUsingManagerRemoteInfo = false;
            }

            JobNum = JobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            StepNum = JobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            StepTool = JobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool");
            DatasetName = AnalysisResources.GetDatasetName(JobParams);

            if (string.IsNullOrWhiteSpace(WorkDir))
                throw new Exception("WorkDir parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteTaskQueuePath))
                throw new Exception("RemoteTaskQueuePath parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteWorkDirPath))
                throw new Exception("RemoteWorkDirPath parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteOrgDBPath))
                throw new Exception("RemoteOrgDBPath parameter is empty; check the manager parameters");

            if (JobNum == 0)
                throw new Exception("JobNum is zero; check the job parameters");

            if (string.IsNullOrWhiteSpace(StepTool))
                throw new Exception("StepTool name is empty; check the job parameters");

            if (string.IsNullOrWhiteSpace(DatasetName))
                throw new Exception("Dataset name is empty; check the job parameters");

            // Validate additional parameters, then load the RSA private key info
            // If successful, mParametersValidated will be set to true
            UpdateParameters();
        }

        /// <summary>
        /// Create a new timestamp using the current time
        /// Store in the job parameters, as parameter RemoteTimestamp
        /// </summary>
        /// <returns>The timestamp</returns>
        public string UpdateRemoteTimestamp()
        {
            // Note: use DateTime.Now and convert to a 24-hour clock
            // Do not use UTCNow since DMS converts the RemoteTimestamp to a DateTime then compares the result to GetDate() to compute job runtime
            var remoteTimestamp = DateTime.Now.ToString("yyyyMMdd_HHmm");

            JobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP, remoteTimestamp);

            return remoteTimestamp;
        }

        /// <summary>
        /// Wait for another manager to finish copying a file to a remote host
        /// </summary>
        /// <param name="sourceFile">Source file</param>
        /// <param name="remoteFile">Remote file</param>
        /// <param name="abortCopy">Output: if true, abort the copy since the remote file is larger than the expected value</param>
        public bool WaitForRemoteFileCopy(FileInfo sourceFile, SftpFile remoteFile, out bool abortCopy)
        {
            var remoteFilePath = remoteFile.FullName;

            var remoteFileInfo = new FileOrDirectoryInfo(remoteFile);
            var remoteDirectoryPath = remoteFileInfo.DirectoryName;

            abortCopy = false;

            while (DateTime.UtcNow.Subtract(remoteFile.LastWriteTimeUtc).TotalMinutes < 15)
            {
                OnDebugEvent("Waiting for another manager to finish copying the file to the remote host; " +
                             "currently {0} bytes for {1} ", remoteFile.Length, remoteFilePath);

                // Wait for 30 seconds
                Global.IdleLoop(10);

                // Update the info on the remote file
                var matchingFiles = GetRemoteFileListing(remoteDirectoryPath, sourceFile.Name);

                if (matchingFiles.Count > 0)
                {
                    var newFileInfo = matchingFiles.First().Value;

                    if (newFileInfo.Length == sourceFile.Length)
                    {
                        // The files now match
                        return true;
                    }

                    if (newFileInfo.Length > sourceFile.Length)
                    {
                        // The remote file is larger than the expected value
                        abortCopy = true;
                        return false;
                    }
                }
                else
                {
                    OnDebugEvent("File no longer exists on the remote host: {0}", remoteFilePath);
                    return false;
                }
            }

            return false;
        }
    }
}
