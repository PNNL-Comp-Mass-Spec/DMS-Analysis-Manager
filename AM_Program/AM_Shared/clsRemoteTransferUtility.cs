using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using DMSUpdateManager;
using PRISM;
using Renci.SshNet.Sftp;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Methods for transferring files to/from a remote host
    /// Uses sftp for file listings
    /// Uses scp for file transfers
    /// </summary>
    public class clsRemoteTransferUtility : RemoteUpdateUtility
    {
        #region "Constants"

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
        public const string STEP_PARAM_REMOTE_PROGRESS = "RemoteProgress";

        private const bool USE_MANAGER_REMOTE_INFO = true;

        #endregion

        #region "Module variables"

        private bool mUsingManagerRemoteInfo;

        #endregion

        #region "Properties"

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
        private IJobParams JobParams { get; }

        /// <summary>
        /// Manager parameters
        /// </summary>
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

                return clsPathUtils.CombineLinuxPaths(RemoteTaskQueuePath, StepTool);
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
        public string RemoteJobStepWorkDirPath => clsPathUtils.CombineLinuxPaths(RemoteWorkDirPath, GetJobStepFileOrFolderName());

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

        #endregion

        #region "Status File Names"

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
        public List<string> StatusFileNames
        {
            get
            {

                var statusFileNames = new List<string>
                {
                    JobStatusFile,
                    ProcessingFailureFile,
                    ProcessingSuccessFile,
                    StatusInfoFile,
                    StatusLockFile
                };

                return statusFileNames;
            }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        public clsRemoteTransferUtility(IMgrParams mgrParams, IJobParams jobParams) : base(new RemoteHostConnectionInfo())
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
        /// <param name="settings"></param>
        /// <returns></returns>
        private static string ConstructRemoteInfoXml(IEnumerable<KeyValuePair<string, string>> settings)
        {
            var xmlText = new StringBuilder();
            foreach (var setting in settings)
            {
                xmlText.Append(string.Format("<{0}>{1}</{0}>", setting.Key, setting.Value));
            }

            // Convert to text, will look like
            // <host>PrismWeb2</host><user>svc-dms</user><taskQueue>/file1/temp/DMSTasks</taskQueue> ...
            return xmlText.ToString();
        }

        /// <summary>
        /// Copy files from the remote host to a local directory
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are not allowed</param>
        /// <param name="localDirectoryPath">Local target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (useful if checking on a running job)</param>
        /// <param name="warnIfMissing">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <returns>
        /// True on success, false if an error
        /// Returns False if if any files were missing, even if warnIfMissing is false
        /// </returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        public bool CopyFilesFromRemote(
            string sourceDirectoryPath,
            IReadOnlyCollection<string> sourceFileNames,
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

            return CopyFilesFromRemote(sourceDirectoryPath, sourceFileNames, localDirectoryPath, warnIfMissing);
        }

        /// <summary>
        /// Copy a single file from a local directory to the remote host
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="useLockFile">True to use a lock file when the destination directory might be accessed via multiple managers simultaneously</param>
        /// <returns>True on success, false if an error</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
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
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are allowed</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (typically should be true)</param>
        /// <param name="useLockFile">True to use a lock file when the destination directory might be accessed via multiple managers simultaneously</param>
        /// <returns>True on success, false if an error</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
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
        /// <param name="infoFilePathRemote">Output: Remote info file path</param>
        /// <remarks>Created in RemoteTaskQueuePath</remarks>
        /// <returns>True on success, false on an error</returns>
        public bool CreateJobTaskInfoFile(out string infoFilePathRemote)
        {
            if (IsParameterUpdateRequired(USE_MANAGER_REMOTE_INFO))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(USE_MANAGER_REMOTE_INFO);
            }

            try
            {

                var remoteTimeStamp = DefineRemoteTimestamp();

                DefineRemoteInfo();

                var infoFileName = GetBaseStatusFilename(remoteTimeStamp) + ".info";

                OnDebugEvent("Creating JobTaskInfo file " + infoFileName);

                var infoFilePathLocal = Path.Combine(WorkDir, infoFileName);

                var remoteDirectoryPath = RemoteTaskQueuePathForTool;
                if (string.IsNullOrWhiteSpace(remoteDirectoryPath))
                {
                    OnErrorEvent("Remote task queue path for this job's step tool is empty; cannot create the task info file");
                    infoFilePathRemote = string.Empty;
                    return false;
                }

                infoFilePathRemote = clsPathUtils.CombineLinuxPaths(remoteDirectoryPath, infoFileName);

                using (var writer = new StreamWriter(new FileStream(infoFilePathLocal, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("Job=" + JobNum);
                    writer.WriteLine("Step=" + StepNum);
                    writer.WriteLine("StepTool=" + StepTool);
                    writer.WriteLine("WorkDir=" + RemoteJobStepWorkDirPath);
                    writer.WriteLine("Staged=" + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }

                // Assure that the target directory exists
                var targetFolderVerified = CreateRemoteDirectory(remoteDirectoryPath);
                if (!targetFolderVerified)
                {
                    OnErrorEvent(string.Format("Unable to verify/create directory {0} on host {1}", remoteDirectoryPath, RemoteHostName));
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

            JobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_INFO, remoteInfo);
        }

        private string DefineRemoteTimestamp()
        {
            // Note: use DateTime.Now and convert to a 24-hour clock
            // Do not use UTCNow since DMS converts the RemoteTimestamp to a DateTime then compares the result to GetDate() to compute job runtime
            var remoteTimestamp = DateTime.Now.ToString("yyyyMMdd_HHmm");

            JobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP, remoteTimestamp);

            return remoteTimestamp;
        }

        /// <summary>
        /// Delete the working directory for the current job and step (parameter RemoteJobStepWorkDirPath)
        /// </summary>
        public void DeleteRemoteWorkDir()
        {

            try
            {
                if (string.IsNullOrEmpty(RemoteWorkDirPath))
                    throw new Exception("RemoteWorkDirPath is empty; cannot delete files");

                DeleteDirectoryAndContents(RemoteJobStepWorkDirPath);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error deleting remote work directory", ex);
            }

        }

        /// <summary>
        /// Construct the base status file name, of the form JobX_StepY_TimeStamp
        /// </summary>
        /// <returns>Status filename</returns>
        /// <remarks>Uses the RemoteTimestamp job parameter</remarks>
        private string GetBaseStatusFilename()
        {

            var remoteTimestamp = JobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);
            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot properly construct the base tracking file name");
                return string.Empty;
            }

            return GetBaseStatusFilename(remoteTimestamp);
        }

        /// <summary>
        /// Construct the base status file name, of the form JobX_StepY_TimeStamp
        /// </summary>
        /// <param name="remoteTimestamp"></param>
        /// <returns>Status filename</returns>
        private string GetBaseStatusFilename(string remoteTimestamp)
        {
            return GetJobStepFileOrFolderName() + "_" + remoteTimestamp;
        }

        /// <summary>
        /// Return text in the form "Job1234_Step3"
        /// </summary>
        /// <returns></returns>
        /// <remarks>Intended for use in file and directory names</remarks>
        private string GetJobStepFileOrFolderName()
        {
            return string.Format("Job{0}_Step{1}", JobNum, StepNum);
        }

        /// <summary>
        /// Retrieve a listing of files in the remoteDirectoryPath directory on the remote host
        /// </summary>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="fileMatchSpec">Filename to find, or files to find if wildcards are used</param>
        /// <param name="recurse">True to </param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (typically should be true)</param>
        /// <returns>List of matching files (full paths)</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
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
        /// <param name="useDefaultManagerRemoteInfo"></param>
        /// <returns>String with XML</returns>
        /// <remarks>RemoteInfo is sent to the database via stored procedure SetStepTaskComplete</remarks>
        private string GetRemoteInfoXml(bool useDefaultManagerRemoteInfo)
        {

            if (useDefaultManagerRemoteInfo)
            {
                return GetRemoteInfoXml(MgrParams);
            }

            var settings = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("host", RemoteHostInfo.HostName),
                new KeyValuePair<string, string>("user", RemoteHostInfo.Username),
                new KeyValuePair<string, string>("dmsPrograms", RemoteHostInfo.BaseDirectoryPath),
                new KeyValuePair<string, string>("taskQueue", RemoteTaskQueuePath),
                new KeyValuePair<string, string>("workDir", RemoteWorkDirPath),
                new KeyValuePair<string, string>("orgDB", RemoteOrgDBPath),
                new KeyValuePair<string, string>("privateKey", Path.GetFileName(RemoteHostInfo.PrivateKeyFile)),
                new KeyValuePair<string, string>("passphrase", Path.GetFileName(RemoteHostInfo.PassphraseFile))
            };

            return ConstructRemoteInfoXml(settings);
        }

        /// <summary>
        /// Construct the default XML string that will be used for jobs staged by this manager
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <returns>String with XML</returns>
        /// <remarks>The RemoteInfo generated here is passed to RequestStepTaskXML when checking for an available job</remarks>
        public static string GetRemoteInfoXml(IMgrParams mgrParams)
        {
            var settings = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("host", mgrParams.GetParam("RemoteHostName")),
                new KeyValuePair<string, string>("user", mgrParams.GetParam("RemoteHostUser")),
                new KeyValuePair<string, string>("dmsPrograms", mgrParams.GetParam("RemoteHostDMSProgramsPath")),
                new KeyValuePair<string, string>("taskQueue", mgrParams.GetParam("RemoteTaskQueuePath")),
                new KeyValuePair<string, string>("workDir", mgrParams.GetParam("RemoteWorkDirPath")),
                new KeyValuePair<string, string>("orgDB", mgrParams.GetParam("RemoteOrgDBPath")),
                new KeyValuePair<string, string>("privateKey",  Path.GetFileName(mgrParams.GetParam("RemoteHostPrivateKeyFile"))),
                new KeyValuePair<string, string>("passphrase", Path.GetFileName(mgrParams.GetParam("RemoteHostPassphraseFile")))
            };

            return ConstructRemoteInfoXml(settings);
        }

        /// <summary>
        /// Find all status files for the current job and job step
        /// </summary>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (useful if checking on a running job)</param>
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
                var remoteTimestamp = JobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);
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
        /// Retrieve the JobX_StepY_RemoteTimestamp.jobstatus file from the remote TaskQueue folder
        /// </summary>
        /// <param name="jobStatusFilePathLocal"></param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Check for the existence of the file by calling GetStatusFiles and looking for a .jobstatus file </remarks>
        public bool RetrieveJobStatusFile(out string jobStatusFilePathLocal)
        {
            if (IsParameterUpdateRequired(false))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(false);
            }

            var remoteTimestamp = JobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_TIMESTAMP);

            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot retrieve the remote .jobstatus file");
                jobStatusFilePathLocal = string.Empty;
                return false;
            }

            return RetrieveStatusFile(JobStatusFile, out jobStatusFilePathLocal);
        }

        /// <summary>
        /// Retrieve the given status file from the remote TaskQueue folder
        /// </summary>
        /// <param name="statusFileName"></param>
        /// <param name="statusFilePathLocal">Output: full path to the status file on the local drive</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Check for the existence of the file by calling GetStatusFiles and looking for desired file </remarks>
        public bool RetrieveStatusFile(string statusFileName, out string statusFilePathLocal)
        {
            statusFilePathLocal = string.Empty;

            try
            {
                var sourceFileNames = new List<string> { statusFileName };

                var success = CopyFilesFromRemote(RemoteTaskQueuePathForTool, sourceFileNames, WorkDir);

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
        /// <returns>True if success, falser if an error</returns>
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
                var targetDirectoryPath = MgrParams.GetParam("RemoteHostDMSProgramsPath");

                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    OnErrorEvent("Manager parameter not found: RemoteHostDMSProgramsPath; cannot run the DMS Update Manager");
                    return false;
                }

                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    OnErrorEvent("Manager parameter not found: RemoteHostDMSProgramsPath; cannot run the DMS Update Manager");
                    return false;
                }

                var sourceDirectoryPath = string.Empty;

                if (string.IsNullOrEmpty(dmsUpdateMgrSource))
                {

                    var sourceCandidates = new List<string>
                    {
                        @"C:\DMS_Programs"
                    };

                    var appFolder = new DirectoryInfo(clsGlobal.GetAppFolderPath());
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

                OnDebugEvent(string.Format("Copying new/updated DMS Programs files from {0} to {1} on remote host {2}",
                                           sourceDirectoryPath, targetDirectoryPath, RemoteHostInfo.HostName));

                RemoteHostInfo.BaseDirectoryPath = targetDirectoryPath;
                var success = StartDMSUpdateManager(sourceDirectoryPath, targetDirectoryPath, filesToIgnore, out var errorMessage);

                if (success)
                    return true;

                var msg = "Error pushing DMS Programs files to the remote host; UpdateRemoteHost returns false";

                if (string.IsNullOrWhiteSpace(errorMessage))
                {
                    OnErrorEvent(msg);
                }
                else
                {
                    OnErrorEvent(msg + ": " + errorMessage);
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
        /// <param name="useDefaultManagerRemoteInfo">
        /// When true, use default manager remote info params
        /// When false, override remote settings using the RemoteInfo parameter in the StepParameters section for the job
        /// </param>
        /// <remarks>
        /// Throws an exception if any parameters are missing or empty
        /// Also throws an exception if there is an error reading the private key information
        /// </remarks>
        public void UpdateParameters(bool useDefaultManagerRemoteInfo)
        {

            WorkDir = MgrParams.GetParam("workdir");
            DebugLevel = MgrParams.GetParam("debuglevel", 2);

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

                var remoteInfo = JobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, STEP_PARAM_REMOTE_INFO);
                if (string.IsNullOrWhiteSpace(remoteInfo))
                {
                    throw new Exception("RemoteInfo job step parameter is empty; the RemoteTransferUtility cannot validate remote info");
                }

                try
                {
                    var doc = XDocument.Parse("<root>" + remoteInfo + "</root>");
                    var elements = doc.Elements("root").ToList();

                    RemoteHostInfo.HostName = clsXMLUtils.GetXmlValue(elements, "host");
                    RemoteHostInfo.Username = clsXMLUtils.GetXmlValue(elements, "user");

                    RemoteHostInfo.BaseDirectoryPath = clsXMLUtils.GetXmlValue(elements, "dmsPrograms");

                    var mgrPrivateKeyFilePath = MgrParams.GetParam("RemoteHostPrivateKeyFile");
                    var mgrPassphraseFilePath = MgrParams.GetParam("RemoteHostPassphraseFile");

                    var jobPrivateKeyFileName = clsXMLUtils.GetXmlValue(elements, "privateKey");
                    var jobPassphraseFileName = clsXMLUtils.GetXmlValue(elements, "passphrase");

                    RemoteHostInfo.PrivateKeyFile = clsPathUtils.ReplaceFilenameInPath(mgrPrivateKeyFilePath, jobPrivateKeyFileName);
                    RemoteHostInfo.PassphraseFile = clsPathUtils.ReplaceFilenameInPath(mgrPassphraseFilePath, jobPassphraseFileName);

                    RemoteTaskQueuePath = clsXMLUtils.GetXmlValue(elements, "taskQueue");
                    RemoteWorkDirPath = clsXMLUtils.GetXmlValue(elements, "workDir");
                    RemoteOrgDBPath = clsXMLUtils.GetXmlValue(elements, "orgDB");

                }
                catch (Exception ex)
                {
                    throw new Exception("Error parsing XML in the RemoteInfo job step parameter: " + ex.Message, ex);
                }

                mUsingManagerRemoteInfo = false;
            }

            JobNum = JobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            StepNum = JobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            StepTool = JobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "StepTool");
            DatasetName = JobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetNum");

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
        /// Wait for another manager to finish copying a file to a remote host
        /// </summary>
        /// <param name="sourceFile">Source file</param>
        /// <param name="remoteFile"></param>
        /// <param name="abortCopy"></param>
        /// <returns></returns>
        public bool WaitForRemoteFileCopy(FileInfo sourceFile, SftpFile remoteFile, out bool abortCopy)
        {
            var remoteFilePath = remoteFile.FullName;
            abortCopy = false;

            while (DateTime.UtcNow.Subtract(remoteFile.LastWriteTimeUtc).TotalMinutes < 15)
            {
                OnDebugEvent(string.Format("Waiting for another manager to finish copying the file to the remote host; " +
                                           "currently {0} bytes for {1} ", remoteFile.Length, remoteFilePath));

                // Wait for 1 minute
                clsGlobal.IdleLoop(60);

                // Update the info on the remote file
                var matchingFiles = GetRemoteFileListing(sourceFile.DirectoryName, sourceFile.Name);
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
                    OnDebugEvent(string.Format("File no longer exists on the remote host: {0}", remoteFilePath));
                    return false;
                }
            }

            return false;
        }
        #endregion
    }
}
