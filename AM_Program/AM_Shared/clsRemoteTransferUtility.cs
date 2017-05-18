using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using PRISM;
using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Used to transfer files to/from a remote host
    /// Use sftp for file listings
    /// Use scp for file transfers
    /// </summary>
    public class clsRemoteTransferUtility : clsEventNotifier
    {
        #region "Constants"

        public const string STEP_PARAM_REMOTE_INFO = "RemoteInfo";

        public const string STEP_PARAM_REMOTE_TIMESTAMP = "RemoteTimestamp";

        private const bool USE_MANAGER_REMOTE_INFO = true;

        #endregion

        #region "Module variables"

        private bool mParametersValidated;

        private bool mUsingManagerRemoteInfo;

        private PrivateKeyFile mPrivateKeyFile;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset Name
        /// </summary>
        public string DatasetName { get; private set; }

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
        /// Remote host name
        /// </summary>
        public string RemoteHostName { get; private set; }

        /// <summary>
        /// Remote host username
        /// </summary>
        public string RemoteHostUser { get; private set; }

        /// <summary>
        /// Path to the file with the RSA private key for connecting to RemoteHostName as user RemoteHostUser
        /// </summary>
        /// <remarks>
        /// For example, C:\DMS_RemoteInfo\user.key
        /// </remarks>
        public string RemoteHostPrivateKeyFile { get; private set; }

        /// <summary>
        /// Path to the file with the passphrase for the RSA private key
        /// </summary>
        /// <remarks>
        /// For example, C:\DMS_RemoteInfo\user.pass
        /// </remarks>
        public string RemoteHostPassphraseFile { get; private set; }

        /// <summary>
        /// Folder for FASTA files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSOrgDBs
        /// </remarks>
        public string RemoteOrgDBPath { get; private set; }

        /// <summary>
        /// Folder for task queue files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSTasks
        /// </remarks>
        public string RemoteTaskQueuePath { get; private set; }

        /// <summary>
        /// Folder for job task data files
        /// </summary>
        /// <remarks>
        /// For example, /file1/temp/DMSWorkDir
        /// </remarks>
        public string RemoteWorkDirPath { get; private set; }

        public string RemoteJobStepWorkDirPath => clsPathUtils.CombineLinuxPaths(RemoteWorkDirPath, GetJobStepFileOrFolderName());

        /// <summary>
        /// Step number for the current job
        /// </summary>
        public int StepNum { get; private set; }

        /// <summary>
        /// Local working directory
        /// </summary>
        public string WorkDir { get; private set; }

        /// <summary>
        /// Job number and text, in the form "job x, step y"
        /// </summary>
        public string JobStepDescription => string.Format("job {0}, step {1}", JobNum, StepNum);

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
        /// Filename of the .info file
        /// </summary>
        public string StatusLockFile => GetBaseStatusFilename() + ".lock";

        #endregion

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        public clsRemoteTransferUtility(IMgrParams mgrParams, IJobParams jobParams)
        {
            MgrParams = mgrParams;
            JobParams = jobParams;

            mParametersValidated = false;
            mUsingManagerRemoteInfo = true;

            // Initialize the properties to have empty strings
            DatasetName = string.Empty;
            RemoteHostName = string.Empty;
            RemoteHostUser = string.Empty;
            RemoteHostPrivateKeyFile = string.Empty;
            RemoteHostPassphraseFile = string.Empty;
            RemoteOrgDBPath = string.Empty;
            RemoteTaskQueuePath = string.Empty;
            RemoteWorkDirPath = string.Empty;
            WorkDir = string.Empty;
        }

        /// <summary>
        /// Copy files from the remote host to a local directory
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are not allowed</param>
        /// <param name="localDirectoryPath">Local target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (useful if checking on a running job)</param>
        /// <returns>True on success, false if an error</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        public bool CopyFilesFromRemote(string sourceDirectoryPath, IReadOnlyCollection<string> sourceFileNames, string localDirectoryPath, bool useDefaultManagerRemoteInfo)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            return CopyFilesFromRemote(sourceDirectoryPath, sourceFileNames, localDirectoryPath);
        }

        /// <summary>
        /// Copy files from the remote host to a local directory
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are not allowed</param>
        /// <param name="localDirectoryPath">Local target directory</param>
        /// <returns>True on success, false if an error</returns>
        private bool CopyFilesFromRemote(string sourceDirectoryPath, IReadOnlyCollection<string> sourceFileNames, string localDirectoryPath)
        {
            // Use scp to retrieve the files
            // scp is faster than sftp, but it has the downside that we can't check for the existence of a file before retrieving it

            var success = false;

            if (!mParametersValidated)
                throw new Exception("Call UpdateParameters before calling CopyFilesToRemote");

            try
            {
                if (sourceFileNames.Count == 1)
                    OnDebugEvent(string.Format("Retrieving file {0} from {1} on host {2}", sourceFileNames.First(), sourceDirectoryPath, RemoteHostName));
                else
                    OnDebugEvent(string.Format("Retrieving {0} files from {1} on host {2}", sourceFileNames.Count, sourceDirectoryPath, RemoteHostName));

                using (var scp = new ScpClient(RemoteHostName, RemoteHostUser, mPrivateKeyFile))
                {
                    scp.Connect();

                    foreach (var sourceFileName in sourceFileNames)
                    {
                        var remoteFilePath = clsPathUtils.CombineLinuxPaths(sourceDirectoryPath, sourceFileName);
                        var targetFile = new FileInfo(clsPathUtils.CombinePathsLocalSepChar(localDirectoryPath, sourceFileName));

                        try
                        {
                            scp.Download(remoteFilePath, targetFile);

                            targetFile.Refresh();
                            if (targetFile.Exists)
                                success = true;
                        }
                        catch (Exception ex)
                        {
                            // ToDo: Explicitly check for FileNotFound exceptions
                            OnWarningEvent(string.Format("Error copying {0}: {1}", remoteFilePath, ex.Message));
                        }

                    }

                    scp.Disconnect();

                }

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error copying files from {0}: {1}", sourceDirectoryPath, ex.Message), ex);
                return false;
            }

        }

        /// <summary>
        /// Copy a single file from a local directory to the remote host
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <returns>True on success, false if an error</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        public bool CopyFileToRemote(string sourceFilePath, string remoteDirectoryPath)
        {
            var sourceFile = new FileInfo(sourceFilePath);
            if (!sourceFile.Exists)
            {
                OnErrorEvent("Cannot copy file to remote; source file not found: " + sourceFilePath);
                return false;
            }

            var sourceFileNames = new List<string> {sourceFile.Name};

            var success = CopyFilesToRemote(sourceFile.DirectoryName, sourceFileNames, remoteDirectoryPath, USE_MANAGER_REMOTE_INFO);
            return success;
        }

        /// <summary>
        /// Copy files from a local directory to the remote host
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are allowed</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (typically should be true)</param>
        /// <returns>True on success, false if an error</returns>
        /// <remarks>Calls UpdateParameters if necessary; that method will throw an exception if there are missing parameters or configuration issues</remarks>
        public bool CopyFilesToRemote(string sourceDirectoryPath, IEnumerable<string> sourceFileNames, string remoteDirectoryPath, bool useDefaultManagerRemoteInfo)
        {
            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            return CopyFilesToRemote(sourceDirectoryPath, sourceFileNames, remoteDirectoryPath);
        }

        /// <summary>
        /// Copy files from a local directory to the remote host
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="sourceFileNames">Source file names; wildcards are allowed</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <returns>True on success, false if an error</returns>
        public bool CopyFilesToRemote(string sourceDirectoryPath, IEnumerable<string> sourceFileNames, string remoteDirectoryPath)
        {
            if (string.IsNullOrWhiteSpace(sourceDirectoryPath))
            {
                OnErrorEvent("Cannot copy files to remote; source directory is empty");
                return false;
            }

            var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
            if (!sourceDirectory.Exists)
            {
                OnErrorEvent("Cannot copy files to remote; source directory not found: " + sourceDirectoryPath);
                return false;
            }

            var sourceDirectoryFiles = sourceDirectory.GetFiles();
            var filesToCopy = new List<FileInfo>();

            foreach (var sourceFileName in sourceFileNames)
            {
                if (sourceFileName.Contains("*") || sourceFileName.Contains("?"))
                {
                    // Filename has a wildcard
                    var matchingFiles = sourceDirectory.GetFiles(sourceFileName);
                    filesToCopy.AddRange(matchingFiles);
                    continue;
                }

                var matchFound = false;
                foreach (var candidateFile in sourceDirectoryFiles)
                {
                    if (!string.Equals(sourceFileName, candidateFile.Name, StringComparison.OrdinalIgnoreCase))
                        continue;

                    filesToCopy.Add(candidateFile);
                    matchFound = true;
                    break;
                }

                if (!matchFound)
                {
                    OnWarningEvent(string.Format("Source file not found; cannot copy {0} to {1}",
                        Path.Combine(sourceDirectory.FullName, sourceFileName), remoteDirectoryPath));
                }
            }

            return CopyFilesToRemote(filesToCopy, remoteDirectoryPath);
        }

        /// <summary>
        /// Copy files from a local directory to the remote host
        /// </summary>
        /// <param name="sourceFiles">Source files</param>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <returns>True on success, false if an error</returns>
        public bool CopyFilesToRemote(IEnumerable<FileInfo> sourceFiles, string remoteDirectoryPath)
        {
            if (!mParametersValidated)
                throw new Exception("Call UpdateParameters before calling CopyFilesToRemote");

            try
            {

                var uniqueFiles = GetUniqueFileList(sourceFiles).ToList();
                if (uniqueFiles.Count == 0)
                {
                    OnErrorEvent(string.Format("Cannot copy files to {0}; sourceFiles list is empty", RemoteHostName));
                    return false;
                }

                var success = false;

                if (uniqueFiles.Count == 1)
                    OnDebugEvent(string.Format("Copying {0} to {1} on {2}", uniqueFiles.First().Name, remoteDirectoryPath, RemoteHostName));
                else
                    OnDebugEvent(string.Format("Copying {0} files to {1} on {2}", uniqueFiles.Count, remoteDirectoryPath, RemoteHostName));

                using (var scp = new ScpClient(RemoteHostName, RemoteHostUser, mPrivateKeyFile))
                {
                    scp.Connect();

                    foreach (var sourceFile in uniqueFiles)
                    {
                        if (!sourceFile.Exists)
                        {
                            OnWarningEvent(string.Format("Source file not found; cannot copy {0} to {1} on {2}",
                                                         sourceFile.FullName, remoteDirectoryPath, RemoteHostName));
                            continue;
                        }

                        OnDebugEvent("  Copying " + sourceFile.FullName);

                        var targetFilePath = clsPathUtils.CombineLinuxPaths(remoteDirectoryPath, sourceFile.Name);
                        scp.Upload(sourceFile, targetFilePath);

                        success = true;
                    }

                    scp.Disconnect();

                }

                if (success)
                {
                    return true;
                }

                OnErrorEvent(string.Format("Cannot copy files to {0}; all of the files in sourceFiles are missing", RemoteHostName));
                return false;

            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error copying files to {0} on {1}: {2}", remoteDirectoryPath, RemoteHostName, ex.Message), ex);
                return false;
            }

        }

        /// <summary>
        /// Copy files for the current job to the remote host
        /// </summary>
        /// <remarks>Creates a new subdirectory below RemoteWorkDirPath</remarks>
        /// <returns>True on success, false on an error</returns>
        public bool CopyJobTaskFilesToRemote()
        {
            if (IsParameterUpdateRequired(USE_MANAGER_REMOTE_INFO))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(USE_MANAGER_REMOTE_INFO);
            }

            try
            {

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error copying files to the remote host", ex);
                return false;
            }
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
                infoFilePathRemote = clsPathUtils.CombineLinuxPaths(RemoteTaskQueuePath, infoFileName);

                using (var writer = new StreamWriter(new FileStream(infoFilePathLocal, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("Job=" + JobNum);
                    writer.WriteLine("Step=" + StepNum);
                    writer.WriteLine("WorkDir=" + RemoteJobStepWorkDirPath);
                    writer.WriteLine("Staged=" + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }

                CopyFileToRemote(infoFilePathLocal, RemoteTaskQueuePath);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error creating the remote job task info file", ex);
                infoFilePathRemote = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Validates that the remote directory exists, creating it if missing
        /// </summary>
        /// <param name="remoteDirectoryPath"></param>
        /// <returns>True on success, otherwise false</returns>
        /// <remarks>The parent directory of remoteDirectoryPath must already exist</remarks>
        public bool CreateRemoteDirectory(string remoteDirectoryPath)
        {
            if (!mParametersValidated)
                throw new Exception("Call UpdateParameters before calling CreateRemoteDirectory");

            return CreateRemoteDirectories(new List<string> { remoteDirectoryPath });
        }

        /// <summary>
        /// Validates that the remote directories exists, creating any that are missing
        /// </summary>
        /// <param name="remoteDirectories"></param>
        /// <returns>True on success, otherwise false</returns>
        /// <remarks>The parent directory of all items in remoteDirectories must already exist</remarks>
        public bool CreateRemoteDirectories(IReadOnlyCollection<string> remoteDirectories)
        {

            if (!mParametersValidated)
                throw new Exception("Call UpdateParameters before calling CreateRemoteDirectories");

            try
            {
                if (remoteDirectories.Count == 0)
                    return true;

                // Keys in this dictionary are parent directory paths; values are subdirectories to find in each
                var parentDirectories = new Dictionary<string, SortedSet<string>>();
                foreach (var remoteDirectory in remoteDirectories)
                {
                    var parentPath = clsPathUtils.GetParentDirectoryPath(remoteDirectory, out var directoryName);
                    if (string.IsNullOrWhiteSpace(parentPath))
                        continue;

                    if (!parentDirectories.TryGetValue(parentPath, out var subDirectories))
                    {
                        subDirectories = new SortedSet<string>();
                        parentDirectories.Add(parentPath, subDirectories);
                    }

                    if (!subDirectories.Contains(directoryName))
                        subDirectories.Add(directoryName);

                }

                OnDebugEvent("Verifying directories on host " + RemoteHostName);

                using (var sftp = new SftpClient(RemoteHostName, RemoteHostUser, mPrivateKeyFile))
                {
                    sftp.Connect();
                    foreach (var parentDirectory in parentDirectories)
                    {
                        var remoteDirectoryPath = parentDirectory.Key;
                        OnDebugEvent("  checking " + remoteDirectoryPath);

                        var filesAndFolders = sftp.ListDirectory(remoteDirectoryPath);
                        var remoteSubdirectories = new SortedSet<string>();

                        foreach (var item in filesAndFolders)
                        {
                            if (!item.IsDirectory || item.Name == "." || item.Name == "..")
                            {
                                continue;
                            }

                            if (!remoteSubdirectories.Contains(item.Name))
                                remoteSubdirectories.Add(item.Name);
                        }

                        foreach (var directoryToVerify in parentDirectory.Value)
                        {
                            if (remoteSubdirectories.Contains(directoryToVerify))
                            {
                                OnDebugEvent("    found " + directoryToVerify);
                                continue;
                            }
                            var directoryPathToCreate = clsPathUtils.CombineLinuxPaths(remoteDirectoryPath, directoryToVerify);

                            OnDebugEvent("  creating " + directoryPathToCreate);
                            sftp.CreateDirectory(directoryPathToCreate);
                        }

                    }
                    sftp.Disconnect();
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error creating remote directories: " + ex.Message, ex);
                return false;
            }
        }

        private void DefineRemoteInfo()
        {
            var remoteInfo = GetRemoteInfoXml(USE_MANAGER_REMOTE_INFO);

            JobParams.AddAdditionalParameter("StepParameters", STEP_PARAM_REMOTE_INFO, remoteInfo);
        }

        private string DefineRemoteTimestamp()
        {
            var remoteTimestamp = DateTime.Now.ToString("yyyyMMdd_hhmm");

            JobParams.AddAdditionalParameter("StepParameters", STEP_PARAM_REMOTE_TIMESTAMP, remoteTimestamp);

            return remoteTimestamp;
        }

        /// <summary>
        /// Construct the base status file name, of the form JobX_StepY_TimeStamp
        /// </summary>
        /// <returns>Status filename</returns>
        /// <remarks>Uses the RemoteTimestamp job parameter</remarks>
        private string GetBaseStatusFilename()
        {

            var remoteTimestamp = JobParams.GetParam("StepParameters", STEP_PARAM_REMOTE_TIMESTAMP);
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
        public Dictionary<string, SftpFile> GetRemoteFileListing(string remoteDirectoryPath, string fileMatchSpec, bool recurse, bool useDefaultManagerRemoteInfo)
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
        /// Retrieve a listing of files in the remoteDirectoryPath directory on the remote host
        /// </summary>
        /// <param name="remoteDirectoryPath">Remote target directory</param>
        /// <param name="fileMatchSpec">Filename to find, or files to find if wildcards are used</param>
        /// <param name="recurse">True to find files in subdirectories</param>
        /// <returns>Dictionary of matching files, where keys are full file paths and values are instances of SFtpFile</returns>
        public Dictionary<string, SftpFile> GetRemoteFileListing(string remoteDirectoryPath, string fileMatchSpec, bool recurse = false)
        {
            var matchingFiles = new Dictionary<string, SftpFile>();

            if (!mParametersValidated)
                throw new Exception("Call UpdateParameters before calling CopyFilesToRemote");

            try
            {
                if (string.IsNullOrWhiteSpace(fileMatchSpec))
                    fileMatchSpec = "*";

                OnDebugEvent(string.Format("Getting file listing for {0} on host {1}", remoteDirectoryPath, RemoteHostName));

                using (var sftp = new SftpClient(RemoteHostName, RemoteHostUser, mPrivateKeyFile))
                {
                    sftp.Connect();
                    GetRemoteFileListing(sftp, new List<string> { remoteDirectoryPath }, fileMatchSpec, recurse, matchingFiles);
                    sftp.Disconnect();
                }

                return matchingFiles;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error retrieving remote file listing: " + ex.Message, ex);
                return matchingFiles;
            }
        }

        /// <summary>
        /// Retrieve a listing of files in the specified remote directories on the remote host
        /// </summary>
        /// <param name="sftp">sftp client</param>
        /// <param name="remoteDirectoryPaths">Paths to check</param>
        /// <param name="fileMatchSpec">Filename to find, or files to find if wildcards are used</param>
        /// <param name="recurse">True to find files in subdirectories</param>
        /// <param name="matchingFiles">Dictionary of matching files, where keys are full file paths and values are instances of SFtpFile</param>
        private void GetRemoteFileListing(
            SftpClient sftp,
            IEnumerable<string> remoteDirectoryPaths,
            string fileMatchSpec,
            bool recurse,
            IDictionary<string, SftpFile> matchingFiles)
        {
            foreach (var remoteDirectory in remoteDirectoryPaths)
            {
                var filesAndFolders = sftp.ListDirectory(remoteDirectory);
                var subdirectoryPaths = new List<string>();

                foreach (var item in filesAndFolders)
                {
                    if (item.IsDirectory)
                    {
                        subdirectoryPaths.Add(item.FullName);
                        continue;
                    }

                    if (fileMatchSpec == "*" || clsPathUtils.FitsMask(item.Name, fileMatchSpec))
                    {
                        try
                        {
                            matchingFiles.Add(item.FullName, item);
                        }
                        catch (ArgumentException)
                        {
                            OnWarningEvent("Skipping duplicate filename: " + item.FullName);
                        }

                    }
                }

                if (recurse && subdirectoryPaths.Count > 0)
                {
                    // Recursively call this function
                    GetRemoteFileListing(sftp, subdirectoryPaths, fileMatchSpec, true, matchingFiles);
                }
            }

        }

        /// <summary>
        /// Construct the XML string that should be stored as job parameter RemoteInfo
        /// </summary>
        /// <param name="useDefaultManagerRemoteInfo"></param>
        /// <returns>String with XML</returns>
        /// <remarks>RemoteInfo is sent to the database via stored procedure SetStepTaskComplete</remarks>
        public string GetRemoteInfoXml(bool useDefaultManagerRemoteInfo = true)
        {
            var settings = new List<Tuple<string, string>>();

            string privateKeyFile;
            string passphraseFile;

            if (useDefaultManagerRemoteInfo)
            {
                privateKeyFile = Path.GetFileName(MgrParams.GetParam("RemoteHostPrivateKeyFile"));
                passphraseFile = Path.GetFileName(MgrParams.GetParam("RemoteHostPassphraseFile"));

                settings.Add(new Tuple<string, string>("host", MgrParams.GetParam("RemoteHostName")));
                settings.Add(new Tuple<string, string>("user", MgrParams.GetParam("RemoteHostUser")));
                settings.Add(new Tuple<string, string>("taskQueue", MgrParams.GetParam("RemoteTaskQueuePath")));
                settings.Add(new Tuple<string, string>("workDir", MgrParams.GetParam("RemoteWorkDirPath")));
                settings.Add(new Tuple<string, string>("orgDB", MgrParams.GetParam("RemoteOrgDBPath")));

            }
            else
            {
                privateKeyFile = Path.GetFileName(RemoteHostPrivateKeyFile);
                passphraseFile = Path.GetFileName(RemoteHostPassphraseFile);

                settings.Add(new Tuple<string, string>("host", RemoteHostName));
                settings.Add(new Tuple<string, string>("user", RemoteHostUser));
                settings.Add(new Tuple<string, string>("taskQueue", RemoteTaskQueuePath));
                settings.Add(new Tuple<string, string>("workDir", RemoteWorkDirPath));
                settings.Add(new Tuple<string, string>("orgDB", RemoteOrgDBPath));

            }

            settings.Add(new Tuple<string, string>("privateKey", privateKeyFile));
            settings.Add(new Tuple<string, string>("passphrase", passphraseFile));

            var xmlText = new StringBuilder();
            foreach (var setting in settings)
            {
                xmlText.Append(string.Format("<{0}>{1}</{0}>", setting.Item1, setting.Item2));
            }

            // Convert to text, will look like
            // <host>PrismWeb2</host><user>svc-dms</user><taskQueue>/file1/temp/DMSTasks</taskQueue> ...
            return xmlText.ToString();
        }

        /// <summary>
        /// Find all status files for the current job and job step
        /// </summary>
        /// <param name="useDefaultManagerRemoteInfo">True to use RemoteInfo defined for the manager; False to use RemoteInfo associated with the job (useful if checking on a running job)</param>
        /// <returns>Dictionary of matching files, where keys are full file paths and values are instances of SFtpFile</returns>
        public Dictionary<string, SftpFile> GetStatusFiles(bool useDefaultManagerRemoteInfo = false)
        {
            Dictionary<string, SftpFile> statusFiles;

            if (IsParameterUpdateRequired(useDefaultManagerRemoteInfo))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(useDefaultManagerRemoteInfo);
            }

            var remoteTimestamp = JobParams.GetParam("StepParameters", STEP_PARAM_REMOTE_TIMESTAMP);
            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot list remote status files");
                statusFiles = new Dictionary<string, SftpFile>();
                return statusFiles;
            }

            var baseTrackingFilename = GetBaseStatusFilename();
            if (string.IsNullOrWhiteSpace(baseTrackingFilename))
            {
                statusFiles = new Dictionary<string, SftpFile>();
                return statusFiles;
            }

            statusFiles = GetRemoteFileListing(RemoteTaskQueuePath, baseTrackingFilename + "*");

            return statusFiles;
        }

        /// <summary>
        /// Return a listing of files where there are no duplicate files (based on full file path)
        /// </summary>
        /// <param name="files">File list to check</param>
        /// <param name="ignoreCase">True to ignore file case</param>
        /// <returns></returns>
        private static IEnumerable<FileInfo> GetUniqueFileList(IEnumerable<FileInfo> files, bool ignoreCase = true)
        {
            var comparer = ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
            var uniqueFiles = new Dictionary<string, FileInfo>(comparer);

            foreach (var file in files)
            {
                if (uniqueFiles.ContainsKey(file.FullName))
                    continue;

                uniqueFiles.Add(file.FullName, file);
            }

            return uniqueFiles.Values;
        }

        private bool IsParameterUpdateRequired(bool useDefaultManagerRemoteInfo)
        {
            return !mParametersValidated || mUsingManagerRemoteInfo != useDefaultManagerRemoteInfo;
        }

        private void LoadRSAPrivateKey()
        {

            var keyFile = new FileInfo(RemoteHostPrivateKeyFile);
            if (!keyFile.Exists)
            {
                throw new FileNotFoundException("Private key file not found: " + keyFile.FullName);
            }

            var passPhraseFile = new FileInfo(RemoteHostPassphraseFile);
            if (!passPhraseFile.Exists)
            {
                throw new FileNotFoundException("Passpharse file not found: " + passPhraseFile.FullName);
            }

            MemoryStream keyFileStream;
            string passphraseEncoded;

            try
            {
                using (var reader = new StreamReader(new FileStream(keyFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    keyFileStream = new MemoryStream(Encoding.ASCII.GetBytes(reader.ReadToEnd()));
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error reading the private key file: " + ex.Message, ex);
            }

            try
            {
                using (var reader = new StreamReader(new FileStream(passPhraseFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    passphraseEncoded = reader.ReadLine();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error reading the passpharse file: " + ex.Message, ex);
            }

            try
            {
                mPrivateKeyFile = new PrivateKeyFile(keyFileStream, clsGlobal.DecodePassword(passphraseEncoded));
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains("Invalid data type"))
                    throw new Exception("Invalid passphrase for the private key file; see manager params RemoteHostPrivateKeyFile and RemoteHostPassphraseFile", ex);

                throw new Exception("Error instantiating the private key " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Retrieve the JobX_StepY_RemoteTimestamp.jobstatus file (if it exists)
        /// </summary>
        /// <param name="jobStatusFilePathLocal"></param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Check for the existence of the file by calling GetStatusFiles and looking for a .jobstatus file </remarks>
        public bool RetrieveJobStatusFile(out string jobStatusFilePathLocal)
        {
            jobStatusFilePathLocal = string.Empty;

            if (IsParameterUpdateRequired(false))
            {
                // Validate that the required parameters are present and load the private key and passphrase from disk
                // This throws an exception if any parameters are missing
                UpdateParameters(false);
            }

            var remoteTimestamp = JobParams.GetParam("StepParameters", STEP_PARAM_REMOTE_TIMESTAMP);
            if (string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                OnErrorEvent("Job parameter RemoteTimestamp is empty; cannot retrieve the remote .jobstatus file");
                return false;
            }

            var sourceFileNames = new List<string> { StatusFileJobStatus };

            var success = CopyFilesFromRemote(RemoteTaskQueuePath, sourceFileNames, WorkDir);

            if (!success)
            {
                return false;
            }

            var jobStatusFile = new FileInfo(Path.Combine(WorkDir, StatusFileJobStatus));
            if (jobStatusFile.Exists)
            {
                jobStatusFilePathLocal = jobStatusFile.FullName;
                return true;
            }

            OnWarningEvent(".jobstatus file not found despite CopyFilesFromRemote reporting success: " + jobStatusFile.FullName);
            return false;
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

                RemoteHostName = MgrParams.GetParam("RemoteHostName");
                RemoteHostUser = MgrParams.GetParam("RemoteHostUser");

                RemoteHostPrivateKeyFile = MgrParams.GetParam("RemoteHostPrivateKeyFile");
                RemoteHostPassphraseFile = MgrParams.GetParam("RemoteHostPassphraseFile");

                RemoteTaskQueuePath = MgrParams.GetParam("RemoteTaskQueuePath");
                RemoteWorkDirPath = MgrParams.GetParam("RemoteWorkDirPath");
                RemoteOrgDBPath = MgrParams.GetParam("RemoteOrgDBPath");

                mUsingManagerRemoteInfo = true;
            }
            else
            {
                // Use settings defined for the running analysis job

                var remoteInfo = JobParams.GetJobParameter("StepParameters", "RemoteInfo");
                if (string.IsNullOrWhiteSpace(remoteInfo))
                {
                    throw new Exception("RemoteInfo job step parameter is empty; the RemoteTransferUtility cannot validate remote info");
                }

                try
                {
                    var doc = XDocument.Parse("<root>" + remoteInfo + "</root>");
                    var elements = doc.Elements("root").ToList();

                    RemoteHostName = clsXMLUtils.GetXmlValue(elements, "host");
                    RemoteHostUser = clsXMLUtils.GetXmlValue(elements, "user");

                    var mgrPrivateKeyFilePath = MgrParams.GetParam("RemoteHostPrivateKeyFile");
                    var mgrPassphraseFilePath = MgrParams.GetParam("RemoteHostPassphraseFile");

                    var jobPrivateKeyFileName = clsXMLUtils.GetXmlValue(elements, "privateKey");
                    var jobPassphraseFileName = clsXMLUtils.GetXmlValue(elements, "passphrase");

                    RemoteHostPrivateKeyFile = clsPathUtils.ReplaceFilenameInPath(mgrPrivateKeyFilePath, jobPrivateKeyFileName);
                    RemoteHostPassphraseFile = clsPathUtils.ReplaceFilenameInPath(mgrPassphraseFilePath, jobPassphraseFileName);

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

            JobNum = JobParams.GetJobParameter("StepParameters", "Job", 0);
            StepNum = JobParams.GetJobParameter("StepParameters", "Step", 0);
            DatasetName = JobParams.GetParam("JobParameters", "DatasetNum");

            if (string.IsNullOrWhiteSpace(WorkDir))
                throw new Exception("WorkDir parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteHostName))
                throw new Exception("RemoteHostName parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteHostUser))
                throw new Exception("RemoteHostUser parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteHostPrivateKeyFile))
                throw new Exception("RemoteHostPrivateKeyFile parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteHostPassphraseFile))
                throw new Exception("RemoteHostPassphraseFile parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteTaskQueuePath))
                throw new Exception("RemoteTaskQueuePath parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteWorkDirPath))
                throw new Exception("RemoteWorkDirPath parameter is empty; check the manager parameters");

            if (string.IsNullOrWhiteSpace(RemoteOrgDBPath))
                throw new Exception("RemoteOrgDBPath parameter is empty; check the manager parameters");

            if (JobNum == 0)
                throw new Exception("JobNum is zero; check the job parameters");

            if (string.IsNullOrWhiteSpace(DatasetName))
                throw new Exception("Dataset name is empty; check the job parameters");

            // Load the RSA private key info
            LoadRSAPrivateKey();

            mParametersValidated = true;
        }

        #endregion

    }
}
