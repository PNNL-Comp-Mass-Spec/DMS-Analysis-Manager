using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.JobConfig;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.FileAndDirectoryTools
{
    /// <summary>
    /// Methods to copy files to the working directory
    /// </summary>
    public class FileCopyUtilities : EventNotifier
    {
        // Ignore Spelling: Emsl, holdoff

        private const int DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS = 15;

        private const string MYEMSL_PATH_FLAG = MyEMSLUtilities.MYEMSL_PATH_FLAG;

        /// <summary>
        /// Storage path info file suffix
        /// </summary>
        public const string STORAGE_PATH_INFO_FILE_SUFFIX = "_StoragePathInfo.txt";

        private readonly int mDebugLevel;

        private readonly MyEMSLUtilities mMyEMSLUtilities;

        private readonly FileTools mFileTools;

        /// <summary>
        /// Event raised to instruct the parent class to call ResetTimestampForQueueWaitTimeLogging
        /// </summary>
        public event ResetTimestampForQueueWaitTimeHandler ResetTimestampForQueueWaitTime;

        /// <summary>
        /// Delegate for ResetTimestampForQueueWaitTime
        /// </summary>
        public delegate void ResetTimestampForQueueWaitTimeHandler();

        /// <summary>
        /// Event raised when CopyWithLocks finishes
        /// </summary>
        public event CopyWithLocksCompleteHandler CopyWithLocksComplete;

        /// <summary>
        /// Delegate for CopyWithLocksComplete
        /// </summary>
        /// <param name="startTimeUtc">Start time, in UTC</param>
        /// <param name="destinationFilePath">Destination file path</param>
        public delegate void CopyWithLocksCompleteHandler(DateTime startTimeUtc, string destinationFilePath);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prismFileTools">FileTools instance</param>
        /// <param name="myEmslUtilities">MyEMSL utilities instance</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public FileCopyUtilities(
            FileTools prismFileTools,
            MyEMSLUtilities myEmslUtilities,
            short debugLevel)
        {
            mFileTools = prismFileTools;
            mMyEMSLUtilities = myEmslUtilities;
            mDebugLevel = debugLevel;
        }

        /// <summary>
        /// Copy a folder from one location to another, optionally skipping some files by name
        /// </summary>
        /// <param name="sourceFolderPath">The source directory path</param>
        /// <param name="destinationFolderPath">The destination directory path</param>
        /// <param name="fileNamesToSkip">
        /// List of file names to skip when copying the directory (and subdirectories)
        /// Can optionally contain full path names to skip
        /// </param>
        public void CopyDirectory(string sourceFolderPath, string destinationFolderPath, List<string> fileNamesToSkip)
        {
            OnResetTimestampForQueueWaitTime();
            mFileTools.CopyDirectory(sourceFolderPath, destinationFolderPath, fileNamesToSkip);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath)
        {
            const int MAX_ATTEMPTS = 3;
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                BaseLogger.LogLevels.ERROR, createStoragePathInfoOnly: false, maxCopyAttempts: MAX_ATTEMPTS);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath, BaseLogger.LogLevels logMsgTypeIfNotFound)
        {
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDir(
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            BaseLogger.LogLevels logMsgTypeIfNotFound,
            int maxCopyAttempts)
        {
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="createStoragePathInfoOnly">True if a storage path info file should be created instead of copying the file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDir(
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            BaseLogger.LogLevels logMsgTypeIfNotFound,
            bool createStoragePathInfoOnly,
            int maxCopyAttempts = 3)
        {
            try
            {
                if (sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    if (sourceFolderPath.Contains(DatasetInfoBase.MYEMSL_FILE_ID_TAG))
                    {
                        return mMyEMSLUtilities.AddFileToDownloadQueue(sourceFolderPath);
                    }
                    return mMyEMSLUtilities.AddFileToDownloadQueue(Path.Combine(sourceFolderPath, sourceFileName));
                }

                var sourceFilePath = Path.Combine(sourceFolderPath, sourceFileName);

                var destinationFilePath = Path.Combine(targetFolderPath, sourceFileName);

                // Verify source file exists
                const int HOLDOFF_SECONDS = 1;
                const int MAX_ATTEMPTS = 1;

                if (!FileExistsWithRetry(sourceFilePath, HOLDOFF_SECONDS, logMsgTypeIfNotFound, MAX_ATTEMPTS))
                {
                    // Errors have already been logged
                    return false;
                }

                if (createStoragePathInfoOnly)
                {
                    // Create a storage path info file
                    return CreateStoragePathInfoFile(sourceFilePath, destinationFilePath);
                }

                if (CopyFileWithRetry(sourceFilePath, destinationFilePath, true, maxCopyAttempts))
                {
                    if (mDebugLevel > 3)
                    {
                        OnStatusEvent("CopyFileToWorkDir, File copied: " + sourceFilePath);
                    }
                    return true;
                }

                OnErrorEvent("Error copying file " + sourceFilePath);
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyFileToWorkDir for " + Path.Combine(sourceFolderPath, sourceFileName), ex);
            }

            return false;
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDirWithRename(
            string datasetName,
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            BaseLogger.LogLevels logMsgTypeIfNotFound,
            int maxCopyAttempts)
        {
            return CopyFileToWorkDirWithRename(datasetName, sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory, renames destination with dataset name
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="createStoragePathInfoOnly">
        /// When true, does not actually copy the specified file, and instead creates a file named FileName_StoragePathInfo.txt
        /// The first line of the StoragePathInfo.txt file will be the full path to the source file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDirWithRename(
            string datasetName,
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            BaseLogger.LogLevels logMsgTypeIfNotFound,
            bool createStoragePathInfoOnly,
            int maxCopyAttempts)
        {
            var sourceFilePath = string.Empty;

            try
            {
                sourceFilePath = Path.Combine(sourceFolderPath, sourceFileName);

                // Verify source file exists
                if (!FileExistsWithRetry(sourceFilePath, logMsgTypeIfNotFound))
                {
                    LogMessageOrError("File not found: " + sourceFilePath, logMsgTypeIfNotFound);
                    return false;
                }

                var sourceFile = new FileInfo(sourceFilePath);
                var targetName = datasetName + sourceFile.Extension;
                var destinationFilePath = Path.Combine(targetFolderPath, targetName);

                if (createStoragePathInfoOnly)
                {
                    // Create a storage path info file
                    return CreateStoragePathInfoFile(sourceFilePath, destinationFilePath);
                }

                if (CopyFileWithRetry(sourceFilePath, destinationFilePath, true, maxCopyAttempts))
                {
                    if (mDebugLevel > 3)
                    {
                        OnStatusEvent("CopyFileToWorkDirWithRename, File copied: " + sourceFilePath);
                    }
                    return true;
                }

                OnErrorEvent("Error copying file " + sourceFilePath);
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyFileToWorkDirWithRename for " + sourceFilePath, ex);
            }

            return false;
        }

        /// <summary>
        /// Copies a file with retries in case of failure
        /// </summary>
        /// <remarks>Logs copy errors</remarks>
        /// <param name="sourceFilePath">Full path to source file</param>
        /// <param name="destinationFilePath">Full path to destination file</param>
        /// <param name="overwrite">True to overwrite existing destination file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileWithRetry(string sourceFilePath, string destinationFilePath, bool overwrite, int maxCopyAttempts = 3)
        {
            const int RETRY_HOLDOFF_SECONDS = 15;

            if (maxCopyAttempts < 1)
                maxCopyAttempts = 1;

            var retryCount = maxCopyAttempts;

            while (true)
            {
                try
                {
                    OnResetTimestampForQueueWaitTime();
                    var startTime = DateTime.UtcNow;

                    if (mFileTools.CopyFileUsingLocks(sourceFilePath, destinationFilePath, overwrite))
                    {
                        OnCopyWithLocksComplete(startTime, destinationFilePath);
                        return true;
                    }

                    OnErrorEvent("CopyFileUsingLocks returned false copying " + sourceFilePath + " to " + destinationFilePath);
                    return false;
                }
                catch (PathTooLongException ex)
                {
                    OnErrorEvent("Exception copying file " + sourceFilePath + " to " + destinationFilePath + "; path too long", ex);
                    return false;
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Exception copying file " + sourceFilePath + " to " + destinationFilePath + "; Retry Count = " + retryCount, ex);

                    retryCount--;

                    if (!overwrite && File.Exists(destinationFilePath))
                    {
                        OnErrorEvent("Tried to overwrite an existing file when Overwrite is false: " + destinationFilePath);
                        return false;
                    }

                    if (retryCount <= 0)
                        break;

                    // Wait several seconds before retrying
                    Global.IdleLoop(RETRY_HOLDOFF_SECONDS);
                }
            }

            // If we got to here, there were too many failures
            return false;
        }

        /// <summary>
        /// Creates a file at destinationFilePath but with "_StoragePathInfo.txt" appended to the name
        /// The file's contents is the path given by sourceFilePath
        /// </summary>
        /// <param name="sourceFilePath">The path to write to the StoragePathInfo file</param>
        /// <param name="destinationFilePath">The path where the file would have been copied to</param>
        public bool CreateStoragePathInfoFile(string sourceFilePath, string destinationFilePath)
        {
            var infoFilePath = string.Empty;

            try
            {
                if (sourceFilePath == null || destinationFilePath == null)
                {
                    return false;
                }

                infoFilePath = destinationFilePath + STORAGE_PATH_INFO_FILE_SUFFIX;

                using var writer = new StreamWriter(new FileStream(infoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(sourceFilePath);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateStoragePathInfoFile for " + infoFilePath, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName">File path</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        public bool FileExistsWithRetry(string fileName, BaseLogger.LogLevels logMsgTypeIfNotFound)
        {
            return FileExistsWithRetry(fileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS, logMsgTypeIfNotFound);
        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName">File path</param>
        /// <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>True if the file exists, otherwise false</returns>
        private bool FileExistsWithRetry(string fileName, int retryHoldoffSeconds, BaseLogger.LogLevels logMsgTypeIfNotFound)
        {
            const int MAX_ATTEMPTS = 3;
            return FileExistsWithRetry(fileName, retryHoldoffSeconds, logMsgTypeIfNotFound, MAX_ATTEMPTS);
        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName">File path</param>
        /// <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <returns>True if the file exists, otherwise false</returns>
        public bool FileExistsWithRetry(string fileName, int retryHoldoffSeconds, BaseLogger.LogLevels logMsgTypeIfNotFound, int maxAttempts)
        {
            if (maxAttempts < 1)
                maxAttempts = 1;

            if (maxAttempts > 10)
                maxAttempts = 10;

            var retryCount = maxAttempts;

            if (retryHoldoffSeconds <= 0)
                retryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS;

            if (retryHoldoffSeconds > 600)
                retryHoldoffSeconds = 600;

            while (retryCount > 0)
            {
                if (File.Exists(fileName))
                {
                    return true;
                }

                if (logMsgTypeIfNotFound == BaseLogger.LogLevels.ERROR)
                {
                    // Only log each failed attempt to find the file if logMsgTypeIfNotFound = ILogger.logMsgType.OnErrorEvent
                    // Otherwise, we won't log each failed attempt
                    LogMessageOrError(string.Format("File {0} not found. Retry count = {1}", fileName, retryCount), logMsgTypeIfNotFound);
                }

                retryCount--;

                if (retryCount > 0)
                {
                    // Wait RetryHoldoffSeconds seconds before retrying
                    Global.IdleLoop(retryHoldoffSeconds);
                }
            }

            // If we got to here, there were too many failures
            string logMessage;

            if (maxAttempts == 1)
            {
                logMessage = "File not found: " + fileName;
            }
            else
            {
                logMessage = "File not be found after " + maxAttempts + " tries: " + fileName;
            }

            LogMessageOrError(logMessage, logMsgTypeIfNotFound);

            return false;
        }

        /// <summary>
        /// Raise event CopyWithLocksComplete
        /// </summary>
        /// <param name="startTimeUtc">Time the copy started (or the time that CopyFileUsingLocks was called)</param>
        /// <param name="destinationFilePath">Destination file path (used to determine the file size)</param>
        private void OnCopyWithLocksComplete(DateTime startTimeUtc, string destinationFilePath)
        {
            CopyWithLocksComplete?.Invoke(startTimeUtc, destinationFilePath);
        }

        private void LogMessageOrError(string msg, BaseLogger.LogLevels logMsgTypeIfNotFound)
        {
            if (logMsgTypeIfNotFound == BaseLogger.LogLevels.ERROR)
                OnErrorEvent(msg);
            else if (logMsgTypeIfNotFound == BaseLogger.LogLevels.WARN)
                OnWarningEvent(msg);
            else
                OnStatusEvent(msg);
        }

        /// <summary>
        /// Instruct the parent class to call ResetTimestampForQueueWaitTimeLogging
        /// </summary>
        private void OnResetTimestampForQueueWaitTime()
        {
            ResetTimestampForQueueWaitTime?.Invoke();
        }
    }
}
