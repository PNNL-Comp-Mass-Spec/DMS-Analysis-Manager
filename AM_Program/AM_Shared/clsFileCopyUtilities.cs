using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Methods to copy files to the working directory
    /// </summary>
    public class clsFileCopyUtilities : clsEventNotifier
    {

        #region "Constants"

        private const int DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS = 15;

        private const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        public const string STORAGE_PATH_INFO_FILE_SUFFIX = "_StoragePathInfo.txt";

        #endregion

        #region "Module variables"

        private readonly int m_DebugLevel;
        private readonly string m_MgrName;
        private readonly clsMyEMSLUtilities m_MyEMSLUtilities;

        private readonly clsFileTools m_FileTools;

        #endregion

        #region "Properties"

        #endregion

        #region "Events"

        public event ResetTimestampForQueueWaitTimeHandler ResetTimestampForQueueWaitTime;
        public delegate void ResetTimestampForQueueWaitTimeHandler();

        public event CopyWithLocksCompleteHandler CopyWithLocksComplete;
        public delegate void CopyWithLocksCompleteHandler(DateTime startTimeUtc, string destFilePath);

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prismFileTools"></param>
        /// <param name="myEmslUtilities"></param>
        /// <param name="mgrName"></param>
        /// <param name="debugLevel"></param>
        public clsFileCopyUtilities(
            clsFileTools prismFileTools,
            clsMyEMSLUtilities myEmslUtilities,
            string mgrName,
            short debugLevel)
        {
            m_FileTools = prismFileTools;
            m_MyEMSLUtilities = myEmslUtilities;
            m_MgrName = mgrName;
            m_DebugLevel = debugLevel;
        }

        /// <summary>
        /// Copy a folder from one location to another, optionally skipping some files by name
        /// </summary>
        /// <param name="sourceFolderPath">The source directory path</param>
        /// <param name="destFolderPath">The destination directory path</param>
        /// <param name="fileNamesToSkip">
        /// List of file names to skip when copying the directory (and subdirectories)
        /// Can optionally contain full path names to skip
        /// </param>
        public void CopyDirectory(string sourceFolderPath, string destFolderPath, List<string> fileNamesToSkip)
        {
            OnResetTimestampForQueueWaitTime();
            m_FileTools.CopyDirectory(sourceFolderPath, destFolderPath, fileNamesToSkip);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath)
        {

            const int MAX_ATTEMPTS = 3;
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly: false, maxCopyAttempts: MAX_ATTEMPTS);

        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, then sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {

            const int MAX_ATTEMPTS = 3;
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: MAX_ATTEMPTS);

        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, then sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            clsLogTools.LogLevels logMsgTypeIfNotFound,
            int maxCopyAttempts)
        {

            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);

        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="createStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            clsLogTools.LogLevels logMsgTypeIfNotFound,
            bool createStoragePathInfoOnly)
        {

            const int MAX_ATTEMPTS = 3;
            return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly, MAX_ATTEMPTS);

        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="createStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, then sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            clsLogTools.LogLevels logMsgTypeIfNotFound,
            bool createStoragePathInfoOnly,
            int maxCopyAttempts)
        {

            try
            {
                var sourceFilePath = Path.Combine(sourceFolderPath, sourceFileName);

                if (sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath);
                }

                var destFilePath = Path.Combine(targetFolderPath, sourceFileName);

                // Verify source file exists
                const int HOLDOFF_SECONDS = 1;
                const int MAX_ATTEMPTS = 1;
                if (!FileExistsWithRetry(sourceFilePath, HOLDOFF_SECONDS, logMsgTypeIfNotFound, MAX_ATTEMPTS))
                {
                    OnErrorEvent("File not found: " + sourceFilePath);
                    return false;
                }

                if (createStoragePathInfoOnly)
                {
                    // Create a storage path info file
                    return CreateStoragePathInfoFile(sourceFilePath, destFilePath);
                }

                if (CopyFileWithRetry(sourceFilePath, destFilePath, true, maxCopyAttempts))
                {
                    if (m_DebugLevel > 3)
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
                OnErrorEvent("Exception in CopyFileToWorkDir for " + Path.Combine(sourceFolderPath, sourceFileName), ex);
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
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool CopyFileToWorkDirWithRename(
            string datasetName,
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath)
        {
            const int maxCopyAttempts = 3;
            return CopyFileToWorkDirWithRename(datasetName, sourceFileName, sourceFolderPath, targetFolderPath,
                clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool CopyFileToWorkDirWithRename(string datasetName, string sourceFileName, string sourceFolderPath, string targetFolderPath, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {
            const int maxCopyAttempts = 3;
            return CopyFileToWorkDirWithRename(datasetName, sourceFileName, sourceFolderPath, targetFolderPath,
                logMsgTypeIfNotFound, createStoragePathInfoOnly: false, maxCopyAttempts: maxCopyAttempts);
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
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool CopyFileToWorkDirWithRename(
            string datasetName,
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            clsLogTools.LogLevels logMsgTypeIfNotFound,
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
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool CopyFileToWorkDirWithRename(
            string datasetName,
            string sourceFileName,
            string sourceFolderPath,
            string targetFolderPath,
            clsLogTools.LogLevels logMsgTypeIfNotFound,
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
                    var msg = "File not found: " + sourceFilePath;
                    LogMessageOrError(msg, logMsgTypeIfNotFound);
                    return false;
                }

                var sourceFile = new FileInfo(sourceFilePath);
                var TargetName = datasetName + sourceFile.Extension;
                var destFilePath = Path.Combine(targetFolderPath, TargetName);

                if (createStoragePathInfoOnly)
                {
                    // Create a storage path info file
                    return CreateStoragePathInfoFile(sourceFilePath, destFilePath);
                }

                if (CopyFileWithRetry(sourceFilePath, destFilePath, true, maxCopyAttempts))
                {
                    if (m_DebugLevel > 3)
                    {
                        OnStatusEvent("CopyFileToWorkDirWithRename, File copied: " + sourceFilePath);
                    }
                    return true;
                }
                else
                {
                    OnErrorEvent("Error copying file " + sourceFilePath);
                    return false;
                }

            }
            catch (Exception ex)
            {
                if (sourceFilePath == null)
                    sourceFilePath = sourceFileName;

                if (sourceFilePath == null)
                    sourceFilePath = "??";

                OnErrorEvent("Exception in CopyFileToWorkDirWithRename for " + sourceFilePath, ex);
            }

            return false;

        }


        /// <summary>
        /// Copies a file with retries in case of failure
        /// </summary>
        /// <param name="SrcFilePath">Full path to source file</param>
        /// <param name="DestFilePath">Full path to destination file</param>
        /// <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
        /// <returns>TRUE for success; FALSE for error</returns>
        /// <remarks>Logs copy errors</remarks>
        public bool CopyFileWithRetry(string SrcFilePath, string DestFilePath, bool Overwrite)
        {
            const int maxCopyAttempts = 3;
            return CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, maxCopyAttempts);
        }

        /// <summary>
        /// Copies a file with retries in case of failure
        /// </summary>
        /// <param name="srcFilePath">Full path to source file</param>
        /// <param name="destFilePath">Full path to destination file</param>
        /// <param name="overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
        /// <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
        /// <returns>TRUE for success; FALSE for error</returns>
        /// <remarks>Logs copy errors</remarks>
        public bool CopyFileWithRetry(string srcFilePath, string destFilePath, bool overwrite, int maxCopyAttempts)
        {

            const int RETRY_HOLDOFF_SECONDS = 15;

            if (maxCopyAttempts < 1)
                maxCopyAttempts = 1;
            var retryCount = maxCopyAttempts;

            while (retryCount > 0)
            {
                try
                {
                    OnResetTimestampForQueueWaitTime();
                    var startTime = DateTime.UtcNow;

                    if (m_FileTools.CopyFileUsingLocks(srcFilePath, destFilePath, m_MgrName, overwrite))
                    {
                        OnCopyWithLocksComplete(startTime, destFilePath);
                        return true;
                    }

                    OnErrorEvent("CopyFileUsingLocks returned false copying " + srcFilePath + " to " + destFilePath);
                    return false;
                }
                catch (PathTooLongException ex)
                {
                    OnErrorEvent("Exception copying file " + srcFilePath + " to " + destFilePath + "; path too long", ex);
                    return false;
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Exception copying file " + srcFilePath + " to " + destFilePath + "; Retry Count = " + retryCount, ex);

                    retryCount -= 1;

                    if (!overwrite && File.Exists(destFilePath))
                    {
                        OnErrorEvent("Tried to overwrite an existing file when Overwrite = False: " + destFilePath);
                        return false;
                    }

                    // Wait several seconds before retrying
                    Thread.Sleep(RETRY_HOLDOFF_SECONDS * 1000);
                }
            }

            // If we got to here, there were too many failures
            if (retryCount < 1)
            {
                return false;
            }

            return false;

        }

        /// <summary>
        /// Creates a file named DestFilePath but with "_StoragePathInfo.txt" appended to the name
        /// The file's contents is the path given by sourceFilePath
        /// </summary>
        /// <param name="sourceFilePath">The path to write to the StoragePathInfo file</param>
        /// <param name="DestFilePath">The path where the file would have been copied to</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool CreateStoragePathInfoFile(string sourceFilePath, string DestFilePath)
        {

            var infoFilePath = string.Empty;

            try
            {
                if (sourceFilePath == null || DestFilePath == null)
                {
                    return false;
                }

                infoFilePath = DestFilePath + STORAGE_PATH_INFO_FILE_SUFFIX;

                using (var swOutFile = new StreamWriter(new FileStream(infoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutFile.WriteLine(sourceFilePath);
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in CreateStoragePathInfoFile for " + infoFilePath, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool FileExistsWithRetry(string fileName, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {

            return FileExistsWithRetry(fileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS, logMsgTypeIfNotFound);

        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>True if the file exists, otherwise false</returns>
        /// <remarks></remarks>
        private bool FileExistsWithRetry(string fileName, int retryHoldoffSeconds, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {

            const int MAX_ATTEMPTS = 3;
            return FileExistsWithRetry(fileName, retryHoldoffSeconds, logMsgTypeIfNotFound, MAX_ATTEMPTS);

        }

        /// <summary>
        /// Test for file existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <returns>True if the file exists, otherwise false</returns>
        /// <remarks></remarks>
        public bool FileExistsWithRetry(string fileName, int retryHoldoffSeconds, clsLogTools.LogLevels logMsgTypeIfNotFound, int maxAttempts)
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

                if (logMsgTypeIfNotFound == clsLogTools.LogLevels.ERROR)
                {
                    // Only log each failed attempt to find the file if logMsgTypeIfNotFound = ILogger.logMsgType.OnErrorEvent
                    // Otherwise, we won't log each failed attempt
                    var msg = "File " + fileName + " not found. Retry count = " + retryCount;
                    LogMessageOrError(msg, logMsgTypeIfNotFound);
                }

                retryCount -= 1;
                if (retryCount > 0)
                {
                    // Wait RetryHoldoffSeconds seconds before retrying
                    Thread.Sleep(new TimeSpan(0, 0, retryHoldoffSeconds));
                }
            }

            // If we got to here, there were too many failures
            if (retryCount < 1)
            {
                string msg;
                if (maxAttempts == 1)
                {
                    msg = "File not found: " + fileName;
                }
                else
                {
                    msg = "File not be found after " + maxAttempts + " tries: " + fileName;
                }

                LogMessageOrError(msg, logMsgTypeIfNotFound);

                return false;
            }

            return false;

        }

        /// <summary>
        /// Raise event CopyWithLocksComplete
        /// </summary>
        /// <param name="startTimeUtc">Time the copy started (or the time that CopyFileUsingLocks was called)</param>
        /// <param name="destFilePath">Destination file path (used to determine the file size)</param>
        private void OnCopyWithLocksComplete(DateTime startTimeUtc, string destFilePath)
        {
            CopyWithLocksComplete?.Invoke(startTimeUtc, destFilePath);
        }

        private void LogMessageOrError(string msg, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {
            if (logMsgTypeIfNotFound == clsLogTools.LogLevels.ERROR)
                OnErrorEvent(msg);
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


        #endregion
    }
}
