using AnalysisManagerBase;
using System;
using System.Data;
using System.IO;
using System.Security.AccessControl;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.StatusReporting;
using PRISMDatabaseUtils;

namespace AnalysisManagerProg
{
    /// <summary>
    /// This creates and deletes file flagFile.txt when the manager starts and stops
    /// It also can remove files left behind in the working directory
    /// </summary>
    public class CleanupMgrErrors : LoggerBase
    {
        // Ignore Spelling: Decon, holdoff, Prog

        private const string SP_NAME_REPORT_MGR_ERROR_CLEANUP = "report_manager_error_cleanup";

        private const int DEFAULT_HOLDOFF_SECONDS = 3;

        /// <summary>
        /// File indicating that the manager is running
        /// </summary>
        public const string FLAG_FILE_NAME = "flagFile.txt";

        /// <summary>
        /// Flag file for DeconTools
        /// </summary>
        /// <remarks>Likely unused in 2017</remarks>
        private const string DECON_SERVER_FLAG_FILE_NAME = "flagFile_Svr.txt";

        /// <summary>
        /// Special file indicating that there was a problem removing files in the working directory and the manager thus exited
        /// On the next start of the manager, this file will be auto-deleted
        /// </summary>
        public const string ERROR_DELETING_FILES_FILENAME = "Error_Deleting_Files_Please_Delete_Me.txt";

        /// <summary>
        /// Options for auto-removing files from the working directory when the manager starts
        /// </summary>
        public enum CleanupModes
        {
            /// <summary>
            /// Never auto-remove files from the working directory
            /// </summary>
            Disabled = 0,

            /// <summary>
            /// Auto-remove files from the working directory once
            /// </summary>
            CleanupOnce = 1,

            /// <summary>
            /// Always auto-remove files from the working directory
            /// </summary>
            CleanupAlways = 2
        }

        /// <summary>
        /// Cleanup status codes for stored procedure report_manager_error_cleanup
        /// </summary>
        public enum CleanupActionCodes
        {
            /// <summary>
            /// Starting
            /// </summary>
            Start = 1,

            /// <summary>
            /// Success
            /// </summary>
            Success = 2,

            /// <summary>
            /// Failed
            /// </summary>
            Fail = 3
        }

        /// <summary>
        /// Full path to the flag file
        /// </summary>
        /// <remarks>Will be in the same directory as the manager executable</remarks>
        public string FlagFilePath => Path.Combine(mMgrDirectoryPath, FLAG_FILE_NAME);

        private readonly bool mInitialized;
        private readonly string mMgrConfigDBConnectionString;
        private readonly string mManagerName;

        private readonly string mMgrDirectoryPath;
        private readonly bool mTraceMode;
        private readonly string mWorkingDirPath;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrConfigDBConnectionString">Connection string to the manager_control database; if empty, database access is disabled</param>
        /// <param name="managerName">Manager name</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="mgrDirectoryPath">Manager directory path</param>
        /// <param name="workingDirPath">Working directory path</param>
        /// <param name="traceMode">True if trace mode is enabled</param>
        public CleanupMgrErrors(
            string mgrConfigDBConnectionString,
            string managerName,
            short debugLevel,
            string mgrDirectoryPath,
            string workingDirPath,
            bool traceMode)
        {
            if (string.IsNullOrEmpty(mgrConfigDBConnectionString) && !Global.OfflineMode)
                throw new Exception("Manager config DB connection string is not defined");

            if (string.IsNullOrEmpty(managerName))
                throw new Exception("Manager name is not defined");

            mMgrConfigDBConnectionString = string.Copy(mgrConfigDBConnectionString ?? "");
            mManagerName = managerName;
            mDebugLevel = debugLevel;

            mMgrDirectoryPath = mgrDirectoryPath;
            mTraceMode = traceMode;
            mWorkingDirPath = workingDirPath;

            mInitialized = true;
        }

        /// <summary>
        /// Automatically clean old files from the work directory if managerErrorCleanupMode is not CleanupModes.Disabled
        /// </summary>
        /// <param name="managerErrorCleanupMode">Manager error cleanup mode enum</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public bool AutoCleanupManagerErrors(CleanupModes managerErrorCleanupMode, int debugLevel)
        {
            if (!mInitialized)
                return false;

            if (managerErrorCleanupMode == CleanupModes.Disabled)
                return false;

            LogMessage("Attempting to automatically clean the work directory");

            // Call SP report_manager_error_cleanup with @ActionCode=1
            ReportManagerErrorCleanup(CleanupActionCodes.Start);

            // Delete all directories and subdirectories in the work directory
            var success = CleanWorkDir(mWorkingDirPath, 1);
            string failureMessage;

            if (!success)
            {
                failureMessage = "unable to clear work directory";
            }
            else
            {
                // If successful, deletes flag files: flagFile.txt and flagFile_Svr.txt
                success = DeleteDeconServerFlagFile(debugLevel);

                if (!success)
                {
                    failureMessage = "error deleting " + DECON_SERVER_FLAG_FILE_NAME;
                }
                else
                {
                    success = DeleteStatusFlagFile(debugLevel);

                    if (!success)
                    {
                        failureMessage = "error deleting " + FLAG_FILE_NAME;
                    }
                    else
                    {
                        failureMessage = string.Empty;
                    }
                }
            }

            // If successful, call SP report_manager_error_cleanup with @ActionCode=2
            // Otherwise call SP report_manager_error_cleanup with @ActionCode=3

            if (success)
            {
                ReportManagerErrorCleanup(CleanupActionCodes.Success);
            }
            else
            {
                ReportManagerErrorCleanup(CleanupActionCodes.Fail, failureMessage);
            }

            return success;
        }

        /// <summary>
        /// Deletes all files in working directory (using a 3 second holdoff after calling GC.Collect via PRISM.ProgRunner.GarbageCollectNow)
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool CleanWorkDir()
        {
            return CleanWorkDir(mWorkingDirPath, DEFAULT_HOLDOFF_SECONDS);
        }

        /// <summary>
        /// Deletes all files in working directory
        /// </summary>
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.ProgRunner.GarbageCollectNow()</param>
        /// <returns>True if success, false if an error</returns>
        public bool CleanWorkDir(float holdoffSeconds)
        {
            return CleanWorkDir(mWorkingDirPath, holdoffSeconds);
        }

        /// <summary>
        /// Deletes all files in working directory
        /// </summary>
        /// <param name="workDirPath">Full path to working directory</param>
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.ProgRunner.GarbageCollectNow()</param>
        /// <returns>True if success, false if an error</returns>
        private bool CleanWorkDir(string workDirPath, float holdoffSeconds)
        {
            if (Global.RunningOnDeveloperComputer() && holdoffSeconds > 1)
                holdoffSeconds = 1;

            // Assure that the holdoff is between 0.1 and 300 seconds
            var actualHoldoffSeconds = Math.Min(300, Math.Max(0.1, holdoffSeconds));

            // Try to ensure there are no open objects with file handles
            PRISM.AppUtils.GarbageCollectNow();
            Global.IdleLoop(actualHoldoffSeconds);

            // Delete all the files and directories in the work directory
            var workDir = new DirectoryInfo(workDirPath);
            return DeleteFilesWithRetry(workDir);
        }

        private bool DeleteFilesWithRetry(DirectoryInfo workDir)
        {
            const int DELETE_RETRY_COUNT = 3;

            var failedDeleteCount = 0;
            var fileTools = new PRISM.FileTools(mManagerName, mDebugLevel);

            // Delete the files
            try
            {
                foreach (var fileToDelete in workDir.GetFiles())
                {
                    if (!fileTools.DeleteFileWithRetry(fileToDelete, DELETE_RETRY_COUNT, out var errorMessage))
                    {
                        LogError(errorMessage);
                        failedDeleteCount++;
                    }
                }

                // Delete the sub directories
                foreach (var subdirectory in workDir.GetDirectories())
                {
                    if (DeleteFilesWithRetry(subdirectory))
                    {
                        // Remove the directory if it is empty
                        subdirectory.Refresh();

                        if (subdirectory.GetFileSystemInfos().Length != 0)
                            continue;

                        try
                        {
                            subdirectory.Delete();
                        }
                        catch (IOException)
                        {
                            // Try re-applying the permissions

                            var directoryAcl = new DirectorySecurity();
                            var currentUser = Environment.UserDomainName + @"\" + Environment.UserName;

                            LogWarning("IOException deleting " + subdirectory.FullName + "; will try granting modify access to user " + currentUser);
                            directoryAcl.AddAccessRule(new FileSystemAccessRule(
                                                           currentUser,
                                                           FileSystemRights.Modify,
                                                           InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                                                           PropagationFlags.None,
                                                           AccessControlType.Allow));

                            try
                            {
                                // To remove existing permissions, use this: directoryAcl.SetAccessRuleProtection(true, false)

                                // Add the new access rule
                                subdirectory.SetAccessControl(directoryAcl);

                                // Make sure the ReadOnly flag and System flags are not set
                                // It's likely not even possible for a directory to have a ReadOnly flag set, but it doesn't hurt to check
                                subdirectory.Refresh();
                                var attributes = subdirectory.Attributes;

                                if ((attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly ||
                                    (attributes & FileAttributes.System) == FileAttributes.System)
                                {
                                    subdirectory.Attributes = attributes & ~FileAttributes.ReadOnly & ~FileAttributes.System;
                                }

                                try
                                {
                                    // Retry the delete
                                    subdirectory.Delete();
                                    LogDebug("Updated permissions, then successfully deleted the directory");
                                }
                                catch (Exception ex3)
                                {
                                    var failureMessage = "Error deleting directory " + subdirectory.FullName + ": " + ex3.Message;
                                    LogError(failureMessage);
                                    failedDeleteCount++;
                                }
                            }
                            catch (Exception ex2)
                            {
                                var failureMessage = "Error updating permissions for directory " + subdirectory.FullName + ": " + ex2.Message;
                                LogError(failureMessage);
                                failedDeleteCount++;
                            }
                        }
                        catch (Exception ex)
                        {
                            var failureMessage = "Error deleting directory " + subdirectory.FullName + ": " + ex.Message;
                            LogError(failureMessage);
                            failedDeleteCount++;
                        }
                    }
                    else
                    {
                        var failureMessage = "Error deleting working directory subdirectory " + subdirectory.FullName;
                        LogError(failureMessage);
                        failedDeleteCount++;
                    }
                }
            }
            catch (Exception ex)
            {
                var failureMessage = "Error deleting files/directories in " + workDir.FullName;
                LogError(failureMessage, ex);
                return false;
            }

            // Return true if no failures
            return failedDeleteCount == 0;
        }

        /// <summary>
        /// Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
        /// </summary>
        public void CreateErrorDeletingFilesFlagFile()
        {
            try
            {
                var path = Path.Combine(mMgrDirectoryPath, ERROR_DELETING_FILES_FILENAME);

                using var writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read));

                writer.WriteLine(DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
            }
            catch (Exception ex)
            {
                LogError("Error creating " + ERROR_DELETING_FILES_FILENAME, ex);
            }
        }

        /// <summary>
        /// Creates a dummy file in the application directory to be used for controlling job request bypass
        /// </summary>
        public void CreateStatusFlagFile()
        {
            try
            {
                var path = FlagFilePath;

                using var writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read));

                writer.WriteLine(DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));
            }
            catch (Exception ex)
            {
                LogError("Error creating " + FLAG_FILE_NAME, ex);
            }
        }

        /// <summary>
        /// Deletes the Decon2LS OA Server flag file
        /// </summary>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        public bool DeleteDeconServerFlagFile(int debugLevel)
        {
            var flagFilePath = Path.Combine(mMgrDirectoryPath, DECON_SERVER_FLAG_FILE_NAME);

            return DeleteFlagFile(flagFilePath, debugLevel);
        }

        /// <summary>
        /// Deletes the file given by flagFilePath
        /// </summary>
        /// <param name="flagFilePath">Full path to the file to delete</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        private bool DeleteFlagFile(string flagFilePath, int debugLevel)
        {
            try
            {
                if (File.Exists(flagFilePath))
                {
                    try
                    {
                        // DeleteFileWithRetries will throw an exception if it cannot delete the file
                        // Thus, need to wrap it with an Exception handler

                        if (AnalysisToolRunnerBase.DeleteFileWithRetries(flagFilePath, debugLevel))
                        {
                            return true;
                        }

                        LogError("Error deleting file " + flagFilePath);
                        return false;
                    }
                    catch (Exception ex)
                    {
                        LogError("DeleteFlagFile", ex);
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("DeleteFlagFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Deletes the analysis manager flag file
        /// </summary>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        public bool DeleteStatusFlagFile(int debugLevel)
        {
            var flagFilePath = FlagFilePath;

            return DeleteFlagFile(flagFilePath, debugLevel);
        }

        /// <summary>
        /// Determines if error deleting files flag file exists in application directory
        /// </summary>
        /// <returns>True if flag file exists, otherwise false</returns>
        public bool DetectErrorDeletingFilesFlagFile()
        {
            var testFile = Path.Combine(mMgrDirectoryPath, ERROR_DELETING_FILES_FILENAME);

            return File.Exists(testFile);
        }

        /// <summary>
        /// Determines if flag file exists in application directory
        /// </summary>
        /// <returns>True if flag file exists, otherwise false</returns>
        public bool DetectStatusFlagFile()
        {
            var flagFile = new FileInfo(FlagFilePath);

            return flagFile.Exists;
        }

        /// <summary>
        /// Deletes the error deleting files flag file
        /// </summary>
        public void DeleteErrorDeletingFilesFlagFile()
        {
            var deletionFlagFile = new FileInfo(Path.Combine(mMgrDirectoryPath, ERROR_DELETING_FILES_FILENAME));

            try
            {
                if (deletionFlagFile.Exists)
                {
                    deletionFlagFile.Delete();
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting " + ERROR_DELETING_FILES_FILENAME, ex);
            }
        }

        private void ReportManagerErrorCleanup(CleanupActionCodes mgrCleanupActionCode)
        {
            ReportManagerErrorCleanup(mgrCleanupActionCode, string.Empty);
        }

        private void ReportManagerErrorCleanup(CleanupActionCodes mgrCleanupActionCode, string failureMessage)
        {
            if (string.IsNullOrWhiteSpace(mMgrConfigDBConnectionString))
            {
                if (Global.OfflineMode)
                    LogDebug("Skipping call to " + SP_NAME_REPORT_MGR_ERROR_CLEANUP + " since offline");
                else
                    LogError("Skipping call to " + SP_NAME_REPORT_MGR_ERROR_CLEANUP + " since the Manager Control connection string is empty");

                return;
            }

            try
            {
                failureMessage ??= string.Empty;

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(mMgrConfigDBConnectionString, mManagerName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mTraceMode);
                RegisterEvents(dbTools);

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_REPORT_MGR_ERROR_CLEANUP, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, mManagerName);
                dbTools.AddParameter(cmd, "@state", SqlType.Int).Value = mgrCleanupActionCode;
                dbTools.AddParameter(cmd, "@failureMsg", SqlType.VarChar, 512, failureMessage);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

                // Call the procedure
                var resCode = dbTools.ExecuteSP(cmd);

                if (resCode == 0)
                {
                    return;
                }

                LogError("ExecuteSP() reported result code {0} calling {1}", resCode, SP_NAME_REPORT_MGR_ERROR_CLEANUP);
            }
            catch (Exception ex)
            {
                string errorMessage;

                if (mMgrConfigDBConnectionString == null)
                {
                    errorMessage = "Error calling " + SP_NAME_REPORT_MGR_ERROR_CLEANUP + " in ReportManagerErrorCleanup; empty connection string";
                }
                else
                {
                    errorMessage = "Error calling " + SP_NAME_REPORT_MGR_ERROR_CLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                }

                LogError(errorMessage, ex);
            }
        }
    }
}
