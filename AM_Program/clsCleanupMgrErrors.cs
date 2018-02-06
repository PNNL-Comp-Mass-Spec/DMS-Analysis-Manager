using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Security.AccessControl;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    /// <summary>
    /// This creates and deletes file flagFile.txt when the manager starts and stops
    /// It also can remove files left behind in the working directory
    /// </summary>
    public class clsCleanupMgrErrors : clsLoggerBase
    {
        #region "Constants"

        private const string SP_NAME_REPORTMGRCLEANUP = "ReportManagerErrorCleanup";

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
        public enum eCleanupModeConstants
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
        /// Cleanup status codes for stored procedure ReportManagerErrorCleanup
        /// </summary>
        public enum eCleanupActionCodeConstants
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

        #endregion

        #region "Properties"

        /// <summary>
        /// Full path to the flag file
        /// </summary>
        /// <remarks>Will be in the same folder as the manager executable</remarks>
        public string FlagFilePath => Path.Combine(mMgrFolderPath, FLAG_FILE_NAME);

        #endregion

        #region "Class wide Variables"

        private readonly bool mInitialized;
        private readonly string mMgrConfigDBConnectionString;
        private readonly string mManagerName;

        private readonly int mDebugLevel;
        private readonly string mMgrFolderPath;

        private readonly string mWorkingDirPath;
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrConfigDBConnectionString">Connection string to the manager_control database; if empty, database access is disabled</param>
        /// <param name="managerName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="mgrFolderPath"></param>
        /// <param name="workingDirPath"></param>
        public clsCleanupMgrErrors(string mgrConfigDBConnectionString, string managerName, int debugLevel, string mgrFolderPath, string workingDirPath)
        {
            if (string.IsNullOrEmpty(mgrConfigDBConnectionString))
                throw new Exception("Manager config DB connection string is not defined");

            if (string.IsNullOrEmpty(managerName))
                throw new Exception("Manager name is not defined");

            mMgrConfigDBConnectionString = string.Copy(mgrConfigDBConnectionString);
            mManagerName = string.Copy(managerName);
            mDebugLevel = debugLevel;

            mMgrFolderPath = mgrFolderPath;
            mWorkingDirPath = workingDirPath;

            mInitialized = true;
        }

        /// <summary>
        /// Automatically clean old files from the work directory if eManagerErrorCleanupMode is not eCleanupModeConstants.Disabled
        /// </summary>
        /// <param name="eManagerErrorCleanupMode"></param>
        /// <param name="debugLevel"></param>
        /// <returns></returns>
        public bool AutoCleanupManagerErrors(eCleanupModeConstants eManagerErrorCleanupMode, int debugLevel)
        {
            if (!mInitialized)
                return false;

            if (eManagerErrorCleanupMode == eCleanupModeConstants.Disabled)
                return false;

            LogMessage("Attempting to automatically clean the work directory");

            // Call SP ReportManagerErrorCleanup @ActionCode=1
            ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start);

            // Delete all folders and subfolders in work folder
            var success = CleanWorkDir(mWorkingDirPath, 1);
            string failureMessage;

            if (!success)
            {
                failureMessage = "unable to clear work directory";
            }
            else
            {
                // If successful, then deletes flag files: flagfile.txt and flagFile_Svr.txt
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

            // If successful, call SP with ReportManagerErrorCleanup @ActionCode=2
            // Otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

            if (success)
            {
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success);
            }
            else
            {
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, failureMessage);
            }

            return success;
        }

        /// <summary>
        /// Deletes all files in working directory (using a 3 second holdoff after calling GC.Collect via PRISM.clsProgRunner.GarbageCollectNow)
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool CleanWorkDir()
        {
            return CleanWorkDir(mWorkingDirPath, DEFAULT_HOLDOFF_SECONDS);
        }

        /// <summary>
        /// Deletes all files in working directory
        /// </summary>
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.clsProgRunner.GarbageCollectNow()</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool CleanWorkDir(float holdoffSeconds)
        {
            return CleanWorkDir(mWorkingDirPath, DEFAULT_HOLDOFF_SECONDS);
        }

        /// <summary>
        /// Deletes all files in working directory
        /// </summary>
        /// <param name="workDir">Full path to working directory</param>
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.clsProgRunner.GarbageCollectNow()</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool CleanWorkDir(string workDir, float holdoffSeconds)
        {
            double actualHoldoffSeconds;

            if (Environment.MachineName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) && holdoffSeconds > 1)
                holdoffSeconds = 1;

            try
            {
                actualHoldoffSeconds = holdoffSeconds;
                if (actualHoldoffSeconds < 0.1)
                    actualHoldoffSeconds = 0.1;
                if (actualHoldoffSeconds > 300)
                    actualHoldoffSeconds = 300;
            }
            catch (Exception)
            {
                actualHoldoffSeconds = 10;
            }

            // Try to ensure there are no open objects with file handles
            PRISM.clsProgRunner.GarbageCollectNow();
            clsGlobal.IdleLoop(actualHoldoffSeconds);

            // Delete all of the files and folders in the work directory
            var diWorkFolder = new DirectoryInfo(workDir);
            if (!DeleteFilesWithRetry(diWorkFolder))
            {
                return false;
            }

            return true;
        }

        private bool DeleteFilesWithRetry(DirectoryInfo diWorkFolder)
        {
            const int DELETE_RETRY_COUNT = 3;

            var failedDeleteCount = 0;
            var oFileTools = new PRISM.clsFileTools(mManagerName, mDebugLevel);

            // Delete the files
            try
            {
                foreach (var fiFile in diWorkFolder.GetFiles())
                {

                    if (!oFileTools.DeleteFileWithRetry(fiFile, DELETE_RETRY_COUNT, out var errorMessage))
                    {
                        LogError(errorMessage);
                        failedDeleteCount += 1;
                    }
                }

                // Delete the sub directories
                foreach (var diSubDirectory in diWorkFolder.GetDirectories())
                {
                    if (DeleteFilesWithRetry(diSubDirectory))
                    {
                        // Remove the folder if it is empty
                        diSubDirectory.Refresh();
                        if (diSubDirectory.GetFileSystemInfos().Length != 0)
                            continue;

                        try
                        {
                            diSubDirectory.Delete();
                        }
                        catch (IOException)
                        {
                            // Try re-applying the permissions

                            var folderAcl = new DirectorySecurity();
                            var currentUser = Environment.UserDomainName + @"\" + Environment.UserName;

                            LogWarning("IOException deleting " + diSubDirectory.FullName + "; will try granting modify access to user " + currentUser);
                            folderAcl.AddAccessRule(new FileSystemAccessRule(currentUser, FileSystemRights.Modify,
                                                                             InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));

                            try
                            {
                                // To remove existing permissions, use this: folderAcl.SetAccessRuleProtection(True, False)

                                // Add the new access rule
                                diSubDirectory.SetAccessControl(folderAcl);

                                // Make sure the readonly flag is not set (it's likely not even possible for a folder to have a readonly flag set, but it doesn't hurt to check)
                                diSubDirectory.Refresh();
                                var attributes = diSubDirectory.Attributes;
                                if ((attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                                {
                                    diSubDirectory.Attributes = attributes & ~FileAttributes.ReadOnly;
                                }

                                try
                                {
                                    // Retry the delete
                                    diSubDirectory.Delete();
                                    LogDebug("Updated permissions, then successfully deleted the folder");
                                }
                                catch (Exception ex3)
                                {
                                    var failureMessage = "Error deleting folder " + diSubDirectory.FullName + ": " + ex3.Message;
                                    LogError(failureMessage);
                                    failedDeleteCount += 1;
                                }
                            }
                            catch (Exception ex2)
                            {
                                var failureMessage = "Error updating permissions for folder " + diSubDirectory.FullName + ": " + ex2.Message;
                                LogError(failureMessage);
                                failedDeleteCount += 1;
                            }
                        }
                        catch (Exception ex)
                        {
                            var failureMessage = "Error deleting folder " + diSubDirectory.FullName + ": " + ex.Message;
                            LogError(failureMessage);
                            failedDeleteCount += 1;
                        }
                    }
                    else
                    {
                        var failureMessage = "Error deleting working directory subfolder " + diSubDirectory.FullName;
                        LogError(failureMessage);
                        failedDeleteCount += 1;
                    }
                }
            }
            catch (Exception ex)
            {
                var failureMessage = "Error deleting files/folders in " + diWorkFolder.FullName;
                LogError(failureMessage, ex);
                return false;
            }

            if (failedDeleteCount == 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
        /// </summary>
        /// <remarks></remarks>
        public void CreateErrorDeletingFilesFlagFile()
        {
            try
            {
                var path = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME);
                using (var writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                LogError("Error creating " + ERROR_DELETING_FILES_FILENAME, ex);
            }
        }

        /// <summary>
        /// Creates a dummy file in the application directory to be used for controlling job request bypass
        /// </summary>
        /// <remarks></remarks>
        public void CreateStatusFlagFile()
        {
            try
            {
                var path = FlagFilePath;
                using (var writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                    writer.Flush();
                }
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
        /// <remarks></remarks>
        public bool DeleteDeconServerFlagFile(int DebugLevel)
        {
            var flagFilePath = Path.Combine(mMgrFolderPath, DECON_SERVER_FLAG_FILE_NAME);

            return DeleteFlagFile(flagFilePath, DebugLevel);
        }

        /// <summary>
        /// Deletes the file given by flagFilePath
        /// </summary>
        /// <param name="flagFilePath">Full path to the file to delete</param>
        /// <param name="debugLevel"></param>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        /// <remarks></remarks>
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

                        if (clsAnalysisToolRunnerBase.DeleteFileWithRetries(flagFilePath, debugLevel))
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
        /// <remarks></remarks>
        public bool DeleteStatusFlagFile(int DebugLevel)
        {
            var flagFilePath = FlagFilePath;

            return DeleteFlagFile(flagFilePath, DebugLevel);
        }

        /// <summary>
        /// Determines if error deleting files flag file exists in application directory
        /// </summary>
        /// <returns>TRUE if flag file exists; FALSE otherwise</returns>
        /// <remarks></remarks>
        public bool DetectErrorDeletingFilesFlagFile()
        {
            var TestFile = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME);

            return File.Exists(TestFile);
        }

        /// <summary>
        /// Determines if flag file exists in application directory
        /// </summary>
        /// <returns>TRUE if flag file exists; FALSE otherwise</returns>
        /// <remarks></remarks>
        public bool DetectStatusFlagFile()
        {
            var flagFile = new FileInfo(FlagFilePath);

            return flagFile.Exists;
        }

        /// <summary>
        /// Deletes the error deleting files flag file
        /// </summary>
        /// <remarks></remarks>
        public void DeleteErrorDeletingFilesFlagFile()
        {
            var deletionFlagFile = new FileInfo(Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME));

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

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode)
        {
            ReportManagerErrorCleanup(eMgrCleanupActionCode, string.Empty);
        }

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode, string failureMessage)
        {
            if (string.IsNullOrWhiteSpace(mMgrConfigDBConnectionString))
            {
                if (clsGlobal.OfflineMode)
                    LogDebug("Skipping call to " + SP_NAME_REPORTMGRCLEANUP + " since offline");
                else
                    LogError("Skipping call to " + SP_NAME_REPORTMGRCLEANUP + " since the Manager Control connection string is empty");

                return;
            }

            try
            {
                if (failureMessage == null)
                    failureMessage = string.Empty;

                var myConnection = new SqlConnection(mMgrConfigDBConnectionString);
                myConnection.Open();

                var cmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REPORTMGRCLEANUP,
                    Connection = myConnection
                };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@ManagerName", SqlDbType.VarChar, 128)).Value = mManagerName;
                cmd.Parameters.Add(new SqlParameter("@State", SqlDbType.Int)).Value = eMgrCleanupActionCode;
                cmd.Parameters.Add(new SqlParameter("@FailureMsg", SqlDbType.VarChar, 512)).Value = failureMessage;
                cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                // Execute the SP
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string errorMessage;
                if (mMgrConfigDBConnectionString == null)
                {
                    errorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup; empty connection string";
                }
                else
                {
                    errorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                }

                LogError(errorMessage, ex);
            }
        }
    }
}
