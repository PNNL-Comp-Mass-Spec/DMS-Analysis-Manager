using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Security.AccessControl;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    public class clsCleanupMgrErrors
    {
        #region "Constants"

        private const string SP_NAME_REPORTMGRCLEANUP = "ReportManagerErrorCleanup";

        private const int DEFAULT_HOLDOFF_SECONDS = 3;
        public const string FLAG_FILE_NAME = "flagFile.txt";
        public const string DECON_SERVER_FLAG_FILE_NAME = "flagFile_Svr.txt";

        public const string ERROR_DELETING_FILES_FILENAME = "Error_Deleting_Files_Please_Delete_Me.txt";
        public enum eCleanupModeConstants
        {
            Disabled = 0,
            CleanupOnce = 1,
            CleanupAlways = 2
        }

        public enum eCleanupActionCodeConstants
        {
            Start = 1,
            Success = 2,
            Fail = 3
        }

        #endregion

        #region "Properties"

        public string FlagFilePath
        {
            get { return Path.Combine(mMgrFolderPath, FLAG_FILE_NAME); }
        }

        #endregion
        #region "Class wide Variables"

        private readonly bool mInitialized = false;
        private readonly string mMgrConfigDBConnectionString = string.Empty;
        private readonly string mManagerName = string.Empty;

        private readonly int mDebugLevel;
        private readonly string mMgrFolderPath = string.Empty;

        private readonly string mWorkingDirPath = string.Empty;
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrConfigDBConnectionString"></param>
        /// <param name="managerName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="mgrFolderPath"></param>
        /// <param name="workingDirPath"></param>
        public clsCleanupMgrErrors(string mgrConfigDBConnectionString, string managerName, int debugLevel, string mgrFolderPath, string workingDirPath)
        {
            if (string.IsNullOrEmpty(mgrConfigDBConnectionString))
            {
                throw new Exception("Manager config DB connection string is not defined");
            }
            else if (string.IsNullOrEmpty(managerName))
            {
                throw new Exception("Manager name is not defined");
            }
            else
            {
                mMgrConfigDBConnectionString = string.Copy(mgrConfigDBConnectionString);
                mManagerName = string.Copy(managerName);
                mDebugLevel = debugLevel;

                mMgrFolderPath = mgrFolderPath;
                mWorkingDirPath = workingDirPath;

                mInitialized = true;
            }
        }

        public bool AutoCleanupManagerErrors(eCleanupModeConstants eManagerErrorCleanupMode, int debugLevel)
        {
            bool blnSuccess = false;
            string strFailureMessage = string.Empty;

            if (!mInitialized)
                return false;

            if (eManagerErrorCleanupMode != eCleanupModeConstants.Disabled)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Attempting to automatically clean the work directory");

                // Call SP ReportManagerErrorCleanup @ActionCode=1
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start);

                // Delete all folders and subfolders in work folder
                blnSuccess = CleanWorkDir(mWorkingDirPath, 1);

                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(strFailureMessage))
                    {
                        strFailureMessage = "unable to clear work directory";
                    }
                }
                else
                {
                    // If successful, then deletes flag files: flagfile.txt and flagFile_Svr.txt
                    blnSuccess = DeleteDeconServerFlagFile(debugLevel);

                    if (!blnSuccess)
                    {
                        strFailureMessage = "error deleting " + DECON_SERVER_FLAG_FILE_NAME;
                    }
                    else
                    {
                        blnSuccess = DeleteStatusFlagFile(debugLevel);
                        if (!blnSuccess)
                        {
                            strFailureMessage = "error deleting " + FLAG_FILE_NAME;
                        }
                    }
                }

                // If successful, then call SP with ReportManagerErrorCleanup @ActionCode=2
                //    otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

                if (blnSuccess)
                {
                    ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success);
                }
                else
                {
                    ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, strFailureMessage);
                }
            }

            return blnSuccess;
        }

        /// <summary>
        /// Deletes all files in working directory (using a 3 second holdoff after calling GC.Collect via PRISM.Processes.clsProgRunner.GarbageCollectNow)
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
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.Processes.clsProgRunner.GarbageCollectNow()</param>
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
        /// <param name="holdoffSeconds">Number of seconds to wait after calling PRISM.Processes.clsProgRunner.GarbageCollectNow()</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool CleanWorkDir(string workDir, float holdoffSeconds)
        {
            int holdoffMilliseconds = 0;

            if (Environment.MachineName.ToLower().StartsWith("monroe") && holdoffSeconds > 1)
                holdoffSeconds = 1;

            try
            {
                holdoffMilliseconds = Convert.ToInt32(holdoffSeconds * 1000);
                if (holdoffMilliseconds < 100)
                    holdoffMilliseconds = 100;
                if (holdoffMilliseconds > 300000)
                    holdoffMilliseconds = 300000;
            }
            catch (Exception ex)
            {
                holdoffMilliseconds = 10000;
            }

            //Try to ensure there are no open objects with file handles
            PRISM.Processes.clsProgRunner.GarbageCollectNow();
            System.Threading.Thread.Sleep(holdoffMilliseconds);

            // Delete all of the files and folders in the work directory
            var diWorkFolder = new DirectoryInfo(workDir);
            if (!DeleteFilesWithRetry(diWorkFolder))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private bool DeleteFilesWithRetry(DirectoryInfo diWorkFolder)
        {
            const int DELETE_RETRY_COUNT = 3;

            var failedDeleteCount = 0;
            var oFileTools = new PRISM.Files.clsFileTools(mManagerName, mDebugLevel);

            // Delete the files
            try
            {
                foreach (var fiFile in diWorkFolder.GetFiles())
                {
                    string errorMessage = string.Empty;

                    if (!oFileTools.DeleteFileWithRetry(fiFile, DELETE_RETRY_COUNT, out errorMessage))
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
                        Console.WriteLine(errorMessage);
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
                        if (diSubDirectory.GetFileSystemInfos().Length == 0)
                        {
                            try
                            {
                                diSubDirectory.Delete();
                            }
                            catch (IOException ex)
                            {
                                // Try re-applying the permissions

                                DirectorySecurity folderAcl = new DirectorySecurity();
                                var currentUser = Environment.UserDomainName + "\\" + Environment.UserName;

                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "IOException deleting " + diSubDirectory.FullName + "; will try granting modify access to user " + currentUser);
                                folderAcl.AddAccessRule(new FileSystemAccessRule(currentUser, FileSystemRights.Modify, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));

                                try
                                {
                                    // To remove existing permissions, use this: folderAcl.SetAccessRuleProtection(True, False)

                                    // Add the new access rule
                                    diSubDirectory.SetAccessControl(folderAcl);

                                    // Make sure the readonly flag is not set (it's likely not even possible for a folder to have a readonly flag set, but it doesn't hurt to check)
                                    diSubDirectory.Refresh();
                                    var attributes = diSubDirectory.Attributes;
                                    if (((attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly))
                                    {
                                        diSubDirectory.Attributes = attributes & (~FileAttributes.ReadOnly);
                                    }

                                    try
                                    {
                                        // Retry the delete
                                        diSubDirectory.Delete();
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updated permissions, then successfully deleted the folder");
                                    }
                                    catch (Exception ex3)
                                    {
                                        string strFailureMessage = "Error deleting folder " + diSubDirectory.FullName + ": " + ex3.Message;
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage);
                                        Console.WriteLine(strFailureMessage);
                                        failedDeleteCount += 1;
                                    }
                                }
                                catch (Exception ex2)
                                {
                                    string strFailureMessage = "Error updating permissions for folder " + diSubDirectory.FullName + ": " + ex2.Message;
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage);
                                    Console.WriteLine(strFailureMessage);
                                    failedDeleteCount += 1;
                                }
                            }
                            catch (Exception ex)
                            {
                                string strFailureMessage = "Error deleting folder " + diSubDirectory.FullName + ": " + ex.Message;
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage);
                                Console.WriteLine(strFailureMessage);
                                failedDeleteCount += 1;
                            }
                        }
                    }
                    else
                    {
                        var strFailureMessage = "Error deleting working directory subfolder " + diSubDirectory.FullName;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage);
                        Console.WriteLine(strFailureMessage);
                        failedDeleteCount += 1;
                    }
                }
            }
            catch (Exception ex)
            {
                string strFailureMessage = "Error deleting files/folders in " + diWorkFolder.FullName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage, ex);
                Console.WriteLine(strFailureMessage + ": " + ex.Message);
                return false;
            }

            if (failedDeleteCount == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
        /// </summary>
        /// <remarks></remarks>
        public void CreateErrorDeletingFilesFlagFile()
        {
            try
            {
                string strPath = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME);
                using (var writer = new StreamWriter(new FileStream(strPath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(System.DateTime.Now.ToString());
                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating " + ERROR_DELETING_FILES_FILENAME + ": " + ex.Message);
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
                string strPath = FlagFilePath;
                using (var writer = new StreamWriter(new FileStream(strPath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(System.DateTime.Now.ToString());
                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating " + FLAG_FILE_NAME + ": " + ex.Message);
            }
        }

        /// <summary>
        /// Deletes the Decon2LS OA Server flag file
        /// </summary>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        /// <remarks></remarks>
        public bool DeleteDeconServerFlagFile(int DebugLevel)
        {
            //Deletes the job request control flag file
            string strFlagFilePath = Path.Combine(mMgrFolderPath, DECON_SERVER_FLAG_FILE_NAME);

            return DeleteFlagFile(strFlagFilePath, DebugLevel);
        }

        /// <summary>
        /// Deletes the file given by strFlagFilePath
        /// </summary>
        /// <param name="strFlagFilePath">Full path to the file to delete</param>
        /// <param name="intDebugLevel"></param>
        /// <returns>True if no flag file exists or if file was successfully deleted</returns>
        /// <remarks></remarks>
        private bool DeleteFlagFile(string strFlagFilePath, int intDebugLevel)
        {
            try
            {
                if (File.Exists(strFlagFilePath))
                {
                    try
                    {
                        // DeleteFileWithRetries will throw an exception if it cannot delete the file
                        // Thus, need to wrap it with an Exception handler

                        if (clsAnalysisToolRunnerBase.DeleteFileWithRetries(strFlagFilePath, intDebugLevel))
                        {
                            return true;
                        }
                        else
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting file " + strFlagFilePath);
                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteFlagFile", ex);
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteFlagFile", ex);
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
            //Deletes the job request control flag file
            string strFlagFilePath = FlagFilePath;

            return DeleteFlagFile(strFlagFilePath, DebugLevel);
        }

        /// <summary>
        /// Determines if error deleting files flag file exists in application directory
        /// </summary>
        /// <returns>TRUE if flag file exists; FALSE otherwise</returns>
        /// <remarks></remarks>
        public bool DetectErrorDeletingFilesFlagFile()
        {
            //Returns True if job request control flag file exists
            string TestFile = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME);

            return File.Exists(TestFile);
        }

        /// <summary>
        /// Determines if flag file exists in application directory
        /// </summary>
        /// <returns>TRUE if flag file exists; FALSE otherwise</returns>
        /// <remarks></remarks>
        public bool DetectStatusFlagFile()
        {
            //Returns True if job request control flag file exists
            string TestFile = FlagFilePath;

            return File.Exists(TestFile);
        }

        /// <summary>
        /// Deletes the error deleting files flag file
        /// </summary>
        /// <remarks></remarks>
        public void DeleteErrorDeletingFilesFlagFile()
        {
            //Deletes the job request control flag file
            string TestFile = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME);

            try
            {
                if (File.Exists(TestFile))
                {
                    File.Delete(TestFile);
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteStatusFlagFile", ex);
            }
        }

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode)
        {
            ReportManagerErrorCleanup(eMgrCleanupActionCode, string.Empty);
        }

        private void ReportManagerErrorCleanup(eCleanupActionCodeConstants eMgrCleanupActionCode, string strFailureMessage)
        {
            try
            {
                if (strFailureMessage == null)
                    strFailureMessage = string.Empty;

                var myConnection = new SqlConnection(mMgrConfigDBConnectionString);
                myConnection.Open();

                var myCmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REPORTMGRCLEANUP,
                    Connection = myConnection
                };

                myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                myCmd.Parameters.Add(new SqlParameter("@ManagerName", SqlDbType.VarChar, 128)).Value = mManagerName;
                myCmd.Parameters.Add(new SqlParameter("@State", SqlDbType.Int)).Value = eMgrCleanupActionCode;
                myCmd.Parameters.Add(new SqlParameter("@FailureMsg", SqlDbType.VarChar, 512)).Value = strFailureMessage;
                myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                //Execute the SP
                myCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string strErrorMessage = null;
                if (mMgrConfigDBConnectionString == null)
                {
                    strErrorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup; empty connection string";
                }
                else
                {
                    strErrorMessage = "Exception calling " + SP_NAME_REPORTMGRCLEANUP + " in ReportManagerErrorCleanup with connection string " + mMgrConfigDBConnectionString;
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage + ex.Message);
            }
        }
    }
}
