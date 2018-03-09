using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using PRISM.Logging;
using PRISM;
using PRISMWin;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/20/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Globally useful methods
    /// </summary>
    public static class clsGlobal
    {

        #region "Constants"

        /// <summary>
        /// Job parameters file prefix
        /// </summary>
        public const string JOB_PARAMETERS_FILE_PREFIX = "JobParameters_";

        /// <summary>
        /// Step tool param file storage path file prefix
        /// </summary>
        public const string STEPTOOL_PARAMFILESTORAGEPATH_PREFIX = "StepTool_ParamFileStoragePath_";

        /// <summary>
        /// Server cache hashcheck file suffix
        /// </summary>
        public const string SERVER_CACHE_HASHCHECK_FILE_SUFFIX = ".hashcheck";

        /// <summary>
        /// Lock file suffix (.lock)
        /// </summary>
        public const string LOCK_FILE_EXTENSION = DMSUpdateManager.RemoteUpdateUtility.LOCK_FILE_EXTENSION;

        #endregion

        #region "Enums"

        /// <summary>
        /// Analysis resource true/false options
        /// </summary>
        public enum eAnalysisResourceOptions
        {
            /// <summary>
            /// If true, a FASTA file or protein collection is required
            /// </summary>
            OrgDbRequired = 0,

            /// <summary>
            /// If true, MyEMSL search is disabled
            /// </summary>
            MyEMSLSearchDisabled = 1
        }

        #endregion

        #region Properties

        /// <summary>
        /// When true, we are running on Linux and thus should not access any Windows features
        /// </summary>
        /// <remarks>Call EnableOfflineMode to set this to true</remarks>
        public static bool LinuxOS { get; private set; }

        /// <summary>
        /// When true, does not contact any databases or remote shares
        /// </summary>
        public static bool OfflineMode { get; private set; }

        /// <summary>
        /// System process info
        /// </summary>
        public static SystemProcessInfo ProcessInfo
        {
            get
            {
                if (mSystemProcessInfo == null)
                {
                    mSystemProcessInfo = new SystemProcessInfo();
                    RegisterEvents(mSystemProcessInfo);

                }
                return mSystemProcessInfo;
            }
        }

        #endregion

        #region "Module variables"

        private static string mAppFolderPath;

        private static SystemProcessInfo mSystemProcessInfo;

        #endregion

        #region "Methods"

        /// <summary>
        /// Appends a string to a job comment string
        /// </summary>
        /// <param name="baseComment">Initial comment</param>
        /// <param name="addnlComment">Comment to be appended</param>
        /// <returns>String containing both comments</returns>
        /// <remarks></remarks>
        public static string AppendToComment(string baseComment, string addnlComment)
        {

            if (string.IsNullOrWhiteSpace(baseComment))
            {
                return addnlComment.Trim();
            }

            if (string.IsNullOrWhiteSpace(addnlComment) || baseComment.Contains(addnlComment))
            {
                // Either addnlComment is empty (unlikely) or addnlComment is a duplicate comment
                // Return the base comment
                return baseComment.Trim();
            }

            // Append a semicolon to baseComment, but only if it doesn't already end in a semicolon
            if (baseComment.TrimEnd().EndsWith(";"))
            {
                return baseComment.TrimEnd() + addnlComment.Trim();
            }

            return baseComment.Trim() + "; " + addnlComment.Trim();
        }

        /// <summary>
        /// Convert Bytes to Gigabytes
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static double BytesToGB(long bytes)
        {
            return bytes / 1024.0 / 1024.0 / 1024.0;
        }

        /// <summary>
        /// Convert Bytes to Megabytes
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static double BytesToMB(long bytes)
        {
            return bytes / 1024.0 / 1024.0;
        }

        /// <summary>
        /// Examines count to determine which string to return
        /// </summary>
        /// <param name="count"></param>
        /// <param name="textIfOneItem"></param>
        /// <param name="textIfZeroOrMultiple"></param>
        /// <returns>Returns textIfOneItem if count is 1; otherwise, returns textIfZeroOrMultiple</returns>
        /// <remarks></remarks>
        public static string CheckPlural(int count, string textIfOneItem, string textIfZeroOrMultiple)
        {
            if (count == 1)
            {
                return textIfOneItem;
            }

            return textIfZeroOrMultiple;
        }

        /// <summary>
        /// Collapse an array of items to a tab-delimited list
        /// </summary>
        /// <param name="items"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string CollapseLine(string[] items)
        {
            if (items == null || items.Length == 0)
            {
                return string.Empty;
            }

            return CollapseList(items.ToList());
        }

        /// <summary>
        /// Collapse a list of items to a tab-delimited list
        /// </summary>
        /// <param name="lstFields"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string CollapseList(List<string> lstFields)
        {

            return FlattenList(lstFields, "\t");

        }

        /// <summary>
        /// Assure that the directory exists; attempt to create it if missing
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <returns>DirectoryInfo object</returns>
        public static DirectoryInfo CreateDirectoryIfMissing(string directoryPath)
        {
            var targetDirectory = new DirectoryInfo(directoryPath);
            if (!targetDirectory.Exists)
            {
                // Note that .NET will automatically create any missing parent directories
                targetDirectory.Create();
            }

            return targetDirectory;
        }

        /// <summary>
        /// Decrypts password
        /// </summary>
        /// <param name="encodedPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        public static string DecodePassword(string encodedPwd)
        {
            return Pacifica.Core.Utilities.DecodePassword(encodedPwd);
        }

        /// <summary>
        /// Delete the lock file for the corresponding data file
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <remarks></remarks>
        public static void DeleteLockFile(string dataFilePath)
        {
            try
            {
                var lockFilePath = dataFilePath + LOCK_FILE_EXTENSION;

                var fiLockFile = new FileInfo(lockFilePath);
                if (fiLockFile.Exists)
                {
                    fiLockFile.Delete();
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <param name="runningLinux">Set to True if running Linux</param>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        public static void EnableOfflineMode(bool runningLinux = true)
        {
            OfflineMode = true;
            LinuxOS = runningLinux;

            LogTools.OfflineMode = true;

            if (runningLinux)
                Console.WriteLine("Offline mode enabled globally (running Linux)");
            else
                Console.WriteLine("Offline mode enabled globally");
        }

        /// <summary>
        /// Flatten a list of items into a single string, with items separated by chDelimiter
        /// </summary>
        /// <param name="lstItems"></param>
        /// <param name="delimiter"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string FlattenList(List<string> lstItems, string delimiter)
        {
            if (lstItems == null || lstItems.Count == 0)
            {
                return string.Empty;
            }

            return string.Join(delimiter, lstItems);
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppFolderPath()
        {
            if (mAppFolderPath != null)
                return mAppFolderPath;

            mAppFolderPath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppFolderPath();

            return mAppFolderPath;
        }

        /// <summary>
        /// Returns the version string of the entry assembly (typically the Program .exe file)
        /// </summary>
        /// <returns>Assembly version, e.g. 1.0.4482.23831</returns>
        public static string GetAssemblyVersion()
        {
            var entryAssembly = Assembly.GetEntryAssembly();
            if (entryAssembly == null)
                return string.Empty;

            return GetAssemblyVersion(entryAssembly);

        }

        /// <summary>
        /// Returns the version string of the specified assembly
        /// </summary>
        /// <returns>Assembly version, e.g. 1.0.4482.23831</returns>
        public static string GetAssemblyVersion(Assembly objAssembly)
        {
            // objAssembly.FullName typically returns something like this:
            // AnalysisManagerProg, Version=2.3.4479.23831, Culture=neutral, PublicKeyToken=null
            //
            // the goal is to extract out the text after Version= but before the next comma

            var reGetVersion = new Regex("version=([0-9.]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var version = objAssembly.FullName;

            var reMatch = reGetVersion.Match(version);

            if (reMatch.Success)
            {
                version = reMatch.Groups[1].Value;
            }

            return version;

        }

        /// <summary>
        /// Runs the specified Sql query
        /// </summary>
        /// <param name="sqlStr">Sql query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Uses a timeout of 30 seconds</remarks>
        public static bool GetDataTableByQuery(string sqlStr, string connectionString, string callingFunction, short retryCount, out DataTable dtResults)
        {

            const int timeoutSeconds = 30;

            return GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the specified Sql query
        /// </summary>
        /// <param name="sqlStr">Sql query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByQuery(
            string sqlStr, string connectionString, string callingFunction,
            short retryCount, out DataTable dtResults, int timeoutSeconds)
        {

            var cmd = new SqlCommand(sqlStr)
            {
                CommandType = CommandType.Text
            };

            return GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the stored procedure or database query defined by "cmd"
        /// </summary>
        /// <param name="cmd">SqlCommand var (query or stored procedure)</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="dtResults">Datatable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByCmd(
            SqlCommand cmd,
            string connectionString,
            string callingFunction,
            short retryCount,
            out DataTable dtResults,
            int timeoutSeconds)
        {

            if (cmd == null)
                throw new ArgumentException("command is undefined", nameof(cmd));

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("ConnectionString cannot be empty", nameof(connectionString));
            }

            if (string.IsNullOrEmpty(callingFunction))
                callingFunction = "UnknownCaller";
            if (retryCount < 1)
                retryCount = 1;
            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            // When data retrieval fails, delay for 5 seconds on the first try
            // Double the delay time for each subsequent attempt, up to a maximum of 90 seconds between attempts
            var retryDelaySeconds = 5;

            while (retryCount > 0)
            {
                try
                {
                    using (var cn = new SqlConnection(connectionString))
                    {

                        cmd.Connection = cn;
                        cmd.CommandTimeout = timeoutSeconds;

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            using (var ds = new DataSet())
                            {
                                da.Fill(ds);
                                dtResults = ds.Tables[0];
                            }
                        }
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    string msg;

                    retryCount -= 1;
                    if (cmd.CommandType == CommandType.StoredProcedure)
                    {
                        msg = callingFunction + "; Exception running stored procedure " + cmd.CommandText;
                    }
                    else if (cmd.CommandType == CommandType.TableDirect)
                    {
                        msg = callingFunction + "; Exception querying table " + cmd.CommandText;
                    }
                    else
                    {
                        msg = callingFunction + "; Exception querying database";
                    }

                    msg += ": " + ex.Message + "; ConnectionString: " + connectionString;
                    msg += ", RetryCount = " + retryCount;

                    if (cmd.CommandType == CommandType.Text)
                    {
                        msg += ", Query = " + cmd.CommandText;
                    }

                    LogTools.LogError(msg);

                    if (retryCount <= 0)
                        break;

                    IdleLoop(retryDelaySeconds);

                    retryDelaySeconds *= 2;
                    if (retryDelaySeconds > 90)
                    {
                        retryDelaySeconds = 90;
                    }
                }
            }

            dtResults = null;
            return false;

        }

        /// <summary>
        /// Determine the version of .NET that is running
        /// </summary>
        /// <returns></returns>
        public static string GetDotNetVersion()
        {
            try
            {
                var versionChecker = new clsDotNETVersionChecker();
                return versionChecker.GetLatestDotNETVersion();
            }
            catch (Exception ex)
            {
                var msg = "Unknown .NET version, " + ex.Message;
                LogTools.LogError(msg, ex);
                return msg;
            }
        }

        /// <summary>
        /// Run a query against a SQL Server database
        /// </summary>
        /// <param name="sqlQuery">Query to run</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="lstResults">Results, as a list of columns (first row only if multiple rows)</param>
        /// <param name="callingFunction">Name of the calling function (for logging purposes)</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Null values are converted to empty strings
        /// Numbers are converted to their string equivalent
        /// Use the GetDataTable functions in this class if you need to retain numeric values or null values
        /// </remarks>
        public static bool GetQueryResultsTopRow(
            string sqlQuery,
            string connectionString,
            out List<string> lstResults,
            string callingFunction,
            short retryCount = 3,
            int timeoutSeconds = 5)
        {


            var success = GetQueryResults(sqlQuery, connectionString, out var lstResultTable, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn: 1);

            if (success)
            {
                lstResults = lstResultTable.FirstOrDefault() ?? new List<string>();
                return true;
            }

            lstResults = new List<string>();
            return false;
        }

        /// <summary>
        /// Run a query against a SQL Server database, return the results as a list of strings
        /// </summary>
        /// <param name="sqlQuery">Query to run</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="lstResults">Results (list of list of strings)</param>
        /// <param name="callingFunction">Name of the calling function (for logging purposes)</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        ///<param name="maxRowsToReturn">Maximum rows to return; 0 to return all rows</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Null values are converted to empty strings
        /// Numbers are converted to their string equivalent
        /// Use the GetDataTable functions in this class if you need to retain numeric values or null values
        /// </remarks>
        public static bool GetQueryResults(
            string sqlQuery,
            string connectionString,
            out List<List<string>> lstResults,
            string callingFunction,
            short retryCount = 3,
            int timeoutSeconds = 30,
            int maxRowsToReturn = 0)
        {

            if (OfflineMode)
            {
                LogTools.LogError(string.Format("Offline mode enabled; {0} cannot execute query {1}", callingFunction, sqlQuery));
                lstResults = new List<List<string>>();
                return false;
            }

            if (retryCount < 1)
                retryCount = 1;
            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var dbTools = new clsDBTools(connectionString);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResults(sqlQuery, out lstResults, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn);

            return success;

        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex)
        {
            return GetExceptionStackTrace(ex, false);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="multiLineOutput">When true, format the stack trace using newline characters instead of -:-</param>
        /// <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex, bool multiLineOutput)
        {
            if (multiLineOutput)
            {
                return clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return clsStackTraceFormatter.GetExceptionStackTrace(ex);

        }

        /// <summary>
        /// Parse settingText to extract the key name and value (separated by an equals sign)
        /// </summary>
        /// <param name="settingText"></param>
        /// <returns>Key/Value pair</returns>
        /// <remarks>If the line starts with # it is treated as a comment line and an empty key/value pair will be returned</remarks>
        public static KeyValuePair<string, string> GetKeyValueSetting(string settingText)
        {

            var emptyKvPair = new KeyValuePair<string, string>(string.Empty, string.Empty);

            if (string.IsNullOrWhiteSpace(settingText))
                return emptyKvPair;

            settingText = settingText.Trim();

            if (settingText.StartsWith("#") || !settingText.Contains('='))
                return emptyKvPair;

            var charIndex = settingText.IndexOf("=", StringComparison.Ordinal);

            if (charIndex <= 0)
                return emptyKvPair;

            var key = settingText.Substring(0, charIndex).Trim();
            string value;

            if (charIndex < settingText.Length - 1)
            {
                value = settingText.Substring(charIndex + 1).Trim();
            }
            else
            {
                value = string.Empty;
            }

            return new KeyValuePair<string, string>(key, value);
        }

        /// <summary>
        /// Sleep for the specified seconds
        /// </summary>
        /// <param name="waitTimeSeconds"></param>
        public static void IdleLoop(double waitTimeSeconds)
        {
            var endTime = DateTime.UtcNow.AddSeconds(waitTimeSeconds);
            while (endTime.Subtract(DateTime.UtcNow).TotalMilliseconds > 10)
            {
                var remainingSeconds = endTime.Subtract(DateTime.UtcNow).TotalSeconds;
                if (remainingSeconds > 10)
                {
                    clsProgRunner.SleepMilliseconds(10000);
                }
                else
                {
                    var sleepTimeMsec = (int)Math.Ceiling(remainingSeconds * 1000);
                    clsProgRunner.SleepMilliseconds(sleepTimeMsec);
                }
            }
        }

        /// <summary>
        /// Compare two strings (not case sensitive)
        /// </summary>
        /// <param name="text1"></param>
        /// <param name="text2"></param>
        /// <returns>True if they match; false if not</returns>
        /// <remarks>A null string is considered equivalent to an empty string.  Thus, two null strings are considered equal</remarks>
        public static bool IsMatch(string text1, string text2)
        {
            return IsMatch(text1, text2, true);
        }

        /// <summary>
        /// Compare two strings (not case sensitive)
        /// </summary>
        /// <param name="text1"></param>
        /// <param name="text2"></param>
        /// <param name="treatNullAsEmptyString">When true, a null string is considered equivalent to an empty string</param>
        /// <returns>True if they match; false if not</returns>
        /// <remarks>Two null strings are considered equal, even if treatNullAsEmptyString is false</remarks>
        public static bool IsMatch(string text1, string text2, bool treatNullAsEmptyString)
        {

            if (treatNullAsEmptyString && string.IsNullOrWhiteSpace(text1) && string.IsNullOrWhiteSpace(text2))
            {
                return true;
            }

            return string.Compare(text1, text2, StringComparison.OrdinalIgnoreCase) == 0;
        }

        /// <summary>
        /// Compare the original contents of a lock file with new contents
        /// </summary>
        /// <param name="lockFilePath">Lock file path (could be Windows or Linux-based; only used for error messages)</param>
        /// <param name="lockFileContents">Original lock file contents</param>
        /// <param name="lockFileContentsNew">Current lock file contents</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if the contents match, otherwise false</returns>
        public static bool LockFilesMatch(
            string lockFilePath,
            IReadOnlyList<string> lockFileContents,
            IReadOnlyList<string> lockFileContentsNew,
            out string errorMessage)
        {
            return DMSUpdateManager.RemoteUpdateUtility.LockFilesMatch(lockFilePath, lockFileContents, lockFileContentsNew, out errorMessage);
        }

        /// <summary>
        /// Parses the headers in headerLine to look for the names specified in headerNames
        /// </summary>
        /// <param name="headerLine"></param>
        /// <param name="headerNames"></param>
        /// <param name="isCaseSensitive"></param>
        /// <returns>Dictionary with the header names and 0-based column index</returns>
        /// <remarks>Header names not found in headerLine will have an index of -1</remarks>
        public static Dictionary<string, int> ParseHeaderLine(string headerLine, List<string> headerNames, bool isCaseSensitive = false)
        {
            var dctHeaderMapping = new Dictionary<string, int>();

            var lstColumns = headerLine.Split('\t').ToList();

            foreach (var headerName in headerNames)
            {
                var colIndex = -1;

                if (isCaseSensitive)
                {
                    colIndex = lstColumns.IndexOf(headerName);
                }
                else
                {
                    for (var i = 0; i <= lstColumns.Count - 1; i++)
                    {
                        if (IsMatch(lstColumns[i], headerName))
                        {
                            colIndex = i;
                            break;
                        }
                    }
                }

                dctHeaderMapping.Add(headerName, colIndex);
            }

            return dctHeaderMapping;

        }

        /// <summary>
        /// Examines filePath to look for spaces
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns>filePath as-is if no spaces, otherwise filePath surrounded by double quotes </returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string filePath)
        {
            return clsPathUtils.PossiblyQuotePath(filePath);
        }

        /// <summary>
        /// Tries to retrieve the string value at index colIndex in dataColumns()
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="value"></param>
        /// <returns>True if success; false if colIndex is less than 0 or colIndex is out of range for dataColumns()</returns>
        /// <remarks></remarks>
        public static bool TryGetValue(string[] dataColumns, int colIndex, out string value)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                value = dataColumns[colIndex];
                if (string.IsNullOrEmpty(value))
                    value = string.Empty;
                return true;
            }

            value = string.Empty;
            return false;
        }


        /// <summary>
        /// Tries to convert the text at index colIndex of dataColumns to an integer
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="value"></param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
        /// <remarks></remarks>
        public static bool TryGetValueInt(string[] dataColumns, int colIndex, out int value)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                if (int.TryParse(dataColumns[colIndex], out value))
                {
                    return true;
                }
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Tries to convert the text at index colIndex of dataColumns to a float
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="value"></param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to a float</returns>
        /// <remarks></remarks>
        public static bool TryGetValueFloat(string[] dataColumns, int colIndex, out float value)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                if (float.TryParse(dataColumns[colIndex], out value))
                {
                    return true;
                }
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Converts a string value to a boolean equivalent
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>Returns false if an exception</remarks>
        public static bool CBoolSafe(string value)
        {

            if (bool.TryParse(value, out var boolValue))
                return boolValue;

            return false;

        }

        /// <summary>
        /// Converts a string value to a boolean equivalent
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue">Boolean value to return if value is empty or an exception occurs</param>
        /// <returns></returns>
        /// <remarks>Returns false if an exception</remarks>
        public static bool CBoolSafe(string value, bool defaultValue)
        {

            if (bool.TryParse(value, out var boolValue))
                return boolValue;

            return defaultValue;

        }

        /// <summary>
        /// Converts value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue">Integer to return if value is not numeric</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static int CIntSafe(string value, int defaultValue)
        {

            if (int.TryParse(value, out var intValue))
                return intValue;

            return defaultValue;

        }

        /// <summary>
        /// Converts value to a single (aka float)
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue">Float to return if value is not numeric</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static float CSngSafe(string value, float defaultValue)
        {

            if (float.TryParse(value, out var sngValue))
                return sngValue;

            return defaultValue;

        }

        /// <summary>
        /// Copies file SourceFilePath to folder TargetFolder, renaming it to TargetFileName.
        /// However, if file TargetFileName already exists, that file will first be backed up
        /// Furthermore, up to VersionCountToKeep old versions of the file will be kept
        /// </summary>
        /// <param name="SourceFilePath"></param>
        /// <param name="TargetFolder"></param>
        /// <param name="TargetFileName"></param>
        /// <param name="VersionCountToKeep">Maximum backup copies of the file to keep; must be 9 or less</param>
        /// <returns>True if Success, false if failure </returns>
        /// <remarks></remarks>
        public static bool CopyAndRenameFileWithBackup(string SourceFilePath, string TargetFolder, string TargetFileName, int VersionCountToKeep)
        {

            try
            {
                var ioSrcFile = new FileInfo(SourceFilePath);
                if (!ioSrcFile.Exists)
                {
                    // Source file not found
                    return false;
                }

                var baseName = Path.GetFileNameWithoutExtension(TargetFileName);
                if (baseName == null)
                {
                    // Cannot continue without a base filename
                    return false;
                }

                var extension = Path.GetExtension(TargetFileName);
                if (string.IsNullOrEmpty(extension))
                {
                    extension = ".bak";
                }

                if (VersionCountToKeep > 9)
                    VersionCountToKeep = 9;
                if (VersionCountToKeep < 0)
                    VersionCountToKeep = 0;

                // Backup any existing copies of bargetFilePath
                for (var revision = VersionCountToKeep - 1; revision >= 0; revision += -1)
                {
                    try
                    {
                        var baseNameCurrent = string.Copy(baseName);
                        if (revision > 0)
                        {
                            baseNameCurrent += "_" + revision;
                        }
                        baseNameCurrent += extension;

                        var ioFileToRename = new FileInfo(Path.Combine(TargetFolder, baseNameCurrent));
                        var newFilePath = Path.Combine(TargetFolder, baseName + "_" + (revision + 1) + extension);

                        // Confirm that newFilePath doesn't exist; delete it if it does
                        if (File.Exists(newFilePath))
                        {
                            File.Delete(newFilePath);
                        }

                        // Rename the current file to newFilePath
                        if (ioFileToRename.Exists)
                        {
                            ioFileToRename.MoveTo(newFilePath);
                        }

                    }
                    catch (Exception)
                    {
                        // Ignore errors here; we'll continue on with the next file
                    }

                }

                var finalFilePath = Path.Combine(TargetFolder, TargetFileName);

                // Now copy the file from SourceFilePath to newFilePath
                ioSrcFile.CopyTo(finalFilePath, true);

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return true;

        }

        /// <summary>
        /// Converts an database field value to a string, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string DbCStr(object dbValue)
        {
            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return string.Empty;
            }

            return Convert.ToString(dbValue);
        }

        /// <summary>
        /// Converts an database field value to a single, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static float DbCSng(object dbValue)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return (float)0.0;
            }

            return Convert.ToSingle(dbValue);

        }

        /// <summary>
        /// Converts an database field value to a double, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static double DbCDbl(object dbValue)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return 0.0;
            }

            return Convert.ToDouble(dbValue);

        }

        /// <summary>
        /// Converts an database field value to an integer (int32), checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static int DbCInt(object dbValue)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToInt32(dbValue);

        }

        /// <summary>
        /// Converts an database field value to a long integer (int64), checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static long DbCLng(object dbValue)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToInt64(dbValue);

        }

        /// <summary>
        /// Converts an database field value to a decimal, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        [Obsolete("Decimal data types should be avoided")]
        public static decimal DbCDec(object dbValue)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(dbValue, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToDecimal(dbValue);

        }

        /// <summary>
        /// Converts a byte array into a hex string
        /// </summary>
        private static string ByteArrayToString(byte[] arrInput)
        {

            var output = new StringBuilder(arrInput.Length);

            for (var i = 0; i <= arrInput.Length - 1; i++)
            {
                output.Append(arrInput[i].ToString("X2"));
            }

            return output.ToString().ToLower();

        }

        /// <summary>
        /// Computes the MD5 hash for a file
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeFileHashMD5(string filePath)
        {

            string hashValue;

            // open file (as read-only)
            using (Stream objReader = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                // Hash contents of this stream
                hashValue = ComputeMD5Hash(objReader);
            }

            return hashValue;

        }

        /// <summary>
        /// Computes the MD5 hash for a string
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeStringHashMD5(string text)
        {

            var hashValue = ComputeMD5Hash(new MemoryStream(Encoding.UTF8.GetBytes(text)));

            return hashValue;

        }

        /// <summary>
        /// Computes the SHA-1 hash for a file
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeFileHashSha1(string filePath)
        {

            string hashValue;

            // open file (as read-only)
            using (Stream objReader = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                // Hash contents of this stream
                hashValue = ComputeSha1Hash(objReader);
            }

            return hashValue;

        }

        /// <summary>
        /// Computes the SHA-1 hash for a string
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeStringHashSha1(string text)
        {

            var hashValue = ComputeSha1Hash(new MemoryStream(Encoding.UTF8.GetBytes(text)));

            return hashValue;

        }

        /// <summary>
        /// Computes the MD5 hash of a given stream
        /// </summary>
        /// <param name="data"></param>
        /// <returns>MD5 hash, as a string</returns>
        /// <remarks></remarks>
        private static string ComputeMD5Hash(Stream data)
        {

            var md5Hasher = new MD5CryptoServiceProvider();
            return ComputeHash(md5Hasher, data);

        }

        /// <summary>
        /// Computes the SHA-1 hash of a given stream
        /// </summary>
        /// <param name="data"></param>
        /// <returns>SHA1 hash, as a string</returns>
        /// <remarks></remarks>
        private static string ComputeSha1Hash(Stream data)
        {

            var sha1Hasher = new SHA1CryptoServiceProvider();
            return ComputeHash(sha1Hasher, data);

        }

        /// <summary>
        /// Use the given hash algorithm to compute a hash of the data stream
        /// </summary>
        /// <param name="hasher"></param>
        /// <param name="data"></param>
        /// <returns>Hash string</returns>
        /// <remarks></remarks>
        private static string ComputeHash(HashAlgorithm hasher, Stream data)
        {
            // hash contents of this stream
            var arrHash = hasher.ComputeHash(data);

            // Return the hash, formatted as a string
            return ByteArrayToString(arrHash);

        }

        /// <summary>
        /// Creates a .hashcheck file for the specified file
        /// The file will be created in the same folder as the data file, and will contain size, modification_date_utc, and hash
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <param name="computeMD5Hash">If True, computes the MD5 hash</param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        public static string CreateHashcheckFile(string dataFilePath, bool computeMD5Hash)
        {

            string md5Hash;

            if (!File.Exists(dataFilePath))
                return string.Empty;

            if (computeMD5Hash)
            {
                md5Hash = ComputeFileHashMD5(dataFilePath);
            }
            else
            {
                md5Hash = string.Empty;
            }

            return CreateHashcheckFile(dataFilePath, md5Hash);

        }

        /// <summary>
        /// Creates a .hashcheck file for the specified file
        /// The file will be created in the same folder as the data file, and will contain size, modification_date_utc, and hash
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <param name="md5Hash"></param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        public static string CreateHashcheckFile(string dataFilePath, string md5Hash)
        {

            var fiDataFile = new FileInfo(dataFilePath);

            if (!fiDataFile.Exists)
                return string.Empty;

            var hashFilePath = fiDataFile.FullName + SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
            if (string.IsNullOrWhiteSpace(md5Hash))
                md5Hash = string.Empty;

            using (var swOutFile = new StreamWriter(new FileStream(hashFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swOutFile.WriteLine("# Hashcheck file created " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                swOutFile.WriteLine("size=" + fiDataFile.Length);
                swOutFile.WriteLine("modification_date_utc=" + fiDataFile.LastWriteTimeUtc.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                swOutFile.WriteLine("hash=" + md5Hash);
            }

            return hashFilePath;

        }

        /// <summary>
        /// Notify the user at console that an error occurred while writing to a log file or posting a log message to the database
        /// </summary>
        /// <param name="logMessage"></param>
        /// <param name="ex"></param>
        public static void ErrorWritingToLog(string logMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError("Error logging errors; log message: " + logMessage, ex);
        }

        /// <summary>
        /// Compares two files, byte-by-byte
        /// </summary>
        /// <param name="filePath1">Path to the first file</param>
        /// <param name="filePath2">Path to the second file</param>
        /// <returns>True if the files match; false if they don't match; also returns false if either file is missing</returns>
        /// <remarks></remarks>
        public static bool FilesMatch(string filePath1, string filePath2)
        {

            try
            {
                var fiFile1 = new FileInfo(filePath1);
                var fiFile2 = new FileInfo(filePath2);

                if (!fiFile1.Exists || !fiFile2.Exists)
                {
                    return false;
                }

                if (fiFile1.Length != fiFile2.Length)
                {
                    return false;
                }

                using (var srFile1 = new BinaryReader(new FileStream(fiFile1.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    using (var srFile2 = new BinaryReader(new FileStream(fiFile2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        while (srFile1.BaseStream.Position < fiFile1.Length)
                        {
                            if (srFile1.ReadByte() != srFile2.ReadByte())
                            {
                                return false;
                            }
                        }
                    }
                }

                return true;

            }
            catch (Exception ex)
            {
                // Ignore errors here
                Console.WriteLine("Error in clsGlobal.FilesMatch: " + ex.Message);
            }

            return false;

        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns>The number of cores on this computer</returns>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        public static int GetCoreCount()
        {
            return ProcessInfo.GetCoreCount();
        }

        private static double GetFreeDiskSpaceLinux(DirectoryInfo diDirectory)
        {
            var diDrive = new DriveInfo(diDirectory.Root.FullName);
            var freeSpaceMB = BytesToMB(diDrive.TotalFreeSpace);
            return freeSpaceMB;
        }

        /// <summary>
        /// Determine the free disk space on the drive with the given directory
        /// </summary>
        /// <param name="diDirectory"></param>
        /// <returns>Free space, in MB</returns>
        private static double GetFreeDiskSpaceWindows(DirectoryInfo diDirectory)
        {
            double freeSpaceMB;

            if (diDirectory.Root.FullName.StartsWith(@"\\") || !diDirectory.Root.FullName.Contains(":"))
            {
                // Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll

                if (clsDiskInfo.GetDiskFreeSpace(
                    diDirectory.FullName,
                    out _,
                    out _,
                    out var totalNumberOfFreeBytes))
                {
                    freeSpaceMB = BytesToMB(totalNumberOfFreeBytes);
                }
                else
                {
                    freeSpaceMB = 0;
                }

            }
            else
            {
                // Directory is a local drive; can query with .NET
                var diDrive = new DriveInfo(diDirectory.Root.FullName);
                freeSpaceMB = BytesToMB(diDrive.TotalFreeSpace);
            }

            return freeSpaceMB;
        }

        /// <summary>
        /// Reports the amount of free memory on this computer (in MB)
        /// </summary>
        /// <returns>Free memory, in MB</returns>
        public static float GetFreeMemoryMB()
        {
            // Use PRISM.SystemInfo to determine free memory
            // This works for both Windows and Linux
            // OS version is determined using:
            //   clsOSVersionInfo().GetOSVersion().ToLower().Contains("windows")

            return SystemInfo.GetFreeMemoryMB();
        }

        /// <summary>
        /// Replaces text in a string, ignoring case
        /// </summary>
        /// <param name="textToSearch"></param>
        /// <param name="textToFind"></param>
        /// <param name="replacementText"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ReplaceIgnoreCase(string textToSearch, string textToFind, string replacementText)
        {

            var charIndex = textToSearch.IndexOf(textToFind, StringComparison.OrdinalIgnoreCase);

            if (charIndex < 0)
            {
                return textToSearch;
            }

            string newText;
            if (charIndex == 0)
            {
                newText = string.Empty;
            }
            else
            {
                newText = textToSearch.Substring(0, charIndex);
            }

            newText += replacementText;

            if (charIndex + textToFind.Length < textToSearch.Length)
            {
                newText += textToSearch.Substring(charIndex + textToFind.Length);
            }

            return newText;
        }

        /// <summary>
        /// Show a trace message at the console, prepending it with a timestamp that includes milliseconds
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTimestampTrace(string message)
        {
            ConsoleMsgUtils.ShowDebug(string.Format("{0:yyyy-MM-dd hh:mm:ss.fff tt}: {1}", DateTime.Now, message));
        }

        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, ignores differences up until the given line number.  If
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before compaing</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, bool ignoreWhitespace)
        {

            const int comparisonStartLine = 0;
            const int comparisonEndLine = 0;

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);

        }


        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, ignores differences up until the given line number.  If
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before compaing</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, int comparisonStartLine, int comparisonEndLine, bool ignoreWhitespace)
        {

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);

        }

        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, ignores differences up until the given line number.
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before compaing</param>
        /// <param name="lineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(
            string filePath1, string filePath2,
            int comparisonStartLine, int comparisonEndLine,
            bool ignoreWhitespace, List<Regex> lineIgnoreRegExSpecs)
        {

            var chWhiteSpaceChars = new List<char>() { '\t', ' ' }.ToArray();

            try
            {
                var lineNumber = 0;

                using (var srFile1 = new StreamReader(new FileStream(filePath1, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    using (var srFile2 = new StreamReader(new FileStream(filePath2, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {

                        while (!srFile1.EndOfStream)
                        {
                            var dataLine1 = srFile1.ReadLine();
                            lineNumber += 1;

                            if (comparisonEndLine > 0 && lineNumber > comparisonEndLine)
                            {
                                // No need to compare further; files match up to this point
                                break;
                            }

                            if (dataLine1 == null)
                                dataLine1 = string.Empty;

                            if (!srFile2.EndOfStream)
                            {
                                var dataLine2 = srFile2.ReadLine();

                                if (lineNumber >= comparisonStartLine)
                                {
                                    if (dataLine2 == null)
                                        dataLine2 = string.Empty;

                                    if (ignoreWhitespace)
                                    {
                                        dataLine1 = dataLine1.Trim(chWhiteSpaceChars);
                                        dataLine2 = dataLine2.Trim(chWhiteSpaceChars);
                                    }

                                    if (dataLine1 != dataLine2)
                                    {
                                        // Lines don't match; are we ignoring both of them?
                                        if (TextFilesMatchIgnoreLine(dataLine1, lineIgnoreRegExSpecs) && TextFilesMatchIgnoreLine(dataLine2, lineIgnoreRegExSpecs))
                                        {
                                            // Ignoring both lines
                                        }
                                        else
                                        {
                                            // Files do not match
                                            return false;
                                        }
                                    }
                                }
                                continue;
                            }

                            // File1 has more lines than file2

                            if (!ignoreWhitespace)
                            {
                                // Files do not match
                                return false;
                            }

                            // Ignoring whitespace
                            // If file1 only has blank lines from here on out, the files match; otherwise, they don't
                            // See if the remaining lines are blank
                            do
                            {
                                if (dataLine1.Length != 0)
                                {
                                    if (!TextFilesMatchIgnoreLine(dataLine1, lineIgnoreRegExSpecs))
                                    {
                                        // Files do not match
                                        return false;
                                    }
                                }

                                if (srFile1.EndOfStream)
                                {
                                    break;
                                }

                                dataLine1 = srFile1.ReadLine();
                                if (dataLine1 == null)
                                    dataLine1 = string.Empty;
                                else
                                    dataLine1 = dataLine1.Trim(chWhiteSpaceChars);

                            } while (true);

                            break;

                        }

                        if (srFile2.EndOfStream)
                            return true;

                        // File2 has more lines than file1
                        if (!ignoreWhitespace)
                        {
                            // Files do not match
                            return false;
                        }

                        // Ignoring whitespace
                        // If file2 only has blank lines from here on out, the files match; otherwise, they don't
                        // See if the remaining lines are blank
                        do
                        {
                            var lineExtra = srFile2.ReadLine();
                            if (lineExtra == null)
                                lineExtra = string.Empty;
                            else
                                lineExtra = lineExtra.Trim(chWhiteSpaceChars);

                            if (lineExtra.Length != 0)
                            {
                                if (!TextFilesMatchIgnoreLine(lineExtra, lineIgnoreRegExSpecs))
                                {
                                    // Files do not match
                                    return false;
                                }
                            }
                        } while (!srFile2.EndOfStream);
                    }
                }

                return true;

            }
            catch (Exception)
            {
                // Error occurred
                return false;
            }

        }

        private static bool TextFilesMatchIgnoreLine(string dataLine, IReadOnlyCollection<Regex> lineIgnoreRegExSpecs)
        {
            if (lineIgnoreRegExSpecs == null)
                return false;

            foreach (var matchSpec in lineIgnoreRegExSpecs)
            {
                if (matchSpec == null)
                    continue;

                if (matchSpec.Match(dataLine).Success)
                {
                    // Line matches; ignore it
                    return true;
                }
            }

            return false;

        }

        /// <summary>
        /// Change the host name in the given share path to use a different host
        /// </summary>
        /// <param name="sharePath"></param>
        /// <param name="newHostName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string UpdateHostName(string sharePath, string newHostName)
        {

            if (!newHostName.StartsWith(@"\\"))
            {
                throw new NotSupportedException(@"\\ not found at the start of newHostName (" + newHostName + "); " +
                                                @"The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\");
            }

            if (!newHostName.EndsWith("\\"))
            {
                newHostName += "\\";
            }

            if (!sharePath.StartsWith(@"\\"))
            {
                throw new NotSupportedException(@"\\ not found at the start of sharePath (" + sharePath + "); " +
                                                @"The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\");
            }

            var slashLoc = sharePath.IndexOf("\\", 3, StringComparison.Ordinal);

            if (slashLoc < 0)
            {
                throw new Exception("Backslash not found after the 3rd character in SharePath, " + sharePath);
            }

            var sharePathNew = newHostName + sharePath.Substring(slashLoc + 1);

            return sharePathNew;

        }

        /// <summary>
        /// Returns True if the computer name is Pub-1000 or higher
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool UsingVirtualMachineOnPIC()
        {
            var rePub1000 = new Regex(@"Pub-1\d{3,}", RegexOptions.IgnoreCase);

            if (rePub1000.IsMatch(Environment.MachineName))
            {
                // The Memory performance counters are not available on Windows instances running under VMWare on PIC
                return true;
            }

            return false;
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file
        /// If found, opens the file and reads the stored values: size, modification_date_utc, and hash
        /// Next compares the stored values to the actual values
        /// Checks file size and file date, but does not compute the hash
        /// </summary>
        /// <param name="dataFilePath">Data file to check.</param>
        /// <param name="hashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(string dataFilePath, string hashFilePath, out string errorMessage)
        {
            return ValidateFileVsHashcheck(dataFilePath, hashFilePath, out errorMessage, checkDate: true, computeHash: false, checkSize: true);
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file
        /// If found, opens the file and reads the stored values: size, modification_date_utc, and hash
        /// Next compares the stored values to the actual values
        /// Checks file size, plus optionally date and hash
        /// </summary>
        /// <param name="dataFilePath">Data file to check.</param>
        /// <param name="hashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="errorMessage"></param>
        /// <param name="checkDate">If True, compares UTC modification time; times must agree within 2 seconds</param>
        /// <param name="computeHash"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(
            string dataFilePath, string hashFilePath, out string errorMessage,
            bool checkDate, bool computeHash)
        {
            return ValidateFileVsHashcheck(dataFilePath, hashFilePath, out errorMessage, checkDate, computeHash, checkSize: true);
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file
        /// If found, opens the file and reads the stored values: size, modification_date_utc, and hash
        /// Next compares the stored values to the actual values
        /// </summary>
        /// <param name="dataFilePath">Data file to check.</param>
        /// <param name="hashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="errorMessage"></param>
        /// <param name="checkDate">If True, compares UTC modification time; times must agree within 2 seconds</param>
        /// <param name="computeHash"></param>
        /// <param name="checkSize"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(
            string dataFilePath, string hashFilePath, out string errorMessage,
            bool checkDate, bool computeHash, bool checkSize)
        {

            var validFile = false;
            errorMessage = string.Empty;

            try
            {
                var fiDataFile = new FileInfo(dataFilePath);

                if (string.IsNullOrEmpty(hashFilePath))
                    hashFilePath = fiDataFile.FullName + SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
                var fiHashCheck = new FileInfo(hashFilePath);

                if (!fiDataFile.Exists)
                {
                    errorMessage = "Data file not found at " + fiDataFile.FullName;
                    return false;
                }

                if (!fiHashCheck.Exists)
                {
                    errorMessage = "Data file at " + fiDataFile.FullName + " does not have a corresponding .hashcheck file named " + fiHashCheck.Name;
                    return false;
                }

                long expectedFileSizeBytes = 0;
                var dtExpectedFileDate = DateTime.MinValue;
                var expectedHash = string.Empty;

                // Read the details in the HashCheck file
                using (var srInfile = new StreamReader(new FileStream(fiHashCheck.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInfile.EndOfStream)
                    {
                        var dataLine = srInfile.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine) || dataLine.StartsWith("#") || !dataLine.Contains('='))
                            continue;

                        var lineParts = dataLine.Split('=');


                        if (lineParts.Length < 2)
                            continue;

                        // Set this to true for now
                        validFile = true;

                        switch (lineParts[0].ToLower())
                        {
                            case "size":
                                long.TryParse(lineParts[1], out expectedFileSizeBytes);
                                break;
                            case "modification_date_utc":
                                DateTime.TryParse(lineParts[1], out dtExpectedFileDate);
                                break;
                            case "hash":
                                expectedHash = string.Copy(lineParts[1]);
                                break;
                        }
                    }
                }

                if (checkSize && fiDataFile.Length != expectedFileSizeBytes)
                {
                    errorMessage = "File size mismatch: expecting " + expectedFileSizeBytes.ToString("#,##0") + " but computed " + fiDataFile.Length.ToString("#,##0");
                    return false;
                }

                // Only compare dates if we are not comparing hash values
                if (!computeHash && checkDate)
                {
                    if (Math.Abs(fiDataFile.LastWriteTimeUtc.Subtract(dtExpectedFileDate).TotalSeconds) > 2)
                    {
                        errorMessage = "File modification date mismatch: expecting " +
                            dtExpectedFileDate.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + " UTC but actually " +
                            fiDataFile.LastWriteTimeUtc.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + " UTC";
                        return false;
                    }
                }

                if (computeHash)
                {
                    // Compute the hash of the file
                    var actualHash = ComputeFileHashMD5(dataFilePath);

                    if (actualHash != expectedHash)
                    {
                        errorMessage = "Hash mismatch: expecting " + expectedHash + " but computed " + actualHash;
                        return false;
                    }
                }

                return validFile;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in ValidateFileVsHashcheck: " + ex.Message);
            }

            return validFile;

        }

        /// <summary>
        /// Check the free space on the drive with the given directory
        /// </summary>
        [Obsolete("Use the version with argument logToDatabase")]
        public static bool ValidateFreeDiskSpace(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            LogTools.LoggerTypes eLogLocationIfNotFound,
            out string errorMessage)
        {
            var logToDatabase = eLogLocationIfNotFound == LogTools.LoggerTypes.LogDb;
            return ValidateFreeDiskSpace(directoryDescription, directoryPath, minFreeSpaceMB, out errorMessage, logToDatabase);
        }

        /// <summary>
        /// Check the free space on the drive with the given directory
        /// </summary>
        /// <param name="directoryDescription"></param>
        /// <param name="directoryPath"></param>
        /// <param name="minFreeSpaceMB"></param>
        /// <param name="logToDatabase"></param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if the drive has sufficient free space, otherwise false</returns>
        public static bool ValidateFreeDiskSpace(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            out string errorMessage,
            bool logToDatabase = false)
        {

            errorMessage = string.Empty;

            var diDirectory = new DirectoryInfo(directoryPath);
            if (!diDirectory.Exists)
            {
                // Example error message: Organism DB directory not found: G:\DMS_Temp_Org
                errorMessage = directoryDescription + " not found: " + directoryPath;
                LogTools.LogError(errorMessage, null, logToDatabase);
                return false;
            }

            double freeSpaceMB;

            if (LinuxOS)
            {
                freeSpaceMB = GetFreeDiskSpaceLinux(diDirectory);
            }
            else
            {
                freeSpaceMB = GetFreeDiskSpaceWindows(diDirectory);
            }

            if (freeSpaceMB < minFreeSpaceMB)
            {
                // Example error message: Organism DB directory drive has less than 6858 MB free: 5794 MB
                errorMessage = $"{directoryDescription} drive has less than {minFreeSpaceMB} MB free: {(int)freeSpaceMB} MB";
                Console.WriteLine(errorMessage);
                LogTools.LogError(errorMessage);
                return false;
            }

            return true;
        }

        #endregion

        #region "clsEventNotifier events"

        private static void RegisterEvents(clsEventNotifier oProcessingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                oProcessingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                oProcessingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            // Ignore: oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private static void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogTools.LogDebug(statusMessage, writeToLog: false);
        }

        private static void DebugEventHandler(string statusMessage)
        {
            LogTools.LogDebug(statusMessage);
        }

        private static void StatusEventHandler(string statusMessage)
        {
            LogTools.LogMessage(statusMessage);
        }

        private static void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogTools.LogError(errorMessage, ex);
        }

        private static void WarningEventHandler(string warningMessage)
        {
            LogTools.LogWarning(warningMessage);
        }

        #endregion

    }
}
