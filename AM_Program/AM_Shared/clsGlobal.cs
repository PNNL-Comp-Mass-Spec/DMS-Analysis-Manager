using PRISM;
using PRISM.Logging;
using PRISMWin;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

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
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
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
        public const string STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX = "StepTool_ParamFileStoragePath_";

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

        private static string mAppDirectoryPath;

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
                if (string.IsNullOrWhiteSpace(addnlComment))
                    return string.Empty;

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
        /// <param name="fieldNames"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string CollapseList(List<string> fieldNames)
        {

            return FlattenList(fieldNames, "\t");

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

                var lockFile = new FileInfo(lockFilePath);
                if (lockFile.Exists)
                {
                    lockFile.Delete();
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
        /// Flatten a list of items into a single string, with items separated by delimiter
        /// </summary>
        /// <param name="itemList"></param>
        /// <param name="delimiter"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string FlattenList(List<string> itemList, string delimiter)
        {
            if (itemList == null || itemList.Count == 0)
            {
                return string.Empty;
            }

            return string.Join(delimiter, itemList);
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        [Obsolete("Use GetAppDirectoryPath")]
        public static string GetAppFolderPath()
        {
            return GetAppDirectoryPath();
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppDirectoryPath()
        {
            if (mAppDirectoryPath != null)
                return mAppDirectoryPath;

            mAppDirectoryPath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppDirectoryPath();

            return mAppDirectoryPath;
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
        public static string GetAssemblyVersion(Assembly assembly)
        {
            // assembly.FullName typically returns something like this:
            // AnalysisManagerProg, Version=2.3.4479.23831, Culture=neutral, PublicKeyToken=null
            //
            // the goal is to extract out the text after Version= but before the next comma

            var reGetVersion = new Regex("version=([0-9.]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var version = assembly.FullName;

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
        /// <param name="queryResults">DataTable (Output Parameter)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Uses a timeout of 30 seconds</remarks>
        public static bool GetDataTableByQuery(string sqlStr, string connectionString, string callingFunction, short retryCount, out DataTable queryResults)
        {

            const int timeoutSeconds = 30;

            return GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, out queryResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the specified Sql query
        /// </summary>
        /// <param name="sqlStr">Sql query</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="queryResults">DataTable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByQuery(
            string sqlStr, string connectionString, string callingFunction,
            short retryCount, out DataTable queryResults, int timeoutSeconds)
        {

            var cmd = new SqlCommand(sqlStr)
            {
                CommandType = CommandType.Text
            };

            return GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, out queryResults, timeoutSeconds);

        }

        /// <summary>
        /// Runs the stored procedure or database query defined by "cmd"
        /// </summary>
        /// <param name="cmd">SqlCommand var (query or stored procedure)</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="callingFunction">Name of the calling function</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="queryResults">DataTable (Output Parameter)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        public static bool GetDataTableByCmd(
            SqlCommand cmd,
            string connectionString,
            string callingFunction,
            short retryCount,
            out DataTable queryResults,
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
                                queryResults = ds.Tables[0];
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

            queryResults = null;
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
                var versionChecker = new DotNETVersionChecker();
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
        /// <param name="firstQueryResult">Results, as a list of columns (first row only if multiple rows)</param>
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
            out List<string> firstQueryResult,
            string callingFunction,
            short retryCount = 3,
            int timeoutSeconds = 5)
        {

            var success = GetQueryResults(sqlQuery, connectionString, out var queryResults, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn: 1);

            if (success)
            {
                firstQueryResult = queryResults.FirstOrDefault() ?? new List<string>();
                return true;
            }

            firstQueryResult = new List<string>();
            return false;
        }

        /// <summary>
        /// Run a query against a SQL Server database, return the results as a list of strings
        /// </summary>
        /// <param name="sqlQuery">Query to run</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="queryResults">Results (list of list of strings)</param>
        /// <param name="callingFunction">Name of the calling function (for logging purposes)</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
        /// <param name="maxRowsToReturn">Maximum rows to return; 0 to return all rows</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Null values are converted to empty strings
        /// Numbers are converted to their string equivalent
        /// Use the GetDataTable functions in this class if you need to retain numeric values or null values
        /// </remarks>
        public static bool GetQueryResults(
            string sqlQuery,
            string connectionString,
            out List<List<string>> queryResults,
            string callingFunction,
            short retryCount = 3,
            int timeoutSeconds = 30,
            int maxRowsToReturn = 0)
        {

            if (OfflineMode)
            {
                LogTools.LogError(string.Format("Offline mode enabled; {0} cannot execute query {1}", callingFunction, sqlQuery));
                queryResults = new List<List<string>>();
                return false;
            }

            if (retryCount < 1)
                retryCount = 1;
            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var DBTools = new DBTools(connectionString);
            RegisterEvents(DBTools);

            var success = DBTools.GetQueryResults(sqlQuery, out queryResults, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn);

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
                return StackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return StackTraceFormatter.GetExceptionStackTrace(ex);

        }

        /// <summary>
        /// Parse settingText to extract the key name and value (separated by an equals sign)
        /// </summary>
        /// <param name="settingText"></param>
        /// <returns>Key/Value pair</returns>
        /// <remarks>
        /// If the line starts with # it is treated as a comment line and an empty key/value pair will be returned
        /// If the line contains a # sign in the middle, the comment is left intact
        /// </remarks>
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
            ConsoleMsgUtils.SleepSeconds(waitTimeSeconds);
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

            return string.Equals(text1, text2, StringComparison.OrdinalIgnoreCase);
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
        /// <param name="headerLine">Tab delimited list of headers</param>
        /// <param name="expectedHeaderNames">Expected header column names</param>
        /// <param name="isCaseSensitive">True if the header names are case sensitive</param>
        /// <returns>Dictionary with the header names and 0-based column index</returns>
        /// <remarks>Header names not found in headerLine will have an index of -1</remarks>
        public static Dictionary<string, int> ParseHeaderLine(string headerLine, List<string> expectedHeaderNames, bool isCaseSensitive = false)
        {
            var headerMapping = new Dictionary<string, int>();

            var columnNames = headerLine.Split('\t').ToList();

            foreach (var expectedName in expectedHeaderNames)
            {
                var colIndex = -1;

                if (isCaseSensitive)
                {
                    colIndex = columnNames.IndexOf(expectedName);
                }
                else
                {
                    for (var i = 0; i <= columnNames.Count - 1; i++)
                    {
                        if (IsMatch(columnNames[i], expectedName))
                        {
                            colIndex = i;
                            break;
                        }
                    }
                }

                headerMapping.Add(expectedName, colIndex);
            }

            return headerMapping;

        }

        /// <summary>
        /// Examines filePath to look for spaces
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns>filePath as-is if no spaces, otherwise filePath surrounded by double quotes </returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string filePath)
        {
            return PathUtils.PossiblyQuotePath(filePath);
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
            return PRISM.DataUtils.StringToValueUtils.TryGetValue(dataColumns, colIndex, out value);
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
            return PRISM.DataUtils.StringToValueUtils.TryGetValueInt(dataColumns, colIndex, out value);
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
            return PRISM.DataUtils.StringToValueUtils.TryGetValueFloat(dataColumns, colIndex, out value);
        }

        /// <summary>
        /// Converts a string value to a boolean equivalent
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>Returns false if an exception</remarks>
        public static bool CBoolSafe(string value)
        {
            return PRISM.DataUtils.StringToValueUtils.CBoolSafe(value);
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
            return PRISM.DataUtils.StringToValueUtils.CBoolSafe(value, defaultValue);
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
            return PRISM.DataUtils.StringToValueUtils.CIntSafe(value, defaultValue);
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
            return PRISM.DataUtils.StringToValueUtils.CFloatSafe(value, defaultValue);
        }

        /// <summary>
        /// Copies file sourceFilePath to directory targetDirectoryPath, renaming it to targetFileName.
        /// However, if file targetFileName already exists, that file will first be backed up
        /// Furthermore, up to versionCountToKeep old versions of the file will be kept
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="targetFileName"></param>
        /// <param name="versionCountToKeep">Maximum backup copies of the file to keep; must be 9 or less</param>
        /// <returns>True if Success, false if failure </returns>
        /// <remarks></remarks>
        public static bool CopyAndRenameFileWithBackup(string sourceFilePath, string targetDirectoryPath, string targetFileName, int versionCountToKeep)
        {

            try
            {
                var sourceFile = new FileInfo(sourceFilePath);
                if (!sourceFile.Exists)
                {
                    // Source file not found
                    return false;
                }

                var baseName = Path.GetFileNameWithoutExtension(targetFileName);
                if (baseName == null)
                {
                    // Cannot continue without a base filename
                    return false;
                }

                var extension = Path.GetExtension(targetFileName);
                if (string.IsNullOrEmpty(extension))
                {
                    extension = ".bak";
                }

                if (versionCountToKeep > 9)
                    versionCountToKeep = 9;
                if (versionCountToKeep < 0)
                    versionCountToKeep = 0;

                // Backup any existing copies of targetFilePath
                for (var revision = versionCountToKeep - 1; revision >= 0; revision += -1)
                {
                    try
                    {
                        var baseNameCurrent = string.Copy(baseName);
                        if (revision > 0)
                        {
                            baseNameCurrent += "_" + revision;
                        }
                        baseNameCurrent += extension;

                        var fileToRename = new FileInfo(Path.Combine(targetDirectoryPath, baseNameCurrent));
                        var newFilePath = Path.Combine(targetDirectoryPath, baseName + "_" + (revision + 1) + extension);

                        // Confirm that newFilePath doesn't exist; delete it if it does
                        if (File.Exists(newFilePath))
                        {
                            File.Delete(newFilePath);
                        }

                        // Rename the current file to newFilePath
                        if (fileToRename.Exists)
                        {
                            fileToRename.MoveTo(newFilePath);
                        }

                    }
                    catch (Exception)
                    {
                        // Ignore errors here; we'll continue on with the next file
                    }

                }

                var finalFilePath = Path.Combine(targetDirectoryPath, targetFileName);

                // Now copy the file from sourceFilePath to newFilePath
                sourceFile.CopyTo(finalFilePath, true);

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
            return DBTools.GetString(dbValue);
        }

        /// <summary>
        /// Converts an database field value to a single, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static float DbCSng(object dbValue)
        {
            return DBTools.GetFloat(dbValue);
        }

        /// <summary>
        /// Converts an database field value to a double, checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static double DbCDbl(object dbValue)
        {
            return DBTools.GetDouble(dbValue);
        }

        /// <summary>
        /// Converts an database field value to an integer (int32), checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static int DbCInt(object dbValue)
        {
            return DBTools.GetInteger(dbValue);
        }

        /// <summary>
        /// Converts an database field value to a long integer (int64), checking for null values
        /// </summary>
        /// <param name="dbValue">Value from database</param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static long DbCLng(object dbValue)
        {
            return DBTools.GetLong(dbValue);
        }

        /// <summary>
        /// Computes the MD5 hash for a file
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISM.HashUtilities.ComputeFileHashMD5")]
        public static string ComputeFileHashMD5(string filePath)
        {
            return HashUtilities.ComputeFileHashMD5(filePath);
        }

        /// <summary>
        /// Computes the MD5 hash for a string
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISM.HashUtilities.ComputeStringHashMD5")]
        public static string ComputeStringHashMD5(string text)
        {
            return HashUtilities.ComputeStringHashMD5(text);
        }

        /// <summary>
        /// Computes the SHA-1 hash for a file
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISM.HashUtilities.ComputeFileHashSha1")]
        public static string ComputeFileHashSha1(string filePath)
        {
            return HashUtilities.ComputeFileHashSha1(filePath);
        }

        /// <summary>
        /// Computes the SHA-1 hash for a string
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISM.HashUtilities.ComputeStringHashSha1")]
        public static string ComputeStringHashSha1(string text)
        {
            return HashUtilities.ComputeStringHashSha1(text);
        }

        /// <summary>
        /// Creates a .hashcheck file for the specified file
        /// The file will be created in the same directory as the data file, and will contain size, modification_date_utc, and hash
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <param name="computeMD5Hash">If True, computes the MD5 hash, otherwise creates a hashcheck file with an empty string for the hash</param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        public static string CreateHashcheckFile(string dataFilePath, bool computeMD5Hash)
        {

            string md5Hash;

            if (!File.Exists(dataFilePath))
                return string.Empty;

            if (computeMD5Hash)
            {
                md5Hash = HashUtilities.ComputeFileHashMD5(dataFilePath);
            }
            else
            {
                md5Hash = string.Empty;
            }

            var hashcheckFilePath = HashUtilities.CreateHashcheckFileWithHash(dataFilePath, HashUtilities.HashTypeConstants.MD5, md5Hash, out var warningMessage);
            if (!string.IsNullOrWhiteSpace(warningMessage))
                ConsoleMsgUtils.ShowWarning(warningMessage);

            return hashcheckFilePath;
        }

        /// <summary>
        /// Creates a .hashcheck file for the specified file, using the given hash string
        /// The file will be created in the same directory as the data file, and will contain size, modification_date_utc, hash, and hash type
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <param name="md5Hash"></param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        [Obsolete("Use PRISM.HashUtilities.CreateHashcheckFile and specify the hash type")]
        public static string CreateHashcheckFile(string dataFilePath, string md5Hash)
        {
            var hashCheckFilePath = HashUtilities.CreateHashcheckFileWithHash(dataFilePath, HashUtilities.HashTypeConstants.MD5, md5Hash, out var warningMessage);
            if (!string.IsNullOrWhiteSpace(warningMessage))
                ConsoleMsgUtils.ShowWarning(warningMessage);

            return hashCheckFilePath;
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
        /// <remarks>See also TextFilesMatch</remarks>
        public static bool FilesMatch(string filePath1, string filePath2)
        {

            try
            {
                var file1 = new FileInfo(filePath1);
                var file2 = new FileInfo(filePath2);

                if (!file1.Exists || !file2.Exists)
                {
                    return false;
                }

                if (file1.Length != file2.Length)
                {
                    return false;
                }

                using (var reader1 = new BinaryReader(new FileStream(file1.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    using (var reader2 = new BinaryReader(new FileStream(file2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        while (reader1.BaseStream.Position < file1.Length)
                        {
                            if (reader1.ReadByte() != reader2.ReadByte())
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

        /// <summary>
        /// Get a DriveInfo instance for the drive with the given target directory (must be on the local host)
        /// Supports both Windows and Linux paths
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <returns></returns>
        public static DriveInfo GetLocalDriveInfo(DirectoryInfo targetDirectory)
        {
            var baseWarningMsg = "Unable to instantiate a DriveInfo object for " + targetDirectory.FullName;

            try
            {
                DriveInfo localDriveInfo;

                if (Path.DirectorySeparatorChar == '/' || targetDirectory.FullName.StartsWith("/"))
                {
                    // Linux system, with a path like /file1/temp/DMSOrgDBs/
                    // The root path that we need to send to DriveInfo is likely /file1
                    // If that doesn't work, try /

                    var candidateRootPaths = new List<string>();
                    var slashIndex = targetDirectory.FullName.IndexOf('/', 1);

                    if (slashIndex > 0)
                    {
                        candidateRootPaths.Add(targetDirectory.FullName.Substring(0, slashIndex));
                    }
                    candidateRootPaths.Add("/");

                    foreach (var candidatePath in candidateRootPaths)
                    {
                        try
                        {
                            localDriveInfo = new DriveInfo(candidatePath);
                            return localDriveInfo;
                        }
                        catch (Exception ex)
                        {
                            ConsoleMsgUtils.ShowDebug(string.Format("Unable to create a DriveInfo object for {0}: {1}", candidatePath, ex.Message));
                        }
                    }
                }
                else
                {
                    // Windows system, with a path like C:\DMS_Temp_Org
                    // Alternatively, a Windows share like \\proto-7\MSGFPlus_Index_Files

                    var driveLetter = targetDirectory.FullName.Substring(0, 2);
                    if (driveLetter.EndsWith(":"))
                    {
                        localDriveInfo = new DriveInfo(driveLetter);
                        return localDriveInfo;
                    }
                }

            }
            catch (Exception ex)
            {
                LogTools.LogWarning(string.Format("{0}: {1}", baseWarningMsg, ex));
            }

            LogTools.LogWarning(baseWarningMsg);
            return null;

        }

        /// <summary>
        /// Determine the free disk space on the drive with the given directory
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <returns></returns>
        private static double GetFreeDiskSpaceLinux(DirectoryInfo targetDirectory)
        {
            var driveInfo = GetLocalDriveInfo(targetDirectory);
            if (driveInfo == null)
                return 0;

            var freeSpaceMB = BytesToMB(driveInfo.TotalFreeSpace);
            return freeSpaceMB;
        }

        /// <summary>
        /// Determine the free disk space on the drive with the given directory
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <returns>Free space, in MB</returns>
        /// <remarks>Supports local drives on Windows and Linux; supports remote shares like \\Server\Share\ on Windows</remarks>
        private static double GetFreeDiskSpaceWindows(DirectoryInfo targetDirectory)
        {
            double freeSpaceMB;

            if (targetDirectory.Root.FullName.StartsWith(@"\\") || !targetDirectory.Root.FullName.Contains(":"))
            {
                // Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll
                var targetFilePath = Path.Combine(targetDirectory.FullName, "DummyFile.txt");

                var success = DiskInfo.GetDiskFreeSpace(
                    targetFilePath, out var totalNumberOfFreeBytes, out var errorMessage, reportFreeSpaceAvailableToUser: false);

                if (success)
                {
                    freeSpaceMB = BytesToMB(totalNumberOfFreeBytes);
                }
                else
                {
                    LogTools.LogWarning(errorMessage);
                    freeSpaceMB = 0;
                }

            }
            else
            {
                // Directory is a local drive; can query with .NET
                var driveInfo = new DriveInfo(targetDirectory.Root.FullName);
                freeSpaceMB = BytesToMB(driveInfo.TotalFreeSpace);
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
            //   OSVersionInfo().GetOSVersion().ToLower().Contains("windows")

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
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before comparing</param>
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
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before comparing</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, int comparisonStartLine, int comparisonEndLine, bool ignoreWhitespace)
        {

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);

        }

        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is greater than 1, ignores differences up until the given line number.
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, removes whitespace from the beginning and end of each line before comparing</param>
        /// <param name="lineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(
            string filePath1, string filePath2,
            int comparisonStartLine, int comparisonEndLine,
            bool ignoreWhitespace, List<Regex> lineIgnoreRegExSpecs)
        {

            var whiteSpaceChars = new List<char>() { '\t', ' ' }.ToArray();

            try
            {
                var lineNumber = 0;

                using (var reader1 = new StreamReader(new FileStream(filePath1, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    using (var reader2 = new StreamReader(new FileStream(filePath2, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {

                        while (!reader1.EndOfStream)
                        {
                            var dataLine1 = reader1.ReadLine();
                            lineNumber += 1;

                            if (comparisonEndLine > 0 && lineNumber > comparisonEndLine)
                            {
                                // No need to compare further; files match up to this point
                                break;
                            }

                            if (dataLine1 == null)
                                dataLine1 = string.Empty;

                            if (!reader2.EndOfStream)
                            {
                                var dataLine2 = reader2.ReadLine();

                                if (lineNumber >= comparisonStartLine)
                                {
                                    if (dataLine2 == null)
                                        dataLine2 = string.Empty;

                                    if (ignoreWhitespace)
                                    {
                                        dataLine1 = dataLine1.Trim(whiteSpaceChars);
                                        dataLine2 = dataLine2.Trim(whiteSpaceChars);
                                    }

                                    if (dataLine1 != dataLine2)
                                    {
                                        // Lines don't match; are we ignoring both of them?
                                        if (TextFilesMatchIgnoreLine(dataLine1, lineIgnoreRegExSpecs) &&
                                            TextFilesMatchIgnoreLine(dataLine2, lineIgnoreRegExSpecs))
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

                                if (reader1.EndOfStream)
                                {
                                    break;
                                }

                                dataLine1 = reader1.ReadLine();
                                if (dataLine1 == null)
                                    dataLine1 = string.Empty;
                                else
                                    dataLine1 = dataLine1.Trim(whiteSpaceChars);

                            } while (true);

                            break;

                        }

                        if (reader2.EndOfStream)
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
                            var lineExtra = reader2.ReadLine();
                            if (lineExtra == null)
                                lineExtra = string.Empty;
                            else
                                lineExtra = lineExtra.Trim(whiteSpaceChars);

                            if (lineExtra.Length != 0)
                            {
                                if (!TextFilesMatchIgnoreLine(lineExtra, lineIgnoreRegExSpecs))
                                {
                                    // Files do not match
                                    return false;
                                }
                            }
                        } while (!reader2.EndOfStream);
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
        [Obsolete("Use PRISM.FileSyncUtils.ValidateFileVsHashcheck")]
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
        [Obsolete("Use PRISM.FileSyncUtils.ValidateFileVsHashcheck")]
        public static bool ValidateFileVsHashcheck(
            string dataFilePath, string hashFilePath, out string errorMessage,
            bool checkDate, bool computeHash)
        {
            return ValidateFileVsHashcheck(dataFilePath, hashFilePath, out errorMessage, checkDate, computeHash, checkSize: true);
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file; returns false if not found
        /// If found, compares the stored values to the actual values (size, modification_date_utc, and hash)
        /// Next compares the stored values to the actual values
        /// </summary>
        /// <param name="dataFilePath">Data file to check</param>
        /// <param name="hashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="checkDate">If True, compares UTC modification time; times must agree within 2 seconds</param>
        /// <param name="computeHash">If true, compute the file hash (every time); if false, only compare file size and date</param>
        /// <param name="checkSize">If true, compare the actual file size to that in the hashcheck file</param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        [Obsolete("Use PRISM.FileSyncUtils.ValidateFileVsHashcheck")]
        public static bool ValidateFileVsHashcheck(
            string dataFilePath, string hashFilePath, out string errorMessage,
            bool checkDate, bool computeHash, bool checkSize)
        {

            var validFile = FileSyncUtils.ValidateFileVsHashcheck(dataFilePath, hashFilePath, out errorMessage, checkDate, computeHash, checkSize);
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
        /// <remarks>Supports local drives on Windows and Linux; supports remote shares like \\Server\Share\ on Windows</remarks>
        public static bool ValidateFreeDiskSpace(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            out string errorMessage,
            bool logToDatabase = false)
        {

            errorMessage = string.Empty;

            var targetDirectory = new DirectoryInfo(directoryPath);
            if (!targetDirectory.Exists)
            {
                // Example error message: Organism DB directory not found: G:\DMS_Temp_Org
                errorMessage = directoryDescription + " not found: " + directoryPath;
                LogTools.LogError(errorMessage, null, logToDatabase);
                return false;
            }

            double freeSpaceMB;

            if (LinuxOS)
            {
                freeSpaceMB = GetFreeDiskSpaceLinux(targetDirectory);
            }
            else
            {
                freeSpaceMB = GetFreeDiskSpaceWindows(targetDirectory);
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

        #region "EventNotifier events"

        private static void RegisterEvents(EventNotifier oProcessingClass, bool writeDebugEventsToLog = true)
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
