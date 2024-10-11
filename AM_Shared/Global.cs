using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.StatusReporting;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMWin;

// ReSharper disable UnusedMember.Global

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
    public static class Global
    {
        // Ignore Spelling: addnl, App, cmd, hashcheck, hyperthreading, prepending, Pwd, Sng, Sql, Utc

        /// <summary>
        /// Job parameters file prefix
        /// </summary>
        public const string JOB_PARAMETERS_FILE_PREFIX = "JobParameters_";

        /// <summary>
        /// Step tool param file storage path file prefix
        /// </summary>
        public const string STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX = "Step_Tool_Param_File_Storage_Path_";

        /// <summary>
        /// Server cache hashcheck file suffix
        /// </summary>
        public const string SERVER_CACHE_HASHCHECK_FILE_SUFFIX = ".hashcheck";

        /// <summary>
        /// Lock file suffix (.lock)
        /// </summary>
        public const string LOCK_FILE_EXTENSION = DMSUpdateManager.RemoteUpdateUtility.LOCK_FILE_EXTENSION;

        /// <summary>
        /// Analysis resource true/false options
        /// </summary>
        public enum AnalysisResourceOptions
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
        /// When true, show trace messages
        /// </summary>
        public static bool TraceMode { get; set; }

        /// <summary>
        /// Trace point at which to immediately halt program execution
        /// </summary>
        public static string TraceStopPoint { get; set; } = string.Empty;

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

        private static string mAppDirectoryPath;

        private static SystemProcessInfo mSystemProcessInfo;

        /// <summary>
        /// Appends a string to a job comment string
        /// </summary>
        /// <param name="baseComment">Initial comment</param>
        /// <param name="addnlComment">Comment to be appended</param>
        /// <returns>String containing both comments</returns>
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
        /// <param name="bytes">Bytes</param>
        public static double BytesToGB(long bytes)
        {
            return DirectorySpaceTools.BytesToGB(bytes);
        }

        /// <summary>
        /// Convert Bytes to Megabytes
        /// </summary>
        /// <param name="bytes">Bytes</param>
        public static double BytesToMB(long bytes)
        {
            return DirectorySpaceTools.BytesToMB(bytes);
        }

        /// <summary>
        /// Examines count to determine which string to return
        /// </summary>
        /// <param name="count">Count of the number of items being described</param>
        /// <param name="textIfOneItem">Text to return if count equals one</param>
        /// <param name="textIfZeroOrMultiple">Text to return if count is not one</param>
        /// <returns>Returns textIfOneItem if count is 1; otherwise, returns textIfZeroOrMultiple</returns>
        public static string CheckPlural(int count, string textIfOneItem, string textIfZeroOrMultiple)
        {
            if (count == 1)
            {
                return textIfOneItem;
            }

            return textIfZeroOrMultiple;
        }

        /// <summary>
        /// If TraceStopPoint matches currentTraceLocation, exit the application immediately
        /// </summary>
        /// <remarks>If TraceMode is true, will show the current trace point name if not a match</remarks>
        /// <param name="currentTraceLocation">Current trace location</param>
        public static void CheckStopTrace(string currentTraceLocation)
        {
            CheckStopTrace(TraceStopPoint, currentTraceLocation, TraceMode);
        }

        /// <summary>
        /// If traceStopPoint matches currentTraceLocation, exit the application immediately
        /// </summary>
        /// <remarks>If traceModeEnabled is true, will show the current trace point name if not a match</remarks>
        /// <param name="traceStopPoint">Trace stop point</param>
        /// <param name="currentTraceLocation">Current trace location</param>
        /// <param name="traceModeEnabled">If true, trace mode is enabled</param>
        public static void CheckStopTrace(string traceStopPoint, string currentTraceLocation, bool traceModeEnabled)
        {
            if (string.IsNullOrEmpty(traceStopPoint))
                return;

            if (traceStopPoint.Equals(currentTraceLocation, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine();
                ConsoleMsgUtils.ShowWarning("Exiting application at trace point " + currentTraceLocation);
                Environment.Exit(0);
            }
            else if (traceModeEnabled)
            {
                ConsoleMsgUtils.ShowDebug("Trace point: " + currentTraceLocation);
            }
        }

        /// <summary>
        /// Collapse a list of items to a tab-delimited list
        /// </summary>
        /// <param name="fieldNames">List of strings</param>
        public static string CollapseList(List<string> fieldNames)
        {
            return FlattenList(fieldNames, "\t");
        }

        /// <summary>
        /// Assure that the directory exists; attempt to create it if missing
        /// </summary>
        /// <param name="directoryPath">Directory path</param>
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
            return AppUtils.DecodeShiftCipher(encodedPwd);
        }

        /// <summary>
        /// Delete the lock file for the corresponding data file
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
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
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        /// <param name="runningLinux">Set to true if running Linux</param>
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
        /// <param name="itemList">List of strings</param>
        /// <param name="delimiter">Delimiter</param>
        public static string FlattenList(List<string> itemList, string delimiter)
        {
            if (itemList == null || itemList.Count == 0)
            {
                return string.Empty;
            }

            return string.Join(delimiter, itemList);
        }

        /// <summary>
        /// Flatten a list of items into a single string, with items separated by delimiter
        /// </summary>
        /// <param name="itemList">List of strings</param>
        /// <param name="delimiter">Delimiter</param>
        public static string FlattenList(SortedSet<string> itemList, string delimiter)
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
        public static string GetAppDirectoryPath()
        {
            if (mAppDirectoryPath != null)
                return mAppDirectoryPath;

            mAppDirectoryPath = AppUtils.GetAppDirectoryPath();

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

            // The goal is to extract out the text after Version= but before the next comma

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
        /// Determine the version of .NET that is running
        /// </summary>
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
        /// <remarks>
        /// Null values are converted to empty strings
        /// Numbers are converted to their string equivalent
        /// Use the GetDataTable functions in this class if you need to retain numeric values or null values
        /// </remarks>
        /// <param name="dbTools">Instance of IDBTools</param>
        /// <param name="sqlQuery">Query to run</param>
        /// <param name="firstQueryResult">Results, as a list of columns (first row only if multiple rows)</param>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <param name="callerName">Name of the calling method (for logging purposes)</param>
        /// <returns>True if success, false if an error</returns>
        public static bool GetQueryResultsTopRow(
            IDBTools dbTools,
            string sqlQuery,
            out List<string> firstQueryResult,
            short retryCount = 3,
            [CallerMemberName] string callerName = "UnknownMethod")
        {
            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount, callingFunction: callerName);

            if (success)
            {
                firstQueryResult = queryResults.FirstOrDefault() ?? new List<string>();
                return true;
            }

            firstQueryResult = new List<string>();
            return false;
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex">Exception</param>
        /// <returns>String similar to "Stack trace: CodeTest.Test-:-CodeTest.TestException-:-CodeTest.InnerTestException in CodeTest.vb:line 86"</returns>
        public static string GetExceptionStackTrace(Exception ex)
        {
            return GetExceptionStackTrace(ex, false);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex">Exception</param>
        /// <param name="multiLineOutput">When true, format the stack trace using newline characters instead of -:-</param>
        /// <returns>String similar to "Stack trace: CodeTest.Test-:-CodeTest.TestException-:-CodeTest.InnerTestException in CodeTest.vb:line 86"</returns>
        public static string GetExceptionStackTrace(Exception ex, bool multiLineOutput)
        {
            if (multiLineOutput)
            {
                return StackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return StackTraceFormatter.GetExceptionStackTrace(ex);
        }

        /// <summary>
        /// Sleep for the specified seconds
        /// </summary>
        /// <param name="waitTimeSeconds">Wait time, in seconds</param>
        public static void IdleLoop(double waitTimeSeconds)
        {
            ConsoleMsgUtils.SleepSeconds(waitTimeSeconds);
        }

        /// <summary>
        /// Compare two strings (not case-sensitive)
        /// </summary>
        /// <remarks>
        /// A null string is considered equivalent to an empty string.
        /// Thus, two null strings are considered equal.
        /// </remarks>
        /// <param name="text1">First string</param>
        /// <param name="text2">Second string</param>
        /// <returns>True if they match; false if not</returns>
        public static bool IsMatch(string text1, string text2)
        {
            return IsMatch(text1, text2, true);
        }

        /// <summary>
        /// Compare two strings (not case-sensitive)
        /// </summary>
        /// <remarks>Two null strings are considered equal, even if treatNullAsEmptyString is false</remarks>
        /// <param name="text1">First string</param>
        /// <param name="text2">Second string</param>
        /// <param name="treatNullAsEmptyString">When true, a null string is considered equivalent to an empty string</param>
        /// <returns>True if they match; false if not</returns>
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
        /// <remarks>Header names not found in headerLine will have an index of -1</remarks>
        /// <param name="headerLine">Tab delimited list of headers</param>
        /// <param name="expectedHeaderNames">Expected header column names</param>
        /// <returns>Dictionary with the header names and 0-based column index</returns>
        public static Dictionary<string, int> ParseHeaderLine(string headerLine, List<string> expectedHeaderNames)
        {
            var columnMap = new Dictionary<string, int>();
            ParseHeaderLine(columnMap, headerLine, expectedHeaderNames);
            return columnMap;
        }

        /// <summary>
        /// Parses the headers in headerLine to look for the names specified in headerNames
        /// </summary>
        /// <remarks>Header names not found in headerLine will have an index of -1</remarks>
        /// <param name="columnMap">
        /// Mapping from column identifier to the index of the column in the header line; this dictionary will be cleared then populated
        /// </param>
        /// <param name="headerLine">Tab delimited list of headers</param>
        /// <param name="expectedHeaderNames">Expected header column names</param>
        /// <returns>Dictionary with the header names and 0-based column index</returns>
        public static bool ParseHeaderLine(Dictionary<string, int> columnMap, string headerLine, List<string> expectedHeaderNames)
        {
            var columnNamesByIdentifier = new Dictionary<string, SortedSet<string>>();

            foreach (var headerName in expectedHeaderNames)
            {
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, headerName);
            }

            return DataTableUtils.GetColumnMappingFromHeaderLine(columnMap, headerLine, columnNamesByIdentifier);
        }

        /// <summary>
        /// Replace invalid characters in the given file name
        /// </summary>
        /// <remarks>
        /// If validating a file name, use false for allowPathSeparators
        /// If validating a full path, use true for allowPathSeparators
        /// </remarks>
        /// <param name="fileNameOrPath">File name or file path to examine</param>
        /// <param name="allowPathSeparators">When true, allow backslash and forward slash characters</param>
        /// <param name="replaceSpaces">When true, replace spaces with underscores</param>
        /// <returns>Updated file name</returns>
        public static string ReplaceInvalidPathChars(string fileNameOrPath, bool allowPathSeparators = false, bool replaceSpaces = true)
        {
            var updatedValue = replaceSpaces
                ? fileNameOrPath.Replace(" ", "_")
                : fileNameOrPath;

            foreach (var invalidChar in Path.GetInvalidPathChars())
            {
                if (updatedValue.Contains(invalidChar))
                    updatedValue = updatedValue.Replace(invalidChar, '_');
            }

            if (allowPathSeparators)
                return updatedValue;

            foreach (var invalidChar in Path.GetInvalidFileNameChars())
            {
                if (updatedValue.Contains(invalidChar))
                    updatedValue = updatedValue.Replace(invalidChar, '_');
            }

            return updatedValue;
        }

        /// <summary>
        /// Examines filePath to look for spaces
        /// </summary>
        /// <param name="filePath">File path</param>
        /// <returns>filePath as-is if no spaces, otherwise filePath surrounded by double quotes </returns>
        public static string PossiblyQuotePath(string filePath)
        {
            return PathUtils.PossiblyQuotePath(filePath);
        }

        /// <summary>
        /// Return true if the current host is DMS developer's computer
        /// </summary>
        public static bool RunningOnDeveloperComputer()
        {
            var hostName = System.Net.Dns.GetHostName();

            return hostName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) ||
                   hostName.StartsWith("WE31383", StringComparison.OrdinalIgnoreCase) ||
                   hostName.StartsWith("WE43320", StringComparison.OrdinalIgnoreCase) ||
                   hostName.StartsWith("WE27676", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Get the named attribute from the given element
        /// </summary>
        /// <param name="item">XElement item</param>
        /// <param name="attributeName">Attribute name</param>
        /// <param name="attributeValue">Output: attribute value</param>
        /// <returns>True if found, otherwise false</returns>
        public static bool TryGetAttribute(XElement item, string attributeName, out string attributeValue)
        {
            if (!item.HasAttributes)
            {
                attributeValue = string.Empty;
                return false;
            }

            var attribute = item.Attribute(attributeName);

            if (attribute == null)
            {
                attributeValue = string.Empty;
                return false;
            }

            attributeValue = attribute.Value;
            return true;
        }

        /// <summary>
        /// Tries to retrieve the string value at index colIndex in dataColumns()
        /// </summary>
        /// <param name="dataColumns">Array of strings</param>
        /// <param name="colIndex">Column index</param>
        /// <param name="value">Output: value at the given index</param>
        /// <returns>True if success; false if colIndex is less than 0 or colIndex is out of range for dataColumns()</returns>
        public static bool TryGetValue(string[] dataColumns, int colIndex, out string value)
        {
            return PRISM.DataUtils.StringToValueUtils.TryGetValue(dataColumns, colIndex, out value);
        }

        /// <summary>
        /// Tries to convert the text at index colIndex of dataColumns to an integer
        /// </summary>
        /// <param name="dataColumns">Array of strings</param>
        /// <param name="colIndex">Column index</param>
        /// <param name="value">Output: integer value at the given index; zero if not an integer</param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
        public static bool TryGetValueInt(string[] dataColumns, int colIndex, out int value)
        {
            return PRISM.DataUtils.StringToValueUtils.TryGetValueInt(dataColumns, colIndex, out value);
        }

        /// <summary>
        /// Tries to convert the text at index colIndex of dataColumns to a float
        /// </summary>
        /// <param name="dataColumns">Array of strings</param>
        /// <param name="colIndex">Column index</param>
        /// <param name="value">Output: float value at the given index; zero if not an integer</param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to a float</returns>
        public static bool TryGetValueFloat(string[] dataColumns, int colIndex, out float value)
        {
            return PRISM.DataUtils.StringToValueUtils.TryGetValueFloat(dataColumns, colIndex, out value);
        }

        /// <summary>
        /// Converts value to an integer
        /// </summary>
        /// <param name="value">Number stored as a string</param>
        /// <param name="defaultValue">Integer to return if value is not numeric</param>
        public static int CIntSafe(string value, int defaultValue)
        {
            return PRISM.DataUtils.StringToValueUtils.CIntSafe(value, defaultValue);
        }

        /// <summary>
        /// Converts value to a single (aka float)
        /// </summary>
        /// <param name="value">Number stored as a string</param>
        /// <param name="defaultValue">Float to return if value is not numeric</param>
        public static float CSngSafe(string value, float defaultValue)
        {
            return PRISM.DataUtils.StringToValueUtils.CFloatSafe(value, defaultValue);
        }

        /// <summary>
        /// Copies file sourceFilePath to directory targetDirectoryPath, renaming it to targetFileName.
        /// However, if file targetFileName already exists, that file will first be backed up
        /// Furthermore, up to versionCountToKeep old versions of the file will be kept
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="targetDirectoryPath">Target directory path</param>
        /// <param name="targetFileName">Target file name</param>
        /// <param name="versionCountToKeep">Maximum backup copies of the file to keep; must be 9 or less</param>
        /// <returns>True if Success, false if failure </returns>
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
                        var baseNameCurrent = baseName;

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
        /// Creates a .hashcheck file for the specified file
        /// The file will be created in the same directory as the data file, and will contain size, modification_date_utc, and hash
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="computeMD5Hash">If true, computes the MD5 hash, otherwise creates a hashcheck file with an empty string for the hash</param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
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
        /// Notify the user at console that an error occurred while writing to a log file or posting a log message to the database
        /// </summary>
        /// <param name="logMessage">Log message</param>
        /// <param name="ex">Exception</param>
        public static void ErrorWritingToLog(string logMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError("Error logging errors; log message: " + logMessage, ex);
        }

        /// <summary>
        /// Compares two files, byte-by-byte
        /// </summary>
        /// <remarks>See also TextFilesMatch</remarks>
        /// <param name="filePath1">Path to the first file</param>
        /// <param name="filePath2">Path to the second file</param>
        /// <returns>True if the files match; false if they don't match; also returns false if either file is missing</returns>
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

                using var reader1 = new BinaryReader(new FileStream(file1.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
                using var reader2 = new BinaryReader(new FileStream(file2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (reader1.BaseStream.Position < file1.Length)
                {
                    if (reader1.ReadByte() != reader2.ReadByte())
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                Console.WriteLine("Error in Global.FilesMatch: " + ex.Message);
            }

            return false;
        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        /// <returns>The number of cores on this computer</returns>
        public static int GetCoreCount()
        {
            return ProcessInfo.GetCoreCount();
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
        /// <param name="textToSearch">Text to search</param>
        /// <param name="textToFind">Text to find</param>
        /// <param name="replacementText">Replacement text</param>
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
        /// <param name="message">Trace message</param>
        public static void ShowTimestampTrace(string message)
        {
            ConsoleMsgUtils.ShowDebug("{0:yyyy-MM-dd hh:mm:ss.fff tt}: {1}", DateTime.Now, message);
        }

        /// <summary>
        /// Compares two files line-by-line. If comparisonStartLine is > 0, ignores differences up until the given line number.
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before comparing</param>
        /// <param name="lineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
        public static bool TextFilesMatch(string filePath1, string filePath2, bool ignoreWhitespace, List<Regex> lineIgnoreRegExSpecs = null)
        {
            const int comparisonStartLine = 0;
            const int comparisonEndLine = 0;

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, lineIgnoreRegExSpecs);
        }

        /// <summary>
        /// Compares two files line-by-line. If comparisonStartLine is > 0, ignores differences up until the given line number.
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, removes white space from the beginning and end of each line before comparing</param>
        public static bool TextFilesMatch(string filePath1, string filePath2, int comparisonStartLine, int comparisonEndLine, bool ignoreWhitespace)
        {
            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);
        }

        /// <summary>
        /// Compares two files line-by-line. If comparisonStartLine is greater than 1, ignores differences up until the given line number.
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, removes whitespace from the beginning and end of each line before comparing</param>
        /// <param name="lineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
        public static bool TextFilesMatch(
            string filePath1, string filePath2,
            int comparisonStartLine, int comparisonEndLine,
            bool ignoreWhitespace, List<Regex> lineIgnoreRegExSpecs)
        {
            var whiteSpaceChars = new List<char> { '\t', ' ' }.ToArray();

            try
            {
                var lineNumber = 0;

                using var reader1 = new StreamReader(new FileStream(filePath1, FileMode.Open, FileAccess.Read, FileShare.Read));
                using var reader2 = new StreamReader(new FileStream(filePath2, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader1.EndOfStream)
                {
                    var dataLine1 = reader1.ReadLine() ?? string.Empty;
                    lineNumber++;

                    if (comparisonEndLine > 0 && lineNumber > comparisonEndLine)
                    {
                        // No need to compare further; files match up to this point
                        break;
                    }

                    if (!reader2.EndOfStream)
                    {
                        var dataLine2 = reader2.ReadLine() ?? string.Empty;

                        if (lineNumber >= comparisonStartLine)
                        {
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
                    while (true)
                    {
                        if (dataLine1.Length > 0)
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

                        dataLine1 = reader1.ReadLine() ?? string.Empty;

                        dataLine1 = dataLine1.Trim(whiteSpaceChars);
                    }

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

                // If file2 only has blank lines from here on out, the files match; otherwise, they don't
                // See if the remaining lines are blank
                do
                {
                    var lineExtra = reader2.ReadLine() ?? string.Empty;

                    var trimmedLine = lineExtra.Trim(whiteSpaceChars);

                    if (trimmedLine.Length == 0)
                    {
                        continue;
                    }

                    if (TextFilesMatchIgnoreLine(trimmedLine, lineIgnoreRegExSpecs))
                    {
                        continue;
                    }

                    // Files do not match
                    return false;
                } while (!reader2.EndOfStream);

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
        /// Returns true if the computer name is Pub-1000 or higher
        /// </summary>
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

        private static void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                processingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                processingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;
            // Ignore: processingClass.ProgressUpdate += ProgressUpdateHandler;
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
    }
}
