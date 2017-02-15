using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using PRISM.DataBase;
using PRISM.Logging;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/20/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    public class clsGlobal
    {

        #region "Constants"
        public const bool LOG_LOCAL_ONLY = true;
        public const bool LOG_DATABASE = false;
        public const string XML_FILENAME_PREFIX = "JobParameters_";

        public const string XML_FILENAME_EXTENSION = "xml";

        public const string STEPTOOL_PARAMFILESTORAGEPATH_PREFIX = "StepTool_ParamFileStoragePath_";

        public const string SERVER_CACHE_HASHCHECK_FILE_SUFFIX = ".hashcheck";
        #endregion

        #region "Enums"
        public enum eAnalysisResourceOptions
        {
            OrgDbRequired = 0,
            MyEMSLSearchDisabled = 1
        }
        #endregion

        #region "Module variables"

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern int GetDiskFreeSpaceEx(
            string lpRootPathName, 
            out ulong lpFreeBytesAvailable, 
            out ulong lpTotalNumberOfBytes, 
            out ulong lpTotalNumberOfFreeBytes);

        private static string mAppFolderPath;

        private static SystemMemoryInfo mSystemMemoryInfo;

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
                return addnlComment;
            }

            if (string.IsNullOrWhiteSpace(addnlComment) || baseComment.Contains(addnlComment))
            {
                // Either addnlComment is empty (unlikely) or addnlComment is a duplicate comment
                // Return the base comment
                return baseComment;
            }

            // Append a semicolon to baseComment, but only if it doesn't already end in a semicolon
            if (baseComment.TrimEnd(' ').EndsWith(";"))
            {
                return baseComment + addnlComment;
            }

            return baseComment + "; " + addnlComment;
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
        /// Examines intCount to determine which string to return
        /// </summary>
        /// <param name="intCount"></param>
        /// <param name="strTextIfOneItem"></param>
        /// <param name="strTextIfZeroOrMultiple"></param>
        /// <returns>Returns strTextIfOneItem if intCount is 1; otherwise, returns strTextIfZeroOrMultiple</returns>
        /// <remarks></remarks>
        public static string CheckPlural(int intCount, string strTextIfOneItem, string strTextIfZeroOrMultiple)
        {
            if (intCount == 1)
            {
                return strTextIfOneItem;
            }

            return strTextIfZeroOrMultiple;
        }

        /// <summary>
        /// Collapse an array of items to a tab-delimited list
        /// </summary>
        /// <param name="strItems"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string CollapseLine(string[] strItems)
        {
            if (strItems == null || strItems.Length == 0)
            {
                return string.Empty;
            }

            return CollapseList(strItems.ToList());
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
        /// Decrypts password received from ini file
        /// </summary>
        /// <param name="enPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        public static string DecodePassword(string enPwd)
        {
            // Decrypts password received from ini file
            // Password was created by alternately subtracting or adding 1 to the ASCII value of each character

            // Convert the password string to a character array
            var pwdChars = enPwd.ToCharArray();
            var pwdBytes = new List<byte>();
            var pwdCharsAdj = new List<char>();

            for (var i = 0; i <= pwdChars.Length - 1; i++)
            {
                pwdBytes.Add((byte)pwdChars[i]);
            }

            // Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string

            for (var byteCntr = 0; byteCntr <= pwdBytes.Count - 1; byteCntr++)
            {
                if (byteCntr % 2 == 0)
                {
                    pwdBytes[byteCntr] += 1;
                }
                else
                {
                    pwdBytes[byteCntr] -= 1;
                }
                pwdCharsAdj.Add((char)pwdBytes[byteCntr]);
            }

            return String.Join("", pwdCharsAdj);

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

            return String.Join(delimiter, lstItems);
        }

        /// <summary>
        /// Returns the directory in which the entry assembly (typically the Program .exe file) resides 
        /// </summary>
        /// <returns>Full directory path</returns>
        public static string GetAppFolderPath()
        {

            if (mAppFolderPath != null)
                return mAppFolderPath;

            var objAssembly = Assembly.GetEntryAssembly();

            if (objAssembly.Location == null)
            {
                mAppFolderPath = string.Empty;
            }
            else
            {
                var fiAssemblyFile = new FileInfo(objAssembly.Location);
                mAppFolderPath = fiAssemblyFile.DirectoryName;
            }
            return mAppFolderPath;

        }

        /// <summary>
        /// Returns the version string of the entry assembly (typically the Program .exe file)
        /// </summary>
        /// <returns>Assembly version, e.g. 1.0.4482.23831</returns>
        public static string GetAssemblyVersion()
        {
            var objAssembly = Assembly.GetEntryAssembly();
            if (objAssembly == null)
                return string.Empty;

            return GetAssemblyVersion(objAssembly);

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
            var strVersion = objAssembly.FullName;

            var reMatch = reGetVersion.Match(strVersion);

            if (reMatch.Success)
            {
                strVersion = reMatch.Groups[1].Value;
            }

            return strVersion;

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

            var cmd = new SqlCommand(sqlStr) {
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
                    msg += ", RetryCount = " + retryCount.ToString();

                    if (cmd.CommandType == CommandType.Text)
                    {
                        msg += ", Query = " + cmd.CommandText;
                    }

                    Console.WriteLine(msg);
                    LogError(msg);
                    Thread.Sleep(retryDelaySeconds * 1000);

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

            List<List<string>> lstResultTable;

            var success = GetQueryResults(sqlQuery, connectionString, out lstResultTable, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn: 1);

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

            if (retryCount < 1)
                retryCount = 1;
            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var dbTools = new clsDBTools(connectionString);

            dbTools.ErrorEvent += DbToolsErrorEventHandler;

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
                return Utilities.GetExceptionStackTraceMultiLine(ex);
            }

            return Utilities.GetExceptionStackTrace(ex);

        }

        public static KeyValuePair<string, string> GetKeyValueSetting(string strText)
        {

            var emptyKvPair = new KeyValuePair<string, string>(string.Empty, string.Empty);

            if (string.IsNullOrWhiteSpace(strText))
                return emptyKvPair;

            strText = strText.Trim();

            if (strText.StartsWith("#") || !strText.Contains('='))
                return emptyKvPair;

            var intCharIndex = strText.IndexOf("=", StringComparison.Ordinal);

            if (intCharIndex <= 0)
                return emptyKvPair;

            var strKey = strText.Substring(0, intCharIndex).Trim();
            string strValue;

            if (intCharIndex < strText.Length - 1)
            {
                strValue = strText.Substring(intCharIndex + 1).Trim();
            }
            else
            {
                strValue = string.Empty;
            }

            return new KeyValuePair<string, string>(strKey, strValue);
        }

        /// <summary>
        /// Compare two strings (not case sensitive)
        /// </summary>
        /// <param name="strText1"></param>
        /// <param name="strText2"></param>
        /// <returns>True if they match; false if not</returns>
        /// <remarks>A null string is considered equivalent to an empty string.  Thus, two null strings are considered equal</remarks>
        public static bool IsMatch(string strText1, string strText2)
        {
            return IsMatch(strText1, strText2, true);
        }

        /// <summary>
        /// Compare two strings (not case sensitive)
        /// </summary>
        /// <param name="strText1"></param>
        /// <param name="strText2"></param>
        /// <param name="treatNullAsEmptyString">When true, a null string is considered equivalent to an empty string</param>
        /// <returns>True if they match; false if not</returns>
        /// <remarks>Two null strings are considered equal, even if treatNullAsEmptyString is false</remarks>
        public static bool IsMatch(string strText1, string strText2, bool treatNullAsEmptyString)
        {

            if (treatNullAsEmptyString && string.IsNullOrWhiteSpace(strText1) && string.IsNullOrWhiteSpace(strText2))
            {
                return true;
            }

            if (String.Compare(strText1, strText2, StringComparison.OrdinalIgnoreCase) == 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Parses the headers in strHeaderLine to look for the names specified in lstHeaderNames
        /// </summary>
        /// <param name="strHeaderLine"></param>
        /// <param name="lstHeaderNames"></param>
        /// <param name="isCaseSensitive"></param>
        /// <returns>Dictionary with the header names and 0-based column index</returns>
        /// <remarks>Header names not found in strHeaderLine will have an index of -1</remarks>
        public static Dictionary<string, int> ParseHeaderLine(string strHeaderLine, List<string> lstHeaderNames, bool isCaseSensitive)
        {
            var dctHeaderMapping = new Dictionary<string, int>();

            var lstColumns = strHeaderLine.Split('\t').ToList();

            foreach (var headerName in lstHeaderNames)
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
        /// Examines strPath to look for spaces
        /// </summary>
        /// <param name="strPath"></param>
        /// <returns>strPath as-is if no spaces, otherwise strPath surrounded by double quotes </returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string strPath)
        {
            if (string.IsNullOrWhiteSpace(strPath))
            {
                return string.Empty;

            }

            if (strPath.Contains(" "))
            {
                if (!strPath.StartsWith("\""))
                {
                    strPath = "\"" + strPath;
                }

                if (!strPath.EndsWith("\""))
                {
                    strPath += "\"";
                }
            }

            return strPath;
        }

        /// <summary>
        /// Converts a string value to a boolean equivalent
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>Returns false if an exception</remarks>
        public static bool CBoolSafe(string value)
        {
            bool blnValue;

            if (Boolean.TryParse(value, out blnValue))
                return blnValue;

            return false;

        }

        /// <summary>
        /// Converts a string value to a boolean equivalent
        /// </summary>
        /// <param name="value"></param>
        /// <param name="blnDefaultValue">Boolean value to return if value is empty or an exception occurs</param>
        /// <returns></returns>
        /// <remarks>Returns false if an exception</remarks>
        public static bool CBoolSafe(string value, bool blnDefaultValue)
        {
            bool blnValue;

            if (Boolean.TryParse(value, out blnValue))
                return blnValue;

            return blnDefaultValue;

        }

        /// <summary>
        /// Converts value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="intDefaultValue">Integer to return if value is not numeric</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static int CIntSafe(string value, int intDefaultValue)
        {
            int intValue;

            if (Int32.TryParse(value, out intValue))
                return intValue;

            return intDefaultValue;

        }

        /// <summary>
        /// Converts value to a single (aka float)
        /// </summary>
        /// <param name="value"></param>
        /// <param name="sngDefaultValue">Single to return if value is not numeric</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static float CSngSafe(string value, float sngDefaultValue)
        {
            float sngValue;

            if (Single.TryParse(value, out sngValue))
                return sngValue;

            return sngDefaultValue;

        }

        /// <summary>
        /// Copies file SourceFilePath to folder TargetFolder, renaming it to TargetFileName.
        /// However, if file TargetFileName already exists, then that file will first be backed up
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

                var strBaseName = Path.GetFileNameWithoutExtension(TargetFileName);
                if (strBaseName == null)
                {
                    // Cannot continue without a base filename
                    return false;
                }

                var strExtension = Path.GetExtension(TargetFileName);
                if (string.IsNullOrEmpty(strExtension))
                {
                    strExtension = ".bak";
                }

                if (VersionCountToKeep > 9)
                    VersionCountToKeep = 9;
                if (VersionCountToKeep < 0)
                    VersionCountToKeep = 0;

                // Backup any existing copies of strTargetFilePath
                for (var intRevision = VersionCountToKeep - 1; intRevision >= 0; intRevision += -1)
                {
                    try
                    {
                        var strBaseNameCurrent = string.Copy(strBaseName);
                        if (intRevision > 0)
                        {
                            strBaseNameCurrent += "_" + intRevision;
                        }
                        strBaseNameCurrent += strExtension;

                        var ioFileToRename = new FileInfo(Path.Combine(TargetFolder, strBaseNameCurrent));
                        var strNewFilePath = Path.Combine(TargetFolder, strBaseName + "_" + (intRevision + 1) + strExtension);

                        // Confirm that strNewFilePath doesn't exist; delete it if it does
                        if (File.Exists(strNewFilePath))
                        {
                            File.Delete(strNewFilePath);
                        }

                        // Rename the current file to strNewFilePath
                        if (ioFileToRename.Exists)
                        {
                            ioFileToRename.MoveTo(strNewFilePath);
                        }

                    }
                    catch (Exception)
                    {
                        // Ignore errors here; we'll continue on with the next file
                    }

                }

                var strFinalFilePath = Path.Combine(TargetFolder, TargetFileName);

                // Now copy the file from SourceFilePath to strNewFilePath
                ioSrcFile.CopyTo(strFinalFilePath, true);

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
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string DbCStr(object InpObj)
        {
            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return string.Empty;
            }

            return Convert.ToString(InpObj);
        }

        /// <summary>
        /// Converts an database field value to a single, checking for null values
        /// </summary>
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static float DbCSng(object InpObj)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return (float)0.0;
            }

            return Convert.ToSingle(InpObj);

        }

        /// <summary>
        /// Converts an database field value to a double, checking for null values
        /// </summary>
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static double DbCDbl(object InpObj)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0;
            }

            return Convert.ToDouble(InpObj);

        }

        /// <summary>
        /// Converts an database field value to an integer (int32), checking for null values
        /// </summary>
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static int DbCInt(object InpObj)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToInt32(InpObj);

        }

        /// <summary>
        /// Converts an database field value to a long integer (int64), checking for null values
        /// </summary>
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        public static long DbCLng(object InpObj)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToInt64(InpObj);

        }

        /// <summary>
        /// Converts an database field value to a decimal, checking for null values
        /// </summary>
        /// <param name="InpObj"></param>
        /// <returns></returns>
        /// <remarks>An exception will be thrown if the value is not numeric</remarks>
        [Obsolete("Decimal data types should be avoided")]
        public static decimal DbCDec(object InpObj)
        {

            // If input var is DbNull, returns "", otherwise returns String representation of var
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return Convert.ToDecimal(InpObj);

        }

        /// <summary>
        /// Converts a byte array into a hex string
        /// </summary>
        private static string ByteArrayToString(byte[] arrInput)
        {
 
            var  strOutput = new StringBuilder(arrInput.Length);

            for (var i = 0; i <= arrInput.Length - 1; i++)
            {
                strOutput.Append(arrInput[i].ToString("X2"));
            }

            return strOutput.ToString().ToLower();

        }

        /// <summary>
        /// Computes the MD5 hash for a file
        /// </summary>
        /// <param name="strPath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeFileHashMD5(string strPath)
        {

            string hashValue;

            // open file (as read-only)
            using (Stream objReader = new FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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
        /// <param name="strPath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ComputeFileHashSha1(string strPath)
        {

            string hashValue;

            // open file (as read-only)
            using (Stream objReader = new FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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

            var objMD5 = new MD5CryptoServiceProvider();
            return ComputeHash(objMD5, data);

        }

        /// <summary>
        /// Computes the SHA-1 hash of a given stream
        /// </summary>
        /// <param name="data"></param>
        /// <returns>SHA1 hash, as a string</returns>
        /// <remarks></remarks>
        private static string ComputeSha1Hash(Stream data)
        {

            var objSha1 = new SHA1CryptoServiceProvider();
            return ComputeHash(objSha1, data);

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
        /// <param name="strDataFilePath"></param>
        /// <param name="blnComputeMD5Hash">If True, then computes the MD5 hash</param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        public static string CreateHashcheckFile(string strDataFilePath, bool blnComputeMD5Hash)
        {

            string strMD5Hash;

            if (!File.Exists(strDataFilePath))
                return string.Empty;

            if (blnComputeMD5Hash)
            {
                strMD5Hash = ComputeFileHashMD5(strDataFilePath);
            }
            else
            {
                strMD5Hash = string.Empty;
            }

            return CreateHashcheckFile(strDataFilePath, strMD5Hash);

        }

        /// <summary>
        /// Creates a .hashcheck file for the specified file
        /// The file will be created in the same folder as the data file, and will contain size, modification_date_utc, and hash
        /// </summary>
        /// <param name="strDataFilePath"></param>
        /// <param name="strMD5Hash"></param>
        /// <returns>The full path to the .hashcheck file; empty string if a problem</returns>
        /// <remarks></remarks>
        public static string CreateHashcheckFile(string strDataFilePath, string strMD5Hash)
        {

            var fiDataFile = new FileInfo(strDataFilePath);

            if (!fiDataFile.Exists)
                return string.Empty;

            var strHashFilePath = fiDataFile.FullName + SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
            if (string.IsNullOrWhiteSpace(strMD5Hash))
                strMD5Hash = string.Empty;

            using (var swOutFile = new StreamWriter(new FileStream(strHashFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swOutFile.WriteLine("# Hashcheck file created " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                swOutFile.WriteLine("size=" + fiDataFile.Length);
                swOutFile.WriteLine("modification_date_utc=" + fiDataFile.LastWriteTimeUtc.ToString("yyyy-MM-dd hh:mm:ss tt"));
                swOutFile.WriteLine("hash=" + strMD5Hash);
            }

            return strHashFilePath;

        }

        /// <summary>
        /// Compares two files, byte-by-byte
        /// </summary>
        /// <param name="strFilePath1">Path to the first file</param>
        /// <param name="strFilePath2">Path to the second file</param>
        /// <returns>True if the files match; false if they don't match; also returns false if either file is missing</returns>
        /// <remarks></remarks>
        public static bool FilesMatch(string strFilePath1, string strFilePath2)
        {

            try
            {
                var fiFile1 = new FileInfo(strFilePath1);
                var fiFile2 = new FileInfo(strFilePath2);

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
        /// Determines free disk space for the disk where the given directory resides.  Supports both fixed drive letters and UNC paths (e.g. \\Server\Share\)
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="freeBytesAvailableToUser"></param>
        /// <param name="totalDriveCapacityBytes"></param>
        /// <param name="totalNumberOfFreeBytes"></param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks></remarks>
        private static bool GetDiskFreeSpace(
            string directoryPath, out ulong freeBytesAvailableToUser,
            out ulong totalDriveCapacityBytes, out ulong totalNumberOfFreeBytes)
        {

            var result = GetDiskFreeSpaceEx(directoryPath, out freeBytesAvailableToUser, out totalDriveCapacityBytes, out totalNumberOfFreeBytes);

            if (result == 0)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reports the amount of free memory on this computer (in MB)
        /// </summary>
        /// <returns>Free memory, in MB</returns>
        public static float GetFreeMemoryMB()
        {
            if (mSystemMemoryInfo == null)
            {
                mSystemMemoryInfo = new SystemMemoryInfo();
                RegisterEvents(mSystemMemoryInfo, writeDebugEventsToLog: false);
            }

            return mSystemMemoryInfo.GetFreeMemoryMB();

        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        /// <remarks>The message is shown in dark grey in the console.</remarks>
        public static void LogDebug(string statusMessage, bool writeToLog = true)
        {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine("  " + statusMessage);
            Console.ResetColor();

            if (writeToLog)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage);
            }
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log (allowed to be nothing)</param>
        /// <remarks>The error is shown in red in the console.  The exception stack trace is shown in cyan</remarks>
        public static void LogError(string errorMessage, Exception ex = null)
        {
            string formattedError;
            if (ex == null || errorMessage.EndsWith(ex.Message))
            {
                formattedError = errorMessage;
            }
            else
            {
                formattedError = errorMessage + ": " + ex.Message;
            }
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(formattedError);

            if (ex != null)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(GetExceptionStackTrace(ex, true));
            }
            Console.ResetColor();
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, formattedError, ex);

        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isError">True if this is an error</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        public static void LogMessage(string statusMessage, bool isError = false, bool writeToLog = true)
        {
            if (isError)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(statusMessage);
                Console.ResetColor();
            }
            else
            {
                Console.WriteLine(statusMessage);
            }

            if (!writeToLog)
                return;

            if (isError)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, statusMessage);
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, statusMessage);
            }
        }

        /// <summary>
        /// Display a warning message at the console and write to the log file
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        public static void LogWarning(string warningMessage)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(warningMessage);
            Console.ResetColor();
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage);
        }

        /// <summary>
        /// Replaces text in a string, ignoring case
        /// </summary>
        /// <param name="strTextToSearch"></param>
        /// <param name="strTextToFind"></param>
        /// <param name="strReplacementText"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ReplaceIgnoreCase(string strTextToSearch, string strTextToFind, string strReplacementText)
        {

            var intCharIndex = strTextToSearch.ToLower().IndexOf(strTextToFind.ToLower(), StringComparison.Ordinal);

            if (intCharIndex < 0)
            {
                return strTextToSearch;
            }

            string strNewText;
            if (intCharIndex == 0)
            {
                strNewText = string.Empty;
            }
            else
            {
                strNewText = strTextToSearch.Substring(0, intCharIndex);
            }

            strNewText += strReplacementText;

            if (intCharIndex + strTextToFind.Length < strTextToSearch.Length)
            {
                strNewText += strTextToSearch.Substring(intCharIndex + strTextToFind.Length);
            }

            return strNewText;
        }

        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number.  If 
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, bool ignoreWhitespace)
        {

            const int comparisonStartLine = 0;
            const int comparisonEndLine = 0;

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);

        }


        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number.  If 
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, int comparisonStartLine, int comparisonEndLine, bool ignoreWhitespace)
        {

            return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, null);

        }

        /// <summary>
        /// Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number. 
        /// </summary>
        /// <param name="filePath1">First file</param>
        /// <param name="filePath2">Second file</param>
        /// <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
        /// <param name="comparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
        /// <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
        /// <param name="lstLineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static bool TextFilesMatch(string filePath1, string filePath2, int comparisonStartLine, int comparisonEndLine, bool ignoreWhitespace, List<Regex> lstLineIgnoreRegExSpecs)
        {

            var chWhiteSpaceChars = new List<char>() {'\t', ' '}.ToArray();

            try
            {
                var intLineNumber = 0;

                using (var srFile1 = new StreamReader(new FileStream(filePath1, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    using (var srFile2 = new StreamReader(new FileStream(filePath2, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {

                        while (!srFile1.EndOfStream)
                        {
                            var strLineIn1 = srFile1.ReadLine();
                            intLineNumber += 1;

                            if (comparisonEndLine > 0 && intLineNumber > comparisonEndLine)
                            {
                                // No need to compare further; files match up to this point
                                break;
                            }

                            if (strLineIn1 == null)
                                strLineIn1 = string.Empty;

                            if (!srFile2.EndOfStream)
                            {
                                var strLineIn2 = srFile2.ReadLine();

                                if (intLineNumber >= comparisonStartLine)
                                {
                                    if (strLineIn2 == null)
                                        strLineIn2 = string.Empty;

                                    if (ignoreWhitespace)
                                    {
                                        strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars);
                                        strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars);
                                    }

                                    if (strLineIn1 != strLineIn2)
                                    {
                                        // Lines don't match; are we ignoring both of them?
                                        if (TextFilesMatchIgnoreLine(strLineIn1, lstLineIgnoreRegExSpecs) && TextFilesMatchIgnoreLine(strLineIn2, lstLineIgnoreRegExSpecs))
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
                            // If file1 only has blank lines from here on out, then the files match; otherwise, they don't
                            // See if the remaining lines are blank
                            do
                            {
                                if (strLineIn1.Length != 0)
                                {
                                    if (!TextFilesMatchIgnoreLine(strLineIn1, lstLineIgnoreRegExSpecs))
                                    {
                                        // Files do not match
                                        return false;
                                    }
                                }

                                if (srFile1.EndOfStream)
                                {
                                    break;
                                }

                                strLineIn1 = srFile1.ReadLine();
                                if (strLineIn1 == null)
                                    strLineIn1 = string.Empty;
                                else
                                    strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars);

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
                        // If file2 only has blank lines from here on out, then the files match; otherwise, they don't
                        // See if the remaining lines are blank
                        do
                        {
                            var strLineExtra = srFile2.ReadLine();
                            if (strLineExtra == null)
                                strLineExtra = string.Empty;
                            else
                                strLineExtra = strLineExtra.Trim(chWhiteSpaceChars);

                            if (strLineExtra.Length != 0)
                            {
                                if (!TextFilesMatchIgnoreLine(strLineExtra, lstLineIgnoreRegExSpecs))
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

        protected static bool TextFilesMatchIgnoreLine(string strText, List<Regex> lstLineIgnoreRegExSpecs)
        {

            if ((lstLineIgnoreRegExSpecs != null))
            {
                foreach (var matchSpec in lstLineIgnoreRegExSpecs)
                {
                    if (matchSpec == null)
                        continue;

                    if (matchSpec.Match(strText).Success)
                    {
                        // Line matches; ignore it
                        return true;
                    }
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
        /// <param name="strDataFilePath">Data file to check.</param>
        /// <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="strErrorMessage"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(string strDataFilePath, string strHashFilePath, out string strErrorMessage)
        {
            return ValidateFileVsHashcheck(strDataFilePath, strHashFilePath, out strErrorMessage, blnCheckDate: true, blnComputeHash: false, blnCheckSize: true);
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file
        /// If found, opens the file and reads the stored values: size, modification_date_utc, and hash
        /// Next compares the stored values to the actual values
        /// Checks file size, plus optionally date and hash
        /// </summary>
        /// <param name="strDataFilePath">Data file to check.</param>
        /// <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="strErrorMessage"></param>
        /// <param name="blnCheckDate">If True, then compares UTC modification time; times must agree within 2 seconds</param>
        /// <param name="blnComputeHash"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(
            string strDataFilePath, string strHashFilePath, out string strErrorMessage, 
            bool blnCheckDate, bool blnComputeHash)
        {
            return ValidateFileVsHashcheck(strDataFilePath, strHashFilePath, out strErrorMessage, blnCheckDate, blnComputeHash, blnCheckSize: true);
        }

        /// <summary>
        /// Looks for a .hashcheck file for the specified data file
        /// If found, opens the file and reads the stored values: size, modification_date_utc, and hash
        /// Next compares the stored values to the actual values
        /// </summary>
        /// <param name="strDataFilePath">Data file to check.</param>
        /// <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
        /// <param name="strErrorMessage"></param>
        /// <param name="blnCheckDate">If True, then compares UTC modification time; times must agree within 2 seconds</param>
        /// <param name="blnComputeHash"></param>
        /// <param name="blnCheckSize"></param>
        /// <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
        /// <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
        public static bool ValidateFileVsHashcheck(
            string strDataFilePath, string strHashFilePath, out string strErrorMessage, 
            bool blnCheckDate, bool blnComputeHash, bool blnCheckSize)
        {

            var blnValidFile = false;
            strErrorMessage = string.Empty;

            try
            {
                var fiDataFile = new FileInfo(strDataFilePath);

                if (string.IsNullOrEmpty(strHashFilePath))
                    strHashFilePath = fiDataFile.FullName + SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
                var fiHashCheck = new FileInfo(strHashFilePath);

                if (!fiDataFile.Exists)
                {
                    strErrorMessage = "Data file not found at " + fiDataFile.FullName;
                    return false;
                }

                if (!fiHashCheck.Exists)
                {
                    strErrorMessage = "Data file at " + fiDataFile.FullName + " does not have a corresponding .hashcheck file named " + fiHashCheck.Name;
                    return false;
                }

                long lngExpectedFileSizeBytes = 0;
                var dtExpectedFileDate = DateTime.MinValue;
                var strExpectedHash = string.Empty;

                // Read the details in the HashCheck file
                using (var srInfile = new StreamReader(new FileStream(fiHashCheck.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInfile.EndOfStream)
                    {
                        var strLineIn = srInfile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn) && !strLineIn.StartsWith("#") && strLineIn.Contains('='))
                        {
                            var strSplitLine = strLineIn.Split('=');


                            if (strSplitLine.Length >= 2)
                            {
                                // Set this to true for now
                               blnValidFile = true;

                                switch (strSplitLine[0].ToLower())
                                {
                                    case "size":
                                        long.TryParse(strSplitLine[1], out lngExpectedFileSizeBytes);
                                        break;
                                    case "modification_date_utc":
                                        DateTime.TryParse(strSplitLine[1], out dtExpectedFileDate);
                                        break;
                                    case "hash":
                                        strExpectedHash = string.Copy(strSplitLine[1]);
                                        break;
                                }
                            }
                        }

                    }
                }

                if (blnCheckSize && fiDataFile.Length != lngExpectedFileSizeBytes)
                {
                    strErrorMessage = "File size mismatch: expecting " + lngExpectedFileSizeBytes.ToString("#,##0") + " but computed " + fiDataFile.Length.ToString("#,##0");
                    return false;
                }

                // Only compare dates if we are not comparing hash values
                if (!blnComputeHash && blnCheckDate)
                {
                    if (Math.Abs(fiDataFile.LastWriteTimeUtc.Subtract(dtExpectedFileDate).TotalSeconds) > 2)
                    {
                        strErrorMessage = "File modification date mismatch: expecting " + dtExpectedFileDate.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + " UTC but actually " + fiDataFile.LastWriteTimeUtc.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) + " UTC";
                        return false;
                    }
                }

                if (blnComputeHash)
                {
                    // Compute the hash of the file
                    var strActualHash = ComputeFileHashMD5(strDataFilePath);

                    if (strActualHash != strExpectedHash)
                    {
                        strErrorMessage = "Hash mismatch: expecting " + strExpectedHash + " but computed " + strActualHash;
                        return false;
                    }
                }

                return blnValidFile;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in ValidateFileVsHashcheck: " + ex.Message);
            }

            return blnValidFile;

        }

        public static bool ValidateFreeDiskSpace(
            string directoryDescription, 
            string directoryPath, 
            int minFreeSpaceMB, 
            clsLogTools.LoggerTypes eLogLocationIfNotFound, 
            out string errorMessage)
        {

            double freeSpaceMB;

            errorMessage = string.Empty;

            var diDirectory = new DirectoryInfo(directoryPath);
            if (!diDirectory.Exists)
            {
                // Example error message: Organism DB directory not found: G:\DMS_Temp_Org
                errorMessage = directoryDescription + " not found: " + directoryPath;
                Console.WriteLine(errorMessage);
                LogError(errorMessage);
                return false;
            }

            if (diDirectory.Root.FullName.StartsWith(@"\\") || !diDirectory.Root.FullName.Contains(":"))
            {
                // Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll
                ulong freeBytesAvailableToUser;
                ulong lngTotalNumberOfBytes;
                ulong totalNumberOfFreeBytes;

                if (GetDiskFreeSpace(diDirectory.FullName, out freeBytesAvailableToUser, out lngTotalNumberOfBytes, out totalNumberOfFreeBytes))
                {
                    freeSpaceMB = BytesToMB((long)totalNumberOfFreeBytes);
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

            if (freeSpaceMB < minFreeSpaceMB)
            {
                // Example error message: Organism DB directory drive has less than 6858 MB free: 5794 MB
                errorMessage = $"{directoryDescription} drive has less than {minFreeSpaceMB} MB free: {(int)freeSpaceMB} MB";
                Console.WriteLine(errorMessage);
                LogError(errorMessage);
                return false;
            }

            return true;
        }

        #endregion

        #region "EventHandlers"

        private static void DbToolsErrorEventHandler(string errorMessage)
        {
            LogError(errorMessage);
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
            LogDebug(statusMessage, writeToLog: false);
        }

        private static void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private static void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        private static void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private static void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        #endregion
    }
}
