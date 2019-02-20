using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Class for reading parameter files with Key=Value settings (as used by MSGF+, MSPathFinder, and TopFD)
    /// </summary>
    public class clsKeyValueParamFileReader : EventNotifier
    {

        /// <summary>
        /// Most recent error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Parameter file path
        /// </summary>
        public string ParamFileName { get; }

        /// <summary>
        /// Parameter file path
        /// </summary>
        public string ParamFilePath { get; }

        /// <summary>
        /// Tool name (for logging)
        /// </summary>
        public string ToolName { get; }

        /// <summary>
        /// Constructor that takes tool name and parameter file path
        /// </summary>
        /// <param name="toolName">Tool name (for logging)</param>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <remarks>
        /// This constructor does not validate that paramFilePath is non-empty
        /// If you want validation, use the constructor that accepts working directory path and parameter file name
        /// </remarks>
        public clsKeyValueParamFileReader(string toolName, string paramFilePath) :
            this(toolName, Path.GetDirectoryName(paramFilePath), Path.GetFileName(paramFilePath))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="toolName">Tool name (for logging)</param>
        /// <param name="workDirPath">Directory with the parameter file</param>
        /// <param name="paramFileName">Parameter file name</param>
        /// <remarks>Parameter file name and working directory path will be validated in ParseKeyValueParameterFile</remarks>
        public clsKeyValueParamFileReader(string toolName, string workDirPath, string paramFileName)
        {
            ErrorMessage = string.Empty;
            ToolName = toolName;
            ParamFileName = paramFileName;
            ParamFilePath = Path.Combine(workDirPath, paramFileName);
        }

        /// <summary>
        /// Convert the parameter info into a command line
        /// </summary>
        /// <param name="paramFileEntries">Parameter names and values read from tool's parameter file</param>
        /// <param name="paramToArgMapping">Dictionary mapping parameter names to argument names</param>
        /// <param name="paramNamesToSkip">Parameter names in paramFileEntries to skip</param>
        /// <param name="argumentPrefix">Argument prefix; typically -- or -</param>
        /// <returns>String with command line arguments</returns>
        /// <remarks>Returns an empty string if multiple parameters resolve to the same argument name</remarks>
        public string ConvertParamsToArgs(
            List<KeyValuePair<string, string>> paramFileEntries,
            Dictionary<string, string> paramToArgMapping,
            SortedSet<string> paramNamesToSkip,
            string argumentPrefix)
        {

            var sbOptions = new StringBuilder(500);

            try
            {
                // Keep track of the arguments already appended (use case-sensitive matching)
                var argumentsAppended = new SortedSet<string>(StringComparer.Ordinal);

                foreach (var kvSetting in paramFileEntries)
                {
                    if (paramNamesToSkip != null && paramNamesToSkip.Contains(kvSetting.Key))
                        continue;

                    // Check whether kvSetting.key is one of the standard keys defined in paramToArgMapping
                    if (paramToArgMapping.TryGetValue(kvSetting.Key, out var argumentName))
                    {
                        if (argumentsAppended.Contains(argumentName))
                        {
                            var errMsg = string.Format("Duplicate argument {0} specified for parameter {1} in the {2} parameter file",
                                                       argumentName, kvSetting.Key, ToolName);
                            LogError(errMsg);
                            return string.Empty;
                        }

                        argumentsAppended.Add(argumentName);

                        sbOptions.Append(" " + argumentPrefix + argumentName + " " + kvSetting.Value);
                    }
                    else
                    {
                        OnWarningEvent(string.Format("Ignoring unknown setting {0} from parameter file {1}",
                                                     kvSetting.Key, Path.GetFileName(ParamFilePath)));
                    }
                }

            }
            catch (Exception ex)
            {
                var errMsg = string.Format("Exception converting parameters loaded from the {0} parameter file into command line arguments",
                                           ToolName);
                LogError(errMsg, ex);
                return string.Empty;
            }

            return sbOptions.ToString();
        }

        private static IEnumerable<KeyValuePair<string, string>> GetKeyValueParameters(IEnumerable<KeyValueParamFileLine> paramFileLines)
        {
            var paramFileEntries = from paramFileLine in paramFileLines
                                   where !string.IsNullOrWhiteSpace(paramFileLine.ParamName)
                                   select new KeyValuePair<string, string>(paramFileLine.ParamName, paramFileLine.ParamValue);
            return paramFileEntries;
        }

        private void LogError(string errorMessage)
        {
            ErrorMessage = errorMessage;
            OnErrorEvent(errorMessage);
        }

        private void LogError(string errorMessage, Exception ex)
        {
            ErrorMessage = errorMessage;
            OnErrorEvent(errorMessage, ex);
        }


        /// <summary>
        /// Returns true if paramFileEntries contains parameter paramName and the parameter's value is True or a positive integer
        /// </summary>
        /// <param name="paramFileEntries"></param>
        /// <param name="paramName"></param>
        /// <param name="caseSensitiveParamName">When true, require a case-sensitive match to the parameter names in paramFileEntries</param>
        /// <returns></returns>
        public bool ParamIsEnabled(List<KeyValuePair<string, string>> paramFileEntries, string paramName, bool caseSensitiveParamName = false)
        {
            var stringComp = caseSensitiveParamName ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

            foreach (var kvSetting in paramFileEntries)
            {
                if (!string.Equals(kvSetting.Key, paramName, stringComp))
                    continue;

                if (bool.TryParse(kvSetting.Value, out var boolValue))
                {
                    return boolValue;
                }

                if (int.TryParse(kvSetting.Value, out var intValue) && intValue > 0)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Read a parameter file with Key=Value settings (as used by MSGF+, MSPathFinder, and TopFD)
        /// </summary>
        /// <param name="paramFileEntries">Output: Dictionary of setting names and values read from the parameter file</param>
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS if success; error code if an error</returns>
        /// <remarks>Values in paramFileEntries will include any comment text</remarks>
        public CloseOutType ParseKeyValueParameterFile(out List<KeyValuePair<string, string>> paramFileEntries)
        {

            var result = ParseKeyValueParameterFileWork(out var paramFileLines);
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                paramFileEntries = new List<KeyValuePair<string, string>>();
                return result;
            }

            paramFileEntries = GetKeyValueParameters(paramFileLines).ToList();

            if (paramFileEntries.Count > 0)
                return CloseOutType.CLOSEOUT_SUCCESS;

            LogError(string.Format("{0} parameter file has no valid Key=Value settings", ToolName));
            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Read a parameter file with Key=Value settings (as used by MSGF+, MSPathFinder, and TopFD)
        /// </summary>
        /// <param name="paramFileLines">Output: contents of the parameter file</param>
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS if success; error code if an error</returns>
        /// <remarks>Values in paramFileLines will include any comment text</remarks>
        public CloseOutType ParseKeyValueParameterFileGetAllLines(out List<KeyValueParamFileLine> paramFileLines)
        {
            var result = ParseKeyValueParameterFileWork(out paramFileLines);

            if (GetKeyValueParameters(paramFileLines).Any())
                return result;

            LogError(string.Format("{0} parameter file has no valid Key=Value settings", ToolName));
            return CloseOutType.CLOSEOUT_FAILED;
        }

        private CloseOutType ParseKeyValueParameterFileWork(out List<KeyValueParamFileLine> paramFileLines)
        {

            paramFileLines = new List<KeyValueParamFileLine>();

            if (string.IsNullOrWhiteSpace(ParamFileName))
            {
                LogError(ToolName + " parameter file not defined in the job settings");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (!File.Exists(ParamFilePath))
            {
                LogError(ToolName + " parameter file not found: " + ParamFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            try
            {
                using (var paramFileReader = new StreamReader(new FileStream(ParamFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var lineNumber = 0;
                    while (!paramFileReader.EndOfStream)
                    {
                        var dataLine = paramFileReader.ReadLine();
                        lineNumber++;

                        var paramFileLine = new KeyValueParamFileLine(lineNumber, dataLine);

                        var kvSetting = clsGlobal.GetKeyValueSetting(dataLine);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key))
                        {
                            paramFileLine.StoreParameter(kvSetting);
                        }

                        paramFileLines.Add(paramFileLine);
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                var errMsg = string.Format("Exception reading {0} parameter file {1} in ParseKeyValueParameterFileWork",
                                           ToolName, Path.GetFileName(ParamFilePath));
                LogError(errMsg, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

    }
}
