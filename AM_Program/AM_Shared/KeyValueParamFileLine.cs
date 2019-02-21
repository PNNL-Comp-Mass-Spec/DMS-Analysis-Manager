using System.Collections.Generic;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Line of data from a Key=Value parameter file
    /// The data line optionally has a parameter name and value
    /// </summary>
    public class KeyValueParamFileLine
    {

        /// <summary>
        /// Line number in the parameter file
        /// </summary>
        public int LineNumber { get; }

        /// <summary>
        /// Text of the line from the parameter file (including the comment, if any)
        /// </summary>
        public string Text { get; protected set; }

        /// <summary>
        /// Parameter name if this line contains a parameter, otherwise an empty string
        /// </summary>
        public string ParamName { get; private set; }

        /// <summary>
        /// Parameter value if this line contains a parameter, otherwise an empty string
        /// </summary>
        public string ParamValue { get; private set; }

        /// <summary>
        /// True if ParamName has text, otherwise false
        /// </summary>
        /// <remarks>If ParamName has text but ParamValue is empty, this still returns true</remarks>
        public bool HasParameter => !string.IsNullOrWhiteSpace(ParamName);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lineNumber"></param>
        /// <param name="lineText"></param>
        public KeyValueParamFileLine(int lineNumber, string lineText)
        {
            LineNumber = lineNumber;
            Text = lineText;
            ParamName = string.Empty;
            ParamValue = string.Empty;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramFileLine"></param>
        public KeyValueParamFileLine(KeyValueParamFileLine paramFileLine) : this(paramFileLine.LineNumber, paramFileLine.Text)
        {
            ParamName = paramFileLine.ParamName;
            ParamValue = paramFileLine.ParamValue;
        }

        /// <summary>
        /// Associate a parameter with this data line
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        public void StoreParameter(string paramName, string paramValue)
        {
            ParamName = paramName;
            ParamValue = paramValue;
        }

        /// <summary>
        /// Associate a parameter with this data line
        /// </summary>
        /// <param name="paramInfo"></param>
        public void StoreParameter(KeyValuePair<string, string> paramInfo)
        {
            ParamName = paramInfo.Key;
            ParamValue = paramInfo.Value;
        }

        /// <summary>
        /// Update the value for this parameter
        /// </summary>
        /// <param name="value"></param>
        protected void UpdateValue(string value)
        {
            ParamValue = value ?? string.Empty;
        }

        /// <summary>
        /// Return the text of the line from the parameter file (including the comment, if any)
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Text;
        }
    }
}
