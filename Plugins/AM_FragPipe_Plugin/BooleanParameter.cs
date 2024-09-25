namespace AnalysisManagerFragPipePlugin
{
    internal class BooleanParameter
    {
        // Ignore Spelling: Frag

        /// <summary>
        /// True if the parameter has a value
        /// </summary>
        public bool IsDefined { get; set; }

        /// <summary>
        /// True if the parameter is required, false if optional
        /// </summary>
        public bool Required { get; }

        /// <summary>
        /// Parameter name
        /// </summary>
        public string ParameterName { get; }

        /// <summary>
        /// Parameter value
        /// </summary>
        public bool ParameterValue { get; private set; }

        /// <summary>
        /// Constructor that accepts a value
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="parameterValue"></param>
        /// <param name="required"></param>
        // ReSharper disable once UnusedMember.Global
        public BooleanParameter(string parameterName, bool parameterValue, bool required = true)
        {
            ParameterName = parameterName;
            Required = required;
            SetValue(parameterValue);
        }

         /// <summary>
        /// Set the value for this parameter
        /// </summary>
        /// <remarks>Sets IsDefined to true</remarks>
        /// <param name="parameterValue"></param>
        public void SetValue(bool parameterValue)
        {
            ParameterValue = parameterValue;
            IsDefined = true;
        }
    }
}
