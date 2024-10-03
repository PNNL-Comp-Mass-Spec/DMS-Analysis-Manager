namespace AnalysisManagerFragPipePlugIn
{
    internal class IntegerParameter
    {
        // Ignore Spelling: Frag

        /// <summary>
        /// True if the parameter has a value
        /// </summary>
        public bool IsDefined { get; set; }

        /// <summary>
        /// Maximum allowed value
        /// </summary>
        /// <remarks>Not applicable if null</remarks>
        public int? MaxValue{ get; }

        /// <summary>
        /// Minimum allowed value
        /// </summary>
        /// <remarks>Not applicable if null</remarks>
        public int? MinValue { get; }

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
        public int ParameterValue { get; private set; }

        /// <summary>
        /// Constructor that accepts a value
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterValue">Parameter value</param>
        /// <param name="required">If true, the parameter is required</param>
        // ReSharper disable once UnusedMember.Global
        public IntegerParameter(string parameterName, int parameterValue, bool required = true)
        {
            ParameterName = parameterName;
            Required = required;
            SetValue(parameterValue);
        }

        /// <summary>
        /// Constructor that accepts a range of allowed values
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="minAllowedValue">Minimum allowed value</param>
        /// <param name="maxAllowedValue">Maximum allowed value</param>
        /// <param name="required">If true, the parameter is required</param>
        public IntegerParameter(string parameterName, int? minAllowedValue, int? maxAllowedValue, bool required = true)
        {
            ParameterName = parameterName;
            MinValue = minAllowedValue;
            MaxValue = maxAllowedValue;
            Required = required;
        }

        /// <summary>
        /// Set the value for this parameter
        /// </summary>
        /// <remarks>Sets IsDefined to true</remarks>
        /// <param name="parameterValue">Parameter value</param>
        public void SetValue(int parameterValue)
        {
            ParameterValue = parameterValue;
            IsDefined = true;
        }
    }
}
