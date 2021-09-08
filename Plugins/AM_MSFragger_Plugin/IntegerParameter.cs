namespace AnalysisManagerMSFraggerPlugIn
{
    internal class IntegerParameter
    {
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
        /// <param name="parameterName"></param>
        /// <param name="parameterValue"></param>
        // ReSharper disable once UnusedMember.Global
        public IntegerParameter(string parameterName, int parameterValue)
        {
            ParameterName = parameterName;
            SetValue(parameterValue);
        }

        /// <summary>
        /// Constructor that accepts the range of allowed values
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="minAllowedValue"></param>
        /// <param name="maxAllowedValue"></param>
        public IntegerParameter(string parameterName, int? minAllowedValue, int? maxAllowedValue)
        {
            ParameterName = parameterName;
            MinValue = minAllowedValue;
            MaxValue = maxAllowedValue;
        }

        /// <summary>
        /// Set the value for this parameter
        /// </summary>
        /// <remarks>Sets IsDefined to true</remarks>
        /// <param name="parameterValue"></param>
        public void SetValue(int parameterValue)
        {
            ParameterValue = parameterValue;
            IsDefined = true;
        }
    }
}
