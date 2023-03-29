namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Analysis job parameter container
    /// </summary>
    public class JobParameter
    {
        /// <summary>
        /// Section name
        /// </summary>
        public string Section { get; }

        /// <summary>
        /// Parameter name
        /// </summary>
        public string ParamName { get; }

        /// <summary>
        /// Parameter value
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="section"></param>
        /// <param name="name"></param>
        /// <param name="value"></param>
        public JobParameter(string section, string name, string value)
        {
            Section = section;
            ParamName = name;
            Value = value;
        }

        /// <summary>
        /// Show the parameter name and value
        /// </summary>
        public override string ToString()
        {
            return ParamName + ": " + Value;
        }
    }
}
