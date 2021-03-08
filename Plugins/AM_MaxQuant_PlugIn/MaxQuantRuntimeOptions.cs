namespace AnalysisManagerMaxQuantPlugIn
{
    internal class MaxQuantRuntimeOptions
    {
        public bool DryRun { get; set; }
        public string ParameterFilePath { get; set; }
        public int StartStepNumber { get; set; }
        public int EndStepNumber { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MaxQuantRuntimeOptions()
        {
            ParameterFilePath = string.Empty;
            StartStepNumber = 0;
            EndStepNumber = 9999;
        }
    }
}
