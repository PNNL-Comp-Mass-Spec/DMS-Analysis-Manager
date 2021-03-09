namespace AnalysisManagerMaxQuantPlugIn
{
    internal class MaxQuantRuntimeOptions
    {
        public const int MAX_STEP_NUMBER = 9999;

        /// <summary>
        /// True if a dry run should be used
        /// </summary>
        public bool DryRun { get; set; }

        /// <summary>
        /// Local parameter file path
        /// </summary>
        public string ParameterFilePath { get; set; }

        /// <summary>
        /// Start step number
        /// </summary>
        public int StartStepNumber { get; set; }

        /// <summary>
        /// End step number
        /// </summary>
        public int EndStepNumber { get; set; }

        /// <summary>
        /// True if a step range is defined
        /// </summary>
        public bool StepRangeDefined => StartStepNumber > 0 || EndStepNumber < MAX_STEP_NUMBER;

        /// <summary>
        /// True once the step range has been validated
        /// </summary>
        public bool StepRangeValidated { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MaxQuantRuntimeOptions()
        {
            ParameterFilePath = string.Empty;
            StartStepNumber = 0;
            EndStepNumber = MAX_STEP_NUMBER;
        }
    }
}
