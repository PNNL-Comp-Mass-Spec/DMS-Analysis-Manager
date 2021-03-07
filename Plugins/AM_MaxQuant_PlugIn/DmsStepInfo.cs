namespace AnalysisManagerMaxQuantPlugIn
{
    // Ignore Spelling: MaxQuant

    internal class DmsStepInfo
    {
        /// <summary>
        /// Step ID
        /// </summary>
        public int ID { get; }

        /// <summary>
        /// Step Tool name
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Start step name
        /// </summary>
        /// <remarks>
        /// Corresponds to one of descriptions shown by a dry run of MaxQuant
        /// </remarks>
        public string StartStepName { get; set; }

        /// <summary>
        /// Start step ID
        /// </summary>
        /// <remarks>
        /// Null if startStepID is "auto" in the parameter file
        /// Otherwise, an integer
        /// </remarks>
        public int? StartStepID { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        public DmsStepInfo(int id)
        {
            ID = id;
            Tool = string.Empty;
            StartStepName = string.Empty;
        }
    }
}
