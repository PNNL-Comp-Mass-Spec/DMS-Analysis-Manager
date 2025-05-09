namespace AnalysisManagerDiaNNPlugIn
{
    public class MassErrorInfo
    {
        // Ignore Spelling: Dia

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; set; }

        /// <summary>
        /// Analysis job number
        /// </summary>
        public int PSMJob { get; set; }

        /// <summary>
        /// Parent ion median mass error, before correction
        /// </summary>
        public double MassErrorPPM { get; set; }

        /// <summary>
        /// Parent ion median mass error, after correction
        /// </summary>
        public double MassErrorPPMCorrected { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MassErrorInfo()
        {
            Clear();
        }

        public void Clear()
        {
            DatasetName = string.Empty;
            PSMJob = 0;
            MassErrorPPM = double.MinValue;
            MassErrorPPMCorrected = double.MinValue;
        }
    }
}
