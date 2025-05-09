namespace AnalysisManagerDiaNNPlugIn
{
    public class MassErrorInfo
    {
        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName;

        /// <summary>
        /// Analysis job number
        /// </summary>
        public int PSMJob;

        /// <summary>
        /// Parent ion median mass error, before correction
        /// </summary>
        public double MassErrorPPM;

        /// <summary>
        /// Parent ion median mass error, after correction
        /// </summary>
        public double MassErrorPPMCorrected;

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
