﻿
namespace AnalysisManagerMzRefineryPlugIn
{
    public class MassErrorInfo
    {
        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName;

        /// <summary>
        /// Analysis Job number
        /// </summary>
        public int PSMJob;

        /// <summary>
        /// Parent ion median mass error, before refinement
        /// </summary>
        public double MassErrorPPM;

        /// <summary>
        /// Parent ion median mass error, after refinement
        /// </summary>
        public double MassErrorPPMRefined;

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
            MassErrorPPMRefined = double.MinValue;
        }
    }
}
