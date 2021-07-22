using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPepProtProphetPlugIn
{
    public enum ReporterIonModes
    {
        Disabled = 0,
        Itraq4 = 1,
        Itraq8 = 2,
        Tmt6 = 3,
        Tmt10 = 4,
        Tmt11 = 5,
        Tmt16 = 6
    }

    internal class MSFraggerOptions
    {
        // Ignore Spelling: quantitation

        /// <summary>
        /// Path to java.exe
        /// </summary>
        public string JavaProgLoc { get; set; }

        /// <summary>
        /// Whether to use match-between runs with running IonQuant
        /// </summary>
        /// <remarks>Defaults to true, but ignored if RunIonQuant is false</remarks>
        public bool MatchBetweenRuns { get; set; }

        /// <summary>
        /// True if the MSFragger parameter file has open search based tolerances
        /// </summary>
        public bool OpenSearch { get; set; }

        /// <summary>
        /// Reporter ion mode defined in the parameter file
        /// </summary>
        public ReporterIonModes ReporterIonMode { get; set; }

        /// <summary>
        /// Whether or not to run Abacus
        /// </summary>
        /// <remarks>Defaults to true, but is ignored if we only have a single experiment group (or no experiment groups)</remarks>
        public bool RunAbacus { get; set; }

        /// <summary>
        /// Whether or not to run FreeQuant
        /// </summary>
        /// <remarks>Defaults to false; ignored if RunIonQuant is enabled</remarks>
        public bool RunFreeQuant { get; set; }

        /// <summary>
        /// Whether to run IonQuant for MS1-based quantitation
        /// </summary>
        /// <remarks>
        /// Auto-set to true if this job has multiple datasets (as defined in a data package)
        /// Also set to true if job parameter RunIonQuant is defined
        /// However, set to false if job parameter MS1QuantDisabled is defined
        /// </remarks>
        public bool RunIonQuant { get; set; }

        /// <summary>
        /// Whether to run PTM-Shepherd
        /// </summary>
        /// <remarks>Defaults to true, but is ignored if OpenSearch is false</remarks>
        public bool RunPTMShepherd { get; set; }

        public MSFraggerOptions(IJobParams jobParams, int datasetCount)
        {
            MatchBetweenRuns = jobParams.GetJobParameter("MatchBetweenRuns", true);

            RunAbacus = jobParams.GetJobParameter("RunAbacus", true);

            var ms1QuantDisabled = jobParams.GetJobParameter("MS1QuantDisabled", false);

            if (ms1QuantDisabled)
            {
                RunFreeQuant = false;
                RunIonQuant = false;
            }
            else
            {
                var runFreeQuant= jobParams.GetJobParameter("RunFreeQuant", false);

                if (datasetCount > 1)
                {
                    // Multi-dataset job
                    // Preferably run IonQuant, but use FreeQuant if RunFreeQuant is true

                    RunFreeQuant = runFreeQuant;
                    RunIonQuant = !runFreeQuant;
                }
                else
                {
                    // Single dataset job
                    // Only enable MS1 quantitation if RunFreeQuant or RunIonQuant is defined
                    if (runFreeQuant)
                    {
                        RunFreeQuant = true;
                        RunIonQuant = false;
                    }
                    else
                    {
                        RunFreeQuant = false;
                        RunIonQuant = jobParams.GetJobParameter("RunIonQuant", false);
                    }
                }
            }

            RunPTMShepherd = jobParams.GetJobParameter("RunPTMShepherd", true);
        }
    }
}
