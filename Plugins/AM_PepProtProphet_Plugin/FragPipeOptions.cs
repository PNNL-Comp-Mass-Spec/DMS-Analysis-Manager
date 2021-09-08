using System.IO;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSFraggerPlugIn;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
    internal class FragPipeOptions : EventNotifier
    {
        // Ignore Spelling: quantitation

        private readonly IJobParams mJobParams;

        /// <summary>
        /// Number of datasets in the data package for this job
        /// </summary>
        /// <remarks>If no data package, this will be 1</remarks>
        public int DatasetCount { get; }

        public MSFraggerOptions FraggerOptions { get; }

        /// <summary>
        /// Path to java.exe
        /// </summary>
        public string JavaProgLoc { get; set; }

        public FragPipeLibFinder LibraryFinder { get; set; }

        /// <summary>
        /// Whether to use match-between runs with running IonQuant
        /// </summary>
        /// <remarks>Defaults to true, but ignored if RunIonQuant is false</remarks>
        public bool MatchBetweenRuns { get; set; }

        /// <summary>
        /// Whether to run PeptideProphet, Percolator, or nothing
        /// </summary>
        /// <remarks>Defaults to PeptideProphet, but auto-set to Percolator if MatchBetweenRuns is true or TMT is in use</remarks>
        public MS1ValidationModes MS1ValidationMode => FraggerOptions.MS1ValidationMode;

        /// <summary>
        /// True if the MSFragger parameter file has open search based tolerances
        /// </summary>
        public bool OpenSearch => FraggerOptions.OpenSearch;

        /// <summary>
        /// Reporter ion mode defined in the parameter file
        /// </summary>
        public ReporterIonModes ReporterIonMode => FraggerOptions.ReporterIonMode;

        /// <summary>
        /// Whether or not to run Abacus
        /// </summary>
        /// <remarks>Defaults to true, but is ignored if we only have a single experiment group (or no experiment groups)</remarks>
        public bool RunAbacus { get; set; }

        /// <summary>
        /// Whether or not to run FreeQuant
        /// </summary>
        /// <remarks>
        /// Defaults to false, but forced to true if reporter ions are used
        /// If no reporter ions, RunFreeQuant is ignored if RunIonQuant is enabled</remarks>
        public bool RunFreeQuant => FraggerOptions.RunFreeQuant;

        /// <summary>
        /// Whether to run IonQuant for MS1-based quantitation
        /// </summary>
        /// <remarks>
        /// Auto-set to true if this job has multiple datasets (as defined in a data package)
        /// Also set to true if job parameter RunIonQuant is defined
        /// However, set to false if job parameter MS1QuantDisabled is defined
        /// </remarks>
        public bool RunIonQuant => FraggerOptions.RunIonQuant;

        /// <summary>
        /// Whether to run PTM-Shepherd
        /// </summary>
        /// <remarks>Defaults to true, but is ignored if OpenSearch is false</remarks>
        public bool RunPTMShepherd { get; set; }

        /// <summary>
        /// Pad width to use when logging calls to external programs
        /// </summary>
        /// <remarks>
        /// Based on the longest directory path in the working directories for experiment groups
        /// </remarks>
        public int WorkingDirectoryPadWidth { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>
        /// philosopherExe can be null if the LibraryFinder property will not be accessed by the calling method
        /// </remarks>
        /// <param name="jobParams"></param>
        /// <param name="philosopherExe">Path to philosopher.exe</param>
        /// <param name="datasetCount"></param>
        public FragPipeOptions(IJobParams jobParams, FileInfo philosopherExe, int datasetCount)
        {
            mJobParams = jobParams;

            FraggerOptions = new MSFraggerOptions(jobParams);
            RegisterEvents(FraggerOptions);

            DatasetCount = datasetCount;

            JavaProgLoc = string.Empty;

            LibraryFinder = new FragPipeLibFinder(philosopherExe);
            RegisterEvents(LibraryFinder);

            MatchBetweenRuns = mJobParams.GetJobParameter("MatchBetweenRuns", false);

            var runPeptideProphetJobParam = mJobParams.GetJobParameter("RunPeptideProphet", string.Empty);
            var runPercolatorJobParam = mJobParams.GetJobParameter("RunPercolator", string.Empty);

            if (FraggerOptions.IsUndefinedOrAuto(runPeptideProphetJobParam) && FraggerOptions.IsUndefinedOrAuto(runPercolatorJobParam))
            {
                // Use Percolator if match-between runs is enabled, otherwise use PeptideProphet
                // This value will get changed by LoadMSFraggerOptions if using an open search or if TMT is defined as a dynamic or static mod
                FraggerOptions.MS1ValidationMode = MatchBetweenRuns ? MS1ValidationModes.Percolator : MS1ValidationModes.PeptideProphet;

                FraggerOptions.MS1ValidationModeAutoDefined = true;
            }
            else
            {
                var runPeptideProphet = !string.IsNullOrWhiteSpace(runPeptideProphetJobParam) && bool.Parse(runPeptideProphetJobParam);

                var runPercolator = !string.IsNullOrWhiteSpace(runPercolatorJobParam) && bool.Parse(runPercolatorJobParam);

                if (runPercolator)
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.Percolator;
                }
                else if (runPeptideProphet)
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                }
                else
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.Disabled;
                }

                FraggerOptions.MS1ValidationModeAutoDefined = false;
            }

            RunAbacus = mJobParams.GetJobParameter("RunAbacus", true);

            var ms1QuantDisabled = mJobParams.GetJobParameter("MS1QuantDisabled", false);

            if (ms1QuantDisabled)
            {
                FraggerOptions.RunFreeQuant = false;
                FraggerOptions.RunIonQuant = false;
            }
            else
            {
                var runFreeQuantJobParam = mJobParams.GetJobParameter("RunFreeQuant", string.Empty);
                var runIonQuantJobParam = mJobParams.GetJobParameter("RunIonQuant", string.Empty);

                if (datasetCount > 1)
                {
                    // Multi-dataset job

                    if (MatchBetweenRuns)
                    {
                        // Run IonQuant since match-between runs is enabled
                        FraggerOptions.RunFreeQuant = false;
                        FraggerOptions.RunIonQuant = true;
                        FraggerOptions.QuantModeAutoDefined = false;
                    }
                    else
                    {
                        SetMS1QuantOptions(runFreeQuantJobParam, runIonQuantJobParam);
                    }

                    // After loading the MSFragger parameter file, if the mods include TMT or iTRAQ, FreeQuant will be auto-enabled
                }
                else
                {
                    // Single dataset job
                    // Only enable MS1 quantitation if RunFreeQuant or RunIonQuant is defined as a job parameter

                    SetMS1QuantOptions(runFreeQuantJobParam, runIonQuantJobParam);
                }
            }

            RunPTMShepherd = mJobParams.GetJobParameter("RunPTMShepherd", true);
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath"></param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadMSFraggerOptions(string paramFilePath)
        {
            return FraggerOptions.LoadMSFraggerOptions(paramFilePath);
        }

        private void SetMS1QuantOptions(string runFreeQuantJobParam, string runIonQuantJobParam)
        {
            if (FraggerOptions.IsUndefinedOrAuto(runFreeQuantJobParam) && FraggerOptions.IsUndefinedOrAuto(runIonQuantJobParam))
            {
                FraggerOptions.RunFreeQuant = false;
                FraggerOptions.RunIonQuant = false;
                FraggerOptions.QuantModeAutoDefined = true;
                return;
            }

            // ReSharper disable once SimplifyConditionalTernaryExpression
            var runFreeQuant = FraggerOptions.IsUndefinedOrAuto(runFreeQuantJobParam)
                ? false
                : mJobParams.GetJobParameter("RunFreeQuant", false);

            if (runFreeQuant)
            {
                FraggerOptions.RunFreeQuant = true;
                FraggerOptions.RunIonQuant = false;
            }
            else
            {
                FraggerOptions.RunFreeQuant = false;

                // ReSharper disable once SimplifyConditionalTernaryExpression
                FraggerOptions.RunIonQuant = FraggerOptions.IsUndefinedOrAuto(runIonQuantJobParam)
                    ? false
                    : mJobParams.GetJobParameter("RunIonQuant", false);
            }

            FraggerOptions.QuantModeAutoDefined = false;
        }
    }
}
