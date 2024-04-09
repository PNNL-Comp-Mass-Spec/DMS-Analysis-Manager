using System.IO;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSFraggerPlugIn;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
    internal class FragPipeOptions : EventNotifier
    {
        // Ignore Spelling: Frag, Fragger, Loc, Prog, Prot, quantitation, Quant

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
        /// When true, use match-between runs when running IonQuant
        /// </summary>
        /// <remarks>Defaults to false; use a settings file to set to true</remarks>
        public bool MatchBetweenRuns { get; set; }

        /// <summary>
        /// Whether to run PeptideProphet, Percolator, or nothing
        /// </summary>
        /// <remarks>
        /// FragPipe v16 defaulted to PeptideProphet for closed searches
        /// FragPipe v17 defaults to Percolator for closed searches
        /// If using iTRAQ or running an open search, default to PeptideProphet
        /// </remarks>
        public MS1ValidationModes MS1ValidationMode => FraggerOptions.MS1ValidationMode;

        /// <summary>
        /// True if the MSFragger parameter file has open search based tolerances
        /// </summary>
        public bool OpenSearch => FraggerOptions.OpenSearch;

        /// <summary>
        /// Reporter ion mode defined in the parameter file
        /// </summary>
        /// <remarks>
        /// In the PepProtProphet plugIn, method UpdateReporterIonModeIfRequired will change this to TMT18 if the data package has experiments with TMT 18 labeling
        /// </remarks>
        public ReporterIonModes ReporterIonMode
        {
            get => FraggerOptions.ReporterIonMode;
            set => FraggerOptions.ReporterIonMode = value;
        }

        /// <summary>
        /// When true, run Abacus
        /// </summary>
        /// <remarks>
        /// <para>Is set to true if job parameters GeneratePeptideLevelSummary or GenerateProteinLevelSummary are true</para>
        /// <para>Ignored if we only have a single experiment group (or no experiment groups)</para>
        /// <para>FragPipe v19 and v20 do not run Abacus; this may or may not be correct behavior</para>
        /// </remarks>
        public bool RunAbacus { get; set; }

        /// <summary>
        /// When true, run FreeQuant
        /// </summary>
        /// <remarks>
        /// Defaults to false, but forced to true if reporter ions are used
        /// If no reporter ions, RunFreeQuant is ignored if RunIonQuant is enabled</remarks>
        public bool RunFreeQuant => FraggerOptions.RunFreeQuant;

        /// <summary>
        /// When true, run IonQuant for MS1-based quantitation
        /// </summary>
        /// <remarks>
        /// Auto-set to true if this job has multiple datasets (as defined in a data package)
        /// Also set to true if job parameter RunIonQuant is defined
        /// However, set to false if job parameter MS1QuantDisabled is defined
        /// </remarks>
        public bool RunIonQuant => FraggerOptions.RunIonQuant;

        /// <summary>
        /// When true, run LabelQuant to quantify reporter ions; ignored if <see cref="ReporterIonMode"/> is ReporterIonModes.Disabled
        /// </summary>
        /// <remarks>
        /// If this is false, will also not run TMT-Integrator
        /// </remarks>
        public bool RunLabelQuant { get; set; }

        /// <summary>
        /// When true, run iProphet
        /// </summary>
        /// <remarks>
        /// <para>Is set to true if job parameter GeneratePeptideLevelSummary true</para>
        /// <para>Ignored if we only have a single experiment group (or no experiment groups)</para>
        /// <para>FragPipe v19 and v20 do not run iProphet when "Generate peptide-level summary" is checked; this may or may not be correct behavior</para>
        /// </remarks>
        public bool RunIProphet { get; set; }

        /// <summary>
        /// When true, run MSBooster
        /// </summary>
        /// <remarks>When MSBooster is used, Percolator must be used (and Peptide Prophet should not be used)</remarks>
        public bool RunMSBooster { get; set; }

        /// <summary>
        /// When true, run Protein Prophet (if Peptide Prophet was used)
        /// </summary>
        /// <remarks>
        /// Defaults to true
        /// </remarks>
        public bool RunProteinProphet { get; set; }

        /// <summary>
        /// When true, run PTM Prophet
        /// </summary>
        /// <remarks>
        /// Defaults to false, but auto-set to true if a TMT search that also looks for phospho STY
        /// Also auto-set to true if running an Open Search
        /// Not set to true if job parameter RunPTMProphet is false
        /// </remarks>
        public bool RunPTMProphet { get; set; }

        /// <summary>
        /// When true, run PTM-Shepherd
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
        /// <param name="jobParams">Job parameters</param>
        /// <param name="philosopherExe">Path to philosopher.exe</param>
        /// <param name="datasetCount">Dataset count</param>
        public FragPipeOptions(IJobParams jobParams, FileInfo philosopherExe, int datasetCount)
        {
            FraggerOptions = new MSFraggerOptions(jobParams);
            RegisterEvents(FraggerOptions);

            DatasetCount = datasetCount;

            JavaProgLoc = string.Empty;

            LibraryFinder = new FragPipeLibFinder(philosopherExe);
            RegisterEvents(LibraryFinder);

            MatchBetweenRuns = jobParams.GetJobParameter("MatchBetweenRuns", false);

            // Class AnalysisToolRunnerPepProtProphet will set this to false if the MSFragger parameter file includes iTRAQ or TMT16 (FragPipe v19 supports MSBooster for TMT10)
            // Additionally, MSBooster is set to false if running an open search
            RunMSBooster = FraggerOptions.GetParameterValueOrDefault("RunMSBooster", true);

            var runPeptideProphetJobParam = jobParams.GetJobParameter("RunPeptideProphet", string.Empty);
            var runPercolatorJobParam = jobParams.GetJobParameter("RunPercolator", string.Empty);

            var databaseSplitCount = jobParams.GetJobParameter("MSFragger", "DatabaseSplitCount", 1);

            if (FraggerOptions.IsUndefinedOrAuto(runPeptideProphetJobParam) && FraggerOptions.IsUndefinedOrAuto(runPercolatorJobParam))
            {
                // Use Percolator by default, unless databaseSplitCount is more than 1
                //   FragPipe v16 defaulted to PeptideProphet for closed searches
                //   FragPipe v17 defaults to Percolator for closed searches

                // After loading the MSFragger parameter file with LoadMSFraggerOptions, if the mods include iTRAQ or if running an open search, this will be changed to PeptideProphet
                FraggerOptions.MS1ValidationMode = databaseSplitCount == 1
                    ? MS1ValidationModes.Percolator
                    : MS1ValidationModes.PeptideProphet;

                FraggerOptions.MS1ValidationModeAutoDefined = true;
            }
            else
            {
                var runPeptideProphet = FraggerOptions.GetParameterValueOrDefault("RunPeptideProphet", true);

                var runPercolator = FraggerOptions.GetParameterValueOrDefault("RunPercolator", false);

                if (runPercolator && databaseSplitCount <= 1)
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.Percolator;
                }
                else if (runPeptideProphet || runPercolator && databaseSplitCount > 1)
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                }
                else
                {
                    FraggerOptions.MS1ValidationMode = MS1ValidationModes.Disabled;
                }

                FraggerOptions.MS1ValidationModeAutoDefined = false;
            }

            if (FraggerOptions.MS1ValidationMode == MS1ValidationModes.PeptideProphet && RunMSBooster)
            {
                OnWarningEvent("Disabling MSBooster since Peptide Prophet is enabled");
                RunMSBooster = false;
            }

            var generatePeptideLevelSummary = FraggerOptions.GetParameterValueOrDefault("GeneratePeptideLevelSummary", true);

            var generateProteinLevelSummary = FraggerOptions.GetParameterValueOrDefault("GenerateProteinLevelSummary", true);

            // Run iProphet if generating a peptide level summary is true (or auto)
            // This only applies if there are multiple experiment groups
            RunIProphet = generatePeptideLevelSummary;

            // Run Abacus if generating a peptide level summary and/or a protein level summary
            // This only applies if there are multiple experiment groups
            RunAbacus = generatePeptideLevelSummary || generateProteinLevelSummary;

            var ms1QuantDisabled = jobParams.GetJobParameter("MS1QuantDisabled", false);

            if (ms1QuantDisabled)
            {
                FraggerOptions.RunFreeQuant = false;
                FraggerOptions.RunIonQuant = false;
            }
            else
            {
                var runFreeQuantJobParam = jobParams.GetJobParameter("RunFreeQuant", string.Empty);
                var runIonQuantJobParam = jobParams.GetJobParameter("RunIonQuant", string.Empty);

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
                        SetMS1QuantOptions(datasetCount, runFreeQuantJobParam, runIonQuantJobParam);
                    }

                    // After loading the MSFragger parameter file, if the mods include TMT or iTRAQ, FreeQuant will be auto-enabled (provided job parameter RunFreeQuant is not false)
                }
                else
                {
                    // Single dataset job
                    // Only enable MS1 quantitation if RunFreeQuant or RunIonQuant is defined as a job parameter

                    SetMS1QuantOptions(datasetCount, runFreeQuantJobParam, runIonQuantJobParam);
                }
            }

            var runLabelQuantJobParam = jobParams.GetJobParameter("RunLabelQuant", string.Empty);

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (FraggerOptions.IsUndefinedOrAuto(runLabelQuantJobParam))
            {
                RunLabelQuant = true;
            }
            else
            {
                RunLabelQuant = FraggerOptions.GetParameterValueOrDefault("RunLabelQuant", true);
            }

            RunPTMShepherd = FraggerOptions.GetParameterValueOrDefault("RunPTMShepherd", true);

            // After loading the MSFragger parameter file, if the mods include TMT and STY Phospho, PTMProphet will be auto-enabled (provided job parameter RunPTMProphet is not false)
            RunPTMProphet = FraggerOptions.GetParameterValueOrDefault("RunPTMProphet", false);

            RunProteinProphet = FraggerOptions.GetParameterValueOrDefault("RunProteinProphet", true);
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadMSFraggerOptions(string paramFilePath)
        {
            return FraggerOptions.LoadMSFraggerOptions(paramFilePath);
        }

        private void SetMS1QuantOptions(int datasetCount, string runFreeQuantJobParam, string runIonQuantJobParam)
        {
            if (FraggerOptions.IsUndefinedOrAuto(runFreeQuantJobParam) && FraggerOptions.IsUndefinedOrAuto(runIonQuantJobParam))
            {
                // Enable running IonQuant if processing multiple datasets
                FraggerOptions.RunIonQuant = datasetCount > 1;
                FraggerOptions.RunFreeQuant = false;
                FraggerOptions.QuantModeAutoDefined = true;
                return;
            }

            var runIonQuant = FraggerOptions.GetParameterValueOrDefault("RunIonQuant", false);

            if (runIonQuant)
            {
                FraggerOptions.RunIonQuant = true;
                FraggerOptions.RunFreeQuant = false;
            }
            else
            {
                FraggerOptions.RunIonQuant = false;
                FraggerOptions.RunFreeQuant = FraggerOptions.GetParameterValueOrDefault("RunFreeQuant", false);
            }

            FraggerOptions.QuantModeAutoDefined = false;
        }
    }
}
