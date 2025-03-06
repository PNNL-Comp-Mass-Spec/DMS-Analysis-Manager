using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase;
using PRISM;
using PRISM.AppSettings;

namespace AnalysisManagerDiaNNPlugIn
{
    // ReSharper disable CommentTypo

    // Ignore Spelling: alkylation, carbamidomethyl, Chymotrypsin, cys, DIA, iodoacetamide, isoform, isotopologues, Glu, Lys
    // Ignore Spelling: peptidoforms, proline, prot, proteomes, proteotypicity, silico

    // ReSharper enable CommentTypo

    /// <summary>
    /// Cross-run normalization modes
    /// </summary>
    public enum CrossRunNormalizationModes
    {
        /// <summary>
        /// Global
        /// </summary>
        Global = 0,

        /// <summary>
        /// RT-Dependent (default)
        /// </summary>
        RTDependent = 1,

        /// <summary>
        /// Off
        /// </summary>
        Off = 2
    }

    /// <summary>
    /// Post translational modification types
    /// </summary>
    public enum ModificationTypes
    {
        /// <summary>
        /// Dynamic (variable) modification
        /// </summary>
        Dynamic = 0,

        /// <summary>
        /// Static (fixed) modification
        /// </summary>
        Static = 1
    }

    /// <summary>
    /// Protein inference modes (aka protein grouping)
    /// </summary>
    public enum ProteinInferenceModes
    {
        /// <summary>
        /// Isoform IDs
        /// </summary>
        IsoformIDs = 0,

        /// <summary>
        /// Protein names from the FASTA file
        /// </summary>
        ProteinNames = 1,

        /// <summary>
        /// Gene names (default)
        /// </summary>
        /// <remarks>For species-specific gene names, use ProteinInferenceModes.Genes and set SpeciesGenes to true</remarks>
        Genes = 2,

        /// <summary>
        /// Disable protein inference
        /// </summary>
        Off = 3
    }

    /// <summary>
    /// Quantification strategy algorithms
    /// </summary>
    public enum QuantificationAlgorithms
    {
        /// <summary>
        /// Legacy quantification
        /// </summary>
        Legacy = 0,

        /// <summary>
        /// Quant UMS, high accuracy
        /// </summary>
        HighAccuracy = 1,

        /// <summary>
        /// Quant UMS, high precision (default)
        /// </summary>
        HighPrecision = 2
    }

    /// <summary>
    /// Class for tracking DIA-NN options
    /// </summary>
    public class DiaNNOptions : EventNotifier
    {
        private const string N_TERM_PEPTIDE = "n";
        private const string N_TERM_PROTEIN = "*n";

        /// <summary>
        /// Parameter file path for DIA-NN options tracked by this class
        /// </summary>
        /// <remarks>Updated when method <see cref="LoadDiaNNOptions"/> is used to read a parameter file</remarks>
        public string ParameterFilePath { get; private set; } = string.Empty;

        // Parameters that control in-silico spectral library generation

        /// <summary>
        /// Use deep learning-based prediction of spectra, retention times and ion mobility values
        /// </summary>
        public bool DeepLearningPredictor { get; set; } = true;

        /// <summary>
        /// Minimum fragment ion m/z value
        /// </summary>
        public int FragmentIonMzMin { get; set; } = 200;

        /// <summary>
        /// Maximum fragment ion m/z value
        /// </summary>
        public int FragmentIonMzMax { get; set; } = 1800;

        /// <summary>
        /// Enable protein N-terminus methionine excision as variable modification for the in-silico digestion
        /// </summary>
        public bool TrimNTerminalMethionine { get; set; } = true;

        /// <summary>
        /// Enzyme cleavage specificity
        /// </summary>
        /// <remarks>
        /// <para>Strict trypsin:                K*,R*,!*P</para>
        /// <para>Trypsin (ignore proline rule): K*,R*</para>
        /// <para>Lys/C:                         K*</para>
        /// <para>Chymotrypsin:                  F*,W*,Y*,L*</para>
        /// <para>Asp-N:                         D*</para>
        /// <para>Glu-C:                         E*,D*</para>
        /// </remarks>
        public string CleavageSpecificity { get; set; } = "K*,R*";

        /// <summary>
        /// Maximum number of allowed missed cleavages
        /// </summary>
        public int MissedCleavages { get; set; } = 2;

        /// <summary>
        /// Minimum peptide length
        /// </summary>
        public int PeptideLengthMin { get; set; } = 7;

        /// <summary>
        /// Maximum peptide length
        /// </summary>
        public int PeptideLengthMax { get; set; } = 30;

        /// <summary>
        /// Minimum precursor ion m/z value
        /// </summary>
        public int PrecursorMzMin { get; set; } = 350;

        /// <summary>
        /// Maximum precursor ion m/z value
        /// </summary>
        public int PrecursorMzMax { get; set; } = 1800;

        /// <summary>
        /// Minimum precursor charge
        /// </summary>
        public int PrecursorChargeMin { get; set; } = 2;

        /// <summary>
        /// Maximum precursor charge
        /// </summary>
        public int PrecursorChargeMax { get; set; } = 4;

        /// <summary>
        /// Static Cys carbamidomethyl (+57.021), aka iodoacetamide alkylation
        /// </summary>
        public bool StaticCysCarbamidomethyl { get; set; }

        /// <summary>
        /// Static (fixed) modification definitions
        /// </summary>
        /// <remarks>Passed to DiaNN.exe using --fixed-mod</remarks>
        public List<ModificationInfo> StaticModDefinitions { get; }

        /// <summary>
        /// Dynamic (variable) modification definitions
        /// </summary>
        /// <remarks>Passed to DiaNN.exe using --var-mod</remarks>
        public List<ModificationInfo> DynamicModDefinitions { get; }

        /// <summary>
        /// Dictionary of static modifications, by residue or position
        /// </summary>
        /// <remarks>
        /// <para>Keys in the modification mass dictionaries are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be a description of the peptide or protein terminus (see <see cref="N_TERM_PROTEIN"/></para> and similar constants)
        /// </remarks>
        public Dictionary<string, SortedSet<double>> StaticModifications { get; }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Dictionary of variable (dynamic) modifications, by residue or position
        /// </summary>
        /// <remarks>
        /// <para>Keys in the modification mass dictionaries are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be symbols indicating N or C terminal peptide or protein (e.g., [^ for protein N-terminus or n^ for peptide N-terminus)</para>
        /// </remarks>
        public Dictionary<string, SortedSet<double>> DynamicModifications { get; }

        /// <summary>
        /// Maximum number of dynamic mods (per peptide)
        /// </summary>
        public int MaxDynamicModsPerPeptide { get; set; } = 3;

        // Parameters that control identifying peptides in DIA spectra

        /// <summary>
        /// Existing spectral library to use (overrides the spectral library created via an in-silico digest of the FASTA file)
        /// </summary>
        public string ExistingSpectralLibrary { get; set; } = string.Empty;

        /// <summary>
        /// MS1 mass accuracy, in ppm
        /// </summary>
        /// <remarks>
        /// If 0, let DIA-NN auto determine the mass accuracy to use
        /// </remarks>
        public float MS1MassAccuracy { get; set; }

        /// <summary>
        /// MS2 mass accuracy, in ppm
        /// </summary>
        /// <remarks>
        /// If 0, let DIA-NN auto determine the mass accuracy to use
        /// </remarks>
        public float MS2MassAccuracy { get; set; }

        /// <summary>
        /// Scan window radius (window half width, in scans)
        /// </summary>
        /// <remarks>
        /// <para>
        /// If 0, let DIA-NN auto-determine the value
        /// </para>
        /// <para>
        /// Ideally, it should be approximately equal to the average number of data points per peak
        /// </para>
        /// </remarks>
        public int ScanWindow { get; set; }

        /// <summary>
        /// Match-between-runs
        /// </summary>
        /// <remarks>
        /// Only applicable if searching multiple datasets as a group
        /// </remarks>
        public bool MatchBetweenRuns { get; set; } = true;

        /// <summary>
        /// Precursor false discovery rate (Q-value)
        /// </summary>
        /// <remarks>
        /// 0.01 means 1% FDR
        /// </remarks>
        public float PrecursorQValue { get; set; } = 0.01f;

        /// <summary>
        /// When true, disable scoring and localization of the dynamic mods
        /// </summary>
        /// <remarks>
        /// <para>Introduced in DIA-NN 1.9, but removed in DIA-NN 2.0 when the peptidoforms and proteoforms scoring modes were added</para>
        /// </remarks>
        [Obsolete("Removed in DIA-NN 2.0")]
        public bool DisableScoring { get; set; }

        /// <summary>
        /// When true, disable peptidoform scoring
        /// </summary>
        /// <remarks>Only valid for dynamic modifications</remarks>
        public bool NoPeptidoforms { get; set; }

        /// <summary>
        /// Generate a spectral library using DIA search results
        /// </summary>
        public bool CreateSpectralLibrary { get; set; } = true;

        /// <summary>
        /// Create expression level matrices
        /// </summary>
        public bool CreateQuantitiesMatrices { get; set; } = true;

        /// <summary>
        /// Use a heuristic protein inference algorithm (similar to the one used by FragPipe)
        /// </summary>
        /// <remarks>
        /// DIA-NN documentation says that this mode is only recommended for benchmarking protein ID numbers, and should thus generally not be used
        /// </remarks>
        public bool HeuristicProteinInference { get; set; } = true;

        /// <summary>
        /// When creating a spectral library from DIA data, use an intelligent algorithm which determines how to extract spectra
        /// </summary>
        /// <remarks>
        /// Highly recommended and should almost always be enabled
        /// </remarks>
        public bool SmartProfilingLibraryGeneration { get; set; } = true;

        /// <summary>
        /// Create extracted ion chromatograms for heavy isotopologues
        /// </summary>
        public bool CreateExtractedChromatograms { get; set; } = true;

        /// <summary>
        /// Protein Inference Mode
        /// </summary>
        /// <remarks>
        /// To disable protein inference (protein grouping), use --no-prot-inf
        /// </remarks>
        public ProteinInferenceModes ProteinInferenceMode { get; set; } = ProteinInferenceModes.Genes;

        // ReSharper disable CommentTypo

        /// <summary>
        /// Add the organism identifier to the gene names (only supports UniProt proteomes)
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is useful when the FASTA file has a mix of organisms; however, when enabled, this affects proteotypicity definition
        /// </para>
        /// <para>
        /// Example protein and gene names when SpeciesGenes is False:
        ///   EFTU_HUMAN  TUFM
        ///   PPOX_HUMAN  PPOX
        /// </para>
        /// <para>
        /// Example protein and gene names when SpeciesGenes is True:
        ///   EFTU_HUMAN  TUFM_HUMAN
        ///   PPOX_HUMAN  PPOX_HUMAN
        /// </para>
        /// </remarks>
        public bool SpeciesGenes { get; set; }

        // ReSharper restore CommentTypo

        /// <summary>
        /// Quantification Strategy
        /// </summary>
        public QuantificationAlgorithms QuantificationStrategy { get; set; } = QuantificationAlgorithms.HighPrecision;

        /// <summary>
        /// Cross-run normalization
        /// </summary>
        public CrossRunNormalizationModes CrossRunNormalization { get; set; } = CrossRunNormalizationModes.RTDependent;

        /// <summary>
        /// Create a PDF report
        /// </summary>
        public bool GeneratePDFReport { get; set; } = true;

        /// <summary>
        /// Number of CPU threads to use
        /// </summary>
        /// <remarks>
        /// 0 or all means to use all available cores
        /// </remarks>
        public int ThreadCount { get; set; }

        /// <summary>
        /// Verbosity level, default to 1; can be 0, 1, 2, 3, or 4
        /// </summary>
        public int LogLevel { get; set; } = 2;

        /// <summary>
        /// Constructor
        /// </summary>
        public DiaNNOptions()
        {
            StaticModDefinitions = new List<ModificationInfo>();
            DynamicModDefinitions = new List<ModificationInfo>();

            StaticModifications = new Dictionary<string, SortedSet<double>>();
            DynamicModifications = new Dictionary<string, SortedSet<double>>();
        }

        private void AppendModificationMass(
            ICollection<string> aminoAcidSymbols,
            IDictionary<string, SortedSet<double>> modificationList,
            ModificationInfo mod)
        {
            var affectedResidues = mod.AffectedResidues;
            var currentChars = new StringBuilder();

            for (var i = 0; i < mod.AffectedResidues.Length; i++)
            {
                if (char.IsWhiteSpace(affectedResidues[i]))
                    continue;

                currentChars.Append(affectedResidues[i]);

                if (!char.IsLetter(affectedResidues[i]))
                {
                    // Position is likely a peptide or protein terminus, e.g. "*n"
                    continue;
                }

                var residueOrPosition = currentChars.ToString();

                if (residueOrPosition.Length == 1)
                {
                    if (!aminoAcidSymbols.Contains(residueOrPosition) && !residueOrPosition.Equals(N_TERM_PEPTIDE))
                    {
                        OnWarningEvent(
                            "Unrecognized amino acid '{0}' in the modification definition: {1}",
                            residueOrPosition,
                            mod.ModificationDefinition);
                    }
                }
                else if (!residueOrPosition.Equals(N_TERM_PROTEIN))
                {
                    OnWarningEvent(
                        "Unrecognized peptide or protein position '{0}' in the modification definition: {1}",
                        residueOrPosition,
                        mod.ModificationDefinition);
                }

                if (modificationList.TryGetValue(residueOrPosition, out var modMasses))
                {
                    // Add the mass to the sorted set
                    modMasses.Add(mod.ModificationMass);
                }
                else
                {
                    modificationList.Add(residueOrPosition, new SortedSet<double> { mod.ModificationMass });
                }

                currentChars.Clear();
            }
        }

        private bool ConvertParameterListToDictionary(
            List<KeyValuePair<string, string>> paramFileEntries,
            out Dictionary<string, string> paramFileSettings)
        {
            // Populate a dictionary with the parameters (ignoring StaticMod and DynamicMod since those can appear more than once)
            var parametersToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "StaticMod",
                "DynamicMod"
            };

            return ConvertParameterListToDictionary(paramFileEntries, parametersToSkip, out paramFileSettings);
        }

        private bool ConvertParameterListToDictionary(
            List<KeyValuePair<string, string>> paramFileEntries,
            ICollection<string> parametersToSkip,
            out Dictionary<string, string> paramFileSettings)
        {
            paramFileSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var parameter in paramFileEntries)
            {
                if (parametersToSkip.Contains(parameter.Key))
                    continue;

                if (paramFileSettings.ContainsKey(parameter.Key))
                {
                    OnErrorEvent("Parameter file setting {0} is defined more than once", parameter.Key);
                    return false;
                }

                paramFileSettings.Add(parameter.Key, parameter.Value);
            }

            return true;
        }

        /// <summary>
        /// Examine the DIA-NN parameters to determine the static and dynamic (variable) modifications
        /// </summary>
        /// <remarks>
        /// <para>Keys in the modification mass dictionaries are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be a description of the peptide or protein terminus (see <see cref="N_TERM_PEPTIDE"/></para> and similar constants)
        /// </remarks>
        /// <param name="paramFileEntries">List of parameter file entries</param>
        /// <param name="staticModDefinitions">Output: list of static modifications</param>
        /// <param name="dynamicModDefinitions">Output: list of dynamic modifications</param>
        /// <param name="staticModsByResidue">Output: dictionary of static modifications, by residue or position</param>
        /// <param name="dynamicModsByResidue">Output: dictionary of dynamic modifications, by residue or position</param>
        /// <returns>True if modifications were successfully parsed, false if an error</returns>
        private bool GetDiaNNModifications(
            IEnumerable<KeyValuePair<string, string>> paramFileEntries,
            out List<ModificationInfo> staticModDefinitions,
            out List<ModificationInfo> dynamicModDefinitions,
            out Dictionary<string, SortedSet<double>> staticModsByResidue,
            out Dictionary<string, SortedSet<double>> dynamicModsByResidue)
        {
            staticModDefinitions = new List<ModificationInfo>();
            dynamicModDefinitions = new List<ModificationInfo>();

            staticModsByResidue = new Dictionary<string, SortedSet<double>>();
            dynamicModsByResidue = new Dictionary<string, SortedSet<double>>();

            var aminoAcidSymbols = new SortedSet<string>
            {
                "G", "A", "S", "P", "V",
                "T", "C", "L", "I", "N",
                "D", "Q", "K", "E", "M",
                "H", "F", "R", "Y", "W",
                "B", "J", "O", "U", "X", "Z"    // Not sure if DIA-NN supports these
            };

            try
            {
                foreach (var kvSetting in paramFileEntries)
                {
                    var paramValue = kvSetting.Value;

                    if (Global.IsMatch(kvSetting.Key, "StaticMod"))
                    {
                        if (string.IsNullOrWhiteSpace(paramValue) || Global.IsMatch(paramValue, "none"))
                            continue;

                        if (!ParseModificationDefinition(ModificationTypes.Static, paramValue, out var staticMod))
                        {
                            // An error should have already been logged
                            return false;
                        }

                        staticModDefinitions.Add(staticMod);

                        AppendModificationMass(aminoAcidSymbols, staticModsByResidue, staticMod);
                    }
                    else if (Global.IsMatch(kvSetting.Key, "DynamicMod"))
                    {
                        if (string.IsNullOrWhiteSpace(paramValue) || Global.IsMatch(paramValue, "none") || Global.IsMatch(paramValue, "defaults"))
                            continue;

                        if (!ParseModificationDefinition(ModificationTypes.Dynamic, paramValue, out var dynamicMod))
                        {
                            // An error should have already been logged
                            return false;
                        }

                        dynamicModDefinitions.Add(dynamicMod);

                        AppendModificationMass(aminoAcidSymbols, dynamicModsByResidue, dynamicMod);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception extracting dynamic and static mod information from the DIA-NN parameter file", ex);
                return false;
            }
        }

        private bool GetParameterValueOrDefault(IReadOnlyDictionary<string, string> paramFileSettings, string parameterName, bool defaultValue)
        {
            if (!paramFileSettings.TryGetValue(parameterName, out var value))
                return defaultValue;

            if (string.IsNullOrWhiteSpace(value))
                return defaultValue;

            if (bool.TryParse(value, out var parsedValue))
                return parsedValue;

            OnWarningEvent("Job parameter {0} should be True, False, but it is {1}", parameterName, value);
            return defaultValue;
        }

        private float GetParameterValueOrDefault(IReadOnlyDictionary<string, string> paramFileSettings, string parameterName, float defaultValue)
        {
            if (!paramFileSettings.TryGetValue(parameterName, out var value))
                return defaultValue;

            if (string.IsNullOrWhiteSpace(value))
                return defaultValue;

            if (float.TryParse(value, out var parsedValue))
                return parsedValue;

            OnWarningEvent("Job parameter {0} should be a number, but it is {1}", parameterName, value);
            return defaultValue;
        }

        private int GetParameterValueOrDefault(IReadOnlyDictionary<string, string> paramFileSettings, string parameterName, int defaultValue)
        {
            if (!paramFileSettings.TryGetValue(parameterName, out var value))
                return defaultValue;

            if (string.IsNullOrWhiteSpace(value))
                return defaultValue;

            if (int.TryParse(value, out var parsedValue))
                return parsedValue;

            OnWarningEvent("Job parameter {0} should be an integer, but it is {1}", parameterName, value);
            return defaultValue;
        }

        private string GetParameterValueOrDefault(IReadOnlyDictionary<string, string> paramFileSettings, string parameterName, string defaultValue)
        {
            if (!paramFileSettings.TryGetValue(parameterName, out var value))
                return defaultValue;

            return !string.IsNullOrWhiteSpace(value) ? value.Trim() : defaultValue;
        }

        /// <summary>
        /// Parse the DIA-NN parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadDiaNNOptions(string paramFilePath)
        {
            StaticModDefinitions.Clear();
            DynamicModDefinitions.Clear();

            StaticModifications.Clear();
            DynamicModifications.Clear();

            try
            {
                // Note that ParseKeyValueParameterFile will log an error if the parameter file is not found
                var paramFile = new FileInfo(paramFilePath);

                var paramFileReader = new KeyValueParamFileReader("DIA-NN", paramFile.DirectoryName, paramFile.Name);
                RegisterEvents(paramFileReader);

                var paramFileLoaded = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries, true);

                if (!paramFileLoaded)
                {
                    return false;
                }

                ParameterFilePath = paramFile.FullName;

                var validMods = GetDiaNNModifications(
                    paramFileEntries,
                    out var staticModDefinitions,
                    out var dynamicModDefinitions,
                    out var staticModsByResidue,
                    out var dynamicModsByResidue);

                if (!validMods)
                    return false;

                StaticModDefinitions.AddRange(staticModDefinitions);

                DynamicModDefinitions.AddRange(dynamicModDefinitions);

                foreach (var item in staticModsByResidue)
                {
                    StaticModifications.Add(item.Key, item.Value);
                }

                foreach (var item in dynamicModsByResidue)
                {
                    DynamicModifications.Add(item.Key, item.Value);
                }

                var validParameters = ConvertParameterListToDictionary(paramFileEntries, out var paramFileSettings);

                if (!validParameters)
                    return false;

                DeepLearningPredictor = GetParameterValueOrDefault(paramFileSettings, "DeepLearningPredictor", DeepLearningPredictor);

                FragmentIonMzMin = GetParameterValueOrDefault(paramFileSettings, "FragmentIonMzMin", FragmentIonMzMin);
                FragmentIonMzMax = GetParameterValueOrDefault(paramFileSettings, "FragmentIonMzMax", FragmentIonMzMax);

                TrimNTerminalMethionine = GetParameterValueOrDefault(paramFileSettings, "TrimNTerminalMethionine", TrimNTerminalMethionine);

                CleavageSpecificity = GetParameterValueOrDefault(paramFileSettings, "CleavageSpecificity", CleavageSpecificity);

                MissedCleavages = GetParameterValueOrDefault(paramFileSettings, "MissedCleavages", MissedCleavages);

                PeptideLengthMin = GetParameterValueOrDefault(paramFileSettings, "PeptideLengthMin", PeptideLengthMin);
                PeptideLengthMax = GetParameterValueOrDefault(paramFileSettings, "PeptideLengthMax", PeptideLengthMax);

                PrecursorMzMin = GetParameterValueOrDefault(paramFileSettings, "PrecursorMzMin", PrecursorMzMin);
                PrecursorMzMax = GetParameterValueOrDefault(paramFileSettings, "PrecursorMzMax", PrecursorMzMax);

                PrecursorChargeMin = GetParameterValueOrDefault(paramFileSettings, "PrecursorChargeMin", PrecursorChargeMin);
                PrecursorChargeMax = GetParameterValueOrDefault(paramFileSettings, "PrecursorChargeMax", PrecursorChargeMax);

                StaticCysCarbamidomethyl = GetParameterValueOrDefault(paramFileSettings, "StaticCysCarbamidomethyl", StaticCysCarbamidomethyl);

                MaxDynamicModsPerPeptide = GetParameterValueOrDefault(paramFileSettings, "MaxDynamicModsPerPeptide", MaxDynamicModsPerPeptide);

                ExistingSpectralLibrary = GetParameterValueOrDefault(paramFileSettings, "ExistingSpectralLibrary", ExistingSpectralLibrary);

                MS1MassAccuracy = GetParameterValueOrDefault(paramFileSettings, "MS1MassAccuracy", MS1MassAccuracy);
                MS2MassAccuracy = GetParameterValueOrDefault(paramFileSettings, "MS2MassAccuracy", MS2MassAccuracy);

                ScanWindow = GetParameterValueOrDefault(paramFileSettings, "ScanWindow", ScanWindow);

                MatchBetweenRuns = GetParameterValueOrDefault(paramFileSettings, "MatchBetweenRuns", MatchBetweenRuns);

                PrecursorQValue = GetParameterValueOrDefault(paramFileSettings, "PrecursorQValue", PrecursorQValue);

                DisableScoring = GetParameterValueOrDefault(paramFileSettings, "DisableScoring", DisableScoring);

                NoPeptidoforms = GetParameterValueOrDefault(paramFileSettings, "NoPeptidoforms", NoPeptidoforms);

                CreateSpectralLibrary = GetParameterValueOrDefault(paramFileSettings, "CreateSpectralLibrary", CreateSpectralLibrary);

                CreateQuantitiesMatrices = GetParameterValueOrDefault(paramFileSettings, "CreateQuantitiesMatrices", CreateQuantitiesMatrices);

                HeuristicProteinInference = GetParameterValueOrDefault(paramFileSettings, "HeuristicProteinInference", HeuristicProteinInference);

                SmartProfilingLibraryGeneration = GetParameterValueOrDefault(paramFileSettings, "SmartProfilingLibraryGeneration", SmartProfilingLibraryGeneration);

                CreateExtractedChromatograms = GetParameterValueOrDefault(paramFileSettings, "CreateExtractedChromatograms", CreateExtractedChromatograms);

                var proteinInferenceMode = GetParameterValueOrDefault(paramFileSettings, "ProteinInferenceMode", (int)ProteinInferenceMode);

                if (!Enum.IsDefined(typeof(ProteinInferenceModes), proteinInferenceMode))
                {
                    OnErrorEvent("Parameter file {0} has an invalid value for ProteinInferenceMode: {1}", paramFile.Name, proteinInferenceMode);
                    return false;
                }

                ProteinInferenceMode = (ProteinInferenceModes)proteinInferenceMode;

                SpeciesGenes = GetParameterValueOrDefault(paramFileSettings, "SpeciesGenes", SpeciesGenes);

                var quantificationStrategyMode = GetParameterValueOrDefault(paramFileSettings, "QuantificationStrategy", (int)QuantificationStrategy);

                if (!Enum.IsDefined(typeof(QuantificationAlgorithms), quantificationStrategyMode))
                {
                    OnErrorEvent("Parameter file {0} has an invalid value for QuantificationStrategy: {1}", paramFile.Name, quantificationStrategyMode);
                    return false;
                }

                QuantificationStrategy = (QuantificationAlgorithms)quantificationStrategyMode;

                var crossRunNormalizationMode = GetParameterValueOrDefault(paramFileSettings, "CrossRunNormalization", (int)CrossRunNormalization);

                if (!Enum.IsDefined(typeof(CrossRunNormalizationModes), crossRunNormalizationMode))
                {
                    OnErrorEvent("Parameter file {0} has an invalid value for CrossRunNormalization: {1}", paramFile.Name, crossRunNormalizationMode);
                    return false;
                }

                CrossRunNormalization = (CrossRunNormalizationModes)crossRunNormalizationMode;

                GeneratePDFReport = GetParameterValueOrDefault(paramFileSettings, "GeneratePDFReport", GeneratePDFReport);

                var threadCountText = GetParameterValueOrDefault(paramFileSettings, "ThreadCount", ThreadCount.ToString());

                if (threadCountText.Equals("all", StringComparison.OrdinalIgnoreCase))
                {
                    ThreadCount = 0;
                }
                else if (int.TryParse(threadCountText, out var threadCount))
                {
                    ThreadCount = threadCount;
                }

                LogLevel = GetParameterValueOrDefault(paramFileSettings, "LogLevel", LogLevel);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadDiaNNOptions", ex);
                return false;
            }
        }

        private bool ParseModificationDefinition(
            ModificationTypes modificationType,
            string modificationDefinition,
            out ModificationInfo modInfo)
        {
            var poundIndex = modificationDefinition.IndexOf('#');

            string mod;

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (poundIndex > 0)
            {
                // comment = mod.Substring(poundIndex);
                mod = modificationDefinition.Substring(0, poundIndex - 1).Trim();
            }
            else
            {
                mod = modificationDefinition.Trim();
            }

            var splitMod = mod.Split(',');

            if (splitMod.Length < 3)
            {
                // Invalid mod definition; must have at least 3 sections
                OnErrorEvent("Invalid modification string; must have 3 sections: " + mod);
                modInfo = ModificationInfo.GetUndefinedModification();
                return false;
            }

            var modificationName = splitMod[0].Trim();

            if (!double.TryParse(splitMod[1], out var modificationMass))
            {
                // Invalid modification mass
                OnErrorEvent("Invalid modification mass {0} in modification definition {1}", splitMod[1], mod);
                modInfo = ModificationInfo.GetUndefinedModification();
                return false;
            }

            var affectedResidues = splitMod[2].Trim();

            // Reconstruct the mod definition, making sure there is no whitespace
            var modDefinitionClean = new StringBuilder();

            modDefinitionClean.Append(splitMod[0].Trim());

            for (var index = 1; index <= splitMod.Length - 1; index++)
            {
                modDefinitionClean.AppendFormat(",{0}", splitMod[index].Trim());
            }

            var isFixedLabelMod = false;

            if (modificationType == ModificationTypes.Static && splitMod.Length > 3)
            {
                if (splitMod[3].Trim().Equals("label", StringComparison.OrdinalIgnoreCase))
                {
                    isFixedLabelMod = true;
                }
                else
                {
                    OnErrorEvent("Unrecognized text '{0}' in the modification definition: {1}", splitMod[3], mod);
                    modInfo = ModificationInfo.GetUndefinedModification();
                    return false;
                }
            }

            modInfo = new ModificationInfo(modificationType, modDefinitionClean.ToString(), modificationName, modificationMass, affectedResidues, isFixedLabelMod);

            if (DisableScoring)
            {
                modInfo.DisableScoring = true;
            }

            return true;
        }

        /// <summary>
        /// Validate options loaded from a DIA-NN parameter file
        /// </summary>
        /// <remarks>Call <see cref="LoadDiaNNOptions"/> before calling this method</remarks>
        /// <param name="spectralLibraryFile">Output: existing spectral library to use, or null if not using an existing file</param>
        /// <returns>True if the parameter file is valid, otherwise false</returns>
        public bool ValidateDiaNNOptions(out FileInfo spectralLibraryFile)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(ParameterFilePath))
                {
                    OnErrorEvent("Call method LoadDiaNNOptions() before calling method ValidateDiaNNOptions()");
                    spectralLibraryFile = null;
                    return false;
                }

                var paramFile = new FileInfo(ParameterFilePath);

                if (!paramFile.Exists)
                {
                    OnErrorEvent("DIA-NN parameter file not found: " + paramFile.FullName);
                    spectralLibraryFile = null;
                    return false;
                }

                var paramFileReader = new KeyValueParamFileReader("DIA-NN", paramFile.DirectoryName, paramFile.Name);
                RegisterEvents(paramFileReader);

                if (string.IsNullOrWhiteSpace(ExistingSpectralLibrary))
                {
                    spectralLibraryFile = null;
                }
                else
                {
                    // Verify that the spectral library file exists

                    try
                    {
                        spectralLibraryFile = new FileInfo(ExistingSpectralLibrary);

                        if (!spectralLibraryFile.Exists)
                        {
                            OnErrorEvent("The spectral library defined in parameter file {0} does not exist: {1}",
                                paramFile.Name, spectralLibraryFile.FullName);

                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Error looking for spectral library {0} (defined in parameter file {1}): {2}",
                            ExistingSpectralLibrary, paramFile.Name, ex.Message);

                        spectralLibraryFile = null;
                        return false;
                    }
                }

                if (MS1MassAccuracy < 0)
                {
                    OnErrorEvent("MS1MassAccuracy must be 0 or a positive number, not negative: {0}", paramFile.Name);
                    return false;
                }

                if (MS2MassAccuracy < 0)
                {
                    OnErrorEvent("MS1MassAccuracy must be 0 or a positive number, not negative: {0}", paramFile.Name);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateDiaNNOptions", ex);
                spectralLibraryFile = null;
                return false;
            }
        }
    }
}
