using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISM.AppSettings;

namespace AnalysisManagerMSFraggerPlugIn
{
    // ReSharper disable CommentTypo

    // Ignore Spelling: acetylation, deisotope, deneutralloss, fragger, nc, plex, quant, quantitation

    // ReSharper restore CommentTypo

    public enum MS1ValidationModes
    {
        Disabled = 0,
        PeptideProphet = 1,
        Percolator = 2
    }

    public class MSFraggerOptions : EventNotifier
    {
        private const string C_TERM_PEPTIDE = "Cterm_peptide";
        private const string N_TERM_PEPTIDE = "Nterm_peptide";
        private const string C_TERM_PROTEIN = "Cterm_protein";
        private const string N_TERM_PROTEIN = "Nterm_protein";

        private readonly IJobParams mJobParams;

        /// <summary>
        /// This is set to true if data_type is 1 or 2, meaning searching DIA data
        /// </summary>
        public bool DIASearchEnabled { get; set; }

        /// <summary>
        /// Whether to run PeptideProphet, Percolator, or nothing
        /// </summary>
        /// <remarks>Defaults to Percolator (prior to v20, would set this to PeptideProphet if using iTRAQ)</remarks>
        public MS1ValidationModes MS1ValidationMode { get; set; }

        /// <summary>
        /// True when the MS1 validation mode was auto-defined (since job parameters RunPeptideProphet and/or RunPercolator were not present)
        /// </summary>
        public bool MS1ValidationModeAutoDefined { get; set; }

        /// <summary>
        /// Dictionary of static modifications, by residue or position
        /// </summary>
        /// <remarks>
        /// <para>Keys are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be a description of the peptide or protein terminus (see <see cref="N_TERM_PROTEIN"/></para> and similar constants)
        /// </remarks>
        public Dictionary<string, SortedSet<double>> StaticModifications { get; }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Dictionary of variable (dynamic) modifications, by residue or position
        /// </summary>
        /// <remarks>
        /// <para>Keys are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be symbols indicating N or C terminal peptide or protein (e.g., [^ for protein N-terminus or n^ for peptide N-terminus)</para>
        /// </remarks>
        public Dictionary<string, SortedSet<double>> VariableModifications { get; }

        /// <summary>
        /// True if the MSFragger parameter file has open search based tolerances
        /// </summary>
        public bool OpenSearch { get; set; }

        /// <summary>
        /// True when FreeQuant and IonQuant are auto-defined
        /// </summary>
        public bool QuantModeAutoDefined { get; set; }

        /// <summary>
        /// Reporter ion mode defined in the parameter file
        /// </summary>
        public ReporterIonInfo.ReporterIonModes ReporterIonMode { get; set; }

        /// <summary>
        /// When true, run FreeQuant
        /// </summary>
        /// <remarks>
        /// Defaults to false, but forced to true if reporter ions are used
        /// If no reporter ions, RunFreeQuant is ignored if RunIonQuant is enabled</remarks>
        public bool RunFreeQuant { get; set; }

        /// <summary>
        /// When true, run IonQuant for MS1-based quantitation
        /// </summary>
        /// <remarks>
        /// Auto-set to true if this job has multiple datasets (as defined in a data package)
        /// Also set to true if job parameter RunIonQuant is defined
        /// However, set to false if job parameter MS1QuantDisabled is defined
        /// </remarks>
        public bool RunIonQuant { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        public MSFraggerOptions(IJobParams jobParams)
        {
            mJobParams = jobParams;

            StaticModifications = new Dictionary<string, SortedSet<double>>();
            VariableModifications = new Dictionary<string, SortedSet<double>>();
        }

        private void AddParameterToValidate(
            IDictionary<string, IntegerParameter> parametersToValidate,
            string parameterName,
            int minValue,
            int maxValue,
            bool required = true)
        {
            parametersToValidate.Add(parameterName, new IntegerParameter(parameterName, minValue, maxValue, required));
        }

        /// <summary>
        /// Add a static or dynamic modification to the modificationsByResidue dictionary
        /// </summary>
        /// <param name="modificationsByResidue">Dictionary of modifications, by residue or position</param>
        /// <param name="residueOrPositionName">Residue or position name</param>
        /// <param name="modificationMass">Modification mass</param>
        private void AppendModificationMass(
            IDictionary<string, SortedSet<double>> modificationsByResidue,
            string residueOrPositionName,
            double modificationMass)
        {
            if (modificationsByResidue.TryGetValue(residueOrPositionName, out var modMasses))
            {
                // Add the mass to the sorted set
                modMasses.Add(modificationMass);
                return;
            }

            modificationsByResidue.Add(residueOrPositionName, new SortedSet<double> { modificationMass });
        }

        // ReSharper disable CommentTypo

        /// <summary>
        /// Examine the dynamic and static mods loaded from a MSFragger parameter file to determine the reporter ion mode
        /// </summary>
        /// <remarks>Peptide and protein static terminal modifications in staticModifications are indicated by Nterm_peptide, Cterm_peptide, add_Nterm_protein, and Cterm_protein</remarks>
        /// <param name="staticModifications">Keys in this dictionary are modification masses; values are a list of the affected residues</param>
        /// <param name="variableModifications">Keys in this dictionary are modification masses; values are a list of the affected residues</param>
        /// <param name="reporterIonMode">Output: reporter ion mode enum</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper restore CommentTypo
        private bool DetermineReporterIonMode(
            IReadOnlyDictionary<string, SortedSet<double>> staticModifications,
            IReadOnlyDictionary<string, SortedSet<double>> variableModifications,
            out ReporterIonInfo.ReporterIonModes reporterIonMode)
        {
            reporterIonMode = ReporterIonInfo.ReporterIonModes.Disabled;

            try
            {
                ReporterIonInfo.ReporterIonModes staticNTermMode;
                ReporterIonInfo.ReporterIonModes staticLysineMode;

                // ReSharper disable once StringLiteralTypo

                if (staticModifications.TryGetValue("Nterm_peptide", out var staticNTermModMass) && staticNTermModMass.Count > 0)
                {
                    staticNTermMode = ReporterIonInfo.GetReporterIonModeFromModMass(staticNTermModMass.First());
                }
                else
                {
                    staticNTermMode = ReporterIonInfo.ReporterIonModes.Disabled;
                }

                if (staticModifications.TryGetValue("K", out var staticLysineModMass) && staticLysineModMass.Count > 0)
                {
                    staticLysineMode = ReporterIonInfo.GetReporterIonModeFromModMass(staticLysineModMass.First());
                }
                else
                {
                    staticLysineMode = ReporterIonInfo.ReporterIonModes.Disabled;
                }

                // Keys in this dictionary are modification masses, values are the reporter ion mode that corresponds to the modification mass (if any)
                var dynamicModModes = new Dictionary<double, ReporterIonInfo.ReporterIonModes>();

                foreach (var residueOrLocation in variableModifications)
                {
                    foreach (var modMass in residueOrLocation.Value)
                    {
                        if (dynamicModModes.Keys.Contains(modMass))
                            continue;

                        dynamicModModes.Add(modMass, ReporterIonInfo.GetReporterIonModeFromModMass(modMass));
                    }
                }

                var reporterIonModeStats = new Dictionary<ReporterIonInfo.ReporterIonModes, int>();

                UpdateReporterIonModeStats(reporterIonModeStats, staticNTermMode);
                UpdateReporterIonModeStats(reporterIonModeStats, staticLysineMode);
                UpdateReporterIonModeStats(reporterIonModeStats, dynamicModModes.Values.ToList());

                // Keys in this dictionary are reporter ion modes, values are the number of dynamic or static modifications that indicate the given mode
                var matchedReporterIonModes = new Dictionary<ReporterIonInfo.ReporterIonModes, int>();

                foreach (var item in reporterIonModeStats)
                {
                    if (item.Key != ReporterIonInfo.ReporterIonModes.Disabled && item.Value > 0)
                    {
                        matchedReporterIonModes.Add(item.Key, item.Value);
                    }
                }

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (matchedReporterIonModes.Count == 0)
                {
                    reporterIonMode = ReporterIonInfo.ReporterIonModes.Disabled;
                    return true;
                }

                if (matchedReporterIonModes.Count == 1)
                {
                    reporterIonMode = DetermineReporterIonMode(matchedReporterIonModes.First().Key);
                    return true;
                }

                OnErrorEvent("The MSFragger parameter file has more than one reporter ion mode defined: {0}", string.Join(", ", matchedReporterIonModes.Keys.ToList()));

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in DetermineReporterIonMode", ex);
                return false;
            }
        }

        /// <summary>
        /// <para>
        /// <see cref="ReporterIonInfo.GetReporterIonModeFromModMass"/> sets the ReporterIonMode to Tmt11 for 6-plex, 10-plex, and 11-plex TMT.
        /// For both 16-plex and 18-plex TMT, it sets ReporterIonMode to Tmt16.
        /// </para>
        /// <para>
        /// When ReporterIonMode is Tmt11 or Tmt16, this method looks for job parameter ReporterIonMode to attempt to determine the actual TMT mode in use.
        /// By default, that job parameter is "auto", but it can be customized by editing a DMS settings file
        /// </para>
        /// </summary>
        /// <remarks>
        /// This method will return reporterIonMode if it is not Tmt11 or Tmt16, or if job parameter ReporterIonMode is undefined to "auto"
        /// </remarks>
        /// <param name="reporterIonMode">Reporter ion mode to use if job parameter "ReporterIonMode" is "auto"</param>
        private ReporterIonInfo.ReporterIonModes DetermineReporterIonMode(ReporterIonInfo.ReporterIonModes reporterIonMode)
        {
            if (reporterIonMode != ReporterIonInfo.ReporterIonModes.Tmt11 && reporterIonMode != ReporterIonInfo.ReporterIonModes.Tmt16)
                return reporterIonMode;

            // Look for a job parameter that specifies the reporter ion mode
            var reporterIonModeName = mJobParams.GetJobParameter("ReporterIonMode", string.Empty);

            // The standard settings files have <item key="ReporterIonMode" value="auto">
            // Check for this, and if found, simply return reporterIonMode
            if (IsUndefinedOrAuto(reporterIonModeName))
            {
                return reporterIonMode;
            }

            return ReporterIonInfo.DetermineReporterIonMode(reporterIonModeName);
        }

        /// <summary>
        /// Look for text in affectedResidueList
        /// For each match found, append to affectedResidues
        /// </summary>
        /// <param name="affectedResidueList">String of affected residues</param>
        /// <param name="residueMatcher">Residue matcher RegEx</param>
        /// <param name="affectedResidues">List of affected residue symbols</param>
        /// <returns>Updated version of affectedResidueList with the matches removed</returns>
        private string ExtractMatches(string affectedResidueList, Regex residueMatcher, ICollection<string> affectedResidues)
        {
            if (string.IsNullOrWhiteSpace(affectedResidueList))
                return affectedResidueList;

            var matches = residueMatcher.Matches(affectedResidueList);

            if (matches.Count <= 0)
            {
                return affectedResidueList;
            }

            foreach (var match in matches)
            {
                affectedResidues.Add(match.ToString());
            }

            return residueMatcher.Replace(affectedResidueList, string.Empty);
        }

        /// <summary>
        /// Returns a description of the number of dynamic mods, e.g. "2 dynamic mods"
        /// </summary>
        /// <param name="dynamicModCount">Dynamic mod count</param>
        /// <returns>Description of the number of dynamic mods</returns>
        public static string GetDynamicModCountDescription(int dynamicModCount)
        {
            return dynamicModCount == 1 ?
                "1 dynamic mod" :
                string.Format("{0} dynamic mods", dynamicModCount);
        }

        /// <summary>
        /// Returns the number of dynamic mods in use, counting a mod multiple times if it can occur on multiple residues
        /// </summary>
        /// <returns>Number of dynamic mods that MSFragger must consider</returns>
        public int GetDynamicModResidueCount()
        {
            var dynamicModCount = 0;

            foreach (var mod in VariableModifications)
            {
                dynamicModCount += mod.Value.Count;
            }

            return dynamicModCount;
        }

        /// <summary>
        /// Examine the MSFragger parameters to determine the static and dynamic (variable) modifications
        /// </summary>
        /// <remarks>
        /// <para>Keys in the output dictionaries are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be a description of the peptide or protein terminus (see <see cref="C_TERM_PEPTIDE"/></para> and similar constants)
        /// </remarks>
        /// <param name="paramFileEntries">List of parameter file entries, as Key/Value pairs</param>
        /// <param name="staticModsByResidue">Output: dictionary of static modifications, by residue or position</param>
        /// <param name="variableModsByResidue">Output: dictionary of dynamic modifications, by residue or position</param>
        /// <returns>True if modifications were successfully parsed, false if an error</returns>
        private bool GetMSFraggerModifications(
            IEnumerable<KeyValuePair<string, string>> paramFileEntries,
            out Dictionary<string, SortedSet<double>> staticModsByResidue,
            out Dictionary<string, SortedSet<double>> variableModsByResidue)
        {
            staticModsByResidue = new Dictionary<string, SortedSet<double>>();
            variableModsByResidue = new Dictionary<string, SortedSet<double>>();

            // ReSharper disable StringLiteralTypo

            var terminalStaticModParameters = new Dictionary<string, string>
            {
                { "add_Cterm_peptide", C_TERM_PEPTIDE },
                { "add_Nterm_peptide", N_TERM_PEPTIDE },
                { "add_Cterm_protein", C_TERM_PROTEIN },
                { "add_Nterm_protein", N_TERM_PROTEIN }
            };

            // ReSharper restore StringLiteralTypo

            var aminoAcidSymbols = new SortedSet<string>
            {
                "G", "A", "S", "P", "V",
                "T", "C", "L", "I", "N",
                "D", "Q", "K", "E", "M",
                "H", "F", "R", "Y", "W",
                "B", "J", "O", "U", "X", "Z"
            };

            foreach (var parameter in paramFileEntries)
            {
                var matchFound = false;

                foreach (var staticModParameter in terminalStaticModParameters)
                {
                    if (!parameter.Key.Equals(staticModParameter.Key))
                        continue;

                    if (!ParseModMass(parameter, out var modMass, out _))
                        return false;

                    if (modMass != 0)
                    {
                        AppendModificationMass(staticModsByResidue, staticModParameter.Value, modMass);
                    }

                    matchFound = true;
                    break;
                }

                if (matchFound)
                    continue;

                foreach (var aminoAcidSymbol in aminoAcidSymbols)
                {
                    if (!parameter.Key.StartsWith(string.Format("add_{0}_", aminoAcidSymbol)))
                        continue;

                    if (!ParseModMass(parameter, out var modMass, out _))
                        return false;

                    if (modMass != 0)
                    {
                        AppendModificationMass(staticModsByResidue, aminoAcidSymbol, modMass);
                    }

                    matchFound = true;
                    break;
                }

                if (matchFound)
                    continue;

                if (!parameter.Key.StartsWith("variable_mod"))
                {
                    continue;
                }

                if (!ParseModMass(parameter, out var dynamicModMass, out var affectedResidues))
                    return false;

                if (dynamicModMass == 0)
                {
                    OnErrorEvent("Variable modification mass in MSFragger parameter file is zero: {0} = {1}", parameter.Key, parameter.Value);
                    return false;
                }

                foreach (var residue in affectedResidues)
                {
                    AppendModificationMass(variableModsByResidue, residue, dynamicModMass);
                }
            }

            return true;
        }

        private bool GetParamValueDouble(KeyValuePair<string, string> parameter, out double value)
        {
            if (double.TryParse(parameter.Value, out value))
                return true;

            OnErrorEvent("Parameter value in MSFragger parameter file is not numeric: {0} = {1}", parameter.Key, parameter.Value);

            return false;
        }

        private bool GetParamValueInt(KeyValuePair<string, string> parameter, out int value)
        {
            if (int.TryParse(parameter.Value, out value))
                return true;

            OnErrorEvent("Parameter value in MSFragger parameter file is not numeric: {0} = {1}", parameter.Key, parameter.Value);

            return false;
        }

        /// <summary>
        /// Examines the value for the given job parameter
        /// If missing or "auto", returns the default value
        /// Otherwise, parses as a boolean and returns the result if "true", "false", "1", "0", "yes", "no"
        /// If not a boolean or 0 or 1, logs a warning and returns the default value
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="defaultValue">Default value</param>
        /// <returns>Parameter value, or the default if missing, "auto", or invalid</returns>
        public bool GetParameterValueOrDefault(string parameterName, bool defaultValue)
        {
            var value = mJobParams.GetJobParameter(parameterName, string.Empty);

            if (IsUndefinedOrAuto(value))
                return defaultValue;

            if (bool.TryParse(value, out var parsedBoolean))
                return parsedBoolean;

            if (int.TryParse(value, out var parsedInteger))
                return parsedInteger != 0;

            if (value.Equals("Yes", StringComparison.OrdinalIgnoreCase))
                return true;

            if (value.Equals("No", StringComparison.OrdinalIgnoreCase))
                return false;

            OnWarningEvent("Job parameter {0} should be True, False, 1, 0, Yes, No, or Auto, but it is {1}", parameterName, value);
            return defaultValue;
        }

        /// <summary>
        /// Return true if the value is an empty string or the word "auto"
        /// </summary>
        /// <param name="value">Parameter value to examine</param>
        public bool IsUndefinedOrAuto(string value)
        {
            return string.IsNullOrWhiteSpace(value) || value.Equals("auto", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadMSFraggerOptions(string paramFilePath)
        {
            StaticModifications.Clear();
            VariableModifications.Clear();

            try
            {
                var paramFile = new FileInfo(paramFilePath);

                var paramFileReader = new KeyValueParamFileReader("MSFragger", paramFile.DirectoryName, paramFile.Name);
                RegisterEvents(paramFileReader);

                var paramFileLoaded = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries, true);

                if (!paramFileLoaded)
                {
                    return false;
                }

                var validMods = GetMSFraggerModifications(
                    paramFileEntries,
                    out var staticModsByResidue,
                    out var variableModsByResidue);

                if (!validMods)
                    return false;

                foreach (var item in staticModsByResidue)
                {
                    StaticModifications.Add(item.Key, item.Value);
                }

                foreach (var item in variableModsByResidue)
                {
                    VariableModifications.Add(item.Key, item.Value);
                }

                var reporterIonModeSuccess = DetermineReporterIonMode(staticModsByResidue, variableModsByResidue, out var reporterIonMode);

                if (!reporterIonModeSuccess)
                {
                    return false;
                }

                ReporterIonMode = reporterIonMode;

                var precursorMassLower = 0.0;
                var precursorMassUpper = 0.0;
                var precursorMassUnits = 0;
                var dataTypeMode = 0;

                foreach (var parameter in paramFileEntries)
                {
                    switch (parameter.Key)
                    {
                        case "precursor_mass_lower":
                            if (!GetParamValueDouble(parameter, out precursorMassLower))
                                return false;

                            break;

                        case "precursor_mass_upper":
                            if (!GetParamValueDouble(parameter, out precursorMassUpper))
                                return false;

                            break;

                        case "precursor_mass_units":

                            // Precursor mass tolerance units (0=Da, 1=ppm)
                            if (!GetParamValueInt(parameter, out precursorMassUnits))
                                return false;

                            break;

                        case "data_type":

                            // Data type (0=DDA, 1=DIA, 2=Gas-phase fractionation DIA, aka DIA-narrow-window)
                            if (!GetParamValueInt(parameter, out dataTypeMode))
                                return false;

                            break;
                    }
                }

                if (precursorMassUnits > 0 && precursorMassLower < -25 && precursorMassUpper > 50)
                {
                    // Wide, Dalton-based tolerances
                    // Assume open search
                    OpenSearch = true;

                    // Preferably use PeptideProphet with open searches, but leave MS1ValidationMode unchanged if MS1ValidationModeAutoDefined is false
                    if (MS1ValidationModeAutoDefined && MS1ValidationMode == MS1ValidationModes.Percolator)
                    {
                        MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                    }
                }
                else
                {
                    OpenSearch = false;

                    if (MS1ValidationModeAutoDefined && MS1ValidationMode != MS1ValidationModes.Disabled)
                    {
                        // ReSharper disable CommentTypo

                        // Prior to FragPipe v20, we would set the MS1 validation mode to PeptideProphet when using iTRAQ
                        // FragPipe v19 and newer support iTRAQ with Percolator

                        // if (MS1ValidationMode == MS1ValidationModes.Percolator &&
                        //     ReporterIonMode is ReporterIonModes.Itraq4 or ReporterIonModes.Itraq8)
                        // {
                        //     // Switch from Percolator to PeptideProphet since using iTRAQ
                        //     MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                        // }

                        // ReSharper restore CommentTypo

                        if (MS1ValidationMode == MS1ValidationModes.PeptideProphet &&
                            ReporterIonMode is
                                ReporterIonInfo.ReporterIonModes.Tmt6 or ReporterIonInfo.ReporterIonModes.Tmt10 or ReporterIonInfo.ReporterIonModes.Tmt11 or
                                ReporterIonInfo.ReporterIonModes.Tmt16 or ReporterIonInfo.ReporterIonModes.Tmt18)
                        {
                            // Switch from PeptideProphet to Percolator since using TMT
                            MS1ValidationMode = MS1ValidationModes.Percolator;
                        }
                    }

                    if (QuantModeAutoDefined && ReporterIonMode != ReporterIonInfo.ReporterIonModes.Disabled)
                    {
                        // RunFreeQuant since TMT or iTRAQ is defined
                        RunFreeQuant = true;

                        // Do not change the value for RunIonQuant; FragPipe v20 supports running both
                    }
                }

                if (dataTypeMode is 1 or 2)
                    DIASearchEnabled = true;

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        private bool ParameterValueInRange(IntegerParameter parameter)
        {
            var value = parameter.ParameterValue;

            if (value >= parameter.MinValue && value <= parameter.MaxValue)
            {
                return true;
            }

            OnErrorEvent("Invalid value for {0} in the MSFraggerParameter file; it should be between {1} and {2}", parameter.ParameterName, parameter.MinValue, parameter.MaxValue);
            return false;
        }

        private List<string> ParseAffectedResidueList(string affectedResidueList)
        {
            // This matches [^ or ]^ or [A
            var proteinTerminusMatcher = new Regex(@"[\[\]][A-Z\^]", RegexOptions.Compiled);

            // This matches nQ or nC or cK or n^
            var peptideTerminusMatcher = new Regex(@"[nc][A-Z\^]", RegexOptions.Compiled);

            // This matches single letter residue symbols or *
            var residueMatcher = new Regex(@"[A-Z\*]", RegexOptions.Compiled);

            var affectedResidues = new List<string>();

            var updatedList1 = ExtractMatches(affectedResidueList, proteinTerminusMatcher, affectedResidues);
            var updatedList2 = ExtractMatches(updatedList1, peptideTerminusMatcher, affectedResidues);
            var updatedList3 = ExtractMatches(updatedList2, residueMatcher, affectedResidues);

            if (!string.IsNullOrWhiteSpace(updatedList3))
            {
                affectedResidues.Add(updatedList3);
            }

            return affectedResidues;
        }

        /// <summary>
        /// Parse a static or dynamic mod parameter to determine the modification mass, and (if applicable) the affected residues
        /// </summary>
        /// <remarks>Assumes the calling method already removed any comment text (beginning with the # sign)</remarks>
        /// <param name="parameter">Key/value parameter</param>
        /// <param name="modMass">Output: modification mass</param>
        /// <param name="affectedResidues">Output: list of affected residues</param>
        /// <returns>True if success, false if an error</returns>
        private bool ParseModMass(
            KeyValuePair<string, string> parameter,
            out double modMass,
            out List<string> affectedResidues)
        {
            // ReSharper disable CommentTypo

            // Example dynamic mods (format is Mass AffectedResidues MaxOccurrences):
            // variable_mod_01 = 15.994900 M 3        # Oxidized methionine
            // variable_mod_02 = 42.010600 [^ 1       # Acetylation protein N-term

            // Example static mods:
            // add_Nterm_peptide = 304.207146    # 16-plex TMT
            // add_K_lysine = 304.207146         # 16-plex TMT

            // ReSharper restore CommentTypo

            var spaceIndex = parameter.Value.IndexOf(' ');
            string parameterValue;

            if (spaceIndex < 0)
            {
                parameterValue = parameter.Value;
                affectedResidues = new List<string>();
            }
            else
            {
                parameterValue = parameter.Value.Substring(0, spaceIndex).Trim();
                var remainingValue = parameter.Value.Substring(spaceIndex + 1).Trim();

                var spaceIndex2 = remainingValue.IndexOf(' ');

                if (spaceIndex2 <= 0)
                {
                    OnWarningEvent("Affected residues not found after the modification mass for parameter '{0} = {1}' in the MSFragger parameter file", parameter.Key, parameter.Value);
                }

                var affectedResidueList = spaceIndex2 > 0 ? remainingValue.Substring(0, spaceIndex2).Trim() : string.Empty;

                affectedResidues = ParseAffectedResidueList(affectedResidueList);
            }

            if (double.TryParse(parameterValue, out modMass))
            {
                return true;
            }

            OnErrorEvent("Modification mass in MSFragger parameter file is not numeric: {0} = {1}", parameter.Key, parameter.Value);

            return false;
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonInfo.ReporterIonModes, int> reporterIonModeStats, ReporterIonInfo.ReporterIonModes reporterIonMode)
        {
            UpdateReporterIonModeStats(reporterIonModeStats, new List<ReporterIonInfo.ReporterIonModes> { reporterIonMode });
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonInfo.ReporterIonModes, int> reporterIonModeStats, IEnumerable<ReporterIonInfo.ReporterIonModes> reporterIonModeList)
        {
            foreach (var reporterIonMode in reporterIonModeList)
            {
                if (reporterIonModeStats.TryGetValue(reporterIonMode, out var currentCount))
                {
                    reporterIonModeStats[reporterIonMode] = currentCount + 1;
                }
                else
                {
                    reporterIonModeStats.Add(reporterIonMode, 1);
                }
            }
        }

        /// <summary>
        /// Examine MSFragger parameters to check for errors
        /// </summary>
        /// <param name="paramFile">Parameter file</param>
        /// <returns>True if no problems, false if errors</returns>
        public bool ValidateMSFraggerOptions(FileInfo paramFile)
        {
            try
            {
                if (!paramFile.Exists)
                {
                    OnErrorEvent("MSFragger parameter file not found: " + paramFile.FullName);
                    return false;
                }

                var paramFileReader = new KeyValueParamFileReader("MSFragger", paramFile.DirectoryName, paramFile.Name);
                RegisterEvents(paramFileReader);

                var paramFileLoaded = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries, true);

                if (!paramFileLoaded)
                {
                    return false;
                }

                var parametersToValidate = new Dictionary<string, IntegerParameter>();

                AddParameterToValidate(parametersToValidate, "precursor_mass_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "data_type", 0, 2);
                AddParameterToValidate(parametersToValidate, "precursor_true_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "fragment_mass_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "calibrate_mass", 0, 2);
                AddParameterToValidate(parametersToValidate, "use_all_mods_in_first_search", 0, 1);
                AddParameterToValidate(parametersToValidate, "deisotope", 0, 2);

                // ReSharper disable once StringLiteralTypo
                AddParameterToValidate(parametersToValidate, "deneutralloss", 0, 1);

                AddParameterToValidate(parametersToValidate, "remove_precursor_peak", 0, 2);
                AddParameterToValidate(parametersToValidate, "intensity_transform", 0, 1);

                // Removed in v19:
                AddParameterToValidate(parametersToValidate, "write_calibrated_mgf", 0, 1, false);

                // Added in v19
                AddParameterToValidate(parametersToValidate, "write_calibrated_mzml", 0, 1, false);
                AddParameterToValidate(parametersToValidate, "write_uncalibrated_mgf", 0, 1, false);

                AddParameterToValidate(parametersToValidate, "mass_diff_to_variable_mod", 0, 2);
                AddParameterToValidate(parametersToValidate, "localize_delta_mass", 0, 1);
                AddParameterToValidate(parametersToValidate, "num_enzyme_termini", 0, 2);
                AddParameterToValidate(parametersToValidate, "allowed_missed_cleavage_1", 1, 5);
                AddParameterToValidate(parametersToValidate, "clip_nTerm_M", 0, 1);
                AddParameterToValidate(parametersToValidate, "allow_multiple_variable_mods_on_residue", 0, 1);
                AddParameterToValidate(parametersToValidate, "max_variable_mods_per_peptide", 1, 10);
                AddParameterToValidate(parametersToValidate, "max_variable_mods_combinations", 1000, 65534);
                AddParameterToValidate(parametersToValidate, "output_report_topN", 1, 10);
                AddParameterToValidate(parametersToValidate, "report_alternative_proteins", 0, 1);
                AddParameterToValidate(parametersToValidate, "override_charge", 0, 1);
                AddParameterToValidate(parametersToValidate, "digest_min_length", 5, 20);
                AddParameterToValidate(parametersToValidate, "digest_max_length", 20, 60);
                AddParameterToValidate(parametersToValidate, "max_fragment_charge", 1, 4);

                foreach (var parameter in paramFileEntries)
                {
                    if (!parametersToValidate.TryGetValue(parameter.Key, out var matchingParameter))
                        continue;

                    if (!GetParamValueInt(parameter, out var parameterValue))
                        return false;

                    matchingParameter.SetValue(parameterValue);
                }

                foreach (var item in parametersToValidate)
                {
                    if (!item.Value.IsDefined)
                    {
                        if (!item.Value.Required)
                            continue;

                        OnErrorEvent("Parameter {0} is missing from the MSFraggerParameter file", item.Key);
                        return false;
                    }

                    if (!ParameterValueInRange(item.Value))
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateMSFraggerOptions", ex);
                return false;
            }
        }
    }
}
