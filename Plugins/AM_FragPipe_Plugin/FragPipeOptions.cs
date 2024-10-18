using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerFragPipePlugin;
using AnalysisManagerMSFraggerPlugIn;
using PRISM;
using PRISM.AppSettings;

namespace AnalysisManagerFragPipePlugIn
{
    // Ignore Spelling: Frag, workflow

    public class FragPipeOptions : EventNotifier
    {
        private const string C_TERM_PEPTIDE = "Cterm_peptide";
        private const string N_TERM_PEPTIDE = "Nterm_peptide";
        private const string C_TERM_PROTEIN = "Cterm_protein";
        private const string N_TERM_PROTEIN = "Nterm_protein";

        private const string FIX_MODS_PARAMETER_NAME = "msfragger.table.fix-mods";
        private const string VAR_MODS_PARAMETER_NAME = "msfragger.table.var-mods";

        private readonly IJobParams mJobParams;

        /// <summary>
        /// Number of datasets in the data package for this job
        /// </summary>
        /// <remarks>If no data package, this will be 1</remarks>
        public int DatasetCount { get; }

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
        /// Reporter ion mode defined in the parameter file
        /// </summary>
        public ReporterIonInfo.ReporterIonModes ReporterIonMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams">Job Parameters</param>
        /// <param name="datasetCount">Dataset count</param>
        public FragPipeOptions(IJobParams jobParams, int datasetCount)
        {
            mJobParams = jobParams;

            DatasetCount = Math.Max(datasetCount, 1);

            StaticModifications = new Dictionary<string, SortedSet<double>>();
            VariableModifications = new Dictionary<string, SortedSet<double>>();
        }

        private void AddParameterToValidate(
            IDictionary<string, BooleanParameter> booleanParametersToValidate,
            string parameterName,
            bool required = true)
        {
            booleanParametersToValidate.Add(parameterName, new BooleanParameter(parameterName, required));
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
        /// Examine the dynamic and static mods loaded from a FragPipe workflow file to determine the reporter ion mode
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

                if (staticModifications.TryGetValue(N_TERM_PEPTIDE, out var staticNTermModMasses) && staticNTermModMasses.Count > 0)
                {
                    staticNTermMode = GetReporterIonModeFromMassList(staticNTermModMasses);
                }
                else
                {
                    staticNTermMode = ReporterIonInfo.ReporterIonModes.Disabled;
                }

                if (staticModifications.TryGetValue("K", out var staticLysineModMasses) && staticLysineModMasses.Count > 0)
                {
                    staticLysineMode = GetReporterIonModeFromMassList(staticLysineModMasses);
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

                OnErrorEvent("The FragPipe workflow file has more than one reporter ion mode defined: {0}", string.Join(", ", matchedReporterIonModes.Keys.ToList()));
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

        private string GetFirstAffectedResidue(IReadOnlyList<string> affectedResidues)
        {
            if (affectedResidues.Count == 0)
                return string.Empty;

            var affectedResidue = affectedResidues[0].Trim();

            // Amino acid static mods have both the symbol and the name, e.g. "K (lysine)"
            // Only return the symbol (note that the calling method should have already removed the space and any following text)

            if (affectedResidue.Length <= 1)
                return affectedResidue;

            if (affectedResidue[1].Equals(' '))
                return affectedResidue[0].ToString();

            return affectedResidue;
        }

        /// <summary>
        /// Examine the FragPipe workflow parameters to determine the static and dynamic (variable) modifications
        /// </summary>
        /// <remarks>
        /// <para>Keys in the output dictionaries are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be a description of the peptide or protein terminus (see <see cref="C_TERM_PEPTIDE"/></para> and similar constants)
        /// </remarks>
        /// <param name="workflowEntries">Parameter file entries loaded from the FragPipe workflow file</param>
        /// <param name="staticModsByResidue">Output: dictionary of static modifications, by residue or position</param>
        /// <param name="variableModsByResidue">Output: dictionary of dynamic modifications, by residue or position</param>
        /// <returns>True if modifications were successfully parsed, false if an error</returns>
        private bool GetMSFraggerModifications(
            IEnumerable<KeyValuePair<string, string>> workflowEntries,
            out Dictionary<string, SortedSet<double>> staticModsByResidue,
            out Dictionary<string, SortedSet<double>> variableModsByResidue)
        {
            staticModsByResidue = new Dictionary<string, SortedSet<double>>();
            variableModsByResidue = new Dictionary<string, SortedSet<double>>();

            // ReSharper disable StringLiteralTypo

            var terminalStaticModParameters = new Dictionary<string, string>
            {
                { "C-Term Peptide", C_TERM_PEPTIDE },
                { "N-Term Peptide", N_TERM_PEPTIDE },
                { "C-Term Protein", C_TERM_PROTEIN },
                { "N-Term Protein", N_TERM_PROTEIN }
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

            foreach (var parameter in workflowEntries)
            {
                var validMods = parameter.Key switch
                {
                    FIX_MODS_PARAMETER_NAME => ParseMSFraggerStaticMods(staticModsByResidue, terminalStaticModParameters, aminoAcidSymbols, parameter.Value),
                    VAR_MODS_PARAMETER_NAME => ParseMSFraggerDynamicMods(variableModsByResidue, parameter.Value),
                    _ => true
                };

                if (!validMods)
                    return false;
            }

            return true;
        }

        private bool GetParamValueBool(KeyValuePair<string, string> parameter, out bool value)
        {
            if (bool.TryParse(parameter.Value, out value))
                return true;

            OnErrorEvent("Parameter value in FragPipe workflow file is not true or false: {0} = {1}", parameter.Key, parameter.Value);

            return false;
        }

        private bool GetParamValueInt(KeyValuePair<string, string> parameter, out int value)
        {
            if (int.TryParse(parameter.Value, out value))
                return true;

            OnErrorEvent("Parameter value in FragPipe workflow file is not numeric: {0} = {1}", parameter.Key, parameter.Value);

            return false;
        }

        /// <summary>
        /// Examine the modification masses to look for a known reporter ion mod mass
        /// </summary>
        /// <param name="modMasses">List of static or dynamic modification masses</param>
        /// <returns>Reporter ion mode</returns>
        private ReporterIonInfo.ReporterIonModes GetReporterIonModeFromMassList(SortedSet<double> modMasses)
        {
            foreach (var modMass in modMasses)
            {
                var reporterIonMode = ReporterIonInfo.GetReporterIonModeFromModMass(modMass);

                if (reporterIonMode != ReporterIonInfo.ReporterIonModes.Disabled)
                {
                    return reporterIonMode;
                }
            }

            return ReporterIonInfo.ReporterIonModes.Disabled;
        }

        /// <summary>
        /// Return true if the value is an empty string or the word "auto"
        /// </summary>
        /// <param name="value">Value</param>
        public bool IsUndefinedOrAuto(string value)
        {
            return string.IsNullOrWhiteSpace(value) || value.Equals("auto", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Parse the FragPipe workflow file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="workflowFilePath">Workflow file path</param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadFragPipeOptions(string workflowFilePath)
        {
            StaticModifications.Clear();
            VariableModifications.Clear();

            try
            {
                var workflowFile = new FileInfo(workflowFilePath);

                var workflowFileReader = new KeyValueParamFileReader("FragPipe", workflowFile.DirectoryName, workflowFile.Name);
                RegisterEvents(workflowFileReader);

                var workflowFileLoaded = workflowFileReader.ParseKeyValueParameterFile(out var workflowEntries, true);

                if (!workflowFileLoaded)
                {
                    return false;
                }

                var validMods = GetMSFraggerModifications(
                    workflowEntries,
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

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadFragPipeOptions", ex);
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

        private bool ParseMSFraggerDynamicMods(
            IDictionary<string, SortedSet<double>> variableModsByResidue,
            string dynamicModList)
        {
            foreach (var dynamicMod in dynamicModList.Split(';'))
            {
                if (!ParseModDefinition(VAR_MODS_PARAMETER_NAME, dynamicMod, out var modEnabled, out var dynamicModMass, out var affectedResidues))
                    return false;

                if (!modEnabled)
                    continue;

                if (dynamicModMass == 0)
                {
                    OnErrorEvent("Variable modification mass in FragPipe workflow file is zero; {0}: {1}", VAR_MODS_PARAMETER_NAME, dynamicMod);
                    return false;
                }

                foreach (var residue in affectedResidues)
                {
                    AppendModificationMass(variableModsByResidue, residue, dynamicModMass);
                }
            }

            return true;
        }

        private bool ParseMSFraggerStaticMods(
            IDictionary<string, SortedSet<double>> staticModsByResidue,
            Dictionary<string, string> terminalStaticModParameters,
            SortedSet<string> aminoAcidSymbols,
            string staticModList)
        {
            var matchFound = false;

            foreach (var staticMod in staticModList.Split(';'))
            {
                if (!ParseModDefinition(FIX_MODS_PARAMETER_NAME, staticMod, terminalStaticModParameters, out var modEnabled, out var modMass, out var affectedResidues))
                    return false;

                if (!modEnabled)
                    continue;

                var affectedResidue = GetFirstAffectedResidue(affectedResidues);

                if (string.IsNullOrWhiteSpace(affectedResidue))
                {
                    OnErrorEvent("Static modification definition in FragPipe workflow file does not have an affected residue; {0}: {1}",
                        FIX_MODS_PARAMETER_NAME, staticMod);

                    return false;
                }

                foreach (var staticModParameter in terminalStaticModParameters)
                {
                    if (!affectedResidue.Equals(staticModParameter.Key))
                        continue;

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
                    if (!affectedResidue.Equals(aminoAcidSymbol))
                        continue;

                    if (modMass != 0)
                    {
                        AppendModificationMass(staticModsByResidue, aminoAcidSymbol, modMass);
                    }

                    matchFound = true;
                    break;
                }
            }

            return true;
        }

        /// <summary>
        /// Parse a static or dynamic mod parameter to determine the modification mass, and (if applicable) the affected residues
        /// </summary>
        /// <param name="modParameterName">Modification parameter name ("msfragger.table.fix-mods" or "msfragger.table.var-mods")</param>
        /// <param name="modDefinition">Modification definition</param>
        /// <param name="modEnabled">Output: true if the modification is enabled, otherwise false</param>
        /// <param name="modMass">Output: modification mass</param>
        /// <param name="affectedResidues">Output: list of affected residues</param>
        /// <returns>True if success, false if an error</returns>
        private bool ParseModDefinition(
            string modParameterName,
            string modDefinition,
            out bool modEnabled,
            out double modMass,
            out List<string> affectedResidues)
        {
            return ParseModDefinition(
                modParameterName,
                modDefinition,
                new Dictionary<string, string>(),
                out modEnabled,
                out modMass,
                out affectedResidues);
        }

        /// <summary>
        /// Parse a static or dynamic mod parameter to determine the modification mass, and (if applicable) the affected residues
        /// </summary>
        /// <param name="modParameterName">Modification parameter name ("msfragger.table.fix-mods" or "msfragger.table.var-mods")</param>
        /// <param name="modDefinition">Modification definition</param>
        /// <param name="terminalStaticModParameters">Terminal static mod parameters</param>
        /// <param name="modEnabled">Output: true if the modification is enabled, otherwise false</param>
        /// <param name="modMass">Output: modification mass</param>
        /// <param name="affectedResidues">Output: list of affected residues</param>
        /// <returns>True if success, false if an error</returns>
        private bool ParseModDefinition(
            string modParameterName,
            string modDefinition,
            Dictionary<string, string> terminalStaticModParameters,
            out bool modEnabled,
            out double modMass,
            out List<string> affectedResidues)
        {
            // ReSharper disable CommentTypo

            // Example dynamic mods from parameter msfragger.table.var-mods
            // Format is Mass, AffectedResidues, Enabled (True or False), MaxOccurrences
            // 15.9949,M,true,3         # Oxidized methionine
            // 42.0106,[^,false,1       # Acetylation protein N-term (not enabled)
            // 79.96633,STY,true,3      # Phosphorylated STY
            // -17.0265,nQnC,false,1
            // -18.0106,nE,false,1
            // 304.20715,n^,false,1
            // 229.16293,S,false,1

            // Example static mods:
            // Format is Mass, AffectedResidues, Enabled (True or False), MaxOccurrences (always -1 for static mods)
            // 0.0,C-Term Peptide,true,-1;
            // 304.20715,N-Term Peptide,true,-1;
            // 0.0,C-Term Protein,true,-1;
            // 0.0,N-Term Protein,true,-1;
            // 0.0,G (glycine),true,-1;
            // 57.02146,C (cysteine),true,-1;
            // 304.20715,K (lysine),true,-1;

            // ReSharper restore CommentTypo

            var modParts = modDefinition.Trim().Split(',');

            if (modParts.Length < 4)
            {
                OnErrorEvent("Modification definition in FragPipe workflow file does not have four parts; {0}: {1}",
                    modParameterName, modDefinition);

                modEnabled = false;
                modMass = 0;
                affectedResidues = new List<string>();
                return false;
            }

            if (!bool.TryParse(modParts[2], out modEnabled))
            {
                OnErrorEvent("Modification definition in FragPipe workflow file does not have true or false for the enabled value (3rd part of the definition); {0}: {1}",
                    modParameterName, modDefinition);

                modMass = 0;
                affectedResidues = new List<string>();
                return false;
            }

            if (!modEnabled)
            {
                modMass = 0;
                affectedResidues = new List<string>();
                return true;
            }

            if (!double.TryParse(modParts[0], out modMass))
            {
                OnErrorEvent("Modification mass in FragPipe workflow file is not numeric; {0}: {1}", modParameterName, modDefinition);
                affectedResidues = new List<string>();
                return false;
            }

            foreach (var staticModParameter in terminalStaticModParameters)
            {
                if (!modParts[1].Equals(staticModParameter.Key))
                    continue;

                affectedResidues = new List<string>
                {
                    modParts[1]
                };

                return true;
            }

            var spaceIndex = modParts[1].IndexOf(' ');

            affectedResidues = ParseAffectedResidueList(spaceIndex > 0 ? modParts[1].Substring(0, spaceIndex) : modParts[1]);

            return true;
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
        /// Examine FragPipe parameters to check for errors
        /// </summary>
        /// <param name="workflowFile">FragPipe workflow file</param>
        /// <returns>True if no problems, false if errors</returns>
        public bool ValidateFragPipeOptions(FileInfo workflowFile)
        {
            try
            {
                if (!workflowFile.Exists)
                {
                    OnErrorEvent("FragPipe workflow file not found: " + workflowFile.FullName);
                    return false;
                }

                var workflowFileReader = new KeyValueParamFileReader("FragPipe", workflowFile.DirectoryName, workflowFile.Name);
                RegisterEvents(workflowFileReader);

                var workflowFileLoaded = workflowFileReader.ParseKeyValueParameterFile(out var workflowEntries, true);

                if (!workflowFileLoaded)
                {
                    return false;
                }

                var booleanParametersToValidate = new Dictionary<string, BooleanParameter>();
                var parametersToValidate = new Dictionary<string, IntegerParameter>();

                AddParameterToValidate(parametersToValidate, "msfragger.precursor_mass_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "msfragger.precursor_true_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "msfragger.fragment_mass_units", 0, 1);
                AddParameterToValidate(parametersToValidate, "msfragger.calibrate_mass", 0, 2);
                AddParameterToValidate(booleanParametersToValidate, "use_all_mods_in_first_search");
                AddParameterToValidate(parametersToValidate, "msfragger.deisotope", 0, 2);

                // ReSharper disable once StringLiteralTypo
                AddParameterToValidate(parametersToValidate, "msfragger.deneutralloss", 0, 1);

                AddParameterToValidate(parametersToValidate, "msfragger.remove_precursor_peak", 0, 2);
                AddParameterToValidate(parametersToValidate, "msfragger.intensity_transform", 0, 1);

                AddParameterToValidate(booleanParametersToValidate, "msfragger.write_calibrated_mzml");
                AddParameterToValidate(booleanParametersToValidate, "msfragger.write_uncalibrated_mgf");

                AddParameterToValidate(parametersToValidate, "msfragger.mass_diff_to_variable_mod", 0, 2);
                AddParameterToValidate(booleanParametersToValidate, "msfragger.localize_delta_mass");
                AddParameterToValidate(parametersToValidate, "msfragger.num_enzyme_termini", 0, 2);
                AddParameterToValidate(parametersToValidate, "msfragger.allowed_missed_cleavage_1", 1, 5);
                AddParameterToValidate(parametersToValidate, "msfragger.allowed_missed_cleavage_2", 1, 5);
                AddParameterToValidate(booleanParametersToValidate, "msfragger.clip_nTerm_M");
                AddParameterToValidate(parametersToValidate, "msfragger.max_variable_mods_per_peptide", 1, 10);
                AddParameterToValidate(parametersToValidate, "msfragger.max_variable_mods_combinations", 1000, 65534);
                AddParameterToValidate(parametersToValidate, "msfragger.output_report_topN", 1, 10);
                AddParameterToValidate(booleanParametersToValidate, "msfragger.report_alternative_proteins");
                AddParameterToValidate(booleanParametersToValidate, "msfragger.override_charge");
                AddParameterToValidate(parametersToValidate, "msfragger.digest_min_length", 5, 20);
                AddParameterToValidate(parametersToValidate, "msfragger.digest_max_length", 20, 60);
                AddParameterToValidate(parametersToValidate, "msfragger.max_fragment_charge", 1, 4);

                foreach (var parameter in workflowEntries)
                {
                    if (!parametersToValidate.TryGetValue(parameter.Key, out var matchingParameter))
                    {
                        continue;
                    }

                    if (!GetParamValueInt(parameter, out var parameterValue))
                        return false;

                    matchingParameter.SetValue(parameterValue);
                }

                foreach (var parameter in workflowEntries)
                {
                    if (!booleanParametersToValidate.TryGetValue(parameter.Key, out var matchingParameter))
                    {
                        continue;
                    }

                    if (!GetParamValueBool(parameter, out var parameterValue))
                        return false;

                    matchingParameter.SetValue(parameterValue);
                }

                foreach (var item in parametersToValidate)
                {
                    if (!item.Value.IsDefined)
                    {
                        if (!item.Value.Required)
                            continue;

                        OnErrorEvent("Parameter {0} is missing from the FragPipe workflow file", item.Key);
                        return false;
                    }

                    if (!ParameterValueInRange(item.Value))
                        return false;
                }

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var item in booleanParametersToValidate)
                {
                    if (item.Value.IsDefined)
                        continue;

                    if (!item.Value.Required)
                        continue;

                    OnErrorEvent("Parameter {0} is missing from the FragPipe workflow file", item.Key);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFragPipeOptions", ex);
                return false;
            }
        }
    }
}
