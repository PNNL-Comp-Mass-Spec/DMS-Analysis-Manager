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
    // ReSharper disable once CommentTypo
    // Ignore Spelling: acetylation, deisotope, deneutralloss, nc, plex, quantitation

    public enum MS1ValidationModes
    {
        Disabled = 0,
        PeptideProphet = 1,
        Percolator = 2
    }

    public enum ReporterIonModes
    {
        Disabled = 0,
        Itraq4 = 1,
        Itraq8 = 2,
        Tmt6 = 3,
        Tmt10 = 4,
        Tmt11 = 5,
        Tmt16 = 6,
        Tmt18 = 7
    }

    public class MSFraggerOptions : EventNotifier
    {
        private readonly IJobParams mJobParams;

        /// <summary>
        /// This is set to true if data_type is 1 or 2, meaning searching DIA data
        /// </summary>
        public bool DIASearchEnabled { get; set; }

        /// <summary>
        /// Whether to run PeptideProphet, Percolator, or nothing
        /// </summary>
        /// <remarks>Defaults to Percolator, but auto-set to PeptideProphet if iTRAQ is in use</remarks>
        public MS1ValidationModes MS1ValidationMode { get; set; }

        /// <summary>
        /// True when the MS1 validation mode was auto-defined (since job parameters RunPeptideProphet and/or RunPercolator were not present)
        /// </summary>
        public bool MS1ValidationModeAutoDefined { get; set; }

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
        public ReporterIonModes ReporterIonMode { get; set; }

        /// <summary>
        /// Whether or not to run FreeQuant
        /// </summary>
        /// <remarks>
        /// Defaults to false, but forced to true if reporter ions are used
        /// If no reporter ions, RunFreeQuant is ignored if RunIonQuant is enabled</remarks>
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
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        public MSFraggerOptions(IJobParams jobParams)
        {
            mJobParams = jobParams;
        }

        private void AddParameterToValidate(IDictionary<string, IntegerParameter> parametersToValidate, string parameterName, int minValue, int maxValue)
        {
            parametersToValidate.Add(parameterName, new IntegerParameter(parameterName, minValue, maxValue));
        }

        /// <summary>
        /// Examine the dynamic and static mods loaded from a MSFragger parameter file to determine the reporter ion mode
        /// </summary>
        /// <param name="paramFileEntries"></param>
        /// <param name="reporterIonMode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool DetermineReporterIonMode(IEnumerable<KeyValuePair<string, string>> paramFileEntries, out ReporterIonModes reporterIonMode)
        {
            reporterIonMode = ReporterIonModes.Disabled;

            try
            {
                var staticNTermModMass = 0.0;
                var staticLysineModMass = 0.0;

                // Keys in this dictionary are modification masses; values are a list of the affected residues
                var variableModMasses = new Dictionary<double, List<string>>();

                foreach (var parameter in paramFileEntries)
                {
                    // ReSharper disable once StringLiteralTypo
                    if (parameter.Key.Equals("add_Nterm_peptide"))
                    {
                        if (!ParseModMass(parameter, out staticNTermModMass, out _))
                            return false;

                        continue;
                    }

                    if (parameter.Key.Equals("add_K_lysine"))
                    {
                        if (!ParseModMass(parameter, out staticLysineModMass, out _))
                            return false;

                        continue;
                    }

                    if (!parameter.Key.StartsWith("variable_mod"))
                    {
                        continue;
                    }

                    if (!ParseModMass(parameter, out var dynamicModMass, out var affectedResidues))
                        return false;

                    if (variableModMasses.TryGetValue(dynamicModMass, out var existingResidueList))
                    {
                        existingResidueList.AddRange(affectedResidues);
                        continue;
                    }

                    variableModMasses.Add(dynamicModMass, affectedResidues);
                }

                var staticNTermMode = GetReporterIonModeFromModMass(staticNTermModMass);
                var staticLysineMode = GetReporterIonModeFromModMass(staticLysineModMass);

                var dynamicModModes = new Dictionary<double, ReporterIonModes>();

                foreach (var item in variableModMasses)
                {
                    dynamicModModes.Add(item.Key, GetReporterIonModeFromModMass(item.Key));

                    // If necessary, we could examine the affected residues to override the auto-determined mode
                    // var affectedResidues = item.Value;
                }

                var reporterIonModeStats = new Dictionary<ReporterIonModes, int>();

                UpdateReporterIonModeStats(reporterIonModeStats, staticNTermMode);
                UpdateReporterIonModeStats(reporterIonModeStats, staticLysineMode);
                UpdateReporterIonModeStats(reporterIonModeStats, dynamicModModes.Values.ToList());

                var matchedReporterIonModes = new Dictionary<ReporterIonModes, int>();
                foreach (var item in reporterIonModeStats)
                {
                    if (item.Key != ReporterIonModes.Disabled && item.Value > 0)
                    {
                        matchedReporterIonModes.Add(item.Key, item.Value);
                    }
                }

                if (matchedReporterIonModes.Count == 0)
                {
                    reporterIonMode = ReporterIonModes.Disabled;
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
        /// <see cref="GetReporterIonModeFromModMass"/> sets the ReporterIonMode to Tmt11 for 6-plex, 10-plex, and 11-plex TMT.
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
        /// <param name="reporterIonMode"></param>
        private ReporterIonModes DetermineReporterIonMode(ReporterIonModes reporterIonMode)
        {
            if (reporterIonMode != ReporterIonModes.Tmt11 && reporterIonMode != ReporterIonModes.Tmt16)
                return reporterIonMode;

            // Look for a job parameter that specifies the reporter ion mode
            var reporterIonModeName = mJobParams.GetJobParameter("ReporterIonMode", string.Empty);

            // The standard settings files have <item key="ReporterIonMode" value="auto">
            // Check for this, and if found, simply return reporterIonMode
            if (IsUndefinedOrAuto(reporterIonModeName))
            {
                return reporterIonMode;
            }

            return reporterIonModeName.ToLower() switch
            {
                "tmt6" => ReporterIonModes.Tmt6,
                "6-plex" => ReporterIonModes.Tmt6,
                "6plex" => ReporterIonModes.Tmt6,
                "tmt10" => ReporterIonModes.Tmt10,
                "10-plex" => ReporterIonModes.Tmt10,
                "10plex" => ReporterIonModes.Tmt10,
                "tmt11" => ReporterIonModes.Tmt11,
                "11-plex" => ReporterIonModes.Tmt11,
                "11plex" => ReporterIonModes.Tmt11,
                "tmt16" => ReporterIonModes.Tmt16,
                "16-plex" => ReporterIonModes.Tmt16,
                "16plex" => ReporterIonModes.Tmt16,
                "tmt18" => ReporterIonModes.Tmt18,
                "18-plex" => ReporterIonModes.Tmt18,
                "18plex" => ReporterIonModes.Tmt18,
                _ => ReporterIonModes.Tmt16
            };
        }

        /// <summary>
        /// Look for text in affectedResidueList
        /// For each match found, append to affectedResidues
        /// </summary>
        /// <param name="affectedResidueList"></param>
        /// <param name="residueMatcher"></param>
        /// <param name="affectedResidues"></param>
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
        /// Otherwise, parses as a boolean and returns the result if "true" or "false" or "1" or "0"
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

            OnWarningEvent("Parameter {0} should be True, False, 1, 0, or Auto, but it is {1}", parameterName, value);
            return defaultValue;
        }

        private ReporterIonModes GetReporterIonModeFromModMass(double modMass)
        {
            if (Math.Abs(modMass - 304.207146) < 0.001)
            {
                // 16-plex and 18-plex TMT
                // Use TMT 16 for now, though method DetermineReporterIonMode(ReporterIonModes reporterIonMode)
                // will look for a job parameter to override this
                return ReporterIonModes.Tmt16;
            }

            if (Math.Abs(modMass - 304.205353) < 0.001)
                return ReporterIonModes.Itraq8;

            if (Math.Abs(modMass - 144.102066) < 0.005)
                return ReporterIonModes.Itraq4;

            if (Math.Abs(modMass - 229.162933) < 0.005)
            {
                // 6-plex, 10-plex, and 11-plex TMT
                // Use TMT 11 for now, though method DetermineReporterIonMode(ReporterIonModes reporterIonMode)
                // will look for a job parameter to override this
                return ReporterIonModes.Tmt11;
            }

            return ReporterIonModes.Disabled;
        }

        /// <summary>
        /// Return true if the value is an empty string or the word "auto"
        /// </summary>
        /// <param name="value"></param>
        public bool IsUndefinedOrAuto(string value)
        {
            return string.IsNullOrWhiteSpace(value) || value.Equals("auto", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="paramFilePath"></param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadMSFraggerOptions(string paramFilePath)
        {
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

                var success = DetermineReporterIonMode(paramFileEntries, out var reporterIonMode);
                if (!success)
                    return false;

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
                            if (!GetParamValueInt(parameter, out precursorMassUnits))
                                return false;

                            break;

                        case "data_type":
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
                        if (MS1ValidationMode == MS1ValidationModes.Percolator &&
                            ReporterIonMode is ReporterIonModes.Itraq4 or ReporterIonModes.Itraq8)
                        {
                            // Switch from Percolator to PeptideProphet since using iTRAQ
                            MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                        }

                        if (MS1ValidationMode == MS1ValidationModes.PeptideProphet &&
                            ReporterIonMode is
                                ReporterIonModes.Tmt6 or ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11 or
                                ReporterIonModes.Tmt16 or ReporterIonModes.Tmt18)
                        {
                            // Switch from PeptideProphet to Percolator  since using TMT
                            MS1ValidationMode = MS1ValidationModes.Percolator;
                        }
                    }

                    if (QuantModeAutoDefined && ReporterIonMode != ReporterIonModes.Disabled)
                    {
                        // RunFreeQuant since TMT or iTRAQ is defined
                        RunFreeQuant = true;
                        RunIonQuant = false;
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
        /// <param name="parameter"></param>
        /// <param name="modMass"></param>
        /// <param name="affectedResidues"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ParseModMass(KeyValuePair<string, string> parameter, out double modMass, out List<string> affectedResidues)
        {
            // Example dynamic mods (format is Mass AffectedResidues MaxOccurrences):
            // variable_mod_01 = 15.994900 M 3        # Oxidized methionine
            // variable_mod_02 = 42.010600 [^ 1       # Acetylation protein N-term

            // Example static mods:
            // add_Nterm_peptide = 304.207146    # 16-plex TMT
            // add_K_lysine = 304.207146         # 16-plex TMT

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

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonModes, int> reporterIonModeStats, ReporterIonModes reporterIonMode)
        {
            UpdateReporterIonModeStats(reporterIonModeStats, new List<ReporterIonModes> { reporterIonMode });
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonModes, int> reporterIonModeStats, IEnumerable<ReporterIonModes> reporterIonModeList)
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
        /// <param name="paramFile"></param>
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
                AddParameterToValidate(parametersToValidate, "write_calibrated_mgf", 0, 1);
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
