using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
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
        Tmt16 = 6
    }

    internal class FragPipeOptions : EventNotifier
    {
        // Ignore Spelling: acetylation, nc, plex, quantitation

        private readonly IJobParams mJobParams;

        /// <summary>
        /// Number of datasets in the data package for this job
        /// </summary>
        /// <remarks>If no data package, this will be 1</remarks>
        public int DatasetCount { get; }

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
        public MS1ValidationModes MS1ValidationMode { get; set; }

        /// <summary>
        /// True when the MS1 validation mode was auto-defined (since job parameters RunPeptideProphet and/or RunPercolator were not present)
        /// </summary>
        public bool MS1ValidationModeAutoDefined { get; }

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

            DatasetCount = datasetCount;

            JavaProgLoc = string.Empty;

            LibraryFinder = new FragPipeLibFinder(philosopherExe);
            RegisterEvents(LibraryFinder);

            MatchBetweenRuns = mJobParams.GetJobParameter("MatchBetweenRuns", false);

            var runPeptideProphetJobParam = mJobParams.GetJobParameter("RunPeptideProphet", string.Empty);
            var runPercolatorJobParam = mJobParams.GetJobParameter("RunPercolator", string.Empty);

            if (IsUndefinedOrAuto(runPeptideProphetJobParam) && IsUndefinedOrAuto(runPercolatorJobParam))
            {
                // Use Percolator if match-between runs is enabled, otherwise use Peptide Prophet
                // This value will get changed by LoadMSFraggerOptions if using an open search or if TMT is defined as a dynamic or static mod
                MS1ValidationMode = MatchBetweenRuns ? MS1ValidationModes.Percolator : MS1ValidationModes.PeptideProphet;

                MS1ValidationModeAutoDefined = true;
            }
            else
            {
                var runPeptideProphet = !string.IsNullOrWhiteSpace(runPeptideProphetJobParam) && bool.Parse(runPeptideProphetJobParam);

                var runPercolator = !string.IsNullOrWhiteSpace(runPercolatorJobParam) && bool.Parse(runPercolatorJobParam);

                if (runPercolator)
                {
                    MS1ValidationMode = MS1ValidationModes.Percolator;
                }
                else if (runPeptideProphet)
                {
                    MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                }
                else
                {
                    MS1ValidationMode = MS1ValidationModes.Disabled;
                }

                MS1ValidationModeAutoDefined = false;
            }

            RunAbacus = mJobParams.GetJobParameter("RunAbacus", true);

            var ms1QuantDisabled = mJobParams.GetJobParameter("MS1QuantDisabled", false);

            if (ms1QuantDisabled)
            {
                RunFreeQuant = false;
                RunIonQuant = false;
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
                        RunFreeQuant = false;
                        RunIonQuant = true;
                        QuantModeAutoDefined = false;
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

                OnErrorEvent(string.Format(
                    "The MSFragger parameter file has more than one reporter ion mode defined: {0}",
                    string.Join(", ", matchedReporterIonModes.Keys.ToList())));

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in DetermineReporterIonMode", ex);
                return false;
            }
        }

        /// <summary>
        /// <see cref="GetReporterIonModeFromModMass"/> sets the ReporterIonMode to Tmt11 for 6-plex, 10-plex, and 11-plex TMT
        /// When ReporterIonMode is Tmt11, this method looks for job parameter ReporterIonMode to attempt to determine the actual TMT mode in use
        /// Otherwise, this method simply returns reporterIonMode
        /// </summary>
        /// <param name="reporterIonMode"></param>
        private ReporterIonModes DetermineReporterIonMode(ReporterIonModes reporterIonMode)
        {
            if (reporterIonMode != ReporterIonModes.Tmt11)
                return reporterIonMode;

            // Look for a job parameter that specifies the reporter ion mode
            var reporterIonModeName = mJobParams.GetJobParameter("ReporterIonMode", string.Empty);
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
                _ => ReporterIonModes.Tmt11
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

            OnErrorEvent(string.Format(
                "Parameter value in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        private bool GetParamValueInt(KeyValuePair<string, string> parameter, out int value)
        {
            if (int.TryParse(parameter.Value, out value))
                return true;

            OnErrorEvent(string.Format(
                "Parameter value in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        private ReporterIonModes GetReporterIonModeFromModMass(double modMass)
        {
            if (Math.Abs(modMass - 304.207146) < 0.001)
                return ReporterIonModes.Tmt16;

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
        private bool IsUndefinedOrAuto(string value)
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

                var paramFileReader = new PRISM.AppSettings.KeyValueParamFileReader("MSFragger", paramFile.DirectoryName, paramFile.Name);
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
                            MS1ValidationMode = MS1ValidationModes.PeptideProphet;
                        }

                        if (MS1ValidationMode == MS1ValidationModes.PeptideProphet &&
                            ReporterIonMode is ReporterIonModes.Tmt6 or ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11 or ReporterIonModes.Tmt16)
                        {
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

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadMSFraggerOptions", ex);
                return false;
            }
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
                    OnWarningEvent(string.Format(
                        "Affected residues not found after the modification mass for parameter '{0} = {1}' in the MSFragger parameter file",
                        parameter.Key, parameter.Value));
                }

                var affectedResidueList = spaceIndex2 > 0 ? remainingValue.Substring(0, spaceIndex2).Trim() : string.Empty;

                affectedResidues = ParseAffectedResidueList(affectedResidueList);
            }

            if (double.TryParse(parameterValue, out modMass))
            {
                return true;
            }

            OnErrorEvent(string.Format(
                "Modification mass in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        private void SetMS1QuantOptions(string runFreeQuantJobParam, string runIonQuantJobParam)
        {
            if (IsUndefinedOrAuto(runFreeQuantJobParam) && IsUndefinedOrAuto(runIonQuantJobParam))
            {
                RunFreeQuant = false;
                RunIonQuant = false;
                QuantModeAutoDefined = true;
                return;
            }

            // ReSharper disable once SimplifyConditionalTernaryExpression
            var runFreeQuant = IsUndefinedOrAuto(runFreeQuantJobParam)
                ? false
                : mJobParams.GetJobParameter("RunFreeQuant", false);

            if (runFreeQuant)
            {
                RunFreeQuant = true;
                RunIonQuant = false;
            }
            else
            {
                RunFreeQuant = false;

                // ReSharper disable once SimplifyConditionalTernaryExpression
                RunIonQuant = IsUndefinedOrAuto(runIonQuantJobParam)
                    ? false
                    : mJobParams.GetJobParameter("RunIonQuant", false);
            }

            QuantModeAutoDefined = false;
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
    }
}
