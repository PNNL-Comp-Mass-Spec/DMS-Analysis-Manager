using AnalysisManagerBase;
using PRISMDatabaseUtils;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace AnalysisManagerMSFraggerPlugIn;

public static class ReporterIonInfo
{
    /// <summary>
    /// Reporter ion modes
    /// </summary>
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

    /// <summary>
    /// Create a file that defines alias names for reporter ions
    /// </summary>
    /// <param name="reporterIonMode">Reporter ion mode</param>
    /// <param name="aliasNameFile">Alias name file (aka annotation.txt file)</param>
    /// <param name="sampleNamePrefix">Sample name prefix</param>
    /// <returns>File info for the created file</returns>
    public static FileInfo CreateReporterIonAnnotationFile(ReporterIonModes reporterIonMode, FileInfo aliasNameFile, string sampleNamePrefix)
    {
        using var writer = new StreamWriter(new FileStream(aliasNameFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

        var reporterIonNames = GetReporterIonNames(reporterIonMode);

        // Example output (space-delimited):
        // 126 sample-01
        // 127N sample-02
        // 127C sample-03
        // 128N sample-04

        // If sampleNamePrefix is not an empty string, it will be included before each sample name, e.g.
        // 126 QC_Shew_20_01_R01_sample-01
        // 127N QC_Shew_20_01_R01_sample-02
        // 127C QC_Shew_20_01_R01_sample-03
        // 128N QC_Shew_20_01_R01_sample-04

        if (string.IsNullOrWhiteSpace(sampleNamePrefix))
        {
            sampleNamePrefix = string.Empty;
        }
        else if (!sampleNamePrefix.EndsWith("_"))
        {
            sampleNamePrefix += "_";
        }

        var sampleNumber = 0;

        foreach (var reporterIon in reporterIonNames)
        {
            sampleNumber++;
            writer.WriteLine("{0} {1}sample-{2:D2}", reporterIon, sampleNamePrefix, sampleNumber);
        }

        aliasNameFile.Refresh();
        return aliasNameFile;
    }

    /// <summary>
    /// Resolve reporter ion mode name to the reporter ion mode enum
    /// </summary>
    /// <param name="reporterIonModeName">Reporter ion mode name</param>
    /// <returns>Reporter ion mode enum</returns>
    public static ReporterIonModes DetermineReporterIonMode(string reporterIonModeName)
    {
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
    /// Obtain a dictionary mapping the experiment group names to abbreviated versions, assuring that each abbreviation is unique
    /// </summary>
    /// <remarks>If there is only one experiment group, the dictionary will have an empty string for the abbreviated name</remarks>
    /// <param name="experimentGroupNames">Experiment group name</param>
    /// <returns>Dictionary mapping experiment group name to abbreviated name</returns>
    public static Dictionary<string, string> GetAbbreviatedExperimentGroupNames(IReadOnlyList<string> experimentGroupNames)
    {
        var experimentGroupNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        switch (experimentGroupNames.Count)
        {
            case 0:
                return experimentGroupNameMap;

            case 1:
                experimentGroupNameMap.Add(experimentGroupNames[0], string.Empty);
                return experimentGroupNameMap;
        }

        var uniquePrefixTool = new ShortestUniquePrefix();

        // Keys in this dictionary are experiment group names
        // Values are the abbreviated name to use
        return uniquePrefixTool.GetShortestUniquePrefix(experimentGroupNames, true);
    }

    /// <summary>
    /// Get the number of channels used by the given reporter ion
    /// </summary>
    /// <param name="reporterIonMode">Reporter ion mode</param>
    /// <returns>Channel count</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static byte GetReporterIonChannelCount(ReporterIonModes reporterIonMode)
    {
        return reporterIonMode switch
        {
            ReporterIonModes.Itraq4 => 4,
            ReporterIonModes.Itraq8 => 8,
            ReporterIonModes.Tmt6 => 6,
            ReporterIonModes.Tmt10 => 10,
            ReporterIonModes.Tmt11 => 11,
            ReporterIonModes.Tmt16 => 16,
            ReporterIonModes.Tmt18 => 18,
            ReporterIonModes.Disabled => 0,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    /// <summary>
    /// Query the database to determine the reporter ion mode(s) associated with the given experiments
    /// If any are 18-plex TMT, set reporterIonModeToUse to ReporterIonModes.Tmt18
    /// </summary>
    /// <param name="dbTools">DB Tools instance</param>
    /// <param name="experimentNames">List of experiment names</param>
    /// <param name="reporterIonMode">Current reporter ion mode</param>
    /// <param name="message">Output: status or error message</param>
    /// <param name="reporterIonModeToUse">Output: reporter ion mode to use</param>
    /// <returns>True if successful, false if an error</returns>
    public static bool GetReporterIonModeForExperiments(
        IDBTools dbTools,
        List<string> experimentNames,
        ReporterIonModes reporterIonMode,
        out string message,
        out ReporterIonModes reporterIonModeToUse)
    {
        try
        {
            // Keys in this dictionary are experiment names, values are quoted experiment names
            var quotedExperimentNames = new Dictionary<string, string>();

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var experiment in experimentNames)
            {
                if (quotedExperimentNames.ContainsKey(experiment))
                    continue;

                quotedExperimentNames.Add(experiment, string.Format("'{0}'", experiment));
            }

            var sqlStr = new StringBuilder();

            sqlStr.Append("SELECT labelling, COUNT(*) AS experiments ");
            sqlStr.Append("FROM V_Experiment_Export ");
            sqlStr.AppendFormat("WHERE experiment IN ({0}) ", string.Join(",", quotedExperimentNames.Values));
            sqlStr.Append("GROUP BY labelling");

            var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

            if (!success)
            {
                message = "Error querying V_Experiment_Export in UpdateReporterIonModeIfRequired";
                reporterIonModeToUse = reporterIonMode;
                return false;
            }

            var experimentLabels = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            foreach (DataRow curRow in resultSet.Rows)
            {
                var labelName = curRow[0].CastDBVal<string>();

                if (experimentLabels.TryGetValue(labelName, out var experimentCount))
                {
                    experimentLabels[labelName] = experimentCount + 1;
                    continue;
                }

                experimentLabels.Add(labelName, 1);
            }

            if (experimentLabels.ContainsKey("TMT18"))
            {
                reporterIonModeToUse = ReporterIonModes.Tmt18;
            }
            else
            {
                reporterIonModeToUse = reporterIonMode;
            }

            if (reporterIonMode != reporterIonModeToUse)
            {
                message = string.Format("Changing the reporter ion mode from {0} to {1} based on experiment labelling info", reporterIonMode, reporterIonModeToUse);
            }
            else
            {
                message = string.Empty;
            }

            return true;
        }
        catch (Exception ex)
        {
            message = string.Format("Error in GetReporterIonModeForExperiments: {0}", ex.Message);
            reporterIonModeToUse = reporterIonMode;
            return false;
        }
    }

    /// <summary>
    /// Examine the modification mass to see if it matches a known reporter ion mass
    /// </summary>
    /// <param name="modMass">Modification mass</param>
    /// <returns>Reporter ion mode enum</returns>
    public static ReporterIonModes GetReporterIonModeFromModMass(double modMass)
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
    /// Return the reporter ion names for a given reporter ion mode
    /// </summary>
    /// <param name="reporterIonMode">Report ion mode enum</param>
    /// <returns>List of reporter ion names</returns>
    public static List<string> GetReporterIonNames(ReporterIonModes reporterIonMode)
    {
        var reporterIonNames = new List<string>();

        switch (reporterIonMode)
        {
            case ReporterIonModes.Tmt6:
                reporterIonNames.Add("126");
                reporterIonNames.Add("127N");
                reporterIonNames.Add("128C");
                reporterIonNames.Add("129N");
                reporterIonNames.Add("130C");
                reporterIonNames.Add("131");
                return reporterIonNames;

            case ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11 or ReporterIonModes.Tmt16 or ReporterIonModes.Tmt18:
                reporterIonNames.Add("126");
                reporterIonNames.Add("127N");
                reporterIonNames.Add("127C");
                reporterIonNames.Add("128N");
                reporterIonNames.Add("128C");
                reporterIonNames.Add("129N");
                reporterIonNames.Add("129C");
                reporterIonNames.Add("130N");
                reporterIonNames.Add("130C");
                reporterIonNames.Add("131N");

                if (reporterIonMode == ReporterIonModes.Tmt10)
                    return reporterIonNames;

                // TMT 11, TMT 16, and TMT 18
                reporterIonNames.Add("131C");

                if (reporterIonMode == ReporterIonModes.Tmt11)
                    return reporterIonNames;

                // TMT 16 and TMT 18
                reporterIonNames.Add("132N");
                reporterIonNames.Add("132C");
                reporterIonNames.Add("133N");
                reporterIonNames.Add("133C");
                reporterIonNames.Add("134N");

                if (reporterIonMode == ReporterIonModes.Tmt16)
                    return reporterIonNames;

                // TMT 18
                reporterIonNames.Add("134C");
                reporterIonNames.Add("135N");

                return reporterIonNames;
        }

        if (reporterIonMode != ReporterIonModes.Itraq4 && reporterIonMode != ReporterIonModes.Itraq8)
        {
            return reporterIonNames;
        }

        if (reporterIonMode == ReporterIonModes.Itraq8)
        {
            // 8-plex iTRAQ
            reporterIonNames.Add("113");
        }

        if (reporterIonMode is ReporterIonModes.Itraq4 or ReporterIonModes.Itraq8)
        {
            // 4-plex and 8-plex iTRAQ
            reporterIonNames.Add("114");
            reporterIonNames.Add("115");
            reporterIonNames.Add("116");
            reporterIonNames.Add("117");
        }

        if (reporterIonMode != ReporterIonModes.Itraq8)
        {
            return reporterIonNames;
        }

        // 8-plex iTRAQ
        reporterIonNames.Add("118");
        reporterIonNames.Add("119");
        reporterIonNames.Add("121");

        return reporterIonNames;
    }
}