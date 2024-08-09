using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;
using PRISMDatabaseUtils;

namespace MASIC_ReporterIonObsStatsUploader
{
    public class ReporterIonStatsUploader : PRISM.FileProcessor.ProcessFilesBase
    {
        // Ignore Spelling: Traq, Glc, Az, labelling

        private const string STORE_REPORTER_ION_OBS_STATS_SP_NAME = "store_reporter_ion_obs_stats";

        /// <summary>
        /// Processing options
        /// </summary>
        private readonly StatsUploaderOptions Options;

        /// <summary>
        /// Look for an XML file with key 'ReporterIonMassMode' in section 'MasicExportOptions'
        /// </summary>
        /// <param name="inputFile"></param>
        /// <param name="masicParameterFilePath"></param>
        private bool FindParameterFile(FileInfo inputFile, out string masicParameterFilePath)
        {
            masicParameterFilePath = string.Empty;

            try
            {
                if (inputFile.Directory == null)
                {
                    OnErrorEvent("Cannot determine the parent directory of the input file, " + inputFile.FullName);
                    return false;
                }

                foreach (var xmlFile in inputFile.Directory.GetFiles("*.xml"))
                {
                    if (xmlFile.Name.EndsWith("_DatasetInfo.xml", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (xmlFile.Name.StartsWith("JobParameters_", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var masicSettings = new XmlSettingsFileAccessor();

                    if (!masicSettings.LoadSettings(xmlFile.FullName))
                    {
                        OnErrorEvent("Error loading XML file " + xmlFile.FullName);
                        return false;
                    }

                    if (!masicSettings.SectionPresent("MasicExportOptions"))
                    {
                        OnDebugEvent("XML file does not have section 'MasicExportOptions': " + xmlFile.Name);
                        continue;
                    }

                    masicSettings.GetParam("MasicExportOptions", "ReporterIonMassMode", 0, out var valueNotPresent);

                    if (valueNotPresent)
                    {
                        OnDebugEvent("XML file does not have key 'ReporterIonMassMode' in section 'MasicExportOptions': " + xmlFile.Name);
                        continue;
                    }

                    masicParameterFilePath = xmlFile.FullName;
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in FindParameterFile", ex);
                return false;
            }
        }

        private bool GetDatasetName(FileSystemInfo inputFile, out string datasetName)
        {
            var suffixesToMatch = new List<string>
            {
                "_MSMethod.txt",
                "_RepIonObsRate.png",
                "_RepIonObsRate.txt",
                "_RepIonStats.txt",
                "_ReporterIons.txt",
                "_ScanStats.txt",
                "_ScanStatsConstant.txt",
                "_ScanStatsEx.txt",
                // ReSharper disable once StringLiteralTypo
                "_SICstats.txt"
            };

            datasetName = string.Empty;

            // ReSharper disable once ForEachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var suffix in suffixesToMatch)
            {
                if (!inputFile.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                    continue;

                datasetName = inputFile.Name.Substring(0, inputFile.Name.Length - suffix.Length);
                return true;
            }

            OnErrorEvent("Unable to determine the dataset name from the input file name: " + inputFile.Name);
            OnWarningEvent("Supported filename suffixes: " + string.Join(", ", suffixesToMatch));

            return false;
        }

        /// <summary>
        /// Get the error message (returns the base class error message)
        /// </summary>
        public override string GetErrorMessage()
        {
            return GetBaseClassErrorMessage();
        }

        private int GetColumnIndex(string headerLine, string columnName, int indexIfMissing)
        {
            var columnNames = headerLine.Split('\t');

            for (var i = 0; i < columnNames.Length; i++)
            {
                if (columnNames[i].Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            OnWarningEvent("Header line does not contain column '{0}'; will presume the data is in column {1}", columnName, indexIfMissing + 1);

            return indexIfMissing;
        }

        /// <summary>
        /// Look for a file named JobParameters_1786663.xml
        /// Alternatively, extract the job from the directory name if of the form SIC202002211558_Auto1786663
        /// </summary>
        /// <param name="inputFile"></param>
        /// <param name="jobNumber"></param>
        private bool GetJobNumber(FileInfo inputFile, out int jobNumber)
        {
            jobNumber = 0;

            try
            {
                if (inputFile.Directory == null)
                {
                    OnErrorEvent("Cannot determine the parent directory of the input file, " + inputFile.FullName);
                    return false;
                }

                var jobMatcher = new Regex(@"JobParameters_(?<Job>\d+)\.xml", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var xmlFile in inputFile.Directory.GetFiles("JobParameters_*.xml"))
                {
                    var jobParametersMatch = jobMatcher.Match(xmlFile.Name);

                    if (!jobParametersMatch.Success)
                        continue;

                    jobNumber = int.Parse(jobParametersMatch.Groups["Job"].Value);
                    return true;
                }

                // The directory doesn't have a JobParameters file
                // Check whether the parent directory is of the form SIC202007311031_Auto1821865

                var directoryMatcher = new Regex(@"[^_]+_Auto(?<Job>\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var match = directoryMatcher.Match(inputFile.Directory.Name);

                if (match.Success)
                {
                    jobNumber = int.Parse(match.Groups["Job"].Value);
                    return true;
                }

                OnErrorEvent("Unable to determine the job number associated with the input file: " + inputFile.FullName);
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in FindParameterFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Get the DMS-compatible reporter ion name from the MASIC reporter ion mass mode
        /// </summary>
        /// <remarks>MASIC mass modes: https://github.com/PNNL-Comp-Mass-Spec/MASIC/blob/59474ab345ce7878f0646a6e83fa1bb22ee84579/ReporterIons.cs#L15
        /// </remarks>
        /// <param name="reporterIonMassMode">MASIC reporter ion mass mode</param>
        public static string GetReporterIonNameFromMassMode(int reporterIonMassMode)
        {
            switch (reporterIonMassMode)
            {
                case 1:
                    // ITraqFourMZ
                    return "iTRAQ";

                case 3:
                    // TMTTwoMZ
                    return "TMT2";

                case 4:
                    // TMTSixMZ
                    return "TMT6";

                case 5:
                    // ITraqEightMZHighRes
                    return "iTRAQ8";

                case 6:
                    // ITraqEightMZLowRes
                    return "iTRAQ8";

                case 10:
                    // TMTTenMZ
                    return "TMT10";

                case 11:
                    // OGlcNAc
                    return "PCGalNAz";

                case 16:
                    // TMTElevenMZ
                    return "TMT11";

                case 18:
                    // TMTSixteenMZ
                    return "TMT16";

                default:
                    return "ReporterIonMassMode_" + reporterIonMassMode;
            }
        }

        /// <summary>
        /// Load reporter ion observation rates from the specified file and store in DMS
        /// The input file can be any of the standard MASIC .txt output files;
        /// will auto-look for the _RepIonObsRate.txt file and MASIC parameter in the same directory as the input file
        /// </summary>
        /// <param name="inputFilePath"></param>
        /// <param name="outputDirectoryPath"></param>
        /// <param name="parameterFilePath"></param>
        /// <param name="resetErrorCode"></param>
        public override bool ProcessFile(string inputFilePath, string outputDirectoryPath, string parameterFilePath, bool resetErrorCode)
        {
            try
            {
                //  Note that CleanupFilePaths() will update mOutputDirectoryPath, which is used by LogMessage()
                if (!CleanupFilePaths(ref inputFilePath, ref outputDirectoryPath))
                {
                    SetBaseClassErrorCode(ProcessFilesErrorCodes.FilePathError);
                    return false;
                }

                var inputFile = new FileInfo(inputFilePath);

                if (!inputFile.Exists)
                {
                    OnErrorEvent("File not found: " + inputFile.FullName);
                    return false;
                }

                string masicParameterFilePath;

                if (string.IsNullOrWhiteSpace(parameterFilePath))
                {
                    var paramFileFound = FindParameterFile(inputFile, out masicParameterFilePath);

                    if (!paramFileFound)
                        return false;
                }
                else
                {
                    masicParameterFilePath = parameterFilePath;
                }

                var success = StoreReporterIonObservationRateStats(inputFile, masicParameterFilePath);
                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Read options from the MASIC parameter file
        /// </summary>
        /// <param name="parameterFilePath"></param>
        /// <param name="reporterIonName"></param>
        /// <param name="reporterIonObservationRateTopNPct"></param>
        private bool ReadMASICParameterFile(
            string parameterFilePath,
            out string reporterIonName,
            out int reporterIonObservationRateTopNPct)
        {
            reporterIonName = string.Empty;
            reporterIonObservationRateTopNPct = 0;

            if (string.IsNullOrWhiteSpace(parameterFilePath))
            {
                OnErrorEvent("MASIC parameter file not defined; unable to continue");
                return false;
            }

            OnDebugEvent("Reading options in MASIC parameter file: " + Path.GetFileName(parameterFilePath));

            var masicSettings = new XmlSettingsFileAccessor();

            if (!masicSettings.LoadSettings(parameterFilePath))
            {
                OnErrorEvent("Error loading parameter file " + parameterFilePath);
                return false;
            }

            if (!masicSettings.SectionPresent("MasicExportOptions"))
            {
                OnErrorEvent("The MASIC parameter file does not have section 'MasicExportOptions': " + parameterFilePath);
                return false;
            }

            var reporterIonMassMode = masicSettings.GetParam("MasicExportOptions", "ReporterIonMassMode", 0, out var valueNotPresent);

            if (valueNotPresent)
            {
                OnErrorEvent("MASIC parameter file does not have key 'ReporterIonMassMode' in section 'MasicExportOptions': " + parameterFilePath);
                return false;
            }

            reporterIonName  = GetReporterIonNameFromMassMode(reporterIonMassMode);

            if (masicSettings.SectionPresent("PlotOptions"))
            {
                reporterIonObservationRateTopNPct = masicSettings.GetParam("PlotOptions", "ReporterIonObservationRateTopNPct", 0);
            }

            return true;
        }

        private bool ReadReporterIonIntensityStatsFile(
            FileSystemInfo intensityStatsFile,
            out List<int> medianIntensitiesTopNPct
        )
        {
            medianIntensitiesTopNPct = new List<int>();

            try
            {
                OnDebugEvent("Reading " + intensityStatsFile.FullName);

                using (var reader = new StreamReader(new FileStream(intensityStatsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (reader.EndOfStream)
                    {
                        OnErrorEvent("Reporter ion intensity stats file is empty: " + intensityStatsFile.FullName);
                        return false;
                    }

                    // Validate the header line
                    var headerLine = reader.ReadLine();

                    var medianColumnIndex = GetColumnIndex(headerLine, "Median_Top80Pct", 2);

                    var channel = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Columns:
                        // Reporter_Ion    NonZeroCount_Top80Pct    Median_Top80Pct    InterQuartileRange_Top80Pct    LowerWhisker_Top80Pct    etc.
                        var lineParts = dataLine.Split('\t');

                        if (lineParts.Length > 0 && lineParts[0].StartsWith("Reporter_Ion", StringComparison.OrdinalIgnoreCase))
                        {
                            // The _RepIonStats.txt file has two tables of intensity stats
                            // We have reached the second table
                            break;
                        }

                        channel++;

                        if (lineParts.Length < medianColumnIndex + 1)
                        {
                            OnErrorEvent("Channel {0} in the reporter ion intensity stats file has fewer than three columns; corrupt file: {1}", channel, intensityStatsFile.FullName);
                            return false;
                        }

                        if (!int.TryParse(lineParts[medianColumnIndex], out var medianTopNPct))
                        {
                            OnErrorEvent("Channel {0} in the reporter ion intensity stats file has a non-integer Median_Top80Pct value: {1}", channel, lineParts[medianColumnIndex]);
                            return false;
                        }

                        medianIntensitiesTopNPct.Add(medianTopNPct);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error reading the _RepIonStats.txt file", ex);
                return false;
            }
        }

        private bool ReadReporterIonObservationRateFile(
            FileSystemInfo observationRateFile,
            out List<double> observationStatsTopNPct)
        {
            observationStatsTopNPct = new List<double>();

            try
            {
                OnDebugEvent("Reading " + observationRateFile.FullName);

                using (var reader = new StreamReader(new FileStream(observationRateFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (reader.EndOfStream)
                    {
                        OnErrorEvent("Reporter ion observation rate file is empty: " + observationRateFile.FullName);
                        return false;
                    }

                    // Validate the header line
                    var headerLine = reader.ReadLine();

                    var obsRateColumnIndex = GetColumnIndex(headerLine, "Observation_Rate_Top80Pct", 2);

                    var channel = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Columns:
                        // Reporter_Ion     Observation_Rate     Observation_Rate_Top80Pct
                        var lineParts = dataLine.Split('\t');

                        channel++;

                        if (lineParts.Length < obsRateColumnIndex + 1)
                        {
                            OnErrorEvent("Channel {0} in the reporter ion observation rate file has fewer than three columns; corrupt file: {1}", channel, observationRateFile.FullName);
                            return false;
                        }

                        if (!double.TryParse(lineParts[obsRateColumnIndex], out var observationRateTopNPct))
                        {
                            OnErrorEvent("Channel {0} in the reporter ion observation rate file has a non-numeric Observation_Rate_Top80Pct value: {1}", channel, lineParts[obsRateColumnIndex]);
                            return false;
                        }

                        observationStatsTopNPct.Add(observationRateTopNPct);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error reading the _RepIonObsRate.txt file", ex);
                return false;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ReporterIonStatsUploader(StatsUploaderOptions options)
        {
            Options = options;
        }

        private bool StoreReporterIonObservationRateStats(FileInfo inputFile, string masicParameterFilePath)
        {
            try
            {
                var inputDirectoryPath = inputFile.DirectoryName;

                if (string.IsNullOrWhiteSpace(inputDirectoryPath))
                {
                    OnErrorEvent("Unable to determine the parent directory of the input file, " + inputFile.FullName);
                    return false;
                }

                var success = GetDatasetName(inputFile, out var datasetName);

                if (!success)
                    return false;

                var jobSuccess = GetJobNumber(inputFile, out var jobNumber);

                if (!jobSuccess)
                    return false;

                var paramFileLoaded = ReadMASICParameterFile(masicParameterFilePath, out var reporterIonName, out var reporterIonObservationRateTopNPct);

                if (!paramFileLoaded)
                    return false;

                var observationRateFile = new FileInfo(Path.Combine(inputDirectoryPath, datasetName + "_RepIonObsRate.txt"));

                if (!observationRateFile.Exists)
                    return true;

                var intensityStatsFile = new FileInfo(Path.Combine(inputDirectoryPath, datasetName + "_RepIonStats.txt"));

                var obsRatesLoaded = ReadReporterIonObservationRateFile(
                    observationRateFile,
                    out var observationStatsTopNPct);

                var intensityStatsLoaded = ReadReporterIonIntensityStatsFile(
                    intensityStatsFile,
                    out var medianIntensitiesTopNPct);

                if (!obsRatesLoaded || !intensityStatsLoaded)
                    return false;

                Console.WriteLine();
                OnStatusEvent("Loaded stats for {0} reporter ions from file {1}", observationStatsTopNPct.Count, observationRateFile.Name);

                if (Options.PreviewMode)
                {
                    OnStatusEvent("Preview call to {0} in DMS5 for Job {1}, Dataset {2}", STORE_REPORTER_ION_OBS_STATS_SP_NAME, jobNumber, datasetName);

                    return true;
                }

                Console.WriteLine();
                OnStatusEvent("Pushing stats into DMS for Job {0}, Dataset {1}", jobNumber, datasetName);

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(Options.ConnectionString, "ReporterIonStatsUploader");

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: Options.VerboseMode);
                dbTools.DebugEvent += ProcedureExecutor_DebugEvent;
                dbTools.StatusEvent += ProcedureExecutor_StatusEvent;
                dbTools.WarningEvent += ProcedureExecutor_WarningEvent;
                dbTools.ErrorEvent += ProcedureExecutor_DBErrorEvent;

                // Call stored procedure store_reporter_ion_obs_stats in DMS5
                // Data is stored in table T_Reporter_Ion_Observation_Rates
                var sqlCmd = dbTools.CreateCommand(STORE_REPORTER_ION_OBS_STATS_SP_NAME, CommandType.StoredProcedure);

                // ReSharper disable once CommentTypo
                // Note that reporterIonName must match a Label in T_Sample_Labelling_Reporter_Ions
                if (string.IsNullOrWhiteSpace(reporterIonName))
                {
                    OnErrorEvent("Reporter ion name is empty for job {0}; " +
                                 "cannot store reporter ion observation stats in the database", jobNumber);

                    return false;
                }

                dbTools.AddParameter(sqlCmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddTypedParameter(sqlCmd, "@job", SqlType.Int, value: jobNumber);
                dbTools.AddParameter(sqlCmd, "@reporterIon", SqlType.VarChar, 64).Value = reporterIonName;
                dbTools.AddParameter(sqlCmd, "@topNPct", SqlType.Int).Value = reporterIonObservationRateTopNPct;
                dbTools.AddParameter(sqlCmd, "@observationStatsTopNPct", SqlType.VarChar, 4000).Value = string.Join(",", observationStatsTopNPct);
                dbTools.AddParameter(sqlCmd, "@medianIntensitiesTopNPct", SqlType.VarChar, 4000).Value = string.Join(",", medianIntensitiesTopNPct);
                var messageParam = dbTools.AddTypedParameter(sqlCmd, "@message", SqlType.VarChar, 255, string.Empty, ParameterDirection.InputOutput);
                dbTools.AddTypedParameter(sqlCmd, "@infoOnly", SqlType.TinyInt, value: 0);

                // Execute the SP (retry the call, up to 3 times)
                var resCode = dbTools.ExecuteSP(sqlCmd);

                if (resCode == 0)
                {
                    OnDebugEvent(" ... complete");
                    return true;
                }

                var errorMessage = string.IsNullOrWhiteSpace(messageParam.Value.ToString())
                                       ? "No error message"
                                       : messageParam.Value.CastDBVal<string>();

                OnErrorEvent("Error storing reporter ion observation stats and median intensities in the database, {0} returned {1}: {2}", STORE_REPORTER_ION_OBS_STATS_SP_NAME, resCode, errorMessage);

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error posting reporter ion observation rate stats", ex);
                return false;
            }
        }

        private void ProcedureExecutor_DebugEvent(string message)
        {
            OnDebugEvent(message);
        }

        private void ProcedureExecutor_StatusEvent(string message)
        {
            OnDebugEvent(message);
        }

        private void ProcedureExecutor_WarningEvent(string message)
        {
            OnWarningEvent(message);
        }

        private void ProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            if (message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                message.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                OnWarningEvent("Permission denied contacting the database");
            }

            OnErrorEvent(message);
        }

    }
}
