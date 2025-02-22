using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
    public class PhilosopherResultsUpdater : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: dia, Hyperscore, Nextscore, Prev, prot, psm

        // ReSharper restore CommentTypo

        /// <summary>
        /// Primary working directory for the analysis job
        /// </summary>
        public DirectoryInfo AnalysisJobWorkingDirectory { get; }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="analysisJobWorkingDirectory">Primary working directory for the analysis job</param>
        public PhilosopherResultsUpdater(string datasetName, DirectoryInfo analysisJobWorkingDirectory)
        {
            AnalysisJobWorkingDirectory = analysisJobWorkingDirectory;
            DatasetName = datasetName;
        }

        private DirectoryInfo GetExperimentGroupWorkingDirectoryToUse(DirectoryInfo experimentGroupWorkingDirectory, string datasetOrExperimentGroupName)
        {
            if (AnalysisManagerMSFraggerPlugIn.AnalysisToolRunnerMSFragger.ExperimentGroupWorkingDirectoryHasResults(experimentGroupWorkingDirectory))
            {
                return experimentGroupWorkingDirectory;
            }

            GetFilePaths(datasetOrExperimentGroupName, AnalysisJobWorkingDirectory, "psm", out var psmFile, out _);

            if (psmFile.Exists)
                return AnalysisJobWorkingDirectory;

            GetFilePaths(datasetOrExperimentGroupName, AnalysisJobWorkingDirectory, "ion", out var ionFile, out _);

            if (ionFile.Exists)
                return AnalysisJobWorkingDirectory;

            GetFilePaths(datasetOrExperimentGroupName, AnalysisJobWorkingDirectory, "peptide", out var peptideFile, out _);

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (peptideFile.Exists)
                return AnalysisJobWorkingDirectory;

            OnWarningEvent(
                "Experiment group working directory is empty, and the psm.tsv, ion.tsv, or peptide.tsv file was not found in the analysis job working directory: {0}",
                experimentGroupWorkingDirectory);

            return AnalysisJobWorkingDirectory;
        }

        private void GetFilePaths(
            string datasetOrExperimentGroupName,
            FileSystemInfo workingDirectory,
            string reportFileName,
            out FileInfo inputFile,
            out FileInfo updatedFile,
            string fileExtension = "tsv")
        {
            inputFile = new FileInfo(Path.Combine(
                workingDirectory.FullName,
                string.Format("{0}.{1}", reportFileName, fileExtension)));

            updatedFile = new FileInfo(Path.Combine(
                workingDirectory.FullName,
                string.Format("{0}_{1}.{2}", datasetOrExperimentGroupName, reportFileName, fileExtension)));
        }

        /// <summary>
        /// Replace the input file with the updated file, renaming the updated file to include the dataset or experiment group name
        /// </summary>
        /// <param name="datasetOrExperimentGroupName">Dataset or experiment group name</param>
        /// <param name="inputFile">Input file</param>
        /// <param name="updatedFile">Updated file</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ReplaceFile(string datasetOrExperimentGroupName, FileInfo inputFile, FileInfo updatedFile)
        {
            try
            {
                if (inputFile.DirectoryName == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of file " + inputFile.FullName);

                var finalFilePath = Path.Combine(
                    AnalysisJobWorkingDirectory.FullName,
                    string.Format("{0}_{1}{2}",
                        datasetOrExperimentGroupName,
                        Path.GetFileNameWithoutExtension(inputFile.Name),
                        inputFile.Extension));

                if (!AnalysisJobWorkingDirectory.FullName.Equals(inputFile.DirectoryName) && File.Exists(finalFilePath))
                {
                    throw new Exception("Philosopher report file already exists, indicating a name conflict: " + finalFilePath);
                }

                var filePathToDelete = inputFile.FullName + "_ToBeDeleted.trash";

                inputFile.MoveTo(filePathToDelete);

                updatedFile.MoveTo(finalFilePath);

                inputFile.Delete();

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReplaceFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Rename and update the report files created by Philosopher
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates files ion.tsv, peptide.tsv, protein.tsv, and psm.tsv in each experiment group working directory,
        /// updating the strings in columns Spectrum, Spectrum File, and Protein ID (<see cref="UpdatePhilosopherPSMFile"/>)
        /// </para>
        /// <para>
        /// If experiment group working directories are present, will move the updated files to the main working directory
        /// </para>
        /// </remarks>
        /// <param name="experimentGroupWorkingDirectories">Experiment group working directories</param>
        /// <param name="usedProteinProphet">True if Protein Prophet was used</param>
        /// <param name="totalPeptideCount">Output: total number of result lines in the peptide.tsv file(s)</param>
        /// <returns>True if successful, false if an error</returns>
        public bool UpdatePhilosopherReportFiles(
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            bool usedProteinProphet,
            out int totalPeptideCount)
        {
            totalPeptideCount = 0;

            try
            {
                var successCount = 0;
                var processedWorkingDirectories = new SortedSet<string>();

                // For DIA searches that use a spectral library, there might only be one psm.tsv file and one protein.tsv file, and they will be in the working directory
                // In that case, exit the for loop after successfully renaming the TSV files
                var psmTsvFiles = AnalysisJobWorkingDirectory.GetFiles("psm.tsv", SearchOption.AllDirectories);

                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var experimentGroup in experimentGroupWorkingDirectories)
                {
                    var datasetOrExperimentGroupName =
                        experimentGroupWorkingDirectories.Count <= 1 || psmTsvFiles.Length == 1
                            ? DatasetName
                            : experimentGroup.Key;

                    if (experimentGroupWorkingDirectories.Count > 1)
                    {
                        // ReSharper disable once CommentTypo
                        // ReSharper disable once StringLiteralTypo

                        // Rename file msstats.csv since there are multiple experiment groups
                        GetFilePaths(datasetOrExperimentGroupName, experimentGroup.Value, "msstats",
                            out var sourceMSStatsFile, out var updatedMSStatsFile, "csv");

                        if (sourceMSStatsFile.Exists)
                        {
                            sourceMSStatsFile.MoveTo(updatedMSStatsFile.FullName);
                        }
                        else
                        {
                            GetFilePaths(datasetOrExperimentGroupName, AnalysisJobWorkingDirectory, "msstats",
                                out var sourceMSStatsFileWorkDir, out var updatedMSStatsFileWorkDir, "csv");

                            if (sourceMSStatsFileWorkDir.Exists)
                            {
                                sourceMSStatsFileWorkDir.MoveTo(updatedMSStatsFileWorkDir.FullName);
                            }
                        }
                    }

                    // For DIA searches that use a spectral library, the results might have been created in the working directory
                    // and not in the experiment group working directory; check for this

                    var experimentGroupWorkingDirectory = experimentGroup.Value;
                    var experimentWorkingDirectory = GetExperimentGroupWorkingDirectoryToUse(experimentGroupWorkingDirectory, datasetOrExperimentGroupName);

                    if (!processedWorkingDirectories.Add(experimentWorkingDirectory.FullName))
                        continue;

                    var psmSuccess = UpdatePhilosopherPSMFile(datasetOrExperimentGroupName, experimentWorkingDirectory);
                    var ionSuccess = UpdatePhilosopherIonFile(datasetOrExperimentGroupName, experimentWorkingDirectory);
                    var peptideSuccess = UpdatePhilosopherPeptideFile(datasetOrExperimentGroupName, experimentWorkingDirectory, out var peptideCount);

                    if (peptideCount == 0)
                    {
                        // The peptide.tsv file is empty; a corresponding protein.tsv file will not exist
                        if (psmSuccess && ionSuccess && peptideSuccess)
                            successCount++;

                        continue;
                    }

                    totalPeptideCount += peptideCount;

                    var proteinSuccess = UpdatePhilosopherProteinFile(datasetOrExperimentGroupName, experimentWorkingDirectory, usedProteinProphet);

                    if (psmSuccess && ionSuccess && peptideSuccess && proteinSuccess)
                    {
                        successCount++;

                        if (psmTsvFiles.Length == 1)
                        {
                            // There is only one psm.tsv file, and it has already been renamed, so exit the for loop
                            break;
                        }
                    }
                }

                return successCount == experimentGroupWorkingDirectories.Count || psmTsvFiles.Length == 1 && successCount > 0;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdatePhilosopherReportFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Rename psm.tsv to DatasetName_psm.tsv or ExperimentGroupName_psm.tsv
        /// </summary>
        /// <remarks>
        /// Other changes:
        ///   Update the Spectrum column to only have the scan number
        ///   Remove duplicate values from the Spectrum File column
        ///   Clear the data in column Protein ID since that column has protein descriptions, which are also listed in protein.tsv
        /// </remarks>
        /// <param name="datasetOrExperimentGroupName">Dataset or experiment group name</param>
        /// <param name="workingDirectory">Working directory</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePhilosopherPSMFile(
            string datasetOrExperimentGroupName,
            FileSystemInfo workingDirectory)
        {
            try
            {
                GetFilePaths(datasetOrExperimentGroupName, workingDirectory, "psm", out var inputFile, out var updatedFile);

                if (!inputFile.Exists)
                {
                    OnErrorEvent("Input file not found in UpdatePhilosopherPSMFile: " + inputFile.FullName);
                    return false;
                }

                OnDebugEvent("Creating {0} using {1}", updatedFile.FullName, inputFile.FullName);

                var headerNames = new List<string>
                {
                    "Spectrum",
                    "Spectrum File",
                    "Peptide",
                    "Modified Peptide",
                    "Prev AA",
                    "Next AA",
                    "Peptide Length",
                    "Charge",
                    "Retention",
                    "Observed Mass",
                    "Calibrated Observed Mass",
                    "Observed M/Z",
                    "Calibrated Observed M/Z",
                    "Calculated Peptide Mass",
                    "Calculated M/Z",
                    "Delta Mass",
                    "Expectation",
                    // ReSharper disable StringLiteralTypo
                    "Hyperscore",
                    "Nextscore",
                    // ReSharper restore StringLiteralTypo
                    "PeptideProphet Probability",
                    "Number of Enzymatic Termini",
                    "Number of Missed Cleavages",
                    "Protein Start",
                    "Protein End",
                    "Intensity",
                    "Assigned Modifications",
                    "Observed Modifications",
                    "Is Unique",
                    "Protein",
                    "Protein ID",
                    "Entry Name",
                    "Gene",
                    "Protein Description",
                    "Mapped Genes",
                    "Mapped Proteins"
                };

                var success = WriteUpdatedFile(headerNames, inputFile, updatedFile, out _);

                if (!success)
                    return false;

                return ReplaceFile(datasetOrExperimentGroupName, inputFile, updatedFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdatePhilosopherPSMFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Rename ion.tsv to DatasetName_ion.tsv or ExperimentGroupName_ion.tsv
        /// In addition, clear the data in column Protein ID since that column has protein descriptions, which are also listed in protein.tsv
        /// </summary>
        /// <param name="datasetOrExperimentGroupName">Dataset or experiment group name</param>
        /// <param name="workingDirectory">Working directory</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePhilosopherIonFile(
            string datasetOrExperimentGroupName,
            FileSystemInfo workingDirectory)
        {
            try
            {
                GetFilePaths(datasetOrExperimentGroupName, workingDirectory, "ion", out var inputFile, out var updatedFile);

                if (!inputFile.Exists)
                {
                    OnErrorEvent("Input file not found in UpdatePhilosopherIonFile: " + inputFile.FullName);
                    return false;
                }

                var headerNames = new List<string>
                {
                    "Peptide Sequence",
                    "Modified Sequence",
                    "Prev AA",
                    "Next AA",
                    "Peptide Length",
                    "M/Z",
                    "Charge",
                    "Observed Mass",
                    "Probability",
                    "Expectation",
                    "Spectral Count",
                    "Intensity",
                    "Assigned Modifications",
                    "Observed Modifications",
                    "Protein",
                    "Protein ID",
                    "Entry Name",
                    "Gene",
                    "Protein Description",
                    "Mapped Genes",
                    "Mapped Proteins"
                };

                var success = WriteUpdatedFile(headerNames, inputFile, updatedFile, out _);

                if (!success)
                    return false;

                return ReplaceFile(datasetOrExperimentGroupName, inputFile, updatedFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdatePhilosopherIonFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Rename peptide.tsv to DatasetName_peptide.tsv or ExperimentGroupName_peptide.tsv
        /// In addition, clear the data in column Protein ID since that column has protein descriptions, which are also listed in protein.tsv
        /// </summary>
        /// <param name="datasetOrExperimentGroupName">Dataset or experiment group name</param>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="peptideCount">Output: number of result lines in the peptide.tsv file</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePhilosopherPeptideFile(
            string datasetOrExperimentGroupName,
            FileSystemInfo workingDirectory,
            out int peptideCount)
        {
            try
            {
                GetFilePaths(datasetOrExperimentGroupName, workingDirectory, "peptide", out var inputFile, out var updatedFile);

                if (!inputFile.Exists)
                {
                    OnErrorEvent("Input file not found in UpdatePhilosopherPeptideFile: " + inputFile.FullName);
                    peptideCount = 0;
                    return false;
                }

                var headerNames = new List<string>
                {
                    "Peptide",
                    "Prev AA",
                    "Next AA",
                    "Peptide Length",
                    "Charges",
                    "Probability",
                    "Spectral Count",
                    "Intensity",
                    "Assigned Modifications",
                    "Observed Modifications",
                    "Protein",
                    "Protein ID",
                    "Entry Name",
                    "Gene",
                    "Protein Description",
                    "Mapped Genes",
                    "Mapped Proteins"
                };

                var success = WriteUpdatedFile(headerNames, inputFile, updatedFile, out peptideCount);

                if (!success)
                    return false;

                return ReplaceFile(datasetOrExperimentGroupName, inputFile, updatedFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdatePhilosopherPeptideFile", ex);
                peptideCount = 0;
                return false;
            }
        }

        /// <summary>
        /// Rename protein.tsv to DatasetName_protein.tsv or ExperimentGroupName_protein.tsv, moving the file to the working directory if necessary
        /// </summary>
        /// <param name="datasetOrExperimentGroupName">Dataset or experiment group name</param>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="usedProteinProphet">True if Protein Prophet was used</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePhilosopherProteinFile(
            string datasetOrExperimentGroupName,
            FileSystemInfo workingDirectory,
            bool usedProteinProphet)
        {
            try
            {
                GetFilePaths(datasetOrExperimentGroupName, workingDirectory, "protein", out var inputFile, out var updatedFile);

                if (!inputFile.Exists)
                {
                    if (!usedProteinProphet)
                    {
                        // Ignore this missing file since we did not run Protein Prophet
                        OnDebugEvent("Skipping update of {0} since it does not exist (UpdatePhilosopherProteinFile)", inputFile.FullName);
                        return true;
                    }

                    OnErrorEvent("Input file not found in UpdatePhilosopherProteinFile: " + inputFile.FullName);
                    return false;
                }

                var finalFile = new FileInfo(Path.Combine(AnalysisJobWorkingDirectory.FullName, updatedFile.Name));

                if (File.Exists(finalFile.FullName))
                {
                    throw new Exception("Philosopher report file already exists, indicating a name conflict: " + finalFile.FullName);
                }

                inputFile.MoveTo(finalFile.FullName);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdatePhilosopherProteinFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Create a copy of the input file, applying various updates
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates applied:
        ///   For column "Protein ID", write an empty string, thus clearing protein descriptions
        ///   For column "Spectrum", replace DatasetName_06May21_20-11-16.07867.07867.2 with the scan number (here 7867)
        ///   For column "Spectrum File", only keep the first instance of each filename
        /// </para>
        /// <para>
        /// Columns "Spectrum" and "Spectrum File" are only present in the psm.tsv file
        /// </para>
        /// </remarks>
        /// <param name="headerNames">Header names</param>
        /// <param name="inputFile">Input file</param>
        /// <param name="outputFile">Output file</param>
        /// <param name="resultLineCount">Number of result lines (not counting the header line)</param>
        /// <returns>True if successful, false if an error</returns>
        private bool WriteUpdatedFile(
            List<string> headerNames,
            FileSystemInfo inputFile,
            FileSystemInfo outputFile,
            out int resultLineCount)
        {
            var spectrumColumnIndex = -1;
            var spectrumFileColumnIndex = -1;
            var proteinIdColumnIndex = -1;

            var scanMatcher = new Regex(@"(?<ScanNumber>\d+)\.\d+\.\d+$", RegexOptions.Compiled);
            var scanWarningsShown = 0;

            resultLineCount = 0;

            var spectrumFiles = new SortedSet<string>();

            using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
            using var writer = new StreamWriter(new FileStream(outputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrEmpty(dataLine))
                {
                    writer.WriteLine();
                    continue;
                }

                var lineParts = dataLine.Split('\t');

                if (proteinIdColumnIndex < 0)
                {
                    // This is the header line
                    writer.WriteLine(dataLine);

                    var columnMap = Global.ParseHeaderLine(dataLine, headerNames);

                    if (!columnMap.TryGetValue("Protein ID", out proteinIdColumnIndex) || proteinIdColumnIndex < 0)
                    {
                        OnErrorEvent("{0} column not found in {1}", "proteinIdColumnIndex", inputFile.FullName);
                        return false;
                    }

                    if (columnMap.TryGetValue("Spectrum", out var columnIndex1))
                        spectrumColumnIndex = columnIndex1;

                    if (columnMap.TryGetValue("Spectrum File", out var columnIndex2))
                        spectrumFileColumnIndex = columnIndex2;

                    continue;
                }

                if (spectrumColumnIndex >= 0 && lineParts.Length >= spectrumColumnIndex)
                {
                    var match = scanMatcher.Match(lineParts[spectrumColumnIndex].Trim());

                    if (match.Success)
                    {
                        lineParts[spectrumColumnIndex] = int.Parse(match.Groups["ScanNumber"].Value).ToString();
                    }
                    else
                    {
                        scanWarningsShown++;

                        if (scanWarningsShown <= 10)
                        {
                            OnWarningEvent("Scan number not found in the spectrum column: " + lineParts[spectrumColumnIndex]);
                        }
                    }
                }

                if (spectrumFileColumnIndex >= 0 && lineParts.Length >= spectrumFileColumnIndex)
                {
                    // ReSharper disable once CanSimplifySetAddingWithSingleCall

                    if (spectrumFiles.Contains(lineParts[spectrumFileColumnIndex]))
                    {
                        // Clear out this field to decrease file size
                        lineParts[spectrumFileColumnIndex] = string.Empty;
                    }
                    else
                    {
                        spectrumFiles.Add(lineParts[spectrumFileColumnIndex]);
                    }
                }

                if (lineParts.Length >= proteinIdColumnIndex)
                {
                    // This is the protein description; clear out this field
                    lineParts[proteinIdColumnIndex] = string.Empty;
                }

                writer.WriteLine(string.Join("\t", lineParts));
                resultLineCount++;
            }

            return true;
        }
    }
}
