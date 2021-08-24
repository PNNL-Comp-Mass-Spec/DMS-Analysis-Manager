using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
    internal class ConsoleOutputFileParser : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: degen, dev, Flammagenitus, Insilicos

        // ReSharper restore CommentTypo

        /// <summary>
        /// Error message from the console output file
        /// </summary>
        public string ConsoleOutputErrorMsg { get; private set; }

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        public short DebugLevel { get; }

        /// <summary>
        /// Philosopher version, as parsed from the program's console output text
        /// </summary>
        public string PhilosopherVersion { get; private set; }

        /// <summary>
        /// This even is raised when an error occurs, but we don't want AnalysisToolRunnerPepProtProphet to update mMessage
        /// </summary>
        public event StatusEventEventHandler ErrorNoMessageUpdateEvent;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        public ConsoleOutputFileParser(short debugLevel)
        {
            ConsoleOutputErrorMsg = string.Empty;
            DebugLevel = debugLevel;
            PhilosopherVersion = string.Empty;
        }

        /// <summary>
        /// Parse the Java console output file
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <param name="cmdRunnerMode"></param>
        public void ParseJavaConsoleOutputFile(
            string consoleOutputFilePath,
            AnalysisToolRunnerPepProtProphet.CmdRunnerModes cmdRunnerMode)
        {
            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (DebugLevel >= 4)
                    {
                        OnDebugEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                ConsoleOutputErrorMsg = string.Empty;

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file " + consoleOutputFilePath);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (DebugLevel >= 2)
                {
                    OnErrorNoMessageUpdate("Error parsing the Java console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Parse the Percolator console output file
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        public void ParsePercolatorConsoleOutputFile(string consoleOutputFilePath)
        {
            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (DebugLevel >= 4)
                    {
                        OnDebugEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                ConsoleOutputErrorMsg = string.Empty;

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file " + consoleOutputFilePath);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (DebugLevel >= 2)
                {
                    OnErrorNoMessageUpdate("Error parsing the Percolator console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Parse the Philosopher console output file to determine the Philosopher version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        public void ParsePhilosopherConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output when initializing the workspace
            // ----------------------------------------------------

            // INFO[17:45:51] Executing Workspace  v3.4.13
            // INFO[17:45:51] Removing workspace
            // INFO[17:45:51] Done

            // ----------------------------------------------------
            // Example Console output when running Peptide Prophet
            // ----------------------------------------------------

            // INFO[11:01:05] Executing PeptideProphet  v3.4.13
            //  file 1: C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pepXML
            //  processed altogether 6982 results
            // INFO: Results written to file: C:\DMS_WorkDir\interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml
            // ...
            // INFO: Processing standard MixtureModel ...
            //  PeptideProphet  (TPP v5.2.1-dev Flammagenitus, Build 201906281613-exported (Windows_NT-x86_64)) AKeller@ISB
            // ...
            // INFO[11:01:25] Done

            // ----------------------------------------------------
            // Example Console output when running Protein Prophet
            // ----------------------------------------------------

            // INFO[11:05:08] Executing ProteinProphet  v3.4.13
            // ProteinProphet (C++) by Insilicos LLC and LabKey Software, after the original Perl by A. Keller (TPP v5.2.1-dev Flammagenitus, Build 201906281613-exported (Windows_NT-x86_64))
            //  (no FPKM) (using degen pep info)
            // Reading in C:/DMS_WorkDir/interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml...
            // ...
            // Finished.
            // INFO[11:05:12] Done

            // ----------------------------------------------------
            // Example Console output when running Filter
            // ----------------------------------------------------

            // INFO[11:07:13] Executing Filter  v3.4.13
            // INFO[11:07:13] Processing peptide identification files
            // ...
            // INFO[11:07:16] Saving
            // INFO[11:07:16] Done

            // ----------------------------------------------------
            // Example Console output when running FreeQuant
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ----------------------------------------------------
            // Example Console output when running LabelQuant
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ----------------------------------------------------
            // Example Console output when running Abacus
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ReSharper restore CommentTypo

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (DebugLevel >= 4)
                    {
                        OnDebugEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                ConsoleOutputErrorMsg = string.Empty;

                var versionMatcher = new Regex(@"INFO.+Executing [^ ]+ +(?<Version>v[^ ]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file " + consoleOutputFilePath);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (DebugLevel >= 2)
                {
                    OnErrorNoMessageUpdate("Error parsing the Philosopher console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        [Obsolete("Old method, superseded by ParsePhilosopherConsoleOutputFile and ParsePercolatorConsoleOutputFile")]
        private void ParseConsoleOutputFile()
        {
            const string BUILD_AND_VERSION = "Current Philosopher build and version";

            var mConsoleOutputFilePath = Path.Combine("Philosopher_ConsoleOutput.txt");

            if (string.IsNullOrWhiteSpace(mConsoleOutputFilePath))
                return;

            // Example Console output
            //
            // INFO[18:17:06] Current Philosopher build and version         build=201904051529 version=20190405
            // WARN[18:17:08] There is a new version of Philosopher available for download: https://github.com/prvst/philosopher/releases

            // INFO[18:25:51] Executing Workspace 20190405
            // INFO[18:25:52] Creating workspace
            // INFO[18:25:52] Done

            var processingSteps = new SortedList<string, int>
            {
                {"Starting", 0},
                {"Current Philosopher build", 1},
                {"Executing Workspace", 2},
                {"Executing Database", 3},
                {"Executing PeptideProphet", 10},
                {"Executing ProteinProphet", 50},
                {"Computing degenerate peptides", 60},
                {"Computing probabilities", 70},
                {"Calculating sensitivity", 80},
                {"Executing Filter", 90},
                {"Executing Report", 95},
                {"Plotting mass distribution", 98},
            };

            // Peptide prophet iterations status:
            // Iterations: .........10.........20.....

            try
            {
                if (!File.Exists(mConsoleOutputFilePath))
                {
                    if (DebugLevel >= 4)
                    {
                        OnDebugEvent("Console output file not found: " + mConsoleOutputFilePath);
                    }

                    return;
                }

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file " + mConsoleOutputFilePath);
                }

                ConsoleOutputErrorMsg = string.Empty;

                // ReSharper disable once NotAccessedVariable
                var currentProgress = 0;

                using var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead <= 5)
                    {
                        // The first line has the path to the Philosopher .exe file and the command line arguments
                        // The second line is dashes
                        // The third line will have the version when philosopher is run with the "version" switch

                        var versionTextStartIndex = dataLine.IndexOf(BUILD_AND_VERSION, StringComparison.OrdinalIgnoreCase);

                        if (string.IsNullOrEmpty(PhilosopherVersion) &&
                            versionTextStartIndex >= 0)
                        {
                            if (DebugLevel >= 2)
                            {
                                OnDebugEvent(dataLine);
                            }

                            PhilosopherVersion = dataLine.Substring(versionTextStartIndex + BUILD_AND_VERSION.Length).Trim();
                        }
                    }
                    else
                    {
                        foreach (var processingStep in processingSteps)
                        {
                            if (dataLine.IndexOf(processingStep.Key, StringComparison.OrdinalIgnoreCase) < 0)
                                continue;

                            currentProgress = processingStep.Value;
                        }

                        // Future:
                        /*
                            if (linesRead > 12 &&
                                dataLineLCase.Contains("error") &&
                                string.IsNullOrEmpty(ConsoleOutputErrorMsg))
                            {
                                ConsoleOutputErrorMsg = "Error running Philosopher: " + dataLine;
                            }
                            */
                    }
                }

                // if (currentProgress > mProgress)
                // {
                //     mProgress = currentProgress;
                // }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (DebugLevel >= 2)
                {
                    OnErrorNoMessageUpdate("Error parsing console output file (" + mConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private void OnErrorNoMessageUpdate(string errorMessage)
        {
            ErrorNoMessageUpdateEvent?.Invoke(errorMessage);
        }
    }
}
