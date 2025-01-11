using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;

namespace AnalysisManagerPepProtProphetPlugIn
{
    internal class ConsoleOutputFileParser : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: degen, dev, Flammagenitus, Insilicos, Prot

        // ReSharper restore CommentTypo

        /// <summary>
        /// Text indicating that philosopher crashed with an exception
        /// </summary>
        public const string PHILOSOPHER_PANIC_ERROR = "panic:";

        /// <summary>
        /// Text indicating that philosopher crashed with a runtime error
        /// </summary>
        public const string PHILOSOPHER_RUNTIME_ERROR = "runtime error:";

        /// <summary>
        /// RegEx for matching color codes that appear in the Philosopher console output file
        /// </summary>
        /// <remarks>
        /// Philosopher colorizes text at the console, resulting in text like the following:
        /// [36mINFO[0m[15:52:40] Done
        /// </remarks>
        public Regex ColorTagMatcher { get; } = new(@"\x1B\[\d+m", RegexOptions.Compiled);

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
        /// Philosopher version, as parsed from the program's console output text, in the form Philosopher v4.1.0
        /// </summary>
        public string PhilosopherVersion { get; private set; }

        /// <summary>
        /// RegEx for extracting the Philosopher version
        /// </summary>
        private Regex PhilosopherVersionMatcher { get; } = new("INFO.+version=(?<Version>[^ ]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This even is raised when an error occurs, but we don't want AnalysisToolRunnerPepProtProphet to update mMessage
        /// </summary>
        public event StatusEventEventHandler ErrorNoMessageUpdateEvent;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        public ConsoleOutputFileParser(short debugLevel)
        {
            ConsoleOutputErrorMsg = string.Empty;
            DebugLevel = debugLevel;
            PhilosopherVersion = string.Empty;
        }

        /// <summary>
        /// Parse the Java console output file
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        /// <param name="cmdRunnerMode">Command runner mode</param>
        public void ParseJavaConsoleOutputFile(
            string consoleOutputFilePath,
            AnalysisToolRunnerPepProtProphet.CmdRunnerModes cmdRunnerMode)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example MSBooster Console output (excerpt)

            // MSBooster v1.1.11
            // Using 4 threads
            // Generating input file for DIA-NN
            // 98095 unique peptides from 145365 PSMs
            // Writing DIA-NN input file
            // Diann input file generation took 1119 milliseconds
            // Input file at  D:\DMS_WorkDir4\Leaf\spectraRT.tsv
            // 98095 unique peptides from 145365 PSMs
            // createFull input file generation took 649 milliseconds
            // Input file at  D:\DMS_WorkDir4\Leaf\spectraRT_full.tsv
            // Generating DIA-NN predictions
            // C:\DMS_Programs\MSFragger\fragpipe\tools\diann\1.8.2_beta_8\win\DiaNN.exe --lib D:\DMS_WorkDir4\Leaf\spectraRT.tsv --predict --threads 4 --strip-unknown-mods --mod TMT,229.1629 --predict-n-frag 100
            // DIA-NN 1.8.2 beta 8 (Data-Independent Acquisition by Neural Networks)
            // ...
            // DIA-NN will use deep learning to predict spectra/RTs/IMs even for peptides carrying modifications which are not recognised by the deep learning predictor. In this scenario, if also generating a spectral library from the DIA data or using the MBR mode, it might or might not be better (depends on the data) to also use the --out-measured-rt option - it's recommended to test it with and without this option
            // Modification TMT with mass delta 229.163 added to the list of recognised modifications for spectral library-based search
            // Deep learning predictor will predict 100 fragments
            // Cannot find a UniMod modification match for TMT: 73.0618 minimal mass discrepancy; using the original modificaiton name
            // ...
            // Done generating DIA-NN predictions
            // Model running took 67742 milliseconds
            // Generating edited pin with following features: [unweightedSpectralEntropy, deltaRTLOESS]
            // Loading predicted spectra
            // Processing Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10.mzML
            // RT regression using 4158 PSMs
            // Edited pin file at D:\DMS_WorkDir4\Leaf\Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10_edited.pin
            // ...
            // Done in 24566 ms
            // ----------------------------------------------------

            // ----------------------------------------------------
            // Example IonQuant Console output (excerpt)

            // IonQuant version IonQuant-1.9.8
            // Batmass-IO version 1.28.12
            // timsdata library version timsdata-2-21-0-4
            // (c) University of Michigan
            // System OS: Windows 10, Architecture: AMD64
            // Java Info: 11.0.12, OpenJDK 64-Bit Server VM, Eclipse Foundation
            // JVM started with 16 GB memory
            // 2023-09-20 19:30:33 [WARNING] - There are only 2 experiments. Will not calculate MaxLFQ intensity.
            // 2023-09-20 19:30:33 [INFO] - Collecting variable modifications from all psm.tsv files...
            // 2023-09-20 19:30:33 [INFO] - Loading and indexing all psm.tsv files...
            // 2023-09-20 19:30:34 [INFO] - Collecting all compensation voltages if applicable...
            // ...
            // 2023-09-20 19:30:53 [INFO] - Quantifying...
            // ...
            // 2023-09-20 19:31:15 [INFO] - Training LDA models for all matched features.
            // ...
            // 2023-09-20 19:31:27 [INFO] - Fitting a mixture model...
            // 2023-09-20 19:31:28 [INFO] - Estimating match-between-runs FDR...
            // 2023-09-20 19:31:28 [INFO] - With ion FDR 0.010000, ion probability threshold is 0.964800
            // 2023-09-20 19:31:28 [INFO] - With peptide FDR 1.000000, peptide probability threshold is -0.000100
            // 2023-09-20 19:31:28 [INFO] - With protein FDR 1.000000, protein probability threshold is -0.000100
            // 2023-09-20 19:31:28 [INFO] - Updating Philosopher's tables...
            // 2023-09-20 19:31:30 [INFO] - Combining experiments and estimating protein intensity...
            // 2023-09-20 19:31:32 [INFO] - Done!
            // ----------------------------------------------------

            // ReSharper restore CommentTypo

            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilePath);

                if (!consoleOutputFile.Exists)
                {
                    if (DebugLevel >= 4)
                    {
                        OnDebugEvent("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file {0} while running {1}", consoleOutputFile.FullName, cmdRunnerMode);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine() ?? string.Empty;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // ToDo: customize the check for errors

                    // ReSharper disable once InvertIf
                    if ((dataLine.StartsWith("ERROR") || dataLine.StartsWith("Exception in thread")) &&
                        !ConsoleOutputErrorMsg.Contains(dataLine))
                    {
                        // Fatal error
                        if (string.IsNullOrWhiteSpace(ConsoleOutputErrorMsg))
                        {
                            ConsoleOutputErrorMsg = string.Format("Error running {0}: {1}", cmdRunnerMode, dataLine);
                            OnWarningEvent(ConsoleOutputErrorMsg);
                        }
                        else
                        {
                            ConsoleOutputErrorMsg += "; " + dataLine;
                            OnWarningEvent(dataLine);
                        }
                    }
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

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Looks for errors of the form
        /// remove C:\Users\D3L243\AppData\Local\Temp\a7785d27-366e-4048-a23f-c1867454f9f0\batchcoverage.exe: The process cannot access the file because it is being used by another process
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        /// <returns>True if the console output file has a file removal access in use error, otherwise false</returns>
        public bool FileHasRemoveFileError(string consoleOutputFilePath)
        {
            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilePath);

                if (!consoleOutputFile.Exists)
                {
                    OnWarningEvent("Cannot check for remove file errors; console output file not found: " + consoleOutputFilePath);
                    return false;
                }

                if (DebugLevel >= 4)
                {
                    OnDebugEvent("Parsing file {0} to look for errors removing files", consoleOutputFilePath);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLineWithColor = reader.ReadLine() ?? string.Empty;

                    var dataLine = ColorTagMatcher.Replace(dataLineWithColor, string.Empty);

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // ReSharper disable once StringLiteralTypo
                    if (!dataLine.StartsWith("ERRO") || ConsoleOutputErrorMsg.Contains(dataLine))
                        continue;

                    if (LineHasRemoveFileError(dataLine))
                    {
                        // The message is similar to the following, and can be ignored
                        // remove C:\Users\D3L243\AppData\Local\Temp\8c5fd63b-9cb8-4fdb-ab2a-62cef7285253\DatabaseParser.exe: The process cannot access the file because it is being used by another process.

                        return true;
                    }
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

            return false;
        }

        /// <summary>
        /// Return true if the data line has an error of the form
        /// remove C:\Users\D3L243\AppData\Local\Temp\a7785d27-366e-4048-a23f-c1867454f9f0\DatabaseParser.exe: The process cannot access the file because it is being used by another process
        /// </summary>
        /// <param name="dataLine">Data line</param>
        private static bool LineHasRemoveFileError(string dataLine)
        {
            var removeMatcher = new Regex(@"remove .+\.exe", RegexOptions.IgnoreCase);

            return removeMatcher.IsMatch(dataLine);
        }

        /// <summary>
        /// Parse the Percolator console output file
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        public void ParsePercolatorConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output (excerpt)
            //
            // Protein decoy-preix used is _ARATH
            // All files have been read
            // Percolator version 3.06.0, Build Date May 11 2022 12:43:39
            // Copyright (c) 2006-9 University of Washington. All rights reserved.
            // Written by Lukas Käll (lukall@u.washington.edu) in the
            // Department of Genome Sciences at the University of Washington.
            // Issued command:
            // C:\DMS_Programs\MSFragger\fragpipe\tools\percolator-306\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 4 --results-psms Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10_percolator_target_psms.tsv --decoy-results-psms Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10_edited.pin
            // Started Wed Sep 20 19:27:27 2023
            // Hyperparameters: selectionFdr=0.01, Cpos=0, Cneg=0, maxNiter=10
            // Reading tab-delimited input from datafile Arabid_Leaf_2_DDM_BU_3June22_Rage_Rep-22-03-10_edited.pin
            // Features:
            // rank abs_ppm isotope_errors log10_evalue hyperscore delta_hyperscore matched_ion_num complementary_ions ion_series weighted_average_abs_fragment_ppm peptide_length ntt nmc charge_1 charge_2 charge_3 charge_4 charge_5 charge_6 charge_7_or_more group_1 group_2 group_3 group_other 15.994915M unweighted_spectral_entropy delta_RT_loess
            // Found 27965 PSMs
            // Concatenated search input detected and --post-processing-tdc flag set. Applying target-decoy competition on Percolator scores.
            // Train/test set contains 18978 positives and 8987 negatives, size ratio=2.11172 and pi0=1
            // Selecting Cpos by cross-validation.
            // Selecting Cneg by cross-validation.
            // Split 1:	Selected feature 4 as initial direction. Could separate 5428 training set positives with q<0.01 in that direction.
            // Split 2:	Selected feature 4 as initial direction. Could separate 5302 training set positives with q<0.01 in that direction.
            // Split 3:	Selected feature 4 as initial direction. Could separate 5422 training set positives with q<0.01 in that direction.
            // Found 8034 test set positives with q<0.01 in initial direction
            // Reading in data and feature calculation took 0.5210 cpu seconds or 1 seconds wall clock time.
            // ---Training with Cpos selected by cross validation, Cneg selected by cross validation, initial_fdr=0.01, fdr=0.01
            // Iteration 1:	Estimated 9378 PSMs with q<0.01
            // ...
            // Learned normalized SVM weights for the 3 cross-validation splits:
            // ...
            // Found 9557 test set PSMs with q<0.01.
            // Selected best-scoring PSM per scan+expMass (target-decoy competition): 18978 target PSMs and 8987 decoy PSMs.
            // Calculating q values.
            // Final list yields 9559 target PSMs with q<0.01.
            // Calculating posterior error probabilities (PEPs).
            // Processing took 4.9100 cpu seconds or 5 seconds wall clock time.
            //
            // ----------------------------------------------------

            // ReSharper restore CommentTypo

            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilePath);

                if (!consoleOutputFile.Exists)
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
                    OnDebugEvent("Parsing file " + consoleOutputFile.FullName);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLineWithColor = reader.ReadLine() ?? string.Empty;

                    var dataLine = ColorTagMatcher.Replace(dataLineWithColor, string.Empty);

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // ToDo: customize the check for errors

                    // ReSharper disable once InvertIf
                    if (dataLine.StartsWith("Error:") && !ConsoleOutputErrorMsg.Contains(dataLine))
                    {
                        // Fatal error
                        if (string.IsNullOrWhiteSpace(ConsoleOutputErrorMsg))
                        {
                            ConsoleOutputErrorMsg = string.Format("Error running {0}: {1}", "Percolator", dataLine);
                            OnWarningEvent(ConsoleOutputErrorMsg);
                        }
                        else
                        {
                            ConsoleOutputErrorMsg += "; " + dataLine;
                            OnWarningEvent(dataLine);
                        }
                    }
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
        /// <param name="consoleOutputFilePath">Conso0le output file path</param>
        /// <param name="toolType">Tool type</param>
        public void ParsePhilosopherConsoleOutputFile(
            string consoleOutputFilePath,
            AnalysisToolRunnerPepProtProphet.PhilosopherToolType toolType)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output when initializing the workspace
            // ----------------------------------------------------

            // INFO[17:45:51] Executing Workspace  v4.0.0
            // INFO[17:45:51] Removing workspace
            // INFO[17:45:51] Done

            // ----------------------------------------------------
            // Example Console output when running PeptideProphet
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
            // Example Console output when running ProteinProphet
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

            // INFO[19:00:24] Executing Label-free quantification  v5.0.0
            // INFO[19:00:24] Indexing PSM information
            // INFO[19:00:24] Reading spectra and tracing peaks
            // INFO[19:00:24] Processing Emory_Rush_TMT_b02_04
            // INFO[19:00:53] Assigning intensities to data layers
            // INFO[19:00:53] Done

            // ----------------------------------------------------
            // Example Console output when running LabelQuant

            // INFO[19:01:58] Executing Isobaric-label quantification  v5.0.0
            // INFO[19:01:58] Calculating intensities and ion interference
            // INFO[19:01:58] Processing Emory_Rush_TMT_b02_04
            // INFO[19:02:25] Filtering spectra for label quantification
            // INFO[19:02:25] Removing 313 PSMs from isobaric quantification
            // INFO[19:02:25] Saving
            // INFO[19:02:26] Done

            // ----------------------------------------------------
            // Example Console output when running iProphet

            // INFO[19:29:20] Executing InterProphet  v5.0.0
            // Running FPKM NSS NRS NSE NSI NSM Model EM:
            // Computing NSS values ...

            // ReSharper restore CommentTypo

            try
            {
                var currentPhilosopherTool = AnalysisToolRunnerPepProtProphet.GetCurrentPhilosopherToolDescription(toolType);

                var consoleOutputFile = new FileInfo(consoleOutputFilePath);

                if (!consoleOutputFile.Exists)
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
                    OnDebugEvent("Parsing file {0} while running {1}", consoleOutputFilePath, currentPhilosopherTool);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLineWithColor = reader.ReadLine() ?? string.Empty;

                    var dataLine = ColorTagMatcher.Replace(dataLineWithColor, string.Empty);

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (toolType == AnalysisToolRunnerPepProtProphet.PhilosopherToolType.ShowVersion)
                    {
                        var match = PhilosopherVersionMatcher.Match(dataLine);

                        if (match.Success)
                        {
                            PhilosopherVersion = "Philosopher " + match.Groups["Version"];
                        }
                    }

                    // ReSharper disable once StringLiteralTypo
                    if (dataLine.StartsWith("ERRO") && !ConsoleOutputErrorMsg.Contains(dataLine))
                    {
                        if (LineHasRemoveFileError(dataLine))
                        {
                            // The message is similar to the following, and can be ignored
                            // remove C:\Users\D3L243\AppData\Local\Temp\8c5fd63b-9cb8-4fdb-ab2a-62cef7285253\DatabaseParser.exe: The process cannot access the file because it is being used by another process.
                        }
                        else
                        {
                            // Error
                            if (string.IsNullOrWhiteSpace(ConsoleOutputErrorMsg))
                            {
                                ConsoleOutputErrorMsg = string.Format("Error running {0}: {1}", "Philosopher", dataLine);
                                OnWarningEvent(ConsoleOutputErrorMsg);
                            }
                            else
                            {
                                ConsoleOutputErrorMsg += "; " + dataLine;
                                OnWarningEvent(dataLine);
                            }
                        }
                    }

                    // ReSharper disable once InvertIf
                    // ReSharper disable once StringLiteralTypo
                    if ((dataLine.StartsWith("FATA") ||
                         dataLine.StartsWith(PHILOSOPHER_PANIC_ERROR) ||
                         dataLine.Contains(PHILOSOPHER_RUNTIME_ERROR)
                        ) && !ConsoleOutputErrorMsg.Contains(dataLine))
                    {
                        // Fatal error
                        if (string.IsNullOrWhiteSpace(ConsoleOutputErrorMsg))
                        {
                            ConsoleOutputErrorMsg = string.Format("Error running {0}: {1}", "Philosopher", dataLine);
                            OnWarningEvent(ConsoleOutputErrorMsg);
                        }
                        else
                        {
                            ConsoleOutputErrorMsg += "; " + dataLine;
                            OnWarningEvent(dataLine);
                        }
                    }
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

        /// <summary>
        /// Parse the PTM Prophet console output file
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        public void ParsePTMProphetConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output

            // [INFO:] Using statically set 15 PPM tolerance ...
            // [INFO:] Writing file interact-NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pep.xml.tmp.a52124 ...
            // [INFO:] Creating 1 threads
            // [INFO:] Wait for threads to finish ...
            // [INFO:] Reading file interact-NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pep.xml ...
            // [INFO:] processed 1000/8771 spectrum_queries
            // [INFO:] processed 2000/8771 spectrum_queries
            // [INFO:] processed 3000/8771 spectrum_queries
            // [INFO:] processed 4000/8771 spectrum_queries
            // [INFO:] processed 5000/8771 spectrum_queries
            // [INFO:] processed 6000/8771 spectrum_queries
            // [INFO:] processed 7000/8771 spectrum_queries
            // [INFO:] processed 8000/8771 spectrum_queries
            // [INFO:] done ...
            // [INFO:] Computing EM Models ...
            // [INFO:] Iterating PTM Model: ....5....done
            // [INFO:] done ...
            // [INFO:] done ...
            // [INFO:] Creating 1 threads
            // [INFO:] Wait for threads to finish ...
            // [INFO:] Reading file interact-NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pep.xml ...
            // [INFO:] written 1000/8771 spectrum_queries
            // [INFO:] written 2000/8771 spectrum_queries
            // [INFO:] written 3000/8771 spectrum_queries
            // [INFO:] written 4000/8771 spectrum_queries
            // [INFO:] written 5000/8771 spectrum_queries
            // [INFO:] written 6000/8771 spectrum_queries
            // [INFO:] written 7000/8771 spectrum_queries
            // [INFO:] written 8000/8771 spectrum_queries
            // [INFO:] done ...
            // [INFO:] Writing file interact-NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml ...
            // [INFO:] done ...

            // ReSharper restore CommentTypo

            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilePath);

                if (!consoleOutputFile.Exists)
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
                    OnDebugEvent("Parsing file " + consoleOutputFile.FullName);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLineWithColor = reader.ReadLine() ?? string.Empty;

                    var dataLine = ColorTagMatcher.Replace(dataLineWithColor, string.Empty);

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // ToDo: customize the check for errors

                    // ReSharper disable once InvertIf
                    if (dataLine.StartsWith("Error:") && !ConsoleOutputErrorMsg.Contains(dataLine))
                    {
                        // Fatal error
                        if (string.IsNullOrWhiteSpace(ConsoleOutputErrorMsg))
                        {
                            ConsoleOutputErrorMsg = string.Format("Error running {0}: {1}", "PTM Prophet", dataLine);
                            OnWarningEvent(ConsoleOutputErrorMsg);
                        }
                        else
                        {
                            ConsoleOutputErrorMsg += "; " + dataLine;
                            OnWarningEvent(dataLine);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (DebugLevel >= 2)
                {
                    OnErrorNoMessageUpdate("Error parsing the PTM Prophet console output file (" + consoleOutputFilePath + "): " + ex.Message);
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
                {"Plotting mass distribution", 98}
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

                        if (string.IsNullOrEmpty(PhilosopherVersion) && versionTextStartIndex >= 0)
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
