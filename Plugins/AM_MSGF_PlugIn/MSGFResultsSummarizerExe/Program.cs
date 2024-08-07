// This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins
// It creates a text result file and posts the results to the DMS database
//
// -------------------------------------------------------------------------------
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Program started in February 2012
//
// E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
// Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics
// -------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using MSGFResultsSummarizer;
using PHRPReader;
using PRISM;

namespace MSGFResultsSummarizerExe
{
    internal static class Program
    {
        // Ignore Spelling: msgfdb, mspath, Phosphopeptides, Tryptic, xt

        private const string PROGRAM_DATE = "April 8, 2024";

        private static string mMSGFSynFilePath = string.Empty;
        private static string mInputDirectoryPath = string.Empty;

        private static string mOutputDirectoryPath = string.Empty;
        private static string mDatasetName = string.Empty;

        private static bool mContactDatabase = true;
        private static int mJob;
        private static bool mSaveResultsAsText = true;

        private static bool mPostResultsToDb;

        /// <summary>
        /// Program entry method
        /// </summary>
        /// <returns>0 if no error, error code if an error</returns>
        public static int Main()
        {
            var commandLineParser = new clsParseCommandLine();

            try
            {
                var proceed = commandLineParser.ParseCommandLine() && SetOptionsUsingCommandLineParameters(commandLineParser);

                if (!proceed || commandLineParser.NeedToShowHelp)
                {
                    ShowProgramHelp();
                    return -1;
                }

                if (commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount == 0)
                {
                    ShowProgramHelp();
                    return -1;
                }

                if (string.IsNullOrEmpty(mMSGFSynFilePath) && string.IsNullOrEmpty(mInputDirectoryPath))
                {
                    ShowErrorMessage("Must define either the MSGFSynFilePath or InputDirectoryPath");
                    ShowProgramHelp();
                    return -1;
                }

                var success = SummarizePSMResults();

                if (success)
                    return 0;

                ConsoleMsgUtils.SleepSeconds(1.5);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                ConsoleMsgUtils.SleepSeconds(1.5);
                return -1;
            }
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SummarizePSMResults()
        {
            try
            {
                // Initialize a dictionary object that will be used to either find the appropriate input file, or determine the file type of the specified input file
                var fileSuffixes = new Dictionary<string, PeptideHitResultTypes>
                {
                    {"_xt_MSGF.txt", PeptideHitResultTypes.XTandem},
                    {"_msgfdb_syn_MSGF.txt", PeptideHitResultTypes.MSGFPlus},
                    {"_msgfplus_syn_MSGF.txt", PeptideHitResultTypes.MSGFPlus},
                    {"_inspect_syn_MSGF.txt", PeptideHitResultTypes.Inspect},
                    {"_syn_MSGF.txt", PeptideHitResultTypes.Sequest},
                    {"_msalign_syn.txt", PeptideHitResultTypes.MSAlign},
                    {"_mspath_syn.txt", PeptideHitResultTypes.MSPathFinder},
                    {"_maxq_syn.txt", PeptideHitResultTypes.MaxQuant}
                };

                var resultType = PeptideHitResultTypes.Unknown;

                if (string.IsNullOrWhiteSpace(mMSGFSynFilePath))
                {
                    if (string.IsNullOrWhiteSpace(mInputDirectoryPath))
                    {
                        ShowErrorMessage("Must define either the MSGFSynFilePath or InputDirectoryPath; unable to continue");
                        return false;
                    }

                    var inputDirectory = new DirectoryInfo(mInputDirectoryPath);

                    if (!inputDirectory.Exists)
                    {
                        ShowErrorMessage("Input directory not found: " + inputDirectory.FullName);
                        return false;
                    }

                    // Determine the input file path by looking for the expected files in mInputDirectoryPath
                    foreach (var suffixEntry in fileSuffixes)
                    {
                        var matchingFiles = inputDirectory.GetFiles("*" + suffixEntry.Key);

                        if (matchingFiles.Length > 0)
                        {
                            // Match found
                            mMSGFSynFilePath = matchingFiles[0].FullName;
                            resultType = suffixEntry.Value;
                            break;
                        }
                    }

                    var suffixesSearched = string.Join(", ", fileSuffixes.Keys.ToList());

                    if (resultType == PeptideHitResultTypes.Unknown)
                    {
                        var warningMessage = "Did not find any files in the source directory with the expected file name suffixes\n" +
                            "Looked for " + suffixesSearched + " in \n" + inputDirectory.FullName;

                        ShowErrorMessage(warningMessage);
                        return false;
                    }
                }
                else
                {
                    // Determine the result type of mMSGFSynFilePath

                    resultType = ReaderFactory.AutoDetermineResultType(mMSGFSynFilePath);

                    if (resultType == PeptideHitResultTypes.Unknown)
                    {
                        foreach (var suffixEntry in fileSuffixes)
                        {
                            if (mMSGFSynFilePath.EndsWith(suffixEntry.Key, StringComparison.OrdinalIgnoreCase))
                            {
                                // Match found
                                resultType = suffixEntry.Value;
                                break;
                            }
                        }
                    }

                    if (resultType == PeptideHitResultTypes.Unknown)
                    {
                        ShowErrorMessage("Unable to determine result type from input file name: " + mMSGFSynFilePath);
                        return false;
                    }
                }

                var sourceFile = new FileInfo(mMSGFSynFilePath);

                if (!sourceFile.Exists)
                {
                    ShowErrorMessage("Input file not found: " + sourceFile.FullName);
                    return false;
                }

                if (string.IsNullOrWhiteSpace(mDatasetName))
                {
                    // Auto-determine the dataset name
                    mDatasetName = ReaderFactory.AutoDetermineDatasetName(sourceFile.Name, resultType);

                    if (string.IsNullOrEmpty(mDatasetName))
                    {
                        ShowErrorMessage("Unable to determine dataset name from input file name: " + mMSGFSynFilePath);
                        return false;
                    }
                }

                if (mJob == 0)
                {
                    // Auto-determine the job number by looking for _Auto000000 in the parent directory name

                    var underscoreIndex = sourceFile.DirectoryName.LastIndexOf("_", StringComparison.Ordinal);

                    if (underscoreIndex > 0)
                    {
                        var namePart = sourceFile.DirectoryName.Substring(underscoreIndex + 1);

                        if (namePart.StartsWith("auto", StringComparison.OrdinalIgnoreCase))
                        {
                            namePart = namePart.Substring(4);
                            int.TryParse(namePart, out mJob);
                        }
                    }

                    if (mJob == 0)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Warning: unable to parse out the job number from " + sourceFile.DirectoryName);
                        Console.WriteLine();
                    }
                }

                const int DEBUG_LEVEL = 1;

                // private const string GIGASAX_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

                const string CONNECTION_STRING = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";

                var summarizer = new ResultsSummarizer(resultType, mDatasetName, mJob, sourceFile.Directory.FullName, CONNECTION_STRING, DEBUG_LEVEL, false)
                {
                    MSGFSpecEValueOrPEPThreshold = ResultsSummarizer.DEFAULT_MSGF_SPEC_EVALUE_THRESHOLD,
                    EValueThreshold = ResultsSummarizer.DEFAULT_EVALUE_THRESHOLD,
                    FDRThreshold = ResultsSummarizer.DEFAULT_FDR_THRESHOLD,
                    OutputDirectoryPath = mOutputDirectoryPath,
                    PostJobPSMResultsToDB = mPostResultsToDb,
                    SaveResultsToTextFile = mSaveResultsAsText,
                    DatasetName = mDatasetName,
                    ContactDatabase = mContactDatabase
                };

                summarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

                var success = summarizer.ProcessPSMResults();

                if (!success)
                {
                    if (!string.IsNullOrWhiteSpace(summarizer.ErrorMessage))
                    {
                        ShowErrorMessage("Processing failed: " + summarizer.ErrorMessage);
                    }
                    else
                    {
                        ShowErrorMessage("Processing failed (unknown reason)");
                    }
                }

                Console.WriteLine("Result Type: ".PadRight(25) + summarizer.ResultTypeName);

                string filterText;

                if (summarizer.ResultType == PeptideHitResultTypes.MSAlign)
                {
                    Console.WriteLine("EValue Threshold: ".PadRight(25) + summarizer.EValueThreshold.ToString("0.00E+00"));
                    filterText = "EValue";
                }
                else if (summarizer.ResultType is PeptideHitResultTypes.DiaNN or PeptideHitResultTypes.MaxQuant)
                {
                    Console.WriteLine("PEP Threshold: ".PadRight(25) + summarizer.MSGFSpecEValueOrPEPThreshold.ToString("0.00E+00"));
                    filterText = "PEP";
                }
                else
                {
                    Console.WriteLine("MSGF SpecEValue Threshold: ".PadRight(25) + summarizer.MSGFSpecEValueOrPEPThreshold.ToString("0.00E+00"));
                    filterText = "MSGF";
                }

                Console.WriteLine("FDR Threshold: ".PadRight(25) + (summarizer.FDRThreshold * 100).ToString("0.0") + "%");
                Console.WriteLine("Spectra Searched: ".PadRight(25) + summarizer.SpectraSearched.ToString("#,##0"));
                Console.WriteLine();
                Console.WriteLine(("Total PSMs (" + filterText + " Filter): ").PadRight(35) + summarizer.TotalPSMsMSGF);
                Console.WriteLine(("Unique Peptides (" + filterText + " Filter): ").PadRight(35) + summarizer.UniquePeptideCountMSGF);
                Console.WriteLine(("Unique Proteins (" + filterText + " Filter): ").PadRight(35) + summarizer.UniqueProteinCountMSGF);

                string detailedStatsFilter;

                if (summarizer.ResultType == PeptideHitResultTypes.MaxQuant)
                {
                    detailedStatsFilter = "PEP Filter";
                }
                else
                {
                    detailedStatsFilter = "FDR Filter";
                }

                Console.WriteLine();
                Console.WriteLine("Total PSMs (FDR Filter): ".PadRight(35) + summarizer.TotalPSMsFDR);
                Console.WriteLine("Unique Peptides (FDR Filter): ".PadRight(35) + summarizer.UniquePeptideCountFDR);
                Console.WriteLine("Unique Proteins (FDR Filter): ".PadRight(35) + summarizer.UniqueProteinCountFDR);

                Console.WriteLine(string.Format("Tryptic Peptides ({0}): ", detailedStatsFilter).PadRight(35) + summarizer.TrypticPeptidesFDR);

                Console.WriteLine();
                Console.WriteLine(string.Format("Unique Phosphopeptides ({0}): ", detailedStatsFilter).PadRight(35) + summarizer.UniquePhosphopeptideCountFDR);
                Console.WriteLine("Phosphopeptides with C-term K: ".PadRight(35) + summarizer.UniquePhosphopeptidesCTermK_FDR);
                Console.WriteLine("Phosphopeptides with C-term R: ".PadRight(35) + summarizer.UniquePhosphopeptidesCTermR_FDR);

                Console.WriteLine();
                Console.WriteLine(string.Format("Missed Cleavage Ratio ({0}): ", detailedStatsFilter).PadRight(35) + summarizer.MissedCleavageRatioFDR);
                Console.WriteLine("Missed Cleavage Ratio for Phosphopeptides: ".PadRight(35) + summarizer.MissedCleavageRatioPhosphoFDR);

                Console.WriteLine();
                Console.WriteLine(string.Format("Keratin Peptides ({0}): ", detailedStatsFilter).PadRight(35) + summarizer.KeratinPeptidesFDR);
                Console.WriteLine(string.Format("Trypsin Peptides ({0}): ", detailedStatsFilter).PadRight(35) + summarizer.TrypsinPeptidesFDR);

                Console.WriteLine();
                Console.WriteLine("Percent MSn Scans No PSM: ".PadRight(38) + summarizer.PercentMSnScansNoPSM.ToString("0.0") + "%");
                Console.WriteLine("Maximum Scan Gap Adjacent MSn Scans: ".PadRight(38) + summarizer.MaximumScanGapAdjacentMSn);

                Console.WriteLine();

                return success;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception in SummarizeMSGFResults", ex);
                return false;
            }
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns true if no problems; otherwise, returns false

            var validParameters = new List<string>
            {
                "I",
                "Directory",
                "Dataset",
                "Job",
                "O",
                "NoDatabase",
                "NoText",
                "DB"
            };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(validParameters))
                {
                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.RetrieveValueForParameter("I", out var synFilePath))
                {
                    mMSGFSynFilePath = synFilePath;
                }
                else if (commandLineParser.NonSwitchParameterCount > 0)
                {
                    mMSGFSynFilePath = commandLineParser.RetrieveNonSwitchParameter(0);
                }

                if (commandLineParser.RetrieveValueForParameter("Directory", out var inputDirectoryPath))
                {
                    mInputDirectoryPath = inputDirectoryPath;
                }

                if (commandLineParser.RetrieveValueForParameter("Dataset", out var datasetName))
                {
                    mDatasetName = datasetName;
                }

                if (commandLineParser.RetrieveValueForParameter("Job", out var jobText))
                {
                    if (!int.TryParse(jobText, out mJob))
                    {
                        ShowErrorMessage("Job number not numeric: " + jobText);
                        return false;
                    }
                }

                commandLineParser.RetrieveValueForParameter("O", out mOutputDirectoryPath);

                if (commandLineParser.IsParameterPresent("NoDatabase"))
                {
                    mContactDatabase = false;
                }

                if (commandLineParser.IsParameterPresent("NoText"))
                {
                    mSaveResultsAsText = false;
                }

                if (commandLineParser.IsParameterPresent("DB"))
                {
                    mPostResultsToDb = true;
                }

                return true;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error parsing the command line parameters", ex);
            }

            return true;
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                  "This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins. " +
                                  "It creates a text result file and optionally posts the results to the DMS database. " +
                                  "Peptides are first filtered on MSGF_SpecProb < 1E-10. " +
                                  "They are next filtered on FDR < 1%"));
                Console.WriteLine();
                Console.WriteLine("Program syntax:\n" + Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location));
                Console.WriteLine(" [MSGFSynFilePath] [/Directory:InputDirectoryPath] [/Dataset:DatasetName]");
                Console.WriteLine(" [/Job:JobNumber] [/O:OutputDirectoryPath] [/NoText] [/DB]");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "MSGFSynFilePath defines the data file to process, for example QC_Shew_11_06_pt5_c_21Feb12_Sphinx_11-08-09_syn_MSGF.txt. " +
                                      "The name of the source file will be auto-determined if the input directory is defined via /Directory"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph("/Directory defines the input directory to process (and also to create the text result file in if /O is not used)"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph("/Dataset defines the dataset name; if /Dataset is not used, the name will be auto-determined"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph("/Job defines the analysis job; if /Job is not provided, the job number will be auto-determined using the input directory name"));
                Console.WriteLine();
                Console.WriteLine("Use /O to define a custom output directory path");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph("Use /NoDatabase to indicate that DMS should not be contacted to lookup scan stats for the dataset"));
                Console.WriteLine();
                Console.WriteLine("Use /NoText to specify that a text file not be created");
                Console.WriteLine("Use /DB to post results to DMS");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics");
                Console.WriteLine();

                // Delay for 750 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error displaying the program syntax", ex);
            }
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage">Error Message</param>
        /// <param name="ex">Exception</param>
        private static void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            ShowErrorMessage(errorMessage, ex);
        }
    }
}
