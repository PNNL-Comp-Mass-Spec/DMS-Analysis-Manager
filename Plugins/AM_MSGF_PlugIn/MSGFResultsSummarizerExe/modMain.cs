// This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins
// It creates a text result file and posts the results to the DMS database
//
// -------------------------------------------------------------------------------
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Program started in February, 2012
//
// E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
// Website: http://omics.pnl.gov/ or http://www.sysbio.org/resources/staff/ or http://panomics.pnnl.gov/
// -------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using PHRPReader;
using System.IO;
using MSGFResultsSummarizer;
using PRISM;

namespace MSGFResultsSummarizerExe
{
    static class modMain
    {
        private const string PROGRAM_DATE = "March 31, 2017";
        private static string mMSGFSynFilePath = string.Empty;
        private static string mInputFolderPath = string.Empty;

        private static string mOutputFolderPath = string.Empty;
        private static string mDatasetName = string.Empty;

        private static bool mContactDatabase = true;
        private static int mJob;
        private static bool mSaveResultsAsText = true;

        private static bool mPostResultsToDb;

        public static int Main()
        {
            // Returns 0 if no error, error code if an error

            var objParseCommandLine = new clsParseCommandLine();

            try
            {
                var blnProceed = false;
                if (objParseCommandLine.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
                        blnProceed = true;
                }

                if (!blnProceed || objParseCommandLine.NeedToShowHelp)
                {
                    ShowProgramHelp();
                    return -1;
                }

                if ((objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0))
                {
                    ShowProgramHelp();
                    return -1;
                }

                if (string.IsNullOrEmpty(mMSGFSynFilePath) && string.IsNullOrEmpty(mInputFolderPath))
                {
                    ShowErrorMessage("Must define either the MSGFSynFilePath or InputFolderPath");
                    ShowProgramHelp();
                    return -1;
                }

                var blnSuccess = SummarizeMSGFResults();

                if (!blnSuccess)
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred in modMain->Main: \n" + ex.Message);
                return -1;
            }

            return 0;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SummarizeMSGFResults()
        {
            var blnSuccess = false;

            try
            {
                // Initialize a dictionary object that will be used to either find the appropriate input file, or determine the file type of the specified input file
                var dctFileSuffixes = new Dictionary<string, clsPHRPReader.ePeptideHitResultType>
                {
                    {"_xt_MSGF.txt", clsPHRPReader.ePeptideHitResultType.XTandem},
                    {"_msgfdb_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.MSGFDB},
                    {"_inspect_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Inspect},
                    {"_syn_MSGF.txt", clsPHRPReader.ePeptideHitResultType.Sequest},
                    {"_msalign_syn.txt", clsPHRPReader.ePeptideHitResultType.MSAlign},
                    {"_mspath_syn.txt", clsPHRPReader.ePeptideHitResultType.MSPathFinder}
                };


                var eResultType = clsPHRPReader.ePeptideHitResultType.Unknown;

                if (string.IsNullOrWhiteSpace(mMSGFSynFilePath))
                {
                    if (string.IsNullOrWhiteSpace(mInputFolderPath))
                    {
                        ShowErrorMessage("Must define either the MSGFSynFilePath or InputFolderPath; unable to continue");
                        return false;
                    }

                    var diFolder = new DirectoryInfo(mInputFolderPath);
                    if (!diFolder.Exists)
                    {
                        ShowErrorMessage("Input folder not found: " + diFolder.FullName);
                        return false;
                    }

                    // Determine the input file path by looking for the expected files in mInputFolderPath
                    foreach (var suffixEntry in dctFileSuffixes)
                    {
                        var fiFiles = diFolder.GetFiles("*" + suffixEntry.Key);

                        if (fiFiles.Length > 0)
                        {
                            // Match found
                            mMSGFSynFilePath = fiFiles[0].FullName;
                            eResultType = suffixEntry.Value;
                            break;
                        }
                    }

                    var strSuffixesSearched = string.Join(", ", dctFileSuffixes.Keys.ToList());

                    if (eResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
                    {
                        var strMsg = "Did not find any files in the source folder with the expected file name suffixes\n" +
                            "Looked for " + strSuffixesSearched + " in \n" + diFolder.FullName;

                        ShowErrorMessage(strMsg);
                        return false;
                    }
                }
                else
                {
                    // Determine the result type of mMSGFSynFilePath

                    eResultType = clsPHRPReader.AutoDetermineResultType(mMSGFSynFilePath);

                    if (eResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
                    {
                        foreach (var suffixEntry in dctFileSuffixes)
                        {
                            if (mMSGFSynFilePath.ToLower().EndsWith(suffixEntry.Key.ToLower()))
                            {
                                // Match found
                                eResultType = suffixEntry.Value;
                                break;
                            }
                        }
                    }

                    if (eResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
                    {
                        ShowErrorMessage("Unable to determine result type from input file name: " + mMSGFSynFilePath);
                        return false;
                    }
                }

                var fiSourceFile = new FileInfo(mMSGFSynFilePath);
                if (!fiSourceFile.Exists)
                {
                    ShowErrorMessage("Input file not found: " + fiSourceFile.FullName);
                    return false;
                }

                if (string.IsNullOrWhiteSpace(mDatasetName))
                {
                    // Auto-determine the dataset name
                    mDatasetName = clsPHRPReader.AutoDetermineDatasetName(fiSourceFile.Name, eResultType);

                    if (string.IsNullOrEmpty(mDatasetName))
                    {
                        ShowErrorMessage("Unable to determine dataset name from input file name: " + mMSGFSynFilePath);
                        return false;
                    }
                }

                if (mJob == 0)
                {
                    // Auto-determine the job number by looking for _Auto000000 in the parent folder name

                    var intUnderscoreIndex = fiSourceFile.DirectoryName.LastIndexOf("_", StringComparison.Ordinal);

                    if (intUnderscoreIndex > 0)
                    {
                        var strNamePart = fiSourceFile.DirectoryName.Substring(intUnderscoreIndex + 1);
                        if (strNamePart.ToLower().StartsWith("auto"))
                        {
                            strNamePart = strNamePart.Substring(4);
                            int.TryParse(strNamePart, out mJob);
                        }
                    }

                    if (mJob == 0)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Warning: unable to parse out the job number from " + fiSourceFile.DirectoryName);
                        Console.WriteLine();
                    }
                }

                var objSummarizer = new clsMSGFResultsSummarizer(eResultType, mDatasetName, mJob, fiSourceFile.Directory.FullName);
                objSummarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

                objSummarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD;
                objSummarizer.EValueThreshold = clsMSGFResultsSummarizer.DEFAULT_EVALUE_THRESHOLD;
                objSummarizer.FDRThreshold = clsMSGFResultsSummarizer.DEFAULT_FDR_THRESHOLD;

                objSummarizer.OutputFolderPath = mOutputFolderPath;
                objSummarizer.PostJobPSMResultsToDB = mPostResultsToDb;
                objSummarizer.SaveResultsToTextFile = mSaveResultsAsText;
                objSummarizer.DatasetName = mDatasetName;
                objSummarizer.ContactDatabase = mContactDatabase;

                blnSuccess = objSummarizer.ProcessMSGFResults();

                if (!blnSuccess)
                {
                    if (!string.IsNullOrWhiteSpace(objSummarizer.ErrorMessage))
                    {
                        ShowErrorMessage("Processing failed: " + objSummarizer.ErrorMessage);
                    }
                    else
                    {
                        ShowErrorMessage("Processing failed (unknown reason)");
                    }
                }

                Console.WriteLine("Result Type: ".PadRight(25) + objSummarizer.ResultTypeName);

                string strFilterText;

                if (objSummarizer.ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    Console.WriteLine("EValue Threshold: ".PadRight(25) + objSummarizer.EValueThreshold.ToString("0.00E+00"));
                    strFilterText = "EValue";
                }
                else
                {
                    Console.WriteLine("MSGF Threshold: ".PadRight(25) + objSummarizer.MSGFThreshold.ToString("0.00E+00"));
                    strFilterText = "MSGF";
                }

                Console.WriteLine("FDR Threshold: ".PadRight(25) + (objSummarizer.FDRThreshold * 100).ToString("0.0") + "%");
                Console.WriteLine("Spectra Searched: ".PadRight(25) + objSummarizer.SpectraSearched.ToString("#,##0"));
                Console.WriteLine();
                Console.WriteLine(("Total PSMs (" + strFilterText + " Filter): ").PadRight(35) + objSummarizer.TotalPSMsMSGF);
                Console.WriteLine(("Unique Peptides (" + strFilterText + " Filter): ").PadRight(35) + objSummarizer.UniquePeptideCountMSGF);
                Console.WriteLine(("Unique Proteins (" + strFilterText + " Filter): ").PadRight(35) + objSummarizer.UniqueProteinCountMSGF);

                Console.WriteLine();
                Console.WriteLine("Total PSMs (FDR Filter): ".PadRight(35) + objSummarizer.TotalPSMsFDR);
                Console.WriteLine("Unique Peptides (FDR Filter): ".PadRight(35) + objSummarizer.UniquePeptideCountFDR);
                Console.WriteLine("Tryptic Peptides (FDR Filter): ".PadRight(35) + objSummarizer.TrypticPeptidesFDR);
                Console.WriteLine("Unique Proteins (FDR Filter): ".PadRight(35) + objSummarizer.UniqueProteinCountFDR);

                Console.WriteLine();
                Console.WriteLine("Unique Phosphopeptides (FDR Filter): ".PadRight(35) + objSummarizer.UniquePhosphopeptideCountFDR);
                Console.WriteLine("Phosphopeptides with C-term K: ".PadRight(35) + objSummarizer.UniquePhosphopeptidesCTermK_FDR);
                Console.WriteLine("Phosphopeptides with C-term R: ".PadRight(35) + objSummarizer.UniquePhosphopeptidesCTermR_FDR);

                Console.WriteLine();
                Console.WriteLine("Missed Cleavage Ratio (FDR Filter): ".PadRight(35) + objSummarizer.MissedCleavageRatioFDR);
                Console.WriteLine("Missed Cleavage Ratio for Phosphopeptides: ".PadRight(35) + objSummarizer.MissedCleavageRatioPhosphoFDR);

                Console.WriteLine();
                Console.WriteLine("Keratin Peptides (FDR Filter): ".PadRight(35) + objSummarizer.KeratinPeptidesFDR);
                Console.WriteLine("Trypsin Peptides (FDR Filter): ".PadRight(35) + objSummarizer.TrypsinPeptidesFDR);

                Console.WriteLine();
                Console.WriteLine("Percent MSn Scans No PSM: ".PadRight(38) + objSummarizer.PercentMSnScansNoPSM.ToString("0.0") + "%");
                Console.WriteLine("Maximum Scan Gap Adjacent MSn Scans: ".PadRight(38) + objSummarizer.MaximumScanGapAdjacentMSn);

                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in SummarizeMSGFResults: " + ex.Message);
            }

            return blnSuccess;
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false

            var strValidParameters = new List<string>
            {
                "I",
                "Folder",
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
                if (objParseCommandLine.InvalidParametersPresent(strValidParameters))
                {
                    return false;
                }
                else
                {
                    // Query objParseCommandLine to see if various parameters are present
                    string strValue;
                    if (objParseCommandLine.RetrieveValueForParameter("I", out strValue))
                    {
                        mMSGFSynFilePath = strValue;
                    }
                    else if (objParseCommandLine.NonSwitchParameterCount > 0)
                    {
                        mMSGFSynFilePath = objParseCommandLine.RetrieveNonSwitchParameter(0);
                    }

                    if (objParseCommandLine.RetrieveValueForParameter("Folder", out strValue))
                    {
                        mInputFolderPath = strValue;
                    }

                    if (objParseCommandLine.RetrieveValueForParameter("Dataset", out strValue))
                    {
                        mDatasetName = strValue;
                    }

                    if (objParseCommandLine.RetrieveValueForParameter("Job", out strValue))
                    {
                        if (!int.TryParse(strValue, out mJob))
                        {
                            ShowErrorMessage("Job number not numeric: " + strValue);
                            return false;
                        }
                    }

                    objParseCommandLine.RetrieveValueForParameter("O", out mOutputFolderPath);

                    if (objParseCommandLine.IsParameterPresent("NoDatabase"))
                    {
                        mContactDatabase = false;
                    }

                    if (objParseCommandLine.IsParameterPresent("NoText"))
                    {
                        mSaveResultsAsText = false;
                    }

                    if (objParseCommandLine.IsParameterPresent("DB"))
                    {
                        mPostResultsToDb = true;
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error parsing the command line parameters: \n" + ex.Message);
            }

            return true;
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.WriteLine(message);
            if (ex != null)
            {
                Console.WriteLine(PRISM.clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex));
            }

            Console.WriteLine(strSeparator);
            Console.WriteLine();
        }

        private static void ShowProgramHelp()
        {
            try
            {
                Console.WriteLine("This program parses MSGF synopsis file results to summarize the number of identified peptides and proteins");
                Console.WriteLine("It creates a text result file and optionally posts the results to the DMS database");
                Console.WriteLine("Peptides are first filtered on MSGF_SpecProb < 1E-10");
                Console.WriteLine("They are next filtered on FDR < 1%");
                Console.WriteLine();
                Console.WriteLine("Program syntax:\n" + Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location));
                Console.WriteLine(" [MSGFSynFilePath] [/Folder:InputFolderPath] [/Dataset:DatasetName]");
                Console.WriteLine(" [/Job:JobNumber] [/O:OutputFolderPath] [/NoText] [/DB]");
                Console.WriteLine();
                Console.WriteLine("MSGFSynFilePath defines the data file to process, for example QC_Shew_11_06_pt5_c_21Feb12_Sphinx_11-08-09_syn_MSGF.txt");
                Console.WriteLine("The name of the source file will be auto-determined if the input folder is defined via /Folder");
                Console.WriteLine();
                Console.WriteLine("/Folder defines the input folder to process (and also to create the text result file in if /O is not used)");
                Console.WriteLine();
                Console.WriteLine("/Dataset defines the dataset name; if /Dataset is not used, then the name will be auto-determined");
                Console.WriteLine();
                Console.WriteLine("/Job defines the analysis job; if /Job is not provided, then will auto-determine the job number using the input folder name");
                Console.WriteLine();
                Console.WriteLine("Use /O to define a custom output folder path");
                Console.WriteLine();
                Console.WriteLine("Use /NoDatabase to indicate that DMS should not be contacted to lookup scan stats for the dataset");
                Console.WriteLine();
                Console.WriteLine("Use /NoText to specify that a text file not be created");
                Console.WriteLine("Use /DB to post results to DMS");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2012");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/");
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error displaying the program syntax: " + ex.Message);
            }
        }

        #region "Event Handlers"

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage">Error Message</param>
        /// <param name="ex">Exception</param>
        private static void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            ShowErrorMessage(errorMessage, ex);
        }

        #endregion
    }
}
