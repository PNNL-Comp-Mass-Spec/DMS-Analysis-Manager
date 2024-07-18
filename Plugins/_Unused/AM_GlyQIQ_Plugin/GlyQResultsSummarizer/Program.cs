using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerGlyQIQPlugin;
using PRISM;

namespace GlyQResultsSummarizer
{
    /// <summary>
    /// This program reads the GlyQ-IQ results file (Dataset_iqResults_.txt) for a given job,
    /// summarizes the results, and calls PostJobResults to store the results in DMS
    /// </summary>
    class Program
    {
        public const string PROGRAM_DATE = "April 25, 2016";
        private const int AUTO_JOB_FLAG = -5000;

        public const string DMS_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

        private static string mGlyQResultsFilePath;
        private static int mJob;

        static int Main(string[] args)
        {
            var parseCommandLine = new clsParseCommandLine();

            try
            {
                var success = false;

                if (parseCommandLine.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(parseCommandLine))
                        success = true;
                }

                if (!success ||
                    parseCommandLine.NeedToShowHelp ||
                    parseCommandLine.ParameterCount + parseCommandLine.NonSwitchParameterCount == 0 ||
                    string.IsNullOrWhiteSpace(mGlyQResultsFilePath))
                {
                    ShowProgramHelp();
                    return -1;

                }

                success = ProcessGlyQResultsFile(mGlyQResultsFilePath, mJob);

                if (!success)
                {
                    ShowErrorMessage("Call to ProcessGlyQResultsFile returned false");
                    return -3;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(ex.StackTrace);
                return -1;
            }

            return 0;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool ProcessGlyQResultsFile(string resultsFilePath, int job)
        {
            var reGetJobFromFolderName = new Regex(@"Auto(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var toolRunner = new AnalysisToolRunnerGlyQIQ();

            var resultsFile = new FileInfo(resultsFilePath);

            if (!resultsFile.Exists)
            {
                ShowErrorMessage("File not found: " + resultsFile.FullName);
                return false;
            }

            if (job == AUTO_JOB_FLAG)
            {
                if (resultsFile.Directory == null)
                {
                    ShowErrorMessage("Cannot determine the parent directory's name for " + resultsFile.FullName);
                    return false;
                }

                var parentDirectoryName = resultsFile.Directory.Name;
                var reMatch = reGetJobFromFolderName.Match(parentDirectoryName);

                if (!reMatch.Success)
                {
                    ShowErrorMessage("Parent directory's name does not end in Auto123456; cannot determine the job number: " + parentDirectoryName);
                    return false;
                }

                var jobText = reMatch.Groups[1].Value;

                if (!int.TryParse(jobText, out job))
                {
                    ShowErrorMessage("Regex parsing error; job number is not numeric: " + jobText);
                    return false;
                }
            }

            Console.WriteLine("Processing results for job " + job + ": " + resultsFile.Name);

            var success = toolRunner.ExamineFilteredResults(resultsFile, job, DMS_CONNECTION_STRING);

            return success;
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine parseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false
            var validParameters = new List<string> { "I", "Job" };

            try
            {
                // Make sure no invalid parameters are present
                if (parseCommandLine.InvalidParametersPresent(validParameters))
                {
                    var badArguments = new List<string>();

                    foreach (var item in parseCommandLine.InvalidParameters(validParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid command line parameters", badArguments);

                    return false;
                }

                // Query parseCommandLine to see if various parameters are present
                if (parseCommandLine.NonSwitchParameterCount > 0)
                {
                    mGlyQResultsFilePath = parseCommandLine.RetrieveNonSwitchParameter(0);
                }

                if (parseCommandLine.RetrieveValueForParameter("I", out var value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        ShowErrorMessage("/I does not have a file path defined");
                        return false;
                    }
                    mGlyQResultsFilePath = value;
                }

                if (parseCommandLine.RetrieveValueForParameter("Job", out value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        ShowErrorMessage("/Job does not have a job number defined");
                        return false;
                    }

                    if (String.Compare(value, "auto", StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        // Automatically determine the job number based on the parent folder name
                        mJob = AUTO_JOB_FLAG;
                    }
                    else
                    {
                        if (!int.TryParse(value, out mJob))
                        {
                            ShowErrorMessage(value + " is not numeric for /Job");
                            return false;
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }

            return false;
        }


        private static void ShowErrorMessage(string message)
        {
            ConsoleMsgUtils.ShowError(message);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            var exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("This program reads the GlyQ-IQ results file (Dataset_iqResults_.txt) for a given job");
                Console.WriteLine("summarizes the results, and calls PostJobResults to store the results in DMS");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine + exeName);
                Console.WriteLine(" GlyQResultsFilePath.txt /Job:JobNumber");
                Console.WriteLine();
                Console.WriteLine("The first parameter is the path to a tab-delimited text file with the GlyQ results to parse");
                Console.WriteLine();
                Console.WriteLine("Use /Job to specify the Job Number for this results file");
                Console.WriteLine();
                Console.WriteLine("If processing a file tracked in DMS, you can use /Job:Auto and this program will");
                Console.WriteLine("automatically determine the Job number from the parent folder name.  This will");
                Console.WriteLine("only work if the parent folder is of the form GLY201503151614_Auto1170022");
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
                ShowErrorMessage("Error displaying the program syntax: " + ex.Message);
            }

        }

    }
}
