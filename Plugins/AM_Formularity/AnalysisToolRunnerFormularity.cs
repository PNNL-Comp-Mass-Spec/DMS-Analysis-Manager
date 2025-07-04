using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerFormularityPlugin
{
    /// <summary>
    /// Class for running Formularity
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerFormularity : AnalysisToolRunnerBase
    {
        // Ignore Spelling: calibrant, cia, Formularity, href, html, nomsi, png, Pre

        private const int PROGRESS_PCT_STARTING_FORMULARITY = 5;
        private const int PROGRESS_PCT_FINISHED_FORMULARITY = 95;
        private const int PROGRESS_PCT_FINISHED_NOMSI = 97;

        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string FORMULARITY_CONSOLE_OUTPUT_FILE = "Formularity_ConsoleOutput.txt";

        private const string INDEX_HTML = "index.html";

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        /// <summary>
        /// Processes data using Formularity
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerFormularity.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to Formularity
                var progLoc = DetermineProgramLocation("FormularityProgLoc", "CIA.exe");

                // Determine the path to NOMSI
                var progLocNOMSI = DetermineProgramLocation("NOMSIProgLoc", "NOMSI.exe");

                if (string.IsNullOrWhiteSpace(progLoc) || string.IsNullOrWhiteSpace(progLocNOMSI))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Formularity.exe version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Formularity version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip the input files (if required)
                var datasetScansFile = mJobParams.GetJobParameter(
                    AnalysisJob.STEP_PARAMETERS_SECTION,
                    AnalysisResourcesFormularity.JOB_PARAM_FORMULARITY_DATASET_SCANS_FILE,
                    string.Empty);

                string datasetScansFilePath;

                if (datasetScansFile.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                {
                    var zipFilePath = Path.Combine(mWorkDir, datasetScansFile);
                    var unzipSuccess = UnzipFile(zipFilePath, mWorkDir);

                    if (!unzipSuccess)
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "Unknown error unzipping " + Path.GetFileName(datasetScansFile);
                        }
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (datasetScansFile.EndsWith("_scans.zip", StringComparison.OrdinalIgnoreCase))
                    {
                        // The .zip file had a series of .xml files; leave datasetScansFilePath blank
                        datasetScansFilePath = string.Empty;
                    }
                    else
                    {
                        // datasetScansFile should have been named Dataset_peaks.zip
                        // Thus, we want to process Dataset_peaks.txt
                        datasetScansFilePath = Path.ChangeExtension(zipFilePath, ".txt");

                        if (!File.Exists(datasetScansFilePath))
                        {
                            mMessage = "Dataset scans file not found in the working directory: " + Path.GetFileName(datasetScansFilePath);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
                else
                {
                    datasetScansFilePath = Path.Combine(mWorkDir, datasetScansFile);
                }

                // Process the data using Formularity
                var processingSuccess = ProcessScansWithFormularity(progLoc, datasetScansFilePath, out var nothingToAlign);

                CloseOutType eReturnCode;

                if (nothingToAlign)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_NO_DATA;
                }
                else if (!processingSuccess)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Look for the result files and call NOMSI to create plots
                    eReturnCode = PostProcessResults(progLocNOMSI);
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                mJobParams.AddResultFileToSkip("JobParameters_" + mJob + ".xml");

                var success = CopyResultsToTransferDirectory();

                return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in FormularityPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory(bool includeSubdirectories = false)
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);

                foreach (var spectrumFile in GetXmlSpectraFiles(workingDirectory, out _))
                {
                    spectrumFile.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveDirectory(includeSubdirectories);
        }

        private CloseOutType CreatePlotViewHTML(FileSystemInfo workDir, IEnumerable<FileInfo> pngFiles)
        {
            const string LITERAL_TEXT_FLAG = "text: ";

            try
            {
                var htmlFilePath = Path.Combine(workDir.FullName, INDEX_HTML);

                var datasetDetailReportText =
                    string.Format("DMS <a href='http://dms2.pnl.gov/dataset/show/{0}'> Dataset Detail Report </a>", mDatasetName);

                var datasetDetailReportLink = LITERAL_TEXT_FLAG + datasetDetailReportText;
                var pngFileTableLayout = PngToPdfConverter.GetPngFileTableLayout(mDatasetName, datasetDetailReportLink);

                var pngFileNames = new SortedSet<string>();

                foreach (var item in pngFiles)
                {
                    pngFileNames.Add(item.Name);
                }

                using var writer = new StreamWriter(new FileStream(htmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 3.2//EN\">");
                writer.WriteLine("<html>");
                writer.WriteLine("<head>");
                writer.WriteLine("  <title>{0}</title>", mDatasetName);
                writer.WriteLine("</head>");
                writer.WriteLine();
                writer.WriteLine("<body>");
                writer.WriteLine("  <h2>{0}</h2>", mDatasetName);
                writer.WriteLine();
                writer.WriteLine("  <table>");

                foreach (var tableRow in pngFileTableLayout)
                {
                    writer.WriteLine("    <tr>");

                    foreach (var tableCell in tableRow)
                    {
                        if (tableCell.StartsWith(LITERAL_TEXT_FLAG))
                        {
                            writer.WriteLine("      <td>{0}</td>", tableCell.Substring(LITERAL_TEXT_FLAG.Length));
                        }
                        else
                        {
                            var pngFileName = tableCell;

                            if (pngFileNames.Contains(pngFileName))
                            {
                                writer.WriteLine("      <td><a href='{0}'><img src='{0}' width='500' border='0'></a></td>", pngFileName);
                            }
                            else
                            {
                                writer.WriteLine("      <td>File not found: {0}</td>", pngFileName);
                            }
                        }
                    }
                    writer.WriteLine("    </tr>");
                }

                writer.WriteLine("  </table>");
                writer.WriteLine();
                writer.WriteLine("</body>");
                writer.WriteLine("</html>");

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error creating the HTML linking to the plots from NOMSI";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Create a PDF file using the PNG plot files
        /// </summary>
        private CloseOutType CreatePDFFromPlots(FileSystemInfo workDir, List<FileInfo> pngFiles)
        {
            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Creating PDF file with plots from NOMSI");
                }

                var converter = new PngToPdfConverter(mDatasetName);
                RegisterEvents(converter);

                var outputFilePath = Path.Combine(workDir.FullName, mDatasetName + "_NOMSI_plots.pdf");

                var success = converter.CreatePdf(outputFilePath, pngFiles);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error creating the PDF file with NOMSI plots";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error creating a PDF file with the plots from NOMSI";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType CreatePlotsUsingNOMSI(string progLocNOMSI, FileSystemInfo reportFile)
        {
            try
            {
                // Set up and execute a program runner to run NOMSI

                var arguments = reportFile.Name;

                if (mDebugLevel >= 1)
                {
                    LogDebug(progLocNOMSI + " " + arguments);
                }

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false
                };

                RegisterEvents(cmdRunner);

                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var success = cmdRunner.RunProgram(progLocNOMSI, arguments, "NOMSI", true);

                mProgress = PROGRESS_PCT_FINISHED_NOMSI;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("NOMSI plot creation complete");
                }

                if (success)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    LogWarning("NOMSI returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("NOMSI failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error creating plots with NOMSI";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        [Obsolete("Unused")]
        private CloseOutType CreateZipFileWithPlotsAndHTML(FileSystemInfo workDir, IReadOnlyCollection<FileInfo> pngFiles)
        {
            var currentTask = "Initializing";

            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Creating zip file with plots and index.html");
                }

                currentTask = "Calling CreatePlotViewHTML";

                var htmlSuccess = CreatePlotViewHTML(workDir, pngFiles);

                if (htmlSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return htmlSuccess;
                }

                var plotDirectory = new DirectoryInfo(Path.Combine(workDir.FullName, "Plots"));

                currentTask = "Moving PNG files into " + plotDirectory.FullName;

                if (!plotDirectory.Exists)
                    plotDirectory.Create();

                foreach (var pngFile in pngFiles)
                {
                    var newPath = Path.Combine(plotDirectory.FullName, pngFile.Name);

                    if (!string.Equals(pngFile.FullName, newPath))
                    {
                        pngFile.MoveTo(newPath);
                    }

                    // Do not call AddResultFileToSkip here since we keep the EC_count PNG file; see PostProcessResults
                }

                currentTask = "Moving index.html into " + plotDirectory.FullName;

                var htmlFile = new FileInfo(Path.Combine(workDir.FullName, INDEX_HTML));
                var newHtmlFile = new FileInfo(Path.Combine(plotDirectory.FullName, htmlFile.Name));

                if (newHtmlFile.Exists)
                    newHtmlFile.Delete();

                htmlFile.MoveTo(Path.Combine(plotDirectory.FullName, htmlFile.Name));
                mJobParams.AddResultFileToSkip(htmlFile.Name);

                var zipFilePath = Path.Combine(workDir.FullName, mDatasetName + "_Plots.zip");

                var zipSuccess = mZipTools.ZipDirectory(plotDirectory.FullName, zipFilePath);

                if (!zipSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error creating zip file with plots from NOMSI; current task: " + currentTask;
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private List<FileInfo> GetXmlSpectraFiles(DirectoryInfo workingDirectory, out string wildcardMatchSpec)
        {
            wildcardMatchSpec = mDatasetName + "_scan*.xml";
            return workingDirectory.GetFiles(wildcardMatchSpec).ToList();
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <remarks>Not used at present</remarks>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            ParseConsoleOutputFile(consoleOutputFilePath, out _, out _, out _);
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <remarks>Not used at present</remarks>
        /// <param name="consoleOutputFilePath"></param>
        /// <param name="fileCountNoPeaks">Output: will be non-zero if Formularity reports "no data points found" for a given file</param>
        /// <param name="nothingToAlign">Output: set to true if Formularity reports "Nothing to align" (meaning non of the input files had peaks)</param>
        /// <param name="calibrationFailed">Output: set to true if calibration failed</param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath, out int fileCountNoPeaks, out bool nothingToAlign, out bool calibrationFailed)
        {
            // Example Console output

            // Started.
            // Loaded parameters.
            // Reading database C:\DMS_Temp_Org\WHOI_CIA_DB_2016_11_21.bin
            // Loaded DB.
            // Loaded calibration.
            // Checked arguments.
            // Opening C:\DMS_WorkDir\DatasetName_peaks.txt
            //
            // Dataset: DatasetName_peaks.txt
            //
            // Pre-alignment:
            //
            // no
            //
            // Calibration: calibrant peaks total 76 matched 30(30 distinct)
            //
            // Formula Finding
            // Processed files:
            //
            // DatasetName_peaks.txt
            //
            // Parameters:
            //
            // <DefaultParameters><InputFilesTab><Adduct></Adduct><Ionization>proton_detachment</Ionization>...
            //
            // Finished.

            fileCountNoPeaks = 0;
            nothingToAlign = false;
            calibrationFailed = false;

            try
            {
                var reErrorMessage = new Regex("Error:(?<ErrorMessage>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    // Check for "Warning: no data points found in FileName"
                    if (dataLine.StartsWith("Warning", StringComparison.OrdinalIgnoreCase) && dataLine.IndexOf("no data points found", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        fileCountNoPeaks++;
                        continue;
                    }

                    //// Check for "Error: Nothing to align; aborting"
                    //if (dataLine.StartsWith("Error", StringComparison.OrdinalIgnoreCase) && dataLine.ToLower().Contains("nothing to align"))
                    //{
                    //    nothingToAlign = true;
                    //    mMessage = dataLine;
                    //    continue;
                    //}

                    // Check for Calibration failed; using uncalibrated masses"
                    if (dataLine.StartsWith("Calibration failed", StringComparison.OrdinalIgnoreCase))
                    {
                        calibrationFailed = true;
                        continue;
                    }

                    // Look for generic errors
                    var reMatch = reErrorMessage.Match(dataLine);

                    if (reMatch.Success)
                    {
                        // Store this error message, plus any remaining console output lines
                        mMessage = reMatch.Groups["ErrorMessage"].Value;
                        StoreConsoleErrorMessage(reader, dataLine);
                    }
                    else if (dataLine.StartsWith("error ", StringComparison.OrdinalIgnoreCase))
                    {
                        // Store this error message, plus any remaining console output lines
                        mMessage = dataLine;
                        StoreConsoleErrorMessage(reader, dataLine);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType PostProcessResults(string progLocNOMSI)
        {
            try
            {
                // The results files will be in a subdirectory with today's date
                // Example filename: DatasetName_peaksResult.csv

                // Search for the CSV file

                var workDir = new DirectoryInfo(mWorkDir);
                var filenameMatchSpec = mDatasetName + "*Result.csv";
                var csvFiles = workDir.GetFiles(filenameMatchSpec, SearchOption.AllDirectories);

                if (csvFiles.Length == 0)
                {
                    LogError("Formularity Result.csv file not found");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (csvFiles.Length > 1)
                {
                    LogError("Multiple Formularity Result.csv files were found matching " + filenameMatchSpec);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var reportFile = csvFiles.First();

                // Rename the report file to be DatasetName_Report.csv (and move it to the work directory)
                reportFile.MoveTo(Path.Combine(mWorkDir, mDatasetName + "_Report.csv"));

                // Ignore the Report*.log files
                // All messages in those files were displayed at the console and are thus already in Formularity_ConsoleOutput.txt

                foreach (var logFile in workDir.GetFiles("Report*.log", SearchOption.AllDirectories))
                {
                    mJobParams.AddResultFileToSkip(logFile.Name);
                }

                // Ignore log.csv files
                foreach (var logFile in workDir.GetFiles("log.csv", SearchOption.AllDirectories))
                {
                    mJobParams.AddResultFileToSkip(logFile.Name);
                }

                var resultCode = CreatePlotsUsingNOMSI(progLocNOMSI, reportFile);

                if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return resultCode;
                }

                // Open nomsi_summary.txt to look for "summary=success"
                var nomsiSummaryResult = ValidateNOMSISummaryFile(workDir);

                if (nomsiSummaryResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return resultCode;
                }

                // Rename the plot files, replacing suffix "_Report.PNG" with ".png"
                var renameResultCode = RenamePlotFiles(workDir, out var pngFiles);

                if (renameResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return renameResultCode;
                }

                // Uncomment the following to create an index.html file for viewing the PNG files on a web page
                // Will also create a .zip file with the index.html file and the PNG files
                //
                // // Create the index.html file then zip the PNG files and HTML file together
                // var zipResultCode = CreateZipFileWithPlotsAndHTML(workDir, pngFiles);
                // if (zipResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                // {
                //     return zipResultCode;
                // }

                // Create a PDF using the PNG plots
                // Note that the PNG files will now be in the plots subdirectory, but pngFiles should be up-to-date
                var pdfResultCode = CreatePDFFromPlots(workDir, pngFiles);

                // Call AddResultFileToSkip for each of the PNG files (to prevent them from being copied to the transfer directory)
                // However, keep the EC_count PNG file; we want it visible from the DMS website
                foreach (var pngFile in pngFiles)
                {
                    if (pngFile.Name.StartsWith("EC_count_"))
                    {
                        var newFilePath = Path.Combine(workDir.FullName, pngFile.Name);

                        if (!string.Equals(pngFile.FullName, newFilePath))
                        {
                            // The file is in the plots subdirectory; move it back to the working directory
                            pngFile.MoveTo(Path.Combine(workDir.FullName, pngFile.Name));
                        }
                    }
                    else
                    {
                        mJobParams.AddResultFileToSkip(pngFile.Name);
                    }
                }

                if (pdfResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return pdfResultCode;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error post-processing results";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Rename the plot files, replacing suffix "_Report.PNG" with ".png"
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="pngFiles"></param>
        private CloseOutType RenamePlotFiles(DirectoryInfo workDir, out List<FileInfo> pngFiles)
        {
            const int MINIMUM_PNG_FILE_COUNT = 11;
            const string REPORT_PNG_FILE_SUFFIX = "_Report.PNG";

            pngFiles = new List<FileInfo>();

            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Renaming PNG plot files created by NOMSI");
                }

                // Confirm that multiple .png files were created
                var sourcePngFiles = workDir.GetFiles("*" + REPORT_PNG_FILE_SUFFIX).ToList();

                if (sourcePngFiles.Count == 0)
                {
                    // NOMSI did not create any PNG files (with suffix _Report.PNG
                    LogError(string.Format("NOMSI did not create any PNG files (with suffix {0})", REPORT_PNG_FILE_SUFFIX));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (sourcePngFiles.Count < MINIMUM_PNG_FILE_COUNT)
                {
                    LogError("NOMSI created {0} PNG files, but it should have created {1} files", sourcePngFiles.Count, MINIMUM_PNG_FILE_COUNT);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                foreach (var pngFile in sourcePngFiles)
                {
                    if (pngFile.Name.EndsWith(REPORT_PNG_FILE_SUFFIX, StringComparison.OrdinalIgnoreCase))
                    {
                        var newPath = pngFile.FullName.Substring(0, pngFile.FullName.Length - REPORT_PNG_FILE_SUFFIX.Length) + ".png";
                        pngFile.MoveTo(newPath);
                        pngFiles.Add(pngFile);
                    }
                    else
                    {
                        LogWarning("PNG file did not end with {0}; this is unexpected: {1}", REPORT_PNG_FILE_SUFFIX, pngFile.Name);
                        pngFiles.Add(pngFile);
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error renaming PNG plot files";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartFormularity(
            string progLoc,
            string inputFilePathOrWildcardMatchSpec,
            string paramFilePath,
            string ciaDbPath,
            string calibrationPeaksFilePath,
            out int fileCountNoPeaks,
            out bool nothingToAlign)
        {
            // Set up and execute a program runner to run Formularity

            var arguments = " CIA " +
                            PossiblyQuotePath(inputFilePathOrWildcardMatchSpec) + " " +
                            PossiblyQuotePath(paramFilePath) + " " +
                            PossiblyQuotePath(ciaDbPath);

            if (!string.IsNullOrWhiteSpace(calibrationPeaksFilePath))
            {
                arguments += " " + PossiblyQuotePath(calibrationPeaksFilePath);
            }

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            mConsoleOutputFile = Path.Combine(mWorkDir, FORMULARITY_CONSOLE_OUTPUT_FILE);

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = mConsoleOutputFile
            };

            RegisterEvents(cmdRunner);

            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            var success = cmdRunner.RunProgram(progLoc, arguments, "Formularity", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(cmdRunner.CachedConsoleOutput);
            }

            // Parse the console output file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile, out fileCountNoPeaks, out nothingToAlign, out var calibrationFailed);

            if (calibrationFailed)
            {
                mMessage = "Calibration failed; used uncalibrated masses";
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("Formularity returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("Formularity failed (but exit code is 0)");
            }

            return false;
        }

        private bool ProcessScansWithFormularity(string progLoc, string datasetScansFilePath, out bool nothingToAlign)
        {
            nothingToAlign = false;

            try
            {
                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Processing data using Formularity");

                var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE));

                if (!File.Exists(paramFilePath))
                {
                    LogError("Parameter file not found", "Parameter file not found: " + paramFilePath);
                    return false;
                }

                var orgDbDirectory = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
                var ciaDbPath = Path.Combine(orgDbDirectory, mJobParams.GetParam("cia_db_name"));

                if (!File.Exists(ciaDbPath))
                {
                    LogError("CIA database not found", "CIA database not found: " + ciaDbPath);
                    return false;
                }

                var calibrationPeaksFileName = mJobParams.GetJobParameter(
                    AnalysisJob.STEP_PARAMETERS_SECTION,
                    AnalysisResourcesFormularity.JOB_PARAM_FORMULARITY_CALIBRATION_PEAKS_FILE,
                    string.Empty);

                string calibrationPeaksFilePath;

                if (string.IsNullOrWhiteSpace(calibrationPeaksFileName))
                {
                    calibrationPeaksFilePath = string.Empty;
                }
                else
                {
                    calibrationPeaksFilePath = Path.Combine(mWorkDir, calibrationPeaksFileName);
                }

                if (!File.Exists(calibrationPeaksFilePath))
                {
                    LogError("Calibration file not found", "Calibration file not found: " + calibrationPeaksFilePath);
                    return false;
                }

                bool success;

                int scanCount;
                int scanCountNoPeaks;

                if (string.IsNullOrWhiteSpace(datasetScansFilePath))
                {
                    // Processing the .xml scans files in the working directory

                    var workingDirectory = new DirectoryInfo(mWorkDir);
                    var spectraFiles = GetXmlSpectraFiles(workingDirectory, out var wildcardMatchSpec);
                    scanCount = spectraFiles.Count;

                    if (scanCount == 0)
                    {
                        mMessage = "XML spectrum files not found matching " + wildcardMatchSpec;
                        return false;
                    }

                    foreach (var spectrumFile in spectraFiles)
                    {
                        mJobParams.AddResultFileToSkip(spectrumFile.Name);
                    }

                    mProgress = PROGRESS_PCT_STARTING_FORMULARITY;

                    success = StartFormularity(progLoc, wildcardMatchSpec, paramFilePath, ciaDbPath, calibrationPeaksFilePath,
                                               out scanCountNoPeaks, out nothingToAlign);
                }
                else
                {
                    // Either processing a ThermoPeakDataExporter .tsv file or a DeconTools _peaks.txt file
                    // Call Formularity for the file

                    scanCount = 1;

                    mJobParams.AddResultFileToSkip(datasetScansFilePath);
                    mProgress = PROGRESS_PCT_STARTING_FORMULARITY;

                    success = StartFormularity(progLoc, datasetScansFilePath, paramFilePath, ciaDbPath, calibrationPeaksFilePath,
                                               out scanCountNoPeaks, out nothingToAlign);
                }

                if (!success)
                    return false;

                mProgress = PROGRESS_PCT_FINISHED_FORMULARITY;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Formularity processing complete");
                }

                if (scanCountNoPeaks <= 0 && !nothingToAlign)
                {
                    return true;
                }

                if (nothingToAlign || scanCountNoPeaks >= scanCount)
                {
                    // None of the scans had peaks
                    mMessage = "No peaks found";

                    if (scanCount > 1)
                        mEvalMessage = "None of the scans had peaks";
                    else
                        mEvalMessage = "Scan did not have peaks";

                    if (!nothingToAlign)
                        nothingToAlign = true;

                    // Do not put the parameter file in the results directory
                    mJobParams.AddResultFileToSkip(paramFilePath);
                }
                else
                {
                    // Some of the scans had no peaks
                    mEvalMessage = scanCountNoPeaks + " / " + scanCount + " scans had no peaks";
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Processing data using Formularity";
                LogError(mMessage, ex);
                return false;
            }
        }

        private void StoreConsoleErrorMessage(StreamReader reader, string dataLine)
        {
            mConsoleOutputErrorMsg = "Error running Formularity: " + dataLine;

            while (!reader.EndOfStream)
            {
                // Store the remaining console output lines
                dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    mConsoleOutputErrorMsg += "; " + dataLine;
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string> {
                "ArrayMath.dll",
                "FindChains.exe",
                // ReSharper disable once StringLiteralTypo
                "TestFSDBSearch.exe"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);

            return success;
        }

        private CloseOutType ValidateNOMSISummaryFile(FileSystemInfo workDir)
        {
            const string NOMSI_SUMMARY_FILE_NAME = "nomsi_summary.txt";

            const string NOMSI_SUCCESS_MESSAGE = "summary=success";

            try
            {
                var nomsiSummaryFile = new FileInfo(Path.Combine(workDir.FullName, NOMSI_SUMMARY_FILE_NAME));

                if (!nomsiSummaryFile.Exists)
                {
                    LogError("NOMSI summary file not found: " + nomsiSummaryFile.Name);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                using (var reader = new StreamReader(new FileStream(nomsiSummaryFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.StartsWith(NOMSI_SUCCESS_MESSAGE))
                        {
                            return CloseOutType.CLOSEOUT_SUCCESS;
                        }
                    }
                }

                LogError("NOMSI summary file did not contain: " + NOMSI_SUCCESS_MESSAGE);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = string.Format("Error validating {0}", NOMSI_SUMMARY_FILE_NAME);
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void CmdRunner_LoopWaiting()
        {
            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, mConsoleOutputFile));

                    LogProgress("Formularity");
                }
            }
        }
    }
}
