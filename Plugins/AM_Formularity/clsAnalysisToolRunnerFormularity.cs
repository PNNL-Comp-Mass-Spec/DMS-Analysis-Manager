using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerFormularityPlugin
{
    /// <summary>
    /// Class for running Formularity
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerFormularity : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const float PROGRESS_PCT_STARTING_FORMULARITY = 5;
        private const float PROGRESS_PCT_FINISHED_FORMULARITY = 95;
        private const float PROGRESS_PCT_FINISHED_NOMSI = 97;

        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string FORMULARITY_CONSOLE_OUTPUT_FILE = "Formularity_ConsoleOutput.txt";

        private const string INDEX_HTML = "index.html";

        #endregion

        #region "Module Variables"

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        #endregion

        #region "Methods"

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

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerFormularity.RunTool(): Enter");
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
                    m_message = "Error determining Formularity version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip the XML files
                var compressedXMLFiles = Path.Combine(m_WorkDir, m_Dataset + "_scans.zip");
                var unzipSuccess = UnzipFile(compressedXMLFiles, m_WorkDir);
                if (!unzipSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error extracting the XML spectra files";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using Formularity
                var processingSuccess = ProcessScansWithFormularity(progLoc, out var nothingToAlign);

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
                    eReturnCode = PostProcessResults(progLocNOMSI, ref processingSuccess);
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                var success = CopyResultsToTransferDirectory();

                return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in FormularityPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                foreach (var xmlFile in GetXmlSpectraFiles(diWorkDir, out _))
                    xmlFile.Delete();

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveFolder();
        }


        private CloseOutType CreatePlotViewHTML(FileSystemInfo workDir, List<FileInfo> pngFiles)
        {

            const string LITERAL_TEXT_FLAG = "text: ";

            try
            {
                var htmlFilePath = Path.Combine(workDir.FullName, INDEX_HTML);


                var datasetDetailReportText =
                    string.Format("DMS <a href='http://dms2.pnl.gov/dataset/show/{0}'> Dataset Detail Report </a>", m_Dataset);

                var datasetDetailReportLink = LITERAL_TEXT_FLAG + datasetDetailReportText;
                var pngFileTableLayout = PngToPdfConverter.GetPngFileTableLayout(m_Dataset, datasetDetailReportLink);

                var pngFileNames = new SortedSet<string>();
                foreach (var item in pngFiles)
                {
                    pngFileNames.Add(item.Name);
                }

                using (var writer = new StreamWriter(new FileStream(htmlFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 3.2//EN\">");
                    writer.WriteLine("<html>");
                    writer.WriteLine("<head>");
                    writer.WriteLine("  <title>{0}</title>", m_Dataset);
                    writer.WriteLine("</head>");
                    writer.WriteLine();
                    writer.WriteLine("<body>");
                    writer.WriteLine("  <h2>{0}</h2>", m_Dataset);
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
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Error creating the HTML linking to the plots from NOMSI";
                LogError(m_message, ex);
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
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Creating PDF file with plots from NOMSI");
                }

                var converter = new PngToPdfConverter(m_Dataset);
                RegisterEvents(converter);

                var outputFilePath = Path.Combine(workDir.FullName, m_Dataset + "_NOMSI_plots.pdf");

                var success = converter.CreatePdf(outputFilePath, pngFiles);

                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error creating the PDF file with NOMSI plots";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Error creating a PDF file with the plots from NOMSI";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private CloseOutType CreatePlotsUsingNOMSI(string progLocNOMSI, FileSystemInfo reportFile)
        {
            try
            {

                // Set up and execute a program runner to run NOMSI

                var cmdStr = reportFile.Name;

                if (m_DebugLevel >= 1)
                {
                    LogDebug(progLocNOMSI + " " + cmdStr);
                }

                var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false
                };
                RegisterEvents(cmdRunner);

                cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

                var success = cmdRunner.RunProgram(progLocNOMSI, cmdStr, "NOMSI", true);

                m_progress = PROGRESS_PCT_FINISHED_NOMSI;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
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
                m_message = "Error creating plots with NOMSI";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        [Obsolete("Unused")]
        private CloseOutType CreateZipFileWithPlotsAndHTML(FileSystemInfo workDir, List<FileInfo> pngFiles)
        {
            var currentTask = "Initializing";

            try
            {
                if (m_DebugLevel >= 3)
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
                m_jobParams.AddResultFileToSkip(htmlFile.Name);

                var zipFilePath = Path.Combine(workDir.FullName, m_Dataset + "_Plots.zip");

                var zipSuccess = m_DotNetZipTools.ZipDirectory(plotDirectory.FullName, zipFilePath);
                if (!zipSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                m_message = "Error creating zip file with plots from NOMSI; current task: " + currentTask;
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private List<FileInfo> GetXmlSpectraFiles(DirectoryInfo diWorkDir, out string wildcardMatchSpec)
        {
            wildcardMatchSpec = m_Dataset + "_scan*.xml";
            var fiSpectraFiles = diWorkDir.GetFiles(wildcardMatchSpec).ToList();
            return fiSpectraFiles;
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            ParseConsoleOutputFile(consoleOutputFilePath, out _, out _, out _);
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <param name="fileCountNoPeaks">Output: will be non-zero if Formularity reports "no data points found" for a given file</param>
        /// <param name="nothingToAlign">Output: set to true if Formularity reports "Nothing to align" (meaning non of the input files had peaks)</param>
        /// <param name="calibrationFailed">Output: set to true if calibration failed</param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath, out int fileCountNoPeaks, out bool nothingToAlign, out bool calibrationFailed)
        {
            // Example Console output
            //
            // Started.
            // Checked arguments.
            // Loaded parameters.
            // Reading database ..\..\..\..\Data\CIA_DB\PNNL_CIA_DB_1500_B.bin
            // Sorting 28,487,622 DB entries
            // Skipping check for duplicate formulas; database was previously validated
            // Loaded DB.
            // Opening F:\Formularity\Data\TestDataXML\Marco_AL1_Bot_23May18_p05_000001_scan1.xml
            // Opening F:\Formularity\Data\TestDataXML\Marco_AL3_Bot_23May18_p05_000001_scan1.xml
            // Aligning
            // Formula Finding
            // Writing results to F:\Formularity\Data\TestDataXML\Report.csv
            // Finished.

            fileCountNoPeaks = 0;
            nothingToAlign = false;
            calibrationFailed = false;

            try
            {

                var reErrorMessage = new Regex(@"Error:(?<ErrorMessage>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        // Check for "Warning: no data points found in FileName"
                        if (dataLine.StartsWith("Warning", StringComparison.OrdinalIgnoreCase) && dataLine.ToLower().Contains("no data points found"))
                        {
                            fileCountNoPeaks++;
                            continue;
                        }

                        // Check for "Error: Nothing to align; aborting"
                        if (dataLine.StartsWith("Error", StringComparison.OrdinalIgnoreCase) && dataLine.ToLower().Contains("nothing to align"))
                        {
                            nothingToAlign = true;
                            m_message = dataLine;
                            continue;
                        }

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
                            m_message = reMatch.Groups["ErrorMessage"].Value;
                            StoreConsoleErrorMessage(reader, dataLine);
                        }
                        else if (dataLine.ToLower().StartsWith("error "))
                        {
                            // Store this error message, plus any remaining console output lines
                            m_message = dataLine;
                            StoreConsoleErrorMessage(reader, dataLine);
                        }

                    }
                }


            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        private CloseOutType PostProcessResults(string progLocNOMSI, ref bool processingSuccess)
        {

            try
            {
                var reportFile = new FileInfo(Path.Combine(m_WorkDir, "Report.csv"));

                if (!reportFile.Exists)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = string.Format("Formularity results not found ({0})", reportFile.Name);
                        processingSuccess = false;
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    // Rename the report file to start with the dataset name
                    reportFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset + "_Report.csv"));
                }

                // Ignore the Report*.log files
                // All messages in those files were displayed at the console and are thus already in Formularity_ConsoleOutput.txt
                var workDir = new DirectoryInfo(m_WorkDir);

                foreach (var logFile in workDir.GetFiles("Report*.log"))
                {
                    m_jobParams.AddResultFileToSkip(logFile.Name);
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
                if (pdfResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return pdfResultCode;
                }

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
                        m_jobParams.AddResultFileToSkip(pngFile.Name);
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Error post processing results";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Rename the plot files, replacing suffix "_Report.PNG" with ".png"
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="pngFiles"></param>
        /// <returns></returns>
        private CloseOutType RenamePlotFiles(DirectoryInfo workDir, out List<FileInfo> pngFiles)
        {
            const int MINIMUM_PNG_FILE_COUNT = 11;
            const string REPORT_PNG_FILE_SUFFIX = "_Report.PNG";

            pngFiles = new List<FileInfo>();

            try
            {
                if (m_DebugLevel >= 3)
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
                    LogError(string.Format("NOMSI created {0} PNG files, but it should have created {1} files",
                                           sourcePngFiles.Count, MINIMUM_PNG_FILE_COUNT));
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
                        LogWarning(string.Format("PNG file did not end with {0}; this is unexpected: {1}",
                                                 REPORT_PNG_FILE_SUFFIX, pngFile.Name));
                        pngFiles.Add(pngFile);
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Error renaming PNG plot files";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartFormularity(
            string progLoc,
            string wildcardMatchSpec,
            string paramFilePath,
            string ciaDbPath,
            string calibrationPeaksFilePath,
            out int fileCountNoPeaks,
            out bool nothingToAlign)
        {

            // Set up and execute a program runner to run Formularity

            var cmdStr = " cia " +
                         PossiblyQuotePath(wildcardMatchSpec) + " " +
                         PossiblyQuotePath(paramFilePath) + " " +
                         PossiblyQuotePath(ciaDbPath);

            if (!string.IsNullOrWhiteSpace(calibrationPeaksFilePath))
            {
                cmdStr += " " + PossiblyQuotePath(calibrationPeaksFilePath);
            }

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            mConsoleOutputFile = Path.Combine(m_WorkDir, FORMULARITY_CONSOLE_OUTPUT_FILE);

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = mConsoleOutputFile
            };
            RegisterEvents(cmdRunner);

            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            var success = cmdRunner.RunProgram(progLoc, cmdStr, "Formularity", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(cmdRunner.CachedConsoleOutput);
                }

            }

            // Parse the console output file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile, out fileCountNoPeaks, out nothingToAlign, out var calibrationFailed);

            if (calibrationFailed)
            {
                m_message = "Calibration failed; used uncalibrated masses";
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

        private bool ProcessScansWithFormularity(string progLoc, out bool nothingToAlign)
        {

            nothingToAlign = false;

            try
            {

                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Processing data using Formularity");

                var paramFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));

                if (!File.Exists(paramFilePath))
                {
                    LogError("Parameter file not found", "Parameter file not found: " + paramFilePath);
                    return false;
                }

                var orgDbDirectory = m_mgrParams.GetParam(clsAnalysisResources.MGR_PARAM_ORG_DB_DIR);
                var ciaDbPath = Path.Combine(orgDbDirectory, m_jobParams.GetParam("cia_db_name"));

                if (!File.Exists(ciaDbPath))
                {
                    LogError("CIA database not found", "CIA database not found: " + ciaDbPath);
                    return false;
                }

                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var spectraFiles = GetXmlSpectraFiles(diWorkDir, out var wildcardMatchSpec);

                if (spectraFiles.Count == 0)
                {
                    m_message = "XML spectrum files not found matching " + wildcardMatchSpec;
                    return false;
                }

                foreach (var spectrumFile in spectraFiles)
                {
                    m_jobParams.AddResultFileToSkip(spectrumFile.Name);
                }

                var calibrationPeaksFileName = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "CalibrationPeaksFile", string.Empty);
                string calibrationPeaksFilePath;
                if (string.IsNullOrWhiteSpace(calibrationPeaksFileName))
                {
                    calibrationPeaksFilePath = string.Empty;
                }
                else
                {
                    calibrationPeaksFilePath = Path.Combine(m_WorkDir, calibrationPeaksFileName);
                }

                m_progress = PROGRESS_PCT_STARTING_FORMULARITY;

                var success = StartFormularity(progLoc, wildcardMatchSpec, paramFilePath, ciaDbPath, calibrationPeaksFilePath,
                                               out var fileCountNoPeaks, out nothingToAlign);

                if (!success)
                    return false;

                m_progress = PROGRESS_PCT_FINISHED_FORMULARITY;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Formularity processing complete");
                }

                if (fileCountNoPeaks <= 0 && nothingToAlign == false)
                {
                    return true;
                }

                if (nothingToAlign || fileCountNoPeaks >= spectraFiles.Count)
                {
                    // None of the scans had peaks
                    m_message = "No peaks found";
                    if (spectraFiles.Count > 1)
                        m_EvalMessage = "None of the scans had peaks";
                    else
                        m_EvalMessage = "Scan did not have peaks";

                    if (!nothingToAlign)
                        nothingToAlign = true;

                    // Do not put the parameter file in the results directory
                    m_jobParams.AddResultFileToSkip(paramFilePath);
                }
                else
                {
                    // Some of the scans had no peaks
                    m_EvalMessage = fileCountNoPeaks + " / " + spectraFiles.Count + " scans had no peaks";
                }

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Processing data using Formularity";
                LogError(m_message, ex);
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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string> {
                "ArrayMath.dll",
                "FindChains.exe",
                // ReSharper disable once StringLiteralTypo
                "TestFSDBSearch.exe"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

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
                m_message = string.Format("Error validating {0}", NOMSI_SUMMARY_FILE_NAME);
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mConsoleOutputFile));

                    LogProgress("Formularity");
                }

            }

        }

        #endregion
    }
}
