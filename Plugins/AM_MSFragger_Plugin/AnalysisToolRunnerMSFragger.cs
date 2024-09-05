//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISM.AppSettings;

namespace AnalysisManagerMSFraggerPlugIn
{
    /// <summary>
    /// Class for running MSFragger
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSFragger : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Batmass, centroided, cp, Da, deisotoping, DIA, fragpipe, fragger, java,
        // Ignore Spelling: na, num, pepindex, postprocessing, timsdata, Xmx
        // Ignore Spelling: \batmass-io, \fragpipe, \tools

        // ReSharper restore CommentTypo

        private const string MSFRAGGER_CONSOLE_OUTPUT = "MSFragger_ConsoleOutput.txt";

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string PIN_EXTENSION = ".pin";

        public const float PROGRESS_PCT_INITIALIZING = 1;

        private enum ProgressPercentValues
        {
            Initializing = 0,
            VerifyingMzMLFiles = 1,
            StartingMSFragger = 2,
            MSFraggerComplete = 90,
            ProcessingComplete = 99
        }

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mMSFraggerVersion;

        private string mMSFraggerProgLoc;

        private string mConsoleOutputErrorMsg;

        private int mDatasetCount;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private bool mWarnedInvalidDatasetCount;

        private RunDosProgram mCmdRunner;

        private static ZipFileTools mZipTool;

        /// <summary>
        /// Runs MSFragger tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                mProgress = (int)ProgressPercentValues.Initializing;

                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerMSFragger.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to MSFragger

                // ReSharper disable once CommentTypo
                // Construct the relative path to the .jar file, for example:
                // fragpipe\tools\MSFragger-4.1\MSFragger-4.1.jar

                var jarFileRelativePath = Path.Combine(FragPipeLibFinder.MSFRAGGER_JAR_DIRECTORY_RELATIVE_PATH, FragPipeLibFinder.MSFRAGGER_JAR_NAME);

                mMSFraggerProgLoc = DetermineProgramLocation("MSFraggerProgLoc", jarFileRelativePath);

                if (string.IsNullOrWhiteSpace(mMSFraggerProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MSFragger version info in the database after the first line is written to file MSFragger_ConsoleOutput.txt
                mToolVersionWritten = false;
                mMSFraggerVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the mzML files using MSFragger
                var processingResult = StartMSFragger(fastaFile);

                mProgress = (int)ProgressPercentValues.ProcessingComplete;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                AppUtils.GarbageCollectNow();

                if (!AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // ToDo: Confirm that it is safe to skip file Dataset_uncalibrated.mgf
                mJobParams.AddResultFileExtensionToSkip("_uncalibrated.mgf");

                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                LogError("Error in MSFraggerPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        public static List<FileInfo> FindDatasetPinFileAndPepXmlFiles(
            DirectoryInfo workingDirectory,
            bool diaSearchEnabled,
            string datasetName,
            out FileInfo pinFile)
        {
            var pepXmlFiles = new List<FileInfo>();

            pinFile = new FileInfo(Path.Combine(workingDirectory.FullName, datasetName + ".pin"));

            if (diaSearchEnabled)
            {
                // Look for files matching DatasetName_rank*.pepXML
                // For example:
                //   QC_Dataset_rank1.pepXML
                //   QC_Dataset_rank2.pepXML

                var searchPattern = string.Format("{0}_rank*{1}", datasetName, PEPXML_EXTENSION);

                pepXmlFiles.AddRange(workingDirectory.GetFiles(searchPattern));

                if (pepXmlFiles.Count > 0)
                    return pepXmlFiles;

                return new List<FileInfo>();
            }

            pepXmlFiles.Add(new FileInfo(Path.Combine(workingDirectory.FullName, datasetName + PEPXML_EXTENSION)));

            if (pepXmlFiles[0].Exists)
                return pepXmlFiles;

            return new List<FileInfo>();
        }

        /// <summary>
        /// Given a linked list of progress values (which should have populated in ascending order), find the next progress value
        /// </summary>
        /// <param name="progressValues"></param>
        /// <param name="currentProgress"></param>
        /// <returns>Next progress value, or 100 if either the current value is not found, or the next value is not defined</returns>
        private static int GetNextProgressValue(LinkedList<int> progressValues, int currentProgress)
        {
            var currentNode = progressValues.Find(currentProgress);

            if (currentNode?.Next == null)
                return 100;

            return currentNode.Next.Value;
        }

        /// <summary>
        /// Determine the number of threads to use for MSFragger
        /// </summary>
        private int GetNumThreadsToUse()
        {
            var coreCount = Global.GetCoreCount();

            if (coreCount > 4)
            {
                return coreCount - 1;
            }

            return coreCount;
        }

        private string GetComment(KeyValueParamFileLine setting, string defaultComment)
        {
            return string.IsNullOrWhiteSpace(setting.Comment)
                ? defaultComment
                : setting.Comment;
        }

        private static Regex GetRegEx(string matchPattern, bool ignoreCase = true)
        {
            var options = ignoreCase ? RegexOptions.Compiled | RegexOptions.IgnoreCase : RegexOptions.Compiled;
            return new Regex(matchPattern, options);
        }

        private bool MzMLFilesAreCentroided(string javaProgLoc, DataPackageInfo dataPackageInfo, FragPipeLibFinder libraryFinder)
        {
            try
            {
                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false
                };
                RegisterEvents(cmdRunner);

                LogMessage("Verifying that the .mzML {0} centroided", dataPackageInfo.DatasetFiles.Count == 1 ? "file is" : "files are");

                // Set up and execute a program runner to run CheckCentroid

                // Find the fragpipe jar file
                if (!libraryFinder.FindJarFileFragPipe(out var jarFileFragPipe))
                    return false;

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Find the Batmass-IO jar file
                if (!libraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // ReSharper restore CommentTypo
                // ReSharper restore IdentifierTypo

                const int threadCount = 4;

                // Examine each .mzML file

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var item in dataPackageInfo.DatasetFiles)
                {
                    var mzMLFile = new FileInfo(Path.Combine(mWorkDir, item.Value));

                    if (!mzMLFile.Exists)
                    {
                        LogError(".mzML file not found: " + mzMLFile.FullName);
                        return false;
                    }

                    // ReSharper disable CommentTypo

                    // Run CheckCentroid, example command line:
                    // java -Xmx4G -cp "C:\DMS_Programs\MSFragger\fragpipe\lib\fragpipe-20.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar" com.dmtavt.fragpipe.util.CheckCentroid DatasetName.mzML 4

                    // ReSharper disable once StringLiteralTypo
                    var arguments = string.Format(
                        "-Xmx4G -cp \"{0};{1}\" com.dmtavt.fragpipe.util.CheckCentroid {2} {3}",
                        jarFileFragPipe.FullName,
                        jarFileBatmassIO.FullName,
                        mzMLFile.FullName,
                        threadCount);

                    // ReSharper restore CommentTypo

                    LogDebug(javaProgLoc + " " + arguments);

                    // Start the program and wait for it to finish
                    var processingSuccess = cmdRunner.RunProgram(javaProgLoc, arguments, "CheckCentroid", true);

                    if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        LogError(mConsoleOutputErrorMsg);
                        return false;
                    }

                    if (!processingSuccess)
                    {
                        LogError("Error running CheckCentroid");

                        if (cmdRunner.ExitCode != 0)
                        {
                            LogWarning("CheckCentroid returned a non-zero exit code: " + cmdRunner.ExitCode);
                        }
                        else
                        {
                            LogWarning("Call to CheckCentroid failed (but exit code is 0)");
                        }

                        return false;
                    }

                    if (cmdRunner.CachedConsoleErrors.Contains("has non-centroid scans"))
                    {
                        LogError("CheckCentroid found non-centroided MS2 spectra; MSFragger requires centroided scans");
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MzMLFilesAreCentroided", ex);
                return false;
            }
        }

        /// <summary>
        /// Parse the MSFragger console output file to determine the MSFragger version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseMSFraggerConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable once IdentifierTypo
            // ReSharper disable once StringLiteralTypo
            const string BATMASS_IO_VERSION = "Batmass-IO version";

            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            // MSFragger version MSFragger-3.3
            // Batmass-IO version 1.23.4
            // timsdata library version timsdata-2-8-7-1
            // (c) University of Michigan
            // RawFileReader reading tool. Copyright (c) 2016 by Thermo Fisher Scientific, Inc. All rights reserved.
            // System OS: Windows 10, Architecture: AMD64
            // Java Info: 1.8.0_232, OpenJDK 64-Bit Server VM,
            // JVM started with 8 GB memory
            // Checking database...
            // Checking spectral files...
            // C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML: Scans = 9698
            // ***********************************FIRST SEARCH************************************
            // Parameters:
            // ...
            // Number of unique peptides
            //         of length 7: 28622
            //         of length 8: 27618
            //         of length 9: 25972
            // ...
            //         of length 50: 3193
            // In total 590010 peptides.
            // Generated 1061638 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 0
            // Selected fragment index width 0.10 Da.
            // 50272922 fragments to be searched in 1 slices (0.75 GB total)
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.38 s
            //         001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML 1.2 s | deisotoping 0.6 s
            //                 [progress: 9451/9451 (100%) - 14191 spectra/s] 0.7s | postprocessing 0.1 s
            // ***************************FIRST SEARCH DONE IN 0.153 MIN**************************
            //
            // *********************MASS CALIBRATION AND PARAMETER OPTIMIZATION*******************
            // ...
            // ************MASS CALIBRATION AND PARAMETER OPTIMIZATION DONE IN 0.523 MIN*********
            //
            // ************************************MAIN SEARCH************************************
            // Checking database...
            // Parameters:
            // ...
            // Number of unique peptides
            //         of length 7: 29253
            //         of length 8: 28510
            // ...
            //         of length 50: 6832
            // In total 778855 peptides.
            // Generated 1469409 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 0
            // Selected fragment index width 0.10 Da.
            // 76707996 fragments to be searched in 1 slices (1.14 GB total)
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.38 s
            //         001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzBIN_calibrated 0.1 s
            //                 [progress: 9380/9380 (100%) - 15178 spectra/s] 0.6s | postprocessing 1.5 s
            // ***************************MAIN SEARCH DONE IN 0.085 MIN***************************
            //
            // *******************************TOTAL TIME 0.761 MIN********************************

            // ----------------------------------------------------
            // Output when multiple datasets:
            // ----------------------------------------------------
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.69 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 1.1 s | deisotoping 0.7 s
            //                 [progress: 8271/8271 (100%) - 23631 spectra/s] 0.4s | postprocessing 0.0 s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzML 1.2 s | deisotoping 0.3 s
            //                 [progress: 20186/20186 (100%) - 19317 spectra/s] 1.0s | postprocessing 0.1 s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzML 1.2 s | deisotoping 0.1 s
            //                 [progress: 18994/18994 (100%) - 20336 spectra/s] 0.9s | postprocessing 0.1 s

            // ----------------------------------------------------
            // Output when multiple slices (and multiple datasets)
            // ----------------------------------------------------
            // Selected fragment index width 0.02 Da.
            // 649333606 fragments to be searched in 2 slices (9.68 GB total)
            // Operating on slice 1 of 2:
            //         Fragment index slice generated in 7.69 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 0.6 s | deisotoping 0.0 s
            //                 [progress: 8271/8271 (100%) - 38115 spectra/s] 0.2s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 19979/19979 (100%) - 13518 spectra/s] 1.5s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 18812/18812 (100%) - 13563 spectra/s] 1.4s
            // Operating on slice 2 of 2:
            //         Fragment index slice generated in 9.02 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 0.6 s | deisotoping 0.0 s
            //                 [progress: 8271/8271 (100%) - 37767 spectra/s] 0.2s | postprocessing 1.0 s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 19979/19979 (100%) - 30690 spectra/s] 0.7s | postprocessing 2.4 s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzBIN_calibrated 0.2 s
            //                 [progress: 18812/18812 (100%) - 29348 spectra/s] 0.6s | postprocessing 1.7 s

            // ----------------------------------------------------
            // Output when running a split FASTA search
            // ----------------------------------------------------
            // STARTED: slice 1 of 8
            // ...
            // DONE: slice 1 of 8
            // STARTED: slice 2 of 8
            // ...
            // DONE: slice 8 of 8

            // ReSharper restore CommentTypo

            const int FIRST_SEARCH_START = (int)ProgressPercentValues.StartingMSFragger + 1;
            const int FIRST_SEARCH_DONE = 44;

            const int MAIN_SEARCH_START = 50;
            const int MAIN_SEARCH_DONE = (int)ProgressPercentValues.MSFraggerComplete;

            var processingSteps = new SortedList<int, Regex>
            {
                { (int)ProgressPercentValues.StartingMSFragger, GetRegEx("^JVM started") },
                { FIRST_SEARCH_START                          , GetRegEx(@"^\*+FIRST SEARCH\*+") },
                { FIRST_SEARCH_DONE                           , GetRegEx(@"^\*+FIRST SEARCH DONE") },
                { FIRST_SEARCH_DONE + 1                       , GetRegEx(@"^\*+MASS CALIBRATION AND PARAMETER OPTIMIZATION\*+") },
                { MAIN_SEARCH_START                           , GetRegEx(@"^\*+MAIN SEARCH\*+") },
                { MAIN_SEARCH_DONE                            , GetRegEx(@"^\*+MAIN SEARCH DONE") }
            };

            var slabProgressRanges = new Dictionary<int, int>
            {
                {FIRST_SEARCH_START, FIRST_SEARCH_DONE},  // First Search Progress Range
                {MAIN_SEARCH_START, MAIN_SEARCH_DONE}     // Main Search Progress Range
            };

            // Use a linked list to keep track of the progress values
            // This makes lookup of the next progress value easier
            var progressValues = new LinkedList<int>();

            foreach (var item in (from progressValue in processingSteps.Keys orderby progressValue select progressValue))
            {
                progressValues.AddLast(item);
            }
            progressValues.AddLast(100);

            // RegEx to match lines like:
            //  001. Sample_Bane_06May21_20-11-16.mzML 1.0 s | deisotoping 0.6 s
            //	001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzBIN_calibrated 0.1 s
            var datasetMatcher = new Regex(@"^[\t ]+(?<DatasetNumber>\d+)\. .+\.(mzML|mzBIN)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // Operating on slice 1 of 2: 4463ms
            var sliceMatcher = new Regex(@"Operating on slice (?<Current>\d+) of (?<Total>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // DatasetName.mzML 7042ms [progress: 29940/50420 (59.38%) - 5945.19 spectra/s]
            var progressMatcher = new Regex(@"progress: \d+/\d+ \((?<PercentComplete>[0-9.]+)%\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var splitFastaMatcher = new Regex(@"^[\t ]*(?<Action>STARTED|DONE): slice (?<CurrentSplitFile>\d+) of (?<TotalSplitFiles>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
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

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;
                var currentSlice = 0;
                var totalSlices = 0;

                var currentSplitFastaFile = 0;
                var splitFastaFileCount = 0;

                var currentDatasetId = 0;
                float datasetProgress = 0;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead < 5)
                    {
                        // The first line has the path to the MSFragger .jar file and the command line arguments
                        // The second line is dashes
                        // The third line should have the MSFragger version

                        if (string.IsNullOrEmpty(mMSFraggerVersion) &&
                            dataLine.StartsWith("MSFragger version", StringComparison.OrdinalIgnoreCase))
                        {
                            LogDebug(dataLine, mDebugLevel);
                            mMSFraggerVersion = string.Copy(dataLine);
                        }

                        if (dataLine.StartsWith(BATMASS_IO_VERSION, StringComparison.OrdinalIgnoreCase) &&
                            mMSFraggerVersion.IndexOf(BATMASS_IO_VERSION, StringComparison.OrdinalIgnoreCase) < 0)
                        {
                            mMSFraggerVersion = mMSFraggerVersion + "; " + dataLine;
                        }

                        continue;
                    }

                    foreach (var processingStep in processingSteps)
                    {
                        if (!processingStep.Value.IsMatch(dataLine))
                            continue;

                        currentProgress = processingStep.Key;

                        if (currentProgress == MAIN_SEARCH_START)
                        {
                            // Reset slice tracking variables
                            currentSlice = 0;
                            totalSlices = 0;

                            currentDatasetId = 0;
                            datasetProgress = 0;
                        }

                        break;
                    }

                    var splitFastaProgressMatch = splitFastaMatcher.Match(dataLine);

                    if (splitFastaProgressMatch.Success &&
                        splitFastaProgressMatch.Groups["Action"].Value.Equals("STARTED", StringComparison.OrdinalIgnoreCase))
                    {
                        currentSplitFastaFile = int.Parse(splitFastaProgressMatch.Groups["CurrentSplitFile"].Value);

                        if (splitFastaFileCount == 0)
                        {
                            splitFastaFileCount = int.Parse(splitFastaProgressMatch.Groups["TotalSplitFiles"].Value);
                        }
                    }
                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running MSFragger: " + dataLine;
                    }

                    var sliceMatch = sliceMatcher.Match(dataLine);

                    if (sliceMatch.Success)
                    {
                        currentSlice = int.Parse(sliceMatch.Groups["Current"].Value);
                        totalSlices = int.Parse(sliceMatch.Groups["Total"].Value);
                    }
                    else if (currentSlice > 0)
                    {
                        var datasetMatch = datasetMatcher.Match(dataLine);

                        if (datasetMatch.Success)
                        {
                            currentDatasetId = int.Parse(datasetMatch.Groups["DatasetNumber"].Value);
                        }

                        var progressMatch = progressMatcher.Match(dataLine);

                        if (progressMatch.Success)
                        {
                            datasetProgress = float.Parse(progressMatch.Groups["PercentComplete"].Value);
                        }
                    }
                }

                float effectiveProgressOverall;

                var processSlab = slabProgressRanges.Any(item => currentProgress >= item.Key && currentProgress < item.Value);

                if (processSlab && totalSlices > 0)
                {
                    float currentProgressOnSlice;
                    float nextProgressOnSlice;

                    if (currentDatasetId > 0 && currentDatasetId > mDatasetCount)
                    {
                        if (!mWarnedInvalidDatasetCount)
                        {
                            if (mDatasetCount == 0)
                            {
                                LogWarning(
                                    "mDatasetCount is 0 in ParseMSFraggerConsoleOutputFile; this indicates a programming bug. " +
                                    "Auto-updating dataset count to " + currentDatasetId);
                            }
                            else
                            {
                                LogWarning("CurrentDatasetId is greater than mDatasetCount in ParseMSFraggerConsoleOutputFile; this indicates a programming bug. " +
                                           "Auto-updating dataset count from {0} to {1}", mDatasetCount, currentDatasetId);
                            }

                            mWarnedInvalidDatasetCount = true;
                        }

                        mDatasetCount = currentDatasetId;
                    }

                    if (currentDatasetId == 0 || mDatasetCount == 0)
                    {
                        currentProgressOnSlice = 0;
                        nextProgressOnSlice = 100;
                    }
                    else
                    {
                        currentProgressOnSlice = (currentDatasetId - 1) * (100f / mDatasetCount);
                        nextProgressOnSlice = currentDatasetId * (100f / mDatasetCount);
                    }

                    // First compute the effective progress for this slice
                    var sliceProgress = ComputeIncrementalProgress(currentProgressOnSlice, nextProgressOnSlice, datasetProgress);

                    // Next compute the progress processing each of the slices (which as a group can be considered a "slab")
                    var currentProgressOnSlab = (currentSlice - 1) * (100f / totalSlices);
                    var nextProgressOnSlab = currentSlice * (100f / totalSlices);

                    var slabProgress = ComputeIncrementalProgress(currentProgressOnSlab, nextProgressOnSlab, sliceProgress);

                    // Now compute the effective overall progress

                    var nextProgress = GetNextProgressValue(progressValues, currentProgress);

                    effectiveProgressOverall = ComputeIncrementalProgress(currentProgress, nextProgress, slabProgress);
                }
                else
                {
                    effectiveProgressOverall = currentProgress;
                }

                if (float.IsNaN(effectiveProgressOverall))
                {
                    return;
                }

                if (currentSplitFastaFile > 0 && splitFastaFileCount > 0)
                {
                    // Compute overall progress as 50 plus a value between 0 and 45, where 45 is MAIN_SEARCH_DONE / 2.0

                    var currentProgressOnSplitFasta = (currentSplitFastaFile - 1) * (MAIN_SEARCH_DONE / 2f / splitFastaFileCount);
                    var nextProgressOnSplitFasta = currentSplitFastaFile * (MAIN_SEARCH_DONE / 2f / splitFastaFileCount);

                    mProgress = 50 + ComputeIncrementalProgress(currentProgressOnSplitFasta, nextProgressOnSplitFasta, 50);
                    return;
                }

                mProgress = effectiveProgressOverall;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate(string.Format(
                        "Error parsing the MSFragger console output file ({0}): {1}",
                        consoleOutputFilePath, ex.Message));
                }
            }
        }

        private CloseOutType StartMSFragger(FileInfo fastaFile)
        {
            try
            {
                LogMessage("Preparing to run MSFragger");

                // If this job applies to a single dataset, dataPackageID will be 0
                // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
                var dataPackageInfo = new DataPackageInfo(dataPackageID, this);
                RegisterEvents(dataPackageInfo);

                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty)");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Customize the path to the FASTA file and the number of threads to use
                var resultCode = UpdateMSFraggerParameterFile(out var paramFilePath, out var diaSearchEnabled);

                if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return resultCode;
                }

                if (string.IsNullOrWhiteSpace(paramFilePath))
                {
                    LogError("MSFragger parameter file name returned by UpdateMSFraggerParameterFile is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var options = new MSFraggerOptions(mJobParams);
                RegisterEvents(options);

                options.LoadMSFraggerOptions(paramFilePath);

                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre11\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to Philosopher
                var philosopherProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PHILOSOPHER_RELATIVE_PATH);

                if (string.IsNullOrWhiteSpace(philosopherProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var philosopherExe = new FileInfo(philosopherProgLoc);

                var libraryFinder = new FragPipeLibFinder(philosopherExe);

                mProgress = (int)ProgressPercentValues.VerifyingMzMLFiles;
                ResetProgRunnerCpuUsage();

                // Confirm that the .mzML files have centroided MS2 spectra
                if (!MzMLFilesAreCentroided(javaProgLoc, dataPackageInfo, libraryFinder))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, MSFRAGGER_CONSOLE_OUTPUT)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;

                var databaseSplitCount = mJobParams.GetJobParameter("MSFragger", "DatabaseSplitCount", 1);

                if (databaseSplitCount > 1 && !FragPipeLibFinder.PythonInstalled)
                {
                    LogError("Could not find Python 3.x; cannot run MSFragger with the split database option");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mDatasetCount = dataPackageInfo.DatasetFiles.Count;
                mWarnedInvalidDatasetCount = false;

                LogMessage("Running MSFragger");
                mProgress = (int)ProgressPercentValues.StartingMSFragger;

                // Set up and execute a program runner to run MSFragger

                bool processingSuccess;

                if (databaseSplitCount <= 1)
                {
                    processingSuccess = StartMSFragger(dataPackageInfo, javaProgLoc, fastaFileSizeMB, paramFilePath, options);
                }
                else
                {
                    processingSuccess = StartMSFraggerSplitFASTA(dataPackageInfo, javaProgLoc, fastaFileSizeMB, paramFilePath, options, databaseSplitCount, libraryFinder);
                }

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mMSFraggerVersion))
                    {
                        ParseMSFraggerConsoleOutputFile(Path.Combine(mWorkDir, MSFRAGGER_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    LogError("Error running MSFragger");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSFragger returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSFragger failed (but exit code is 0)");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var successCount = 0;

                // Validate that MSFragger created a .pepXML file for each dataset
                // For DIA data, the program creates several .pepXML files

                // If databaseSplitCount is 1, there should also be a .tsv file and a .pin file for each dataset (though with DIA data there is only a .pin file, not a .tsv file)
                // If databaseSplitCount is more than 1, we will create a .tsv file using the data in the .pepXML file

                // Zip each .pepXML file
                foreach (var item in dataPackageInfo.Datasets)
                {
                    var datasetName = item.Value;

                    string optionalDatasetInfo;

                    if (dataPackageInfo.Datasets.Count > 0)
                    {
                        optionalDatasetInfo = " for dataset " + datasetName;
                    }
                    else
                    {
                        optionalDatasetInfo = string.Empty;
                    }

                    var workingDirectory = new DirectoryInfo(mWorkDir);
                    var pepXmlFiles = FindDatasetPinFileAndPepXmlFiles(workingDirectory, diaSearchEnabled, datasetName, out var pinFile);

                    if (pepXmlFiles.Count == 0)
                    {
                        // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                        if (diaSearchEnabled)
                        {
                            // MSFragger did not create any .pepXML files for dataset
                            LogError(string.Format("MSFragger did not create any .pepXML files{0}", optionalDatasetInfo));
                        }
                        else
                        {
                            // MSFragger did not create a .pepXML file for dataset
                            LogError(string.Format("MSFragger did not create a .pepXML file{0}", optionalDatasetInfo));
                        }

                        // Treat this as a fatal error
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    var tsvFile = new FileInfo(Path.Combine(mWorkDir, datasetName + ".tsv"));

                    var splitFastaSearch = databaseSplitCount > 1;

                    if (!diaSearchEnabled && !tsvFile.Exists)
                    {
                        if (!splitFastaSearch)
                        {
                            LogError(string.Format("MSFragger did not create a .tsv file{0}", optionalDatasetInfo));
                        }

                        // ToDo: create a .tsv file using the .pepXML file
                    }

                    if (!pinFile.Exists && !splitFastaSearch)
                    {
                        LogError(string.Format("MSFragger did not create a .pin file{0}", optionalDatasetInfo));
                    }

                    var zipSuccess = ZipPepXmlAndPinFiles(this, dataPackageInfo, datasetName, pepXmlFiles, pinFile.Exists);

                    if (!zipSuccess)
                        continue;

                    mJobParams.AddResultFileExtensionToSkip(PEPXML_EXTENSION);
                    mJobParams.AddResultFileExtensionToSkip(PIN_EXTENSION);

                    successCount++;
                }

                mStatusTools.UpdateAndWrite(mProgress);
                LogDebug("MSFragger Search Complete", mDebugLevel);

                return successCount == dataPackageInfo.Datasets.Count ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in StartMSFragger", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartMSFragger(
            DataPackageInfo dataPackageInfo,
            string javaProgLoc,
            double fastaFileSizeMB,
            string paramFilePath,
            MSFraggerOptions options)
        {
            // Larger FASTA files need more memory
            // Additional memory is also required as the number of dynamic mods being considered increases

            // 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods

            var dynamicModCount = options.GetDynamicModResidueCount();

            var javaMemorySizeMB = AnalysisResourcesMSFragger.GetJavaMemorySizeToUse(mJobParams, fastaFileSizeMB, dynamicModCount, out var msFraggerJavaMemorySizeMB);

            if (javaMemorySizeMB > msFraggerJavaMemorySizeMB)
            {
                var dynamicModCountDescription = MSFraggerOptions.GetDynamicModCountDescription(dynamicModCount);

                var msg = string.Format("Allocating {0:N0} MB to Java for a {1:N0} MB FASTA file and {2}", javaMemorySizeMB, fastaFileSizeMB, dynamicModCountDescription);
                LogMessage(msg);

                mEvalMessage = Global.AppendToComment(mEvalMessage, msg);
            }

            if (Global.RunningOnDeveloperComputer())
            {
                var freeMemoryMB = Global.GetFreeMemoryMB();

                if (javaMemorySizeMB > freeMemoryMB * 0.9)
                {
                    ConsoleMsgUtils.ShowWarning(
                        "Decreasing Java memory size from {0:N0} MB to {1:N0} MB since running on developer machine and not enough free memory",
                        javaMemorySizeMB, freeMemoryMB * 0.9);

                    javaMemorySizeMB = (int)Math.Round(freeMemoryMB * 0.9, 0);
                    ConsoleMsgUtils.SleepSeconds(2);
                }
            }
            // ReSharper disable CommentTypo

            // Example command line:
            // java -jar -Dfile.encoding=UTF-8 -Xmx11G C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\MSFragger-4.1.jar C:\DMS_WorkDir\fragger.params C:\DMS_WorkDir\Dataset.mzML

            // ReSharper restore CommentTypo

            var arguments = new StringBuilder();

            // ReSharper disable once StringLiteralTypo
            arguments.AppendFormat("-Dfile.encoding=UTF-8 -Xmx{0}M -jar {1}", javaMemorySizeMB, mMSFraggerProgLoc);

            arguments.AppendFormat(" {0}", paramFilePath);

            switch (dataPackageInfo.DatasetFiles.Count)
            {
                case 0:
                    LogError("DatasetFiles list in dataPackageInfo is empty");
                    return false;

                case > 1:
                    // One way to run MSFragger is by listing each .mzML file to process
                    // However, the Windows command line is limited to 8191 characters, which is likely to be exceeded if the data package has over ~75 datasets
                    // Instead, use a wildcard to specify the input files
                    // Assure that the input files all have the same extension (which should be .mzML)

                    var datasetFileExtension = string.Empty;

                    foreach (var item in dataPackageInfo.DatasetFiles)
                    {
                        if (string.IsNullOrEmpty(datasetFileExtension))
                        {
                            datasetFileExtension = Path.GetExtension(item.Value);
                            continue;
                        }

                        var fileExtension = Path.GetExtension(item.Value);

                        if (fileExtension.Equals(datasetFileExtension, StringComparison.OrdinalIgnoreCase))
                            continue;

                        LogError("Files in dataPackageInfo.DatasetFiles do not all have the same file extension; expecting {0} but found {1}", datasetFileExtension, fileExtension);

                        return false;
                    }

                    // Append text of the form C:\DMS_WorkDir1\*.mzML
                    arguments.AppendFormat(" {0}{1}*{2}", mWorkDir, Path.DirectorySeparatorChar, datasetFileExtension);
                    break;

                default:
                    // Processing a single .mzML file
                    arguments.AppendFormat(" {0}", Path.Combine(mWorkDir, dataPackageInfo.DatasetFiles.First().Value));
                    break;
            }

            LogDebug(javaProgLoc + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(javaProgLoc, arguments.ToString(), "MSFragger", true);
        }

        private bool StartMSFraggerSplitFASTA(
            DataPackageInfo dataPackageInfo,
            string javaProgLoc,
            double fastaFileSizeMB,
            string paramFilePath,
            MSFraggerOptions options,
            int databaseSplitCount,
            FragPipeLibFinder libraryFinder)
        {
            var pythonExe = FragPipeLibFinder.PythonPath;

            if (!libraryFinder.FindFragPipeToolsDirectory(out var toolsDirectory))
            {
                // The error has already been logged
                return false;
            }

            var msFraggerScript = new FileInfo(Path.Combine(toolsDirectory.FullName, "msfragger_pep_split.py"));

            if (!msFraggerScript.Exists)
            {
                LogError("MSFragger script not found; cannot run a split FASTA search: " + msFraggerScript.FullName);
                return false;
            }

            var dynamicModCount = options.GetDynamicModResidueCount();

            AnalysisResourcesMSFragger.GetJavaMemorySizeToUse(mJobParams, fastaFileSizeMB, dynamicModCount, out var msFraggerJavaMemorySizeMB);

            var msg = string.Format(
                "Allocating {0:N0} MB to Java, splitting the {1:N0} MB FASTA file into {2} parts",
                msFraggerJavaMemorySizeMB, fastaFileSizeMB, databaseSplitCount);

            LogMessage(msg);

            mEvalMessage = Global.AppendToComment(mEvalMessage, msg);

            // ReSharper disable CommentTypo

            // Example command line:
            // C:\Python39\python.exe C:\DMS_Programs\MSFragger\fragpipe\tools\msfragger_pep_split.py 2 "java -jar -Dfile.encoding=UTF-8 -Xmx14G" C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\MSFragger-4.1.jar C:\DMS_WorkDir\MSFragger_ParamFile.params C:\DMS_WorkDir\DatasetName.mzML

            // ReSharper restore CommentTypo

            var arguments = new StringBuilder();

            arguments.AppendFormat("{0} {1}", msFraggerScript.FullName, databaseSplitCount);

            // ReSharper disable once StringLiteralTypo
            arguments.AppendFormat(" \"{0} -jar -Dfile.encoding=UTF-8 -Xmx{1}M \"", javaProgLoc, msFraggerJavaMemorySizeMB);

            arguments.AppendFormat(" {0}", mMSFraggerProgLoc);

            arguments.AppendFormat(" {0}", paramFilePath);

            // Append the .mzML files
            foreach (var item in dataPackageInfo.DatasetFiles)
            {
                arguments.AppendFormat(" {0}", Path.Combine(mWorkDir, item.Value));
            }

            LogDebug(pythonExe + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(pythonExe, arguments.ToString(), "MSFragger", true);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info", mDebugLevel);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMSFraggerProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(mMSFraggerVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the FASTA file name defined in the MSFragger parameter file
        /// In addition, determine whether this is a DIA search
        /// </summary>
        /// <param name="paramFilePath">Output: parameter file path</param>
        /// <param name="diaSearchEnabled">Output: set to true if data_type is 1 or 2, meaning DIA data</param>
        private CloseOutType UpdateMSFraggerParameterFile(out string paramFilePath, out bool diaSearchEnabled)
        {
            const string FASTA_FILE_COMMENT = "FASTA File (should include decoy proteins)";
            const string FILE_FORMAT_COMMENT = "File format of output files; Percolator uses .pin files";
            const string THREAD_COUNT_COMMENT = "Number of CPU threads to use (0=poll CPU to set num threads)";

            const string REQUIRED_OUTPUT_FORMAT = "tsv_pepxml_pin";

            paramFilePath = string.Empty;
            diaSearchEnabled = false;

            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, paramFileName + ".new"));

                var fastaFileDefined = false;
                var threadsDefined = false;
                var outputFormatDefined = false;

                var numThreadsToUse = GetNumThreadsToUse();

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var lineNumber = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        lineNumber++;

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var trimmedLine = dataLine.Trim();

                        if (trimmedLine.StartsWith("database_name"))
                        {
                            if (fastaFileDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FASTA_FILE_COMMENT);

                            WriteParameterFileSetting(writer, "database_name", mLocalFASTAFilePath, comment);

                            fastaFileDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith("num_threads"))
                        {
                            if (threadsDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, THREAD_COUNT_COMMENT);

                            WriteParameterFileSetting(writer, "num_threads", numThreadsToUse.ToString(), comment);

                            threadsDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith("output_format"))
                        {
                            if (outputFormatDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FILE_FORMAT_COMMENT);

                            if (setting.ParamValue.Equals(REQUIRED_OUTPUT_FORMAT, StringComparison.OrdinalIgnoreCase))
                            {
                                writer.WriteLine(dataLine);
                            }
                            else
                            {
                                // Auto-change the output format to tsv_pepxml_pin
                                WriteParameterFileSetting(writer, "output_format", REQUIRED_OUTPUT_FORMAT, comment);

                                LogWarning("Auto-updated the MSFragger output format from {0} to {1} because Percolator requires .pin files", setting.ParamValue, REQUIRED_OUTPUT_FORMAT);
                            }

                            outputFormatDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith("data_type"))
                        {
                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);

                            if (!int.TryParse(setting.ParamValue, out var dataTypeMode))
                            {
                                LogError("The data_type setting in the MSFragger parameter file is not followed by an integer: " + dataLine);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }

                            if (dataTypeMode is 1 or 2)
                                diaSearchEnabled = true;
                        }

                        writer.WriteLine(dataLine);
                    }

                    if (!fastaFileDefined)
                    {
                        WriteParameterFileSetting(writer, "database_name", mLocalFASTAFilePath, FASTA_FILE_COMMENT);
                    }

                    if (!threadsDefined)
                    {
                        WriteParameterFileSetting(writer, "num_threads", numThreadsToUse.ToString(), THREAD_COUNT_COMMENT);
                    }

                    if (!outputFormatDefined)
                    {
                        WriteParameterFileSetting(writer, "output_format", REQUIRED_OUTPUT_FORMAT, FILE_FORMAT_COMMENT);
                    }
                }

                // Replace the original parameter file with the updated one
                sourceFile.Delete();
                updatedFile.MoveTo(Path.Combine(mWorkDir, paramFileName));

                paramFilePath = updatedFile.FullName;

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error updating the MSFragger parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool ValidateFastaFile(out FileInfo fastaFile)
        {
            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinCollectionList = mJobParams.GetParam("ProteinCollectionList");

            var fastaHasDecoys = ValidateFastaHasDecoyProteins(fastaFile);

            if (!fastaHasDecoys)
            {
                string warningMessage;

                if (string.IsNullOrWhiteSpace(proteinCollectionList) || proteinCollectionList.Equals("na", StringComparison.OrdinalIgnoreCase))
                {
                    warningMessage = "Using a legacy FASTA file that does not have decoy proteins; " +
                                     "this will lead to errors with Peptide Prophet or Percolator";
                }
                else
                {
                    warningMessage = "Protein options for this analysis job contain seq_direction=forward; " +
                                     "decoy proteins will not be used (which will lead to errors with Peptide Prophet or Percolator)";
                }

                // The FASTA file does not have decoy sequences
                // MSFragger will be unable to optimize parameters and Peptide Prophet will likely fail
                LogError(warningMessage, true);

                // Abort processing
                return false;
            }

            // Copy the FASTA file to the working directory
            // This is done because MSFragger indexes the file based on the dynamic and static mods,
            // and we want that index file to be in the working directory

            // ReSharper disable once CommentTypo

            // Example index file name: ID_007564_FEA6EC69.fasta.1.pepindex

            mLocalFASTAFilePath = Path.Combine(mWorkDir, fastaFile.Name);

            fastaFile.CopyTo(mLocalFASTAFilePath, true);

            mJobParams.AddResultFileToSkip(fastaFile.Name);

            // ReSharper disable once StringLiteralTypo
            mJobParams.AddResultFileExtensionToSkip("pepindex");

            mJobParams.AddResultFileExtensionToSkip("peptide_idx_dict");

            return true;
        }

        private bool ValidateFastaHasDecoyProteins(FileInfo fastaFile)
        {
            const string DECOY_PREFIX = "XXX_";

            try
            {
                // If using a protein collection, could check for "seq_direction=decoy" in proteinOptions
                // But, we'll instead examine the actual protein names for both Protein Collection-based and Legacy FASTA-based jobs

                var forwardCount = 0;
                var decoyCount = 0;

                var reader = new ProteinFileReader.FastaFileReader(fastaFile.FullName);

                while (reader.ReadNextProteinEntry())
                {
                    if (reader.ProteinName.StartsWith(DECOY_PREFIX))
                        decoyCount++;
                    else
                        forwardCount++;
                }

                var fileSizeMB = fastaFile.Length / 1024.0 / 1024;

                if (decoyCount == 0)
                {
                    LogDebug("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins, but no decoy proteins", fastaFile.Name, fileSizeMB, forwardCount);
                    return false;
                }

                LogDebug("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins and {3:N0} decoy proteins", fastaFile.Name, fileSizeMB, forwardCount, decoyCount);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidateFastaHasDecoyProteins", ex);
                return false;
            }
        }

        private void WriteParameterFileSetting(TextWriter writer, string paramName, string paramValue, string comment)
        {
            if (!comment.Trim().StartsWith("#"))
            {
                comment = "# " + comment.Trim();
            }

            var parameterNameAndValue = string.Format("{0} = {1}", paramName, paramValue);
            writer.WriteLine("{0,-38} {1}", parameterNameAndValue, comment);
        }

        /// <summary>
        /// Zip the .pepXML file(s) created by MSFragger
        /// </summary>
        /// <param name="toolRunner">Tool runner instance (since this is a static method)</param>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="pepXmlFiles">Typically this is a single .pepXML file, but for DIA searches, this is a set of .pepXML files</param>
        /// <param name="addPinFile">When true, add the .pin file to the first zipped .pepXML file</param>
        /// <returns>True if success, false if an error</returns>
        public static bool ZipPepXmlAndPinFiles(
            AnalysisToolRunnerBase toolRunner,
            DataPackageInfo dataPackageInfo,
            string datasetName,
            List<FileInfo> pepXmlFiles,
            bool addPinFile)
        {
            if (pepXmlFiles.Count == 0)
            {
                toolRunner.LogError("Empty file list sent to method ZipPepXmlAndPinFiles");
                return false;
            }

            var primaryPepXmlFile = new List<FileInfo>();
            var additionalPepXmlFiles = new List<FileInfo>();

            if (pepXmlFiles.Count == 1)
            {
                primaryPepXmlFile.Add(pepXmlFiles[0]);
            }
            else
            {
                // Determine the .pepXML file to store in zip file DatasetName_pepXML.zip
                // Look for the _rank1.pepXML file in pepXmlFiles
                // If not found, just use pepXmlFiles[0]

                foreach (var item in pepXmlFiles)
                {
                    if (primaryPepXmlFile.Count == 0 && item.Name.EndsWith("_rank1.pepXML", StringComparison.OrdinalIgnoreCase))
                        primaryPepXmlFile.Add(item);
                    else
                        additionalPepXmlFiles.Add(item);
                }

                if (primaryPepXmlFile.Count == 0)
                    primaryPepXmlFile.Add(pepXmlFiles[0]);
            }

            if (primaryPepXmlFile.Count == 0)
                return false;

            if (primaryPepXmlFile[0].Length == 0)
            {
                string optionalDatasetInfo;

                if (dataPackageInfo.Datasets.Count > 0)
                {
                    optionalDatasetInfo = " for dataset " + datasetName;
                }
                else
                {
                    optionalDatasetInfo = string.Empty;
                }

                // pepXML file created by MSFragger is empty for dataset
                toolRunner.LogError("pepXML file created by MSFragger is empty{0}", optionalDatasetInfo);
            }

            var success = ZipPepXmlAndPinFile(toolRunner, datasetName, primaryPepXmlFile[0], addPinFile);

            if (!success)
                return false;

            if (additionalPepXmlFiles.Count == 0)
                return true;

            var successCount = 0;

            foreach (var pepXmlFile in additionalPepXmlFiles)
            {
                var zipFileNameOverride = string.Format("{0}_pepXML.zip", Path.GetFileNameWithoutExtension(pepXmlFile.Name));

                var success2 = ZipPepXmlAndPinFile(toolRunner, datasetName, pepXmlFile, false, zipFileNameOverride);

                if (success2)
                    successCount++;
            }

            if (successCount == additionalPepXmlFiles.Count)
                return true;

            toolRunner.LogError("Zip failure for {0} / {1} .pepXML files created by MSFragger", additionalPepXmlFiles.Count - successCount, additionalPepXmlFiles.Count);

            return false;
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Zip the .pepXML file created by MSFragger
        /// </summary>
        /// <param name="toolRunner">Tool runner instance (since this is a static method)</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="pepXmlFile">.pepXML file</param>
        /// <param name="addPinFile">If true, add this dataset's .pin file to the .zip file</param>
        /// <param name="zipFileNameOverride">If an empty string, name the .zip file DatasetName_pepXML.zip; otherwise, use this name</param>
        /// <returns>True if success, false if an error</returns>
        private static bool ZipPepXmlAndPinFile(AnalysisToolRunnerBase toolRunner, string datasetName, FileInfo pepXmlFile, bool addPinFile, string zipFileNameOverride = "")
        {
            mZipTool ??= new ZipFileTools(toolRunner.DebugLevel, toolRunner.WorkingDirectory);

            var zipSuccess = toolRunner.ZipOutputFile(pepXmlFile, ".pepXML file");

            if (!zipSuccess)
            {
                return false;
            }

            // Rename the zipped file
            var zipFile = new FileInfo(Path.ChangeExtension(pepXmlFile.FullName, ".zip"));

            if (!zipFile.Exists)
            {
                toolRunner.LogError("Zipped pepXML file not found; cannot rename");
                return false;
            }

            if (string.IsNullOrWhiteSpace(zipFileNameOverride))
            {
                zipFileNameOverride = datasetName + "_pepXML.zip";
            }

            var newZipFilePath = Path.Combine(toolRunner.WorkingDirectory, zipFileNameOverride);

            var existingTargetFile = new FileInfo(newZipFilePath);

            if (existingTargetFile.Exists)
            {
                toolRunner.LogMessage("Replacing {0} with updated version", existingTargetFile.Name);
                existingTargetFile.Delete();
            }

            zipFile.MoveTo(newZipFilePath);

            if (!addPinFile)
                return true;

            // Add the .pin file to the zip file

            var pinFile = new FileInfo(Path.Combine(toolRunner.WorkingDirectory, datasetName + PIN_EXTENSION));

            if (!pinFile.Exists)
            {
                toolRunner.LogError(".pin file not found; cannot add: " + pinFile.Name);
                return false;
            }

            var success = mZipTool.AddToZipFile(zipFile.FullName, pinFile);

            if (success)
            {
                return true;
            }

            toolRunner.LogError("Error adding {0} to {1}", pinFile.Name, zipFile.FullName);
            return false;
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseMSFraggerConsoleOutputFile(Path.Combine(mWorkDir, MSFRAGGER_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSFraggerVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MSFragger");
        }
    }
}
