//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/29/2009
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PeptideToProteinMapEngine;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

// ReSharper disable IdentifierTypo
// ReSharper disable StringLiteralTypo

namespace AnalysisManagerInspResultsAssemblyPlugIn
{
    /// <summary>
    /// Class for running Inspect Results Assembler
    /// </summary>
    public class AnalysisToolRunnerInspResultsAssembly : AnalysisToolRunnerBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: fasta, InspResults, Pvalue, trie

        private const string PVALUE_MINLENGTH5_SCRIPT = "PValue_MinLength5.py";

        private const string ORIGINAL_INSPECT_FILE_SUFFIX = "_inspect.txt";
        private const string FIRST_HITS_INSPECT_FILE_SUFFIX = "_inspect_fht.txt";
        private const string FILTERED_INSPECT_FILE_SUFFIX = "_inspect_filtered.txt";

        // Used for result file type
        public enum ResultFileType
        {
            INSPECT_RESULT = 0,
            INSPECT_ERROR = 1,
            INSPECT_SEARCH = 2,
            INSPECT_CONSOLE = 3
        }

        // Note: if you add/remove any steps, update PERCENT_COMPLETE_LEVEL_COUNT and update the population of mPercentCompleteStartLevels()
        public enum InspectResultsProcessingSteps
        {
            Starting = 0,
            AssembleResults = 1,
            RunpValue = 2,
            ZipInspectResults = 3,
            CreatePeptideToProteinMapping = 4
        }

        private struct ModInfo
        {
            public string ModName;
            public string ModMass;             // Storing as a string since reading from a text file and writing to a text file
            public string Residues;
        }

        public const string INSPECT_INPUT_PARAMS_FILENAME = "inspect_input.txt";

        private string mInspectResultsFileName;

        // Note that PeptideToProteinMapEngine utilizes System.Data.SQLite.dll
        private clsPeptideToProteinMapEngine mPeptideToProteinMapper;

        // mPercentCompleteStartLevels is an array that lists the percent complete value to report
        //  at the start of each of the various processing steps performed in this procedure
        // The percent complete values range from 0 to 100
        private const int PERCENT_COMPLETE_LEVEL_COUNT = 5;
        private float[] mPercentCompleteStartLevels;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Initializes class-wide variables</remarks>
        public AnalysisToolRunnerInspResultsAssembly()
        {
            InitializeVariables();
        }

        /// <summary>
        /// Runs InSpecT tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            var filteredResultsAreEmpty = false;

            // We no longer need to index the FASTA file (since we're no longer using PValue.py with the -a switch or Summary.py

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerInspResultsAssembly.RunTool(): Enter");
                }

                // Call base class for initial setup
                base.RunTool();

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Inspect Results Assembly version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine if this is a parallelized job
                var numClonedSteps = mJobParams.GetParam("NumberOfClonedSteps");

                var processingSuccess = true;
                bool isParallelized;

                if (string.IsNullOrEmpty(numClonedSteps))
                {
                    // This is not a parallelized job; no need to assemble the results

                    // FilterInspectResultsByFirstHits will create file _inspect_fht.txt
                    FilterInspectResultsByFirstHits();

                    // FilterInspectResultsByPValue will create file _inspect_filtered.txt
                    var pValueFilterResult = FilterInspectResultsByPValue();
                    if (pValueFilterResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        processingSuccess = false;
                    }
                    isParallelized = false;
                }
                else
                {
                    // This is a parallelized job; need to re-assemble the results
                    var numResultFiles = Convert.ToInt32(numClonedSteps);

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Assembling parallelized inspect files; file count = " + numResultFiles);
                    }

                    // AssembleResults will create _inspect.txt, _inspect_fht.txt, and _inspect_filtered.txt
                    var assemblyResult = AssembleResults(numResultFiles);

                    if (assemblyResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        processingSuccess = false;
                    }
                    isParallelized = true;
                }

                if (processingSuccess)
                {
                    // Rename and zip up files _inspect_filtered.txt and _inspect.txt
                    var zipResult = ZipInspectResults();
                    if (zipResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        processingSuccess = false;
                    }
                }

                if (processingSuccess)
                {
                    // Create the Peptide to Protein map file
                    var pepToProteinMappingResult = CreatePeptideToProteinMapping();
                    if (pepToProteinMappingResult == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        filteredResultsAreEmpty = true;
                    }
                    else if (pepToProteinMappingResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        processingSuccess = false;
                    }
                }

                mProgress = 100;
                UpdateStatusRunning();

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var copySuccess = CopyResultsToTransferDirectory();

                if (!copySuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                // If parallelized, remove multiple Result files from server
                if (isParallelized)
                {
                    if (!RemoveNonResultServerFiles())
                    {
                        LogWarning("Error deleting non Result files from directory on server, job " + mJob + ", step " + mJobParams.GetParam("Step"));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                return filteredResultsAreEmpty ? CloseOutType.CLOSEOUT_NO_DATA : CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception during Inspect Results Assembly", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object containing manager parameters</param>
        /// <param name="jobParams">Object containing job parameters</param>
        /// <param name="statusTools">Object for updating status file as job progresses</param>
        /// <param name="summaryFile">Object for creating an analysis job summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities</param>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, SummaryFile summaryFile, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, summaryFile, myEMSLUtilities);

            mInspectResultsFileName = mDatasetName + ORIGINAL_INSPECT_FILE_SUFFIX;
        }

        private CloseOutType AssembleResults(int numResultFiles)
        {
            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect result files");
                }

                UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.AssembleResults]);

                // Combine the individual _xx_inspect.txt files to create the single _inspect.txt file
                var result = AssembleFiles(mInspectResultsFileName, ResultFileType.INSPECT_RESULT, numResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect error files");
                }

                var fileName = mDatasetName + "_error.txt";
                result = AssembleFiles(fileName, ResultFileType.INSPECT_ERROR, numResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(fileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect search log files");
                }

                fileName = "InspectSearchLog.txt";
                result = AssembleFiles(fileName, ResultFileType.INSPECT_SEARCH, numResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(fileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect console output files");
                }

                fileName = "InspectConsoleOutput.txt";
                result = AssembleFiles(fileName, ResultFileType.INSPECT_CONSOLE, numResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(fileName);

                // FilterInspectResultsByFirstHits will create file _inspect_fht.txt
                result = FilterInspectResultsByFirstHits();

                // Re-score the assembled inspect results using PValue_MinLength5.py (which is similar to PValue.py but retains peptides of length 5 or greater)
                // This will create files _inspect_fht.txt and _inspect_filtered.txt
                result = ReScoreAssembledInspectResults();
                return result;
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->AssembleResults";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Assemble the result files
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType AssembleFiles(string combinedFileName, ResultFileType resFileType, int numResultFiles)
        {
            var inspectResultsFile = "";

            var filesContainHeaderLine = false;
            var headerLineWritten = false;
            var addSegmentNumberToEachLine = false;
            var addBlankLineBetweenFiles = false;

            try
            {
                var DatasetName = mDatasetName;

                var writer = CreateNewExportFile(Path.Combine(mWorkDir, combinedFileName));
                if (writer == null)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                for (var fileNameCounter = 1; fileNameCounter <= numResultFiles; fileNameCounter++)
                {
                    var error = false;
                    switch (resFileType)
                    {
                        case ResultFileType.INSPECT_RESULT:
                            inspectResultsFile = DatasetName + "_" + fileNameCounter + ORIGINAL_INSPECT_FILE_SUFFIX;
                            filesContainHeaderLine = true;
                            addSegmentNumberToEachLine = false;
                            addBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_ERROR:
                            inspectResultsFile = DatasetName + "_" + fileNameCounter + "_error.txt";
                            filesContainHeaderLine = false;
                            addSegmentNumberToEachLine = true;
                            addBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_SEARCH:
                            inspectResultsFile = "InspectSearchLog_" + fileNameCounter + ".txt";
                            filesContainHeaderLine = true;
                            addSegmentNumberToEachLine = true;
                            addBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_CONSOLE:
                            inspectResultsFile = "InspectConsoleOutput_" + fileNameCounter + ".txt";
                            filesContainHeaderLine = false;
                            addSegmentNumberToEachLine = false;
                            addBlankLineBetweenFiles = true;

                            break;
                        default:
                            // Unknown ResultFileType
                            LogError("AnalysisToolRunnerInspResultsAssembly->AssembleFiles: Unknown Inspect Result File Type: " + resFileType);
                            error = true;
                            break;
                    }
                    if (error)
                    {
                        break;
                    }

                    if (!File.Exists(Path.Combine(mWorkDir, inspectResultsFile)))
                        continue;

                    var linesRead = 0;

                    var reader = new StreamReader(new FileStream(Path.Combine(mWorkDir, inspectResultsFile), FileMode.Open, FileAccess.Read, FileShare.Read));
                    var dataLine = reader.ReadLine();

                    while (dataLine != null)
                    {
                        linesRead++;

                        if (linesRead == 1)
                        {
                            if (filesContainHeaderLine)
                            {
                                // Handle the header line
                                if (!headerLineWritten)
                                {
                                    if (addSegmentNumberToEachLine)
                                    {
                                        dataLine = "Segment\t" + dataLine;
                                    }
                                    writer.WriteLine(dataLine);
                                }
                            }
                            else
                            {
                                if (addSegmentNumberToEachLine)
                                {
                                    if (!headerLineWritten)
                                    {
                                        writer.WriteLine("Segment\tMessage");
                                    }
                                    writer.WriteLine(fileNameCounter + "\t" + dataLine);
                                }
                                else
                                {
                                    writer.WriteLine(dataLine);
                                }
                            }
                            headerLineWritten = true;
                        }
                        else
                        {
                            if (resFileType == ResultFileType.INSPECT_RESULT)
                            {
                                // Parse each line of the Inspect Results files to remove the directory path information from the first column
                                try
                                {
                                    var tabIndex = dataLine.IndexOf('\t');
                                    if (tabIndex > 0)
                                    {
                                        // Note: .LastIndexOf will start at index tabIndex and search backwards until the first match is found
                                        // (this is a bit counter-intuitive, but that's what it does)
                                        var slashIndex = dataLine.LastIndexOf(Path.DirectorySeparatorChar, tabIndex);
                                        if (slashIndex > 0)
                                        {
                                            dataLine = dataLine.Substring(slashIndex + 1);
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    // Ignore errors here
                                }
                            }

                            if (addSegmentNumberToEachLine)
                            {
                                writer.WriteLine(fileNameCounter + "\t" + dataLine);
                            }
                            else
                            {
                                writer.WriteLine(dataLine);
                            }
                        }

                        // Read the next line
                        dataLine = reader.ReadLine();
                    }
                    reader.Close();

                    if (addBlankLineBetweenFiles)
                    {
                        Console.WriteLine();
                    }
                }

                // close the main result file
                writer.Close();

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->AssembleFiles";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private StreamWriter CreateNewExportFile(string exportFilePath)
        {
            if (File.Exists(exportFilePath))
            {
                // Post error to log
                LogError("AnalysisToolRunnerInspResultsAssembly->createNewExportFile: Export file already exists " +
                         "(" + exportFilePath + "); this is unexpected");
                return null;
            }

            return new StreamWriter(new FileStream(exportFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
        }

        private CloseOutType CreatePeptideToProteinMapping()
        {
            var orgDbDir = mMgrParams.GetParam("OrgDbDir");

            // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var dbFilename = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

            var ignorePeptideToProteinMapperErrors = false;

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.CreatePeptideToProteinMapping]);

            var inputFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);

            try
            {
                // Validate that the input file has at least one entry; if not, no point in continuing
                var linesRead = 0;

                using (var reader = new StreamReader(new FileStream(inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!reader.EndOfStream && linesRead < 10)
                    {
                        var dataLine = reader.ReadLine();
                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            linesRead++;
                        }
                    }
                }

                if (linesRead <= 1)
                {
                    // File is empty or only contains a header line
                    LogError("No results above threshold; filtered inspect results file is empty");

                    // Storing "No results above threshold" in mMessage will result in the job being assigned state No Export (14) in DMS
                    // See stored procedure UpdateJobState
                    mMessage = NO_RESULTS_ABOVE_THRESHOLD;
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error validating Inspect results file contents in InspectResultsAssembly->CreatePeptideToProteinMapping";

                LogError(mMessage + ", job " + mJob, ex);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("Creating peptide to protein map file");
                }

                ignorePeptideToProteinMapperErrors = mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false);

                var options = new ProteinCoverageSummarizer.ProteinCoverageSummarizerOptions()
                {
                    IgnoreILDifferences = false,
                    MatchPeptidePrefixAndSuffixToProtein = false,
                    OutputProteinSequence = false,
                    PeptideFileSkipFirstLine = false,
                    ProteinInputFilePath = Path.Combine(orgDbDir, dbFilename),
                    SaveProteinToPeptideMappingFile = true,
                    SearchAllProteinsForPeptideSequence = true,
                    SearchAllProteinsSkipCoverageComputationSteps = true
                };

                mPeptideToProteinMapper = new clsPeptideToProteinMapEngine(options);

                RegisterEvents(mPeptideToProteinMapper);
                mPeptideToProteinMapper.ProgressUpdate -= ProgressUpdateHandler;
                mPeptideToProteinMapper.ProgressUpdate += PeptideToProteinMapper_ProgressChanged;

                mPeptideToProteinMapper.DeleteTempFiles = true;
                mPeptideToProteinMapper.InspectParameterFilePath = Path.Combine(mWorkDir, INSPECT_INPUT_PARAMS_FILENAME);

                if (mDebugLevel > 2)
                {
                    mPeptideToProteinMapper.LogMessagesToFile = true;
                    mPeptideToProteinMapper.LogDirectoryPath = mWorkDir;
                }
                else
                {
                    mPeptideToProteinMapper.LogMessagesToFile = false;
                }

                mPeptideToProteinMapper.PeptideInputFileFormat = clsPeptideToProteinMapEngine.PeptideInputFileFormatConstants.InspectResultsFile;

                var success = mPeptideToProteinMapper.ProcessFile(inputFilePath, mWorkDir, string.Empty, true);

                mPeptideToProteinMapper.CloseLogFileNow();

                if (success)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Peptide to protein mapping complete");
                    }
                }
                else
                {
                    LogError("Error running the PeptideToProteinMapEngine: " + mPeptideToProteinMapper.GetErrorMessage());

                    if (ignorePeptideToProteinMapperErrors)
                    {
                        LogWarning("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->CreatePeptideToProteinMapping";

                LogError("AnalysisToolRunnerInspResultsAssembly.CreatePeptideToProteinMapping, Error running the PeptideToProteinMapEngine, job " +
                    mJob, ex);

                if (ignorePeptideToProteinMapperErrors)
                {
                    LogWarning("Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Reads the modification information defined in inspectParameterFilePath, storing it in modList
        /// </summary>
        /// <param name="inspectParameterFilePath"></param>
        /// <param name="modList"></param>
        private bool ExtractModInfoFromInspectParamFile(string inspectParameterFilePath, out List<ModInfo> modList)
        {
            modList = new List<ModInfo>();

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerInspResultsAssembly.ExtractModInfoFromInspectParamFile(): Reading " + inspectParameterFilePath);
                }

                // Read the Inspect parameter file
                using var reader = new StreamReader(new FileStream(inspectParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var dataLineTrimmed = dataLine.Trim();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (dataLineTrimmed[0] == '#')
                    {
                        // Comment line; skip it
                        continue;
                    }

                    if (dataLineTrimmed.StartsWith("mod", StringComparison.OrdinalIgnoreCase))
                    {
                        // Modification definition line

                        // Split the line on commas
                        var splitLine = dataLineTrimmed.Split(',');

                        if (splitLine.Length >= 5 && splitLine[0].ToLower().Trim() == "mod")
                        {
                            var mod = new ModInfo
                            {
                                ModName = splitLine[4],
                                ModMass = splitLine[1],
                                Residues = splitLine[2]
                            };

                            modList.Add(mod);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->ExtractModInfoFromInspectParamFile";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Use PValue_MinLength5.py to only retain the top hit for each scan (no p-value filtering is actually applied)
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType FilterInspectResultsByFirstHits()
        {
            var inspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var filteredFilePath = Path.Combine(mWorkDir, mDatasetName + FIRST_HITS_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.RunpValue]);

            // Note that RunPValue() will log any errors that occur
            var result = RunPValue(inspectResultsFilePath, filteredFilePath, false, true);

            return result;
        }

        /// <summary>
        /// Filters the inspect results using PValue_MinLength5.py"
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType FilterInspectResultsByPValue()
        {
            var inspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var filteredFilePath = Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.RunpValue]);

            // Note that RunPValue() will log any errors that occur
            var result = RunPValue(inspectResultsFilePath, filteredFilePath, true, false);

            return result;
        }

        private void InitializeVariables()
        {
            // Define the percent complete values to use for the start of each processing step

            mPercentCompleteStartLevels = new float[PERCENT_COMPLETE_LEVEL_COUNT + 1];

            mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.Starting] = 0;
            mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.AssembleResults] = 5;
            mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.RunpValue] = 10;
            mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.ZipInspectResults] = 65;
            mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.CreatePeptideToProteinMapping] = 66;
            mPercentCompleteStartLevels[PERCENT_COMPLETE_LEVEL_COUNT] = 100;
        }

        private bool RenameAndZipInspectFile(string sourceFilePath, string zipFilePath, bool deleteSourceAfterZip)
        {
            // Zip up file specified by sourceFilePath
            // Rename to _inspect.txt before zipping
            var fileInfo = new FileInfo(sourceFilePath);

            if (!fileInfo.Exists)
            {
                LogError("Inspect results file not found; nothing to zip: " + fileInfo.FullName);
                return false;
            }

            var targetFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            if (mDebugLevel >= 3)
            {
                LogDebug("Renaming " + fileInfo.FullName + " to " + targetFilePath);
            }

            fileInfo.MoveTo(targetFilePath);
            fileInfo.Refresh();

            var success = ZipFile(fileInfo.FullName, deleteSourceAfterZip, zipFilePath);

            mJobParams.AddResultFileToKeep(Path.GetFileName(zipFilePath));

            return success;
        }

        /// <summary>
        /// Uses PValue_MinLength5.py to recompute the p-value and FScore values for Inspect results computed in parallel then reassembled
        /// In addition, filters the data on p-value of 0.1 or smaller
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType ReScoreAssembledInspectResults()
        {
            var inspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var filteredFilePath = Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.RunpValue]);

            // Note that RunPValue() will log any errors that occur
            var result = RunPValue(inspectResultsFilePath, filteredFilePath, true, false);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            try
            {
                // Make sure the filtered inspect results file is not zero-length
                // Also, log some stats on the size of the filtered file vs. the original one
                var reScoredFile = new FileInfo(filteredFilePath);
                var originalFile = new FileInfo(inspectResultsFilePath);

                if (!reScoredFile.Exists)
                {
                    LogError("Re-scored Inspect Results file not found: " + reScoredFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (originalFile.Length == 0)
                {
                    // Assembled inspect results file is 0-bytes; this is unexpected
                    LogError("Assembled Inspect Results file is 0 bytes: " + originalFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(
                        "Re-scored Inspect results file created; size is " +
                        (reScoredFile.Length / (float)originalFile.Length * 100).ToString("0.0") + "% of the original (" +
                        reScoredFile.Length + " bytes vs. " + originalFile.Length + " bytes in original)");
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->ReScoreAssembledInspectResults";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPValue(string inspectResultsInputFilePath, string outputFilePath, bool createImageFiles, bool topHitOnly)
        {
            var inspectDir = mMgrParams.GetParam("InspectDir");
            var pValueDistributionFilename = Path.Combine(mWorkDir, mDatasetName + "_PValueDistribution.txt");

            // The following code is only required if you use the -a and -d switches
            //'var orgDbDir = mMgrParams.GetParam("OrgDbDir")
            //'var fastaFilename = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"))
            //'var dbFilename = fastaFilename.Replace("fasta", "trie")

            var pythonProgLoc = mMgrParams.GetParam("PythonProgLoc");

            // Check whether a shuffled DB was created prior to running Inspect
            var shuffledDBUsed = ValidateShuffledDBInUse(inspectResultsInputFilePath);

            // Lookup the p-value to filter on
            var pThresh = mJobParams.GetJobParameter("InspectPvalueThreshold", "0.1");

            var cmdRunner = new RunDosProgram(inspectDir, mDebugLevel);
            RegisterEvents(cmdRunner);

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerInspResultsAssembly.RunPValue(): Enter");
            }

            // verify that python program file exists
            var progLoc = pythonProgLoc;
            if (!File.Exists(progLoc))
            {
                LogError("Cannot find python.exe program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // verify that PValue python script exists
            var pvalueScriptPath = Path.Combine(inspectDir, PVALUE_MINLENGTH5_SCRIPT);
            if (!File.Exists(pvalueScriptPath))
            {
                LogError("Cannot find PValue script: " + pvalueScriptPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Possibly required: Update the PTMods.txt file in InspectDir to contain the modification details, as defined in inspect_input.txt
            UpdatePTModsFile(inspectDir, Path.Combine(mWorkDir, "inspect_input.txt"));

            // Set up and execute a program runner to run PVALUE_MINLENGTH5_SCRIPT.py
            // Note that PVALUE_MINLENGTH5_SCRIPT.py is nearly identical to PValue.py but it retains peptides with 5 amino acids (default is 7)
            // -r is the input file
            // -w is the output file
            // -s saves the p-value distribution to a text file
            // -H means to not remove entries mapped to shuffled proteins (created by shuffleDB.py);
            //    shuffled protein names start with XXX;
            //    we use this option when creating the First Hits file so that we retain the top hit, even if it is from a shuffled protein
            // -p 0.1 will filter out results with p-value <= 0.1 (this threshold was suggested by Sam Payne)
            // -i means to create a PValue distribution image file (.PNG)
            // -S 0.5 means that 50% of the proteins in the DB come from shuffled proteins

            // Other parameters not used:
            // -1 means to only retain the top match for each scan
            // -x means to retain "bad" matches (those with poor delta-score values, a p-value below the threshold, or an MQScore below -1)
            // -a means to perform protein selection (sort of like protein prophet, but not very good, according to Sam Payne)
            // -d .trie file to use (only used if -a is enabled)

            var arguments = " " + pvalueScriptPath +
                            " -r " + inspectResultsInputFilePath +
                            " -w " + outputFilePath +
                            " -s " + pValueDistributionFilename;

            if (createImageFiles)
            {
                arguments += " -i";
            }

            if (topHitOnly)
            {
                arguments += " -H -1 -p 1";
            }
            else
            {
                arguments += " -p " + pThresh;
            }

            if (shuffledDBUsed)
            {
                arguments += " -S 0.5";
            }

            // The following could be used to enable protein selection
            // That would require that the database file be present, and this can take quite a bit longer
            //'arguments += " -a -d " + dbFilename

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "PValue_ConsoleOutput.txt");

            if (!cmdRunner.RunProgram(progLoc, arguments, "PValue", false))
            {
                // Error running program; the error should have already been logged
            }

            if (cmdRunner.ExitCode != 0)
            {
                // Note: Log the non-zero exit code as an error, but return CLOSEOUT_SUCCESS anyway
                LogError(Path.GetFileName(pvalueScriptPath) + " returned a non-zero exit code: " + cmdRunner.ExitCode);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var appFolderPath = Global.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Inspect Results Assembly Plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerInspResultsAssemblyPlugIn"))
            {
                return false;
            }

            // Store version information for the PeptideToProteinMapEngine and its associated DLLs
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, Path.Combine(appFolderPath, "PeptideToProteinMapEngine.dll"));
            if (!success)
                return false;

            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, Path.Combine(appFolderPath, "ProteinFileReader.dll"));
            if (!success)
                return false;

            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, Path.Combine(appFolderPath, "System.Data.SQLite.dll"));
            if (!success)
                return false;

            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, Path.Combine(appFolderPath, "ProteinCoverageSummarizer.dll"));
            if (!success)
                return false;

            // Store the path to important DLLs in toolFiles
            // Skip System.Data.SQLite.dll; we don't need to track the file date
            var toolFiles = new List<FileInfo>
            {
                new(Path.Combine(appFolderPath, "AnalysisManagerInspResultsAssemblyPlugIn.dll")),
                new(Path.Combine(appFolderPath, "PeptideToProteinMapEngine.dll")),
                new(Path.Combine(appFolderPath, "ProteinFileReader.dll")),
                new(Path.Combine(appFolderPath, "ProteinCoverageSummarizer.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Assures that the PTMods.txt file in inspectDirectoryPath contains the modification info defined in inspectInputFilePath
        /// Note: We run the risk that two InspectResultsAssembly tasks will run simultaneously and will both try to update PTMods.txt
        /// </summary>
        /// <param name="inspectDirectoryPath"></param>
        /// <param name="inspectParameterFilePath"></param>
        private bool UpdatePTModsFile(string inspectDirectoryPath, string inspectParameterFilePath)
        {
            var prevLineWasBlank = false;

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Enter ");
                }

                // Read the mods defined in inspectInputFilePath
                if (ExtractModInfoFromInspectParamFile(inspectParameterFilePath, out var modList))
                {
                    if (modList.Count > 0)
                    {
                        // Initialize modProcessed()
                        var modProcessed = new bool[modList.Count];

                        // Read PTMods.txt to look for the mods in modList
                        // While reading, will create a new file with any required updates
                        // In case two managers are doing this simultaneously, we'll put a unique string in pTModsFilePathNew

                        var ptModsFilePath = Path.Combine(inspectDirectoryPath, "PTMods.txt");
                        var ptModsFilePathNew = ptModsFilePath + ".Job" + mJob + ".tmp";

                        if (mDebugLevel > 4)
                        {
                            LogDebug("AnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Open " + ptModsFilePath);
                            LogDebug("AnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Create " + ptModsFilePathNew);
                        }

                        var differenceFound = false;

                        using (var reader = new StreamReader(new FileStream(ptModsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        using (var writer = new StreamWriter(new FileStream(ptModsFilePathNew, FileMode.Create, FileAccess.Write, FileShare.Read)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();
                                if (string.IsNullOrWhiteSpace(dataLine))
                                    continue;

                                var trimmedLine = dataLine.Trim();

                                if (!string.IsNullOrEmpty(trimmedLine))
                                {
                                    if (trimmedLine[0] == '#')
                                    {
                                        // Comment line; skip it
                                    }
                                    else
                                    {
                                        // Split the line on tabs
                                        var splitLine = trimmedLine.Split('\t');

                                        if (splitLine.Length >= 3)
                                        {
                                            var modName = splitLine[0].ToLower();

                                            var matchFound = false;

                                            int index;
                                            for (index = 0; index < modList.Count; index++)
                                            {
                                                if (modList[index].ModName.ToLower() == modName)
                                                {
                                                    // Match found
                                                    matchFound = true;
                                                    break;
                                                }
                                            }

                                            if (matchFound)
                                            {
                                                if (modProcessed[index])
                                                {
                                                    // This mod was already processed; don't write the line out again
                                                    trimmedLine = string.Empty;
                                                }
                                                else
                                                {
                                                    var mod = modList[index];
                                                    // First time we've seen this mod; make sure the mod mass and residues are correct
                                                    if (splitLine[1] != mod.ModMass || splitLine[2] != mod.Residues)
                                                    {
                                                        // Mis-match; update the line
                                                        trimmedLine = mod.ModName + "\t" + mod.ModMass + "\t" + mod.Residues;

                                                        if (mDebugLevel > 4)
                                                        {
                                                            LogDebug(
                                                                "AnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Mod def in PTMods.txt doesn't match required mod def; updating to: " +
                                                                trimmedLine);
                                                        }

                                                        differenceFound = true;
                                                    }
                                                    modProcessed[index] = true;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (prevLineWasBlank && trimmedLine.Length == 0)
                                {
                                    // Don't write out two blank lines in a row; skip this line
                                }
                                else
                                {
                                    writer.WriteLine(trimmedLine);

                                    prevLineWasBlank = trimmedLine.Length == 0;
                                }
                            }

                            // Look for any unprocessed mods
                            for (var index = 0; index < modList.Count; index++)
                            {
                                if (!modProcessed[index])
                                {
                                    var mod = modList[index];
                                    var dataLine = mod.ModName + "\t" + mod.ModMass + "\t" + mod.Residues;
                                    writer.WriteLine(dataLine);

                                    differenceFound = true;
                                }
                            }
                        } // end using

                        if (differenceFound)
                        {
                            // Replace PTMods.txt with pTModsFilePathNew

                            try
                            {
                                var ptModsFilePathOld = ptModsFilePath + ".old";
                                if (File.Exists(ptModsFilePathOld))
                                {
                                    File.Delete(ptModsFilePathOld);
                                }

                                File.Move(ptModsFilePath, ptModsFilePathOld);
                                File.Move(ptModsFilePathNew, ptModsFilePath);
                            }
                            catch (Exception ex)
                            {
                                LogError("AnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile, " +
                                         "Error swapping in the new PTMods.txt file : " + mJob + "; " + ex.Message);
                                return false;
                            }
                        }
                        else
                        {
                            // No difference was found; delete the .tmp file
                            try
                            {
                                File.Delete(ptModsFilePathNew);
                            }
                            catch (Exception)
                            {
                                // Ignore errors here
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->UpdatePTModsFile";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ValidateShuffledDBInUse(string inspectResultsPath)
        {
            var sepChars = new[] { '\t' };

            var shuffledDBUsed = mJobParams.GetJobParameter("InspectUsesShuffledDB", false);

            if (!shuffledDBUsed)
                return false;

            // Open the _inspect.txt file and make sure proteins exist that start with XXX
            // If not, change shuffledDBUsed back to false

            try
            {
                var decoyProteinCount = 0;

                using (var reader = new StreamReader(new FileStream(inspectResultsPath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        // Protein info should be stored in the fourth column (index=3)
                        var splitLine = dataLine.Split(sepChars, 5);

                        if (linesRead == 1)
                        {
                            // Verify that splitLine[3] is "Protein"
                            if (!splitLine[3].StartsWith("protein", StringComparison.OrdinalIgnoreCase))
                            {
                                LogWarning("The fourth column in the Inspect results file does not start with 'Protein'; this is unexpected");
                            }
                        }
                        else
                        {
                            if (splitLine[3].StartsWith("XXX"))
                            {
                                decoyProteinCount++;
                            }
                        }

                        if (decoyProteinCount > 10)
                            break;
                    }
                }

                if (decoyProteinCount == 0)
                {
                    LogWarning(
                        "The job has 'InspectUsesShuffledDB' set to True in the Settings file, but none of the proteins in the result file starts with XXX. " +
                        "Will assume the FASTA file did NOT have shuffled proteins, and will thus NOT use '-S 0.5' when calling " + PVALUE_MINLENGTH5_SCRIPT);
                    shuffledDBUsed = false;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->inspectResultsPath";
                LogError(mMessage + ": " + ex.Message);
            }

            return shuffledDBUsed;
        }

        /// <summary>
        /// Stores the _inspect.txt file in _inspect_all.zip
        /// Stores the _inspect_fht.txt file in _inspect_fht.zip (but renames it to _inspect.txt before storing)
        /// Stores the _inspect_filtered.txt file in _inspect.zip (but renames it to _inspect.txt before storing)
        /// </summary>
        private CloseOutType ZipInspectResults()
        {
            try
            {
                UpdateStatusRunning(mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.ZipInspectResults]);

                // Zip up the _inspect.txt file into _inspect_all.zip
                // Rename to _inspect.txt before zipping
                // Delete the _inspect.txt file after zipping
                var success = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + ORIGINAL_INSPECT_FILE_SUFFIX),
                                                         Path.Combine(mWorkDir, mDatasetName + "_inspect_all.zip"), true);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Zip up the _inspect_fht.txt file into _inspect_fht.zip
                // Rename to _inspect.txt before zipping
                // Delete the _inspect.txt file after zipping
                success = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + FIRST_HITS_INSPECT_FILE_SUFFIX),
                    Path.Combine(mWorkDir, mDatasetName + "_inspect_fht.zip"), true);

                if (!success)
                {
                    // Ignore errors creating the _fht.zip file
                }

                // Zip up the _inspect_filtered.txt file into _inspect.zip
                // Rename to _inspect.txt before zipping
                // Do not delete the _inspect.txt file after zipping since it is used in function CreatePeptideToProteinMapping
                success = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX),
                    Path.Combine(mWorkDir, mDatasetName + "_inspect.zip"), false);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Add the _inspect.txt file to .FilesToDelete since we only want to keep the Zipped version
                mJobParams.AddResultFileToSkip(mInspectResultsFileName);
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->ZipInspectResults";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void PeptideToProteinMapper_ProgressChanged(string taskDescription, float percentComplete)
        {
            // Note that percentComplete is a value between 0 and 100

            var startPercent = mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.CreatePeptideToProteinMapping];
            var endPercent = mPercentCompleteStartLevels[(int)InspectResultsProcessingSteps.CreatePeptideToProteinMapping + 1];

            var percentCompleteEffective = startPercent + (float)(percentComplete / 100.0 * (endPercent - startPercent));

            UpdateStatusFile(percentCompleteEffective);

            LogProgress("Mapping peptides to proteins", 3);
        }
    }
}
