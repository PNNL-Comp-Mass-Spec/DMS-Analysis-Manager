//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/29/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using PeptideToProteinMapEngine;
using PRISM;

namespace AnalysisManagerInspResultsAssemblyPlugIn
{
    /// <summary>
    /// Class for running Inspect Results Assembler
    /// </summary>
    public class clsAnalysisToolRunnerInspResultsAssembly : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

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
        public enum eInspectResultsProcessingSteps
        {
            Starting = 0,
            AssembleResults = 1,
            RunpValue = 2,
            ZipInspectResults = 3,
            CreatePeptideToProteinMapping = 4
        }

        #endregion

        #region "Structures"

        protected struct udtModInfoType
        {
            public string ModName;
            public string ModMass;             // Storing as a string since reading from a text file and writing to a text file
            public string Residues;
        }

        #endregion

        #region "Module Variables"

        public const string INSPECT_INPUT_PARAMS_FILENAME = "inspect_input.txt";

        protected string mInspectResultsFileName;

        protected string mInspectSearchLogFilePath = "InspectSearchLog.txt";      // This value gets updated in function RunInSpecT

        // Note that clsPeptideToProteinMapEngine utilizes System.Data.SQLite.dll
        private clsPeptideToProteinMapEngine mPeptideToProteinMapper;

        // mPercentCompleteStartLevels is an array that lists the percent complete value to report
        //  at the start of each of the various processing steps performed in this procedure
        // The percent complete values range from 0 to 100
        const int PERCENT_COMPLETE_LEVEL_COUNT = 5;
        protected float[] mPercentCompleteStartLevels;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Initializes classwide variables</remarks>
        public clsAnalysisToolRunnerInspResultsAssembly()
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

            // We no longer need to index the .Fasta file (since we're no longer using PValue.py with the -a switch or Summary.py

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerInspResultsAssembly.RunTool(): Enter");
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
                    else if (pepToProteinMappingResult != CloseOutType.CLOSEOUT_SUCCESS && pepToProteinMappingResult != CloseOutType.CLOSEOUT_NO_DATA)
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
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
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
        /// <remarks></remarks>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsSummaryFile summaryFile, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, summaryFile, myEMSLUtilities);

            mInspectResultsFileName = mDatasetName + ORIGINAL_INSPECT_FILE_SUFFIX;
        }

        private CloseOutType AssembleResults(int intNumResultFiles)
        {
            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect result files");
                }

                UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.AssembleResults]);

                // Combine the individual _xx_inspect.txt files to create the single _inspect.txt file
                var result = AssembleFiles(mInspectResultsFileName, ResultFileType.INSPECT_RESULT, intNumResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect error files");
                }

                var strFileName = mDatasetName + "_error.txt";
                result = AssembleFiles(strFileName, ResultFileType.INSPECT_ERROR, intNumResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(strFileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect search log files");
                }

                strFileName = "InspectSearchLog.txt";
                result = AssembleFiles(strFileName, ResultFileType.INSPECT_SEARCH, intNumResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(strFileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Assembling parallelized inspect console output files");
                }

                strFileName = "InspectConsoleOutput.txt";
                result = AssembleFiles(strFileName, ResultFileType.INSPECT_CONSOLE, intNumResultFiles);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
                mJobParams.AddResultFileToKeep(strFileName);

                // FilterInspectResultsByFirstHits will create file _inspect_fht.txt
                result = FilterInspectResultsByFirstHits();

                // Rescore the assembled inspect results using PValue_MinLength5.py (which is similar to PValue.py but retains peptides of length 5 or greater)
                // This will create files _inspect_fht.txt and _inspect_filtered.txt
                result = RescoreAssembledInspectResults();
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
        /// <remarks></remarks>
        private CloseOutType AssembleFiles(string strCombinedFileName, ResultFileType resFileType, int intNumResultFiles)
        {
            var inspectResultsFile = "";

            var blnFilesContainHeaderLine = false;
            var blnHeaderLineWritten = false;
            var blnAddSegmentNumberToEachLine = false;
            var blnAddBlankLineBetweenFiles = false;

            try
            {
                var DatasetName = mDatasetName;

                var writer = CreateNewExportFile(Path.Combine(mWorkDir, strCombinedFileName));
                if (writer == null)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                for (var fileNameCounter = 1; fileNameCounter <= intNumResultFiles; fileNameCounter++)
                {
                    var error = false;
                    switch (resFileType)
                    {
                        case ResultFileType.INSPECT_RESULT:
                            inspectResultsFile = DatasetName + "_" + fileNameCounter + ORIGINAL_INSPECT_FILE_SUFFIX;
                            blnFilesContainHeaderLine = true;
                            blnAddSegmentNumberToEachLine = false;
                            blnAddBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_ERROR:
                            inspectResultsFile = DatasetName + "_" + fileNameCounter + "_error.txt";
                            blnFilesContainHeaderLine = false;
                            blnAddSegmentNumberToEachLine = true;
                            blnAddBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_SEARCH:
                            inspectResultsFile = "InspectSearchLog_" + fileNameCounter + ".txt";
                            blnFilesContainHeaderLine = true;
                            blnAddSegmentNumberToEachLine = true;
                            blnAddBlankLineBetweenFiles = false;

                            break;
                        case ResultFileType.INSPECT_CONSOLE:
                            inspectResultsFile = "InspectConsoleOutput_" + fileNameCounter + ".txt";
                            blnFilesContainHeaderLine = false;
                            blnAddSegmentNumberToEachLine = false;
                            blnAddBlankLineBetweenFiles = true;

                            break;
                        default:
                            // Unknown ResultFileType
                            LogError("clsAnalysisToolRunnerInspResultsAssembly->AssembleFiles: Unknown Inspect Result File Type: " + resFileType);
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
                        linesRead += 1;

                        if (linesRead == 1)
                        {
                            if (blnFilesContainHeaderLine)
                            {
                                // Handle the header line
                                if (!blnHeaderLineWritten)
                                {
                                    if (blnAddSegmentNumberToEachLine)
                                    {
                                        dataLine = "Segment\t" + dataLine;
                                    }
                                    writer.WriteLine(dataLine);
                                }
                            }
                            else
                            {
                                if (blnAddSegmentNumberToEachLine)
                                {
                                    if (!blnHeaderLineWritten)
                                    {
                                        writer.WriteLine("Segment\t" + "Message");
                                    }
                                    writer.WriteLine(fileNameCounter + "\t" + dataLine);
                                }
                                else
                                {
                                    writer.WriteLine(dataLine);
                                }
                            }
                            blnHeaderLineWritten = true;
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
                                        // Note: .LastIndexOf will start at index intTabIndex and search backwards until the first match is found
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

                            if (blnAddSegmentNumberToEachLine)
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

                    if (blnAddBlankLineBetweenFiles)
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
                LogError("clsAnalysisToolRunnerInspResultsAssembly->createNewExportFile: Export file already exists " +
                         "(" + exportFilePath + "); this is unexpected");
                return null;
            }

            return new StreamWriter(new FileStream(exportFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
        }

        private CloseOutType CreatePeptideToProteinMapping()
        {
            var orgDbDir = mMgrParams.GetParam("OrgDbDir");

            // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
            var dbFilename = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

            var ignorePeptideToProteinMapperErrors = false;

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.CreatePeptideToProteinMapping]);

            var strInputFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);

            try
            {
                // Validate that the input file has at least one entry; if not, no point in continuing
                var linesRead = 0;

                using (var reader = new StreamReader(new FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!reader.EndOfStream && linesRead < 10)
                    {
                        var dataLine = reader.ReadLine();
                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            linesRead += 1;
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

                mPeptideToProteinMapper = new clsPeptideToProteinMapEngine();

                RegisterEvents(mPeptideToProteinMapper);
                mPeptideToProteinMapper.ProgressUpdate -= ProgressUpdateHandler;
                mPeptideToProteinMapper.ProgressUpdate += PeptideToProteinMapper_ProgressChanged;

                mPeptideToProteinMapper.DeleteInspectTempFiles = true;
                mPeptideToProteinMapper.IgnoreILDifferences = false;
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

                mPeptideToProteinMapper.MatchPeptidePrefixAndSuffixToProtein = false;
                mPeptideToProteinMapper.OutputProteinSequence = false;
                mPeptideToProteinMapper.PeptideInputFileFormat = clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.InspectResultsFile;
                mPeptideToProteinMapper.PeptideFileSkipFirstLine = false;
                mPeptideToProteinMapper.ProteinInputFilePath = Path.Combine(orgDbDir, dbFilename);
                mPeptideToProteinMapper.SaveProteinToPeptideMappingFile = true;
                mPeptideToProteinMapper.SearchAllProteinsForPeptideSequence = true;
                mPeptideToProteinMapper.SearchAllProteinsSkipCoverageComputationSteps = true;

                var blnSuccess = mPeptideToProteinMapper.ProcessFile(strInputFilePath, mWorkDir, string.Empty, true);

                mPeptideToProteinMapper.CloseLogFileNow();

                if (blnSuccess)
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

                LogError("clsAnalysisToolRunnerInspResultsAssembly.CreatePeptideToProteinMapping, Error running the PeptideToProteinMapEngine, job " +
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
        /// Reads the modification information defined in inspectParameterFilePath, storing it in udtModList
        /// </summary>
        /// <param name="inspectParameterFilePath"></param>
        /// <param name="udtModList"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ExtractModInfoFromInspectParamFile(string inspectParameterFilePath, ref udtModInfoType[] udtModList)
        {
            try
            {
                // Initialize udtModList
                var modCount = 0;
                udtModList = new udtModInfoType[-1 + 1];

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerInspResultsAssembly.ExtractModInfoFromInspectParamFile(): Reading " + inspectParameterFilePath);
                }

                // Read the contents of strProteinToPeptideMappingFilePath
                using (var reader = new StreamReader((new FileStream(inspectParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))))
                {
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

                        if (dataLineTrimmed.ToLower().StartsWith("mod"))
                        {
                            // Modification definition line

                            // Split the line on commas
                            var strSplitLine = dataLineTrimmed.Split(',');

                            if (strSplitLine.Length >= 5 && strSplitLine[0].ToLower().Trim() == "mod")
                            {
                                if (udtModList.Length == 0)
                                {
                                    udtModList = new udtModInfoType[1];
                                }
                                else if (modCount >= udtModList.Length)
                                {
                                    Array.Resize(ref udtModList, udtModList.Length * 2);
                                }

                                // var mod = udtModList[intModCount];
                                // mod.ModName = strSplitLine[4];
                                // mod.ModMass = strSplitLine[1];
                                // mod.Residues = strSplitLine[2];

                                modCount += 1;
                            }
                        }
                    }

                    // Shrink udtModList to the appropriate length
                    Array.Resize(ref udtModList, modCount);
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
        /// <remarks></remarks>
        private CloseOutType FilterInspectResultsByFirstHits()
        {
            var strInspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var strFilteredFilePath = Path.Combine(mWorkDir, mDatasetName + FIRST_HITS_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.RunpValue]);

            // Note that RunPvalue() will log any errors that occur
            var eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, false, true);

            return eResult;
        }

        /// <summary>
        /// Filters the inspect results using PValue_MinLength5.py"
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType FilterInspectResultsByPValue()
        {
            var strInspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var strFilteredFilePath = Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.RunpValue]);

            // Note that RunPvalue() will log any errors that occur
            var eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, true, false);

            return eResult;
        }

        protected void InitializeVariables()
        {
            // Define the percent complete values to use for the start of each processing step

            mPercentCompleteStartLevels = new float[PERCENT_COMPLETE_LEVEL_COUNT + 1];

            mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.Starting] = 0;
            mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.AssembleResults] = 5;
            mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.RunpValue] = 10;
            mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.ZipInspectResults] = 65;
            mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.CreatePeptideToProteinMapping] = 66;
            mPercentCompleteStartLevels[PERCENT_COMPLETE_LEVEL_COUNT] = 100;
        }

        private bool RenameAndZipInspectFile(string strSourceFilePath, string strZipFilePath, bool blnDeleteSourceAfterZip)
        {
            // Zip up file specified by strSourceFilePath
            // Rename to _inspect.txt before zipping
            var fiFileInfo = new FileInfo(strSourceFilePath);

            if (!fiFileInfo.Exists)
            {
                LogError("Inspect results file not found; nothing to zip: " + fiFileInfo.FullName);
                return false;
            }

            var strTargetFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            if (mDebugLevel >= 3)
            {
                LogDebug("Renaming " + fiFileInfo.FullName + " to " + strTargetFilePath);
            }

            fiFileInfo.MoveTo(strTargetFilePath);
            fiFileInfo.Refresh();

            var blnSuccess = ZipFile(fiFileInfo.FullName, blnDeleteSourceAfterZip, strZipFilePath);

            mJobParams.AddResultFileToKeep(Path.GetFileName(strZipFilePath));

            return blnSuccess;
        }

        /// <summary>
        /// Uses PValue_MinLength5.py to recompute the p-value and FScore values for Inspect results computed in parallel then reassembled
        /// In addition, filters the data on p-value of 0.1 or smaller
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType RescoreAssembledInspectResults()
        {
            var strInspectResultsFilePath = Path.Combine(mWorkDir, mInspectResultsFileName);
            var strFilteredFilePath = Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX);

            UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.RunpValue]);

            // Note that RunPvalue() will log any errors that occur
            var eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, true, false);

            try
            {
                // Make sure the filtered inspect results file is not zero-length
                // Also, log some stats on the size of the filtered file vs. the original one
                var fiRescoredFile = new FileInfo(strFilteredFilePath);
                var fiOriginalFile = new FileInfo(strInspectResultsFilePath);

                if (!fiRescoredFile.Exists)
                {
                    LogError("Rescored Inspect Results file not found: " + fiRescoredFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (fiOriginalFile.Length == 0)
                {
                    // Assembled inspect results file is 0-bytes; this is unexpected
                    LogError("Assembled Inspect Results file is 0 bytes: " + fiOriginalFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(
                        "Rescored Inspect results file created; size is " +
                        (fiRescoredFile.Length / (float)fiOriginalFile.Length * 100).ToString("0.0") + "% of the original (" +
                        fiRescoredFile.Length + " bytes vs. " + fiOriginalFile.Length + " bytes in original)");
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->RescoreAssembledInspectResults";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunpValue(string inspectResultsInputFilePath, string outputFilePath, bool createImageFiles, bool topHitOnly)
        {
            var InspectDir = mMgrParams.GetParam("inspectdir");
            var pvalDistributionFilename = Path.Combine(mWorkDir, mDatasetName + "_PValueDistribution.txt");

            // The following code is only required if you use the -a and -d switches
            //'var orgDbDir = mMgrParams.GetParam("OrgDbDir")
            //'var fastaFilename = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"))
            //'var dbFilename = fastaFilename.Replace("fasta", "trie")

            var pythonProgLoc = mMgrParams.GetParam("pythonprogloc");

            // Check whether a shuffled DB was created prior to running Inspect
            var blnShuffledDBUsed = ValidateShuffledDBInUse(inspectResultsInputFilePath);

            // Lookup the p-value to filter on
            var pthresh = mJobParams.GetJobParameter("InspectPvalueThreshold", "0.1");

            var cmdRunner = new clsRunDosProgram(InspectDir, mDebugLevel);
            RegisterEvents(cmdRunner);

            if (mDebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerInspResultsAssembly.RunpValue(): Enter");
            }

            // verify that python program file exists
            var progLoc = pythonProgLoc;
            if (!File.Exists(progLoc))
            {
                LogError("Cannot find python.exe program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // verify that PValue python script exists
            var pvalueScriptPath = Path.Combine(InspectDir, PVALUE_MINLENGTH5_SCRIPT);
            if (!File.Exists(pvalueScriptPath))
            {
                LogError("Cannot find PValue script: " + pvalueScriptPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Possibly required: Update the PTMods.txt file in InspectDir to contain the modification details, as defined in inspect_input.txt
            UpdatePTModsFile(InspectDir, Path.Combine(mWorkDir, "inspect_input.txt"));

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

            var cmdStr = " " + pvalueScriptPath + " -r " + inspectResultsInputFilePath + " -w " + outputFilePath + " -s " + pvalDistributionFilename;

            if (createImageFiles)
            {
                cmdStr += " -i";
            }

            if (topHitOnly)
            {
                cmdStr += " -H -1 -p 1";
            }
            else
            {
                cmdStr += " -p " + pthresh;
            }

            if (blnShuffledDBUsed)
            {
                cmdStr += " -S 0.5";
            }

            // The following could be used to enable protein selection
            // That would require that the database file be present, and this can take quite a bit longer
            //'cmdStr += " -a -d " + dbFilename

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "PValue_ConsoleOutput.txt");

            if (!cmdRunner.RunProgram(progLoc, cmdStr, "PValue", false))
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
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;
            var strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Inspect Results Assembly Plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerInspResultsAssemblyPlugIn"))
            {
                return false;
            }

            // Store version information for the PeptideToProteinMapEngine and its associated DLLs
            var blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, Path.Combine(strAppFolderPath, "PeptideToProteinMapEngine.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, Path.Combine(strAppFolderPath, "ProteinFileReader.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, Path.Combine(strAppFolderPath, "System.Data.SQLite.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, Path.Combine(strAppFolderPath, "ProteinCoverageSummarizer.dll"));
            if (!blnSuccess)
                return false;

            // Store the path to important DLLs in toolFiles
            // Skip System.Data.SQLite.dll; we don't need to track the file date
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerInspResultsAssemblyPlugIn.dll")),
                new FileInfo(Path.Combine(strAppFolderPath, "PeptideToProteinMapEngine.dll")),
                new FileInfo(Path.Combine(strAppFolderPath, "ProteinFileReader.dll")),
                new FileInfo(Path.Combine(strAppFolderPath, "ProteinCoverageSummarizer.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Assures that the PTMods.txt file in inspectDirectoryPath contains the modification info defined in strInspectInputFilePath
        /// Note: We run the risk that two InspectResultsAssembly tasks will run simultaneously and will both try to update PTMods.txt
        /// </summary>
        /// <param name="inspectDirectoryPath"></param>
        /// <param name="inspectParameterFilePath"></param>
        /// <remarks></remarks>
        private bool UpdatePTModsFile(string inspectDirectoryPath, string inspectParameterFilePath)
        {
            var udtModList = new udtModInfoType[-1 + 1];

            var blnPrevLineWasBlank = false;

            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Enter ");
                }

                // Read the mods defined in strInspectInputFilePath
                if (ExtractModInfoFromInspectParamFile(inspectParameterFilePath, ref udtModList))
                {
                    if (udtModList.Length > 0)
                    {
                        // Initialize blnModProcessed()
                        var blnModProcessed = new bool[udtModList.Length];

                        // Read PTMods.txt to look for the mods in udtModList
                        // While reading, will create a new file with any required updates
                        // In case two managers are doing this simultaneously, we'll put a unique string in strPTModsFilePathNew

                        var strPTModsFilePath = Path.Combine(inspectDirectoryPath, "PTMods.txt");
                        var strPTModsFilePathNew = strPTModsFilePath + ".Job" + mJob + ".tmp";

                        if (mDebugLevel > 4)
                        {
                            LogDebug("clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Open " + strPTModsFilePath);
                            LogDebug("clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Create " + strPTModsFilePathNew);
                        }

                        var blnDifferenceFound = false;

                        using (var reader = new StreamReader(new FileStream(strPTModsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        using (var writer = new StreamWriter(new FileStream(strPTModsFilePathNew, FileMode.Create, FileAccess.Write, FileShare.Read)))
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
                                        var strSplitLine = trimmedLine.Split('\t');

                                        if (strSplitLine.Length >= 3)
                                        {
                                            var strModName = strSplitLine[0].ToLower();

                                            var blnMatchFound = false;

                                            int intIndex;
                                            for (intIndex = 0; intIndex <= udtModList.Length - 1; intIndex++)
                                            {
                                                if (udtModList[intIndex].ModName.ToLower() == strModName)
                                                {
                                                    // Match found
                                                    blnMatchFound = true;
                                                    break;
                                                }
                                            }

                                            if (blnMatchFound)
                                            {
                                                if (blnModProcessed[intIndex])
                                                {
                                                    // This mod was already processed; don't write the line out again
                                                    trimmedLine = string.Empty;
                                                }
                                                else
                                                {
                                                    var mod = udtModList[intIndex];
                                                    // First time we've seen this mod; make sure the mod mass and residues are correct
                                                    if (strSplitLine[1] != mod.ModMass || strSplitLine[2] != mod.Residues)
                                                    {
                                                        // Mis-match; update the line
                                                        trimmedLine = mod.ModName + "\t" + mod.ModMass + "\t" + mod.Residues;

                                                        if (mDebugLevel > 4)
                                                        {
                                                            LogDebug(
                                                                "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Mod def in PTMods.txt doesn't match required mod def; updating to: " +
                                                                trimmedLine);
                                                        }

                                                        blnDifferenceFound = true;
                                                    }
                                                    blnModProcessed[intIndex] = true;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (blnPrevLineWasBlank && trimmedLine.Length == 0)
                                {
                                    // Don't write out two blank lines in a row; skip this line
                                }
                                else
                                {
                                    writer.WriteLine(trimmedLine);

                                    if (trimmedLine.Length == 0)
                                    {
                                        blnPrevLineWasBlank = true;
                                    }
                                    else
                                    {
                                        blnPrevLineWasBlank = false;
                                    }
                                }
                            }

                            // Look for any unprocessed mods
                            for (var intIndex = 0; intIndex <= udtModList.Length - 1; intIndex++)
                            {
                                if (!blnModProcessed[intIndex])
                                {
                                    var mod = udtModList[intIndex];
                                    var dataLine = mod.ModName + "\t" + mod.ModMass + "\t" + mod.Residues;
                                    writer.WriteLine(dataLine);

                                    blnDifferenceFound = true;
                                }
                            }
                        } // end using

                        if (blnDifferenceFound)
                        {
                            // Replace PTMods.txt with strPTModsFilePathNew

                            try
                            {
                                var strPTModsFilePathOld = strPTModsFilePath + ".old";
                                if (File.Exists(strPTModsFilePathOld))
                                {
                                    File.Delete(strPTModsFilePathOld);
                                }

                                File.Move(strPTModsFilePath, strPTModsFilePathOld);
                                File.Move(strPTModsFilePathNew, strPTModsFilePath);
                            }
                            catch (Exception ex)
                            {
                                LogError("clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile, " +
                                         "Error swapping in the new PTMods.txt file : " + mJob + "; " + ex.Message);
                                return false;
                            }
                        }
                        else
                        {
                            // No difference was found; delete the .tmp file
                            try
                            {
                                File.Delete(strPTModsFilePathNew);
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
            var chSepChars = new[] { '\t' };

            var shuffledDBUsed = mJobParams.GetJobParameter("InspectUsesShuffledDB", false);

            if (!shuffledDBUsed)
                return false;

            // Open the _inspect.txt file and make sure proteins exist that start with XXX
            // If not, change blnShuffledDBUsed back to false

            try
            {
                var decoyProteinCount = 0;

                using (var reader = new StreamReader(new FileStream(inspectResultsPath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {

                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        // Protein info should be stored in the fourth column (index=3)
                        var strSplitLine = dataLine.Split(chSepChars, 5);

                        if (linesRead == 1)
                        {
                            // Verify that strSplitLine[3] is "Protein"
                            if (!strSplitLine[3].ToLower().StartsWith("protein"))
                            {
                                LogWarning("The fourth column in the Inspect results file does not start with 'Protein'; this is unexpected");
                            }
                        }
                        else
                        {
                            if (strSplitLine[3].StartsWith("XXX"))
                            {
                                decoyProteinCount += 1;
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
                        "Will assume the fasta file did NOT have shuffled proteins, and will thus NOT use '-S 0.5' when calling " + PVALUE_MINLENGTH5_SCRIPT);
                    shuffledDBUsed = false;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectResultsAssembly->strInspectResultsPath";
                LogError(mMessage + ": " + ex.Message);
            }

            return shuffledDBUsed;
        }

        /// <summary>
        /// Stores the _inspect.txt file in _inspect_all.zip
        /// Stores the _inspect_fht.txt file in _inspect_fht.zip (but renames it to _inspect.txt before storing)
        /// Stores the _inspect_filtered.txt file in _inspect.zip (but renames it to _inspect.txt before storing)
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType ZipInspectResults()
        {
            try
            {
                UpdateStatusRunning(mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.ZipInspectResults]);

                // Zip up the _inspect.txt file into _inspect_all.zip
                // Rename to _inspect.txt before zipping
                // Delete the _inspect.txt file after zipping
                var blnSuccess = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + ORIGINAL_INSPECT_FILE_SUFFIX),
                                                         Path.Combine(mWorkDir, mDatasetName + "_inspect_all.zip"), true);

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Zip up the _inspect_fht.txt file into _inspect_fht.zip
                // Rename to _inspect.txt before zipping
                // Delete the _inspect.txt file after zipping
                blnSuccess = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + FIRST_HITS_INSPECT_FILE_SUFFIX),
                    Path.Combine(mWorkDir, mDatasetName + "_inspect_fht.zip"), true);

                if (!blnSuccess)
                {
                    // Ignore errors creating the _fht.zip file
                }

                // Zip up the _inspect_filtered.txt file into _inspect.zip
                // Rename to _inspect.txt before zipping
                // Do not delete the _inspect.txt file after zipping since it is used in function CreatePeptideToProteinMapping
                blnSuccess = RenameAndZipInspectFile(Path.Combine(mWorkDir, mDatasetName + FILTERED_INSPECT_FILE_SUFFIX),
                    Path.Combine(mWorkDir, mDatasetName + "_inspect.zip"), false);

                if (!blnSuccess)
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

        #endregion

        #region "Event Handlers"

        private void PeptideToProteinMapper_ProgressChanged(string taskDescription, float percentComplete)
        {
            // Note that percentComplete is a value between 0 and 100

            var startPercent = mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.CreatePeptideToProteinMapping];
            var endPercent = mPercentCompleteStartLevels[(int)eInspectResultsProcessingSteps.CreatePeptideToProteinMapping + 1];

            var percentCompleteEffective = startPercent + (float)(percentComplete / 100.0 * (endPercent - startPercent));

            UpdateStatusFile(percentCompleteEffective);

            LogProgress("Mapping peptides to proteins", 3);
        }

        #endregion
    }
}
