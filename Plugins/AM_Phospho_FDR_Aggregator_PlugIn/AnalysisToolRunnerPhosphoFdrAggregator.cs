//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using AnalysisManagerBase;
using Mage;
using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPhospho_FDR_AggregatorPlugIn
{
    /// <summary>
    /// Class for running Phospho_FDRAggregator analysis
    /// </summary>
    public class AnalysisToolRunnerPhosphoFdrAggregator : AnalysisToolRunnerBase
    {
        // Ignore Spelling: cid, Da, etd, hcd, fht, xt

        #region "Constants and Enums"

        protected const string ASCORE_CONSOLE_OUTPUT_PREFIX = "AScore_ConsoleOutput";

        protected const string FILE_SUFFIX_ASCORE_RESULTS = "_ascore.txt";
        protected const string FILE_SUFFIX_SYN_PLUS_ASCORE = "_plus_ascore.txt";

        protected const float PROGRESS_PCT_PHOSPHO_FDR_RUNNING = 5;
        protected const float PROGRESS_PCT_PHOSPHO_FDR_COMPLETE = 99;

        protected enum DatasetTypeConstants
        {
            Unknown = 0,
            CID = 1,
            ETD = 2,
            HCD = 3
        }

        #endregion

        #region "Structures"

        protected struct JobMetadataForAScore
        {
            public int Job;
            public string Dataset;
            public string ToolName;
            public string FirstHitsFilePath;
            public string SynopsisFilePath;
            public string ToolNameForAScore;
            public string SpectrumFilePath;
        }

        #endregion

        #region "Module Variables"

        protected string mConsoleOutputErrorMsg;

        protected int mJobFoldersProcessed;
        protected int mTotalJobFolders;

        protected RunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs PhosphoFdrAggregator tool
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
                    LogDebug("AnalysisToolRunnerPhosphoFdrAggregator.RunTool(): Enter");
                }

                // Determine the path to the AScore program
                // AScoreProgLoc will be something like this: "C:\DMS_Programs\AScore\AScore_Console.exe"
                var progLocAScore = mMgrParams.GetParam("AScoreProgLoc");
                if (!File.Exists(progLocAScore))
                {
                    if (string.IsNullOrWhiteSpace(progLocAScore))
                        progLocAScore = "Parameter 'AScoreProgLoc' not defined for this manager";
                    mMessage = "Cannot find AScore program file";
                    LogError(mMessage + ": " + progLocAScore);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AScore version info in the database
                if (!StoreToolVersionInfo(progLocAScore))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining AScore version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run AScore for each of the jobs in the data package
                var processingSuccess = ProcessSynopsisFiles(progLocAScore, out var fileSuffixesToCombine, out var processingRunTimes);

                if (fileSuffixesToCombine != null)
                {
                    // Concatenate the results

                    foreach (var fileSuffix in fileSuffixesToCombine)
                    {
                        var concatenateSuccess = ConcatenateResultFiles(fileSuffix + FILE_SUFFIX_ASCORE_RESULTS);
                        if (!concatenateSuccess)
                        {
                            processingSuccess = false;
                        }

                        concatenateSuccess = ConcatenateResultFiles(fileSuffix + FILE_SUFFIX_SYN_PLUS_ASCORE);
                        if (!concatenateSuccess)
                        {
                            processingSuccess = false;
                        }
                    }
                }

                // Concatenate the log files
                ConcatenateLogFiles(processingRunTimes);

                mProgress = PROGRESS_PCT_PHOSPHO_FDR_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the dataset name and transfer folder path so that the results get copied to the correct location
                RedefineAggregationJobDatasetAndTransferFolder();

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in PhosphoFdrAggregator->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        protected bool AddMSGFSpecProbValues(int jobNumber, string synFilePath, string fileTypeTag)
        {
            try
            {
                var synFile = new FileInfo(synFilePath);
                if (synFile.Directory == null)
                {
                    LogError("Cannot determine the parent directory of " + synFile.FullName);
                    return false;
                }

                var msgfFileName = Path.GetFileNameWithoutExtension(synFile.Name) + "_MSGF.txt";

                var msgfFile = new FileInfo(Path.Combine(synFile.Directory.FullName, msgfFileName));

                if (!msgfFile.Exists)
                {
                    LogWarning("MSGF file not found for job " + jobNumber, true);
                    LogWarning("Cannot add MSGF_SpecProb values to the " + fileTypeTag + " file; " + msgfFile.FullName);
                    return true;
                }

                // Use Mage to create an updated synopsis file, with column MSGF_SpecProb added

                // First cache the MSGFSpecProb values using a delimited file reader

                var msgfReader = new DelimitedFileReader {
                    FilePath = msgfFile.FullName
                };

                var lookupSink = new KVSink
                {
                    KeyColumnName = "Result_ID",
                    ValueColumnName = "SpecProb"
                };

                var cachePipeline = ProcessingPipeline.Assemble("Lookup pipeline", msgfReader, lookupSink);
                cachePipeline.RunRoot(null);

                if (lookupSink.Values.Count == 0)
                {
                    mMessage = msgfFile.Name + " was empty for job " + jobNumber;
                    LogError(mMessage);
                    return false;
                }

                // Next create the updated synopsis / first hits file using the values cached in lookupSink

                var updatedFile = new FileInfo(msgfFile.FullName + ".msgf");

                var synReader = new DelimitedFileReader { FilePath = synFile.FullName };

                var synWriter = new DelimitedFileWriter { FilePath = updatedFile.FullName };

                var mergeFilter = new MergeFromLookup
                {
                    OutputColumnList = "*, MSGF_SpecProb|+|text",
                    LookupKV = lookupSink.Values,
                    KeyColName = "HitNum",
                    MergeColName = "MSGF_SpecProb"
                };

                var mergePipeline = ProcessingPipeline.Assemble("Main pipeline", synReader, mergeFilter, synWriter);
                mergePipeline.RunRoot(null);

                updatedFile.Refresh();
                if (!updatedFile.Exists)
                {
                    mMessage = "Mage did not create " + updatedFile.Name + " for job " + jobNumber;
                    LogError(mMessage);
                    return false;
                }

                if (updatedFile.Length == 0)
                {
                    mMessage = updatedFile.Name + " is 0 bytes for job " + jobNumber;
                    LogError(mMessage);
                    return false;
                }

                // Replace the original file with the new one
                var originalFilePath = synFile.FullName;

                synFile.MoveTo(synFile.FullName + ".old");

                updatedFile.MoveTo(originalFilePath);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in AddMSGFSpecProbValues: " + ex.Message);
                return false;
            }
        }

        protected void CacheFileSuffix(List<string> fileSuffixesToCombine, string datasetName, string fileName)
        {
            var baseName = Path.GetFileNameWithoutExtension(fileName);
            if (string.IsNullOrWhiteSpace(baseName))
                return;

            var nameToAdd = baseName.Substring(datasetName.Length);

            if (!fileSuffixesToCombine.Contains(nameToAdd))
            {
                fileSuffixesToCombine.Add(nameToAdd);
            }
        }

        protected bool ConcatenateLogFiles(Dictionary<string, double> processingRunTimes)
        {
            try
            {
                var targetFilePath = Path.Combine(mWorkDir, ASCORE_CONSOLE_OUTPUT_PREFIX + ".txt");
                using var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var jobFolderList = GetJobFolderList();

                foreach (var jobFolder in jobFolderList)
                {
                    var jobNumber = jobFolder.Key;

                    var logFiles = jobFolder.Value.GetFiles(ASCORE_CONSOLE_OUTPUT_PREFIX + "*").ToList();
                    if (logFiles.Count == 0)
                    {
                        continue;
                    }

                    writer.WriteLine("----------------------------------------------------------");
                    writer.WriteLine("Job: " + jobNumber);

                    foreach (var logFile in logFiles)
                    {
                        // Log file name should be of the form AScore_ConsoleOutput_syn.txt
                        // Parse out the tag from it -- in this case "syn"
                        var fileTypeTag = Path.GetFileNameWithoutExtension(logFile.Name).Substring(ASCORE_CONSOLE_OUTPUT_PREFIX.Length + 1);

                        processingRunTimes.TryGetValue(jobNumber + fileTypeTag, out var runtimeMinutes);

                        using (var reader = new StreamReader(new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();
                                if (!string.IsNullOrWhiteSpace(dataLine))
                                {
                                    if (dataLine.StartsWith("Percent Completion"))
                                        continue;
                                    if (dataLine.Trim().StartsWith("Skipping PHRP result"))
                                        continue;

                                    writer.WriteLine(dataLine);
                                }
                            }
                        }

                        writer.WriteLine("Processing time: " + runtimeMinutes.ToString("0.0") + " minutes");
                        writer.WriteLine();
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("ConcatenateLogFiles: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool ConcatenateResultFiles(string fileSuffix)
        {
            var currentFile = string.Empty;
            var firstFileProcessed = false;

            try
            {
                var targetFilePath = Path.Combine(mWorkDir, "Concatenated" + fileSuffix);
                using var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var jobFolderList = GetJobFolderList();
                foreach (var jobFolder in jobFolderList)
                {
                    var jobNumber = jobFolder.Key;

                    foreach (var resultFile in jobFolder.Value.GetFiles("*" + fileSuffix))
                    {
                        currentFile = Path.GetFileName(resultFile.FullName);

                        using var reader = new StreamReader(new FileStream(resultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                        if (reader.EndOfStream)
                            continue;

                        var headerLine = reader.ReadLine();
                        if (headerLine == null)
                            continue;

                        var replaceFirstColumnWithJob = headerLine.StartsWith("job", StringComparison.OrdinalIgnoreCase);

                        if (firstFileProcessed)
                        {
                            // Skip this header line
                        }
                        else
                        {
                            // Write the header line
                            if (replaceFirstColumnWithJob)
                            {
                                // The Job column is already present
                                writer.WriteLine(headerLine);
                            }
                            else
                            {
                                // Add the Job column header
                                writer.WriteLine("Job\t" + headerLine);
                            }

                            firstFileProcessed = true;
                        }

                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();
                            if (string.IsNullOrWhiteSpace(dataLine))
                                continue;

                            if (replaceFirstColumnWithJob)
                            {
                                // Remove the first column from dataLine
                                var charIndex = dataLine.IndexOf('\t');
                                if (charIndex >= 0)
                                {
                                    writer.WriteLine(jobNumber + "\t" + dataLine.Substring(charIndex + 1));
                                }
                                continue;
                            }

                            writer.WriteLine(jobNumber + "\t" + dataLine);
                        }
                    }    // for each resultFile
                }    // for each jobFolder

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "File could not be concatenated: " + currentFile;
                LogError("ConcatenateResultFiles, " + mMessage + ": " + ex.Message);
                return false;
            }
        }

        protected void CreateJobToDatasetMapFile(List<JobMetadataForAScore> jobsProcessed)
        {
            var outputFilePath = Path.Combine(mWorkDir, "Job_to_Dataset_Map.txt");

            using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

            writer.WriteLine("Job\tTool\tDataset");

            foreach (var job in jobsProcessed)
            {
                writer.WriteLine(job.Job + "\t" + job.ToolName + "\t" + job.Dataset);
            }
        }

        protected string DetermineAScoreParamFilePath(string settingsFileName)
        {
            string bestAScoreParamFileName;

            var datasetType = DatasetTypeConstants.Unknown;

            if (settingsFileName.IndexOf("_cid", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                datasetType = DatasetTypeConstants.CID;
            }

            if (settingsFileName.IndexOf("_etd", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                datasetType = DatasetTypeConstants.ETD;
            }

            if (settingsFileName.IndexOf("_hcd", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                datasetType = DatasetTypeConstants.HCD;
            }

            switch (datasetType)
            {
                case DatasetTypeConstants.CID:
                case DatasetTypeConstants.Unknown:
                    bestAScoreParamFileName = GetBestAScoreParamFile(new List<string>
                    {
                        "AScoreCIDParamFile",
                        "AScoreHCDParamFile",
                        "AScoreETDParamFile"
                    });
                    break;
                case DatasetTypeConstants.ETD:
                    bestAScoreParamFileName = GetBestAScoreParamFile(new List<string>
                    {
                        "AScoreETDParamFile",
                        "AScoreHCDParamFile",
                        "AScoreCIDParamFile"
                    });
                    break;
                case DatasetTypeConstants.HCD:
                    bestAScoreParamFileName = GetBestAScoreParamFile(new List<string>
                    {
                        "AScoreHCDParamFile",
                        "AScoreCIDParamFile",
                        "AScoreETDParamFile"
                    });
                    break;
                default:
                    throw new Exception("Programming bug in ProcessSynopsisFiles; unrecognized value for datasetType: " + datasetType);
            }

            if (string.IsNullOrWhiteSpace(bestAScoreParamFileName))
            {
                mMessage = "Programming bug, AScore parameter file not found in ProcessSynopsisFiles " +
                    "(AnalysisResourcesPhosphoFdrAggregator.GetResources should have already flagged this as an error)";
                LogError(mMessage);
                return string.Empty;
            }

            return Path.Combine(mWorkDir, bestAScoreParamFileName);
        }

        protected bool DetermineInputFilePaths(DirectoryInfo jobFolder, ref JobMetadataForAScore udtJobMetadata, List<string> fileSuffixesToCombine)
        {
            var fhtFile = string.Empty;
            var synFile = string.Empty;
            var runningSequest = false;

            if (udtJobMetadata.ToolName.StartsWith("sequest", StringComparison.OrdinalIgnoreCase))
            {
                runningSequest = true;
                fhtFile = udtJobMetadata.Dataset + "_fht.txt";
                synFile = udtJobMetadata.Dataset + "_syn.txt";
                udtJobMetadata.ToolNameForAScore = "sequest";
            }

            if (udtJobMetadata.ToolName.StartsWith("xtandem", StringComparison.OrdinalIgnoreCase))
            {
                fhtFile = udtJobMetadata.Dataset + "_xt_fht.txt";
                synFile = udtJobMetadata.Dataset + "_xt_syn.txt";
                udtJobMetadata.ToolNameForAScore = "xtandem";
            }

            if (udtJobMetadata.ToolName.StartsWith("msgfplus", StringComparison.OrdinalIgnoreCase))
            {
                fhtFile = udtJobMetadata.Dataset + "_msgfplus_fht.txt";
                synFile = udtJobMetadata.Dataset + "_msgfplus_syn.txt";
                udtJobMetadata.ToolNameForAScore = "msgfplus";
            }

            if (string.IsNullOrWhiteSpace(fhtFile))
            {
                mMessage = "Analysis tool " + udtJobMetadata.ToolName + " is not supported by the PhosphoFdrAggregator";
                return false;
            }

            udtJobMetadata.FirstHitsFilePath = Path.Combine(jobFolder.FullName, fhtFile);
            udtJobMetadata.SynopsisFilePath = Path.Combine(jobFolder.FullName, synFile);

            bool success;

            if (!File.Exists(udtJobMetadata.FirstHitsFilePath))
            {
                var fhtFileAlternate = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.FirstHitsFilePath, "Dataset_msgfdb.txt");
                if (File.Exists(fhtFileAlternate))
                {
                    udtJobMetadata.FirstHitsFilePath = fhtFileAlternate;
                    fhtFile = Path.GetFileName(fhtFileAlternate);
                }
            }

            if (!File.Exists(udtJobMetadata.SynopsisFilePath))
            {
                var synFileAlternate = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.SynopsisFilePath, "Dataset_msgfdb.txt");
                if (File.Exists(synFileAlternate))
                {
                    udtJobMetadata.SynopsisFilePath = synFileAlternate;
                    synFile = Path.GetFileName(synFileAlternate);
                }
            }

            if (File.Exists(udtJobMetadata.FirstHitsFilePath))
            {
                CacheFileSuffix(fileSuffixesToCombine, udtJobMetadata.Dataset, fhtFile);
                if (runningSequest)
                {
                    success = AddMSGFSpecProbValues(udtJobMetadata.Job, udtJobMetadata.FirstHitsFilePath, "fht");
                    if (!success)
                        return false;
                }
            }
            else
            {
                udtJobMetadata.FirstHitsFilePath = string.Empty;
            }

            if (File.Exists(udtJobMetadata.SynopsisFilePath))
            {
                CacheFileSuffix(fileSuffixesToCombine, udtJobMetadata.Dataset, synFile);
                if (runningSequest)
                {
                    success = AddMSGFSpecProbValues(udtJobMetadata.Job, udtJobMetadata.SynopsisFilePath, "syn");
                    if (!success)
                        return false;
                }
            }
            else
            {
                udtJobMetadata.SynopsisFilePath = string.Empty;
            }

            if (string.IsNullOrWhiteSpace(udtJobMetadata.FirstHitsFilePath) && string.IsNullOrWhiteSpace(udtJobMetadata.SynopsisFilePath))
            {
                LogWarning("Did not find a synopsis or first hits file for job " + udtJobMetadata.Job);
                return false;
            }

            return true;
        }

        private string DetermineSpectrumFilePath(DirectoryInfo diJobFolder)
        {
            var dtaFiles = diJobFolder.GetFiles("*_dta.zip");
            if (dtaFiles.Length > 0)
            {
                var dtaFile = dtaFiles.First();
                if (!UnzipFile(dtaFile.FullName))
                {
                    mMessage = "Error unzipping " + dtaFile.Name;
                    return string.Empty;
                }

                return Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(dtaFile.Name) + ".txt");
            }

            var mzMLFiles = diJobFolder.GetFiles("*.mzML.gz");
            if (mzMLFiles.Length > 0)
            {
                var mzMLFile = mzMLFiles.First();
                if (!GUnzipFile(mzMLFile.FullName))
                {
                    mMessage = "Error unzipping " + mzMLFile.Name;
                    return string.Empty;
                }

                return Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mzMLFile.Name));
            }

            mMessage = "Folder " + diJobFolder.Name + " does not have a _dta.zip file or .mzML.gz file";
            return string.Empty;
        }

        private string GetBestAScoreParamFile(IEnumerable<string> parameterNames)
        {
            foreach (var paramName in parameterNames)
            {
                var paramFileName = mJobParams.GetJobParameter(paramName, string.Empty);
                if (string.IsNullOrWhiteSpace(paramFileName))
                {
                    continue;
                }

                if (File.Exists(Path.Combine(mWorkDir, paramFileName)))
                {
                    return paramFileName;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Finds the directories that start with Job
        /// </summary>
        /// <returns>Dictionary where key is the Job number and value is a DirectoryInfo object</returns>
        protected Dictionary<int, DirectoryInfo> GetJobFolderList()
        {
            var jobFolderList = new Dictionary<int, DirectoryInfo>();

            var diWorkingFolder = new DirectoryInfo(mWorkDir);

            foreach (var jobFolder in diWorkingFolder.GetDirectories("Job*"))
            {
                var jobNumberText = jobFolder.Name.Substring(3);

                if (int.TryParse(jobNumberText, out var jobNumber))
                {
                    jobFolderList.Add(jobNumber, jobFolder);
                }
            }

            return jobFolderList;
        }

        // Example Console output
        //
        // Fragment Type:  HCD
        // Mass Tolerance: 0.05 Da
        // Caching data in E:\DMS_WorkDir\Job1153717\HarrisMS_batch2_Ppp_A4-2_22Dec14_Frodo_14-12-07_msgfplus_syn.txt
        // Computing AScore values and Writing results to E:\DMS_WorkDir
        // Modifications for Dataset: HarrisMS_batch2_Ppp_A4-2_22Dec14_Frodo_14-12-07
        // 	Static,   57.021465 on C
        // 	Dynamic,  79.966331 on STY
        // Percent Completion 0%
        // Percent Completion 0%
        // Percent Completion 1%
        // Percent Completion 1%
        // Percent Completion 1%
        // Percent Completion 2%
        // Percent Completion 2%
        private const string REGEX_AScore_PROGRESS = @"Percent Completion (\d+)\%";
        private readonly Regex reCheckProgress = new(REGEX_AScore_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the ProMex console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                // Value between 0 and 100
                var ascoreProgress = 0;
                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var dataLineLCase = dataLine.ToLower();

                            if (dataLineLCase.StartsWith("error:") || dataLineLCase.Contains("unhandled exception"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running AScore:";
                                }
                                mConsoleOutputErrorMsg += "; " + dataLine;
                                continue;
                            }

                            var oMatch = reCheckProgress.Match(dataLine);
                            if (oMatch.Success)
                            {
                                int.TryParse(oMatch.Groups[1].ToString(), out ascoreProgress);
                            }
                        }
                    }
                }

                var percentCompleteStart = mJobFoldersProcessed / (float)mTotalJobFolders * 100f;
                var percentCompleteEnd = (mJobFoldersProcessed + 1) / (float)mTotalJobFolders * 100f;
                var subtaskProgress = ComputeIncrementalProgress(percentCompleteStart, percentCompleteEnd, ascoreProgress);

                var progressComplete = ComputeIncrementalProgress(PROGRESS_PCT_PHOSPHO_FDR_RUNNING, PROGRESS_PCT_PHOSPHO_FDR_COMPLETE, subtaskProgress);

                if (mProgress < progressComplete)
                {
                    mProgress = progressComplete;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Run AScore against the Synopsis and First hits files in the Job subdirectories
        /// </summary>
        /// <param name="progLoc">AScore exe path</param>
        /// <param name="fileSuffixesToCombine">Output parameter: File suffixes that were processed</param>
        /// <param name="processingRunTimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
        /// <returns>True if success, false if an error</returns>
        protected bool ProcessSynopsisFiles(string progLoc, out List<string> fileSuffixesToCombine, out Dictionary<string, double> processingRunTimes)
        {
            var successOverall = true;

            fileSuffixesToCombine = new List<string>();
            processingRunTimes = new Dictionary<string, double>();

            try
            {
                // Extract the dataset raw file paths
                var jobToDatasetMap = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP);
                var jobToSettingsFileMap = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP);
                var jobToToolMap = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_JOB_TOOL_MAP);
                var jobsProcessed = new List<JobMetadataForAScore>();

                var jobCountSkippedUnknownJob = 0;
                var jobCountSkippedNoSpectrumFile = 0;
                var jobCountSkippedNoSynFile = 0;

                var jobFolderList = GetJobFolderList();

                mProgress = PROGRESS_PCT_PHOSPHO_FDR_RUNNING;

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mJobFoldersProcessed = 0;
                mTotalJobFolders = jobFolderList.Count;

                // Process each Job folder
                foreach (var jobFolder in jobFolderList)
                {
                    var synopsisFiles = jobFolder.Value.GetFiles("*syn*.txt");

                    var firstHitsFiles = jobFolder.Value.GetFiles("*fht*.txt");
                    if (synopsisFiles.Length + firstHitsFiles.Length == 0)
                    {
                        continue;
                    }

                    var udtJobMetadata = new JobMetadataForAScore {
                        Job = jobFolder.Key
                    };

                    if (!jobToDatasetMap.TryGetValue(udtJobMetadata.Job.ToString(), out var datasetName))
                    {
                        mMessage = "Job " + udtJobMetadata.Job + " not found in packed job parameter " +
                                    AnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP;
                        LogError("Error in ProcessSynopsisFiles: " + mMessage);
                        jobCountSkippedUnknownJob++;
                        continue;
                    }

                    udtJobMetadata.Dataset = datasetName;

                    var settingsFileName = jobToSettingsFileMap[udtJobMetadata.Job.ToString()];
                    udtJobMetadata.ToolName = jobToToolMap[udtJobMetadata.Job.ToString()];

                    // Determine the AScore parameter file to use
                    var bestAScoreParamFilePath = DetermineAScoreParamFilePath(settingsFileName);

                    if (string.IsNullOrWhiteSpace(bestAScoreParamFilePath))
                    {
                        return false;
                    }

                    // Find the spectrum file; should be _dta.zip or .mzML.gz
                    udtJobMetadata.SpectrumFilePath = DetermineSpectrumFilePath(jobFolder.Value);

                    if (string.IsNullOrWhiteSpace(udtJobMetadata.SpectrumFilePath))
                    {
                        jobCountSkippedNoSpectrumFile++;
                        continue;
                    }

                    // Find any first hits and synopsis files
                    var success = DetermineInputFilePaths(jobFolder.Value, ref udtJobMetadata, fileSuffixesToCombine);
                    if (!success)
                    {
                        jobCountSkippedNoSynFile++;
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(udtJobMetadata.FirstHitsFilePath))
                        {
                            // Analyze the first hits file with AScore
                            success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.FirstHitsFilePath, bestAScoreParamFilePath, "fht", processingRunTimes);
                            if (!success)
                            {
                                // An error has already been logged, and mMessage has been updated
                                successOverall = false;
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(udtJobMetadata.SynopsisFilePath))
                        {
                            // Analyze the synopsis file with AScore
                            success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.SynopsisFilePath, bestAScoreParamFilePath, "syn", processingRunTimes);
                            if (!success)
                            {
                                // An error has already been logged, and mMessage has been updated
                                successOverall = false;
                            }
                        }

                        jobsProcessed.Add(udtJobMetadata);
                    }

                    // Delete the unzipped spectrum file
                    try
                    {
                        File.Delete(udtJobMetadata.SpectrumFilePath);
                    }
                    catch (Exception)
                    {
                        // Ignore errors
                    }

                    mJobFoldersProcessed++;
                    var subTaskProgress = mJobFoldersProcessed / (float)mTotalJobFolders * 100f;

                    mProgress = ComputeIncrementalProgress(PROGRESS_PCT_PHOSPHO_FDR_RUNNING, PROGRESS_PCT_PHOSPHO_FDR_COMPLETE, subTaskProgress);
                }

                // see the DMS_FailedResults directory ...
                var failedResultsDirInfo = string.Format("see the {0} directory for results from successfully processed jobs",
                                                         DMS_FAILED_RESULTS_DIRECTORY_NAME);

                if (jobCountSkippedUnknownJob > 0)
                {
                    var msg = "Skipped " + jobCountSkippedUnknownJob + " job(s) " +
                              "because the job number was not defined in the job to dataset mapping dictionary; " + failedResultsDirInfo;
                    LogWarning(msg);
                    UpdateStatusMessage(msg);
                    successOverall = false;
                }

                if (jobCountSkippedNoSpectrumFile > 0)
                {
                    var msg = "Skipped " + jobCountSkippedNoSpectrumFile + " job(s) " +
                              "because the _dta.txt or .mzML file could not be found for the dataset; " + failedResultsDirInfo;
                    LogWarning(msg);
                    UpdateStatusMessage(msg);
                    successOverall = false;
                }

                if (jobCountSkippedNoSynFile > 0)
                {
                    var msg = "Skipped " + jobCountSkippedNoSynFile + " job(s) " +
                              "because a synopsis or first hits file was not found; " + failedResultsDirInfo;
                    LogWarning(msg);
                    UpdateStatusMessage(msg);
                }

                // Create the job to dataset map file
                CreateJobToDatasetMapFile(jobsProcessed);
            }
            catch (Exception ex)
            {
                if (string.IsNullOrEmpty(mMessage))
                    mMessage = "Error in ProcessSynopsisFiles";
                LogError("Error in ProcessSynopsisFiles: " + ex.Message);
                return false;
            }

            return successOverall;
        }

        /// <summary>
        /// Runs AScore on the specified file
        /// </summary>
        /// <param name="progLoc"></param>
        /// <param name="udtJobMetadata"></param>
        /// <param name="inputFilePath"></param>
        /// <param name="ascoreParamFilePath"></param>
        /// <param name="fileTypeTag">Should be syn or fht; append to the AScore_ConsoleOutput file</param>
        /// <param name="processingRunTimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RunAscore(
            string progLoc,
            JobMetadataForAScore udtJobMetadata,
            string inputFilePath,
            string ascoreParamFilePath,
            string fileTypeTag,
            Dictionary<string, double> processingRunTimes)
        {
            // Set up and execute a program runner to run AScore

            mConsoleOutputErrorMsg = string.Empty;

            var fiSourceFile = new FileInfo(inputFilePath);
            if (fiSourceFile.Directory == null)
            {
                LogError("Cannot determine the parent directory of " + fiSourceFile.FullName);
                return false;
            }

            var currentWorkingDir = fiSourceFile.Directory.FullName;
            var updatedInputFileName = Path.GetFileNameWithoutExtension(fiSourceFile.Name) + FILE_SUFFIX_SYN_PLUS_ASCORE;

            var arguments = " -T:" + udtJobMetadata.ToolNameForAScore +                     // Search engine name
                            " -F:" + PossiblyQuotePath(inputFilePath) +                     // Input file path
                            " -D:" + PossiblyQuotePath(udtJobMetadata.SpectrumFilePath) +   // DTA or mzML file path
                            " -P:" + PossiblyQuotePath(ascoreParamFilePath) +               // AScore parameter file
                            " -O:" + PossiblyQuotePath(currentWorkingDir) +                 // Output directory
                            " -U:" + PossiblyQuotePath(updatedInputFileName);               // Create an updated version of the input file,
                                                                                            // with updated peptide sequences and
                                                                                            // appended AScore-related columns

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (string.IsNullOrWhiteSpace(fileTypeTag))
            {
                fileTypeTag = "";
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(currentWorkingDir, ASCORE_CONSOLE_OUTPUT_PREFIX + "_" + fileTypeTag + ".txt");

            var dtStartTime = DateTime.UtcNow;

            var blnSuccess = mCmdRunner.RunProgram(progLoc, arguments, "AScore", true);

            var runtimeMinutes = DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes;
            processingRunTimes.Add(udtJobMetadata.Job + fileTypeTag, runtimeMinutes);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(mCmdRunner.CachedConsoleOutput);
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            // Parse the console output file one more time to check for errors
            Global.IdleLoop(0.25);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                var msg = "Error running AScore for job " + udtJobMetadata.Job;
                LogError(msg, msg + ", file " + fiSourceFile.Name + ", data package job " + udtJobMetadata.Job);

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("AScore returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to AScore failed (but exit code is 0)");
                }

                return false;
            }

            mStatusTools.UpdateAndWrite(mProgress);
            if (mDebugLevel >= 3)
            {
                LogDebug("AScore search complete for data package job " + udtJobMetadata.Job);
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "AScore_DLL.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for mCmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            // Parse the console output file every 15 seconds
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                LogProgress("PhosphoFdrAggregator");
            }
        }

        #endregion
    }
}
