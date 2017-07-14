//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;
using Mage;
using PHRPReader;

namespace AnalysisManagerPhospho_FDR_AggregatorPlugIn
{
    /// <summary>
    /// Class for running Phospho_FDRAggregator analysis
    /// </summary>
    public class clsAnalysisToolRunnerPhosphoFdrAggregator : clsAnalysisToolRunnerBase
    {
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

        protected struct udtJobMetadataForAScore
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

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs PhosphoFdrAggregator tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
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
                    LogDebug("clsAnalysisToolRunnerPhosphoFdrAggregator.RunTool(): Enter");
                }

                // Determine the path to the Ascore program
                // AScoreProgLoc will be something like this: "C:\DMS_Programs\AScore\AScore_Console.exe"
                var progLocAScore = m_mgrParams.GetParam("AScoreprogloc");
                if (!File.Exists(progLocAScore))
                {
                    if (string.IsNullOrWhiteSpace(progLocAScore))
                        progLocAScore = "Parameter 'AScoreprogloc' not defined for this manager";
                    m_message = "Cannot find AScore program file";
                    LogError(m_message + ": " + progLocAScore);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AScore version info in the database
                if (!StoreToolVersionInfo(progLocAScore))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AScore version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run AScore for each of the jobs in the data package
                var processingSuccess = ProcessSynopsisFiles(progLocAScore, out var fileSuffixesToCombine, out var processingRuntimes);

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
                ConcatenateLogFiles(processingRuntimes);

                m_progress = PROGRESS_PCT_PHOSPHO_FDR_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Thread.Sleep(1000);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the dataset name and transfer folder path so that the results get copied to the correct location
                RedefineAggregationJobDatasetAndTransferFolder();

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in PhosphoFdrAggregator->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        protected bool AddMSGFSpecProbValues(int jobNumber, string synFilePath, string fileTypeTag)
        {
            try
            {
                var fiSynFile = new FileInfo(synFilePath);
                if (fiSynFile.Directory == null)
                {
                    LogError("Cannot determine the parent directory of " + fiSynFile.FullName);
                    return false;
                }

                var msgfFileName = Path.GetFileNameWithoutExtension(fiSynFile.Name) + "_MSGF.txt";

                var fiMsgfFile = new FileInfo(Path.Combine(fiSynFile.Directory.FullName, msgfFileName));

                if (!fiMsgfFile.Exists)
                {
                    var warningMessage = "MSGF file not found for job " + jobNumber;
                    m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, warningMessage);

                    warningMessage += "; cannot add MSGF_SpecProb values to the " + fileTypeTag + " file; " + fiMsgfFile.FullName;
                    LogWarning(warningMessage);
                    return true;
                }

                // Use Mage to create an updated synopsis file, with column MSGF_SpecProb added

                // First cache the MSGFSpecProb values using a delimited file reader

                var msgfReader = new DelimitedFileReader {
                    FilePath = fiMsgfFile.FullName
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
                    m_message = fiMsgfFile.Name + " was empty for job " + jobNumber;
                    LogError(m_message);
                    return false;
                }

                // Next create the updated synopsis / first hits file using the values cached in lookupSink

                var fiUpdatedfile = new FileInfo(fiMsgfFile.FullName + ".msgf");

                var synReader = new DelimitedFileReader { FilePath = fiSynFile.FullName };

                var synWriter = new DelimitedFileWriter { FilePath = fiUpdatedfile.FullName };

                var mergeFilter = new MergeFromLookup
                {
                    OutputColumnList = "*, MSGF_SpecProb|+|text",
                    LookupKV = lookupSink.Values,
                    KeyColName = "HitNum",
                    MergeColName = "MSGF_SpecProb"
                };

                var mergePipeline = ProcessingPipeline.Assemble("Main pipeline", synReader, mergeFilter, synWriter);
                mergePipeline.RunRoot(null);

                fiUpdatedfile.Refresh();
                if (!fiUpdatedfile.Exists)
                {
                    m_message = "Mage did not create " + fiUpdatedfile.Name + " for job " + jobNumber;
                    LogError(m_message);
                    return false;
                }

                if (fiUpdatedfile.Length == 0)
                {
                    m_message = fiUpdatedfile.Name + " is 0 bytes for job " + jobNumber;
                    LogError(m_message);
                    return false;
                }

                Thread.Sleep(100);

                // Replace the original file with the new one
                var originalFilePath = fiSynFile.FullName;

                fiSynFile.MoveTo(fiSynFile.FullName + ".old");

                fiUpdatedfile.MoveTo(originalFilePath);

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

        protected bool ConcatenateLogFiles(Dictionary<string, double> processingRuntimes)
        {
            try
            {
                var targetFile = Path.Combine(m_WorkDir, ASCORE_CONSOLE_OUTPUT_PREFIX + ".txt");
                using (var swConcatenatedFile = new StreamWriter(new FileStream(targetFile, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var jobFolderlist = GetJobFolderList();

                    foreach (var jobFolder in jobFolderlist)
                    {
                        var jobNumber = jobFolder.Key;

                        var logFiles = jobFolder.Value.GetFiles(ASCORE_CONSOLE_OUTPUT_PREFIX + "*").ToList();
                        if (logFiles.Count == 0)
                        {
                            continue;
                        }

                        swConcatenatedFile.WriteLine("----------------------------------------------------------");
                        swConcatenatedFile.WriteLine("Job: " + jobNumber);

                        foreach (var logFile in logFiles)
                        {
                            // Logfile name should be of the form AScore_ConsoleOutput_syn.txt
                            // Parse out the tag from it -- in this case "syn"
                            var fileTypeTag = Path.GetFileNameWithoutExtension(logFile.Name).Substring(ASCORE_CONSOLE_OUTPUT_PREFIX.Length + 1);

                            processingRuntimes.TryGetValue(jobNumber + fileTypeTag, out var runtimeMinutes);

                            using (var srInputFile = new StreamReader(new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                            {
                                while (!srInputFile.EndOfStream)
                                {
                                    var dataLine = srInputFile.ReadLine();
                                    if (!string.IsNullOrWhiteSpace(dataLine))
                                    {
                                        if (dataLine.StartsWith("Percent Completion"))
                                            continue;
                                        if (dataLine.Trim().StartsWith("Skipping PHRP result"))
                                            continue;

                                        swConcatenatedFile.WriteLine(dataLine);
                                    }
                                }
                            }

                            swConcatenatedFile.WriteLine("Processing time: " + runtimeMinutes.ToString("0.0") + " minutes");
                            swConcatenatedFile.WriteLine();
                        }
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
            var firstfileProcessed = false;

            try
            {
                var targetFile = Path.Combine(m_WorkDir, "Concatenated" + fileSuffix);
                using (var swConcatenatedFile = new StreamWriter(new FileStream(targetFile, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var jobFolderlist = GetJobFolderList();
                    foreach (var jobFolder in jobFolderlist)
                    {
                        var jobNumber = jobFolder.Key;

                        var filesToCombine = jobFolder.Value.GetFiles("*" + fileSuffix).ToList();

                        foreach (var fiResultFile in filesToCombine)
                        {
                            currentFile = Path.GetFileName(fiResultFile.FullName);

                            using (var srInputFile = new StreamReader(new FileStream(fiResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                            {
                                if (srInputFile.EndOfStream)
                                    continue;

                                var headerLine = srInputFile.ReadLine();
                                if (headerLine == null)
                                    continue;

                                var replaceFirstColumnWithJob = headerLine.ToLower().StartsWith("job");

                                if (firstfileProcessed)
                                {
                                    // Skip this header line
                                }
                                else
                                {
                                    // Write the header line
                                    if (replaceFirstColumnWithJob)
                                    {
                                        // The Job column is already present
                                        swConcatenatedFile.WriteLine(headerLine);
                                    }
                                    else
                                    {
                                        // Add the Job column header
                                        swConcatenatedFile.WriteLine("Job\t" + headerLine);
                                    }

                                    firstfileProcessed = true;
                                }

                                while (!srInputFile.EndOfStream)
                                {
                                    var dataLine = srInputFile.ReadLine();
                                    if (string.IsNullOrWhiteSpace(dataLine))
                                        continue;

                                    if (replaceFirstColumnWithJob)
                                    {
                                        // Remove the first column from dataLine
                                        var charIndex = dataLine.IndexOf('\t');
                                        if (charIndex >= 0)
                                        {
                                            swConcatenatedFile.WriteLine(jobNumber + "\t" + dataLine.Substring(charIndex + 1));
                                        }
                                        continue;
                                    }

                                    swConcatenatedFile.WriteLine(jobNumber + "\t" + dataLine);
                                }

                            }
                        }    // foreach fiResultFile
                    }    // foreach jobFolder
                }   // using swConcatenatedFile

                return true;
            }
            catch (Exception ex)
            {
                m_message = "File could not be concatenated: " + currentFile;
                LogError("ConcatenateResultFiles, " + m_message + ": " + ex.Message);
                return false;
            }
        }

        protected void CreateJobToDatasetMapFile(List<udtJobMetadataForAScore> jobsProcessed)
        {
            var outputFilePath = Path.Combine(m_WorkDir, "Job_to_Dataset_Map.txt");

            using (var swMapFile = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swMapFile.WriteLine("Job\t" + "Tool\t" + "Dataset");

                foreach (var job in jobsProcessed)
                {
                    swMapFile.WriteLine(job.Job + "\t" + job.ToolName + "\t" + job.Dataset);
                }
            }
        }

        protected string DetermineAScoreParamFilePath(string settingsFileName)
        {
            string bestAScoreParamFileName;

            var datasetType = DatasetTypeConstants.Unknown;

            if (settingsFileName.ToLower().Contains("_cid"))
            {
                datasetType = DatasetTypeConstants.CID;
            }

            if (settingsFileName.ToLower().Contains("_etd"))
            {
                datasetType = DatasetTypeConstants.ETD;
            }

            if (settingsFileName.ToLower().Contains("_hcd"))
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
                m_message = "Programming bug, AScore parameter file not found in ProcessSynopsisFiles " +
                    "(clsAnalysisResourcesPhosphoFdrAggregator.GetResources should have already flagged this as an error)";
                LogError(m_message);
                return string.Empty;
            }

            return Path.Combine(m_WorkDir, bestAScoreParamFileName);
        }

        protected bool DetermineInputFilePaths(DirectoryInfo jobFolder, ref udtJobMetadataForAScore udtJobMetadata, List<string> fileSuffixesToCombine)
        {
            var fhtfile = string.Empty;
            var synFile = string.Empty;
            var runningSequest = false;

            if (udtJobMetadata.ToolName.ToLower().StartsWith("sequest"))
            {
                runningSequest = true;
                fhtfile = udtJobMetadata.Dataset + "_fht.txt";
                synFile = udtJobMetadata.Dataset + "_syn.txt";
                udtJobMetadata.ToolNameForAScore = "sequest";
            }

            if (udtJobMetadata.ToolName.ToLower().StartsWith("xtandem"))
            {
                fhtfile = udtJobMetadata.Dataset + "_xt_fht.txt";
                synFile = udtJobMetadata.Dataset + "_xt_syn.txt";
                udtJobMetadata.ToolNameForAScore = "xtandem";
            }

            if (udtJobMetadata.ToolName.ToLower().StartsWith("msgfplus"))
            {
                fhtfile = udtJobMetadata.Dataset + "_msgfplus_fht.txt";
                synFile = udtJobMetadata.Dataset + "_msgfplus_syn.txt";
                udtJobMetadata.ToolNameForAScore = "msgfplus";
            }

            if (string.IsNullOrWhiteSpace(fhtfile))
            {
                m_message = "Analysis tool " + udtJobMetadata.ToolName + " is not supported by the PhosphoFdrAggregator";
                return false;
            }

            udtJobMetadata.FirstHitsFilePath = Path.Combine(jobFolder.FullName, fhtfile);
            udtJobMetadata.SynopsisFilePath = Path.Combine(jobFolder.FullName, synFile);

            bool success;

            if (!File.Exists(udtJobMetadata.FirstHitsFilePath))
            {
                var fhtFileAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.FirstHitsFilePath, "Dataset_msgfdb.txt");
                if (File.Exists(fhtFileAlternate))
                {
                    udtJobMetadata.FirstHitsFilePath = fhtFileAlternate;
                    fhtfile = Path.GetFileName(fhtFileAlternate);
                }
            }

            if (!File.Exists(udtJobMetadata.SynopsisFilePath))
            {
                var synFileAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.SynopsisFilePath, "Dataset_msgfdb.txt");
                if (File.Exists(synFileAlternate))
                {
                    udtJobMetadata.SynopsisFilePath = synFileAlternate;
                    synFile = Path.GetFileName(synFileAlternate);
                }
            }

            if (File.Exists(udtJobMetadata.FirstHitsFilePath))
            {
                CacheFileSuffix(fileSuffixesToCombine, udtJobMetadata.Dataset, fhtfile);
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
                    m_message = "Error unzipping " + dtaFile.Name;
                    return string.Empty;
                }

                return Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(dtaFile.Name) + ".txt");
            }

            var mzMLFiles = diJobFolder.GetFiles("*.mzML.gz");
            if (mzMLFiles.Length > 0)
            {
                var mzMLFile = mzMLFiles.First();
                if (!GUnzipFile(mzMLFile.FullName))
                {
                    m_message = "Error unzipping " + mzMLFile.Name;
                    return string.Empty;
                }

                return Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mzMLFile.Name));
            }

            m_message = "Folder " + diJobFolder.Name + " does not have a _dta.zip file or .mzML.gz file";
            return string.Empty;
        }

        private string GetBestAScoreParamFile(IEnumerable<string> parameterNames)
        {
            foreach (var paramName in parameterNames)
            {
                var paramFileName = m_jobParams.GetJobParameter(paramName, string.Empty);
                if (string.IsNullOrWhiteSpace(paramFileName))
                {
                    continue;
                }

                if (File.Exists(Path.Combine(m_WorkDir, paramFileName)))
                {
                    return paramFileName;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Finds the folders that start with Job
        /// </summary>
        /// <returns>Dictionary where key is the Job number and value is a DirectoryInfo object</returns>
        /// <remarks></remarks>
        protected Dictionary<int, DirectoryInfo> GetJobFolderList()
        {
            var jobFolderList = new Dictionary<int, DirectoryInfo>();

            var diWorkingFolder = new DirectoryInfo(m_WorkDir);

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
        private readonly Regex reCheckProgress = new Regex(REGEX_AScore_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the ProMex console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                // Value between 0 and 100
                var ascoreProgress = 0;
                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            var strLineInLCase = strLineIn.ToLower();

                            if (strLineInLCase.StartsWith("error:") || strLineInLCase.Contains("unhandled exception"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running AScore:";
                                }
                                mConsoleOutputErrorMsg += "; " + strLineIn;
                                continue;
                            }

                            var oMatch = reCheckProgress.Match(strLineIn);
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

                if (m_progress < progressComplete)
                {
                    m_progress = progressComplete;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Run AScore against the Synopsis and First hits files in the Job subfolders
        /// </summary>
        /// <param name="progLoc">AScore exe path</param>
        /// <param name="fileSuffixesToCombine">Output parameter: File suffixes that were processed</param>
        /// <param name="processingRuntimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool ProcessSynopsisFiles(string progLoc, out List<string> fileSuffixesToCombine, out Dictionary<string, double> processingRuntimes)
        {
            var successOverall = true;

            fileSuffixesToCombine = new List<string>();
            processingRuntimes = new Dictionary<string, double>();

            try
            {
                // Extract the dataset raw file paths
                var jobToDatasetMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP);
                var jobToSettingsFileMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP);
                var jobToToolMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_TOOL_MAP);
                var jobsProcessed = new List<udtJobMetadataForAScore>();

                var jobCountSkipped = 0;

                var jobFolderlist = GetJobFolderList();

                m_progress = PROGRESS_PCT_PHOSPHO_FDR_RUNNING;

                mCmdRunner = new clsRunDosProgram(m_WorkDir);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mJobFoldersProcessed = 0;
                mTotalJobFolders = jobFolderlist.Count;

                // Process each Job folder
                foreach (var jobFolder in jobFolderlist)
                {
                    var synopsisFiles = jobFolder.Value.GetFiles("*syn*.txt");

                    var firstHitsFiles = jobFolder.Value.GetFiles("*fht*.txt");
                    if (synopsisFiles.Length + firstHitsFiles.Length == 0)
                    {
                        continue;
                    }

                    var udtJobMetadata = new udtJobMetadataForAScore {
                        Job = jobFolder.Key
                    };

                    if (!jobToDatasetMap.TryGetValue(udtJobMetadata.Job.ToString(), out var datasetName))
                    {
                        m_message = "Job " + udtJobMetadata.Job + " not found in packed job parameter " +
                                    clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP;
                        LogError("Error in ProcessSynopsisFiles: " + m_message);
                        return false;
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
                        return false;
                    }

                    // Find any first hits and synopsis files
                    var success = DetermineInputFilePaths(jobFolder.Value, ref udtJobMetadata, fileSuffixesToCombine);
                    if (!success)
                    {
                        jobCountSkipped += 1;
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(udtJobMetadata.FirstHitsFilePath))
                        {
                            // Analyze the first hits file with AScore
                            success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.FirstHitsFilePath, bestAScoreParamFilePath, "fht", processingRuntimes);
                            if (!success)
                            {
                                // An error has already been logged, and m_message has been updated
                                successOverall = false;
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(udtJobMetadata.SynopsisFilePath))
                        {
                            // Analyze the synopsis file with AScore
                            success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.SynopsisFilePath, bestAScoreParamFilePath, "syn", processingRuntimes);
                            if (!success)
                            {
                                // An error has already been logged, and m_message has been updated
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

                    mJobFoldersProcessed += 1;
                    var subTaskProgress = mJobFoldersProcessed / (float)mTotalJobFolders * 100f;

                    m_progress = ComputeIncrementalProgress(PROGRESS_PCT_PHOSPHO_FDR_RUNNING, PROGRESS_PCT_PHOSPHO_FDR_COMPLETE, subTaskProgress);
                }

                if (jobCountSkipped > 0)
                {
                    var msg = "Skipped " + jobCountSkipped + " job(s) because a synopsis or first hits file was not found";
                    LogWarning(msg);
                    UpdateStatusMessage(msg);
                }

                // Create the job to dataset map file
                CreateJobToDatasetMapFile(jobsProcessed);
            }
            catch (Exception ex)
            {
                if (string.IsNullOrEmpty(m_message))
                    m_message = "Error in ProcessSynopsisFiles";
                LogError("Error in ProcessSynopsisFiles: " + ex.Message);
                return false;
            }

            return successOverall;
        }

        /// <summary>
        /// Runs ascore on the specified file
        /// </summary>
        /// <param name="progLoc"></param>
        /// <param name="udtJobMetadata"></param>
        /// <param name="inputFilePath"></param>
        /// <param name="ascoreParamFilePath"></param>
        /// <param name="fileTypeTag">Should be syn or fht; appened to the AScore_ConsoleOutput file</param>
        /// <param name="processingRuntimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool RunAscore(
            string progLoc,
            udtJobMetadataForAScore udtJobMetadata,
            string inputFilePath,
            string ascoreParamFilePath,
            string fileTypeTag,
            Dictionary<string, double> processingRuntimes)
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

            var cmdStr = "";

            // Search engine name
            cmdStr += " -T:" + udtJobMetadata.ToolNameForAScore;

            // Input file path
            cmdStr += " -F:" + PossiblyQuotePath(inputFilePath);

            // DTA or mzML file path
            cmdStr += " -D:" + PossiblyQuotePath(udtJobMetadata.SpectrumFilePath);

            // AScore parameter file
            cmdStr += " -P:" + PossiblyQuotePath(ascoreParamFilePath);

            // Output folder
            cmdStr += " -O:" + PossiblyQuotePath(currentWorkingDir);

            // Create an updated version of the input file, with updated peptide sequences and appended AScore-related columns
            cmdStr += " -U:" + PossiblyQuotePath(updatedInputFileName);

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
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

            var blnSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "AScore", true);

            var runtimeMinutes = DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes;
            processingRuntimes.Add(udtJobMetadata.Job + fileTypeTag, runtimeMinutes);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            // Parse the console output file one more time to check for errors
            Thread.Sleep(250);
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

            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("AScore search complete for data package job " + udtJobMetadata.Job);
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strProgLoc)
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }
            }

            // Lookup the version of the .NET application
            var blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, fiProgram.FullName);
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                fiProgram
            };

            if (fiProgram.Directory != null)
            {
                ioToolFiles.Add(new FileInfo(Path.Combine(fiProgram.Directory.FullName, "AScore_DLL.dll")));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for mCmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            // Parse the console output file every 15 seconds
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                LogProgress("PhosphoFdrAggregator");
            }
        }

        #endregion
    }
}
