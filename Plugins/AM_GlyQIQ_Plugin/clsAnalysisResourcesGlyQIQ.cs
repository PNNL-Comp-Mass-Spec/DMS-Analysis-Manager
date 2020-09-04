//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/29/2014
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AnalysisManagerGlyQIQPlugin
{

    /// <summary>
    /// Retrieve resources for the GlyQ-IQ plugin
    /// </summary>
    public class clsAnalysisResourcesGlyQIQ : clsAnalysisResources
    {
        /// <summary>
        /// Locks folder name
        /// </summary>
        protected const string LOCKS_FOLDER_NAME = "LocksFolder";

        /// <summary>
        /// Job parameter tracking GlyQ-IQ core count usage
        /// </summary>
        public const string JOB_PARAM_ACTUAL_CORE_COUNT = "GlyQ_IQ_ActualCoreCount";

        /// <summary>
        /// Executor parameters file
        /// </summary>
        public const string EXECUTOR_PARAMETERS_FILE = "ExecutorParametersSK.xml";

        /// <summary>
        /// Start Program batch file prefix
        /// </summary>
        public const string START_PROGRAM_BATCH_FILE_PREFIX = "StartProgram_Core";

        /// <summary>
        /// GlyQ-IQ parameter file prefix
        /// </summary>
        public const string GLYQIQ_PARAMS_FILE_PREFIX = "GlyQIQ_Params_";

        /// <summary>
        /// Alignment parameterse file
        /// </summary>
        public const string ALIGNMENT_PARAMETERS_FILENAME = "AlignmentParameters.xml";

        protected struct udtGlyQIQParams
        {
            public Dictionary<int, DirectoryInfo> WorkingParameterFolders;
            public string FactorsName;
            public string TargetsName;
            public int NumTargets;
            public string ConsoleOperatingParametersFileName;
            public string IQParamFileName;
        }

        private udtGlyQIQParams mGlyQIQParams;

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            mGlyQIQParams = new udtGlyQIQParams();

            var coreCountText = mJobParams.GetJobParameter("GlyQ-IQ", "Cores", "All");

            // Use all the cores if the system has 4 or fewer cores
            // Otherwise, use TotalCoreCount - 1
            var maxAllowedCores = mStatusTools.GetCoreCount();
            if (maxAllowedCores > 4)
                maxAllowedCores -= 1;

            var coreCount = clsAnalysisToolRunnerBase.ParseThreadCount(coreCountText, maxAllowedCores);

            mJobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString());

            mGlyQIQParams.WorkingParameterFolders = CreateSubdirectories(coreCount);
            if (mGlyQIQParams.WorkingParameterFolders.Count == 0)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!RetrieveGlyQIQParameters(coreCount))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!RetrievePeaksAndRawData())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CopyFileToWorkingDirectories(string sourceFileName, string sourceFolderPath, string fileDesription)
        {
            foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
            {
                if (!CopyFileToWorkDir(sourceFileName, sourceFolderPath, workingDirectory.Value.FullName))
                {
                    mMessage += " (" + fileDesription + ")";
                    return false;
                }
            }

            return true;
        }

        private int CountTargets(string targetsFilePath)
        {
            try
            {
                // numTargets is initialized to -1 because we don't want to count the header line
                var numTargets = -1;

                using (var reader = new StreamReader(new FileStream(targetsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        reader.ReadLine();
                        numTargets += 1;
                    }
                }

                return numTargets;
            }
            catch (Exception ex)
            {
                mMessage = "Exception counting the targets in " + Path.GetFileName(targetsFilePath);
                LogError(mMessage + ": " + ex.Message);
                return 0;
            }
        }

        private bool CreateConsoleOperatingParametersFile()
        {
            try
            {
                // Define the output file name
                mGlyQIQParams.ConsoleOperatingParametersFileName = GLYQIQ_PARAMS_FILE_PREFIX + DatasetName + ".txt";
                LogMessage(
                    "Creating the Operating Parameters file, " + mGlyQIQParams.ConsoleOperatingParametersFileName);

                foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
                {
                    var outputFilePath = Path.Combine(workingDirectory.Value.FullName, mGlyQIQParams.ConsoleOperatingParametersFileName);

                    using (var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        writer.WriteLine("ResultsFolderPath" + "," + Path.Combine(mWorkDir, "Results"));
                        writer.WriteLine("LoggingFolderPath" + "," + Path.Combine(mWorkDir, "Results"));
                        writer.WriteLine("FactorsFile" + "," + mGlyQIQParams.FactorsName + ".txt");
                        writer.WriteLine("ExecutorParameterFile" + "," + EXECUTOR_PARAMETERS_FILE);
                        writer.WriteLine("XYDataFolder" + "," + "XYDataWriter");
                        writer.WriteLine("WorkflowParametersFile" + "," + mGlyQIQParams.IQParamFileName);
                        writer.WriteLine("Alignment" + "," + Path.Combine(workingDirectory.Value.FullName, ALIGNMENT_PARAMETERS_FILENAME));

                        // The following file doesn't have to exist
                        writer.WriteLine("BasicTargetedParameters" + "," +
                                          Path.Combine(workingDirectory.Value.FullName, "BasicTargetedWorkflowParameters.xml"));
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateConsoleOperatingParametersFile";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private bool CreateLauncherBatchFiles(IReadOnlyDictionary<int, FileInfo> splitTargetFileInfo)
        {
            try
            {
                LogMessage("Creating the Launcher batch files");

                // Determine the path to the IQGlyQ program

                var progLoc = clsAnalysisToolRunnerBase.DetermineProgramLocation("GlyQIQ", "GlyQIQProgLoc", "IQGlyQ_Console.exe", "", mMgrParams, out mMessage);
                if (string.IsNullOrEmpty(progLoc))
                {
                    LogError("DetermineProgramLocation returned an empty string: " + mMessage);
                    return false;
                }

                foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
                {
                    var core = workingDirectory.Key;

                    var batchFilePath = Path.Combine(mWorkDir, START_PROGRAM_BATCH_FILE_PREFIX + workingDirectory.Key + ".bat");

                    using (var writer = new StreamWriter(new FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Note that clsGlyQIqRunner expects this batch file to be in a specific format:
                        // GlyQIQProgramPath "WorkingDirectoryPath" "DatasetName" "DatasetSuffix" "TargetsFileName" "ParamFileName"
                        //                   "WorkingParametersFolderPath" "LockFileName" "ResultsFolderPath" "CoreNumber"
                        //
                        // It will read and parse the batch file to determine the TargetsFile name and folder path so that it can cache the target code values
                        // Thus, if you change this code, also update clsGlyQIqRunner

                        writer.Write(clsGlobal.PossiblyQuotePath(progLoc));

                        writer.Write(" " + "\"" + mWorkDir + "\"");
                        writer.Write(" " + "\"" + DatasetName + "\"");
                        writer.Write(" " + "\"" + "raw" + "\"");

                        if (!splitTargetFileInfo.TryGetValue(core, out var targetsFile))
                        {
                            LogError("Logic error; core " + core + " not found in dictionary splitTargetFileInfo");
                            return false;
                        }

                        writer.Write(" " + "\"" + targetsFile.Name + "\"");

                        writer.Write(" " + "\"" + mGlyQIQParams.ConsoleOperatingParametersFileName + "\"");

                        writer.Write(" " + "\"" + workingDirectory.Value.FullName + "\"");

                        writer.Write(" " + "\"" + "Lock_" + core + "\"");

                        writer.Write(" " + "\"" + Path.Combine(mWorkDir, "Results") + "\"");

                        writer.Write(" " + "\"" + core + "\"");

                        writer.WriteLine();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateLauncherBatchFiles";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private Dictionary<int, DirectoryInfo> CreateSubdirectories(int coreCount)
        {
            try
            {
                // Make sure that required subdirectories exist in the working directory
                var workingDirectories = new Dictionary<int, DirectoryInfo>();

                for (var core = 1; core <= coreCount; core++)
                {
                    var directoryName = "WorkingParametersCore" + core;

                    workingDirectories.Add(core, new DirectoryInfo(Path.Combine(mWorkDir, directoryName)));
                }

                foreach (var workingDirectory in workingDirectories)
                {
                    if (!workingDirectory.Value.Exists)
                    {
                        workingDirectory.Value.Create();
                    }

                    var locksDirectory = new DirectoryInfo(Path.Combine(workingDirectory.Value.FullName, LOCKS_FOLDER_NAME));
                    if (!locksDirectory.Exists)
                        locksDirectory.Create();
                }

                return workingDirectories;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateSubdirectories";
                LogError(mMessage + ": " + ex.Message);
                return new Dictionary<int, DirectoryInfo>();
            }
        }

        private bool RetrieveGlyQIQParameters(int coreCount)
        {
            try
            {
                // Define the base source folder path
                // Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ
                var paramFileStoragePathBase = mJobParams.GetParam("ParmFileStoragePath");

                mGlyQIQParams.IQParamFileName = mJobParams.GetJobParameter("ParmFileName", "");
                if (string.IsNullOrEmpty(mGlyQIQParams.IQParamFileName))
                {
                    LogError("Job Parameter File name is empty");
                    return false;
                }

                // Retrieve the GlyQ-IQ parameter file
                // Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ\ParameterFiles
                var sourceFolderPath = Path.Combine(paramFileStoragePathBase, "ParameterFiles");
                var sourceFileName = string.Copy(mGlyQIQParams.IQParamFileName);

                if (!CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "IQ Parameter File"))
                {
                    return false;
                }

                mGlyQIQParams.FactorsName = mJobParams.GetJobParameter("Factors", string.Empty);
                mGlyQIQParams.TargetsName = mJobParams.GetJobParameter("Targets", string.Empty);

                // Make sure factor name and target name do not have an extension
                mGlyQIQParams.FactorsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.FactorsName);
                mGlyQIQParams.TargetsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.TargetsName);

                if (string.IsNullOrEmpty(mGlyQIQParams.FactorsName))
                {
                    LogError("Factors parameter is empty");
                    return false;
                }

                if (string.IsNullOrEmpty(mGlyQIQParams.TargetsName))
                {
                    LogError("Targets parameter is empty");
                    return false;
                }

                // Retrieve the factors file
                sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Factors");
                sourceFileName = mGlyQIQParams.FactorsName + ".txt";

                if (!CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "Factors File"))
                {
                    return false;
                }

                // Retrieve the Targets file
                sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Libraries");
                sourceFileName = mGlyQIQParams.TargetsName + ".txt";

                if (!CopyFileToWorkDir(sourceFileName, sourceFolderPath, mWorkDir))
                {
                    mMessage += " (Targets File)";
                    return false;
                }

                // There is no need to store the targets file in the job result folder
                mJobParams.AddResultFileToSkip(sourceFileName);

                var fiTargetsFile = new FileInfo(Path.Combine(mWorkDir, sourceFileName));

                // Count the number of targets
                mGlyQIQParams.NumTargets = CountTargets(fiTargetsFile.FullName);
                if (mGlyQIQParams.NumTargets < 1)
                {
                    LogError("Targets file is empty: " + Path.Combine(sourceFolderPath, sourceFileName));
                    return false;
                }

                if (mGlyQIQParams.NumTargets < coreCount)
                {
                    for (var coreToRemove = mGlyQIQParams.NumTargets + 1; coreToRemove <= coreCount; coreToRemove++)
                    {
                        mGlyQIQParams.WorkingParameterFolders[coreToRemove].Delete(true);
                        mGlyQIQParams.WorkingParameterFolders.Remove(coreToRemove);
                    }

                    coreCount = mGlyQIQParams.NumTargets;
                    mJobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString());
                }

                Dictionary<int, FileInfo> splitTargetFileInfo;

                if (mGlyQIQParams.WorkingParameterFolders.Count == 1)
                {
                    // Running on just one core
                    fiTargetsFile.MoveTo(Path.Combine(mGlyQIQParams.WorkingParameterFolders.First().Value.FullName, sourceFileName));

                    splitTargetFileInfo = new Dictionary<int, FileInfo> {
                        { 1, fiTargetsFile}
                    };
                }
                else
                {
                    // Split the targets file based on the number of cores
                    splitTargetFileInfo = SplitTargetsFile(fiTargetsFile, mGlyQIQParams.NumTargets);
                }

                // Retrieve the alignment parameters
                sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles");
                sourceFileName = ALIGNMENT_PARAMETERS_FILENAME;

                if (!CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "AlignmentParameters File"))
                {
                    return false;
                }

                // Retrieve the Executor Parameters
                // Note that the file paths in this file don't matter, but the file is required, so we retrieve it
                sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles");
                sourceFileName = EXECUTOR_PARAMETERS_FILE;

                if (!CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "Executor File"))
                {
                    return false;
                }

                // Create the ConsoleOperating Parameters file
                if (!CreateConsoleOperatingParametersFile())
                {
                    return false;
                }

                // Create the Launcher Batch files (one for each core)
                if (!CreateLauncherBatchFiles(splitTargetFileInfo))
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveGlyQIQParameters";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private bool RetrievePeaksAndRawData()
        {
            try
            {
                var rawDataType = mJobParams.GetJobParameter("RawDataType", "");
                var eRawDataType = GetRawDataType(rawDataType);

                if (eRawDataType == eRawDataTypeConstants.ThermoRawFile)
                {
                    mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                }
                else
                {
                    LogError("GlyQ-IQ presently only supports Thermo .Raw files");
                    return false;
                }

                // Retrieve the _peaks.txt file

                var fileToFind = DatasetName + "_peaks.txt";
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToFind, false, false, out var sourceFolderPath))
                {
                    mMessage = "Could not find the _peaks.txt file; this is typically created by the DeconPeakDetector job step; rerun that job step if it has been deleted";
                    return false;
                }
                mJobParams.AddResultFileToSkip(fileToFind);
                mJobParams.AddResultFileExtensionToSkip("_peaks.txt");

                var diTransferFolder = new DirectoryInfo(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH));
                var diSourceFolder = new DirectoryInfo(sourceFolderPath);
                if ((diSourceFolder.FullName.StartsWith(diTransferFolder.FullName, StringComparison.OrdinalIgnoreCase)))
                {
                    // The Peaks.txt file is in the transfer folder
                    // If the analysis finishes successfully, we can delete the file from the transfer folder
                    mJobParams.AddServerFileToDelete(Path.Combine(sourceFolderPath, fileToFind));
                }

                // Retrieve the instrument data file
                if (!FileSearch.RetrieveSpectra(rawDataType))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error retrieving instrument data file";
                    }

                    LogError("clsAnalysisResourcesGlyQIQ.GetResources: " + mMessage);
                    return false;
                }

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrievePeaksAndRawData";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="fiTargetsFile"></param>
        /// <param name="numTargets"></param>
        /// <returns>List of FileInfo objects for the newly created target files (key is core number, value is the Targets file path)</returns>
        /// <remarks></remarks>
        private Dictionary<int, FileInfo> SplitTargetsFile(FileSystemInfo fiTargetsFile, int numTargets)
        {
            try
            {
                var lstOutputFiles = new Dictionary<int, FileInfo>();

                using (var srReader = new StreamReader(new FileStream(fiTargetsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Read the header line
                    var headerLine = srReader.ReadLine();

                    // Create the output files
                    var lstWriters = new List<StreamWriter>();
                    foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
                    {
                        var core = workingDirectory.Key;

                        var outputFilePath = Path.Combine(workingDirectory.Value.FullName,
                            Path.GetFileNameWithoutExtension(fiTargetsFile.Name) + "_Part" + core + ".txt");
                        lstWriters.Add(new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)));
                        lstOutputFiles.Add(core, new FileInfo(outputFilePath));
                    }

                    // Write the header line ot each writer
                    foreach (var targetFileWriter in lstWriters)
                    {
                        targetFileWriter.WriteLine(headerLine);
                    }

                    // When targetsWritten reaches nextThreshold, we will switch to the next file
                    var nextThreshold = (int)Math.Floor(numTargets / (float)mGlyQIQParams.WorkingParameterFolders.Count);
                    if (nextThreshold < 1)
                        nextThreshold = 1;

                    var targetsWritten = 0;
                    var outputFileIndex = 0;
                    var outputFileIndexMax = mGlyQIQParams.WorkingParameterFolders.Count - 1;

                    // Read the targets
                    while (!srReader.EndOfStream)
                    {
                        var lineIn = srReader.ReadLine();

                        if (outputFileIndex > outputFileIndexMax)
                        {
                            // This shouldn't happen, but double checking to be sure
                            outputFileIndex = outputFileIndexMax;
                        }

                        lstWriters[outputFileIndex].WriteLine(lineIn);

                        targetsWritten += 1;
                        if (targetsWritten >= nextThreshold)
                        {
                            // Advance the output file index
                            outputFileIndex += 1;

                            var newThreshold = (int)Math.Floor(numTargets / (float)mGlyQIQParams.WorkingParameterFolders.Count * (outputFileIndex + 1));
                            if (newThreshold > nextThreshold)
                            {
                                nextThreshold = newThreshold;
                            }
                            else
                            {
                                nextThreshold += 1;
                            }
                        }
                    }

                    foreach (var targetFileWriter in lstWriters)
                    {
                        targetFileWriter.Close();
                    }
                }

                return lstOutputFiles;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in SplitTargetsFile";
                LogError(mMessage + ": " + ex.Message);
                return new Dictionary<int, FileInfo>();
            }
        }
    }
}
