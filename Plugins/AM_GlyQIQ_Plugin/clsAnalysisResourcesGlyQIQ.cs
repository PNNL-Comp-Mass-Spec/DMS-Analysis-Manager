//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/29/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerGlyQIQPlugin
{
    public class clsAnalysisResourcesGlyQIQ : clsAnalysisResources
    {
        // Public Const WORKING_PARAMETERS_FOLDER_NAME As String = "WorkingParameters"
        protected const string LOCKS_FOLDER_NAME = "LocksFolder";

        public const string JOB_PARAM_ACTUAL_CORE_COUNT = "GlyQ_IQ_ActualCoreCount";

        public const string EXECUTOR_PARAMETERS_FILE = "ExecutorParametersSK.xml";
        public const string START_PROGRAM_BATCH_FILE_PREFIX = "StartProgram_Core";
        public const string GLYQIQ_PARAMS_FILE_PREFIX = "GlyQIQ_Params_";

        public const string ALIGNMENT_PARAMETERS_FILENAME = "AlignmentParameters.xml";

        protected struct udtGlyQIQParams
        {
            //Public ApplicationsFolderPath As String
            public Dictionary<int, DirectoryInfo> WorkingParameterFolders;
            public string FactorsName;
            public string TargetsName;
            public int NumTargets;
            //Public TimeStamp As String
            public string ConsoleOperatingParametersFileName;
            //Public OperationParametersFileName As String
            public string IQParamFileName;
        }

        #region "Classwide variables"

        private udtGlyQIQParams mGlyQIQParams;

        #endregion

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            mGlyQIQParams = new udtGlyQIQParams();

            var coreCountText = m_jobParams.GetJobParameter("GlyQ-IQ", "Cores", "All");

            // Use all the cores if the system has 4 or fewer cores
            // Otherwise, use TotalCoreCount - 1
            var maxAllowedCores = m_StatusTools.GetCoreCount();
            if (maxAllowedCores > 4)
                maxAllowedCores -= 1;

            int coreCount = clsAnalysisToolRunnerBase.ParseThreadCount(coreCountText, maxAllowedCores);

            m_jobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString());

            mGlyQIQParams.WorkingParameterFolders = CreateSubFolders(coreCount);
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
                    m_message += " (" + fileDesription + ")";
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
                int numTargets = -1;

                using (var srInFile = new StreamReader(new FileStream(targetsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        srInFile.ReadLine();
                        numTargets += 1;
                    }
                }

                return numTargets;
            }
            catch (Exception ex)
            {
                m_message = "Exception counting the targets in " + Path.GetFileName(targetsFilePath);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return 0;
            }
        }

        private bool CreateConsoleOperatingParametersFile()
        {
            try
            {
                // Define the output file name
                mGlyQIQParams.ConsoleOperatingParametersFileName = GLYQIQ_PARAMS_FILE_PREFIX + DatasetName + ".txt";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                    "Creating the Operating Parameters file, " + mGlyQIQParams.ConsoleOperatingParametersFileName);

                foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
                {
                    var outputFilePath = Path.Combine(workingDirectory.Value.FullName, mGlyQIQParams.ConsoleOperatingParametersFileName);

                    using (var swOutFile = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swOutFile.WriteLine("ResultsFolderPath" + "," + Path.Combine(m_WorkingDir, "Results"));
                        swOutFile.WriteLine("LoggingFolderPath" + "," + Path.Combine(m_WorkingDir, "Results"));
                        swOutFile.WriteLine("FactorsFile" + "," + mGlyQIQParams.FactorsName + ".txt");
                        swOutFile.WriteLine("ExecutorParameterFile" + "," + EXECUTOR_PARAMETERS_FILE);
                        swOutFile.WriteLine("XYDataFolder" + "," + "XYDataWriter");
                        swOutFile.WriteLine("WorkflowParametersFile" + "," + mGlyQIQParams.IQParamFileName);
                        swOutFile.WriteLine("Alignment" + "," + Path.Combine(workingDirectory.Value.FullName, ALIGNMENT_PARAMETERS_FILENAME));

                        // The following file doesn't have to exist
                        swOutFile.WriteLine("BasicTargetedParameters" + "," +
                                            Path.Combine(workingDirectory.Value.FullName, "BasicTargetedWorkflowParameters.xml"));
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in CreateConsoleOperatingParametersFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }
        }

        private bool CreateLauncherBatchFiles(Dictionary<int, FileInfo> splitTargetFileInfo)
        {
            try
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the Launcher batch files");

                // Determine the path to the IQGlyQ program

                var progLoc = clsAnalysisToolRunnerBase.DetermineProgramLocation("GlyQIQ", "GlyQIQProgLoc", "IQGlyQ_Console.exe", "", m_mgrParams, out m_message);
                if (string.IsNullOrEmpty(progLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "DetermineProgramLocation returned an empty string: " + m_message);
                    return false;
                }

                foreach (var workingDirectory in mGlyQIQParams.WorkingParameterFolders)
                {
                    var core = workingDirectory.Key;

                    var batchFilePath = Path.Combine(m_WorkingDir, START_PROGRAM_BATCH_FILE_PREFIX + workingDirectory.Key + ".bat");

                    using (var swOutFile = new StreamWriter(new FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Note that clsGlyQIqRunner expects this batch file to be in a specific format:
                        // GlyQIQProgramPath "WorkingDirectoryPath" "DatasetName" "DatasetSuffix" "TargetsFileName" "ParamFileName" "WorkingParametersFolderPath" "LockFileName" "ResultsFolderPath" "CoreNumber"
                        //
                        // It will read and parse the batch file to determine the TargetsFile name and folder path so that it can cache the target code values
                        // Thus, if you change this code, also update clsGlyQIqRunner

                        swOutFile.Write(clsGlobal.PossiblyQuotePath(progLoc));

                        swOutFile.Write(" " + "\"" + m_WorkingDir + "\"");
                        swOutFile.Write(" " + "\"" + DatasetName + "\"");
                        swOutFile.Write(" " + "\"" + "raw" + "\"");

                        FileInfo targetsFile = null;
                        if (!splitTargetFileInfo.TryGetValue(core, out targetsFile))
                        {
                            LogError("Logic error; core " + core + " not found in dictionary splitTargetFileInfo");
                            return false;
                        }

                        swOutFile.Write(" " + "\"" + targetsFile.Name + "\"");

                        swOutFile.Write(" " + "\"" + mGlyQIQParams.ConsoleOperatingParametersFileName + "\"");

                        swOutFile.Write(" " + "\"" + workingDirectory.Value.FullName + "\"");

                        swOutFile.Write(" " + "\"" + "Lock_" + core + "\"");

                        swOutFile.Write(" " + "\"" + Path.Combine(m_WorkingDir, "Results") + "\"");

                        swOutFile.Write(" " + "\"" + core + "\"");

                        swOutFile.WriteLine();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in CreateLauncherBatchFiles";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }
        }

        private Dictionary<int, DirectoryInfo> CreateSubFolders(int coreCount)
        {
            try
            {
                // Make sure that required subfolders exist in the working directory
                var lstWorkingDirectories = new Dictionary<int, DirectoryInfo>();

                for (var core = 1; core <= coreCount; core++)
                {
                    string folderName = "WorkingParametersCore" + core;

                    lstWorkingDirectories.Add(core, new DirectoryInfo(Path.Combine(m_WorkingDir, folderName)));
                }

                foreach (var workingDirectory in lstWorkingDirectories)
                {
                    if (!workingDirectory.Value.Exists)
                    {
                        workingDirectory.Value.Create();
                    }

                    var diLocksFolder = new DirectoryInfo(Path.Combine(workingDirectory.Value.FullName, LOCKS_FOLDER_NAME));
                    if (!diLocksFolder.Exists)
                        diLocksFolder.Create();
                }

                return lstWorkingDirectories;
            }
            catch (Exception ex)
            {
                m_message = "Exception in CreateSubFolders";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return new Dictionary<int, DirectoryInfo>();
            }
        }

        private bool RetrieveGlyQIQParameters(int coreCount)
        {
            try
            {
                string sourceFolderPath = null;
                string sourceFileName = null;

                // Define the base source folder path
                // Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ
                var paramFileStoragePathBase = m_jobParams.GetParam("ParmFileStoragePath");

                mGlyQIQParams.IQParamFileName = m_jobParams.GetJobParameter("ParmFileName", "");
                if (string.IsNullOrEmpty(mGlyQIQParams.IQParamFileName))
                {
                    LogError("Job Parameter File name is empty");
                    return false;
                }

                // Retrieve the GlyQ-IQ parameter file
                // Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ\ParameterFiles
                sourceFolderPath = Path.Combine(paramFileStoragePathBase, "ParameterFiles");
                sourceFileName = string.Copy(mGlyQIQParams.IQParamFileName);

                if (!CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "IQ Parameter File"))
                {
                    return false;
                }

                mGlyQIQParams.FactorsName = m_jobParams.GetJobParameter("Factors", string.Empty);
                mGlyQIQParams.TargetsName = m_jobParams.GetJobParameter("Targets", string.Empty);

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

                if (!CopyFileToWorkDir(sourceFileName, sourceFolderPath, m_WorkingDir))
                {
                    m_message += " (Targets File)";
                    return false;
                }

                // There is no need to store the targets file in the job result folder
                m_jobParams.AddResultFileToSkip(sourceFileName);

                var fiTargetsFile = new FileInfo(Path.Combine(m_WorkingDir, sourceFileName));

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
                    m_jobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString());
                }

                Dictionary<int, FileInfo> splitTargetFileInfo;

                if (mGlyQIQParams.WorkingParameterFolders.Count == 1)
                {
                    // Running on just one core
                    fiTargetsFile.MoveTo(Path.Combine(mGlyQIQParams.WorkingParameterFolders.First().Value.FullName, sourceFileName));

                    splitTargetFileInfo = new Dictionary<int, FileInfo>();
                    splitTargetFileInfo.Add(1, fiTargetsFile);
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
                m_message = "Exception in RetrieveGlyQIQParameters";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }
        }

        private bool RetrievePeaksAndRawData()
        {
            try
            {
                string rawDataType = m_jobParams.GetJobParameter("RawDataType", "");
                var eRawDataType = GetRawDataType(rawDataType);

                if (eRawDataType == eRawDataTypeConstants.ThermoRawFile)
                {
                    m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                }
                else
                {
                    LogError("GlyQ-IQ presently only supports Thermo .Raw files");
                    return false;
                }

                // Retrieve the _peaks.txt file
                string fileToFind = null;
                string sourceFolderPath = string.Empty;

                fileToFind = DatasetName + "_peaks.txt";
                if (!FileSearch.FindAndRetrieveMiscFiles(fileToFind, unzip: false, searchArchivedDatasetFolder: false, sourceFolderPath: out sourceFolderPath))
                {
                    m_message = "Could not find the _peaks.txt file; this is typically created by the DeconPeakDetector job step; rerun that job step if it has been deleted";
                    return false;
                }
                m_jobParams.AddResultFileToSkip(fileToFind);
                m_jobParams.AddResultFileExtensionToSkip("_peaks.txt");

                var diTransferFolder = new DirectoryInfo(m_jobParams.GetParam("transferFolderPath"));
                var diSourceFolder = new DirectoryInfo(sourceFolderPath);
                if ((diSourceFolder.FullName.ToLower().StartsWith(diTransferFolder.FullName.ToLower())))
                {
                    // The Peaks.txt file is in the transfer folder
                    // If the analysis finishes successfully, then we can delete the file from the transfer folder
                    m_jobParams.AddServerFileToDelete(Path.Combine(sourceFolderPath, fileToFind));
                }

                // Retrieve the instrument data file
                if (!FileSearch.RetrieveSpectra(rawDataType))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error retrieving instrument data file";
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "clsAnalysisResourcesGlyQIQ.GetResources: " + m_message);
                    return false;
                }

                if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrievePeaksAndRawData";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
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
        private Dictionary<int, FileInfo> SplitTargetsFile(FileInfo fiTargetsFile, int numTargets)
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
                            Path.GetFileNameWithoutExtension(fiTargetsFile.Name) + "_Part" + core.ToString() + ".txt");
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
                m_message = "Exception in SplitTargetsFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return new Dictionary<int, FileInfo>();
            }
        }
    }
}
