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

namespace AnalysisManagerFragPipePlugIn
{
    /// <summary>
    /// Class for running FragPipe
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerFragPipe : AnalysisToolRunnerBase
    {
        // Ignore Spelling: dia, frag

        private const string FRAGPIPE_INSTANCE_DIRECTORY = "fragpipe_v22.0";

        private const string FRAGPIPE_BATCH_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\bin\fragpipe.bat";

        private const string FRAGPIPE_TOOLS_DIRECTORY_PATH= FRAGPIPE_INSTANCE_DIRECTORY + @"\tools";

        private const string FRAGPIPE_DIANN_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\tools\diann\1.8.2_beta_8\win\DiaNN.exe";

        private const string FRAGPIPE_CONSOLE_OUTPUT = "FragPipe_ConsoleOutput.txt";

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string PIN_EXTENSION = ".pin";

        internal const float PROGRESS_PCT_INITIALIZING = 1;

        private enum ProgressPercentValues
        {
            Initializing = 0,
            StartingFragPipe = 1,
            FragPipeComplete = 95,
            ProcessingComplete = 99
        }

        private RunDosProgram mCmdRunner;

        private string mConsoleOutputErrorMsg;

        /// <summary>
        /// Dictionary of experiment group working directories
        /// </summary>
        /// <remarks>
        /// Keys are experiment group name, values are the corresponding working directory
        /// </remarks>
        private Dictionary<string, DirectoryInfo> mExperimentGroupWorkingDirectories;

        // Populate this with a tool version reported to the console
        private string mFragPipeVersion;

        private string mFragPipeProgLoc;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private bool mToolVersionWritten;

        private DirectoryInfo mWorkingDirectory;

        private static ZipFileTools mZipTool;

        /// <summary>
        /// Runs FragPipe tool
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
                    LogDebug("AnalysisToolRunnerFragPipe.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                mExperimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>();

                mWorkingDirectory = new DirectoryInfo(mWorkDir);

                // Determine the path to FragPipe

                // ReSharper disable once CommentTypo
                // Construct the relative path to the FragPipe batch file, for example:
                // fragpipe_v22.0\bin\fragpipe.bat

                mFragPipeProgLoc = DetermineProgramLocation("FragPipeProgLoc", FRAGPIPE_BATCH_FILE_PATH);

                if (string.IsNullOrWhiteSpace(mFragPipeProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the FragPipe version info in the database after the first line is written to file FragPipe_ConsoleOutput.txt
                mToolVersionWritten = false;
                mFragPipeVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the mzML files using FragPipe
                var processingResult = StartFragPipe(fastaFile);

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
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
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
                LogError("Error in FragPipePlugin->RunTool", ex);
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

        /// <summary>
        /// Create the manifest file, which lists the input .mzML files, the experiment for each (optional), and the data type for each
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="manifestFilePath">Output: manifest file path</param>
        /// <param name="diaSearchEnabled">Output: true if the data type is DIA, DIA-Quant, or DIA-Lib</param>
        private bool CreateManifestFile(DataPackageInfo dataPackageInfo, out string manifestFilePath, out bool diaSearchEnabled)
        {
            diaSearchEnabled = false;

            try
            {
                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("DatasetFiles list in dataPackageInfo is empty");
                    manifestFilePath = string.Empty;
                    return false;
                }

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

                    LogError("Files in dataPackageInfo.DatasetFiles do not all have the same file extension; expecting {0} but found {1}",
                        datasetFileExtension, fileExtension);

                    manifestFilePath = string.Empty;
                    return false;
                }

                manifestFilePath = Path.Combine(mWorkDir, "datasets.fp-manifest");

                using var writer = new StreamWriter(new FileStream(manifestFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var outputDirectoryPaths = new SortedSet<string>();

                foreach (var item in dataPackageInfo.DatasetFiles)
                {
                    var datasetID = item.Key;
                    var datasetFilePath = item.Value;

                    string experimentNameToUse;

                    if (dataPackageInfo.Experiments.TryGetValue(datasetID, out var experimentName))
                    {
                        experimentNameToUse = experimentName;
                    }
                    else
                    {
                        experimentNameToUse = "Results";
                    }

                    outputDirectoryPaths.Add(Path.Combine(mWorkDir, experimentNameToUse));

                    string dataType;

                    // Data types supported by FragPipe: DDA, DDA+, DIA, DIA-Quant, DIA-Lib
                    // This method chooses either DDA or DIA, depending on the dataset type

                    if (dataPackageInfo.DatasetTypes.TryGetValue(datasetID, out var datasetType))
                    {
                        dataType = datasetType.Contains("DIA") ? "DIA" : "DDA";
                    }
                    else
                    {
                        dataType = "DDA";
                    }

                    const string BIOREPLICATE = "";

                    writer.WriteLine("{0}\t{1}\t{2}\t{3}", datasetFilePath, experimentNameToUse, BIOREPLICATE, dataType);
                }

                // Make sure each of the output directories exists
                foreach (var outputDirectoryPath in outputDirectoryPaths)
                {
                    var outputDirectory = new DirectoryInfo(outputDirectoryPath);
                    if (!outputDirectory.Exists)
                        outputDirectory.Create();
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateManifestFile", ex);
                manifestFilePath = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Get appropriate path of the working directory for the given experiment
        /// </summary>
        /// <remarks>
        /// <para>If all the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        /// <param name="experimentGroupName">Experiment group name</param>
        /// <param name="experimentGroupCount">Experiment group count</param>
        private DirectoryInfo GetExperimentGroupWorkingDirectory(string experimentGroupName, int experimentGroupCount)
        {
            if (experimentGroupCount <= 1)
                return mWorkingDirectory;

            var cleanName = Global.ReplaceInvalidPathChars(experimentGroupName);

            return new DirectoryInfo(Path.Combine(mWorkingDirectory.FullName, cleanName));
        }

        private static List<FileInfo> FindDatasetPinFileAndPepXmlFiles(
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
        /// Given a linked list of progress values (which should have been populated in ascending order), find the next progress value
        /// </summary>
        /// <param name="progressValues">List of progress values</param>
        /// <param name="currentProgress">Current progress</param>
        /// <returns>Next progress value, or 100 if either the current value is not found, or the next value is not defined</returns>
        private static int GetNextProgressValue(LinkedList<int> progressValues, int currentProgress)
        {
            var currentNode = progressValues.Find(currentProgress);

            if (currentNode?.Next == null)
                return 100;

            return currentNode.Next.Value;
        }

        /// <summary>
        /// Determine the number of threads to use for FragPipe
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

        private bool MoveDatasetsIntoSubdirectories(DataPackageInfo dataPackageInfo, SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            try
            {
                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var datasetId in item.Value)
                    {
                        var datasetFile = dataPackageInfo.DatasetFiles[datasetId];

                        var sourceDirectoryPath = mWorkingDirectory.FullName;
                        var targetDirectoryPath = experimentWorkingDirectory.FullName;

                        var success = MoveFile(sourceDirectoryPath, datasetFile, targetDirectoryPath);

                        if (!success)
                            return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveResultsIntoSubdirectories", ex);
                return false;
            }
        }

        private bool MoveFile(string sourceDirectoryPath, string fileName, string targetDirectoryPath)
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(sourceDirectoryPath, fileName));

                var targetPath = Path.Combine(targetDirectoryPath, sourceFile.Name);

                sourceFile.MoveTo(targetPath);

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error in MoveFile for {0}", fileName), ex);
                return false;
            }
        }

        /// <summary>
        /// Organize .mzML files and populate several dictionaries
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment group
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// </param>
        /// <returns>Result code</returns>
        private CloseOutType OrganizeDatasetFiles(
            out DataPackageInfo dataPackageInfo,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            // Keys in this dictionary are experiment group names, values are the working directory to use
            mExperimentGroupWorkingDirectories.Clear();

            // If this job applies to a single dataset, dataPackageID will be 0
            // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
            dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            var dataPackageDatasets = dataPackageInfo.GetDataPackageDatasets();

            datasetIDsByExperimentGroup = DataPackageInfoLoader.GetDataPackageDatasetsByExperimentGroup(dataPackageDatasets);

            // If Experiment Groups are defined, create a subdirectory for each experiment group
            // If using TMT, put a separate annotation.txt file in each subdirectory (the filename must end with "annotation.txt")

            var experimentGroupNames = new SortedSet<string>();

            foreach (var item in datasetIDsByExperimentGroup.Keys)
            {
                experimentGroupNames.Add(item);
            }

            var experimentCount = experimentGroupNames.Count;

            // Populate a dictionary with experiment group names and corresponding working directories
            foreach (var experimentGroupName in experimentGroupNames)
            {
                var workingDirectory = GetExperimentGroupWorkingDirectory(experimentGroupName, experimentCount);

                mExperimentGroupWorkingDirectories.Add(experimentGroupName, workingDirectory);
            }

            if (experimentCount <= 1)
                return CloseOutType.CLOSEOUT_SUCCESS;

            // Since we have multiple experiment groups, create a subdirectory for each one
            foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
            {
                if (!experimentGroupDirectory.Exists)
                {
                    experimentGroupDirectory.Create();
                }
            }

            // Since we have multiple experiment groups, move the pepXML and .pin files into subdirectories
            var moveSuccess = MoveDatasetsIntoSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Parse the FragPipe console output file to determine the FragPipe version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        private void ParseFragPipeConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            // System OS: Windows 11, Architecture: AMD64
            // Java Info: 11.0.24, OpenJDK 64-Bit Server VM, Eclipse Adoptium
            // .NET Core Info: 6.0.425
            //
            // Version info:
            // FragPipe version 22.0
            // MSFragger version 4.1
            // IonQuant version 1.10.27
            // diaTracer version 1.1.5
            // Philosopher version 5.1.1
            //
            // LCMS files:
            //   Experiment/Group: NYBB_30_P01_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   Experiment/Group: NYBB_30_P02_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //
            // 68 commands to execute:
            // CheckCentroid
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Xmx9G -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar com.dmtavt.fragpipe.util.CheckCentroid C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 15
            // WorkspaceCleanInit [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // ...
            // MSFragger [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -jar -Dfile.encoding=UTF-8 -Xmx9G C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\MSFragger-4.1.jar C:\DMS_WorkDir\fragger.params C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML
            // MSFragger move pepxml
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pepXML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pepXML
            // MSFragger move tsv
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.tsv C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.tsv
            // MSFragger move pin
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pin C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pin
            // Percolator [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\percolator_3_6_5\windows\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 15 --results-psms NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv --decoy-results-psms NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.pin
            // ...
            // PTMProphet [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\PTMProphet\PTMProphetParser.exe NOSTACK KEEPOLD STATIC EM=1 NIONS=b STY:79.966331,M:15.9949 MINPROB=0.5 MAXTHREADS=1 interact-NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.pep.xml interact-NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml
            // ...
            // ProteinProphet [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe proteinprophet --maxppmdiff 2000000 --minprob 0.5 --output combined C:\DMS_WorkDir\filelist_proteinprophet.txt
            // PhilosopherDbAnnotate [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe database --annotate C:\FragPipe_TestTMT_Phospho_Multi_TwoGroups\ID_008380_0E5568A3.fasta --prefix XXX_
            // PhilosopherFilter [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml C:\DMS_WorkDir\NYBB_30_P01_P --protxml C:\DMS_WorkDir\combined.prot.xml --razor
            // PhilosopherFilter [Work dir: C:\DMS_WorkDir\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml C:\DMS_WorkDir\NYBB_30_P02_P --dbbin C:\DMS_WorkDir\NYBB_30_P01_P --protxml C:\DMS_WorkDir\combined.prot.xml --probin C:\DMS_WorkDir\NYBB_30_P01_P --razor
            // PhilosopherReport [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // PhilosopherReport [Work dir: C:\DMS_WorkDir\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // WorkspaceClean [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // IonQuant [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx9G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\thermo -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\IonQuant-1.10.27.jar ionquant.IonQuant --threads 15 --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10 --ionmobility 0 --site-reports 0 --msstats 0 --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist C:\DMS_WorkDir\filelist_ionquant.txt --modlist C:\DMS_WorkDir\modmasses_ionquant.txt
            // IonQuant [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx9G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\thermo -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\IonQuant-1.10.27.jar ionquant.IonQuant --threads 15 --perform-ms1quant 0 --perform-isoquant 1 --isotol 20.0 --isolevel 2 --isotype TMT-16 --ionmobility 0 --site-reports 0 --msstats 0 --annotation C:\DMS_WorkDir\NYBB_30_P01_P\psm.tsv=C:\DMS_WorkDir\NYBB_30_P01_P\annotation_01_annotation.txt --annotation C:\DMS_WorkDir\NYBB_30_P02_P\psm.tsv=C:\DMS_WorkDir\NYBB_30_P02_P\annotation_02_annotation.txt --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist C:\DMS_WorkDir\filelist_ionquant.txt --modlist C:\DMS_WorkDir\modmasses_ionquant.txt
            // TmtIntegrator [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Xmx9G -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\tmt-integrator-5.0.9.jar tmtintegrator.TMTIntegrator C:\DMS_WorkDir\tmt-integrator-conf.yml C:\DMS_WorkDir\NYBB_30_P01_P\psm.tsv C:\DMS_WorkDir\NYBB_30_P02_P\psm.tsv
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // Execution order:
            //
            //     Cmd: [START], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [CheckCentroid], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir\NYBB_30_P01_P]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir\NYBB_30_P02_P]
            //     Cmd: [MSFragger], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [Percolator], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PTMProphet], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [ProteinProphet], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherDbAnnotate], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherFilter], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherReport], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir\NYBB_30_P01_P]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir\NYBB_30_P02_P]
            //     Cmd: [IonQuant], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [IonQuant], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [TmtIntegrator], Work dir: [C:\DMS_WorkDir]
            //
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~Sample of C:\FragPipe_TestTMT_Phospho_Multi_TwoGroups\ID_008380_0E5568A3.fasta~~~~~~~
            // >Contaminant_ALBU_BOVIN SERUM ALBUMIN PRECURSOR. (sp|P02769|ALBU_BOVIN, gi|113574)
            // >XXX_sp|O43741|AAKB2_HUMAN 5'-AMP-activated protein kinase subunit beta-2 OS=Homo sapiens OX=9606 GN=PRKAB2 PE=1 SV=1
            // >XXX_sp|P0DPD6|ECE2_HUMAN Endothelin-converting enzyme 2 OS=Homo sapiens OX=9606 GN=ECE2 PE=1 SV=1
            // ...
            // >XXX_sp|Q9Y6X6|MYO16_HUMAN Unconventional myosin-XVI OS=Homo sapiens OX=9606 GN=MYO16 PE=1 SV=3
            // >sp|O43776|SYNC_HUMAN Asparagine--tRNA ligase, cytoplasmic OS=Homo sapiens OX=9606 GN=NARS1 PE=1 SV=1
            // >sp|P0DPI2|GAL3A_HUMAN Glutamine amidotransferase-like class 1 domain-containing protein 3, mitochondrial OS=Homo sapiens OX=9606 GN=GATD3 PE=1 SV=1
            // ...
            // >sp|Q9NVI1|FANCI_HUMAN Fanconi anemia group I protein OS=Homo sapiens OX=9606 GN=FANCI PE=1 SV=4
            // >sp|S4R3Y5|HMN11_HUMAN Humanin-like 11 OS=Homo sapiens OX=9606 GN=MTRNR2L11 PE=3 SV=1
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~annotation files~~~~~~~
            // C:\DMS_WorkDir\NYBB_30_P01_P\annotation_01_annotation.txt:
            // 126 sample-01
            // 127N sample-02
            // 127C sample-03
            // 128N sample-04
            // 128C sample-05
            // 129N sample-06
            // 129C sample-07
            // 130N sample-08
            // 130C sample-09
            // 131N sample-10
            // 131C sample-11
            // 132N sample-12
            // 132C sample-13
            // 133N sample-14
            // 133C sample-15
            // 134N sample-16
            // C:\DMS_WorkDir\NYBB_30_P02_P\annotation_02_annotation.txt:
            // 126 sample-01
            // 127N sample-02
            // ...
            // 133C sample-15
            // 134N sample-16
            // ~~~~~~~~~~~~~~~~~~~~~~
            //

            // ReSharper restore CommentTypo

            const int CHECK_CENTROID = (int)ProgressPercentValues.StartingFragPipe + 1;

            const int FRAG_PIPE_COMPLETE = (int)ProgressPercentValues.FragPipeComplete;

            var processingSteps = new SortedList<int, Regex>
            {
                { CHECK_CENTROID          , GetRegEx("^CheckCentroid") },
                { CHECK_CENTROID + 1      , GetRegEx("^MSFragger *") },
                { 50                      , GetRegEx("^Percolator *") },
                { 55                      , GetRegEx("^PTMProphet *") },
                { 60                      , GetRegEx("^ProteinProphet *") },
                { 70                      , GetRegEx("^PhilosopherDbAnnotate *") },
                { 75                      , GetRegEx("^PhilosopherFilter *") },
                { 80                      , GetRegEx("^PhilosopherReport *") },
                { 85                      , GetRegEx("^IonQuant *") },
                { 90                      , GetRegEx("^TmtIntegrator *") },
                { FRAG_PIPE_COMPLETE      , GetRegEx("^Please cite") }
            };

            // Use a linked list to keep track of the progress values
            // This makes lookup of the next progress value easier
            var progressValues = new LinkedList<int>();

            foreach (var item in (from progressValue in processingSteps.Keys orderby progressValue select progressValue))
            {
                progressValues.AddLast(item);
            }

            progressValues.AddLast(100);

            /*
             * ToDo: determine if any of these should be used to compute progress more accurately
             * For implementation details, see the MSFragger plugin

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
             */

            var linesRead = 0;

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

                /*
                 * ToDo: remove these if not needed

                var currentSlice = 0;
                var totalSlices = 0;

                var currentSplitFastaFile = 0;
                var splitFastaFileCount = 0;

                var currentDatasetId = 0;
                float datasetProgress = 0;
                */

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var versionFound = false;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (!versionFound)
                    {
                        // Determine the FragPipe version

                        if (string.IsNullOrEmpty(mFragPipeVersion) &&
                            dataLine.StartsWith("FragPipe version", StringComparison.OrdinalIgnoreCase))
                        {
                            LogDebug(dataLine, mDebugLevel);
                            mFragPipeVersion = string.Copy(dataLine);
                        }

                        // The next few lines should have version numbers for additional programs, including MSFragger, IonQuant, and Philosopher

                        while (!reader.EndOfStream)
                        {
                            var versionInfo = reader.ReadLine();
                            linesRead++;

                            if (!string.IsNullOrWhiteSpace(versionInfo) && versionInfo.Contains(" version"))
                            {
                                mFragPipeVersion = string.Format("{0}; {1}", mFragPipeVersion, versionInfo);
                                versionFound = true;
                            }
                        }

                        continue;
                    }

                    foreach (var processingStep in processingSteps)
                    {
                        if (!processingStep.Value.IsMatch(dataLine))
                            continue;

                        currentProgress = processingStep.Key;
                        break;
                    }

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running FragPipe: " + dataLine;
                    }
                }

                var effectiveProgressOverall = currentProgress;

                if (float.IsNaN(effectiveProgressOverall))
                {
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
                        "Error parsing the FragPipe console output file at line {0} ({1}): {2}",
                        linesRead,
                        consoleOutputFilePath, ex.Message));
                }
            }
        }

        private CloseOutType StartFragPipe(FileInfo fastaFile)
        {
            try
            {
                LogMessage("Preparing to run FragPipe");

                var moveFilesSuccess = OrganizeDatasetFiles(
                    out var dataPackageInfo,
                    out var datasetIDsByExperimentGroup);

                if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return moveFilesSuccess;
                }

                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty)");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Create the manifest file
                var manifestCreated = CreateManifestFile(dataPackageInfo, out var manifestFilePath, out var diaSearchEnabled);

                if (!manifestCreated)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Customize the path to the FASTA file
                var workflowResultCode = UpdateFragPipeWorkflowFile(out var workflowFilePath, out var databaseSplitCount);

                if (workflowResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return workflowResultCode;
                }

                if (string.IsNullOrWhiteSpace(workflowFilePath))
                {
                    LogError("FragPipe workflow file name returned by UpdateFragPipeParameterFile is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var datasetCount = dataPackageInfo.DatasetFiles.Count;

                var options = new FragPipeOptions(mJobParams, datasetCount);
                RegisterEvents(options);

                options.LoadFragPipeOptions(workflowFilePath);

                LogMessage("Running FragPipe");
                mProgress = (int)ProgressPercentValues.StartingFragPipe;
                ResetProgRunnerCpuUsage();

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;

                // Set up and execute a program runner to run FragPipe

                var processingSuccess = StartFragPipe(fastaFileSizeMB, manifestFilePath, workflowFilePath, options);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mFragPipeVersion))
                    {
                        ParseFragPipeConsoleOutputFile(Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    LogError("Error running FragPipe");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("FragPipe returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to FragPipe failed (but exit code is 0)");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var successCount = 0;

                // Validate that FragPipe created a .pepXML file for each dataset
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
                            // FragPipe did not create any .pepXML files for dataset
                            LogError(string.Format("FragPipe did not create any .pepXML files{0}", optionalDatasetInfo));
                        }
                        else
                        {
                            // FragPipe did not create a .pepXML file for dataset
                            LogError(string.Format("FragPipe did not create a .pepXML file{0}", optionalDatasetInfo));
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
                            LogError(string.Format("FragPipe did not create a .tsv file{0}", optionalDatasetInfo));
                        }

                        // ToDo: create a .tsv file using the .pepXML file
                    }

                    if (!pinFile.Exists && !splitFastaSearch)
                    {
                        LogError(string.Format("FragPipe did not create a .pin file{0}", optionalDatasetInfo));
                    }

                    var zipSuccess = ZipPepXmlAndPinFiles(this, dataPackageInfo, datasetName, pepXmlFiles, pinFile.Exists);

                    if (!zipSuccess)
                        continue;

                    mJobParams.AddResultFileExtensionToSkip(PEPXML_EXTENSION);
                    mJobParams.AddResultFileExtensionToSkip(PIN_EXTENSION);

                    successCount++;
                }

                mStatusTools.UpdateAndWrite(mProgress);
                LogDebug("FragPipe Search Complete", mDebugLevel);

                return successCount == dataPackageInfo.Datasets.Count ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in StartFragPipe", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartFragPipe(
            double fastaFileSizeMB,
            string manifestFilePath,
            string workflowFilePath,
            FragPipeOptions options)
        {
            // Larger FASTA files need more memory
            // Additional memory is also required as the number of dynamic mods being considered increases

            // 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods

            var dynamicModCount = options.GetDynamicModResidueCount();

            var fragPipeMemorySizeGB = AnalysisResourcesFragPipe.GetFragPipeMemorySizeToUse(mJobParams, fastaFileSizeMB, dynamicModCount, out var fragPipeMemorySizeMB);

            if (fragPipeMemorySizeGB > fragPipeMemorySizeMB / 1024.0)
            {
                var dynamicModCountDescription = FragPipeOptions.GetDynamicModCountDescription(dynamicModCount);

                var msg = string.Format("Allocating {0:N0} GB to FragPipe for a {1:N0} MB FASTA file and {2}", fragPipeMemorySizeGB, fastaFileSizeMB, dynamicModCountDescription);
                LogMessage(msg);

                mEvalMessage = Global.AppendToComment(mEvalMessage, msg);
            }

            if (Global.RunningOnDeveloperComputer())
            {
                var freeMemoryMB = Global.GetFreeMemoryMB();

                if (fragPipeMemorySizeGB * 1024 > freeMemoryMB * 0.9)
                {
                    var memorySizeToUse = (int)Math.Floor(freeMemoryMB * 0.9 / 1024);

                    ConsoleMsgUtils.ShowWarning(
                        "Decreasing FragPipe memory size from {0:N0} GB to {1:N0} GB since running on developer machine and not enough free memory",
                        fragPipeMemorySizeGB, memorySizeToUse);

                    fragPipeMemorySizeGB = memorySizeToUse;
                    ConsoleMsgUtils.SleepSeconds(2);
                }
            }

            var numThreadsToUse = GetNumThreadsToUse();

            // ReSharper disable CommentTypo

            // Example command line:
            // fragpipe.bat --headless --ram 0 --threads 15 --workflow C:\DMS_WorkDir\FragPipe_TMT16-phospho_2024-09-09.workflow --manifest C:\DMS_WorkDir\datasets.fp-manifest --workdir C:\DMS_WorkDir --config-tools-folder C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools --config-diann C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\diann\1.8.2_beta_8\win\DiaNN.exe --config-python C:\Python3\python.exe

            // ReSharper restore CommentTypo

            // ToDo: Determine these using a new method
            var toolsDirectory = new DirectoryInfo(@"C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools");
            var diannExePath = new FileInfo(@"C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\diann\1.8.2_beta_8\win\DiaNN.exe");
            var pythonExePath = new FileInfo(@"C:\Python3\python.exe");

            var arguments = new StringBuilder();

            // ReSharper disable once StringLiteralTypo
            arguments.AppendFormat("--headless --ram {0} --threads {1}", fragPipeMemorySizeGB, numThreadsToUse);

            arguments.AppendFormat(" --workflow {0}", workflowFilePath);

            arguments.AppendFormat(" --manifest {0}", manifestFilePath);

            arguments.AppendFormat(" --workdir {0}", mWorkDir);
            arguments.AppendFormat(" --config-tools-folder {0}", toolsDirectory.FullName);
            arguments.AppendFormat(" --config-diann {0}", diannExePath.FullName);
            arguments.AppendFormat(" --config-python {0}", pythonExePath.FullName);

            LogDebug(mFragPipeProgLoc + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(mFragPipeProgLoc, arguments.ToString(), "FragPipe", true);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info", mDebugLevel);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mFragPipeProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(mFragPipeProgLoc, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the FASTA file name defined in the FragPipe workflow file
        /// </summary>
        /// <param name="workflowFilePath">Output: workflow file path</param>
        /// <param name="databaseSplitCount">Output: database split count</param>
        private CloseOutType UpdateFragPipeWorkflowFile(out string workflowFilePath, out int databaseSplitCount)
        {
            const string FASTA_FILE_COMMENT = "FASTA File (should include decoy proteins)";
            const string FILE_FORMAT_COMMENT = "File format of output files; Percolator uses .pin files";

            const string REQUIRED_OUTPUT_FORMAT = "tsv_pepxml_pin";

            const string DATABASE_PATH_PARAMETER = "database.db-path";
            const string OUTPUT_FORMAT_PARAMETER = "msfragger.output_format";
            const string SLICE_DB_PARAMETER = "msfragger.misc.slice-db";

            try
            {
                var workflowFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName + ".new"));

                databaseSplitCount = mJobParams.GetJobParameter(
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_SECTION,
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_PARAM,
                    1);

                var fastaFileDefined = false;
                var outputFormatDefined = false;
                var dbSplitCountDefined = false;

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

                        if (trimmedLine.StartsWith(DATABASE_PATH_PARAMETER))
                        {
                            if (fastaFileDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FASTA_FILE_COMMENT);

                            // On Windows, the path to the FASTA file must use '\\' for directory separators, instead of '\'

                            var pathToUse = mLocalFASTAFilePath.Replace(@"\", @"\\");

                            WriteWorkflowFileSetting(writer, DATABASE_PATH_PARAMETER, pathToUse, comment);

                            fastaFileDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith(OUTPUT_FORMAT_PARAMETER))
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
                                WriteWorkflowFileSetting(writer, OUTPUT_FORMAT_PARAMETER, REQUIRED_OUTPUT_FORMAT, comment);

                                LogWarning("Auto-updated the MSFragger output format from {0} to {1} because Percolator requires .pin files", setting.ParamValue, REQUIRED_OUTPUT_FORMAT);
                            }

                            outputFormatDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith(SLICE_DB_PARAMETER))
                        {
                            if (dbSplitCountDefined)
                                continue;

                            writer.WriteLine("{0}={1}", SLICE_DB_PARAMETER, Math.Max(1, databaseSplitCount));

                            dbSplitCountDefined = true;
                            continue;
                        }

                        writer.WriteLine(dataLine);
                    }

                    if (!fastaFileDefined)
                    {
                        WriteWorkflowFileSetting(writer, DATABASE_PATH_PARAMETER, mLocalFASTAFilePath, FASTA_FILE_COMMENT);
                    }

                    if (!outputFormatDefined)
                    {
                        WriteWorkflowFileSetting(writer, OUTPUT_FORMAT_PARAMETER, REQUIRED_OUTPUT_FORMAT, FILE_FORMAT_COMMENT);
                    }
                }

                // Replace the original parameter file with the updated one
                sourceFile.Delete();
                updatedFile.MoveTo(Path.Combine(mWorkDir, workflowFileName));

                workflowFilePath = updatedFile.FullName;

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error updating the FragPipe workflow file", ex);
                workflowFilePath = string.Empty;
                databaseSplitCount = 1;
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
                // FragPipe will be unable to optimize parameters and Peptide Prophet will likely fail
                LogError(warningMessage, true);

                // Abort processing
                return false;
            }

            // Copy the FASTA file to the working directory
            // This is done because FragPipe indexes the file based on the dynamic and static mods,
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

        private void WriteWorkflowFileSetting(TextWriter writer, string paramName, string paramValue, string comment)
        {
            if (!string.IsNullOrWhiteSpace(comment))
            {
                if (comment.Trim().StartsWith("#"))
                {
                    writer.WriteLine(comment.Trim());
                }
                else
                {
                    writer.WriteLine("# {0}", comment.Trim());
                }
            }

            writer.WriteLine("{0}={1}", paramName, paramValue);
        }

        /// <summary>
        /// Zip the .pepXML file(s) created by FragPipe
        /// </summary>
        /// <param name="toolRunner">Tool runner instance (since this is a static method)</param>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="pepXmlFiles">Typically this is a single .pepXML file, but for DIA searches, this is a set of .pepXML files</param>
        /// <param name="addPinFile">When true, add the .pin file to the first zipped .pepXML file</param>
        /// <returns>True if success, false if an error</returns>
        private static bool ZipPepXmlAndPinFiles(
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

                // pepXML file created by FragPipe is empty for dataset
                toolRunner.LogError("pepXML file created by FragPipe is empty{0}", optionalDatasetInfo);
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

            toolRunner.LogError("Zip failure for {0} / {1} .pepXML files created by FragPipe", additionalPepXmlFiles.Count - successCount, additionalPepXmlFiles.Count);

            return false;
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Zip the .pepXML file created by FragPipe
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

            ParseFragPipeConsoleOutputFile(Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mFragPipeVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("FragPipe");
        }
    }
}
