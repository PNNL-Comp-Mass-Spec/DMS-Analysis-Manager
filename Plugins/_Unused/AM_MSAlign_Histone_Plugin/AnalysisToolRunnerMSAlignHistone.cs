//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSAlignHistonePlugIn
{
    /// <summary>
    /// Class for running MSAlign Histone
    /// </summary>
    public class AnalysisToolRunnerMSAlignHistone : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Bruker, Bruker's, classpath, cutofftype, Cystein, filteration, Frag, Histone, html, parm, Prsm, ptm, Xmx, xsl

        // ReSharper restore CommentTypo

        private const string MSAlign_CONSOLE_OUTPUT = "MSAlign_ConsoleOutput.txt";
        private const string MSAlign_Report_CONSOLE_OUTPUT = "MSAlign_Report_ConsoleOutput.txt";
        private const string MSAlign_JAR_NAME = "MsAlignPipeline.jar";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        /// <summary>
        /// XML file created by MsAlignPipeline.jar; detailed results
        /// </summary>
        private const string OUTPUT_FILE_EXTENSION_PTM_SEARCH = "PTM_SEARCH_RESULT";

        /// <summary>
        /// XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with the top hit for each spectrum
        /// </summary>
        private const string OUTPUT_FILE_EXTENSION_TOP_RESULT = "TOP_RESULT";

        /// <summary>
        /// XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with E-Values assigned
        /// </summary>
        private const string OUTPUT_FILE_EXTENSION_E_VALUE_RESULT = "E_VALUE_RESULT";

        /// <summary>
        /// XML file created by MsAlignPipeline.jar; new version of the E_VALUE_RESULT file with Species_ID assigned
        /// </summary>
        private const string OUTPUT_FILE_EXTENSION_OUTPUT_RESULT = "OUTPUT_RESULT";

        /// <summary>
        /// Tab-delimited text file created by MsAlignPipeline.jar; same content as the OUTPUT_RESULT file
        /// </summary>
        private const string RESULT_TABLE_FILE_EXTENSION = "OUTPUT_TABLE";

        /// <summary>
        /// This DMS plugin will rename the DatasetName.OUTPUT_TABLE file to DatasetName_MSAlign_ResultTable.txt
        /// </summary>
        private const string RESULT_TABLE_NAME_SUFFIX = "_MSAlign_ResultTable.txt";

        // XML file created by MsAlignPipeline.jar; we do not keep this file
        // private string OUTPUT_FILE_EXTENSION_FAST_FILTER_COMBINED = "FAST_FILTER_COMBINED";

        /// <summary>
        /// Note that newer versions are assumed to have higher enum values
        /// </summary>
        private enum MSAlignVersionType
        {
            v0pt9 = 0
        }

        private struct InputPropertyValues
        {
            public string FastaFileName;
            public string SpectrumFileName;

            public void Clear()
            {
                FastaFileName = string.Empty;
                SpectrumFileName = string.Empty;
            }
        }

        private bool mToolVersionWritten;
        private string mMSAlignVersion;

        private string mMSAlignProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mMSAlignWorkFolderPath;
        private InputPropertyValues mInputPropertyValues;

        /// <summary>
        /// Runs MSAlign tool
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
                    LogDebug("AnalysisToolRunnerMSAlignHistone.RunTool(): Enter");
                }

                // Verify that program files exist

                // javaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
                // Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MSAlign_Histone program
                // Note that
                mMSAlignProgLoc = DetermineProgramLocation("MSAlignHistoneProgLoc", Path.Combine("jar", MSAlign_JAR_NAME));

                if (string.IsNullOrWhiteSpace(mMSAlignProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Assume v0.9
                // MSAlignVersionType msAlignVersion = MSAlignVersionType.v0pt9;

                // We will store the specific MSAlign version info in the database after the first line is written to file MSAlign_ConsoleOutput.txt

                mToolVersionWritten = false;
                mMSAlignVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Clear InputProperties parameters
                mInputPropertyValues.Clear();
                mMSAlignWorkFolderPath = string.Empty;

                // Copy the MS Align program files and associated files to the work directory
                // Note that this method will update mMSAlignWorkFolderPath
                if (!CopyMSAlignProgramFiles(mMSAlignProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize the files in the input folder
                if (!InitializeInputFolder(mMSAlignWorkFolderPath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Read the MSAlign Parameter File
                var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParamFileName"));

                var cmdLineGenerated = CreateMSAlignCommandLine(paramFilePath, out var msalignCmdLineOptions);

                if (!cmdLineGenerated)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error parsing the MSAlign parameter file";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrEmpty(msalignCmdLineOptions))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Problem parsing MSAlign parameter file: command line switches are not present";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MSAlign_Histone");

                // Lookup the amount of memory to reserve for Java; default to 2 GB
                var javaMemorySize = mJobParams.GetJobParameter("MSAlignJavaMemorySize", 2000);

                if (javaMemorySize < 512)
                    javaMemorySize = 512;

                // Set up and execute a program runner to run MSAlign_Histone
                var arguments = " -Xmx" + javaMemorySize + "M" +
                                @" -classpath jar\*; edu.iupui.msalign.align.histone.pipeline.MsAlignHistonePipelineConsole " +
                                msalignCmdLineOptions;

                LogDebug(javaProgLoc + " " + arguments);

                var cmdRunner = new RunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT);

                mProgress = PROGRESS_PCT_STARTING;

                var processingSuccess = cmdRunner.RunProgram(javaProgLoc, arguments, "MSAlign_Histone", true);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mMSAlignVersion))
                    {
                        ParseConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!processingSuccess && string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    // Parse the console output file one more time to see if an exception was logged
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                CloseOutType eResult;

                if (!processingSuccess)
                {
                    LogError("Error running MSAlign_Histone");

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSAlign_Histone returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSAlign_Histone failed (but exit code is 0)");
                    }

                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Make sure the output files were created
                    if (!ValidateResultFiles())
                    {
                        processingSuccess = false;
                        eResult = CloseOutType.CLOSEOUT_FAILED;
                    }
                    else
                    {
                        // Create the HTML and XML files
                        // Need to call MsAlignPipeline.jar again, but this time with a different classpath

                        var reportGenerated = MakeReportFiles(javaProgLoc, msalignCmdLineOptions, javaMemorySize);

                        if (!reportGenerated)
                            processingSuccess = false;

                        // Move the result files
                        var filesMoved = MoveMSAlignResultFiles();

                        if (!filesMoved)
                        {
                            processingSuccess = false;
                        }

                        var resultTableSourcePath = Path.Combine(mWorkDir, mDatasetName + "_" + RESULT_TABLE_FILE_EXTENSION);

                        if (processingSuccess && File.Exists(resultTableSourcePath))
                        {
                            // Make sure the _OUTPUT_TABLE.txt file is not empty
                            // Make a copy of the OUTPUT_TABLE.txt file so that we can fix the header row (creating the RESULT_TABLE_NAME_SUFFIX file)

                            if (ValidateResultTableFile(resultTableSourcePath))
                            {
                                eResult = CloseOutType.CLOSEOUT_SUCCESS;
                            }
                            else
                            {
                                eResult = CloseOutType.CLOSEOUT_NO_DATA;
                            }
                        }
                        else
                        {
                            eResult = CloseOutType.CLOSEOUT_FAILED;
                        }

                        mStatusTools.UpdateAndWrite(mProgress);

                        if (mDebugLevel >= 3)
                        {
                            LogDebug("MSAlign Search Complete");
                        }
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? eResult : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MSAlignHistone->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool CopyFastaCheckResidues(string sourceFilePath, string targetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            var warningCount = 0;

            try
            {
                var reInvalidResidues = new Regex("[BJOUXZ]", RegexOptions.Compiled);

                var reader = new ProteinFileReader.FastaFileReader();

                if (!reader.OpenFile(sourceFilePath))
                {
                    mMessage = "Error opening FASTA file in CopyFastaCheckResidues";
                    return false;
                }

                using (var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (reader.ReadNextProteinEntry())
                    {
                        writer.WriteLine(reader.ProteinLineStartChar + reader.HeaderLine);
                        var proteinResidues = reInvalidResidues.Replace(reader.ProteinSequence, "-");

                        if (warningCount < 5 && proteinResidues.GetHashCode() != reader.ProteinSequence.GetHashCode())
                        {
                            LogWarning("Changed invalid residues to '-' in protein " + reader.ProteinName);
                            warningCount++;
                        }

                        var index = 0;
                        var residueCount = proteinResidues.Length;

                        while (index < proteinResidues.Length)
                        {
                            var length = Math.Min(RESIDUES_PER_LINE, residueCount - index);
                            writer.WriteLine(proteinResidues.Substring(index, length));
                            index += RESIDUES_PER_LINE;
                        }
                    }
                }

                reader.CloseFile();
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CopyFastaCheckResidues";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            try
            {
                mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);

                // Copy any search result files that are not empty from the MSAlign folder to the work directory
                var resultFiles = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in resultFiles)
                {
                    var searchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (searchResultFile.Exists && searchResultFile.Length > 0)
                    {
                        searchResultFile.CopyTo(Path.Combine(mWorkDir, Path.GetFileName(searchResultFile.Name)));
                    }
                }

                File.Delete(Path.Combine(mWorkDir, mDatasetName + ".mzXML"));
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool CopyMSAlignProgramFiles(string msalignJarFilePath)
        {
            try
            {
                var msalignJarFile = new FileInfo(msalignJarFilePath);

                if (!msalignJarFile.Exists)
                {
                    LogError("MSAlign .Jar file not found: " + msalignJarFile.FullName);
                    return false;
                }

                if (msalignJarFile.Directory == null)
                {
                    LogError("Unable to determine the parent directory of " + msalignJarFile.FullName);
                    return false;
                }

                if (msalignJarFile.Directory.Parent == null)
                {
                    LogError("Unable to determine the parent directory of " + msalignJarFile.Directory.FullName);
                    return false;
                }

                // The source folder is one level up from the .Jar file
                var msAlignSourceDirectory = new DirectoryInfo(msalignJarFile.Directory.Parent.FullName);
                var msAlignWorkingDirectory = new DirectoryInfo(Path.Combine(mWorkDir, "MSAlign"));

                LogMessage("Copying MSAlign program file to the Work Directory");

                // Make sure the directory doesn't already exist
                if (msAlignWorkingDirectory.Exists)
                {
                    msAlignWorkingDirectory.Delete(true);
                }

                // Create the directory
                msAlignWorkingDirectory.Create();
                mMSAlignWorkFolderPath = msAlignWorkingDirectory.FullName;

                // Create the subdirectories
                msAlignWorkingDirectory.CreateSubdirectory("html");
                msAlignWorkingDirectory.CreateSubdirectory("jar");
                msAlignWorkingDirectory.CreateSubdirectory("xml");
                msAlignWorkingDirectory.CreateSubdirectory("xsl");
                msAlignWorkingDirectory.CreateSubdirectory("etc");

                // Copy all files in the jar and xsl folders to the target
                var subdirectoryNames = new List<string> {"jar", "xsl", "etc"};

                foreach (var subdirectoryName in subdirectoryNames)
                {
                    var targetSubdirectoryPath = Path.Combine(msAlignWorkingDirectory.FullName, subdirectoryName);

                    var sourceSubdirectory = msAlignSourceDirectory.GetDirectories(subdirectoryName);

                    if (sourceSubdirectory.Length == 0)
                    {
                        LogError("Source MSAlign subdirectory not found: " + targetSubdirectoryPath);
                        return false;
                    }

                    foreach (var sourceFile in sourceSubdirectory[0].GetFiles())
                    {
                        sourceFile.CopyTo(Path.Combine(targetSubdirectoryPath, sourceFile.Name));
                    }
                }

                // Copy the histone ptm XML files
                var sourcePtmFiles = msAlignSourceDirectory.GetFiles("histone*_ptm.xml").ToList();

                foreach (var sourceFile in sourcePtmFiles)
                {
                    sourceFile.CopyTo(Path.Combine(msAlignWorkingDirectory.FullName, sourceFile.Name));
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CopyMSAlignProgramFiles", ex);
                return false;
            }

            return true;
        }

        private bool CreateMSAlignCommandLine(string paramFilePath, out string commandLine)
        {
            // MSAlign_Histone syntax

            // -a, --activation <CID|HCD|ETD|FILE>
            //        MS/MS activation type: use FILE for data set with several activation types.
            // -c  --cutoff <float>
            //        Cutoff value. (Use this option with -t).
            //        Default value is 0.01.
            // -e  --error <integer>
            //        Error tolerance in ppm.
            //        Default value 15.
            // -m, --modification <0|1|2>
            //        Number of modifications.
            //        Default value: 2.
            // -p, --protection <C0|C57|C58>
            //        Cystein protection.
            //        Default value: C0.
            // -r, --report <integer>
            //        Number of reported Protein-Spectrum-Matches for each spectrum.
            //        Default value 1.
            // -s, --search <TARGET|TARGET+DECOY>
            //        Searching against target or target+decoy (scrambled) protein database.
            // -t, --cutofftype <EVALUE|FDR>
            //        Use either EVALUE or FDR to filter out identified Protein-Spectrum-Matches.
            //        Default value EVALUE.

            // These key names must be lowercase
            const string INSTRUMENT_ACTIVATION_TYPE_KEY = "activation";
            const string SEARCH_TYPE_KEY = "search";

            commandLine = string.Empty;

            try
            {
                // Initialize the dictionary that maps parameter names in the parameter file to command line switches
                var parameterMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"activation", "a"},
                    {"search", "s"},
                    {"protection", "p"},
                    {"modification", "m"},
                    {"error", "e"},
                    {"cutoffType", "t"},
                    {"cutoff", "c"},
                    {"report", "r"}
                };

                // Open the parameter file
                using var reader = new StreamReader(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // The first two parameters on the command line are FASTA File name and input file name
                commandLine += mInputPropertyValues.FastaFileName + " " + mInputPropertyValues.SpectrumFileName;

                // Now append the parameters defined in the parameter file
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine) || dataLine.TrimStart().StartsWith("#"))
                    {
                        // Comment line or blank line; skip it
                        continue;
                    }

                    // Look for an equals sign
                    var equalsIndex = dataLine.IndexOf('=');

                    if (equalsIndex <= 0)
                    {
                        // Unknown line format; skip it
                        continue;
                    }

                    // Split the line on the equals sign
                    var keyName = dataLine.Substring(0, equalsIndex).TrimEnd();
                    string value;

                    if (equalsIndex < dataLine.Length - 1)
                    {
                        value = dataLine.Substring(equalsIndex + 1).Trim();
                    }
                    else
                    {
                        value = string.Empty;
                    }

                    if (keyName.ToLower() == INSTRUMENT_ACTIVATION_TYPE_KEY)
                    {
                        // If this is a Bruker dataset, we need to make sure that the value for this entry is not FILE
                        // The reason is that the mzXML file created by Bruker's compass program does not include the ScanType information (CID, ETD, etc.)

                        // The ToolName job parameter holds the name of the job script we are executing
                        var scriptName = mJobParams.GetParam("ToolName");

                        if (scriptName.StartsWith("MSAlign_Bruker", StringComparison.OrdinalIgnoreCase) ||
                            scriptName.StartsWith("MSAlign_Histone", StringComparison.OrdinalIgnoreCase))
                        {
                            if (string.Equals(value, "FILE", StringComparison.OrdinalIgnoreCase))
                            {
                                mMessage = "Must specify an explicit scan type for " + keyName +
                                           " in the MSAlign parameter file (CID, HCD, or ETD)";

                                LogError(mMessage + "; this is required because Bruker-created mzXML files " +
                                         "do not include activationMethod information in the precursorMz tag");

                                return false;
                            }
                        }
                    }

                    if (keyName.ToLower() == SEARCH_TYPE_KEY)
                    {
                        if (string.Equals(value, "TARGET+DECOY", StringComparison.OrdinalIgnoreCase))
                        {
                            // Make sure the protein collection is not a Decoy protein collection
                            var proteinOptions = mJobParams.GetParam("ProteinOptions");

                            if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                mMessage =
                                    "MSAlign parameter file contains searchType=TARGET+DECOY; " +
                                    "protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy";

                                LogError(mMessage);

                                return false;
                            }
                        }
                    }

                    if (parameterMap.TryGetValue(keyName, out var switchName))
                    {
                        commandLine += " -" + switchName + " " + value;
                    }
                    else
                    {
                        LogWarning("Ignoring unrecognized MSAlign_Histone parameter: " + keyName);
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateMSAlignCommandLine";
                LogError("Exception in CreateMSAlignCommandLine", ex);
                return false;
            }

            return true;
        }

        private Dictionary<string, string> GetExpectedMSAlignResultFiles(string datasetName)
        {
            // Keys in this dictionary are the expected file name
            // Values are the new name to rename the file to
            var resultFiles = new Dictionary<string, string>();
            var baseName = Path.GetFileNameWithoutExtension(mInputPropertyValues.SpectrumFileName);

            resultFiles.Add(baseName + "." + OUTPUT_FILE_EXTENSION_PTM_SEARCH, datasetName + "_PTM_Search_Result.xml");
            resultFiles.Add(baseName + "." + OUTPUT_FILE_EXTENSION_TOP_RESULT, string.Empty);
            // Don't keep this file since it's virtually identical to the E_VALUE_RESULT file
            resultFiles.Add(baseName + "." + OUTPUT_FILE_EXTENSION_E_VALUE_RESULT, datasetName + "_PTM_Search_Result_EValue.xml");
            resultFiles.Add(baseName + "." + OUTPUT_FILE_EXTENSION_OUTPUT_RESULT, datasetName + "_PTM_Search_Result_Final.xml");

            resultFiles.Add(baseName + "." + RESULT_TABLE_FILE_EXTENSION, datasetName + "_" + RESULT_TABLE_FILE_EXTENSION);

            return resultFiles;
        }

        private bool InitializeInputFolder(string msalignWorkFolderPath)
        {
            try
            {
                var sourceDirectory = new DirectoryInfo(mWorkDir);

                // Copy the FASTA file into the MSInput folder
                // MSAlign will crash if any non-standard residues are present (BJOUXZ)
                // Thus, we will read the source file with a reader and create a new FASTA file

                // Define the path to the FASTA file
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");
                var fastaFilePath = Path.Combine(orgDbDir, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

                var fastaFile = new FileInfo(fastaFilePath);

                if (!fastaFile.Exists)
                {
                    // FASTA file not found
                    LogError("FASTA file not found: " + fastaFile.FullName);
                    return false;
                }

                mInputPropertyValues.FastaFileName = fastaFile.Name;

                if (!CopyFastaCheckResidues(fastaFile.FullName, Path.Combine(msalignWorkFolderPath, mInputPropertyValues.FastaFileName)))
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "CopyFastaCheckResidues returned false";
                    LogError(mMessage);
                    return false;
                }

                // Move the _msdeconv.msalign file to the MSAlign work folder
                var matchingFiles = sourceDirectory.GetFiles("*" + AnalysisResourcesMSAlignHistone.MSDECONV_MSALIGN_FILE_SUFFIX);

                if (matchingFiles.Length == 0)
                {
                    LogError("MSAlign file not found in work directory");
                    return false;
                }

                mInputPropertyValues.SpectrumFileName = matchingFiles[0].Name;
                matchingFiles[0].MoveTo(Path.Combine(msalignWorkFolderPath, mInputPropertyValues.SpectrumFileName));
            }
            catch (Exception ex)
            {
                LogError("Exception in InitializeMSInputFolder", ex);
                return false;
            }

            return true;
        }

        private bool MakeReportFiles(string javaProgLoc, string msalignCmdLineOptions, int javaMemorySize)
        {
            bool success;

            try
            {
                LogMessage("Creating MSAlign_Histone Report Files");

                // Set up and execute a program runner to run MSAlign_Histone
                var arguments = " -Xmx" + javaMemorySize + "M" +
                                @" -classpath jar\*; edu.iupui.msalign.align.histone.view.HistoneHtmlConsole " +
                                msalignCmdLineOptions;

                LogDebug(javaProgLoc + " " + arguments);

                var cmdRunner = new RunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSAlign_Report_CONSOLE_OUTPUT);

                success = cmdRunner.RunProgram(javaProgLoc, arguments, "MSAlign_Histone", true);

                if (!success)
                {
                    LogError("Error running MSAlign_Histone to create HTML and XML files");

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSAlign_Histone returned a non-zero exit code during report creation: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSAlign_Histone failed during report creation (but exit code is 0)");
                    }
                }
                else
                {
                    mJobParams.AddResultFileToSkip(MSAlign_Report_CONSOLE_OUTPUT);
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception creating MSAlign_Histone HTML and XML files";
                LogError("Exception in MakeReportFiles", ex);
                return false;
            }

            return success;
        }

        // Example Console output

        // Start at Thu Apr 04 15:10:48 PDT 2013
        // MS-Align+ 0.9.0.16 2013-02-02
        // Fast filteration started.
        // Fast filteration finished.
        // Ptm search: Processing spectrum scan 4353...9% finished (0 minutes used).
        // Ptm search: Processing spectrum scan 4354...18% finished (1 minutes used).

        private readonly Regex reExtractPercentFinished = new("(\\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSAlign console output file to determine the MSAlign version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
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

                short actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            if (linesRead <= 4 && string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                // Originally the second line was the MSAlign version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The fourth line is the MSAlign version
                                if (string.IsNullOrEmpty(mMSAlignVersion) && dataLine.IndexOf("ms-align", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    if (mDebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSAlignVersion))
                                    {
                                        LogDebug("MSAlign version: " + dataLine);
                                    }

                                    mMSAlignVersion = dataLine;
                                }
                                else
                                {
                                    if (dataLine.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0 || dataLine.Contains("[ java.lang"))
                                    {
                                        mConsoleOutputErrorMsg = "Error running MSAlign: " + dataLine;
                                    }
                                }
                            }

                            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg += "; " + dataLine;
                            }
                            else
                            {
                                // Update progress if the line starts with Processing spectrum
                                if (dataLine.IndexOf("Processing spectrum", StringComparison.Ordinal) >= 0)
                                {
                                    var match = reExtractPercentFinished.Match(dataLine);

                                    if (match.Success)
                                    {
                                        if (short.TryParse(match.Groups[1].Value, out var progress))
                                        {
                                            actualProgress = progress;
                                        }
                                    }
                                }
                                else if (dataLine.Contains("[ java.lang"))
                                {
                                    // This is likely an exception
                                    mConsoleOutputErrorMsg = "Error running MSAlign: " + dataLine;
                                }
                            }
                        }
                    }
                }

                if (mProgress < actualProgress)
                {
                    mProgress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mMSAlignVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMSAlignProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool MoveMSAlignResultFiles()
        {
            var eValueResultFilePath = string.Empty;
            var finalResultFilePath = string.Empty;

            try
            {
                var resultsFilesToMove = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in resultsFilesToMove)
                {
                    var searchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (!searchResultFile.Exists)
                    {
                        // Note that ValidateResultFiles should have already logged the missing files
                    }
                    else
                    {
                        // Copy the results file to the work directory
                        // Rename the file as we copy it

                        if (string.IsNullOrEmpty(kvItem.Value))
                        {
                            // Skip this file
                        }
                        else
                        {
                            var targetFilePath = Path.Combine(mWorkDir, kvItem.Value);

                            searchResultFile.CopyTo(targetFilePath, true);

                            if (kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_E_VALUE_RESULT))
                            {
                                eValueResultFilePath = targetFilePath;
                            }
                            else if (kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_OUTPUT_RESULT))
                            {
                                finalResultFilePath = targetFilePath;
                            }
                        }
                    }
                }

                // Zip the Html and XML folders
                ZipMSAlignResultFolder("html");
                ZipMSAlignResultFolder("XML");

                // Skip the E_VALUE_RESULT file if it is identical to the OUTPUT_RESULT file

                if (!string.IsNullOrEmpty(eValueResultFilePath) && !string.IsNullOrEmpty(finalResultFilePath))
                {
                    if (Global.FilesMatch(eValueResultFilePath, finalResultFilePath))
                    {
                        mJobParams.AddResultFileToSkip(Path.GetFileName(eValueResultFilePath));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateAndCopyResultFiles", ex);
                return false;
            }

            return true;
        }

        private bool ValidateResultFiles()
        {
            var processingError = false;

            try
            {
                var resultFiles = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in resultFiles)
                {
                    var searchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (!searchResultFile.Exists)
                    {
                        const string msg = "MSAlign results file not found";

                        if (!processingError)
                        {
                            // This is the first missing file; update the base-class comment
                            LogError(msg + ": " + kvItem.Key);
                            processingError = true;
                        }

                        LogErrorNoMessageUpdate(msg + ": " + searchResultFile.FullName);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultFiles", ex);
                return false;
            }

            return !processingError;
        }

        private bool ValidateResultTableFile(string sourceFilePath)
        {
            try
            {
                var validDataFound = false;
                var linesRead = 0;

                var outputFilePath = Path.Combine(mWorkDir, mDatasetName + RESULT_TABLE_NAME_SUFFIX);

                if (!File.Exists(sourceFilePath))
                {
                    if (mDebugLevel >= 2)
                    {
                        LogWarning("MSAlign OUTPUT_TABLE file not found: " + sourceFilePath);
                    }
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "MSAlign OUTPUT_TABLE file not found";
                    }
                    return false;
                }

                if (mDebugLevel >= 2)
                {
                    LogMessage("Validating that the MSAlign OUTPUT_TABLE file is not empty");
                }

                // Open the input file and
                // create the output file
                using (var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            if (linesRead == 1 && dataLine.EndsWith("FDR\t"))
                            {
                                // The header line is missing the final column header; add it
                                dataLine += "FragMethod";
                            }

                            if (!validDataFound)
                            {
                                var splitLine = dataLine.Split('\t');

                                if (splitLine.Length > 1)
                                {
                                    // The first column has the source .msalign file name
                                    // The second column has Prsm_ID

                                    // Look for an integer in the second column
                                    if (int.TryParse(splitLine[1], out _))
                                    {
                                        // Integer found; line is valid
                                        validDataFound = true;
                                    }
                                }
                            }

                            writer.WriteLine(dataLine);
                        }
                    }
                }

                if (!validDataFound)
                {
                    LogError("MSAlign OUTPUT_TABLE file is empty");
                    return false;
                }

                // Don't keep the original output table; only the new file we just created
                mJobParams.AddResultFileToSkip(Path.GetFileName(sourceFilePath));
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultTableFile", ex);
                return false;
            }

            return true;
        }

        private bool ZipMSAlignResultFolder(string folderName)
        {
            try
            {
                var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_MSAlign_Results_" + folderName.ToUpper() + ".zip");
                var sourceFolderPath = Path.Combine(mMSAlignWorkFolderPath, folderName);

                // Confirm that the directory has one or more files or subdirectories
                var sourceDirectory = new DirectoryInfo(sourceFolderPath);

                if (sourceDirectory.GetFileSystemInfos().Length == 0)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogWarning("MSAlign results directory is empty; nothing to zip: " + sourceFolderPath);
                    }
                    return false;
                }

                if (mDebugLevel >= 1)
                {
                    var logMessage = "Zipping " + folderName.ToUpper() + " folder at " + sourceFolderPath;

                    if (mDebugLevel >= 2)
                    {
                        logMessage += ": " + targetFilePath;
                    }
                    LogMessage(logMessage);
                }

                var zipper = new Ionic.Zip.ZipFile(targetFilePath);
                zipper.AddDirectory(sourceFolderPath);
                zipper.Save();
            }
            catch (Exception ex)
            {
                LogError("Exception in ZipMSAlignResultFolder", ex);
                return false;
            }

            return true;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSAlignVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                LogProgress("MSAlign Histone");
            }
        }
    }
}
