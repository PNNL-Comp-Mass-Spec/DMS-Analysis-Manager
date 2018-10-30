//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerMSAlignHistonePlugIn
{
    /// <summary>
    /// Class for running MSAlign Histone
    /// </summary>
    public class clsAnalysisToolRunnerMSAlignHistone : clsAnalysisToolRunnerBase
    {
        //*********************************************************************************************************
        // Class for running MSAlign Histone analysis
        //*********************************************************************************************************

        #region "Constants and Enums"

        protected const string MSAlign_CONSOLE_OUTPUT = "MSAlign_ConsoleOutput.txt";
        protected const string MSAlign_Report_CONSOLE_OUTPUT = "MSAlign_Report_ConsoleOutput.txt";
        protected const string MSAlign_JAR_NAME = "MsAlignPipeline.jar";

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        // XML file created by MsAlignPipeline.jar; detailed results
        protected const string OUTPUT_FILE_EXTENSION_PTM_SEARCH = "PTM_SEARCH_RESULT";
        // XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with the top hit for each spectrum
        protected const string OUTPUT_FILE_EXTENSION_TOP_RESULT = "TOP_RESULT";
        // XML file created by MsAlignPipeline.jar; filtered version of the PTM_SEARCH_RESULT file with E-Values assigned
        protected const string OUTPUT_FILE_EXTENSION_E_VALUE_RESULT = "E_VALUE_RESULT";
        // XML file created by MsAlignPipeline.jar; new version of the E_VALUE_RESULT file with Species_ID assigned
        protected const string OUTPUT_FILE_EXTENSION_OUTPUT_RESULT = "OUTPUT_RESULT";

        // Tab-delimited text file created by MsAlignPipeline.jar; same content as the OUTPUT_RESULT file
        protected const string RESULT_TABLE_FILE_EXTENSION = "OUTPUT_TABLE";
        // This DMS plugin will rename the DatasetName.OUTPUT_TABLE file to DatasetName_MSAlign_ResultTable.txt
        protected const string RESULT_TABLE_NAME_SUFFIX = "_MSAlign_ResultTable.txt";

        // XML file created by MsAlignPipeline.jar; we do not keep this file
        protected const string OUTPUT_FILE_EXTENSION_FAST_FILTER_COMBINED = "FAST_FILTER_COMBINED";

        // Note that newer versions are assumed to have higher enum values
        protected enum eMSAlignVersionType
        {
            v0pt9 = 0
        }

        #endregion

        #region "Structures"

        protected struct udtInputPropertyValuesType
        {
            public string FastaFileName;
            public string SpectrumFileName;

            public void Clear()
            {
                FastaFileName = string.Empty;
                SpectrumFileName = string.Empty;
            }
        }

        #endregion

        #region "Module Variables"

        protected bool mToolVersionWritten;
        protected string mMSAlignVersion;

        protected string mMSAlignProgLoc;
        protected string mConsoleOutputErrorMsg;

        protected string mMSAlignWorkFolderPath;
        protected udtInputPropertyValuesType mInputPropertyValues;

        #endregion

        #region "Methods"

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
                    LogDebug("clsAnalysisToolRunnerMSAlignHistone.RunTool(): Enter");
                }

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
                // Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
                var JavaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(JavaProgLoc))
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
                var eMSalignVersion = eMSAlignVersionType.v0pt9;

                // We will store the specific MSAlign version info in the database after the first line is written to file MSAlign_ConsoleOutput.txt

                mToolVersionWritten = false;
                mMSAlignVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Clear InputProperties parameters
                mInputPropertyValues.Clear();
                mMSAlignWorkFolderPath = string.Empty;

                // Copy the MS Align program files and associated files to the work directory
                // Note that this function will update mMSAlignWorkFolderPath
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
                var strParamFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));

                var cmdLineGenerated = CreateMSAlignCommandLine(strParamFilePath, out var strMSAlignCmdLineOptions);
                if (!cmdLineGenerated)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error parsing the MSAlign parameter file";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrEmpty(strMSAlignCmdLineOptions))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Problem parsing MSAlign parameter file: command line switches are not present";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MSAlign_Histone");

                // Lookup the amount of memory to reserve for Java; default to 2 GB
                var intJavaMemorySize = mJobParams.GetJobParameter("MSAlignJavaMemorySize", 2000);
                if (intJavaMemorySize < 512)
                    intJavaMemorySize = 512;

                // Set up and execute a program runner to run MSAlign_Histone
                var cmdStr = " -Xmx" + intJavaMemorySize +
                         "M -classpath jar\\*; edu.iupui.msalign.align.histone.pipeline.MsAlignHistonePipelineConsole " + strMSAlignCmdLineOptions;

                LogDebug(JavaProgLoc + " " + cmdStr);

                var cmdRunner = new clsRunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT);

                mProgress = PROGRESS_PCT_STARTING;

                var processingSuccess = cmdRunner.RunProgram(JavaProgLoc, cmdStr, "MSAlign_Histone", true);

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

                        var reportGenerated = MakeReportFiles(JavaProgLoc, strMSAlignCmdLineOptions, intJavaMemorySize);
                        if (!reportGenerated)
                            processingSuccess = false;

                        // Move the result files
                        var filesMoved = MoveMSAlignResultFiles();
                        if (!filesMoved)
                        {
                            processingSuccess = false;
                        }

                        var strResultTableSourcePath = Path.Combine(mWorkDir, mDatasetName + "_" + RESULT_TABLE_FILE_EXTENSION);

                        if (processingSuccess && File.Exists(strResultTableSourcePath))
                        {
                            // Make sure the _OUTPUT_TABLE.txt file is not empty
                            // Make a copy of the OUTPUT_TABLE.txt file so that we can fix the header row (creating the RESULT_TABLE_NAME_SUFFIX file)

                            if (ValidateResultTableFile(eMSalignVersion, strResultTableSourcePath))
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
                PRISM.ProgRunner.GarbageCollectNow();

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

        protected bool CopyFastaCheckResidues(string strSourceFilePath, string strTargetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            var intWarningCount = 0;

            try
            {
                var reInvalidResidues = new Regex("[BJOUXZ]", RegexOptions.Compiled);

                var oReader = new ProteinFileReader.FastaFileReader();
                if (!oReader.OpenFile(strSourceFilePath))
                {
                    mMessage = "Error opening fasta file in CopyFastaCheckResidues";
                    return false;
                }

                using (var writer = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (oReader.ReadNextProteinEntry())
                    {
                        writer.WriteLine(oReader.ProteinLineStartChar + oReader.HeaderLine);
                        var strProteinResidues = reInvalidResidues.Replace(oReader.ProteinSequence, "-");

                        if (intWarningCount < 5 && strProteinResidues.GetHashCode() != oReader.ProteinSequence.GetHashCode())
                        {
                            LogWarning("Changed invalid residues to '-' in protein " + oReader.ProteinName);
                            intWarningCount += 1;
                        }

                        var intIndex = 0;
                        var intResidueCount = strProteinResidues.Length;
                        while (intIndex < strProteinResidues.Length)
                        {
                            var intLength = Math.Min(RESIDUES_PER_LINE, intResidueCount - intIndex);
                            writer.WriteLine(strProteinResidues.Substring(intIndex, intLength));
                            intIndex += RESIDUES_PER_LINE;
                        }
                    }
                }

                oReader.CloseFile();
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
                mJobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);

                // Copy any search result files that are not empty from the MSAlign folder to the work directory
                var dctResultFiles = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in dctResultFiles)
                {
                    var fiSearchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (fiSearchResultFile.Exists && fiSearchResultFile.Length > 0)
                    {
                        fiSearchResultFile.CopyTo(Path.Combine(mWorkDir, Path.GetFileName(fiSearchResultFile.Name)));
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

        private bool CopyMSAlignProgramFiles(string strMSAlignJarFilePath)
        {
            try
            {
                var fiMSAlignJarFile = new FileInfo(strMSAlignJarFilePath);

                if (!fiMSAlignJarFile.Exists)
                {
                    LogError("MSAlign .Jar file not found: " + fiMSAlignJarFile.FullName);
                    return false;
                }

                if (fiMSAlignJarFile.Directory == null)
                {
                    LogError("Unable to determine the parent directory of " + fiMSAlignJarFile.FullName);
                    return false;
                }

                if (fiMSAlignJarFile.Directory.Parent == null)
                {
                    LogError("Unable to determine the parent directory of " + fiMSAlignJarFile.Directory.FullName);
                    return false;
                }

                // The source folder is one level up from the .Jar file
                var diMSAlignSrc = new DirectoryInfo(fiMSAlignJarFile.Directory.Parent.FullName);
                var diMSAlignWork = new DirectoryInfo(Path.Combine(mWorkDir, "MSAlign"));

                LogMessage("Copying MSAlign program file to the Work Directory");

                // Make sure the directory doesn't already exist
                if (diMSAlignWork.Exists)
                {
                    diMSAlignWork.Delete(true);
                }

                // Create the directory
                diMSAlignWork.Create();
                mMSAlignWorkFolderPath = diMSAlignWork.FullName;

                // Create the subdirectories
                diMSAlignWork.CreateSubdirectory("html");
                diMSAlignWork.CreateSubdirectory("jar");
                diMSAlignWork.CreateSubdirectory("xml");
                diMSAlignWork.CreateSubdirectory("xsl");
                diMSAlignWork.CreateSubdirectory("etc");

                // Copy all files in the jar and xsl folders to the target
                var lstSubfolderNames = new List<string> {"jar", "xsl", "etc"};

                foreach (var strSubFolder in lstSubfolderNames)
                {
                    var strTargetSubfolder = Path.Combine(diMSAlignWork.FullName, strSubFolder);

                    var diSubfolder = diMSAlignSrc.GetDirectories(strSubFolder);

                    if (diSubfolder.Length == 0)
                    {
                        LogError("Source MSAlign subfolder not found: " + strTargetSubfolder);
                        return false;
                    }

                    foreach (var fiFile in diSubfolder[0].GetFiles())
                    {
                        fiFile.CopyTo(Path.Combine(strTargetSubfolder, fiFile.Name));
                    }
                }

                // Copy the histone ptm XML files
                var fiSourceFiles = diMSAlignSrc.GetFiles("histone*_ptm.xml").ToList();

                foreach (var fiFile in fiSourceFiles)
                {
                    fiFile.CopyTo(Path.Combine(diMSAlignWork.FullName, fiFile.Name));
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CopyMSAlignProgramFiles", ex);
                return false;
            }

            return true;
        }

        protected bool CreateMSAlignCommandLine(string paramFilePath, out string commandLine)
        {
            // MSAlign_Histone syntax
            //
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
                var dctParameterMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
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
                using (var reader = new StreamReader(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // The first two parameters on the command line are Fasta File name and input file name
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
                            // If this is a bruker dataset, we need to make sure that the value for this entry is not FILE
                            // The reason is that the mzXML file created by Bruker's compass program does not include the scantype information (CID, ETD, etc.)
                            var toolName = mJobParams.GetParam("ToolName");

                            if (toolName == "MSAlign_Bruker" || toolName == "MSAlign_Histone_Bruker")
                            {
                                if (value.ToUpper() == "FILE")
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
                            if (value.ToUpper() == "TARGET+DECOY")
                            {
                                // Make sure the protein collection is not a Decoy protein collection
                                var proteinOptions = mJobParams.GetParam("ProteinOptions");

                                if (proteinOptions.ToLower().Contains("seq_direction=decoy"))
                                {
                                    mMessage =
                                        "MSAlign parameter file contains searchType=TARGET+DECOY; " +
                                        "protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy";

                                    LogError(mMessage);

                                    return false;
                                }
                            }
                        }

                        if (dctParameterMap.TryGetValue(keyName, out var switchName))
                        {
                            commandLine += " -" + switchName + " " + value;
                        }
                        else
                        {
                            LogWarning("Ignoring unrecognized MSAlign_Histone parameter: " + keyName);
                        }

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

        protected Dictionary<string, string> GetExpectedMSAlignResultFiles(string strDatasetName)
        {
            // Keys in this dictionary are the expected file name
            // Values are the new name to rename the file to
            var dctResultFiles = new Dictionary<string, string>();
            var strBaseName = Path.GetFileNameWithoutExtension(mInputPropertyValues.SpectrumFileName);

            dctResultFiles.Add(strBaseName + "." + OUTPUT_FILE_EXTENSION_PTM_SEARCH, strDatasetName + "_PTM_Search_Result.xml");
            dctResultFiles.Add(strBaseName + "." + OUTPUT_FILE_EXTENSION_TOP_RESULT, string.Empty);
            // Don't keep this file since it's virtually identical to the E_VALUE_RESULT file
            dctResultFiles.Add(strBaseName + "." + OUTPUT_FILE_EXTENSION_E_VALUE_RESULT, strDatasetName + "_PTM_Search_Result_EValue.xml");
            dctResultFiles.Add(strBaseName + "." + OUTPUT_FILE_EXTENSION_OUTPUT_RESULT, strDatasetName + "_PTM_Search_Result_Final.xml");

            dctResultFiles.Add(strBaseName + "." + RESULT_TABLE_FILE_EXTENSION, strDatasetName + "_" + RESULT_TABLE_FILE_EXTENSION);

            return dctResultFiles;
        }

        protected bool InitializeInputFolder(string strMSAlignWorkFolderPath)
        {
            try
            {
                var fiSourceFolder = new DirectoryInfo(mWorkDir);

                // Copy the .Fasta file into the MSInput folder
                // MSAlign will crash if any non-standard residues are present (BJOUXZ)
                // Thus, we will read the source file with a reader and create a new fasta file

                // Define the path to the fasta file
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");
                var strFASTAFilePath = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

                var fiFastaFile = new FileInfo(strFASTAFilePath);

                if (!fiFastaFile.Exists)
                {
                    // Fasta file not found
                    LogError("Fasta file not found: " + fiFastaFile.FullName);
                    return false;
                }

                mInputPropertyValues.FastaFileName = string.Copy(fiFastaFile.Name);

                if (!CopyFastaCheckResidues(fiFastaFile.FullName, Path.Combine(strMSAlignWorkFolderPath, mInputPropertyValues.FastaFileName)))
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "CopyFastaCheckResidues returned false";
                    LogError(mMessage);
                    return false;
                }

                // Move the _msdeconv.msalign file to the MSAlign work folder
                var fiFiles = fiSourceFolder.GetFiles("*" + clsAnalysisResourcesMSAlignHistone.MSDECONV_MSALIGN_FILE_SUFFIX);
                if (fiFiles.Length == 0)
                {
                    LogError("MSAlign file not found in work directory");
                    return false;
                }

                mInputPropertyValues.SpectrumFileName = string.Copy(fiFiles[0].Name);
                fiFiles[0].MoveTo(Path.Combine(strMSAlignWorkFolderPath, mInputPropertyValues.SpectrumFileName));
            }
            catch (Exception ex)
            {
                LogError("Exception in InitializeMSInputFolder", ex);
                return false;
            }

            return true;
        }

        protected bool MakeReportFiles(string JavaProgLoc, string strMSAlignCmdLineOptions, int intJavaMemorySize)
        {
            bool blnSuccess;

            try
            {
                LogMessage("Creating MSAlign_Histone Report Files");

                // Set up and execute a program runner to run MSAlign_Histone
                var cmdStr = " -Xmx" + intJavaMemorySize + "M -classpath jar\\*; edu.iupui.msalign.align.histone.view.HistoneHtmlConsole " +
                                strMSAlignCmdLineOptions;

                LogDebug(JavaProgLoc + " " + cmdStr);

                var cmdRunner = new clsRunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                cmdRunner.CreateNoWindow = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.EchoOutputToConsole = true;

                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSAlign_Report_CONSOLE_OUTPUT);

                blnSuccess = cmdRunner.RunProgram(JavaProgLoc, cmdStr, "MSAlign_Histone", true);

                if (!blnSuccess)
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

            return blnSuccess;
        }

        // Example Console output
        //
        // Start at Thu Apr 04 15:10:48 PDT 2013
        // MS-Align+ 0.9.0.16 2013-02-02
        // Fast filteration started.
        // Fast filteration finished.
        // Ptm search: Processing spectrum scan 4353...9% finished (0 minutes used).
        // Ptm search: Processing spectrum scan 4354...18% finished (1 minutes used).
        private readonly Regex reExtractPercentFinished = new Regex("(\\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSAlign console output file to determine the MSAlign version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
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
                        linesRead += 1;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            if (linesRead <= 4 && string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                // Originally the second line was the MSAlign version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The fourth line is the MSAlign version
                                if (string.IsNullOrEmpty(mMSAlignVersion) && dataLine.ToLower().Contains("ms-align"))
                                {
                                    if (mDebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSAlignVersion))
                                    {
                                        LogDebug("MSAlign version: " + dataLine);
                                    }

                                    mMSAlignVersion = string.Copy(dataLine);
                                }
                                else
                                {
                                    if (dataLine.ToLower().Contains("error") || dataLine.Contains("[ java.lang"))
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
                                    var oMatch = reExtractPercentFinished.Match(dataLine);
                                    if (oMatch.Success)
                                    {
                                        if (short.TryParse(oMatch.Groups[1].Value, out var intProgress))
                                        {
                                            actualProgress = intProgress;
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
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSAlignVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mMSAlignProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        protected bool MoveMSAlignResultFiles()
        {
            var strEValueResultFilePath = string.Empty;
            var strFinalResultFilePath = string.Empty;

            try
            {
                var dctResultsFilesToMove = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in dctResultsFilesToMove)
                {
                    var fiSearchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (!fiSearchResultFile.Exists)
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
                            var strTargetFilePath = Path.Combine(mWorkDir, kvItem.Value);

                            fiSearchResultFile.CopyTo(strTargetFilePath, true);

                            if (kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_E_VALUE_RESULT))
                            {
                                strEValueResultFilePath = strTargetFilePath;
                            }
                            else if (kvItem.Key.EndsWith(OUTPUT_FILE_EXTENSION_OUTPUT_RESULT))
                            {
                                strFinalResultFilePath = strTargetFilePath;
                            }
                        }
                    }
                }

                // Zip the Html and XML folders
                ZipMSAlignResultFolder("html");
                ZipMSAlignResultFolder("XML");

                // Skip the E_VALUE_RESULT file if it is identical to the OUTPUT_RESULT file

                if (!string.IsNullOrEmpty(strEValueResultFilePath) && !string.IsNullOrEmpty(strFinalResultFilePath))
                {
                    if (clsGlobal.FilesMatch(strEValueResultFilePath, strFinalResultFilePath))
                    {
                        mJobParams.AddResultFileToSkip(Path.GetFileName(strEValueResultFilePath));
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

        protected bool ValidateResultFiles()
        {
            var processingError = false;

            try
            {
                var dctResultFiles = GetExpectedMSAlignResultFiles(mDatasetName);

                foreach (var kvItem in dctResultFiles)
                {
                    var fiSearchResultFile = new FileInfo(Path.Combine(mMSAlignWorkFolderPath, kvItem.Key));

                    if (!fiSearchResultFile.Exists)
                    {
                        var msg = "MSAlign results file not found";

                        if (!processingError)
                        {
                            // This is the first missing file; update the base-class comment
                            LogError(msg + ": " + kvItem.Key);
                            processingError = true;
                        }

                        LogErrorNoMessageUpdate(msg + ": " + fiSearchResultFile.FullName);

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

        protected bool ValidateResultTableFile(eMSAlignVersionType eMSalignVersion, string strSourceFilePath)
        {
            try
            {
                var blnValidDataFound = false;
                var linesRead = 0;

                var strOutputFilePath = Path.Combine(mWorkDir, mDatasetName + RESULT_TABLE_NAME_SUFFIX);

                if (!File.Exists(strSourceFilePath))
                {
                    if (mDebugLevel >= 2)
                    {
                        LogWarning("MSAlign OUTPUT_TABLE file not found: " + strSourceFilePath);
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
                using (var reader = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            if (linesRead == 1 && dataLine.EndsWith("FDR\t"))
                            {
                                // The header line is missing the final column header; add it
                                dataLine += "FragMethod";
                            }

                            if (!blnValidDataFound)
                            {
                                var strSplitLine = dataLine.Split('\t');

                                if (strSplitLine.Length > 1)
                                {
                                    // The first column has the source .msalign file name
                                    // The second column has Prsm_ID

                                    // Look for an integer in the second column
                                    if (int.TryParse(strSplitLine[1], out _))
                                    {
                                        // Integer found; line is valid
                                        blnValidDataFound = true;
                                    }
                                }
                            }

                            writer.WriteLine(dataLine);
                        }
                    }
                }

                if (!blnValidDataFound)
                {
                    LogError("MSAlign OUTPUT_TABLE file is empty");
                    return false;
                }

                // Don't keep the original output table; only the new file we just created
                mJobParams.AddResultFileToSkip(Path.GetFileName(strSourceFilePath));
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultTableFile", ex);
                return false;
            }

            return true;
        }

        protected bool ZipMSAlignResultFolder(string strFolderName)
        {
            try
            {
                var strTargetFilePath = Path.Combine(mWorkDir, mDatasetName + "_MSAlign_Results_" + strFolderName.ToUpper() + ".zip");
                var strSourceFolderPath = Path.Combine(mMSAlignWorkFolderPath, strFolderName);

                // Confirm that the directory has one or more files or subfolders
                var diSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (diSourceFolder.GetFileSystemInfos().Length == 0)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogWarning("MSAlign results folder is empty; nothing to zip: " + strSourceFolderPath);
                    }
                    return false;
                }

                if (mDebugLevel >= 1)
                {
                    var strLogMessage = "Zipping " + strFolderName.ToUpper() + " folder at " + strSourceFolderPath;

                    if (mDebugLevel >= 2)
                    {
                        strLogMessage += ": " + strTargetFilePath;
                    }
                    LogMessage(strLogMessage);
                }

                var objZipper = new Ionic.Zip.ZipFile(strTargetFilePath);
                objZipper.AddDirectory(strSourceFolderPath);
                objZipper.Save();
            }
            catch (Exception ex)
            {
                LogError("Exception in ZipMSAlignResultFolder", ex);
                return false;
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
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

        #endregion
    }
}
