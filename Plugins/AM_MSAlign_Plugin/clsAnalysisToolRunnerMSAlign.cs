//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerMSAlignPlugIn
{
    /// <summary>
    /// Class for running MSAlign analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerMSAlign : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const string MSAlign_CONSOLE_OUTPUT = "MSAlign_ConsoleOutput.txt";
        protected const string MSAlign_JAR_NAME = "MSAlign.jar";

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string RESULT_TABLE_NAME_SUFFIX = "_MSAlign_ResultTable.txt";
        protected const string RESULT_TABLE_NAME_LEGACY = "result_table.txt";

        protected const string RESULT_DETAILS_NAME_SUFFIX = "_MSAlign_ResultDetails.txt";
        protected const string RESULT_DETAILS_NAME_LEGACY = "result.txt";
        // Note that newer versions are assumed to have higher enum values
        protected enum eMSAlignVersionType
        {
            Unknown = 0,
            v0pt5 = 1,
            v0pt6 = 2,
            v0pt7 = 3
        }

        #endregion

        #region "Structures"

        protected struct udtInputPropertyValuesType
        {
            public string FastaFileName;
            public string SpectrumFileName;
            public string ResultTableFileName;
            public string ResultDetailsFileName;

            public void Clear()
            {
                FastaFileName = string.Empty;
                SpectrumFileName = string.Empty;
                ResultTableFileName = string.Empty;
                ResultDetailsFileName = string.Empty;
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

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSAlign tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            var processingError = false;

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMSAlign.RunTool(): Enter");
                }

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
                // Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
                var JavaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(JavaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MSAlign program
                // Note that
                mMSAlignProgLoc = DetermineProgramLocation("MSAlignProgLoc", Path.Combine("jar", MSAlign_JAR_NAME));

                if (string.IsNullOrWhiteSpace(mMSAlignProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                eMSAlignVersionType eMSAlignVersion;
                if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.5" + Path.DirectorySeparatorChar))
                {
                    eMSAlignVersion = eMSAlignVersionType.v0pt5;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.6."))
                {
                    eMSAlignVersion = eMSAlignVersionType.v0pt6;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.7."))
                {
                    eMSAlignVersion = eMSAlignVersionType.v0pt7;
                }
                else
                {
                    // Assume v0.7
                    eMSAlignVersion = eMSAlignVersionType.v0pt7;
                }

                // Store the MSAlign version info in the database after the first line is written to file MSAlign_ConsoleOutput.txt
                // (only valid for MSAlign 0.6.2 or newer)

                mToolVersionWritten = false;
                mMSAlignVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Clear InputProperties parameters
                mInputPropertyValues.Clear();
                mMSAlignWorkFolderPath = string.Empty;

                // Copy the MS Align program files and associated files to the work directory
                if (!CopyMSAlignProgramFiles(mMSAlignProgLoc, eMSAlignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize the MSInput folder
                if (!InitializeMSInputFolder(mMSAlignWorkFolderPath, eMSAlignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MSAlign");

                // Lookup the amount of memory to reserve for Java; default to 2 GB
                var intJavaMemorySize = mJobParams.GetJobParameter("MSAlignJavaMemorySize", 2000);
                if (intJavaMemorySize < 512)
                    intJavaMemorySize = 512;

                // Set up and execute a program runner to run MSAlign
                string cmdStr;
                if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                {
                    cmdStr = " -Xmx" + intJavaMemorySize + "M -classpath jar\\malign.jar;jar\\* edu.ucsd.msalign.spec.web.Pipeline .\\";
                }
                else
                {
                    cmdStr = " -Xmx" + intJavaMemorySize + "M -classpath jar\\*; edu.ucsd.msalign.align.console.MsAlignPipeline .\\";
                }

                LogDebug(JavaProgLoc + " " + cmdStr);

                mCmdRunner = new clsRunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = false;
                mCmdRunner.EchoOutputToConsole = true;

                if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                {
                    mCmdRunner.WriteConsoleOutputToFile = false;
                }
                else
                {
                    mCmdRunner.WriteConsoleOutputToFile = true;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT);
                }

                mProgress = PROGRESS_PCT_STARTING;
                ResetProgRunnerCpuUsage();

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(JavaProgLoc, cmdStr, "MSAlign", true);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mMSAlignVersion))
                    {
                        ParseConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                CloseOutType eResult;
                if (!processingSuccess)
                {
                    LogError("Error running MSAlign");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSAlign returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSAlign failed (but exit code is 0)");
                    }

                    processingError = true;
                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Make sure the output files were created
                    if (!ValidateAndCopyResultFiles(eMSAlignVersion))
                    {
                        processingError = true;
                    }

                    var strResultTableFilePath = Path.Combine(mWorkDir, mDatasetName + RESULT_TABLE_NAME_SUFFIX);

                    if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                    {
                        // Add a header to the _ResultTable.txt file
                        AddResultTableHeaderLine(strResultTableFilePath);
                    }

                    // Make sure the _ResultTable.txt file is not empty
                    if (processingError)
                    {
                        eResult = CloseOutType.CLOSEOUT_FAILED;
                    }
                    else
                    {
                        if (ValidateResultTableFile(strResultTableFilePath))
                        {
                            eResult = CloseOutType.CLOSEOUT_SUCCESS;
                        }
                        else
                        {
                            eResult = CloseOutType.CLOSEOUT_NO_DATA;
                        }
                    }

                    mStatusTools.UpdateAndWrite(mProgress);
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("MSAlign Search Complete");
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (eMSAlignVersion != eMSAlignVersionType.v0pt5)
                {
                    // Trim the console output file to remove the majority of the % finished messages
                    TrimConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));
                }

                if (processingError)
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
                mMessage = "Error in MSAlignPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        protected bool AddResultTableHeaderLine(string strSourceFilePath)
        {
            try
            {
                if (!File.Exists(strSourceFilePath))
                    return false;

                if (mDebugLevel >= 1)
                {
                    LogMessage("Adding header line to MSAlign_ResultTable.txt file");
                }

                var strTargetFilePath = strSourceFilePath + ".tmp";

                // Open the input file and
                // create the output file
                using (var reader = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var headerLine = "Prsm_ID\t" + "Spectrum_ID\t" + "Protein_Sequence_ID\t" + "Spectrum_ID\t" + "Scan(s)\t" + "#peaks\t" + "Charge\t" +
                                     "Precursor_mass\t" + "Protein_name\t" + "Protein_mass\t" + "First_residue\t" + "Last_residue\t" + "Peptide\t" +
                                     "#unexpected_modifications\t" + "#matched_peaks\t" + "#matched_fragment_ions\t" + "E-value";

                    writer.WriteLine(headerLine);

                    while (!reader.EndOfStream)
                    {
                        writer.WriteLine(reader.ReadLine());
                    }
                }


                // Delete the source file, then rename the new file to match the source file
                File.Delete(strSourceFilePath);

                File.Move(strTargetFilePath, strSourceFilePath);
            }
            catch (Exception ex)
            {
                LogError("Exception in AddResultTableHeaderLine", ex);
                return false;
            }

            return true;
        }

        protected bool CopyFastaCheckResidues(string strSourceFilePath, string strTargetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            var intWarningCount = 0;

            try
            {
                var reInvalidResidues = new Regex(@"[BJOUXZ]", RegexOptions.Compiled);

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
            mJobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool CopyMSAlignProgramFiles(string strMSAlignJarFilePath, eMSAlignVersionType eMSAlignVersion)
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
                diMSAlignWork.CreateSubdirectory("msinput");
                diMSAlignWork.CreateSubdirectory("msoutput");
                diMSAlignWork.CreateSubdirectory("xml");
                diMSAlignWork.CreateSubdirectory("xsl");
                if (eMSAlignVersion != eMSAlignVersionType.v0pt5)
                {
                    diMSAlignWork.CreateSubdirectory("etc");
                }

                // Copy all files in the jar and xsl folders to the target
                var lstSubfolderNames = new List<string> {
                    "jar", "xsl"
                };

                if (eMSAlignVersion != eMSAlignVersionType.v0pt5)
                {
                    lstSubfolderNames.Add("etc");
                }

                foreach (var strSubFolder in lstSubfolderNames)
                {
                    var strTargetSubfolder = Path.Combine(diMSAlignWork.FullName, strSubFolder);

                    var diSubfolder = diMSAlignSrc.GetDirectories(strSubFolder);

                    if (diSubfolder.Length == 0)
                    {
                        LogError("Source MSAlign subfolder not found: " + strTargetSubfolder);
                        return false;
                    }

                    foreach (var ioFile in diSubfolder[0].GetFiles())
                    {
                        ioFile.CopyTo(Path.Combine(strTargetSubfolder, ioFile.Name));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CopyMSAlignProgramFiles", ex);
                return false;
            }

            return true;
        }

        protected bool CreateInputPropertiesFile(string strParamFilePath, string strMSInputFolderPath, eMSAlignVersionType eMSAlignVersion)
        {
            // ReSharper disable StringLiteralTypo

            const string DB_FILENAME_LEGACY = "database";
            const string DB_FILENAME = "databaseFileName";
            const string SPEC_FILENAME = "spectrumFileName";
            const string TABLE_OUTPUT_FILENAME = "tableOutputFileName";
            const string DETAIL_OUTPUT_FILENAME = "detailOutputFileName";

            const string DB_FILENAME_LEGACY_LOWER = "database";

            const string DB_FILENAME_LOWER = "databasefilename";
            const string SPEC_FILENAME_LOWER = "spectrumfilename";
            const string TABLE_OUTPUT_FILENAME_LOWER = "tableoutputfilename";
            const string DETAIL_OUTPUT_FILENAME_LOWER = "detailoutputfilename";

            // These key names must be lowercase
            const string INSTRUMENT_TYPE_KEY = "instrument";          // This only applies to v0.5 of MSAlign
            const string INSTRUMENT_ACTIVATION_TYPE_KEY = "activation";
            const string SEARCH_TYPE_KEY = "searchtype";
            const string CUTOFF_TYPE_KEY = "cutofftype";
            const string CUTOFF_KEY = "cutoff";

            // ReSharper restore StringLiteralTypo

            var blnEValueCutoffType = false;

            try
            {
                // Initialize the dictionary that maps new names to legacy names
                // Version 0.5 used the legacy names, e.g. it used "threshold" instead of "eValueThreshold"
                var dctLegacyKeyMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"errorTolerance", "fragmentTolerance"},
                    {"eValueThreshold", "threshold"},
                    {"shiftNumber", "shifts"},
                    {"cysteineProtection", "protection"},
                    {"activation", "instrument"}
                };

                // Note: Starting with version 0.7, the "eValueThreshold" parameter was replaced with two new parameters:
                // cutoffType and cutoff

                var strOutputFilePath = Path.Combine(strMSInputFolderPath, "input.properties");

                // Open the input file and
                // Create the output file
                using (var reader = new StreamReader(new FileStream(strParamFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the database name and input file name
                    if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                    {
                        writer.WriteLine(DB_FILENAME_LEGACY + "=" + mInputPropertyValues.FastaFileName);
                        // Input file name is assumed to be input_data
                    }
                    else
                    {
                        writer.WriteLine(DB_FILENAME + "=" + mInputPropertyValues.FastaFileName);
                        writer.WriteLine(SPEC_FILENAME + "=" + mInputPropertyValues.SpectrumFileName);
                    }

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine) || dataLine.TrimStart().StartsWith("#"))
                        {
                            // Comment line or blank line; write it out as-is
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        // Look for an equals sign
                        var intEqualsIndex = dataLine.IndexOf('=');

                        if (intEqualsIndex <= 0)
                        {
                            // Unknown line format; skip it
                            continue;
                        }

                        // Split the line on the equals sign
                        var strKeyName = dataLine.Substring(0, intEqualsIndex).TrimEnd();
                        string strValue;

                        if (intEqualsIndex < dataLine.Length - 1)
                        {
                            strValue = dataLine.Substring(intEqualsIndex + 1).Trim();
                        }
                        else
                        {
                            strValue = string.Empty;
                        }

                        if (strKeyName.ToLower() == INSTRUMENT_ACTIVATION_TYPE_KEY || strKeyName.ToLower() == INSTRUMENT_TYPE_KEY)
                        {
                            // If this is a bruker dataset, we need to make sure that the value for this entry is not FILE
                            // The reason is that the mzXML file created by Bruker's compass program does not include the ScanType information (CID, ETD, etc.)
                            var strToolName = mJobParams.GetParam("ToolName");

                            if (strToolName == "MSAlign_Bruker")
                            {
                                if (strValue.ToUpper() == "FILE")
                                {
                                    mMessage = "Must specify an explicit scan type for " + strKeyName +
                                                " in the MSAlign parameter file (CID, HCD, or ETD)";

                                    LogError(mMessage +
                                             "; this is required because Bruker-created mzXML files do not include activationMethod information in the precursorMz tag");

                                    return false;
                                }
                            }
                        }

                        if (strKeyName.ToLower() == SEARCH_TYPE_KEY)
                        {
                            if (strValue.ToUpper() == "TARGET+DECOY")
                            {
                                // Make sure the protein collection is not a Decoy protein collection
                                var strProteinOptions = mJobParams.GetParam("ProteinOptions");

                                if (strProteinOptions.ToLower().Contains("seq_direction=decoy"))
                                {
                                    mMessage = "MSAlign parameter file contains searchType=TARGET+DECOY; " +
                                                "protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy";

                                    LogError(mMessage);

                                    return false;
                                }
                            }
                        }

                        // Examine the key name to determine what to do
                        switch (strKeyName.ToLower())
                        {
                            case DB_FILENAME_LEGACY_LOWER:
                            case DB_FILENAME_LOWER:
                            case SPEC_FILENAME_LOWER:
                                break;
                            // Skip this line; we defined it above
                            case TABLE_OUTPUT_FILENAME_LOWER:
                            case DETAIL_OUTPUT_FILENAME_LOWER:
                                break;
                            // Skip this line; we'll define it later
                            default:

                                if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                                {
                                    // Running a legacy version; rename the keys

                                    if (dctLegacyKeyMap.TryGetValue(strKeyName, out var strLegacyKeyName))
                                    {
                                        switch (strLegacyKeyName)
                                        {
                                            case "protection":
                                                // Need to update the value
                                                switch (strValue.ToUpper())
                                                {
                                                    case "C57":
                                                        strValue = "Carbamidomethylation";
                                                        break;
                                                    case "C58":
                                                        strValue = "Carboxymethylation";
                                                        break;
                                                    default:
                                                        // Includes "C0"
                                                        strValue = "None";
                                                        break;
                                                }
                                                break;

                                            case "instrument":
                                                if (strValue == "FILE")
                                                {
                                                    // Legacy mode does not support "FILE"
                                                    // Auto-switch to CID and log a warning message
                                                    strValue = "CID";
                                                    LogWarning(
                                                        "Using instrument mode 'CID' since v0.5 of MSAlign does not support reading the activation mode from the msalign file");
                                                }
                                                break;

                                            default:
                                                if (strKeyName.ToLower() == CUTOFF_TYPE_KEY)
                                                {
                                                    if (strValue.ToUpper() == "EVALUE")
                                                    {
                                                        blnEValueCutoffType = true;
                                                    }
                                                }
                                                else if (strKeyName.ToLower() == CUTOFF_KEY)
                                                {
                                                    // v0.5 doesn't support the cutoff parameter
                                                    // If the parameter file had cutoffType=EVALUE then we're OK; otherwise abort
                                                    if (blnEValueCutoffType)
                                                    {
                                                        strLegacyKeyName = "threshold";
                                                    }
                                                    else
                                                    {
                                                        mMessage =
                                                            "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.5";
                                                        LogError(mMessage);
                                                        return false;
                                                    }
                                                }
                                                break;
                                        }

                                        writer.WriteLine(strLegacyKeyName + "=" + strValue);
                                    }
                                }
                                else
                                {
                                    if (eMSAlignVersion >= eMSAlignVersionType.v0pt7 && strKeyName.ToLower() == "eValueThreshold")
                                    {
                                        // v0.7 and up use cutoffType and cutoff instead of eValueThreshold
                                        writer.WriteLine("cutoffType=EVALUE");
                                        writer.WriteLine("cutoff=" + strValue);
                                    }
                                    else if (eMSAlignVersion == eMSAlignVersionType.v0pt6 && strKeyName.ToLower() == CUTOFF_TYPE_KEY)
                                    {
                                        if (strValue.ToUpper() == "EVALUE")
                                        {
                                            blnEValueCutoffType = true;
                                        }
                                    }
                                    else if (eMSAlignVersion == eMSAlignVersionType.v0pt6 && strKeyName.ToLower() == CUTOFF_KEY)
                                    {
                                        if (blnEValueCutoffType)
                                        {
                                            // v0.6 doesn't support the cutoff parameter, just eValueThreshold
                                            writer.WriteLine("eValueThreshold=" + strValue);
                                        }
                                        else
                                        {
                                            mMessage =
                                                "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.6";
                                            LogError(mMessage);
                                            return false;
                                        }
                                    }
                                    else
                                    {
                                        // Write out as-is
                                        writer.WriteLine(dataLine);
                                    }
                                }

                                break;
                        }
                    }

                    if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                    {
                        mInputPropertyValues.ResultTableFileName = RESULT_TABLE_NAME_LEGACY;
                        mInputPropertyValues.ResultDetailsFileName = RESULT_DETAILS_NAME_LEGACY;
                    }
                    else
                    {
                        mInputPropertyValues.ResultTableFileName = mDatasetName + RESULT_TABLE_NAME_SUFFIX;
                        mInputPropertyValues.ResultDetailsFileName = mDatasetName + RESULT_DETAILS_NAME_SUFFIX;
                    }

                    if (eMSAlignVersion != eMSAlignVersionType.v0pt5)
                    {
                        writer.WriteLine(TABLE_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultTableFileName);
                        writer.WriteLine(DETAIL_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultDetailsFileName);
                    }
                }

                // Copy the newly created input.properties file to the work directory
                File.Copy(strOutputFilePath, Path.Combine(mWorkDir, Path.GetFileName(strOutputFilePath)), true);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateInputPropertiesFile";
                LogError("Exception in CreateInputPropertiesFile", ex);
                return false;
            }

            return true;
        }

        protected bool InitializeMSInputFolder(string strMSAlignWorkFolderPath, eMSAlignVersionType eMSAlignVersion)
        {

            try
            {
                var strMSInputFolderPath = Path.Combine(strMSAlignWorkFolderPath, "msinput");
                var fiSourceFolder = new DirectoryInfo(mWorkDir);

                // Copy the .Fasta file into the MSInput folder
                // MSAlign will crash if any non-standard residues are present (BJOUXZ)
                // Thus, we will read the source file with a reader and create a new fasta file

                // Define the path to the fasta file
                var OrgDbDir = mMgrParams.GetParam("OrgDbDir");
                var strFASTAFilePath = Path.Combine(OrgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

                var fiFastaFile = new FileInfo(strFASTAFilePath);

                if (!fiFastaFile.Exists)
                {
                    // Fasta file not found
                    LogError("Fasta file not found: " + fiFastaFile.FullName);
                    return false;
                }

                mInputPropertyValues.FastaFileName = string.Copy(fiFastaFile.Name);

                if (!CopyFastaCheckResidues(fiFastaFile.FullName, Path.Combine(strMSInputFolderPath, mInputPropertyValues.FastaFileName)))
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "CopyFastaCheckResidues returned false";
                    LogError(mMessage);
                    return false;
                }

                // Move the _msdeconv.msalign file to the MSInput folder
                var fiFiles = fiSourceFolder.GetFiles("*" + clsAnalysisResourcesMSAlign.MSDECONV_MSALIGN_FILE_SUFFIX);
                if (fiFiles.Length == 0)
                {
                    LogError("MSAlign file not found in work directory");
                    return false;
                }

                if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                {
                    // Rename the file to input_data when we move it
                    mInputPropertyValues.SpectrumFileName = "input_data";
                }
                else
                {
                    mInputPropertyValues.SpectrumFileName = string.Copy(fiFiles[0].Name);
                }
                fiFiles[0].MoveTo(Path.Combine(strMSInputFolderPath, mInputPropertyValues.SpectrumFileName));

                var strParamFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParmFileName"));

                if (!CreateInputPropertiesFile(strParamFilePath, strMSInputFolderPath, eMSAlignVersion))
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in InitializeMSInputFolder", ex);
                return false;
            }

            return true;
        }

        // Example Console output (v0.5 does not have console output)
        //
        // Initializing indexes...
        // Processing spectrum scan 660...         0% finished (0 minutes used).
        // Processing spectrum scan 1329...        1% finished (0 minutes used).
        // Processing spectrum scan 1649...        1% finished (0 minutes used).
        private readonly Regex reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSAlign console output file to determine the MSAlign version and to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
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

                short actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            if (linesRead <= 3)
                            {
                                // Originally the first line was the MSAlign version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the MSAlign version
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
                                    if (dataLine.ToLower().Contains("error"))
                                    {
                                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                        {
                                            mConsoleOutputErrorMsg = "Error running MSAlign:";
                                        }
                                        mConsoleOutputErrorMsg += "; " + dataLine;
                                    }
                                }
                            }

                            // Update progress if the line starts with Processing spectrum
                            if (dataLine.StartsWith("Processing spectrum"))
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
                            else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                if (dataLine.ToLower().StartsWith("error"))
                                {
                                    mConsoleOutputErrorMsg += "; " + dataLine;
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
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + ")", ex);
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
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private readonly Regex reExtractScan = new Regex(@"Processing spectrum scan (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Reads the console output file and removes the majority of the percent finished messages
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string strConsoleOutputFilePath)
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
                    LogDebug("Trimming console output file at " + strConsoleOutputFilePath);
                }

                var strMostRecentProgressLine = string.Empty;
                var strMostRecentProgressLineWritten = string.Empty;

                var strTrimmedFilePath = strConsoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intScanNumberOutputThreshold = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var blnKeepLine = true;

                        var oMatch = reExtractScan.Match(dataLine);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out var intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                    strMostRecentProgressLineWritten = string.Copy(dataLine);
                                }
                            }
                            strMostRecentProgressLine = string.Copy(dataLine);
                        }
                        else if (dataLine.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (string.CompareOrdinal(strMostRecentProgressLine, strMostRecentProgressLineWritten) != 0)
                            {
                                writer.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(strConsoleOutputFilePath);
                    File.Move(strTrimmedFilePath, strConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + strConsoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + strConsoleOutputFilePath + ")", ex);
                }
            }
        }

        protected bool ValidateAndCopyResultFiles(eMSAlignVersionType eMSAlignVersion)
        {
            var strResultsFolderPath = Path.Combine(mMSAlignWorkFolderPath, "msoutput");
            var lstResultsFilesToMove = new List<string>();
            var processingError = false;

            try
            {
                lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultTableFileName));
                lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultDetailsFileName));

                foreach (var resultFilePath in lstResultsFilesToMove)
                {
                    var fiSearchResultFile = new FileInfo(resultFilePath);

                    if (!fiSearchResultFile.Exists)
                    {
                        var msg = "MSAlign results file not found";

                        if (!processingError)
                        {
                            // This is the first missing file; update the base-class comment
                            LogError(msg + ": " + fiSearchResultFile.Name);
                            processingError = true;
                        }

                        LogErrorNoMessageUpdate(msg + ": " + fiSearchResultFile.FullName);
                    }
                    else
                    {
                        // Copy the results file to the work directory
                        var strTargetFileName = string.Copy(fiSearchResultFile.Name);

                        if (eMSAlignVersion == eMSAlignVersionType.v0pt5)
                        {
                            // Rename the file when we copy it
                            switch (fiSearchResultFile.Name)
                            {
                                case RESULT_TABLE_NAME_LEGACY:
                                    strTargetFileName = mDatasetName + RESULT_TABLE_NAME_SUFFIX;
                                    break;
                                case RESULT_DETAILS_NAME_LEGACY:
                                    strTargetFileName = mDatasetName + RESULT_DETAILS_NAME_SUFFIX;
                                    break;
                            }
                        }

                        fiSearchResultFile.CopyTo(Path.Combine(mWorkDir, strTargetFileName), true);
                    }
                }

                // Zip the Html and XML folders
                ZipMSAlignResultFolder("html");
                ZipMSAlignResultFolder("XML");
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateAndCopyResultFiles", ex);
                return false;
            }

            if (processingError)
            {
                return false;
            }

            return true;
        }

        protected bool ValidateResultTableFile(string sourceFilePath)
        {
            try
            {
                var validFile = false;

                if (!File.Exists(sourceFilePath))
                {
                    if (mDebugLevel >= 2)
                    {
                        LogWarning("MSAlign_ResultTable.txt file not found: " + sourceFilePath);
                    }
                    return false;
                }

                if (mDebugLevel >= 2)
                {
                    LogMessage("Validating that the MSAlign_ResultTable.txt file is not empty");
                }

                // Open the input file
                using (var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrEmpty(dataLine))
                        {
                            var dataCols = dataLine.Split('\t');

                            if (dataCols.Length > 1)
                            {
                                // Look for an integer in the first or second column
                                // Version 0.5 and 0.6 had Prsm_ID in the first column
                                // Version 0.7 moved Prsm_ID to the second column
                                if (int.TryParse(dataCols[1], out _) || int.TryParse(dataCols[0], out _))
                                {
                                    // Integer found; line is valid
                                    validFile = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (!validFile)
                {
                    LogError("MSAlign_ResultTable.txt file is empty");
                    return false;
                }
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
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MSAlign_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSAlignVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MSAlign");
            }
        }

        #endregion
    }
}
