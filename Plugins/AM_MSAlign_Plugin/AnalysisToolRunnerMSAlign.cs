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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSAlignPlugIn
{
    /// <summary>
    /// Class for running MSAlign analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSAlign : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Bruker, Bruker's, Carbamidomethylation, Carboxymethylation, classpath, html, msinput, msoutput, Prsm, Parm, xsl, Xmx
        // Ignore Spelling: databasefilename, spectrumfilename, tableoutputfilename, detailoutputfilename,searchtype, cutofftype

        // ReSharper restore CommentTypo

        private const string MSAlign_CONSOLE_OUTPUT = "MSAlign_ConsoleOutput.txt";
        private const string MSAlign_JAR_NAME = "MSAlign.jar";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string RESULT_TABLE_NAME_SUFFIX = "_MSAlign_ResultTable.txt";
        private const string RESULT_TABLE_NAME_LEGACY = "result_table.txt";

        private const string RESULT_DETAILS_NAME_SUFFIX = "_MSAlign_ResultDetails.txt";
        private const string RESULT_DETAILS_NAME_LEGACY = "result.txt";

        // Note that newer versions are assumed to have higher enum values
        private enum MSAlignVersionType
        {
            Unknown = 0,
            v0pt5 = 1,
            v0pt6 = 2,
            v0pt7 = 3
        }

        private struct InputPropertyValues
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

        private bool mToolVersionWritten;
        private string mMSAlignVersion;

        private string mMSAlignProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mMSAlignWorkFolderPath;
        private InputPropertyValues mInputPropertyValues;

        private RunDosProgram mCmdRunner;

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
                    LogDebug("AnalysisToolRunnerMSAlign.RunTool(): Enter");
                }

                // Verify that program files exist

                // javaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
                // Note that we need to run MSAlign with a 64-bit version of Java since it prefers to use 2 or more GB of ram
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrEmpty(javaProgLoc))
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

                MSAlignVersionType msAlignVersion;

                if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.5" + Path.DirectorySeparatorChar))
                {
                    msAlignVersion = MSAlignVersionType.v0pt5;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.6."))
                {
                    msAlignVersion = MSAlignVersionType.v0pt6;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.7."))
                {
                    msAlignVersion = MSAlignVersionType.v0pt7;
                }
                else
                {
                    // Assume v0.7
                    msAlignVersion = MSAlignVersionType.v0pt7;
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
                if (!CopyMSAlignProgramFiles(mMSAlignProgLoc, msAlignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize the MSInput folder
                if (!InitializeMSInputFolder(mMSAlignWorkFolderPath, msAlignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MSAlign");

                // Lookup the amount of memory to reserve for Java; default to 2 GB
                var javaMemorySize = mJobParams.GetJobParameter("MSAlignJavaMemorySize", 2000);

                if (javaMemorySize < 512)
                    javaMemorySize = 512;

                // Set up and execute a program runner to run MSAlign
                string arguments;

                if (msAlignVersion == MSAlignVersionType.v0pt5)
                {
                    arguments = " -Xmx" + javaMemorySize + "M -classpath jar\\malign.jar;jar\\* edu.ucsd.msalign.spec.web.Pipeline .\\";
                }
                else
                {
                    arguments = " -Xmx" + javaMemorySize + "M -classpath jar\\*; edu.ucsd.msalign.align.console.MsAlignPipeline .\\";
                }

                LogDebug(javaProgLoc + " " + arguments);

                mCmdRunner = new RunDosProgram(mMSAlignWorkFolderPath, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = false;
                mCmdRunner.EchoOutputToConsole = true;

                if (msAlignVersion == MSAlignVersionType.v0pt5)
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
                var processingSuccess = mCmdRunner.RunProgram(javaProgLoc, arguments, "MSAlign", true);

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
                    if (!ValidateAndCopyResultFiles(msAlignVersion))
                    {
                        processingError = true;
                    }

                    var resultTableFilePath = Path.Combine(mWorkDir, mDatasetName + RESULT_TABLE_NAME_SUFFIX);

                    if (msAlignVersion == MSAlignVersionType.v0pt5)
                    {
                        // Add a header to the _ResultTable.txt file
                        AddResultTableHeaderLine(resultTableFilePath);
                    }

                    // Make sure the _ResultTable.txt file is not empty
                    if (processingError)
                    {
                        eResult = CloseOutType.CLOSEOUT_FAILED;
                    }
                    else
                    {
                        if (ValidateResultTableFile(resultTableFilePath))
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
                PRISM.AppUtils.GarbageCollectNow();

                if (msAlignVersion != MSAlignVersionType.v0pt5)
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

        private bool AddResultTableHeaderLine(string sourceFilePath)
        {
            try
            {
                if (!File.Exists(sourceFilePath))
                    return false;

                if (mDebugLevel >= 1)
                {
                    LogMessage("Adding header line to MSAlign_ResultTable.txt file");
                }

                var targetFilePath = sourceFilePath + ".tmp";

                // Open the input file and
                // create the output file
                using (var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    const string headerLine = "Prsm_ID\t" + "Spectrum_ID\t" + "Protein_Sequence_ID\t" + "Spectrum_ID\t" + "Scan(s)\t" + "#peaks\t" + "Charge\t" +
                                     "Precursor_mass\t" + "Protein_name\t" + "Protein_mass\t" + "First_residue\t" + "Last_residue\t" + "Peptide\t" +
                                     "#unexpected_modifications\t" + "#matched_peaks\t" + "#matched_fragment_ions\t" + "E-value";

                    writer.WriteLine(headerLine);

                    while (!reader.EndOfStream)
                    {
                        writer.WriteLine(reader.ReadLine());
                    }
                }

                // Delete the source file, then rename the new file to match the source file
                File.Delete(sourceFilePath);

                File.Move(targetFilePath, sourceFilePath);
            }
            catch (Exception ex)
            {
                LogError("Exception in AddResultTableHeaderLine", ex);
                return false;
            }

            return true;
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
            mJobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool CopyMSAlignProgramFiles(string msAlignJarFilePath, MSAlignVersionType msAlignVersion)
        {
            try
            {
                var msAlignJarFile = new FileInfo(msAlignJarFilePath);

                if (!msAlignJarFile.Exists)
                {
                    LogError("MSAlign .Jar file not found: " + msAlignJarFile.FullName);
                    return false;
                }

                if (msAlignJarFile.Directory == null)
                {
                    LogError("Unable to determine the parent directory of " + msAlignJarFile.FullName);
                    return false;
                }

                if (msAlignJarFile.Directory.Parent == null)
                {
                    LogError("Unable to determine the parent directory of " + msAlignJarFile.Directory.FullName);
                    return false;
                }

                // The source folder is one level up from the .Jar file
                var msAlignSrc = new DirectoryInfo(msAlignJarFile.Directory.Parent.FullName);
                var msAlignWork = new DirectoryInfo(Path.Combine(mWorkDir, "MSAlign"));

                LogMessage("Copying MSAlign program file to the Work Directory");

                // Make sure the directory doesn't already exist
                if (msAlignWork.Exists)
                {
                    msAlignWork.Delete(true);
                }

                // Create the directory
                msAlignWork.Create();
                mMSAlignWorkFolderPath = msAlignWork.FullName;

                // Create the subdirectories
                msAlignWork.CreateSubdirectory("html");
                msAlignWork.CreateSubdirectory("jar");
                msAlignWork.CreateSubdirectory("msinput");
                msAlignWork.CreateSubdirectory("msoutput");
                msAlignWork.CreateSubdirectory("xml");
                msAlignWork.CreateSubdirectory("xsl");

                if (msAlignVersion != MSAlignVersionType.v0pt5)
                {
                    msAlignWork.CreateSubdirectory("etc");
                }

                // Copy all files in the jar and xsl directories to the target
                var subdirectoryNames = new List<string> {
                    "jar", "xsl"
                };

                if (msAlignVersion != MSAlignVersionType.v0pt5)
                {
                    subdirectoryNames.Add("etc");
                }

                foreach (var subdirectoryToFind in subdirectoryNames)
                {
                    var targetSubdirectory = Path.Combine(msAlignWork.FullName, subdirectoryToFind);

                    var matchingSubdirectory = msAlignSrc.GetDirectories(subdirectoryToFind);

                    if (matchingSubdirectory.Length == 0)
                    {
                        LogError("Source MSAlign subdirectory not found: " + targetSubdirectory);
                        return false;
                    }

                    foreach (var file in matchingSubdirectory[0].GetFiles())
                    {
                        file.CopyTo(Path.Combine(targetSubdirectory, file.Name));
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

        private bool CreateInputPropertiesFile(string paramFilePath, string mSInputFolderPath, MSAlignVersionType msAlignVersion)
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

            var eValueCutoffType = false;

            try
            {
                // Initialize the dictionary that maps new names to legacy names
                // Version 0.5 used the legacy names, e.g. it used "threshold" instead of "eValueThreshold"
                var legacyKeyMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"errorTolerance", "fragmentTolerance"},
                    {"eValueThreshold", "threshold"},
                    {"shiftNumber", "shifts"},
                    {"cysteineProtection", "protection"},
                    {"activation", "instrument"}
                };

                // Note: Starting with version 0.7, the "eValueThreshold" parameter was replaced with two new parameters:
                // cutoffType and cutoff

                var outputFilePath = Path.Combine(mSInputFolderPath, "input.properties");

                // Open the input file and
                // Create the output file
                using (var reader = new StreamReader(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the database name and input file name
                    if (msAlignVersion == MSAlignVersionType.v0pt5)
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

                        if (keyName.ToLower() == INSTRUMENT_ACTIVATION_TYPE_KEY || keyName.ToLower() == INSTRUMENT_TYPE_KEY)
                        {
                            // If this is a Bruker dataset, we need to make sure that the value for this entry is not FILE
                            // The reason is that the mzXML file created by Bruker's compass program does not include the ScanType information (CID, ETD, etc.)

                            // The ToolName job parameter holds the name of the job script we are executing
                            var scriptName = mJobParams.GetParam("ToolName");

                            if (scriptName == "MSAlign_Bruker")
                            {
                                if (string.Equals(value, "FILE", StringComparison.OrdinalIgnoreCase))
                                {
                                    mMessage = "Must specify an explicit scan type for " + keyName +
                                                " in the MSAlign parameter file (CID, HCD, or ETD)";

                                    LogError(mMessage +
                                             "; this is required because Bruker-created mzXML files do not include activationMethod information in the precursorMz tag");

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
                                    mMessage = "MSAlign parameter file contains searchType=TARGET+DECOY; " +
                                                "protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy";

                                    LogError(mMessage);

                                    return false;
                                }
                            }
                        }

                        // Examine the key name to determine what to do
                        switch (keyName.ToLower())
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

                                if (msAlignVersion == MSAlignVersionType.v0pt5)
                                {
                                    // Running a legacy version; rename the keys

                                    if (legacyKeyMap.TryGetValue(keyName, out var legacyKeyName))
                                    {
                                        switch (legacyKeyName)
                                        {
                                            case "protection":
                                                // Need to update the value
                                                value = value.ToUpper() switch
                                                {
                                                    "C57" => "Carbamidomethylation",
                                                    "C58" => "Carboxymethylation",
                                                    _ => "None"
                                                };
                                                break;

                                            case "instrument":
                                                if (value == "FILE")
                                                {
                                                    // Legacy mode does not support "FILE"
                                                    // Auto-switch to CID and log a warning message
                                                    value = "CID";
                                                    LogWarning(
                                                        "Using instrument mode 'CID' since v0.5 of MSAlign does not support reading the activation mode from the MSAlign file");
                                                }
                                                break;

                                            default:
                                                if (keyName.ToLower() == CUTOFF_TYPE_KEY)
                                                {
                                                    if (string.Equals(value, "EVALUE", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        eValueCutoffType = true;
                                                    }
                                                }
                                                else if (keyName.ToLower() == CUTOFF_KEY)
                                                {
                                                    // v0.5 doesn't support the cutoff parameter
                                                    // If the parameter file had cutoffType=EVALUE then we're OK; otherwise abort
                                                    if (eValueCutoffType)
                                                    {
                                                        legacyKeyName = "threshold";
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

                                        writer.WriteLine(legacyKeyName + "=" + value);
                                    }
                                }
                                else
                                {
                                    if (msAlignVersion >= MSAlignVersionType.v0pt7 && string.Equals(keyName, "eValueThreshold", StringComparison.OrdinalIgnoreCase))
                                    {
                                        // v0.7 and up use cutoffType and cutoff instead of eValueThreshold
                                        writer.WriteLine("cutoffType=EVALUE");
                                        writer.WriteLine("cutoff=" + value);
                                    }
                                    else if (msAlignVersion == MSAlignVersionType.v0pt6 && keyName.ToLower() == CUTOFF_TYPE_KEY)
                                    {
                                        if (string.Equals(value, "EVALUE", StringComparison.OrdinalIgnoreCase))
                                        {
                                            eValueCutoffType = true;
                                        }
                                    }
                                    else if (msAlignVersion == MSAlignVersionType.v0pt6 && keyName.ToLower() == CUTOFF_KEY)
                                    {
                                        if (eValueCutoffType)
                                        {
                                            // v0.6 doesn't support the cutoff parameter, just eValueThreshold
                                            writer.WriteLine("eValueThreshold=" + value);
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

                    if (msAlignVersion == MSAlignVersionType.v0pt5)
                    {
                        mInputPropertyValues.ResultTableFileName = RESULT_TABLE_NAME_LEGACY;
                        mInputPropertyValues.ResultDetailsFileName = RESULT_DETAILS_NAME_LEGACY;
                    }
                    else
                    {
                        mInputPropertyValues.ResultTableFileName = mDatasetName + RESULT_TABLE_NAME_SUFFIX;
                        mInputPropertyValues.ResultDetailsFileName = mDatasetName + RESULT_DETAILS_NAME_SUFFIX;
                    }

                    if (msAlignVersion != MSAlignVersionType.v0pt5)
                    {
                        writer.WriteLine(TABLE_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultTableFileName);
                        writer.WriteLine(DETAIL_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultDetailsFileName);
                    }
                }

                // Copy the newly created input.properties file to the work directory
                File.Copy(outputFilePath, Path.Combine(mWorkDir, Path.GetFileName(outputFilePath)), true);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateInputPropertiesFile";
                LogError("Exception in CreateInputPropertiesFile", ex);
                return false;
            }

            return true;
        }

        private bool InitializeMSInputFolder(string msAlignWorkFolderPath, MSAlignVersionType msAlignVersion)
        {
            try
            {
                var msInputFolderPath = Path.Combine(msAlignWorkFolderPath, "msinput");
                var sourceDirectory = new DirectoryInfo(mWorkDir);

                // Copy the FASTA file into the MSInput folder
                // MSAlign will crash if any non-standard residues are present (BJOUXZ)
                // Thus, we will read the source file with a reader and create a new FASTA file

                // Define the path to the FASTA file
                var organismDbDirectory = mMgrParams.GetParam("OrgDbDir");
                var fastaFilePath = Path.Combine(organismDbDirectory, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

                var fastaFile = new FileInfo(fastaFilePath);

                if (!fastaFile.Exists)
                {
                    // FASTA file not found
                    LogError("FASTA file not found: " + fastaFile.FullName);
                    return false;
                }

                mInputPropertyValues.FastaFileName = fastaFile.Name;

                if (!CopyFastaCheckResidues(fastaFile.FullName, Path.Combine(msInputFolderPath, mInputPropertyValues.FastaFileName)))
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "CopyFastaCheckResidues returned false";
                    LogError(mMessage);
                    return false;
                }

                // Move the _msdeconv.msalign file to the MSInput folder
                var msAlignFile = sourceDirectory.GetFiles("*" + AnalysisResourcesMSAlign.MSDECONV_MSALIGN_FILE_SUFFIX);

                if (msAlignFile.Length == 0)
                {
                    LogError("MSAlign file not found in work directory");
                    return false;
                }

                if (msAlignVersion == MSAlignVersionType.v0pt5)
                {
                    // Rename the file to input_data when we move it
                    mInputPropertyValues.SpectrumFileName = "input_data";
                }
                else
                {
                    mInputPropertyValues.SpectrumFileName = msAlignFile[0].Name;
                }
                msAlignFile[0].MoveTo(Path.Combine(msInputFolderPath, mInputPropertyValues.SpectrumFileName));

                var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("ParamFileName"));

                if (!CreateInputPropertiesFile(paramFilePath, msInputFolderPath, msAlignVersion))
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

        // Initializing indexes...
        // Processing spectrum scan 660...         0% finished (0 minutes used).
        // Processing spectrum scan 1329...        1% finished (0 minutes used).
        // Processing spectrum scan 1649...        1% finished (0 minutes used).

        private readonly Regex reExtractPercentFinished = new(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                            if (linesRead <= 3)
                            {
                                // Originally the first line was the MSAlign version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the MSAlign version
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
                                    if (dataLine.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0)
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
                                var match = reExtractPercentFinished.Match(dataLine);

                                if (match.Success)
                                {
                                    if (short.TryParse(match.Groups[1].Value, out var progress))
                                    {
                                        actualProgress = progress;
                                    }
                                }
                            }
                            else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                if (dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase))
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
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private readonly Regex reExtractScan = new(@"Processing spectrum scan (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Reads the console output file and removes the majority of the percent finished messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
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
                    LogDebug("Trimming console output file at " + consoleOutputFilePath);
                }

                var mostRecentProgressLine = string.Empty;
                var mostRecentProgressLineWritten = string.Empty;

                var trimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var scanNumberOutputThreshold = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var keepLine = true;

                        var match = reExtractScan.Match(dataLine);

                        if (match.Success)
                        {
                            if (int.TryParse(match.Groups[1].Value, out var scanNumber))
                            {
                                if (scanNumber < scanNumberOutputThreshold)
                                {
                                    keepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up scanNumberOutputThreshold by 100
                                    scanNumberOutputThreshold += 100;
                                    mostRecentProgressLineWritten = dataLine;
                                }
                            }
                            mostRecentProgressLine = dataLine;
                        }
                        else if (dataLine.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (!string.Equals(mostRecentProgressLine, mostRecentProgressLineWritten))
                            {
                                writer.WriteLine(mostRecentProgressLine);
                            }
                        }

                        if (keepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(consoleOutputFilePath);
                    File.Move(trimmedFilePath, consoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + consoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        private bool ValidateAndCopyResultFiles(MSAlignVersionType msAlignVersion)
        {
            var resultsFolderPath = Path.Combine(mMSAlignWorkFolderPath, "msoutput");
            var resultsFilesToMove = new List<string>();
            var processingError = false;

            try
            {
                resultsFilesToMove.Add(Path.Combine(resultsFolderPath, mInputPropertyValues.ResultTableFileName));
                resultsFilesToMove.Add(Path.Combine(resultsFolderPath, mInputPropertyValues.ResultDetailsFileName));

                foreach (var resultFilePath in resultsFilesToMove)
                {
                    var searchResultFile = new FileInfo(resultFilePath);

                    if (!searchResultFile.Exists)
                    {
                        const string msg = "MSAlign results file not found";

                        if (!processingError)
                        {
                            // This is the first missing file; update the base-class comment
                            LogError(msg + ": " + searchResultFile.Name);
                            processingError = true;
                        }

                        LogErrorNoMessageUpdate(msg + ": " + searchResultFile.FullName);
                    }
                    else
                    {
                        // Copy the results file to the work directory
                        var targetFileName = searchResultFile.Name;

                        if (msAlignVersion == MSAlignVersionType.v0pt5)
                        {
                            // Rename the file when we copy it
                            switch (searchResultFile.Name)
                            {
                                case RESULT_TABLE_NAME_LEGACY:
                                    targetFileName = mDatasetName + RESULT_TABLE_NAME_SUFFIX;
                                    break;
                                case RESULT_DETAILS_NAME_LEGACY:
                                    targetFileName = mDatasetName + RESULT_DETAILS_NAME_SUFFIX;
                                    break;
                            }
                        }

                        searchResultFile.CopyTo(Path.Combine(mWorkDir, targetFileName), true);
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

            return !processingError;
        }

        private bool ValidateResultTableFile(string sourceFilePath)
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

        private bool ZipMSAlignResultFolder(string folderName)
        {
            try
            {
                var zipFilePath = Path.Combine(mWorkDir, mDatasetName + "_MSAlign_Results_" + folderName.ToUpper() + ".zip");
                var sourceFolderPath = Path.Combine(mMSAlignWorkFolderPath, folderName);

                // Confirm that the directory has one or more files or subdirectories
                var sourceFolder = new DirectoryInfo(sourceFolderPath);

                if (sourceFolder.GetFileSystemInfos().Length == 0)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogWarning("MSAlign results folder is empty; nothing to zip: " + sourceFolderPath);
                    }
                    return false;
                }

                if (mDebugLevel >= 1)
                {
                    var logMessage = "Zipping " + folderName.ToUpper() + " folder at " + sourceFolderPath;

                    if (mDebugLevel >= 2)
                    {
                        logMessage += ": " + zipFilePath;
                    }
                    LogMessage(logMessage);
                }

                mZipTools.ZipDirectory(sourceFolderPath, zipFilePath);
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
    }
}
