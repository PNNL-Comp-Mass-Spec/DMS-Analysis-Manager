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
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMSAlignPlugIn
{
    /// <summary>
    /// Class for running MSAlign analysis
    /// </summary>
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
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string CmdStr = null;
            int intJavaMemorySize = 0;

            var blnProcessingError = false;

            CloseOutType eResult = CloseOutType.CLOSEOUT_SUCCESS;
            bool blnSuccess = false;

            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSAlign.RunTool(): Enter");
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
                mMSAlignProgLoc = DetermineProgramLocation("MSAlign", "MSAlignProgLoc", Path.Combine("jar", MSAlign_JAR_NAME));

                if (string.IsNullOrWhiteSpace(mMSAlignProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var eMSalignVersion = eMSAlignVersionType.Unknown;
                if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.5" + Path.DirectorySeparatorChar))
                {
                    eMSalignVersion = eMSAlignVersionType.v0pt5;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.6."))
                {
                    eMSalignVersion = eMSAlignVersionType.v0pt6;
                }
                else if (mMSAlignProgLoc.Contains(Path.DirectorySeparatorChar + "v0.7."))
                {
                    eMSalignVersion = eMSAlignVersionType.v0pt7;
                }
                else
                {
                    // Assume v0.7
                    eMSalignVersion = eMSAlignVersionType.v0pt7;
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
                if (!CopyMSAlignProgramFiles(mMSAlignProgLoc, eMSalignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize the MSInput folder
                if (!InitializeMSInputFolder(mMSAlignWorkFolderPath, eMSalignVersion))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSAlign");

                // Lookup the amount of memory to reserve for Java; default to 2 GB
                intJavaMemorySize = m_jobParams.GetJobParameter("MSAlignJavaMemorySize", 2000);
                if (intJavaMemorySize < 512)
                    intJavaMemorySize = 512;

                //Set up and execute a program runner to run MSAlign
                if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                {
                    CmdStr = " -Xmx" + intJavaMemorySize.ToString() + "M -classpath jar\\malign.jar;jar\\* edu.ucsd.msalign.spec.web.Pipeline .\\";
                }
                else
                {
                    CmdStr = " -Xmx" + intJavaMemorySize.ToString() + "M -classpath jar\\*; edu.ucsd.msalign.align.console.MsAlignPipeline .\\";
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc + " " + CmdStr);

                mCmdRunner = new clsRunDosProgram(mMSAlignWorkFolderPath);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = false;
                mCmdRunner.EchoOutputToConsole = true;

                if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                {
                    mCmdRunner.WriteConsoleOutputToFile = false;
                }
                else
                {
                    mCmdRunner.WriteConsoleOutputToFile = true;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT);
                }

                m_progress = PROGRESS_PCT_STARTING;
                ResetProgRunnerCpuUsage();

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                blnSuccess = mCmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSAlign", true);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mMSAlignVersion))
                    {
                        ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
                }

                if (!blnSuccess)
                {
                    string Msg = null;
                    Msg = "Error running MSAlign";
                    m_message = clsGlobal.AppendToComment(m_message, Msg);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);

                    if (mCmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "MSAlign returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Call to MSAlign failed (but exit code is 0)");
                    }

                    blnProcessingError = true;
                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Make sure the output files were created
                    if (!ValidateAndCopyResultFiles(eMSalignVersion))
                    {
                        blnProcessingError = true;
                    }

                    string strResultTableFilePath = null;
                    strResultTableFilePath = Path.Combine(m_WorkDir, m_Dataset + RESULT_TABLE_NAME_SUFFIX);

                    if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                    {
                        // Add a header to the _ResultTable.txt file
                        AddResultTableHeaderLine(strResultTableFilePath);
                    }

                    // Make sure the _ResultTable.txt file is not empty
                    if (blnProcessingError)
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

                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSAlign Search Complete");
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                mCmdRunner = null;

                //Make sure objects are released
                Thread.Sleep(500);        // 500 msec delay
                PRISM.clsProgRunner.GarbageCollectNow();

                if (eMSalignVersion != eMSAlignVersionType.v0pt5)
                {
                    // Trim the console output file to remove the majority of the % finished messages
                    TrimConsoleOutputFile(Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT));
                }

                if (blnProcessingError)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging MSAlign problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in MSAlignPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return eResult;
        }

        protected bool AddResultTableHeaderLine(string strSourceFilePath)
        {
            try
            {
                if (!File.Exists(strSourceFilePath))
                    return false;

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Adding header line to MSAlign_ResultTable.txt file");
                }

                string strTargetFilePath = null;
                strTargetFilePath = strSourceFilePath + ".tmp";

                // Open the input file and
                // create the output file
                using (var srInFile = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    string strHeaderLine = null;
                    strHeaderLine = "Prsm_ID\t" + "Spectrum_ID\t" + "Protein_Sequence_ID\t" + "Spectrum_ID\t" + "Scan(s)\t" + "#peaks\t" + "Charge\t" +
                                    "Precursor_mass\t" + "Protein_name\t" + "Protein_mass\t" + "First_residue\t" + "Last_residue\t" + "Peptide\t" +
                                    "#unexpected_modifications\t" + "#matched_peaks\t" + "#matched_fragment_ions\t" + "E-value";

                    swOutFile.WriteLine(strHeaderLine);

                    while (!srInFile.EndOfStream)
                    {
                        swOutFile.WriteLine(srInFile.ReadLine());
                    }
                }

                Thread.Sleep(500);

                // Delete the source file, then rename the new file to match the source file
                File.Delete(strSourceFilePath);
                Thread.Sleep(500);

                File.Move(strTargetFilePath, strSourceFilePath);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in AddResultTableHeaderLine: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool CopyFastaCheckResidues(string strSourceFilePath, string strTargetFilePath)
        {
            const int RESIDUES_PER_LINE = 60;

            string strProteinResidues = null;

            int intIndex = 0;
            int intResidueCount = 0;
            int intLength = 0;
            var intWarningCount = 0;

            try
            {
                var reInvalidResidues = new Regex(@"[BJOUXZ]", RegexOptions.Compiled);

                var oReader = new ProteinFileReader.FastaFileReader();
                if (!oReader.OpenFile(strSourceFilePath))
                {
                    m_message = "Error opening fasta file in CopyFastaCheckResidues";
                    return false;
                }

                using (var swNewFasta = new StreamWriter(new FileStream(strTargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (oReader.ReadNextProteinEntry())
                    {
                        swNewFasta.WriteLine(oReader.ProteinLineStartChar + oReader.HeaderLine);
                        strProteinResidues = reInvalidResidues.Replace(oReader.ProteinSequence, "-");

                        if (intWarningCount < 5 && strProteinResidues.GetHashCode() != oReader.ProteinSequence.GetHashCode())
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "Changed invalid residues to '-' in protein " + oReader.ProteinName);
                            intWarningCount += 1;
                        }

                        intIndex = 0;
                        intResidueCount = strProteinResidues.Length;
                        while (intIndex < strProteinResidues.Length)
                        {
                            intLength = Math.Min(RESIDUES_PER_LINE, intResidueCount - intIndex);
                            swNewFasta.WriteLine(strProteinResidues.Substring(intIndex, intLength));
                            intIndex += RESIDUES_PER_LINE;
                        }
                    }
                }

                oReader.CloseFile();
            }
            catch (Exception ex)
            {
                m_message = "Exception in CopyFastaCheckResidues";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
                return false;
            }

            return true;
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the .mzXML file first)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + ".mzXML"));
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        private bool CopyMSAlignProgramFiles(string strMSAlignJarFilePath, eMSAlignVersionType eMSalignVersion)
        {
            try
            {
                var fiMSAlignJarFile = new FileInfo(strMSAlignJarFilePath);

                if (!fiMSAlignJarFile.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "MSAlign .Jar file not found: " + fiMSAlignJarFile.FullName);
                    return false;
                }

                // The source folder is one level up from the .Jar file
                var diMSAlignSrc = new DirectoryInfo(fiMSAlignJarFile.Directory.Parent.FullName);
                var diMSAlignWork = new DirectoryInfo(Path.Combine(m_WorkDir, "MSAlign"));

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying MSAlign program file to the Work Directory");

                // Make sure the folder doesn't already exit
                if (diMSAlignWork.Exists)
                {
                    diMSAlignWork.Delete(true);
                    Thread.Sleep(500);
                }

                // Create the folder
                diMSAlignWork.Create();
                mMSAlignWorkFolderPath = diMSAlignWork.FullName;

                // Create the subdirectories
                diMSAlignWork.CreateSubdirectory("html");
                diMSAlignWork.CreateSubdirectory("jar");
                diMSAlignWork.CreateSubdirectory("msinput");
                diMSAlignWork.CreateSubdirectory("msoutput");
                diMSAlignWork.CreateSubdirectory("xml");
                diMSAlignWork.CreateSubdirectory("xsl");
                if (eMSalignVersion != eMSAlignVersionType.v0pt5)
                {
                    diMSAlignWork.CreateSubdirectory("etc");
                }

                // Copy all files in the jar and xsl folders to the target
                List<string> lstSubfolderNames = new List<string>();
                lstSubfolderNames.Add("jar");
                lstSubfolderNames.Add("xsl");
                if (eMSalignVersion != eMSAlignVersionType.v0pt5)
                {
                    lstSubfolderNames.Add("etc");
                }

                foreach (string strSubFolder in lstSubfolderNames)
                {
                    var strTargetSubfolder = Path.Combine(diMSAlignWork.FullName, strSubFolder);

                    DirectoryInfo[] diSubfolder = null;
                    diSubfolder = diMSAlignSrc.GetDirectories(strSubFolder);

                    if (diSubfolder.Length == 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "Source MSAlign subfolder not found: " + strTargetSubfolder);
                        return false;
                    }

                    foreach (FileInfo ioFile in diSubfolder[0].GetFiles())
                    {
                        ioFile.CopyTo(Path.Combine(strTargetSubfolder, ioFile.Name));
                    }
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in CopyMSAlignProgramFiles: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool CreateInputPropertiesFile(string strParamFilePath, string strMSInputFolderPath, eMSAlignVersionType eMSalignVersion)
        {
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

            string strLineIn = null;

            int intEqualsIndex = 0;
            string strKeyName = null;
            string strValue = null;
            var blnEValueCutoffType = false;

            try
            {
                // Initialize the dictionary that maps new names to legacy names
                // Version 0.5 used the legacy names, e.g. it used "threshold" instead of "eValueThreshold"
                var dctLegacyKeyMap = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                dctLegacyKeyMap.Add("errorTolerance", "fragmentTolerance");
                dctLegacyKeyMap.Add("eValueThreshold", "threshold");
                dctLegacyKeyMap.Add("shiftNumber", "shifts");
                dctLegacyKeyMap.Add("cysteineProtection", "protection");
                dctLegacyKeyMap.Add("activation", "instrument");

                // Note: Starting with version 0.7, the "eValueThreshold" parameter was replaced with two new parameters:
                // cutoffType and cutoff

                string strOutputFilePath = Path.Combine(strMSInputFolderPath, "input.properties");

                // Open the input file and
                // Create the output file
                using (var srInFile = new StreamReader(new FileStream(strParamFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the database name and input file name
                    if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                    {
                        swOutFile.WriteLine(DB_FILENAME_LEGACY + "=" + mInputPropertyValues.FastaFileName);
                        // Input file name is assumed to be input_data
                    }
                    else
                    {
                        swOutFile.WriteLine(DB_FILENAME + "=" + mInputPropertyValues.FastaFileName);
                        swOutFile.WriteLine(SPEC_FILENAME + "=" + mInputPropertyValues.SpectrumFileName);
                    }

                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        if (strLineIn.TrimStart().StartsWith("#") || string.IsNullOrWhiteSpace(strLineIn))
                        {
                            // Comment line or blank line; write it out as-is
                            swOutFile.WriteLine(strLineIn);
                        }
                        else
                        {
                            // Look for an equals sign
                            intEqualsIndex = strLineIn.IndexOf('=');

                            if (intEqualsIndex > 0)
                            {
                                // Split the line on the equals sign
                                strKeyName = strLineIn.Substring(0, intEqualsIndex).TrimEnd();
                                if (intEqualsIndex < strLineIn.Length - 1)
                                {
                                    strValue = strLineIn.Substring(intEqualsIndex + 1).Trim();
                                }
                                else
                                {
                                    strValue = string.Empty;
                                }

                                if (strKeyName.ToLower() == INSTRUMENT_ACTIVATION_TYPE_KEY || strKeyName.ToLower() == INSTRUMENT_TYPE_KEY)
                                {
                                    // If this is a bruker dataset, then we need to make sure that the value for this entry is not FILE
                                    // The reason is that the mzXML file created by Bruker's compass program does not include the scantype information (CID, ETD, etc.)
                                    string strToolName = null;
                                    strToolName = m_jobParams.GetParam("ToolName");

                                    if (strToolName == "MSAlign_Bruker")
                                    {
                                        if (strValue.ToUpper() == "FILE")
                                        {
                                            m_message = "Must specify an explicit scan type for " + strKeyName +
                                                        " in the MSAlign parameter file (CID, HCD, or ETD)";

                                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; this is required because Bruker-created mzXML files do not include activationMethod information in the precursorMz tag");

                                            return false;
                                        }
                                    }
                                }

                                if (strKeyName.ToLower() == SEARCH_TYPE_KEY)
                                {
                                    if (strValue.ToUpper() == "TARGET+DECOY")
                                    {
                                        // Make sure the protein collection is not a Decoy protein collection
                                        string strProteinOptions = null;
                                        strProteinOptions = m_jobParams.GetParam("ProteinOptions");

                                        if (strProteinOptions.ToLower().Contains("seq_direction=decoy"))
                                        {
                                            m_message = "MSAlign parameter file contains searchType=TARGET+DECOY; protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy";

                                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);

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

                                        if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                                        {
                                            // Running a legacy version; rename the keys

                                            string strLegacyKeyName = string.Empty;

                                            if (dctLegacyKeyMap.TryGetValue(strKeyName, out strLegacyKeyName))
                                            {
                                                if (strLegacyKeyName == "protection")
                                                {
                                                    // Need to update the value
                                                    switch (strValue.ToUpper())
                                                    {
                                                        case "C57":
                                                            strValue = "Carbamidoemetylation";
                                                            break;
                                                        case "C58":
                                                            strValue = "Carboxymethylation";
                                                            break;
                                                        default:
                                                            // Includes "C0"
                                                            strValue = "None";
                                                            break;
                                                    }
                                                }
                                                else if (strLegacyKeyName == "instrument")
                                                {
                                                    if (strValue == "FILE")
                                                    {
                                                        // Legacy mode does not support "FILE"
                                                        // Auto-switch to CID and log a warning message
                                                        strValue = "CID";
                                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                                            "Using instrument mode 'CID' since v0.5 of MSAlign does not support reading the activation mode from the msalign file");
                                                    }
                                                }
                                                else if (strKeyName.ToLower() == CUTOFF_TYPE_KEY)
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
                                                        m_message = "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.5";
                                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                                                        return false;
                                                    }
                                                }

                                                swOutFile.WriteLine(strLegacyKeyName + "=" + strValue);
                                            }
                                        }
                                        else
                                        {
                                            if (eMSalignVersion >= eMSAlignVersionType.v0pt7 && strKeyName.ToLower() == "eValueThreshold")
                                            {
                                                // v0.7 and up use cutoffType and cutoff instead of eValueThreshold
                                                swOutFile.WriteLine("cutoffType=EVALUE");
                                                swOutFile.WriteLine("cutoff=" + strValue);
                                            }
                                            else if (eMSalignVersion == eMSAlignVersionType.v0pt6 && strKeyName.ToLower() == CUTOFF_TYPE_KEY)
                                            {
                                                if (strValue.ToUpper() == "EVALUE")
                                                {
                                                    blnEValueCutoffType = true;
                                                }
                                            }
                                            else if (eMSalignVersion == eMSAlignVersionType.v0pt6 && strKeyName.ToLower() == CUTOFF_KEY)
                                            {
                                                if (blnEValueCutoffType)
                                                {
                                                    // v0.6 doesn't support the cutoff parameter, just eValueThreshold
                                                    swOutFile.WriteLine("eValueThreshold=" + strValue);
                                                }
                                                else
                                                {
                                                    m_message = "MSAlign parameter file contains a non-EValue cutoff value; this is not compatible with MSAlign v0.6";
                                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                                                    return false;
                                                }
                                            }
                                            else
                                            {
                                                // Write out as-is
                                                swOutFile.WriteLine(strLineIn);
                                            }
                                        }
                                        break;
                                }
                            }
                            else
                            {
                                // Unknown line format; skip it
                            }
                        }
                    }

                    if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                    {
                        mInputPropertyValues.ResultTableFileName = RESULT_TABLE_NAME_LEGACY;
                        mInputPropertyValues.ResultDetailsFileName = RESULT_DETAILS_NAME_LEGACY;
                    }
                    else
                    {
                        mInputPropertyValues.ResultTableFileName = m_Dataset + RESULT_TABLE_NAME_SUFFIX;
                        mInputPropertyValues.ResultDetailsFileName = m_Dataset + RESULT_DETAILS_NAME_SUFFIX;
                    }

                    if (eMSalignVersion != eMSAlignVersionType.v0pt5)
                    {
                        swOutFile.WriteLine(TABLE_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultTableFileName);
                        swOutFile.WriteLine(DETAIL_OUTPUT_FILENAME + "=" + mInputPropertyValues.ResultDetailsFileName);
                    }
                }

                // Copy the newly created input.properties file to the work directory
                File.Copy(strOutputFilePath, Path.Combine(m_WorkDir, Path.GetFileName(strOutputFilePath)), true);
            }
            catch (Exception ex)
            {
                m_message = "Exception in CreateInputPropertiesFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in CreateInputPropertiesFile: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool InitializeMSInputFolder(string strMSAlignWorkFolderPath, eMSAlignVersionType eMSalignVersion)
        {
            string strMSInputFolderPath = null;
            FileInfo[] fiFiles = null;

            try
            {
                strMSInputFolderPath = Path.Combine(strMSAlignWorkFolderPath, "msinput");
                var fiSourceFolder = new DirectoryInfo(m_WorkDir);

                // Copy the .Fasta file into the MSInput folder
                // MSAlign will crash if any non-standard residues are present (BJOUXZ)
                // Thus, we will read the source file with a reader and create a new fasta file

                // Define the path to the fasta file
                string OrgDbDir = m_mgrParams.GetParam("orgdbdir");
                string strFASTAFilePath = Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

                var fiFastaFile = new FileInfo(strFASTAFilePath);

                if (!fiFastaFile.Exists)
                {
                    // Fasta file not found
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " + fiFastaFile.FullName);
                    return false;
                }

                mInputPropertyValues.FastaFileName = string.Copy(fiFastaFile.Name);

                if (!CopyFastaCheckResidues(fiFastaFile.FullName, Path.Combine(strMSInputFolderPath, mInputPropertyValues.FastaFileName)))
                {
                    if (string.IsNullOrEmpty(m_message))
                        m_message = "CopyFastaCheckResidues returned false";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }

                // Move the _msdeconv.msalign file to the MSInput folder
                fiFiles = fiSourceFolder.GetFiles("*" + clsAnalysisResourcesMSAlign.MSDECONV_MSALIGN_FILE_SUFFIX);
                if (fiFiles.Length == 0)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSAlign file not found in work directory");
                    return false;
                }
                else
                {
                    if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                    {
                        // Rename the file to input_data when we move it
                        mInputPropertyValues.SpectrumFileName = "input_data";
                    }
                    else
                    {
                        mInputPropertyValues.SpectrumFileName = string.Copy(fiFiles[0].Name);
                    }
                    fiFiles[0].MoveTo(Path.Combine(strMSInputFolderPath, mInputPropertyValues.SpectrumFileName));
                }

                string strParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));

                if (!CreateInputPropertiesFile(strParamFilePath, strMSInputFolderPath, eMSalignVersion))
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in InitializeMSInputFolder: " + ex.Message);
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
        private Regex reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                int intLinesRead = 0;

                short intActualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            if (intLinesRead <= 3)
                            {
                                // Originally the first line was the MSAlign version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the MSAlign version
                                if (string.IsNullOrEmpty(mMSAlignVersion) && strLineIn.ToLower().Contains("ms-align"))
                                {
                                    if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mMSAlignVersion))
                                    {
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                            "MSAlign version: " + strLineIn);
                                    }

                                    mMSAlignVersion = string.Copy(strLineIn);
                                }
                                else
                                {
                                    if (strLineIn.ToLower().Contains("error"))
                                    {
                                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                        {
                                            mConsoleOutputErrorMsg = "Error running MSAlign:";
                                        }
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }

                            // Update progress if the line starts with Processing spectrum
                            if (strLineIn.StartsWith("Processing spectrum"))
                            {
                                var oMatch = reExtractPercentFinished.Match(strLineIn);
                                if (oMatch.Success)
                                {
                                    short intProgress;
                                    if (short.TryParse(oMatch.Groups[1].Value, out intProgress))
                                    {
                                        intActualProgress = intProgress;
                                    }
                                }
                            }
                            else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                if (strLineIn.ToLower().StartsWith("error"))
                                {
                                    mConsoleOutputErrorMsg += "; " + strLineIn;
                                }
                            }
                        }
                    }
                }

                if (m_progress < intActualProgress)
                {
                    m_progress = intActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSAlignVersion);

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(mMSAlignProgLoc));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private Regex reExtractScan = new Regex(@"Processing spectrum scan (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Trimming console output file at " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                bool blnKeepLine = false;

                int intScanNumber = 0;
                string strMostRecentProgressLine = string.Empty;
                string strMostRecentProgressLineWritten = string.Empty;

                int intScanNumberOutputThreshold = 0;

                string strTrimmedFilePath = null;
                strTrimmedFilePath = strConsoleOutputFilePath + ".trimmed";

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    intScanNumberOutputThreshold = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        blnKeepLine = true;

                        var oMatch = reExtractScan.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                    strMostRecentProgressLineWritten = string.Copy(strLineIn);
                                }
                            }
                            strMostRecentProgressLine = string.Copy(strLineIn);
                        }
                        else if (strLineIn.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (string.Compare(strMostRecentProgressLine, strMostRecentProgressLineWritten) != 0)
                            {
                                swOutFile.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            swOutFile.WriteLine(strLineIn);
                        }
                    }
                }

                // Wait 500 msec, then swap the files
                Thread.Sleep(500);

                try
                {
                    File.Delete(strConsoleOutputFilePath);
                    File.Move(strTrimmedFilePath, strConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "Error replacing original console output file (" + strConsoleOutputFilePath + ") with trimmed version: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error trimming console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        protected bool ValidateAndCopyResultFiles(eMSAlignVersionType eMSalignVersion)
        {
            string strResultsFolderPath = Path.Combine(mMSAlignWorkFolderPath, "msoutput");
            var lstResultsFilesToMove = new List<string>();
            var blnProcessingError = false;

            try
            {
                lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultTableFileName));
                lstResultsFilesToMove.Add(Path.Combine(strResultsFolderPath, mInputPropertyValues.ResultDetailsFileName));

                foreach (string strResultFilePath in lstResultsFilesToMove)
                {
                    if (!File.Exists(strResultFilePath))
                    {
                        string Msg = null;
                        Msg = "MSAlign results file not found (" + Path.GetFileName(strResultFilePath) + ")";

                        if (!blnProcessingError)
                        {
                            // This is the first missing file; update the base-class comment
                            m_message = clsGlobal.AppendToComment(m_message, Msg);
                        }

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            Msg + " (" + strResultFilePath + ")" + ", job " + m_JobNum);
                        blnProcessingError = true;
                    }
                    else
                    {
                        // Copy the results file to the work directory
                        string strSourceFileName = Path.GetFileName(strResultFilePath);
                        string strTargetFileName = null;
                        strTargetFileName = string.Copy(strSourceFileName);

                        if (eMSalignVersion == eMSAlignVersionType.v0pt5)
                        {
                            // Rename the file when we copy it
                            switch (strSourceFileName)
                            {
                                case RESULT_TABLE_NAME_LEGACY:
                                    strTargetFileName = m_Dataset + RESULT_TABLE_NAME_SUFFIX;
                                    break;
                                case RESULT_DETAILS_NAME_LEGACY:
                                    strTargetFileName = m_Dataset + RESULT_DETAILS_NAME_SUFFIX;
                                    break;
                            }
                        }

                        File.Copy(strResultFilePath, Path.Combine(m_WorkDir, strTargetFileName), true);
                    }
                }

                // Zip the Html and XML folders
                ZipMSAlignResultFolder("html");
                ZipMSAlignResultFolder("XML");
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in ValidateAndCopyResultFiles: " + ex.Message);
                return false;
            }

            if (blnProcessingError)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        protected bool ValidateResultTableFile(string strSourceFilePath)
        {
            string strLineIn = null;

            try
            {
                bool blnValidFile = false;
                blnValidFile = false;

                if (!File.Exists(strSourceFilePath))
                {
                    if (m_DebugLevel >= 2)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "MSAlign_ResultTable.txt file not found: " + strSourceFilePath);
                    }
                    return false;
                }

                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Validating that the MSAlign_ResultTable.txt file is not empty");
                }

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            string[] strSplitLine = null;
                            strSplitLine = strLineIn.Split('\t');

                            if (strSplitLine.Length > 1)
                            {
                                // Look for an integer in the first or second column
                                // Version 0.5 and 0.6 had Prsm_ID in the first column
                                // Version 0.7 moved Prsm_ID to the second column
                                int intValue = 0;
                                if (int.TryParse(strSplitLine[1], out intValue) || int.TryParse(strSplitLine[0], out intValue))
                                {
                                    // Integer found; line is valid
                                    blnValidFile = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (!blnValidFile)
                {
                    string Msg = null;
                    Msg = "MSAlign_ResultTable.txt file is empty";
                    m_message = clsGlobal.AppendToComment(m_message, Msg);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);
                    return false;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in ValidateResultTableFile: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool ZipMSAlignResultFolder(string strFolderName)
        {
            string strTargetFilePath = null;
            string strSourceFolderPath = null;

            try
            {
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_MSAlign_Results_" + strFolderName.ToUpper() + ".zip");
                strSourceFolderPath = Path.Combine(mMSAlignWorkFolderPath, strFolderName);

                // Confirm that the folder has one or more files or subfolders
                DirectoryInfo diSourceFolder = new DirectoryInfo(strSourceFolderPath);
                if (diSourceFolder.GetFileSystemInfos().Length == 0)
                {
                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "MSAlign results folder is empty; nothing to zip: " + strSourceFolderPath);
                    }
                    return false;
                }

                if (m_DebugLevel >= 1)
                {
                    string strLogMessage = "Zipping " + strFolderName.ToUpper() + " folder at " + strSourceFolderPath;

                    if (m_DebugLevel >= 2)
                    {
                        strLogMessage += ": " + strTargetFilePath;
                    }
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strLogMessage);
                }

                var objZipper = new Ionic.Zip.ZipFile(strTargetFilePath);
                objZipper.AddDirectory(strSourceFolderPath);
                objZipper.Save();
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception in ZipMSAlignResultFolder: " + ex.Message);
                return false;
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSAlign_CONSOLE_OUTPUT));

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
