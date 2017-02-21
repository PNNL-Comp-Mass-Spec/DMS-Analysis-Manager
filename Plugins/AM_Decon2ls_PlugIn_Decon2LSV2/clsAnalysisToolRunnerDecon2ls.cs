//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 09/14/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    public class clsAnalysisToolRunnerDecon2ls : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        private const string DECON2LS_SCANS_FILE_SUFFIX = "_scans.csv";
        private const string DECON2LS_ISOS_FILE_SUFFIX = "_isos.csv";
        private const string DECON2LS_PEAKS_FILE_SUFFIX = "_peaks.txt";

        #endregion

        #region "Module variables"

        private clsAnalysisResources.eRawDataTypeConstants mRawDataType = clsAnalysisResources.eRawDataTypeConstants.Unknown;
        private string mRawDataTypeName = string.Empty;

        private string mInputFilePath = string.Empty;

        private int mDeconConsoleBuild;

        private bool mDeconToolsExceptionThrown;
        private bool mDeconToolsFinishedDespiteProgRunnerError;

        private udtDeconToolsStatusType mDeconToolsStatus;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Enums and Structures"

        private enum DeconToolsStateType
        {
            Idle = 0,
            Running = 1,
            Complete = 2,
            ErrorCode = 3,
            BadErrorLogFile = 4
        }

        private enum DeconToolsFileTypeConstants
        {
            Undefined = 0,
            Agilent_WIFF = 1,
            Agilent_D = 2,
            Ascii = 3,
            Bruker = 4,
            Bruker_Ascii = 5,
            Finnigan = 6,
            ICR2LS_Rawdata = 7,
            Micromass_Rawdata = 8,
            MZXML_Rawdata = 9,
            PNNL_IMS = 10,
            PNNL_UIMF = 11,
            SUNEXTREL = 12
        }

        private struct udtDeconToolsStatusType
        {
            public int CurrentLCScan;       // LC Scan number or IMS Frame Number
            public float PercentComplete;
            public bool IsUIMF;

            public void Clear()
            {
                CurrentLCScan = 0;
                PercentComplete = 0;
                IsUIMF = false;
            }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        public clsAnalysisToolRunnerDecon2ls()
        {
        }

        /// <summary>
        /// Validate the result files
        /// (legacy code would assemble result files from looping, but that code has been removed)
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType AssembleResults(clsXMLParamFileReader oDeconToolsParamFileReader)
        {
            var blnDotDFolder = false;

            try
            {
                var ScansFilePath = Path.Combine(m_WorkDir, m_Dataset + DECON2LS_SCANS_FILE_SUFFIX);
                var IsosFilePath = Path.Combine(m_WorkDir, m_Dataset + DECON2LS_ISOS_FILE_SUFFIX);
                var PeaksFilePath = Path.Combine(m_WorkDir, m_Dataset + DECON2LS_PEAKS_FILE_SUFFIX);

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the Decon2LS output files are created inside the .D folder
                        // Still true as of 5/18/2012
                        blnDotDFolder = true;
                        break;
                    default:
                        if (!File.Exists(IsosFilePath) & !File.Exists(ScansFilePath))
                        {
                            if (mInputFilePath.ToLower().EndsWith(".d"))
                            {
                                blnDotDFolder = true;
                            }
                        }
                        break;
                }

                if (blnDotDFolder && !File.Exists(IsosFilePath) && !File.Exists(ScansFilePath))
                {
                    // Copy the files from the .D folder to the work directory
                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Copying Decon2LS result files from the .D folder to the working directory");
                    }

                    var fiSrcFilePath = new FileInfo(Path.Combine(mInputFilePath, m_Dataset + DECON2LS_SCANS_FILE_SUFFIX));
                    if (fiSrcFilePath.Exists)
                    {
                        fiSrcFilePath.CopyTo(ScansFilePath);
                    }

                    fiSrcFilePath = new FileInfo(Path.Combine(mInputFilePath, m_Dataset + DECON2LS_ISOS_FILE_SUFFIX));
                    if (fiSrcFilePath.Exists)
                    {
                        fiSrcFilePath.CopyTo(IsosFilePath);
                    }

                    fiSrcFilePath = new FileInfo(Path.Combine(mInputFilePath, m_Dataset + DECON2LS_PEAKS_FILE_SUFFIX));
                    if (fiSrcFilePath.Exists)
                    {
                        fiSrcFilePath.CopyTo(PeaksFilePath);
                    }
                }

                m_jobParams.AddResultFileToKeep(ScansFilePath);
                m_jobParams.AddResultFileToKeep(IsosFilePath);

                var blnWritePeaksToTextFile = oDeconToolsParamFileReader.GetParameter("WritePeaksToTextFile", false);

                // Examine the Peaks File to check whether it only has a header line, or it has multiple data lines
                if (!ResultsFileHasData(PeaksFilePath))
                {
                    // The file does not have any data lines
                    // Raise an error if it should have had data
                    if (blnWritePeaksToTextFile)
                    {
                        m_EvalMessage = "Warning: no results in DeconTools Peaks.txt file";
                        LogWarning(m_EvalMessage);
                    }
                    else
                    {
                        // Superfluous file; delete it
                        try
                        {
                            if (File.Exists(PeaksFilePath))
                            {
                                File.Delete(PeaksFilePath);
                            }
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }
                    }
                }

                var strDeconvolutionType = oDeconToolsParamFileReader.GetParameter("DeconvolutionType", string.Empty);
                var blnEmptyIsosFileExpected = strDeconvolutionType == "None";

                if (blnEmptyIsosFileExpected)
                {
                    // The _isos.csv file should be empty; delete it
                    if (!ResultsFileHasData(IsosFilePath))
                    {
                        // The file does not have any data lines
                        try
                        {
                            if (File.Exists(IsosFilePath))
                            {
                                File.Delete(IsosFilePath);
                            }
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }
                    }
                }
                else
                {
                    // Make sure the Isos File exists
                    if (!File.Exists(IsosFilePath))
                    {
                        var msg = "DeconTools Isos file Not Found";
                        LogError(msg, msg + ": " + IsosFilePath);
                        return CloseOutType.CLOSEOUT_NO_OUT_FILES;
                    }

                    // Make sure the Isos file contains at least one row of data
                    if (!IsosFileHasData(IsosFilePath))
                    {
                        LogError("No results in DeconTools Isos file");
                        return CloseOutType.CLOSEOUT_NO_DATA;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("AssembleResults error", "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step") + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private clsXMLParamFileReader CacheDeconToolsParamFile(string strParamFilePath)
        {
            clsXMLParamFileReader oDeconToolsParamFileReader;

            try
            {
                oDeconToolsParamFileReader = new clsXMLParamFileReader(strParamFilePath);

                if (oDeconToolsParamFileReader.ParameterCount == 0)
                {
                    LogError("DeconTools parameter file is empty (or could not be parsed)");
                    return null;
                }
            }
            catch (Exception ex)
            {
                LogError("Error parsing parameter file", ex);
                return null;
            }

            return oDeconToolsParamFileReader;
        }

        /// <summary>
        /// Use MSFileInfoScanner to create QC Plots
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType CreateQCPlots()
        {
            bool blnSuccess;

            try
            {
                var strInputFilePath = Path.Combine(m_WorkDir, m_Dataset + DECON2LS_ISOS_FILE_SUFFIX);
                if (!File.Exists(strInputFilePath))
                {
                    // Do not treat this as a fatal error
                    // It's possible that this analysis job used a parameter file that only picks peaks but doesn't deisotope, e.g. PeakPicking_NonThresholded_PeakBR2_SN7.xml
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir");
                if (string.IsNullOrEmpty(strMSFileInfoScannerDir))
                {
                    var msg = "Manager parameter 'MSFileInfoScannerDir' is not defined";
                    LogError("Error in CreateQCPlots: " + msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll");
                if (!File.Exists(strMSFileInfoScannerDLLPath))
                {
                    var msg = "File Not Found: " + strMSFileInfoScannerDLLPath;
                    LogError("Error in CreateQCPlots: " + msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var objQCPlotGenerator = new clsDeconToolsQCPlotsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel);
                RegisterEvents(objQCPlotGenerator);

                // Create the QC Plot .png files and associated Index.html file
                blnSuccess = objQCPlotGenerator.CreateQCPlots(strInputFilePath, m_WorkDir);

                if (blnSuccess)
                {
                    // Make sure the key png files were created
                    var expectedFileExtensions = new List<string> {
                        "_BPI_MS.png|_BPI_MSn.png",
                        "_HighAbu_LCMS.png|_HighAbu_LCMS_MSn.png",
                        "_LCMS.png|_LCMS_MSn.png",
                        "_TIC.png"
                    };

                    foreach (var fileExtension in expectedFileExtensions)
                    {
                        var filesToFind = new List<FileInfo>();
                        string fileDescription;
                        if (fileExtension.Contains("|"))
                        {
                            fileDescription = "";

                            // fileExtension contains a list of files
                            // Require that at least once of them exists
                            foreach (var extension in fileExtension.Split('|'))
                            {
                                filesToFind.Add(new FileInfo(Path.Combine(m_WorkDir, m_Dataset + extension)));
                                if (fileDescription.Length == 0)
                                    fileDescription = extension;
                                else
                                    fileDescription += " or " + extension;
                            }

                        } else
                        {
                            filesToFind.Add(new FileInfo(Path.Combine(m_WorkDir, m_Dataset + fileExtension)));
                            fileDescription = fileExtension;
                        }

                        var filesFound = 0;
                        foreach (var pngFile in filesToFind)
                        {
                            if (pngFile.Exists)
                                filesFound++;
                        }

                        if (filesFound == 0)
                        {
                            LogError("QC png file not found, extension " + fileDescription);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }                   

                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("Generated QC Plots file using " + strInputFilePath);
                    }
                }
                else
                {
                    LogError("Error generating QC Plots files with clsDeconToolsQCPlotsGenerator");
                    LogMessage(objQCPlotGenerator.ErrorMessage, 0, true);
                    
                    if (objQCPlotGenerator.MSFileInfoScannerErrorCount > 0)
                    {
                        LogWarning("MSFileInfoScanner encountered " + objQCPlotGenerator.MSFileInfoScannerErrorCount + " errors");
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in CreateQCPlots", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return (CloseOutType.CLOSEOUT_FAILED);
        }

        /// <summary>
        /// Examines IsosFilePath to look for data lines (does not read the entire file, just the first two lines)
        /// </summary>
        /// <param name="IsosFilePath"></param>
        /// <returns>True if it has one or more lines of data, otherwise, returns False</returns>
        /// <remarks></remarks>
        private bool IsosFileHasData(string IsosFilePath)
        {
            int intDataLineCount;
            return IsosFileHasData(IsosFilePath, out intDataLineCount, false);
        }

        /// <summary>
        /// Examines IsosFilePath to look for data lines
        /// </summary>
        /// <param name="IsosFilePath"></param>
        /// <param name="intDataLineCount">Output parameter: total data line count</param>
        /// <param name="blnCountTotalDataLines">True to count all of the data lines; false to just look for the first data line</param>
        /// <returns>True if it has one or more lines of data, otherwise, returns False</returns>
        /// <remarks></remarks>
        private bool IsosFileHasData(string IsosFilePath, out int intDataLineCount, bool blnCountTotalDataLines)
        {
            intDataLineCount = 0;
            var blnHeaderLineProcessed = false;

            try
            {
                if (File.Exists(IsosFilePath))
                {
                    var srInFile = new StreamReader(new FileStream(IsosFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                            continue;

                        if (blnHeaderLineProcessed)
                        {
                            // This is a data line
                            if (blnCountTotalDataLines)
                            {
                                intDataLineCount += 1;
                            }
                            else
                            {
                                intDataLineCount = 1;
                                break;
                            }
                        }
                        else
                        {
                            blnHeaderLineProcessed = true;
                        }
                    }

                    srInFile.Close();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            if (intDataLineCount > 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string errorMessage;

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerDecon2ls.RunTool(): Enter");
            }

            mRawDataTypeName = clsAnalysisResources.GetRawDataTypeName(m_jobParams, out errorMessage);

            if (string.IsNullOrWhiteSpace(mRawDataTypeName))
            {
                if (string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogError("Unable to determine the instrument data type using GetRawDataTypeName");
                }
                else
                {
                    LogError(errorMessage);
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            mRawDataType = clsAnalysisResources.GetRawDataType(mRawDataTypeName);

            // Set this to success for now
            var eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            // Run Decon2LS
            var eResult = RunDecon2Ls();
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the eResult folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error running Decon2LS";
                }

                if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    eReturnCode = eResult;
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        m_message = "No results in DeconTools Isos file";
                    }
                }
                else
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Create the QC plots
                eReturnCode = CreateQCPlots();
            }

            if (m_jobParams.GetJobParameter(clsAnalysisResourcesDecon2ls.JOB_PARAM_PROCESSMSMS_AUTO_ENABLED, false))
            {
                m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, "Note: auto-enabled ProcessMSMS in the parameter file");
                LogMessage(m_EvalMessage);
            }

            // Zip the _Peaks.txt file (if it exists)
            ZipPeaksFile();

            // Delete the raw data files
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file");
            }

            var messageSaved = string.Copy(m_message);

            if (DeleteRawDataFiles(mRawDataType) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                LogMessage("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " + m_message, 0, true);

                // Don't treat this as a critical error; leave eReturnCode unchanged and restore m_message
                if (!clsGlobal.IsMatch(m_message, messageSaved))
                {
                    m_message = messageSaved;
                }
            }

            // Update the job summary file
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file");
            }
            UpdateSummaryFile();

            // Make the results folder
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder");
            }

            eResult = MakeResultsFolder();
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            eResult = MoveResultFiles();
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // MoveResultFiles moves the eResult files to the eResult folder
                m_message = "Error moving files into results folder";
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            if (eReturnCode == CloseOutType.CLOSEOUT_FAILED)
            {
                // Try to save whatever files were moved into the results folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            eResult = CopyResultsFolderToServer();
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return eResult;
            }

            // If we get to here, return the return code
            return eReturnCode;
        }

        private CloseOutType RunDecon2Ls()
        {
            var strParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"));
            var blnDecon2LSError = false;

            // Cache the parameters in the DeconTools parameter file

            var oDeconToolsParamFileReader = CacheDeconToolsParamFile(strParamFilePath);

            if (oDeconToolsParamFileReader == null)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get file type of the raw data file
            var filetype = GetInputFileType(mRawDataType);

            if (filetype == DeconToolsFileTypeConstants.Undefined)
            {
                LogError("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while getting file type: " + mRawDataType);
                m_message = "Invalid raw data type specified";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Specify Input file or folder
            mInputFilePath = GetInputFilePath(mRawDataType);
            if (string.IsNullOrWhiteSpace(mInputFilePath))
            {
                LogError("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while input file name: " + mRawDataType);
                m_message = "Invalid raw data type specified";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Determine the path to the DeconTools folder
            var progLoc = DetermineProgramLocation("DeconTools", "DeconToolsProgLoc", "DeconConsole.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the DeconTools version info in the database
            m_message = string.Empty;
            if (!StoreToolVersionInfo(progLoc))
            {
                LogMessage("Aborting since StoreToolVersionInfo returned false", 0, true);
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error determining DeconTools version";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Reset the log file tracking variables
            mDeconToolsExceptionThrown = false;
            mDeconToolsFinishedDespiteProgRunnerError = false;

            // Reset the state variables
            mDeconToolsStatus.Clear();

            if (filetype == DeconToolsFileTypeConstants.PNNL_UIMF)
            {
                mDeconToolsStatus.IsUIMF = true;
            }
            else
            {
                mDeconToolsStatus.IsUIMF = false;
            }

            // Start Decon2LS and wait for it to finish
            var eDeconToolsStatus = StartDeconTools(progLoc, mInputFilePath, strParamFilePath, filetype);

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Make sure objects are released
            Thread.Sleep(1000);           //1 second delay
            PRISM.clsProgRunner.GarbageCollectNow();

            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished");
            }

            // Determine reason for Decon2LS finish
            if (mDeconToolsFinishedDespiteProgRunnerError & !mDeconToolsExceptionThrown)
            {
                // ProgRunner reported an error code
                // However, the log file says things completed successfully
                // We'll trust the log file
                blnDecon2LSError = false;
            }
            else
            {
                switch (eDeconToolsStatus)
                {
                    case DeconToolsStateType.Complete:
                        // This is normal, do nothing else
                        blnDecon2LSError = false;

                        break;
                    case DeconToolsStateType.ErrorCode:
                        m_message = "Decon2LS error";
                        blnDecon2LSError = true;

                        break;
                    case DeconToolsStateType.BadErrorLogFile:
                        blnDecon2LSError = true;

                        // Sleep for 1 minute
                        LogDebug("Sleeping for 1 minute");
                        Thread.Sleep(60 * 1000);

                        break;
                    case DeconToolsStateType.Idle:
                        // DeconTools never actually started
                        m_message = "Decon2LS error";
                        blnDecon2LSError = true;

                        break;
                    case DeconToolsStateType.Running:
                        // We probably shouldn't get here
                        // But, we'll assume success
                        blnDecon2LSError = false;
                        break;
                }
            }

            if (!blnDecon2LSError)
            {
                var eResult = AssembleResults(oDeconToolsParamFileReader);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Check for no data first. If no data, then exit but still copy results to server
                    if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        return eResult;
                    }

                    LogError("AssembleResults returned " + eResult.ToString());
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (blnDecon2LSError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DeconToolsStateType StartDeconTools(string ProgLoc, string strInputFilePath, string strParamFilePath, DeconToolsFileTypeConstants eFileType)
        {
            DeconToolsStateType eDeconToolsStatus;

            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsDeIsotope.StartDeconTools(), Starting deconvolution");
            }

            try
            {
                string CmdStr;

                if (eFileType == DeconToolsFileTypeConstants.Undefined)
                {
                    LogError("Undefined file type found in StartDeconTools");
                    return DeconToolsStateType.ErrorCode;
                }

                var strFileTypeText = GetDeconFileTypeText(eFileType);

                // Set up and execute a program runner to run DeconTools
                if (mDeconConsoleBuild < 4400)
                {
                    CmdStr = strInputFilePath + " " + strFileTypeText + " " + strParamFilePath;
                }
                else
                {
                    CmdStr = strInputFilePath + " " + strParamFilePath;
                }

                if (m_DebugLevel >= 1)
                {
                    LogDebug(ProgLoc + " " + CmdStr);
                }

                mCmdRunner = new clsRunDosProgram(m_WorkDir);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = true;
                mCmdRunner.EchoOutputToConsole = true;

                // We don't need to capture the console output since the DeconTools log file has very similar information
                mCmdRunner.WriteConsoleOutputToFile = false;

                eDeconToolsStatus = DeconToolsStateType.Running;

                m_progress = 0;
                ResetProgRunnerCpuUsage();

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var success = mCmdRunner.RunProgram(ProgLoc, CmdStr, "DeconConsole", true);

                if (!success)
                {
                    m_message = "Error running DeconTools";
                    LogMessage(m_message + ", job " + m_JobNum, 0, true);
                }

                // Parse the DeconTools .Log file to see whether it contains message "Finished file processing"
                var dtFinishTime = DateTime.Now;
                bool blnFinishedProcessing;

                ParseDeconToolsLogFile(out blnFinishedProcessing, ref dtFinishTime);

                if (mDeconToolsExceptionThrown)
                {
                    eDeconToolsStatus = DeconToolsStateType.ErrorCode;
                }
                else if (success)
                {
                    eDeconToolsStatus = DeconToolsStateType.Complete;
                }
                else if (blnFinishedProcessing)
                {
                    mDeconToolsFinishedDespiteProgRunnerError = true;
                    eDeconToolsStatus = DeconToolsStateType.Complete;
                }
                else
                {
                    eDeconToolsStatus = DeconToolsStateType.ErrorCode;
                }

                // Look for file Dataset*BAD_ERROR_log.txt
                // If it exists, an exception occurred
                var diWorkdir = new DirectoryInfo(Path.Combine(m_WorkDir));

                foreach (var fiFile in diWorkdir.GetFiles(m_Dataset + "*BAD_ERROR_log.txt"))
                {
                    m_message = "Error running DeconTools; Bad_Error_log file exists";
                    LogMessage(m_message + ": " + fiFile.Name, 0, true);
                    eDeconToolsStatus = DeconToolsStateType.BadErrorLogFile;
                    break;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception calling DeconConsole", ex);
                eDeconToolsStatus = DeconToolsStateType.ErrorCode;
            }

            return eDeconToolsStatus;
        }

        private string GetDeconFileTypeText(DeconToolsFileTypeConstants eDeconFileType)
        {
            switch (eDeconFileType)
            {
                case DeconToolsFileTypeConstants.Agilent_WIFF:
                    return "Agilent_WIFF";
                case DeconToolsFileTypeConstants.Agilent_D:
                    return "Agilent_D";
                case DeconToolsFileTypeConstants.Ascii:
                    return "Ascii";
                case DeconToolsFileTypeConstants.Bruker:
                    return "Bruker";
                case DeconToolsFileTypeConstants.Bruker_Ascii:
                    return "Bruker_Ascii";
                case DeconToolsFileTypeConstants.Finnigan:
                    return "Finnigan";
                case DeconToolsFileTypeConstants.ICR2LS_Rawdata:
                    return "ICR2LS_Rawdata";
                case DeconToolsFileTypeConstants.Micromass_Rawdata:
                    return "Micromass_Rawdata";
                case DeconToolsFileTypeConstants.MZXML_Rawdata:
                    return "MZXML_Rawdata";
                // Future: Case DeconToolsFileTypeConstants.MZML_Rawdata : Return "MZML_Rawdata"
                case DeconToolsFileTypeConstants.PNNL_IMS:
                    return "PNNL_IMS";
                case DeconToolsFileTypeConstants.PNNL_UIMF:
                    return "PNNL_UIMF";
                case DeconToolsFileTypeConstants.SUNEXTREL:
                    return "SUNEXTREL";
                default:
                    return "Undefined";
            }
        }

        private DeconToolsFileTypeConstants GetInputFileType(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            var InstrumentClass = m_jobParams.GetParam("instClass");

            // Gets the Decon2LS file type based on the input data type
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:

                    return DeconToolsFileTypeConstants.Finnigan;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:

                    return DeconToolsFileTypeConstants.Agilent_WIFF;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:

                    return DeconToolsFileTypeConstants.PNNL_UIMF;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:

                    return DeconToolsFileTypeConstants.Agilent_D;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:

                    return DeconToolsFileTypeConstants.Micromass_Rawdata;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:
                    if (InstrumentClass.ToLower() == "brukerftms")
                    {
                        // Data from Bruker FTICR
                        return DeconToolsFileTypeConstants.Bruker;
                    }

                    if (InstrumentClass.ToLower() == "finnigan_fticr")
                    {
                        // Data from old Finnigan FTICR
                        return DeconToolsFileTypeConstants.SUNEXTREL;
                    }
                    
                    // Should never get here
                    return DeconToolsFileTypeConstants.Undefined;

                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:

                    return DeconToolsFileTypeConstants.Bruker;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:

                    // TODO: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.Bruker_15T

                    LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" + eRawDataType.ToString() + ")");

                    return DeconToolsFileTypeConstants.Undefined;

                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:

                    // TODO: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.Bruker_15T

                    LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" + eRawDataType.ToString() + ")");

                    return DeconToolsFileTypeConstants.Undefined;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:

                    return DeconToolsFileTypeConstants.MZXML_Rawdata;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    // TODO: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.MZML_Rawdata

                    LogError("Decon2LS_V2 does not yet support mzML data");

                    return DeconToolsFileTypeConstants.Undefined;
                default:
                    // Should never get this value
                    return DeconToolsFileTypeConstants.Undefined;
            }
        }

        private void ParseDeconToolsLogFile(out bool blnFinishedProcessing, ref DateTime dtFinishTime)
        {
            var strScanFrameLine = string.Empty;

            blnFinishedProcessing = false;

            try
            {
                string strLogFilePath;

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the _Log.txt file is created inside the .D folder
                        strLogFilePath = Path.Combine(mInputFilePath, m_Dataset) + "_log.txt";
                        break;
                    default:
                        strLogFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) + "_log.txt");
                        break;
                }

                if (File.Exists(strLogFilePath))
                {
                    using (var srInFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();

                            if (string.IsNullOrWhiteSpace(strLineIn))
                                continue;

                            var intCharIndex = strLineIn.ToLower().IndexOf("finished file processing", StringComparison.Ordinal);

                            if (intCharIndex >= 0)
                            {
                                var blnDateValid = false;
                                if (intCharIndex > 1)
                                {
                                    // Parse out the date from strLineIn
                                    if (DateTime.TryParse(strLineIn.Substring(0, intCharIndex).Trim(), out dtFinishTime))
                                    {
                                        blnDateValid = true;
                                    }
                                    else
                                    {
                                        // Unable to parse out the date
                                        LogMessage("Unable to parse date from string '" + 
                                            strLineIn.Substring(0, intCharIndex).Trim() + "'; " + 
                                            "will use file modification date as the processing finish time", 0, true);
                                    }
                                }

                                if (!blnDateValid)
                                {
                                    var fiFileInfo = new FileInfo(strLogFilePath);
                                    dtFinishTime = fiFileInfo.LastWriteTime;
                                }

                                if (m_DebugLevel >= 3)
                                {
                                    LogDebug("DeconTools log file reports 'finished file processing' at " + 
                                        dtFinishTime.ToString(DATE_TIME_FORMAT));
                                }

                                blnFinishedProcessing = true;
                                break;
                            }

                            intCharIndex = strLineIn.ToLower().IndexOf("scan/frame", StringComparison.Ordinal);
                            if (intCharIndex >= 0)
                            {
                                strScanFrameLine = strLineIn.Substring(intCharIndex);
                            }

                            intCharIndex = strLineIn.IndexOf("ERROR THROWN", StringComparison.Ordinal);
                            if (intCharIndex > 0)
                            {
                                // An exception was reported in the log file; treat this as a fatal error
                                m_message = "Error thrown by DeconTools";

                                LogMessage("DeconTools reports " + strLineIn.Substring(intCharIndex), 0, true);
                                mDeconToolsExceptionThrown = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 4)
                {
                    LogWarning("Exception in ParseDeconToolsLogFile: " + ex.Message);
                }
            }

            if (!string.IsNullOrWhiteSpace(strScanFrameLine))
            {
                // Parse strScanFrameLine
                // It will look like:
                // Scan/Frame= 347; PercentComplete= 2.7; AccumlatedFeatures= 614

                var strProgressStats = strScanFrameLine.Split(';');

                for (var i = 0; i <= strProgressStats.Length - 1; i++)
                {
                    var kvStat = ParseKeyValue(strProgressStats[i]);
                    if (!string.IsNullOrWhiteSpace(kvStat.Key))
                    {
                        switch (kvStat.Key)
                        {
                            case "Scan/Frame":
                                int.TryParse(kvStat.Value, out mDeconToolsStatus.CurrentLCScan);
                                break;
                            case "PercentComplete":
                                float.TryParse(kvStat.Value, out mDeconToolsStatus.PercentComplete);
                                break;
                            case "AccumlatedFeatures":

                                break;
                        }
                    }
                }

                m_progress = mDeconToolsStatus.PercentComplete;
            }
        }

        /// <summary>
        /// Looks for an equals sign in strData
        /// Returns a KeyValuePair object with the text before the equals sign and the text after the equals sign
        /// </summary>
        /// <param name="strData"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private KeyValuePair<string, string> ParseKeyValue(string strData)
        {
            var intCharIndex = strData.IndexOf('=');

            if (intCharIndex > 0)
            {
                try
                {
                    return new KeyValuePair<string, string>(strData.Substring(0, intCharIndex).Trim(), strData.Substring(intCharIndex + 1).Trim());
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }

            return new KeyValuePair<string, string>(string.Empty, string.Empty);
        }

        /// <summary>
        /// Opens the specified results file from DeconTools and looks for at least two non-blank lines
        /// </summary>
        /// <param name="strFilePath"></param>
        /// <returns>True if two or more non-blank lines; otherwise false</returns>
        /// <remarks></remarks>
        private bool ResultsFileHasData(string strFilePath)
        {
            if (!File.Exists(strFilePath))
            {
                LogMessage("DeconTools results file not found: " + strFilePath);
                return false;
            }

            var intDataLineCount = 0;

            // Open the DeconTools results file
            // The first line is the header lines
            // Lines after that are data lines

            LogDebug("Opening the DeconTools results file: " + strFilePath);

            using (var srReader = new StreamReader(new FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srReader.EndOfStream && intDataLineCount < 2)
                {
                    var strLineIn = srReader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(strLineIn))
                    {
                        intDataLineCount += 1;
                    }
                }
            }

            if (intDataLineCount >= 2)
            {
                LogDebug("DeconTools results file has at least two non-blank lines");
                return true;
            }

            LogDebug("DeconTools results file is empty");
            return false;
        }

        public string GetInputFilePath(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            return GetInputFilePath(m_WorkDir, m_Dataset, eRawDataType);
        }

        /// <summary>
        /// assembles a string telling Decon2LS the name of the input file or folder
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="eRawDataType"></param>
        /// <param name="workDirPath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string GetInputFilePath(string workDirPath, string datasetName, clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:

                    return Path.Combine(workDirPath, datasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:

                    return Path.Combine(workDirPath, datasetName + clsAnalysisResources.DOT_WIFF_EXTENSION);
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:

                    return Path.Combine(workDirPath, datasetName + clsAnalysisResources.DOT_UIMF_EXTENSION);
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:

                    return Path.Combine(workDirPath, datasetName) + clsAnalysisResources.DOT_D_EXTENSION;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:

                    return Path.Combine(workDirPath, datasetName) + clsAnalysisResources.DOT_RAW_EXTENSION + "/_FUNC001.DAT";
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:

                    return Path.Combine(workDirPath, datasetName);
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    // Bruker_FT folders are actually .D folders

                    return Path.Combine(workDirPath, datasetName) + clsAnalysisResources.DOT_D_EXTENSION;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                    // Bruker_TOFBaf folders are actually .D folders

                    return Path.Combine(workDirPath, datasetName) + clsAnalysisResources.DOT_D_EXTENSION;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:
                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    return Path.Combine(workDirPath, datasetName);
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:
                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    return Path.Combine(workDirPath, datasetName);
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:

                    return Path.Combine(workDirPath, datasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);
                case clsAnalysisResources.eRawDataTypeConstants.mzML:

                    return Path.Combine(workDirPath, datasetName + clsAnalysisResources.DOT_MZML_EXTENSION);
                default:
                    // Should never get this value
                    return string.Empty;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strDeconToolsProgLoc)
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioDeconToolsInfo = new FileInfo(strDeconToolsProgLoc);
            if (!ioDeconToolsInfo.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogMessage("Exception calling SetStepTaskToolVersion: " + ex.Message, 0, true);
                    return false;
                }
            }

            // Lookup the version of the DeconConsole application
            var blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, ioDeconToolsInfo.FullName);
            if (!blnSuccess)
                return false;

            // Parse out the DeconConsole Build number using a RegEx
            // strToolVersionInfo should look like: DeconConsole, Version=1.0.4400.22961

            mDeconConsoleBuild = 0;
            var reParseVersion = new Regex(@"Version=\d+\.\d+\.(\d+)");
            var reMatch = reParseVersion.Match(strToolVersionInfo);
            if (reMatch.Success)
            {
                if (!int.TryParse(reMatch.Groups[1].Value, out mDeconConsoleBuild))
                {
                    // Error parsing out the version
                    m_message = "Error determining DeconConsole version, cannot convert build to integer";
                    LogMessage(m_message + ": " + strToolVersionInfo, 0, true);
                    return false;
                }
            }
            else
            {
                m_message = "Error determining DeconConsole version, RegEx did not match";
                LogMessage(m_message + ": " + strToolVersionInfo, 0, true);
                return false;
            }

            // Lookup the version of the DeconTools Backend (in the DeconTools folder)
            var strDeconToolsBackendPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconTools.Backend.dll");
            blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strDeconToolsBackendPath);
            if (!blnSuccess)
                return false;

            // Lookup the version of the UIMFLibrary (in the DeconTools folder)
            var strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "UIMFLibrary.dll");
            blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strDLLPath);
            if (!blnSuccess)
                return false;

            // Old: Lookup the version of DeconEngine (in the DeconTools folder)
            // Disabled July 31, 2014 because support for Rapid was removed from DeconTools.Backend.dll and thus DeconEngine.dll is no longer required
            // strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngine.dll")
            // blnSuccess = MyBase.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDLLPath)
            // If Not blnSuccess Then Return False

            // Old: Lookup the version of DeconEngineV2 (in the DeconTools folder)
            // Disabled May 20, 2016 because the C++ code that was in DeconEngineV2.dll has been ported to C# and is now part of DeconTools.Backend
            // See DeconTools.Backend\ProcessingTasks\Deconvoluters\HornDeconvolutor\ThrashV1\ThrashV1_Readme.txt
            //
            // strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngineV2.dll")
            // blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strDLLPath)
            // If Not blnSuccess Then Return False

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(strDeconToolsProgLoc),
                new FileInfo(strDeconToolsBackendPath)};

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void ZipPeaksFile()
        {
            try
            {
                var strPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset + DECON2LS_PEAKS_FILE_SUFFIX);
                var strZippedPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset + "_peaks.zip");

                if (File.Exists(strPeaksFilePath))
                {
                    if (!ZipFile(strPeaksFilePath, false, strZippedPeaksFilePath))
                    {
                        var msg = "Error zipping " + DECON2LS_PEAKS_FILE_SUFFIX + " file, job " + m_JobNum;
                        LogMessage(msg, 0, true);
                        m_message = clsGlobal.AppendToComment(m_message, "Error zipping Peaks.txt file");
                        return;
                    }

                    // Add the _peaks.txt file to .FilesToDelete since we only want to keep the Zipped version
                    m_jobParams.AddResultFileToSkip(Path.GetFileName(strPeaksFilePath));
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping Peaks.txt file", ex);
            }
        }

        #endregion

        private DateTime dtLastLogCheckTime = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the log file every 30 seconds to determine the % complete
            if (DateTime.UtcNow.Subtract(dtLastLogCheckTime).TotalSeconds < SECONDS_BETWEEN_UPDATE)
            {
                return;
            }

            dtLastLogCheckTime = DateTime.UtcNow;

            var dtFinishTime = DateTime.UtcNow;
            bool blnFinishedProcessing;

            ParseDeconToolsLogFile(out blnFinishedProcessing, ref dtFinishTime);

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            string strProgressMessage;

            if (mDeconToolsStatus.IsUIMF)
            {
                strProgressMessage = "Frame=" + mDeconToolsStatus.CurrentLCScan;
            }
            else
            {
                strProgressMessage = "Scan=" + mDeconToolsStatus.CurrentLCScan;
            }

            strProgressMessage = "DeconTools, " + strProgressMessage;

            int logIntervalMinutes;
            if (m_DebugLevel >= 5)
            {
                logIntervalMinutes = 1;
            }
            else if (m_DebugLevel >= 4)
            {
                logIntervalMinutes = 2;
            }
            else if (m_DebugLevel >= 3)
            {
                logIntervalMinutes = 5;
            }
            else if (m_DebugLevel >= 2)
            {
                logIntervalMinutes = 10;
            }
            else
            {
                logIntervalMinutes = 15;
            }

            LogProgress(strProgressMessage, logIntervalMinutes);

            const int MAX_LOGFINISHED_WAITTIME_SECONDS = 120;
            if (blnFinishedProcessing)
            {
                // The Decon2LS Log File reports that the task is complete
                // If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, then send an abort to the CmdRunner

                if (DateTime.Now.Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS)
                {
                    LogDebug("Note: Log file reports finished over " + MAX_LOGFINISHED_WAITTIME_SECONDS +
                             " seconds ago, but the DeconTools CmdRunner is still active");

                    mDeconToolsFinishedDespiteProgRunnerError = true;

                    // Abort processing
                    mCmdRunner.AbortProgramNow();

                    Thread.Sleep(3000);
                }
            }
        }
    }
}