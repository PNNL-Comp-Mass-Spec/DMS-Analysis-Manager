//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 09/14/2006
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    /// <summary>
    /// Class for running DeconTools
    /// </summary>
    public class clsAnalysisToolRunnerDecon2ls : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        private const string DECON2LS_SCANS_FILE_SUFFIX = "_scans.csv";
        private const string DECON2LS_ISOS_FILE_SUFFIX = "_isos.csv";
        private const string DECON2LS_PEAKS_FILE_SUFFIX = "_peaks.txt";

        private const string MS_FILE_INFO_SCANNER_NO_ISOS_DATA = "No data found in the _isos.csv file";
        #endregion

        #region "Module variables"

        private clsAnalysisResources.eRawDataTypeConstants mRawDataType = clsAnalysisResources.eRawDataTypeConstants.Unknown;
        private string mRawDataTypeName = string.Empty;

        private string mInputFilePath = string.Empty;

        private int mDeconConsoleBuild;

        private bool mDeconToolsExceptionThrown;
        private bool mDeconToolsFinishedDespiteProgRunnerError;

        private bool mMSFileInfoScannerReportsEmptyIsosFile;

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
            Thermo_Raw = 6,
            ICR2LS_RawData = 7,
            Micromass_RawData = 8,
            MZXML_RawData = 9,
            PNNL_IMS = 10,
            PNNL_UIMF = 11,
            SUN_EXTREL = 12
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
        /// Validate the result files
        /// (legacy code would assemble result files from looping, but that code has been removed)
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType AssembleResults(clsXMLParamFileReader oDeconToolsParamFileReader)
        {
            var dotDFolder = false;

            try
            {
                var scansFilePath = Path.Combine(mWorkDir, mDatasetName + DECON2LS_SCANS_FILE_SUFFIX);
                var isosFilePath = Path.Combine(mWorkDir, mDatasetName + DECON2LS_ISOS_FILE_SUFFIX);
                var peaksFilePath = Path.Combine(mWorkDir, mDatasetName + DECON2LS_PEAKS_FILE_SUFFIX);

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the Decon2LS output files are created inside the .D folder
                        // Still true as of 5/18/2012
                        dotDFolder = true;
                        break;
                    default:
                        if (!File.Exists(isosFilePath) && !File.Exists(scansFilePath))
                        {
                            if (mInputFilePath.ToLower().EndsWith(".d"))
                            {
                                dotDFolder = true;
                            }
                        }
                        break;
                }

                if (dotDFolder && !File.Exists(isosFilePath) && !File.Exists(scansFilePath))
                {
                    // Copy the files from the .D folder to the work directory
                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Copying Decon2LS result files from the .D folder to the working directory");
                    }

                    var srcFilePath = new FileInfo(Path.Combine(mInputFilePath, mDatasetName + DECON2LS_SCANS_FILE_SUFFIX));
                    if (srcFilePath.Exists)
                    {
                        srcFilePath.CopyTo(scansFilePath);
                    }

                    srcFilePath = new FileInfo(Path.Combine(mInputFilePath, mDatasetName + DECON2LS_ISOS_FILE_SUFFIX));
                    if (srcFilePath.Exists)
                    {
                        srcFilePath.CopyTo(isosFilePath);
                    }

                    srcFilePath = new FileInfo(Path.Combine(mInputFilePath, mDatasetName + DECON2LS_PEAKS_FILE_SUFFIX));
                    if (srcFilePath.Exists)
                    {
                        srcFilePath.CopyTo(peaksFilePath);
                    }
                }

                mJobParams.AddResultFileToKeep(scansFilePath);
                mJobParams.AddResultFileToKeep(isosFilePath);

                var writePeaksToTextFile = oDeconToolsParamFileReader.GetParameter("WritePeaksToTextFile", false);

                // Examine the Peaks File to check whether it only has a header line, or it has multiple data lines
                if (!ResultsFileHasData(peaksFilePath, DECON2LS_PEAKS_FILE_SUFFIX))
                {
                    // The file does not have any data lines
                    // Raise an error if it should have had data
                    if (writePeaksToTextFile)
                    {
                        LogWarning("Warning: no results in DeconTools Peaks.txt file", true);
                    }
                    else
                    {
                        // Superfluous file; delete it
                        try
                        {
                            if (File.Exists(peaksFilePath))
                            {
                                File.Delete(peaksFilePath);
                            }
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }
                    }
                }

                var deconvolutionType = oDeconToolsParamFileReader.GetParameter("DeconvolutionType", string.Empty);
                var emptyIsosFileExpected = deconvolutionType == "None";

                if (emptyIsosFileExpected)
                {
                    // The _isos.csv file should be empty; delete it
                    if (!ResultsFileHasData(isosFilePath, DECON2LS_ISOS_FILE_SUFFIX))
                    {
                        // The file does not have any data lines
                        try
                        {
                            if (File.Exists(isosFilePath))
                            {
                                File.Delete(isosFilePath);
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
                    if (!File.Exists(isosFilePath))
                    {
                        var msg = "DeconTools Isos file Not Found";
                        LogError(msg, msg + ": " + isosFilePath);
                        return CloseOutType.CLOSEOUT_NO_OUT_FILES;
                    }

                    // Make sure the Isos file contains at least one row of data
                    // Using a loose isotopic fit value filter of 0.3
                    if (!IsosFileHasData(isosFilePath, 0.3))
                    {
                        LogError("No results in DeconTools Isos file");
                        return CloseOutType.CLOSEOUT_NO_DATA;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("AssembleResults error", "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " + mJob + ", step " + mJobParams.GetParam("Step") + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private clsXMLParamFileReader CacheDeconToolsParamFile(string paramFilePath)
        {
            clsXMLParamFileReader oDeconToolsParamFileReader;

            try
            {
                oDeconToolsParamFileReader = new clsXMLParamFileReader(paramFilePath);

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
            try
            {
                var isosFilePath = Path.Combine(mWorkDir, mDatasetName + DECON2LS_ISOS_FILE_SUFFIX);
                if (!File.Exists(isosFilePath))
                {
                    // Do not treat this as a fatal error
                    // It's possible that this analysis job used a parameter file that only picks peaks but doesn't deisotope,
                    //   e.g. PeakPicking_NonThresholded_PeakBR2_SN7.xml
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var isosDataLineCount = -1;

                var msFileInfoScannerDir = mMgrParams.GetParam("MSFileInfoScannerDir");
                if (string.IsNullOrEmpty(msFileInfoScannerDir))
                {
                    var msg = "Manager parameter 'MSFileInfoScannerDir' is not defined";
                    LogError("Error in CreateQCPlots: " + msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msFileInfoScannerDLLPath = Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");
                if (!File.Exists(msFileInfoScannerDLLPath))
                {
                    var msg = "File Not Found: " + msFileInfoScannerDLLPath;
                    LogError("Error in CreateQCPlots: " + msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mMSFileInfoScannerReportsEmptyIsosFile = false;

                var qcPlotGenerator = new clsDeconToolsQCPlotsGenerator(msFileInfoScannerDLLPath, mDebugLevel);
                RegisterEvents(qcPlotGenerator);
                qcPlotGenerator.ErrorEvent += QCPlotGenerator_ErrorEvent;

                // Create the QC Plot .png files and associated Index.html file
                var success = qcPlotGenerator.CreateQCPlots(isosFilePath, mWorkDir);

                if (!success)
                {
                    LogError("Error generating QC Plots files with clsDeconToolsQCPlotsGenerator");
                    LogMessage(qcPlotGenerator.ErrorMessage, 0, true);

                    if (qcPlotGenerator.MSFileInfoScannerErrorCount > 0)
                    {
                        LogWarning("MSFileInfoScanner encountered " + qcPlotGenerator.MSFileInfoScannerErrorCount + " errors");
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mMSFileInfoScannerReportsEmptyIsosFile)
                {
                    // None of the data in the _isos.csv file passed the isotopic fit filter
                    if (mMessage.StartsWith(MS_FILE_INFO_SCANNER_NO_ISOS_DATA, StringComparison.OrdinalIgnoreCase))
                    {
                        mMessage = "";
                    }

                    LogError("Unable to create QC plots since no data in _isos.csv passed thresholds");

                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                // Make sure the key png files were created
                var expectedFileExtensions = new List<string>
                {
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
                            filesToFind.Add(new FileInfo(Path.Combine(mWorkDir, mDatasetName + extension)));
                            if (fileDescription.Length == 0)
                                fileDescription = extension;
                            else
                                fileDescription += " or " + extension;
                        }
                    }
                    else
                    {
                        filesToFind.Add(new FileInfo(Path.Combine(mWorkDir, mDatasetName + fileExtension)));
                        fileDescription = fileExtension;
                    }

                    var filesFound = 0;
                    foreach (var pngFile in filesToFind)
                    {
                        if (pngFile.Exists)
                            filesFound++;
                    }

                    if (filesFound != 0)
                        continue;

                    if (fileExtension.Contains("_HighAbu_LCMS.png") || fileExtension.Contains("_LCMS.png"))
                    {
                        // This file may be missing if _isos.csv only contains one data point
                        if (isosDataLineCount < 0)
                        {
                            var isosFileHasData = IsosFileHasData(isosFilePath, out isosDataLineCount, countTotalDataLines: true);
                            if (!isosFileHasData || isosDataLineCount == 0)
                            {
                                LogError("No results in DeconTools Isos file");
                                return CloseOutType.CLOSEOUT_NO_DATA;
                            }
                        }

                        if (isosDataLineCount <= 3)
                        {
                            // The Isos file has very little data (with isotopic fit <= 0.15)
                            // Allow this plot to be missing
                            continue;
                        }
                    }

                    LogError("QC png file not found, extension " + fileDescription);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage("Generated QC Plots file using " + isosFilePath);
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateQCPlots", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Examines isosFilePath to look for data lines (does not read the entire file, just the first two lines)
        /// </summary>
        /// <param name="isosFilePath"></param>
        /// <param name="maxFitValue">Fit value threshold to apply; use 1 to use all data</param>
        /// <returns>True if it has one or more lines of data, otherwise, returns False</returns>
        /// <remarks></remarks>
        private bool IsosFileHasData(string isosFilePath, double maxFitValue = 0.15)
        {
            return IsosFileHasData(isosFilePath, out _, false, maxFitValue);
        }

        /// <summary>
        /// Examines isosFilePath to look for data lines
        /// </summary>
        /// <param name="isosFilePath"></param>
        /// <param name="dataLineCount">Output parameter: total data line count</param>
        /// <param name="countTotalDataLines">True to count all of the data lines; false to just look for the first data line</param>
        /// <param name="maxFitValue">Fit value threshold to apply; use 1 to use all data</param>
        /// <returns>True if it has one or more lines of data, otherwise, returns False</returns>
        /// <remarks></remarks>
        private bool IsosFileHasData(string isosFilePath, out int dataLineCount, bool countTotalDataLines, double maxFitValue = 0.15)
        {
            dataLineCount = 0;
            var headerLineProcessed = false;

            var fitColumnIndex = -1;

            try
            {
                if (!File.Exists(isosFilePath))
                {
                    // File not found
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(isosFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (headerLineProcessed)
                        {
                            // This is a data line
                            if (maxFitValue < 1 && fitColumnIndex >= 0)
                            {
                                // Filter on isotopic Fit value
                                var dataColumns = dataLine.Split(',');
                                if (dataColumns.Length < fitColumnIndex)
                                    continue;

                                if (double.TryParse(dataColumns[fitColumnIndex], out var fitValue))
                                {
                                    if (fitValue <= maxFitValue)
                                        dataLineCount = 1;
                                }

                            }
                            else
                            {
                                dataLineCount = 1;
                            }

                            if (!countTotalDataLines && dataLineCount > 0)
                            {
                                // At least one valid data line has been found
                                return true;
                            }

                        }
                        else
                        {
                            var dataColumns = dataLine.Split(',');

                            for (var i = 0; i < dataColumns.Length; i++)
                            {
                                if (string.Equals(dataColumns[i], "fit", StringComparison.OrdinalIgnoreCase))
                                {
                                    fitColumnIndex = i;
                                    break;
                                }
                            }

                            headerLineProcessed = true;
                        }
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return dataLineCount > 0;
        }

        /// <summary>
        /// Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass
        /// </summary>
        /// <returns></returns>
        public override CloseOutType RunTool()
        {

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerDecon2ls.RunTool(): Enter");
            }

            mRawDataTypeName = clsAnalysisResources.GetRawDataTypeName(mJobParams, out var errorMessage);

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
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error running Decon2LS";
                }

                if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    eReturnCode = eResult;
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        mMessage = "No results in DeconTools Isos file";
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

            if (mJobParams.GetJobParameter(clsAnalysisResourcesDecon2ls.JOB_PARAM_PROCESSMSMS_AUTO_ENABLED, false))
            {
                mEvalMessage = clsGlobal.AppendToComment(mEvalMessage, "Note: auto-enabled ProcessMSMS in the parameter file");
                LogMessage(mEvalMessage);
            }

            // Zip the _Peaks.txt file (if it exists)
            ZipPeaksFile();

            // Delete the raw data files
            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file");
            }

            var messageSaved = string.Copy(mMessage);

            var deleteSuccess = DeleteRawDataFiles(mRawDataType);

            if (!deleteSuccess)
            {
                LogMessage("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " + mMessage, 0, true);

                // Don't treat this as a critical error; leave eReturnCode unchanged and restore mMessage
                if (!clsGlobal.IsMatch(mMessage, messageSaved))
                {
                    mMessage = messageSaved;
                }
            }

            // Update the job summary file
            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file");
            }

            UpdateSummaryFile();

            if (eResult == CloseOutType.CLOSEOUT_FAILED)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var success = CopyResultsToTransferDirectory();

            return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;

        }

        private CloseOutType RunDecon2Ls()
        {
            var paramFileNameOverride = mJobParams.GetJobParameter(
                clsAnalysisJob.JOB_PARAMETERS_SECTION,
                clsAnalysisResourcesDecon2ls.JOB_PARAM_DECON_TOOLS_PARAMETER_FILE_NAME,
                string.Empty);

            string paramFileName;
            if (string.IsNullOrWhiteSpace(paramFileNameOverride))
            {
                paramFileName = mJobParams.GetParam("ParmFileName");
            }
            else
            {
                paramFileName = paramFileNameOverride;
            }

            var paramFilePath = Path.Combine(mWorkDir, paramFileName);

            var decon2LSError = false;

            // Cache the parameters in the DeconTools parameter file

            var oDeconToolsParamFileReader = CacheDeconToolsParamFile(paramFilePath);

            if (oDeconToolsParamFileReader == null)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get file type of the raw data file
            var fileType = GetInputFileType(mRawDataType);

            if (fileType == DeconToolsFileTypeConstants.Undefined)
            {
                LogError("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specified while getting file type: " + mRawDataType);
                mMessage = "Invalid raw data type specified";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Specify Input file or folder
            mInputFilePath = GetInputFilePath(mRawDataType);
            if (string.IsNullOrWhiteSpace(mInputFilePath))
            {
                LogError("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specified while input file name: " + mRawDataType);
                mMessage = "Invalid raw data type specified";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Determine the path to the DeconTools folder
            var progLoc = DetermineProgramLocation("DeconToolsProgLoc", "DeconConsole.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the DeconTools version info in the database
            mMessage = string.Empty;
            if (!StoreToolVersionInfo(progLoc))
            {
                LogMessage("Aborting since StoreToolVersionInfo returned false", 0, true);
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error determining DeconTools version";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Reset the log file tracking variables
            mDeconToolsExceptionThrown = false;
            mDeconToolsFinishedDespiteProgRunnerError = false;

            // Reset the state variables
            mDeconToolsStatus.Clear();

            if (fileType == DeconToolsFileTypeConstants.PNNL_UIMF)
            {
                mDeconToolsStatus.IsUIMF = true;
            }
            else
            {
                mDeconToolsStatus.IsUIMF = false;
            }

            // Start Decon2LS and wait for it to finish
            var eDeconToolsStatus = StartDeconTools(progLoc, mInputFilePath, paramFilePath, fileType);

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Make sure objects are released
            PRISM.ProgRunner.GarbageCollectNow();

            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished");
            }

            // Determine reason for Decon2LS finish
            if (mDeconToolsFinishedDespiteProgRunnerError && !mDeconToolsExceptionThrown)
            {
                // ProgRunner reported an error code
                // However, the log file says things completed successfully
                // We'll trust the log file
            }
            else
            {
                switch (eDeconToolsStatus)
                {
                    case DeconToolsStateType.Complete:
                        // This is normal, do nothing else
                        break;

                    case DeconToolsStateType.ErrorCode:
                        mMessage = "Decon2LS error";
                        decon2LSError = true;
                        break;

                    case DeconToolsStateType.BadErrorLogFile:
                        decon2LSError = true;

                        // Sleep for 1 minute
                        LogDebug("Sleeping for 1 minute");
                        PRISM.ProgRunner.SleepMilliseconds(60 * 1000);
                        break;

                    case DeconToolsStateType.Idle:
                        // DeconTools never actually started
                        mMessage = "Decon2LS error";
                        decon2LSError = true;
                        break;

                    case DeconToolsStateType.Running:
                        // We probably shouldn't get here
                        // But, we'll assume success
                        break;
                }
            }

            if (decon2LSError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var eResult = AssembleResults(oDeconToolsParamFileReader);

            if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_SUCCESS;

            // Check for no data first. If no data, exit but still copy results to server
            if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
            {
                return eResult;
            }

            LogError("AssembleResults returned " + eResult);
            return CloseOutType.CLOSEOUT_FAILED;

        }

        private DeconToolsStateType StartDeconTools(string progLoc, string inputFilePath, string paramFilePath, DeconToolsFileTypeConstants eFileType)
        {
            DeconToolsStateType eDeconToolsStatus;

            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerDecon2lsDeIsotope.StartDeconTools(), Starting deconvolution");
            }

            try
            {
                string arguments;

                if (eFileType == DeconToolsFileTypeConstants.Undefined)
                {
                    LogError("Undefined file type found in StartDeconTools");
                    return DeconToolsStateType.ErrorCode;
                }

                var fileTypeText = GetDeconFileTypeText(eFileType);

                // Set up and execute a program runner to run DeconTools
                if (mDeconConsoleBuild < 4400)
                {
                    arguments = inputFilePath + " " + fileTypeText + " " + paramFilePath;
                }
                else
                {
                    arguments = inputFilePath + " " + paramFilePath;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + arguments);
                }

                mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = true;
                mCmdRunner.EchoOutputToConsole = true;

                // We don't need to capture the console output since the DeconTools log file has very similar information
                mCmdRunner.WriteConsoleOutputToFile = false;

                // ReSharper disable once RedundantAssignment
                eDeconToolsStatus = DeconToolsStateType.Running;

                mProgress = 0;
                ResetProgRunnerCpuUsage();

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var success = mCmdRunner.RunProgram(progLoc, arguments, "DeconConsole", true);

                if (!success)
                {
                    mMessage = "Error running DeconTools";
                    LogMessage(mMessage + ", job " + mJob, 0, true);
                }

                // Parse the DeconTools .Log file to see whether it contains message "Finished file processing"
                var finishTime = DateTime.Now;

                ParseDeconToolsLogFile(out var finishedProcessing, ref finishTime);

                if (mDeconToolsExceptionThrown)
                {
                    eDeconToolsStatus = DeconToolsStateType.ErrorCode;
                }
                else if (success)
                {
                    eDeconToolsStatus = DeconToolsStateType.Complete;
                }
                else if (finishedProcessing)
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
                var workDir = new DirectoryInfo(Path.Combine(mWorkDir));

                foreach (var logFile in workDir.GetFiles(mDatasetName + "*BAD_ERROR_log.txt"))
                {
                    mMessage = "Error running DeconTools; Bad_Error_log file exists";
                    LogMessage(mMessage + ": " + logFile.Name, 0, true);
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
                case DeconToolsFileTypeConstants.Thermo_Raw:
                    return "Thermo_Raw";
                case DeconToolsFileTypeConstants.ICR2LS_RawData:
                    return "ICR2LS_RawData";
                case DeconToolsFileTypeConstants.Micromass_RawData:
                    return "Micromass_RawData";
                case DeconToolsFileTypeConstants.MZXML_RawData:
                    return "MZXML_RawData";
                // Future: Case DeconToolsFileTypeConstants.MZML_RawData : Return "MZML_RawData"
                case DeconToolsFileTypeConstants.PNNL_IMS:
                    return "PNNL_IMS";
                case DeconToolsFileTypeConstants.PNNL_UIMF:
                    return "PNNL_UIMF";
                case DeconToolsFileTypeConstants.SUN_EXTREL:
                    // ReSharper disable once StringLiteralTypo
                    return "SUNEXTREL";
                default:
                    return "Undefined";
            }
        }

        private DeconToolsFileTypeConstants GetInputFileType(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            var InstrumentClass = mJobParams.GetParam("instClass");

            // Gets the Decon2LS file type based on the input data type
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    return DeconToolsFileTypeConstants.Thermo_Raw;

                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    return DeconToolsFileTypeConstants.Agilent_WIFF;

                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    return DeconToolsFileTypeConstants.PNNL_UIMF;

                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    return DeconToolsFileTypeConstants.Agilent_D;

                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    return DeconToolsFileTypeConstants.Micromass_RawData;

                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:
                    if (string.Equals(InstrumentClass, "BrukerFTMS", StringComparison.OrdinalIgnoreCase))
                    {
                        // Data from Bruker FTICR
                        return DeconToolsFileTypeConstants.Bruker;
                    }

                    if (InstrumentClass.ToLower() == "finnigan_fticr")
                    {
                        // Data from old Finnigan FTICR
                        return DeconToolsFileTypeConstants.SUN_EXTREL;
                    }

                    // Should never get here
                    return DeconToolsFileTypeConstants.Undefined;

                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                    return DeconToolsFileTypeConstants.Bruker;

                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:

                    // Future: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.Bruker_15T

                    LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" + eRawDataType + ")");
                    return DeconToolsFileTypeConstants.Undefined;

                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:

                    // Future: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.Bruker_15T

                    LogError("Decon2LS_V2 does not yet support Bruker MALDI data (" + eRawDataType + ")");
                    return DeconToolsFileTypeConstants.Undefined;

                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    return DeconToolsFileTypeConstants.MZXML_RawData;

                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    // Future: Add support for this after Decon2LS is updated
                    // Return DeconToolsFileTypeConstants.MZML_RawData

                    LogError("Decon2LS_V2 does not yet support mzML data");
                    return DeconToolsFileTypeConstants.Undefined;

                default:
                    // Should never get this value
                    return DeconToolsFileTypeConstants.Undefined;
            }
        }

        private void ParseDeconToolsLogFile(out bool finishedProcessing, ref DateTime finishTime)
        {
            var scanFrameLine = string.Empty;

            finishedProcessing = false;

            try
            {
                string logFilePath;

                switch (mRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                        // As of 11/19/2010, the _Log.txt file is created inside the .D folder
                        logFilePath = Path.Combine(mInputFilePath, mDatasetName) + "_log.txt";
                        break;
                    default:
                        logFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) + "_log.txt");
                        break;
                }

                if (File.Exists(logFilePath))
                {
                    using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (string.IsNullOrWhiteSpace(dataLine))
                                continue;

                            var charIndex = dataLine.ToLower().IndexOf("finished file processing", StringComparison.Ordinal);

                            if (charIndex >= 0)
                            {
                                var dateValid = false;
                                if (charIndex > 1)
                                {
                                    // Parse out the date from dataLine
                                    if (DateTime.TryParse(dataLine.Substring(0, charIndex).Trim(), out finishTime))
                                    {
                                        dateValid = true;
                                    }
                                    else
                                    {
                                        // Unable to parse out the date
                                        LogMessage("Unable to parse date from string '" +
                                            dataLine.Substring(0, charIndex).Trim() + "'; " +
                                            "will use file modification date as the processing finish time", 0, true);
                                    }
                                }

                                if (!dateValid)
                                {
                                    var fileInfo = new FileInfo(logFilePath);
                                    finishTime = fileInfo.LastWriteTime;
                                }

                                if (mDebugLevel >= 3)
                                {
                                    LogDebug("DeconTools log file reports 'finished file processing' at " +
                                        finishTime.ToString(DATE_TIME_FORMAT));
                                }

                                finishedProcessing = true;
                                break;
                            }

                            charIndex = dataLine.ToLower().IndexOf("scan/frame", StringComparison.Ordinal);
                            if (charIndex >= 0)
                            {
                                scanFrameLine = dataLine.Substring(charIndex);
                            }

                            charIndex = dataLine.IndexOf("ERROR THROWN", StringComparison.Ordinal);
                            if (charIndex > 0)
                            {
                                // An exception was reported in the log file; treat this as a fatal error
                                mMessage = "Error thrown by DeconTools";

                                LogMessage("DeconTools reports " + dataLine.Substring(charIndex), 0, true);
                                mDeconToolsExceptionThrown = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 4)
                {
                    LogWarning("Exception in ParseDeconToolsLogFile: " + ex.Message);
                }
            }

            if (!string.IsNullOrWhiteSpace(scanFrameLine))
            {
                // Parse scanFrameLine
                // It will look like:
                // Scan/Frame= 347; PercentComplete= 2.7; AccumulatedFeatures= 614

                var progressStats = scanFrameLine.Split(';');

                for (var i = 0; i <= progressStats.Length - 1; i++)
                {
                    var kvStat = ParseKeyValue(progressStats[i]);
                    if (string.IsNullOrWhiteSpace(kvStat.Key)) continue;

                    switch (kvStat.Key)
                    {
                        case "Scan/Frame":
                            if (int.TryParse(kvStat.Value.Replace(",", string.Empty), out var currentScan))
                            {
                                mDeconToolsStatus.CurrentLCScan = currentScan;
                            }
                            break;

                        case "PercentComplete":
                            float.TryParse(kvStat.Value, out mDeconToolsStatus.PercentComplete);
                            break;

                        // ReSharper disable once StringLiteralTypo
                        case "AccumlatedFeatures":
                        case "AccumulatedFeatures":
                        case "ScansProcessed":
                        case "ScansPerMinute":
                            // Ignore these
                            break;
                    }
                }

                mProgress = mDeconToolsStatus.PercentComplete;
            }
        }

        /// <summary>
        /// Looks for an equals sign in data
        /// Returns a KeyValuePair object with the text before the equals sign and the text after the equals sign
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private KeyValuePair<string, string> ParseKeyValue(string data)
        {
            var charIndex = data.IndexOf('=');

            if (charIndex > 0)
            {
                try
                {
                    return new KeyValuePair<string, string>(data.Substring(0, charIndex).Trim(), data.Substring(charIndex + 1).Trim());
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
        /// <param name="filePath"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if two or more non-blank lines; otherwise false</returns>
        /// <remarks></remarks>
        private bool ResultsFileHasData(string filePath, string fileDescription)
        {
            if (!File.Exists(filePath))
            {
                LogMessage("DeconTools results file not found: " + filePath);
                return false;
            }

            var dataLineCount = 0;

            // Open the DeconTools results file
            // The first line is the header lines
            // Lines after that are data lines

            LogDebug("Opening the DeconTools results file: " + filePath);

            using (var srReader = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srReader.EndOfStream && dataLineCount < 2)
                {
                    var lineIn = srReader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(lineIn))
                    {
                        dataLineCount += 1;
                    }
                }
            }

            if (dataLineCount >= 2)
            {
                LogDebug("DeconTools results file has at least two non-blank lines");
                return true;
            }

            LogDebug("DeconTools " + fileDescription + " file is empty");
            return false;
        }

        public string GetInputFilePath(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            return GetInputFilePath(mWorkDir, mDatasetName, eRawDataType);
        }

        /// <summary>
        /// Assembles a string telling Decon2LS the name of the input file or folder
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
                    // Future: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    return Path.Combine(workDirPath, datasetName);

                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:
                    ////////////////////////////////////
                    // Future: Finalize this code
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
        private bool StoreToolVersionInfo(string deconToolsProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioDeconToolsInfo = new FileInfo(deconToolsProgLoc);
            if (!ioDeconToolsInfo.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogMessage("Exception calling SetStepTaskToolVersion: " + ex.Message, 0, true);
                    return false;
                }
            }

            // Lookup the version of the DeconConsole application
            var success = mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, ioDeconToolsInfo.FullName);
            if (!success)
                return false;

            // Parse out the DeconConsole Build number using a RegEx
            // toolVersionInfo should look like: DeconConsole, Version=1.0.4400.22961

            mDeconConsoleBuild = 0;
            var reParseVersion = new Regex(@"Version=\d+\.\d+\.(\d+)");
            var reMatch = reParseVersion.Match(toolVersionInfo);
            if (reMatch.Success)
            {
                if (!int.TryParse(reMatch.Groups[1].Value, out mDeconConsoleBuild))
                {
                    // Error parsing out the version
                    mMessage = "Error determining DeconConsole version, cannot convert build to integer";
                    LogMessage(mMessage + ": " + toolVersionInfo, 0, true);
                    return false;
                }
            }
            else
            {
                mMessage = "Error determining DeconConsole version, RegEx did not match";
                LogMessage(mMessage + ": " + toolVersionInfo, 0, true);
                return false;
            }

            string deconToolsBackendPath;
            if (ioDeconToolsInfo.DirectoryName == null)
            {
                deconToolsBackendPath = string.Empty;
            }
            else
            {
                // Lookup the version of the DeconTools Backend (in the DeconTools folder)
                deconToolsBackendPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconTools.Backend.dll");
                success = mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, deconToolsBackendPath);
                if (!success)
                    return false;

                // Lookup the version of the UIMFLibrary (in the DeconTools folder)
                var dllPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "UIMFLibrary.dll");
                success = mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dllPath);
                if (!success)
                    return false;
            }

            // Old: Lookup the version of DeconEngine (in the DeconTools folder)
            // Disabled July 31, 2014 because support for Rapid was removed from DeconTools.Backend.dll and thus DeconEngine.dll is no longer required
            // dllPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngine.dll")
            // success = MyBase.StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath)
            // If Not success Then Return False

            // ReSharper disable CommentTypo

            // Old: Lookup the version of DeconEngineV2 (in the DeconTools folder)
            // Disabled May 20, 2016 because the C++ code that was in DeconEngineV2.dll has been ported to C# and is now part of DeconTools.Backend
            // See DeconTools.Backend\ProcessingTasks\Deconvoluters\HornDeconvolutor\ThrashV1\ThrashV1_Readme.txt

            // ReSharper restore CommentTypo

            // dllPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngineV2.dll")
            // success = mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dllPath)
            // If Not success Then Return False

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(deconToolsProgLoc),
                new FileInfo(deconToolsBackendPath)};

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
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
                var peaksFilePath = Path.Combine(mWorkDir, mDatasetName + DECON2LS_PEAKS_FILE_SUFFIX);
                var zippedPeaksFilePath = Path.Combine(mWorkDir, mDatasetName + "_peaks.zip");

                if (File.Exists(peaksFilePath))
                {
                    if (!ZipFile(peaksFilePath, false, zippedPeaksFilePath))
                    {
                        LogError("Error zipping " + DECON2LS_PEAKS_FILE_SUFFIX + " file");
                        return;
                    }

                    // Add the _peaks.txt file to .FilesToDelete since we only want to keep the Zipped version
                    mJobParams.AddResultFileToSkip(Path.GetFileName(peaksFilePath));
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping " + DECON2LS_PEAKS_FILE_SUFFIX + " file", ex);
            }
        }

        #endregion

        #region "Event Handlers"

        private DateTime mLastLogCheckTime = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the log file every 30 seconds to determine the % complete
            if (DateTime.UtcNow.Subtract(mLastLogCheckTime).TotalSeconds < SECONDS_BETWEEN_UPDATE)
            {
                return;
            }

            mLastLogCheckTime = DateTime.UtcNow;

            var finishTime = DateTime.UtcNow;

            ParseDeconToolsLogFile(out var finishedProcessing, ref finishTime);

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            string progressMessage;

            if (mDeconToolsStatus.IsUIMF)
            {
                progressMessage = "Frame=" + mDeconToolsStatus.CurrentLCScan;
            }
            else
            {
                progressMessage = "Scan=" + mDeconToolsStatus.CurrentLCScan;
            }

            progressMessage = "DeconTools, " + progressMessage;

            int logIntervalMinutes;
            if (mDebugLevel >= 5)
            {
                logIntervalMinutes = 1;
            }
            else if (mDebugLevel >= 4)
            {
                logIntervalMinutes = 2;
            }
            else if (mDebugLevel >= 3)
            {
                logIntervalMinutes = 5;
            }
            else if (mDebugLevel >= 2)
            {
                logIntervalMinutes = 10;
            }
            else
            {
                logIntervalMinutes = 15;
            }

            LogProgress(progressMessage, logIntervalMinutes);

            const int MAX_LOG_FINISHED_WAIT_TIME_SECONDS = 120;
            if (!finishedProcessing)
                return;

            // The Decon2LS Log File reports that the task is complete
            // If it finished over MAX_LOG_FINISHED_WAIT_TIME_SECONDS seconds ago, send an abort to the CmdRunner

            if (DateTime.Now.Subtract(finishTime).TotalSeconds >= MAX_LOG_FINISHED_WAIT_TIME_SECONDS)
            {
                LogDebug("Note: Log file reports finished over " + MAX_LOG_FINISHED_WAIT_TIME_SECONDS +
                         " seconds ago, but the DeconTools CmdRunner is still active");

                mDeconToolsFinishedDespiteProgRunnerError = true;

                // Abort processing
                mCmdRunner.AbortProgramNow();

                clsGlobal.IdleLoop(3);
            }
        }

        private void QCPlotGenerator_ErrorEvent(string message, Exception ex)
        {
            if (message.ToLower().Contains(MS_FILE_INFO_SCANNER_NO_ISOS_DATA.ToLower()))
                mMSFileInfoScannerReportsEmptyIsosFile = true;
        }


        #endregion
    }
}