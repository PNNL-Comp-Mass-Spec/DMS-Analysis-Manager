//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/29/2009
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerDtaSplitPlugIn
{
    /// <summary>
    /// Class for running DTA splitter
    /// </summary>
    public class AnalysisToolRunnerDtaSplit : AnalysisToolRunnerBase
    {
        // Ignore Spelling: pre

        private Regex r_FileSeparator;
        private Regex r_DTAFirstLine;        // Presently not used

        /// <summary>
        /// Constructor
        /// </summary>
        public AnalysisToolRunnerDtaSplit()
        {
            r_FileSeparator = new Regex(@"^\s*[=]{5,}\s+\""(?<rootname>.+)\.(?<startscan>\d+)\." +
                                        @"(?<endscan>\d+)\.(?<chargestate>\d+)\.(?<filetype>.+)\""\s+[=]{5,}\s*$",
                                             RegexOptions.CultureInvariant | RegexOptions.Compiled);

            // Presently not used
            r_DTAFirstLine = new Regex(@"^\s*(?<parentmass>\d+\.\d+)\s+\d+\s+scan\=(?<scannum>\d+)\s+" +
                                       @"cs\=(?<chargestate>\d+)$",
                                            RegexOptions.CultureInvariant | RegexOptions.Compiled);
        }

        /// <summary>
        /// Runs DTA splitter tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                base.RunTool();

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining DtaSplit version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var cdtaFile = Path.Combine(mWorkDir, mDatasetName + "_dta.txt");

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile())
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                int segmentCountToCreate;
                try
                {
                    segmentCountToCreate = mJobParams.GetJobParameter("NumberOfClonedSteps", 0);
                    if (segmentCountToCreate == 0)
                    {
                        LogWarning("Setting 'NumberOfClonedSteps' not found in the job parameters; will assume NumberOfClonedSteps=4");
                        segmentCountToCreate = 4;
                    }
                }
                catch (Exception)
                {
                    LogWarning("Setting 'NumberOfClonedSteps' is not numeric in the job parameters; will assume NumberOfClonedSteps=4");
                    segmentCountToCreate = 4;
                }

                // Note: splitToEqualScanCounts is no longer used
                // splitToEqualScanCounts = mJobParams.GetJobParameter("ClonedStepsHaveEqualNumSpectra", True)

                // Start the job timer
                mStartTime = DateTime.UtcNow;

                var result = SplitCattedDtaFileIntoSegments(cdtaFile, segmentCountToCreate);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                UpdateStatusRunning(100, segmentCountToCreate);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in DtaSplitPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Split the dta.txt file into multiple files
        /// </summary>
        /// <param name="sourceFilePath">Input data file path</param>
        /// <param name="segmentCountToCreate">Number of segments to create</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType SplitCattedDtaFileIntoSegments(string sourceFilePath, int segmentCountToCreate)
        {
            const float STATUS_UPDATE_INTERVAL_SECONDS = 15;

            var spectraCountExpected = 0;
            var lastStatusUpdate = DateTime.UtcNow;

            try
            {
                if (segmentCountToCreate < 1)
                    segmentCountToCreate = 1;

                if (segmentCountToCreate > 1)
                {
                    // Need to pre-scan the file to count the number of spectra in it
                    spectraCountExpected = CountSpectraInCattedDtaFile(sourceFilePath);

                    if (spectraCountExpected == 0)
                    {
                        LogWarning("CountSpectraInCattedDtaFile returned a spectrum count of 0; this is unexpected");
                    }
                }

                var fi = new FileInfo(sourceFilePath);

                if (segmentCountToCreate == 1)
                {
                    // Nothing to do except create a file named Dataset_1_dta.txt
                    // Simply rename the input file

                    try
                    {
                        var destFileName = GetNewSplitDTAFileName(1);

                        fi.MoveTo(destFileName);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error in SplitCattedDtaFileIntoSegments renaming file: " + sourceFilePath + " to _1_dta.txt; " + ex.Message, ex);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var lineEndCharCount = LineEndCharacterCount(fi);

                var targetSpectraPerSegment = (int)Math.Ceiling(spectraCountExpected / (float)segmentCountToCreate);
                if (targetSpectraPerSegment < 1)
                    targetSpectraPerSegment = 1;

                if (mDebugLevel >= 1)
                {
                    var segmentDescription = "spectra per segment = " + targetSpectraPerSegment;
                    LogDebug(
                        "Splitting " + Path.GetFileName(sourceFilePath) + " into " + segmentCountToCreate + " segments; " +
                        segmentDescription);
                }

                // Create all of the output files since we will write spectra to them in a round-robin fashion
                var spectraCountBySegment = new int[segmentCountToCreate + 1];
                var writer = new StreamWriter[segmentCountToCreate + 1];

                for (var splitFileNum = 1; splitFileNum <= segmentCountToCreate; splitFileNum++)
                {
                    writer[splitFileNum] = CreateNewSplitDTAFile(splitFileNum);
                    if (writer[splitFileNum] == null)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                // Open the input file
                using (var reader = new StreamReader(sourceFilePath))
                {
                    var splitFileNum = 1;
                    var spectraCountRead = 0;
                    var bytesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            bytesRead += lineEndCharCount;
                            writer[splitFileNum].WriteLine();
                            continue;
                        }

                        // Increment the bytes read counter
                        bytesRead += dataLine.Length + lineEndCharCount;

                        // Look for the spectrum separator line
                        var splitMatch = r_FileSeparator.Match(dataLine);
                        if (splitMatch.Success)
                        {
                            if (spectraCountRead > 0)
                            {
                                // Increment splitFileNum, but only after the first spectrum has been read
                                splitFileNum++;
                                if (splitFileNum > segmentCountToCreate)
                                {
                                    splitFileNum = 1;
                                }

                                if (spectraCountBySegment[splitFileNum] == 0)
                                {
                                    // Add a blank line to the top of each file
                                    writer[splitFileNum].WriteLine();
                                }
                            }

                            spectraCountRead++;
                            spectraCountBySegment[splitFileNum]++;
                        }

                        if (DateTime.UtcNow.Subtract(lastStatusUpdate).TotalSeconds >= STATUS_UPDATE_INTERVAL_SECONDS)
                        {
                            lastStatusUpdate = DateTime.UtcNow;
                            var percentComplete = bytesRead / (float)reader.BaseStream.Length * 100;
                            UpdateStatusRunning(percentComplete, spectraCountRead);
                        }

                        writer[splitFileNum].WriteLine(dataLine);
                    }
                }

                for (var splitFileNum = 1; splitFileNum <= segmentCountToCreate; splitFileNum++)
                {
                    writer[splitFileNum].Flush();
                    writer[splitFileNum].Dispose();
                }
            }
            catch (Exception ex)
            {
                if (sourceFilePath == null)
                    sourceFilePath = "??";
                LogError("Error in SplitCattedDtaFileIntoSegments reading file: " + sourceFilePath + "; " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Counts the number of spectra in the input concatenated DTA file (_dta.txt file)
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <returns>The number of spectra found (i.e. the number of header lines found); returns 0 if any problems</returns>
        private int CountSpectraInCattedDtaFile(string sourceFilePath)
        {
            var spectraCount = 0;

            try
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("Counting the number of spectra in the source _Dta.txt file: " + Path.GetFileName(sourceFilePath));
                }

                // Open the input file
                using var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var splitMatch = r_FileSeparator.Match(dataLine);
                    if (splitMatch.Success)
                    {
                        spectraCount++;
                    }
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug("Spectrum count in source _Dta.txt file: " + spectraCount);
                }
            }
            catch (Exception ex)
            {
                if (sourceFilePath == null)
                    sourceFilePath = "??";
                LogError("Error counting the number of spectra in '" + sourceFilePath + "'; " + ex.Message, ex);
                spectraCount = 0;
            }

            return spectraCount;
        }

        private StreamWriter CreateNewSplitDTAFile(int fileNameCounter)
        {
            var fileName = string.Empty;
            StreamWriter writer = null;

            try
            {
                var filePath = GetNewSplitDTAFileName(fileNameCounter);

                fileName = Path.GetFileName(filePath);

                if (File.Exists(filePath))
                {
                    LogWarning("Warning: Split DTA file already exists " + filePath);
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Creating split DTA file " + fileName);
                }

                writer = new StreamWriter(filePath, false);
            }
            catch (Exception ex)
            {
                if (fileName == null)
                    fileName = "??";
                LogError("Error in CreateNewSplitDTAFile creating file: " + fileName + "; " + ex.Message, ex);
            }

            return writer;
        }

        private string GetNewSplitDTAFileName(int fileNameCounter)
        {
            var fileName = mDatasetName + "_" + Convert.ToString(fileNameCounter) + "_dta.txt";
            mJobParams.AddResultFileToKeep(fileName);

            var filePath = Path.Combine(mWorkDir, fileName);

            return filePath;
        }

        /// <summary>
        /// This function reads the input file one byte at a time, looking for the first occurrence of character code 10 or 13 (aka CR or LF)
        /// When found, the next byte is examined
        /// If the next byte is also character code 10 or 13, the line terminator is assumed to be 2 bytes; if not found, it is assumed to be one byte
        /// </summary>
        /// <param name="fi"></param>
        /// <returns>1 if a one-byte line terminator; 2 if a two-byte line terminator</returns>
        private int LineEndCharacterCount(FileInfo fi)
        {
            var endCount = 1;         // Initially assume a one-byte line terminator

            if (!fi.Exists)
                return endCount;

            TextReader tr = fi.OpenText();
            for (var counter = 1; counter <= fi.Length; counter++)
            {
                var testCode = tr.Read();
                if (testCode == 10 || testCode == 13)
                {
                    var testCode2 = tr.Read();
                    if (testCode2 == 10 || testCode2 == 13)
                    {
                        endCount = 2;
                        break;
                    }

                    endCount = 1;
                    break;
                }
            }

            tr.Close();

            return endCount;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var dtaSplitDLL = Path.Combine(Global.GetAppDirectoryPath(), "AnalysisManagerDtaSplitPlugIn.dll");
            var success = StoreDotNETToolVersionInfo(dtaSplitDLL, true);

            return success;
        }
    }
}
