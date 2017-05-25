//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/29/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerDtaSplitPlugIn
{
    /// <summary>
    /// Class for running DTA splitter
    /// </summary>
    public class clsAnalysisToolRunnerDtaSplit : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected Regex r_FileSeparator;
        protected Regex r_DTAFirstLine;        // Presently not used

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsAnalysisToolRunnerDtaSplit()
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
        /// Runs InSpecT tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                base.RunTool();

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining DtaSplit version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var strCDTAFile = Path.Combine(m_WorkDir, m_Dataset + "_dta.txt");

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile())
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                int intSegmentCountToCreate;
                try
                {
                    intSegmentCountToCreate = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                    if (intSegmentCountToCreate == 0)
                    {
                        LogWarning("Setting 'NumberOfClonedSteps' not found in the job parameters; will assume NumberOfClonedSteps=4");
                        intSegmentCountToCreate = 4;
                    }
                }
                catch (Exception)
                {
                    LogWarning("Setting 'NumberOfClonedSteps' is not numeric in the job parameters; will assume NumberOfClonedSteps=4");
                    intSegmentCountToCreate = 4;
                }

                // Note: blnSplitToEqualScanCounts is no longer used
                // blnSplitToEqualScanCounts = m_jobParams.GetJobParameter("ClonedStepsHaveEqualNumSpectra", True)

                //Start the job timer
                m_StartTime = DateTime.UtcNow;

                var result = SplitCattedDtaFileIntoSegments(strCDTAFile, intSegmentCountToCreate);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                UpdateStatusRunning(100, intSegmentCountToCreate);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in DtaSplitPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
            
        }

        /// <summary>
        /// Split the dta txt file into multiple files
        /// </summary>
        /// <param name="strSourceFilePath">Input data file path</param>
        /// <param name="intSegmentCountToCreate">Number of segments to create</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType SplitCattedDtaFileIntoSegments(string strSourceFilePath, int intSegmentCountToCreate)
        {
            const float STATUS_UPDATE_INTERVAL_SECONDS = 15;

            var intSpectraCountExpected = 0;
            var dtLastStatusUpdate = DateTime.UtcNow;

            try
            {
                if (intSegmentCountToCreate < 1)
                    intSegmentCountToCreate = 1;

                if (intSegmentCountToCreate > 1)
                {
                    // Need to pre-scan the file to count the number of spectra in it
                    intSpectraCountExpected = CountSpectraInCattedDtaFile(strSourceFilePath);

                    if (intSpectraCountExpected == 0)
                    {
                        LogWarning("CountSpectraInCattedDtaFile returned a spectrum count of 0; this is unexpected");
                    }
                }

                var fi = new FileInfo(strSourceFilePath);

                if (intSegmentCountToCreate == 1)
                {
                    // Nothing to do except create a file named Dataset_1_dta.txt
                    // Simply rename the input file

                    try
                    {
                        var strDestFileName = GetNewSplitDTAFileName(1);

                        fi.MoveTo(strDestFileName);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error in SplitCattedDtaFileIntoSegments renaming file: " + strSourceFilePath + " to _1_dta.txt; " + ex.Message, ex);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var lineEndCharCount = LineEndCharacterCount(fi);

                var intTargetSpectraPerSegment = (int)Math.Ceiling(intSpectraCountExpected / (float)intSegmentCountToCreate);
                if (intTargetSpectraPerSegment < 1)
                    intTargetSpectraPerSegment = 1;

                if (m_DebugLevel >= 1)
                {
                    var strSegmentDescription = "spectra per segment = " + intTargetSpectraPerSegment;
                    LogDebug(
                        "Splitting " + Path.GetFileName(strSourceFilePath) + " into " + intSegmentCountToCreate + " segments; " +
                        strSegmentDescription);
                }

                // Create all of the output files since we will write spectra to them in a round-robin fashion
                var intSpectraCountBySegment = new int[intSegmentCountToCreate + 1];
                var swOutFile = new StreamWriter[intSegmentCountToCreate + 1];

                for (var intSplitFileNum = 1; intSplitFileNum <= intSegmentCountToCreate; intSplitFileNum++)
                {
                    swOutFile[intSplitFileNum] = CreateNewSplitDTAFile(intSplitFileNum);
                    if (swOutFile[intSplitFileNum] == null)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                // Open the input file
                using (var srInFile = new StreamReader(strSourceFilePath))
                {
                    var intSplitFileNum = 1;
                    var intSpectraCountRead = 0;
                    var lngBytesRead = 0;

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                        {
                            lngBytesRead += lineEndCharCount;
                            swOutFile[intSplitFileNum].WriteLine();
                            continue;
                        }

                        // Increment the bytes read counter
                        lngBytesRead += strLineIn.Length + lineEndCharCount;

                        // Look for the spectrum separator line
                        var splitMatch = r_FileSeparator.Match(strLineIn);
                        if (splitMatch.Success)
                        {
                            if (intSpectraCountRead > 0)
                            {
                                // Increment intSplitFileNum, but only after the first spectrum has been read
                                intSplitFileNum += 1;
                                if (intSplitFileNum > intSegmentCountToCreate)
                                {
                                    intSplitFileNum = 1;
                                }

                                if (intSpectraCountBySegment[intSplitFileNum] == 0)
                                {
                                    // Add a blank line to the top of each file
                                    swOutFile[intSplitFileNum].WriteLine();
                                }
                            }

                            intSpectraCountRead += 1;
                            intSpectraCountBySegment[intSplitFileNum] += 1;
                        }

                        if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= STATUS_UPDATE_INTERVAL_SECONDS)
                        {
                            dtLastStatusUpdate = DateTime.UtcNow;
                            var sngPercentComplete = lngBytesRead / (float)srInFile.BaseStream.Length * 100;
                            UpdateStatusRunning(sngPercentComplete, intSpectraCountRead);
                        }

                        swOutFile[intSplitFileNum].WriteLine(strLineIn);
                    }

                }

                for (var intSplitFileNum = 1; intSplitFileNum <= intSegmentCountToCreate; intSplitFileNum++)
                {
                    swOutFile[intSplitFileNum].Flush();
                    swOutFile[intSplitFileNum].Dispose();
                }
            }
            catch (Exception ex)
            {
                if (strSourceFilePath == null)
                    strSourceFilePath = "??";
                LogError("Error in SplitCattedDtaFileIntoSegments reading file: " + strSourceFilePath + "; " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Counts the number of spectra in the input concatenated DTA file (_dta.txt file)
        /// </summary>
        /// <param name="strSourceFilePath"></param>
        /// <returns>The number of spectra found (i.e. the number of header lines found); returns 0 if any problems</returns>
        /// <remarks></remarks>
        private int CountSpectraInCattedDtaFile(string strSourceFilePath)
        {
            var intSpectraCount = 0;

            try
            {

                if (m_DebugLevel >= 2)
                {
                    LogDebug("Counting the number of spectra in the source _Dta.txt file: " + Path.GetFileName(strSourceFilePath));
                }

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var splitMatch = r_FileSeparator.Match(strLineIn);
                        if (splitMatch.Success)
                        {
                            intSpectraCount += 1;
                        }
                    }

                    if (m_DebugLevel >= 1)
                    {
                        LogDebug("Spectrum count in source _Dta.txt file: " + intSpectraCount);
                    }
                }
            }
            catch (Exception ex)
            {
                if (strSourceFilePath == null)
                    strSourceFilePath = "??";
                LogError("Error counting the number of spectra in '" + strSourceFilePath + "'; " + ex.Message, ex);
                intSpectraCount = 0;
            }

            return intSpectraCount;
        }

        private StreamWriter CreateNewSplitDTAFile(int fileNameCounter)
        {
            var strFileName = string.Empty;
            StreamWriter swOutFile = null;

            try
            {
                var strFilePath = GetNewSplitDTAFileName(fileNameCounter);

                strFileName = Path.GetFileName(strFilePath);

                if (File.Exists(strFilePath))
                {
                    LogWarning("Warning: Split DTA file already exists " + strFilePath);
                }

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Creating split DTA file " + strFileName);
                }

                swOutFile = new StreamWriter(strFilePath, false);
            }
            catch (Exception ex)
            {
                if (strFileName == null)
                    strFileName = "??";
                LogError("Error in CreateNewSplitDTAFile creating file: " + strFileName + "; " + ex.Message, ex);
            }

            return swOutFile;
        }

        private string GetNewSplitDTAFileName(int fileNameCounter)
        {
            var strFileName = m_Dataset + "_" + Convert.ToString(fileNameCounter) + "_dta.txt";
            m_jobParams.AddResultFileToKeep(strFileName);

            var strFilePath = Path.Combine(m_WorkDir, strFileName);

            return strFilePath;
        }

        /// <summary>
        /// This function reads the input file one byte at a time, looking for the first occurence of Chr(10) or Chr(13) (aka vbCR or VBLF)
        /// When found, the next byte is examined
        /// If the next byte is also Chr(10) or Chr(13), then the line terminator is assumed to be 2 bytes; if not found, then it is assumed to be one byte
        /// </summary>
        /// <param name="fi"></param>
        /// <returns>1 if a one-byte line terminator; 2 if a two-byte line terminator</returns>
        /// <remarks></remarks>
        private int LineEndCharacterCount(FileInfo fi)
        {
            var endCount = 1;         // Initially assume a one-byte line terminator

            if (!fi.Exists)
                return endCount;

            TextReader tr = fi.OpenText();
            for (var counter = 1; counter <= fi.Length; counter++)
            {
                var testcode = tr.Read();
                if (testcode == 10 | testcode == 13)
                {
                    var testcode2 = tr.Read();
                    if (testcode2 == 10 | testcode2 == 13)
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
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the AnalysisManagerDtaSplitPlugIn
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerDtaSplitPlugIn"))
            {
                return false;
            }

            // Store the path to AnalysisManagerDtaSplitPlugIn.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), "AnalysisManagerDtaSplitPlugIn.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        #endregion
    }
}
