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
            this.r_FileSeparator = new Regex(@"^\s*[=]{5,}\s+\""(?<rootname>.+)\.(?<startscan>\d+)\." +
                                             @"(?<endscan>\d+)\.(?<chargestate>\d+)\.(?<filetype>.+)\""\s+[=]{5,}\s*$",
                                             RegexOptions.CultureInvariant | RegexOptions.Compiled);

            // Presently not used
            this.r_DTAFirstLine = new Regex(@"^\s*(?<parentmass>\d+\.\d+)\s+\d+\s+scan\=(?<scannum>\d+)\s+" +
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
            string strCDTAFile = null;
            int intSegmentCountToCreate = 0;

            try
            {
                //Call base class for initial setup
                base.RunTool();

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining DtaSplit version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                strCDTAFile = Path.Combine(m_WorkDir, m_Dataset + "_dta.txt");

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile())
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                try
                {
                    intSegmentCountToCreate = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                    if (intSegmentCountToCreate == 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Setting 'NumberOfClonedSteps' not found in the job parameters; will assume NumberOfClonedSteps=4");
                        intSegmentCountToCreate = 4;
                    }
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Setting 'NumberOfClonedSteps' is not numeric in the job parameters; will assume NumberOfClonedSteps=4");
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
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                UpdateStatusRunning(100, intSegmentCountToCreate);

                result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //TODO: What do we do here?
                    return result;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //TODO: What do we do here?
                    return result;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //TODO: What do we do here?
                    return result;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in DtaSplitPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //No failures so everything must have succeeded
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

            StreamReader srInFile = null;
            string strLineIn = null;
            Match splitMatch = null;
            int intSplitFileNum = 0;

            int intTargetSpectraPerSegment = 0;
            int intSpectraCountRead = 0;
            int intSpectraCountExpected = 0;

            long lngBytesRead = 0;

            int lineEndCharCount = 0;

            StreamWriter[] swOutFile = null;
            int[] intSpectraCountBySegment = null;

            string strSegmentDescription = null;

            float sngPercentComplete = 0;
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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "CountSpectraInCattedDtaFile returned a spectrum count of 0; this is unexpected");
                    }
                }

                var fi = new FileInfo(strSourceFilePath);

                if (intSegmentCountToCreate == 1)
                {
                    // Nothing to do except create a file named Dataset_1_dta.txt
                    // Simply rename the input file

                    try
                    {
                        string strDestFileName = null;
                        strDestFileName = GetNewSplitDTAFileName(1);

                        fi.MoveTo(strDestFileName);
                    }
                    catch (Exception ex)
                    {
                        if (strSourceFilePath == null)
                            strSourceFilePath = "??";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "Error in SplitCattedDtaFileIntoSegments renaming file: " + strSourceFilePath + " to _1_dta.txt; " + ex.Message);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                lineEndCharCount = LineEndCharacterCount(fi);

                intTargetSpectraPerSegment = Convert.ToInt32(Math.Ceiling(intSpectraCountExpected / Convert.ToDouble(intSegmentCountToCreate)));
                if (intTargetSpectraPerSegment < 1)
                    intTargetSpectraPerSegment = 1;

                if (m_DebugLevel >= 1)
                {
                    strSegmentDescription = "spectra per segment = " + intTargetSpectraPerSegment;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Splitting " + Path.GetFileName(strSourceFilePath) + " into " + intSegmentCountToCreate + " segments; " +
                        strSegmentDescription);
                }

                // Create all of the output files since we will write spectra to them in a round-robin fashion
                intSpectraCountBySegment = new int[intSegmentCountToCreate + 1];

                for (intSplitFileNum = 1; intSplitFileNum <= intSegmentCountToCreate; intSplitFileNum++)
                {
                    swOutFile[intSplitFileNum] = CreateNewSplitDTAFile(intSplitFileNum);
                    if (swOutFile[intSplitFileNum] == null)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                // Open the input file
                srInFile = new StreamReader(strSourceFilePath);

                intSplitFileNum = 1;
                intSpectraCountRead = 0;
                lngBytesRead = 0;

                while (!srInFile.EndOfStream)
                {
                    strLineIn = srInFile.ReadLine();

                    // Increment the bytes read counter
                    lngBytesRead += strLineIn.Length + lineEndCharCount;

                    // Look for the spectrum separator line
                    splitMatch = this.r_FileSeparator.Match(strLineIn);
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
                        sngPercentComplete = (lngBytesRead / Convert.ToSingle(srInFile.BaseStream.Length) * 100);
                        UpdateStatusRunning(sngPercentComplete, intSpectraCountRead);
                    }

                    swOutFile[intSplitFileNum].WriteLine(strLineIn);
                }

                // Close the input file and each of the output files
                srInFile.Close();

                for (intSplitFileNum = 1; intSplitFileNum <= intSegmentCountToCreate; intSplitFileNum++)
                {
                    swOutFile[intSplitFileNum].Close();
                }
            }
            catch (Exception ex)
            {
                if (strSourceFilePath == null)
                    strSourceFilePath = "??";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error in SplitCattedDtaFileIntoSegments reading file: " + strSourceFilePath + "; " + ex.Message);
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
            string strLineIn = null;
            Match splitMatch = null;

            int intSpectraCount = 0;

            try
            {
                intSpectraCount = 0;

                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Counting the number of spectra in the source _Dta.txt file: " + Path.GetFileName(strSourceFilePath));
                }

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(strSourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        splitMatch = this.r_FileSeparator.Match(strLineIn);
                        if (splitMatch.Success)
                        {
                            intSpectraCount += 1;
                        }
                    }

                    if (m_DebugLevel >= 1)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Spectrum count in source _Dta.txt file: " + intSpectraCount);
                    }
                }
            }
            catch (Exception ex)
            {
                if (strSourceFilePath == null)
                    strSourceFilePath = "??";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error counting the number of spectra in '" + strSourceFilePath + "'; " + ex.Message);
                intSpectraCount = 0;
            }

            return intSpectraCount;
        }

        private StreamWriter CreateNewSplitDTAFile(int fileNameCounter)
        {
            string strFileName = string.Empty;
            string strFilePath = null;
            StreamWriter swOutFile = null;

            try
            {
                strFilePath = GetNewSplitDTAFileName(fileNameCounter);

                strFileName = Path.GetFileName(strFilePath);

                if (File.Exists(strFilePath))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Warning: Split DTA file already exists " + strFilePath);
                }

                if (m_DebugLevel >= 3)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating split DTA file " + strFileName);
                }

                swOutFile = new StreamWriter(strFilePath, false);
            }
            catch (Exception ex)
            {
                if (strFileName == null)
                    strFileName = "??";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error in CreateNewSplitDTAFile creating file: " + strFileName + "; " + ex.Message);
            }

            return swOutFile;
        }

        private string GetNewSplitDTAFileName(int fileNameCounter)
        {
            string strFileName = null;
            string strFilePath = null;

            strFileName = m_Dataset + "_" + Convert.ToString(fileNameCounter) + "_dta.txt";
            m_jobParams.AddResultFileToKeep(strFileName);

            strFilePath = Path.Combine(m_WorkDir, strFileName);

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
            int testcode = 0;
            int testcode2 = 0;
            long counter = 0;
            var endCount = 1;         // Initially assume a one-byte line terminator

            if ((fi.Exists))
            {
                TextReader tr = fi.OpenText();
                for (counter = 1; counter <= fi.Length; counter++)
                {
                    testcode = tr.Read();
                    if (testcode == 10 | testcode == 13)
                    {
                        testcode2 = tr.Read();
                        if (testcode2 == 10 | testcode2 == 13)
                        {
                            endCount = 2;
                            break;
                        }
                        else
                        {
                            endCount = 1;
                            break;
                        }
                    }
                }

                tr.Close();
            }

            return endCount;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of the AnalysisManagerDtaSplitPlugIn
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerDtaSplitPlugIn"))
            {
                return false;
            }

            // Store the path to AnalysisManagerDtaSplitPlugIn.dll in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), "AnalysisManagerDtaSplitPlugIn.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
