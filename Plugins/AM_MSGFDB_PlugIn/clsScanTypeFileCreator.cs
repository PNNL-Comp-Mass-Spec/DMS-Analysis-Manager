using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using ThermoRawFileReader;

namespace AnalysisManagerMSGFDBPlugIn
{
    public class clsScanTypeFileCreator
    {
        private string mErrorMessage;
        private string mExceptionDetails;

        private Dictionary<int, string> mScanTypeMap;

        private readonly string mWorkDir;
        private readonly string mDatasetName;
        private string mScanTypeFilePath;
        private int mValidScanTypeLineCount;

        #region "Properties"

        public string DatasetName
        {
            get { return mDatasetName; }
        }

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        public string ExceptionDetails
        {
            get { return mExceptionDetails; }
        }

        public string ScanTypeFilePath
        {
            get { return mScanTypeFilePath; }
        }

        public int ValidScanTypeLineCount
        {
            get { return mValidScanTypeLineCount; }
        }

        public string WorkDir
        {
            get { return mWorkDir; }
        }

        #endregion

        public clsScanTypeFileCreator(string strWorkDirectoryPath, string strDatasetName)
        {
            mWorkDir = strWorkDirectoryPath;
            mDatasetName = strDatasetName;
            mErrorMessage = string.Empty;
            mExceptionDetails = string.Empty;
            mScanTypeFilePath = string.Empty;
        }

        private bool CacheScanTypeUsingScanStatsEx(string strScanStatsExFilePath)
        {
            try
            {
                if (mScanTypeMap == null)
                {
                    mScanTypeMap = new Dictionary<int, string>();
                }
                else
                {
                    mScanTypeMap.Clear();
                }

                if (!File.Exists(strScanStatsExFilePath))
                {
                    mErrorMessage = "_ScanStatsEx.txt file not found: " + strScanStatsExFilePath;
                    return false;
                }

                using (var srScanStatsExFile = new StreamReader(new FileStream(strScanStatsExFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Define the default column mapping
                    var scanNumberColIndex = 1;
                    var collisionModeColIndex = 7;
                    var scanFilterColIndex = 8;

                    var linesRead = 0;
                    while (!srScanStatsExFile.EndOfStream)
                    {
                        var dataLine = srScanStatsExFile.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        linesRead += 1;
                        var dataColumns = dataLine.Split('\t');

                        var firstColumnIsNumber = FirstColumnIsInteger(dataColumns);

                        if (linesRead == 1 && dataColumns.Length > 0 && !firstColumnIsNumber)
                        {
                            // This is a header line; define the column mapping

                            const bool IS_CASE_SENSITIVE = false;
                            var lstHeaderNames = new List<string> {
                                "ScanNumber",
                                "Collision Mode",
                                "Scan Filter Text"
                            };

                            // Keys in this dictionary are column names, values are the 0-based column index
                            var dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, lstHeaderNames, IS_CASE_SENSITIVE);

                            scanNumberColIndex = dctHeaderMapping["ScanNumber"];
                            collisionModeColIndex = dctHeaderMapping["Collision Mode"];
                            scanFilterColIndex = dctHeaderMapping["Scan Filter Text"];
                            continue;
                        }

                        // Parse out the values

                        if (TryGetValueInt(dataColumns, scanNumberColIndex, out var scanNumber))
                        {
                            var strCollisionMode = string.Empty;
                            var storeData = false;

                            if (TryGetValueStr(dataColumns, collisionModeColIndex, out strCollisionMode))
                            {
                                storeData = true;
                            }
                            else
                            {
                                var filterText = string.Empty;
                                if (TryGetValueStr(dataColumns, scanFilterColIndex, out filterText))
                                {
                                    filterText = dataColumns[scanFilterColIndex];

                                    // Parse the filter text to determine scan type
                                    strCollisionMode = XRawFileIO.GetScanTypeNameFromFinniganScanFilterText(filterText);

                                    storeData = true;
                                }
                            }

                            if (storeData)
                            {
                                if (string.IsNullOrEmpty(strCollisionMode))
                                {
                                    strCollisionMode = "MS";
                                }
                                else if (strCollisionMode == "0")
                                {
                                    strCollisionMode = "MS";
                                }

                                if (!mScanTypeMap.ContainsKey(scanNumber))
                                {
                                    mScanTypeMap.Add(scanNumber, strCollisionMode);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in CacheScanTypeUsingScanStatsEx: " + ex.GetType().Name;
                mExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        public bool CreateScanTypeFile()
        {
            try
            {
                mErrorMessage = string.Empty;
                mExceptionDetails = string.Empty;

                mValidScanTypeLineCount = 0;

                var strScanStatsFilePath = Path.Combine(mWorkDir, mDatasetName + "_ScanStats.txt");
                var strScanStatsExFilePath = Path.Combine(mWorkDir, mDatasetName + "_ScanStatsEx.txt");

                if (!File.Exists(strScanStatsFilePath))
                {
                    mErrorMessage = "_ScanStats.txt file not found: " + strScanStatsFilePath;
                    return false;
                }

                var blnDetailedScanTypesDefined = clsAnalysisResourcesMSGFDB.ValidateScanStatsFileHasDetailedScanTypes(strScanStatsFilePath);

                // Open the input file
                using (var srScanStatsFile = new StreamReader(new FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    mScanTypeFilePath = Path.Combine(mWorkDir, mDatasetName + "_ScanType.txt");

                    // Create the scan type output file
                    using (var swOutFile = new StreamWriter(new FileStream(mScanTypeFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swOutFile.WriteLine("ScanNumber\t" + "ScanTypeName\t" + "ScanType\t" + "ScanTime");

                        // Define the default column mapping
                        var scanNumberColIndex = 1;
                        var scanTimeColIndex = 2;
                        var scanTypeColIndex = 3;
                        var scanTypeNameColIndex = -1;
                        var scanStatsExLoaded = false;

                        var linesRead = 0;
                        while (!srScanStatsFile.EndOfStream)
                        {
                            var dataLine = srScanStatsFile.ReadLine();
                            if (string.IsNullOrWhiteSpace(dataLine))
                            {
                                continue;
                            }

                            linesRead += 1;
                            var dataColumns = dataLine.Split('\t');

                            var firstColumnIsNumber = FirstColumnIsInteger(dataColumns);

                            if (linesRead == 1 && dataColumns.Length > 0 && !firstColumnIsNumber)
                            {
                                // This is a header line; define the column mapping

                                const bool IS_CASE_SENSITIVE = false;
                                var lstHeaderNames = new List<string> {
                                    "ScanNumber",
                                    "ScanTime",
                                    "ScanType",
                                    "ScanTypeName"
                                };

                                // Keys in this dictionary are column names, values are the 0-based column index
                                var dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, lstHeaderNames, IS_CASE_SENSITIVE);

                                scanNumberColIndex = dctHeaderMapping["ScanNumber"];
                                scanTimeColIndex = dctHeaderMapping["ScanTime"];
                                scanTypeColIndex = dctHeaderMapping["ScanType"];
                                scanTypeNameColIndex = dctHeaderMapping["ScanTypeName"];
                                continue;
                            }

                            if (linesRead == 1 && firstColumnIsNumber && dataColumns.Length >= 11 && blnDetailedScanTypesDefined)
                            {
                                // This is a ScanStats file that does not have a header line
                                // Assume the column indices are 1, 2, 3, and 10

                                scanNumberColIndex = 1;
                                scanTimeColIndex = 2;
                                scanTypeColIndex = 3;
                                scanTypeNameColIndex = 10;
                            }

                            if (scanTypeNameColIndex < 0 && !scanStatsExLoaded)
                            {
                                // Need to read the ScanStatsEx file

                                if (!CacheScanTypeUsingScanStatsEx(strScanStatsExFilePath))
                                {
                                    srScanStatsFile.Close();
                                    swOutFile.Close();
                                    return false;
                                }

                                scanStatsExLoaded = true;
                            }

                            if (!TryGetValueInt(dataColumns, scanNumberColIndex, out var scanNumber))
                                continue;

                            if (!TryGetValueInt(dataColumns, scanTypeColIndex, out var scanType))
                                continue;

                            var scanTypeName = string.Empty;
                            if (scanStatsExLoaded)
                            {
                                mScanTypeMap.TryGetValue(scanNumber, out scanTypeName);
                            }
                            else if (scanTypeNameColIndex >= 0)
                            {
                                TryGetValueStr(dataColumns, scanTypeNameColIndex, out scanTypeName);
                            }

                            TryGetValueSng(dataColumns, scanTimeColIndex, out var scanTime);

                            swOutFile.WriteLine(scanNumber + "\t" + scanTypeName + "\t" + scanType + "\t" + scanTime.ToString("0.0000"));

                            mValidScanTypeLineCount += 1;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in CreateScanTypeFile: " + ex.GetType().Name;
                mExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Return true if the value in the first index of dataColumns is an Integer
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <returns></returns>
        private bool FirstColumnIsInteger(string[] dataColumns)
        {
            return int.TryParse(dataColumns[0], out var dataValue);
        }

        /// <summary>
        /// Tries to convert the text at index colIndex of strData to an integer
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="intValue"></param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
        /// <remarks></remarks>
        private bool TryGetValueInt(string[] dataColumns, int colIndex, out int intValue)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                if (int.TryParse(dataColumns[colIndex], out intValue))
                {
                    return true;
                }
            }

            intValue = 0;
            return false;
        }

        /// <summary>
        /// Tries to convert the text at index colIndex of strData to a float
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="sngValue"></param>
        /// <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
        /// <remarks></remarks>
        private bool TryGetValueSng(string[] dataColumns, int colIndex, out float sngValue)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                if (float.TryParse(dataColumns[colIndex], out sngValue))
                {
                    return true;
                }
            }

            sngValue = 0;
            return false;
        }

        /// <summary>
        /// Tries to retrieve the string value at index colIndex in dataColumns()
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <param name="colIndex"></param>
        /// <param name="strValue"></param>
        /// <returns>True if success; false if colIndex is less than 0 or colIndex is out of range for dataColumns()</returns>
        /// <remarks></remarks>
        private bool TryGetValueStr(string[] dataColumns, int colIndex, out string strValue)
        {
            if (colIndex >= 0 && colIndex < dataColumns.Length)
            {
                strValue = dataColumns[colIndex];
                if (string.IsNullOrEmpty(strValue))
                    strValue = string.Empty;
                return true;
            }

            strValue = string.Empty;
            return false;
        }
    }
}