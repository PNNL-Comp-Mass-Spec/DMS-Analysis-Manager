using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using ThermoRawFileReader;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// ScanType file creator
    /// </summary>
    public class ScanTypeFileCreator
    {
        private Dictionary<int, string> mScanTypeMap;

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; }

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Exception details
        /// </summary>
        public string ExceptionDetails { get; private set; }

        /// <summary>
        /// Scan type file path
        /// </summary>
        public string ScanTypeFilePath { get; private set; }

        /// <summary>
        /// Number of valid ScanType lines
        /// </summary>
        public int ValidScanTypeLineCount { get; private set; }

        /// <summary>
        /// Working directory
        /// </summary>
        public string WorkDir { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDirectoryPath">Working directory path</param>
        /// <param name="datasetName">Dataset name</param>
        public ScanTypeFileCreator(string workDirectoryPath, string datasetName)
        {
            WorkDir = workDirectoryPath;
            DatasetName = datasetName;
            ErrorMessage = string.Empty;
            ExceptionDetails = string.Empty;
            ScanTypeFilePath = string.Empty;
        }

        private bool CacheScanTypeUsingScanStatsEx(string scanStatsExFilePath)
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

                if (!File.Exists(scanStatsExFilePath))
                {
                    ErrorMessage = "_ScanStatsEx.txt file not found: " + scanStatsExFilePath;
                    return false;
                }

                using var reader = new StreamReader(new FileStream(scanStatsExFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Define the default column mapping
                var scanNumberColIndex = 1;
                var collisionModeColIndex = 7;
                var scanFilterColIndex = 8;

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    linesRead++;
                    var dataCols = dataLine.Split('\t');

                    var firstColumnIsNumber = FirstColumnIsInteger(dataCols);

                    if (linesRead == 1 && dataCols.Length > 0 && !firstColumnIsNumber)
                    {
                        // This is a header line; define the column mapping

                        var headerNames = new List<string> {
                            "ScanNumber",
                            "Collision Mode",
                            "Scan Filter Text"
                        };

                        // Keys in this dictionary are column names, values are the 0-based column index
                        var columnMap = Global.ParseHeaderLine(dataLine, headerNames);

                        scanNumberColIndex = columnMap["ScanNumber"];
                        collisionModeColIndex = columnMap["Collision Mode"];
                        scanFilterColIndex = columnMap["Scan Filter Text"];
                        continue;
                    }

                    // Parse out the values

                    if (!Global.TryGetValueInt(dataCols, scanNumberColIndex, out var scanNumber))
                    {
                        continue;
                    }

                    var storeData = false;

                    if (Global.TryGetValue(dataCols, collisionModeColIndex, out var collisionMode))
                    {
                        storeData = true;
                    }
                    else
                    {
                        if (Global.TryGetValue(dataCols, scanFilterColIndex, out var filterText))
                        {
                            filterText = dataCols[scanFilterColIndex];

                            // Parse the filter text to determine scan type
                            collisionMode = XRawFileIO.GetScanTypeNameFromThermoScanFilterText(filterText, false);

                            storeData = true;
                        }
                    }

                    if (!storeData)
                    {
                        continue;
                    }

                    if (string.IsNullOrEmpty(collisionMode))
                    {
                        collisionMode = "MS";
                    }
                    else if (collisionMode == "0")
                    {
                        collisionMode = "MS";
                    }

                    if (!mScanTypeMap.ContainsKey(scanNumber))
                    {
                        mScanTypeMap.Add(scanNumber, collisionMode);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in CacheScanTypeUsingScanStatsEx: " + ex.GetType().Name;
                ExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        /// <summary>
        /// create the ScanType file using the MASIC ScanStats file
        /// </summary>
        public bool CreateScanTypeFile()
        {
            try
            {
                ErrorMessage = string.Empty;
                ExceptionDetails = string.Empty;

                ValidScanTypeLineCount = 0;

                var scanStatsFilePath = Path.Combine(WorkDir, DatasetName + AnalysisResources.SCAN_STATS_FILE_SUFFIX);
                var scanStatsExFilePath = Path.Combine(WorkDir, DatasetName + AnalysisResources.SCAN_STATS_EX_FILE_SUFFIX);

                if (!File.Exists(scanStatsFilePath))
                {
                    ErrorMessage = "_ScanStats.txt file not found: " + scanStatsFilePath;
                    return false;
                }

                var detailedScanTypesDefined = AnalysisResourcesMSGFDB.ValidateScanStatsFileHasDetailedScanTypes(scanStatsFilePath);

                // Open the input file
                using var scanStatsReader = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                ScanTypeFilePath = Path.Combine(WorkDir, DatasetName + "_ScanType.txt");

                // Create the scan type output file
                using var writer = new StreamWriter(new FileStream(ScanTypeFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var headerNames = new List<string>
                {
                    "ScanNumber",
                    "ScanTypeName",
                    "ScanType",
                    "ScanTime"
                };
                writer.WriteLine(string.Join("\t", headerNames));

                // Define the default column mapping
                var scanNumberColIndex = 1;
                var scanTimeColIndex = 2;
                var scanTypeColIndex = 3;
                var scanTypeNameColIndex = -1;
                var scanStatsExLoaded = false;

                var linesRead = 0;

                while (!scanStatsReader.EndOfStream)
                {
                    var dataLine = scanStatsReader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    linesRead++;
                    var dataColumns = dataLine.Split('\t');

                    var firstColumnIsNumber = FirstColumnIsInteger(dataColumns);

                    if (linesRead == 1 && dataColumns.Length > 0 && !firstColumnIsNumber)
                    {
                        // This is a header line; define the column mapping

                        // Keys in this dictionary are column names, values are the 0-based column index
                        var columnMap = Global.ParseHeaderLine(dataLine, headerNames);

                        scanNumberColIndex = columnMap["ScanNumber"];
                        scanTimeColIndex = columnMap["ScanTime"];
                        scanTypeColIndex = columnMap["ScanType"];
                        scanTypeNameColIndex = columnMap["ScanTypeName"];
                        continue;
                    }

                    if (linesRead == 1 && firstColumnIsNumber && dataColumns.Length >= 11 && detailedScanTypesDefined)
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

                        if (!CacheScanTypeUsingScanStatsEx(scanStatsExFilePath))
                        {
                            scanStatsReader.Close();
                            writer.Close();
                            return false;
                        }

                        scanStatsExLoaded = true;
                    }

                    if (!Global.TryGetValueInt(dataColumns, scanNumberColIndex, out var scanNumber))
                        continue;

                    if (!Global.TryGetValueInt(dataColumns, scanTypeColIndex, out var scanType))
                        continue;

                    var scanTypeName = string.Empty;

                    if (scanStatsExLoaded)
                    {
                        mScanTypeMap.TryGetValue(scanNumber, out scanTypeName);
                    }
                    else if (scanTypeNameColIndex >= 0)
                    {
                        Global.TryGetValue(dataColumns, scanTypeNameColIndex, out scanTypeName);
                    }

                    Global.TryGetValueFloat(dataColumns, scanTimeColIndex, out var scanTime);

                    writer.WriteLine(scanNumber + "\t" + scanTypeName + "\t" + scanType + "\t" + scanTime.ToString("0.0000"));

                    ValidScanTypeLineCount++;
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in CreateScanTypeFile: " + ex.GetType().Name;
                ExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Return true if the value in the first index of dataColumns is an integer
        /// </summary>
        /// <param name="dataColumns">List of data columns</param>
        private bool FirstColumnIsInteger(IReadOnlyList<string> dataColumns)
        {
            return int.TryParse(dataColumns[0], out _);
        }
    }
}