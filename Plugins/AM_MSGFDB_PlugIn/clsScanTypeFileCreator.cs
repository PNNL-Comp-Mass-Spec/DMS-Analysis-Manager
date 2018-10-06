using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using ThermoRawFileReader;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// ScanType file creator
    /// </summary>
    public class clsScanTypeFileCreator
    {
        private Dictionary<int, string> mScanTypeMap;

        #region "Properties"

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

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDirectoryPath"></param>
        /// <param name="datasetName"></param>
        public clsScanTypeFileCreator(string workDirectoryPath, string datasetName)
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

                using (var reader = new StreamReader(new FileStream(scanStatsExFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
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

                        linesRead += 1;
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
                            var dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, headerNames);

                            scanNumberColIndex = dctHeaderMapping["ScanNumber"];
                            collisionModeColIndex = dctHeaderMapping["Collision Mode"];
                            scanFilterColIndex = dctHeaderMapping["Scan Filter Text"];
                            continue;
                        }

                        // Parse out the values

                        if (clsGlobal.TryGetValueInt(dataCols, scanNumberColIndex, out var scanNumber))
                        {
                            var storeData = false;

                            if (clsGlobal.TryGetValue(dataCols, collisionModeColIndex, out var collisionMode))
                            {
                                storeData = true;
                            }
                            else
                            {
                                if (clsGlobal.TryGetValue(dataCols, scanFilterColIndex, out var filterText))
                                {
                                    filterText = dataCols[scanFilterColIndex];

                                    // Parse the filter text to determine scan type
                                    collisionMode = XRawFileIO.GetScanTypeNameFromFinniganScanFilterText(filterText);

                                    storeData = true;
                                }
                            }

                            if (storeData)
                            {
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
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in CacheScanTypeUsingScanStatsEx: " + ex.GetType().Name;
                ExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        /// <summary>
        /// create the ScanType file using the MASIC ScanStats file
        /// </summary>
        /// <returns></returns>
        public bool CreateScanTypeFile()
        {
            try
            {
                ErrorMessage = string.Empty;
                ExceptionDetails = string.Empty;

                ValidScanTypeLineCount = 0;

                var scanStatsFilePath = Path.Combine(WorkDir, DatasetName + "_ScanStats.txt");
                var scanStatsExFilePath = Path.Combine(WorkDir, DatasetName + "_ScanStatsEx.txt");

                if (!File.Exists(scanStatsFilePath))
                {
                    ErrorMessage = "_ScanStats.txt file not found: " + scanStatsFilePath;
                    return false;
                }

                var detailedScanTypesDefined = clsAnalysisResourcesMSGFDB.ValidateScanStatsFileHasDetailedScanTypes(scanStatsFilePath);

                // Open the input file
                using (var scanStatsReader = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    ScanTypeFilePath = Path.Combine(WorkDir, DatasetName + "_ScanType.txt");

                    // Create the scan type output file
                    using (var writer = new StreamWriter(new FileStream(ScanTypeFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        writer.WriteLine("ScanNumber\t" + "ScanTypeName\t" + "ScanType\t" + "ScanTime");

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

                            linesRead += 1;
                            var dataColumns = dataLine.Split('\t');

                            var firstColumnIsNumber = FirstColumnIsInteger(dataColumns);

                            if (linesRead == 1 && dataColumns.Length > 0 && !firstColumnIsNumber)
                            {
                                // This is a header line; define the column mapping

                                var headerNames = new List<string> {
                                    "ScanNumber",
                                    "ScanTime",
                                    "ScanType",
                                    "ScanTypeName"
                                };

                                // Keys in this dictionary are column names, values are the 0-based column index
                                var dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, headerNames);

                                scanNumberColIndex = dctHeaderMapping["ScanNumber"];
                                scanTimeColIndex = dctHeaderMapping["ScanTime"];
                                scanTypeColIndex = dctHeaderMapping["ScanType"];
                                scanTypeNameColIndex = dctHeaderMapping["ScanTypeName"];
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

                            if (!clsGlobal.TryGetValueInt(dataColumns, scanNumberColIndex, out var scanNumber))
                                continue;

                            if (!clsGlobal.TryGetValueInt(dataColumns, scanTypeColIndex, out var scanType))
                                continue;

                            var scanTypeName = string.Empty;
                            if (scanStatsExLoaded)
                            {
                                mScanTypeMap.TryGetValue(scanNumber, out scanTypeName);
                            }
                            else if (scanTypeNameColIndex >= 0)
                            {
                                clsGlobal.TryGetValue(dataColumns, scanTypeNameColIndex, out scanTypeName);
                            }

                            clsGlobal.TryGetValueFloat(dataColumns, scanTimeColIndex, out var scanTime);

                            writer.WriteLine(scanNumber + "\t" + scanTypeName + "\t" + scanType + "\t" + scanTime.ToString("0.0000"));

                            ValidScanTypeLineCount += 1;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in CreateScanTypeFile: " + ex.GetType().Name;
                ExceptionDetails = ex.Message;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Return true if the value in the first index of dataColumns is an Integer
        /// </summary>
        /// <param name="dataColumns"></param>
        /// <returns></returns>
        private bool FirstColumnIsInteger(IReadOnlyList<string> dataColumns)
        {
            return int.TryParse(dataColumns[0], out var _);
        }

    }
}