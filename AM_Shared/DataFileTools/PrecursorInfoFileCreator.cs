using System;
using System.Collections.Generic;
using System.IO;
using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;
using PRISM;

namespace AnalysisManagerBase.DataFileTools
{
    // ReSharper disable once CommentTypo

    /// <summary>
    /// This class creates a _PrecursorInfo.txt file using the dataset's _ScanStats.txt and _ScanStatsEx.txt files
    /// </summary>
    public class PrecursorInfoFileCreator : EventNotifier
    {
        // ReSharper disable once CommentTypo

        /// <summary>
        /// Create a _PrecursorInfo.txt file using the dataset's _ScanStats.txt and _ScanStatsEx.txt files
        /// </summary>
        /// <param name="workingDirectory">Working directory path</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>True if success, false if an error</returns>
        public bool CreatePrecursorInfoFile(string workingDirectory, string datasetName)
        {
            try
            {
                var scanStatsFile = new FileInfo(Path.Combine(workingDirectory, datasetName + ReaderFactory.SCAN_STATS_FILENAME_SUFFIX));

                var extendedScanStatsFile = new FileInfo(Path.Combine(workingDirectory, datasetName + ReaderFactory.EXTENDED_SCAN_STATS_FILENAME_SUFFIX));

                return CreatePrecursorInfoFile(datasetName, scanStatsFile, extendedScanStatsFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreatePrecursorInfoFile", ex);
                return false;
            }
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Create a _PrecursorInfo.txt file using the dataset's _ScanStats.txt and _ScanStatsEx.txt files
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="scanStatsFile">Scan stats file</param>
        /// <param name="extendedScanStatsFile">Extended scan stats file</param>
        /// <returns>True if success, false if an error</returns>
        public bool CreatePrecursorInfoFile(string datasetName, FileInfo scanStatsFile, FileInfo extendedScanStatsFile)
        {
            try
            {
                var parentDirectory = scanStatsFile.Directory;

                if (parentDirectory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of the ScanStats file: " + scanStatsFile.FullName);
                    return false;
                }

                if (!scanStatsFile.Exists)
                {
                    OnErrorEvent("ScanStats file not found: " + scanStatsFile.FullName);
                    return false;
                }

                if (!extendedScanStatsFile.Exists)
                {
                    OnErrorEvent("ScanStatsEx file not found: " + extendedScanStatsFile.FullName);
                    return false;
                }

                var scanStatsReader = new ScanStatsReader();
                RegisterEvents(scanStatsReader);

                var scanStats = scanStatsReader.ReadScanStatsData(scanStatsFile.FullName);

                if (scanStatsReader.ErrorMessage.Length > 0)
                {
                    OnErrorEvent("Error reading ScanStats data: " + scanStatsReader.ErrorMessage);
                    return false;
                }

                var extendedScanStatsReader = new ExtendedScanStatsReader();
                RegisterEvents(extendedScanStatsReader);

                var extendedScanStats = extendedScanStatsReader.ReadExtendedScanStatsData(extendedScanStatsFile.FullName);

                if (extendedScanStatsReader.ErrorMessage.Length > 0)
                {
                    OnErrorEvent("Error reading Extended ScanStats data: " + extendedScanStatsReader.ErrorMessage);
                    return false;
                }

                // Construct the path to the _PrecursorInfo.txt file
                var precursorInfoFilePath = Path.Combine(parentDirectory.FullName, string.Format("{0}{1}", datasetName, ReaderFactory.PRECURSOR_INFO_FILENAME_SUFFIX));
                using var writer = new StreamWriter(new FileStream(precursorInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                var columnData = new List<string>
                {
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.ScanNumber),
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.ScanTime),
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.ScanType),
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.ScanTypeName),
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.PrecursorMz),
                    PrecursorInfoFileReader.GetColumnNameByID(PrecursorInfoFileColumns.ScanFilterText)
                };

                writer.WriteLine(string.Join("\t", columnData));

                var warningCount = 0;
                var warningCountLogTarget = 20;

                foreach (var item in scanStats)
                {
                    var scanInfo = item.Value;

                    if (!extendedScanStats.TryGetValue(scanInfo.ScanNumber, out var extendedScanStatsInfo))
                    {
                        OnWarningEvent("Did not find scan {0} in the extended scan stats file; this is unexpected", scanInfo.ScanNumber);
                        continue;
                    }

                    if (scanInfo.ScanType == 1)
                        continue;

                    string warningMessage;
                    double parentIonMz;

                    if (string.IsNullOrWhiteSpace(extendedScanStatsInfo.ScanFilterText))
                    {
                        parentIonMz = 0;
                        warningMessage = string.Format("Unable to determine the parent ion m/z for scan {0} since the scan filter is empty", scanInfo.ScanNumber);
                    }
                    else
                    {
                        var success = ThermoRawFileReader.XRawFileIO.ExtractParentIonMZFromFilterText(extendedScanStatsInfo.ScanFilterText, out parentIonMz);

                        warningMessage = success
                            ? string.Empty
                            : string.Format("Unable to determine the parent ion m/z for scan {0} using {1}", scanInfo.ScanNumber, extendedScanStatsInfo.ScanFilterText);
                    }

                    if (warningMessage.Length > 0)
                    {
                        warningCount++;

                        if (warningCount < 10)
                        {
                            OnWarningEvent(warningMessage);
                        }
                        else if (warningCount == warningCountLogTarget)
                        {
                            warningCountLogTarget *= 2;
                            OnWarningEvent(warningMessage);
                        }

                        continue;
                    }

                    columnData.Clear();

                    columnData.Add(scanInfo.ScanNumber.ToString());
                    columnData.Add(StringUtilities.DblToString(scanInfo.ScanTimeMinutes, 4));
                    columnData.Add(scanInfo.ScanType.ToString());
                    columnData.Add(scanInfo.ScanTypeName);
                    columnData.Add(StringUtilities.DblToString(parentIonMz, 4));
                    columnData.Add(extendedScanStatsInfo.ScanFilterText);

                    writer.WriteLine(string.Join("\t", columnData));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreatePrecursorInfoFile", ex);
                return false;
            }
        }
    }
}
