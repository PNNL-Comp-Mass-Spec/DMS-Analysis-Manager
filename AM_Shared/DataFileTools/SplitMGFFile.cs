using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// This class splits a Mascot Generic File (mgf file) into multiple parts
    /// </summary>
    public class SplitMGFFile : EventNotifier
    {
        // Ignore Spelling: mgf

        /// <summary>
        /// Output file info
        /// </summary>
        private struct OutputFileInfo
        {
            /// <summary>
            /// Output file object
            /// </summary>
            public FileInfo OutputFile;

            /// <summary>
            /// Number of spectra written
            /// </summary>
            public int SpectraWritten;

            /// <summary>
            /// Writer
            /// </summary>
            public StreamWriter Writer;

            /// <summary>
            /// Part number
            /// </summary>
            public int PartNumber;
        }

        /// <summary>
        /// Scan number matcher
        /// </summary>
        private readonly Regex mExtractScan;

        /// <summary>
        ///  Constructor
        /// </summary>
        public SplitMGFFile()
        {
            mExtractScan = new Regex(@".+\.(\d+)\.\d+\.\d?", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Splits a Mascot Generic File (mgf file) into splitCount parts
        /// </summary>
        /// <remarks>Exceptions will be reported using event ErrorEvent</remarks>
        /// <param name="mgfFilePath">.mgf file path</param>
        /// <param name="splitCount">Number of parts; minimum is 2</param>
        /// <returns>True if success, false is an error</returns>
        public List<FileInfo> SplitMgfFile(string mgfFilePath, int splitCount)
        {
            return SplitMgfFile(mgfFilePath, splitCount, "_Part");
        }

        /// <summary>
        /// Splits a Mascot Generic File (mgf file) into splitCount parts
        /// </summary>
        /// <remarks>Exceptions will be reported using event ErrorEvent</remarks>
        /// <param name="mgfFilePath">.mgf file path</param>
        /// <param name="splitCount">Number of parts; minimum is 2</param>
        /// <param name="fileSuffix">Text to append to each split file (just before the file extension)</param>
        /// <returns>List of split files if success; empty list if an error</returns>
        public List<FileInfo> SplitMgfFile(string mgfFilePath, int splitCount, string fileSuffix)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(fileSuffix))
                {
                    fileSuffix = "_Part";
                }

                var mgfFile = new FileInfo(mgfFilePath);
                if (!mgfFile.Exists)
                {
                    OnErrorEvent("File not found: " + mgfFilePath);
                    return new List<FileInfo>();
                }

                if (mgfFile.Length == 0)
                {
                    OnErrorEvent("MGF file is empty: " + mgfFilePath);
                    return new List<FileInfo>();
                }

                if (splitCount < 2)
                    splitCount = 2;
                var lastProgress = DateTime.UtcNow;

                OnProgressUpdate("Splitting " + mgfFile.Name + " into " + splitCount + " parts", 0);

                var scanMapFile = Path.GetFileNameWithoutExtension(mgfFile.Name) + "_mgfScanMap.txt";

                if (mgfFile.DirectoryName == null)
                {
                    throw new DirectoryNotFoundException("Cannot determine the parent directory of " + mgfFile.FullName);
                }

                var scanMapFilePath = Path.Combine(mgfFile.DirectoryName, scanMapFile);

                var splitMgfFiles = new List<FileInfo>();

                using var mgfFileReader = new StreamReader(new FileStream(mgfFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var scanToPartMapWriter = new StreamWriter(new FileStream(scanMapFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Write the header to the map file
                scanToPartMapWriter.WriteLine("ScanNumber" + '\t' + "ScanIndexOriginal" + '\t' + "MgfFilePart" + '\t' + "ScanIndex");

                // Create the writers
                // Keys are each StreamWriter, values are the number of spectra written to the file
                var splitFileWriters = new Queue<OutputFileInfo>();

                for (var partNum = 1; partNum <= splitCount; partNum++)
                {
                    var msgFileName = Path.GetFileNameWithoutExtension(mgfFile.Name) + fileSuffix + partNum + ".mgf";

                    var outputFilePath = Path.Combine(mgfFile.DirectoryName, msgFileName);

                    var nextWriter = new OutputFileInfo
                    {
                        OutputFile = new FileInfo(outputFilePath),
                        SpectraWritten = 0,
                        PartNumber = partNum
                    };

                    splitMgfFiles.Add(nextWriter.OutputFile);
                    nextWriter.Writer =
                        new StreamWriter(new FileStream(nextWriter.OutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                    splitFileWriters.Enqueue(nextWriter);
                }

                long bytesRead = 0;
                var totalSpectraWritten = 0;

                var previousLine = string.Empty;
                while (true)
                {
                    var spectrumData = GetNextMGFSpectrum(mgfFileReader, ref previousLine, ref bytesRead, out var scanNumber);
                    if (spectrumData.Count == 0)
                    {
                        break;
                    }

                    var nextWriter = splitFileWriters.Dequeue();
                    foreach (var dataLine in spectrumData)
                    {
                        nextWriter.Writer.WriteLine(dataLine);
                    }

                    nextWriter.SpectraWritten++;
                    totalSpectraWritten++;

                    scanToPartMapWriter.WriteLine(scanNumber + '\t' + totalSpectraWritten + '\t' + nextWriter.PartNumber + '\t' +
                                                  nextWriter.SpectraWritten);

                    splitFileWriters.Enqueue(nextWriter);

                    if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 5)
                    {
                        lastProgress = DateTime.UtcNow;
                        var percentComplete = bytesRead / (float)mgfFileReader.BaseStream.Length * 100;
                        if (percentComplete > 100)
                            percentComplete = 100;
                        OnProgressUpdate("Splitting MGF file", (int)percentComplete);
                    }
                }

                // Close the writers
                // In addition, delete any output files that did not have any spectra written to them
                totalSpectraWritten = 0;

                while (splitFileWriters.Count > 0)
                {
                    var nextWriter = splitFileWriters.Dequeue();
                    nextWriter.Writer.Close();

                    if (nextWriter.SpectraWritten == 0)
                    {
                        nextWriter.OutputFile.Delete();
                        splitMgfFiles.Remove(nextWriter.OutputFile);
                    }
                    else
                    {
                        totalSpectraWritten += nextWriter.SpectraWritten;
                    }
                }

                if (totalSpectraWritten == 0)
                {
                    OnErrorEvent("No spectra were read from the source MGF file (BEGIN IONS not found)");
                    return new List<FileInfo>();
                }

                return splitMgfFiles;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SplitMgfFile: " + ex.Message);
                return new List<FileInfo>();
            }
        }

        private List<string> GetNextMGFSpectrum(StreamReader mgfFileReader, ref string previousLine, ref long bytesRead, out int scanNumber)
        {
            var spectrumFound = false;
            var spectrumData = new List<string>();
            scanNumber = 0;

            if (mgfFileReader.EndOfStream)
                return spectrumData;

            string dataLine;
            if (string.IsNullOrWhiteSpace(previousLine))
            {
                dataLine = mgfFileReader.ReadLine();
                bytesRead += 2;
            }
            else
            {
                dataLine = previousLine;
                previousLine = string.Empty;
            }

            while (true)
            {
                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    bytesRead += dataLine.Length;

                    if (dataLine.StartsWith("BEGIN IONS", StringComparison.OrdinalIgnoreCase))
                    {
                        if (spectrumFound)
                        {
                            // The previous spectrum was missing the END IONS line
                            // This is unexpected, but we'll allow it
                            previousLine = dataLine;

                            spectrumData.Add("END IONS");
                            return spectrumData;
                        }
                        spectrumFound = true;
                    }
                    else if (dataLine.StartsWith("TITLE", StringComparison.OrdinalIgnoreCase))
                    {
                        // Parse out the scan number
                        var reMatch = mExtractScan.Match(dataLine);
                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].Value, out scanNumber);
                        }
                    }

                    if (spectrumFound)
                    {
                        spectrumData.Add(dataLine);
                    }

                    if (dataLine.StartsWith("END IONS", StringComparison.OrdinalIgnoreCase))
                    {
                        return spectrumData;
                    }
                }

                if (mgfFileReader.EndOfStream)
                    break;

                dataLine = mgfFileReader.ReadLine();
                bytesRead += 2;
            }

            return spectrumData;
        }
    }
}
