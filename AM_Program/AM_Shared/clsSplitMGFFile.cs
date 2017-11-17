using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// This class splits a Mascot Generic File (mgf file) into multiple parts
    /// </summary>
    /// <remarks></remarks>
    public class clsSplitMGFFile : clsEventNotifier
    {
        /// <summary>
        /// Output file info
        /// </summary>
        protected struct udtOutputFileType
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
        protected readonly Regex mExtractScan;

        /// <summary>
        ///  Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsSplitMGFFile()
        {
            mExtractScan = new Regex(@".+\.(\d+)\.\d+\.\d?", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Splits a Mascot Generic File (mgf file) into splitCount parts
        /// </summary>
        /// <param name="mgfFilePath">.mgf file path</param>
        /// <param name="splitCount">Number of parts; minimum is 2</param>
        /// <returns>True if success, False is an error</returns>
        /// <remarks>Exceptions will be reported using event ErrorEvent</remarks>
        public List<FileInfo> SplitMgfFile(string mgfFilePath, int splitCount)
        {
            return SplitMgfFile(mgfFilePath, splitCount, "_Part");
        }

        /// <summary>
        /// Splits a Mascot Generic File (mgf file) into splitCount parts
        /// </summary>
        /// <param name="mgfFilePath">.mgf file path</param>
        /// <param name="splitCount">Number of parts; minimum is 2</param>
        /// <param name="fileSuffix">Text to append to each split file (just before the file extension)</param>
        /// <returns>List of split files if success; empty list if an error</returns>
        /// <remarks>Exceptions will be reported using event ErrorEvent</remarks>
        public List<FileInfo> SplitMgfFile(string mgfFilePath, int splitCount, string fileSuffix)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(fileSuffix))
                {
                    fileSuffix = "_Part";
                }

                var fiMgfFile = new FileInfo(mgfFilePath);
                if (!fiMgfFile.Exists)
                {
                    OnErrorEvent("File not found: " + mgfFilePath);
                    return new List<FileInfo>();
                }

                if (fiMgfFile.Length == 0)
                {
                    OnErrorEvent("MGF file is empty: " + mgfFilePath);
                    return new List<FileInfo>();
                }

                if (splitCount < 2)
                    splitCount = 2;
                var dtLastProgress = DateTime.UtcNow;

                OnProgressUpdate("Splitting " + fiMgfFile.Name + " into " + splitCount + " parts", 0);

                var scanMapFile = Path.GetFileNameWithoutExtension(fiMgfFile.Name) + "_mgfScanMap.txt";

                var scanMapFilePath = Path.Combine(fiMgfFile.DirectoryName, scanMapFile);

                var lstSplitMgfFiles = new List<FileInfo>();

                using (var srMgfFile = new StreamReader(new FileStream(fiMgfFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    using (var swScanToPartMapFile = new StreamWriter(new FileStream(scanMapFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {


                        // Write the header to the map file

                        {
                            swScanToPartMapFile.WriteLine("ScanNumber" + '\t' + "ScanIndexOriginal" + '\t' + "MgfFilePart" + '\t' + "ScanIndex");

                            // Create the writers
                            // Keys are each StreamWriter, values are the number of spectra written to the file
                            var swWriters = new Queue<udtOutputFileType>();

                            for (var partNum = 1; partNum <= splitCount; partNum++)
                            {
                                var msgFileName = Path.GetFileNameWithoutExtension(fiMgfFile.Name) + fileSuffix + partNum + ".mgf";

                                var outputFilePath = Path.Combine(fiMgfFile.DirectoryName, msgFileName);

                                var nextWriter = new udtOutputFileType
                                {
                                    OutputFile = new FileInfo(outputFilePath),
                                    SpectraWritten = 0,
                                    PartNumber = partNum
                                };

                                lstSplitMgfFiles.Add(nextWriter.OutputFile);
                                nextWriter.Writer = new StreamWriter(new FileStream(nextWriter.OutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                                swWriters.Enqueue(nextWriter);
                            }

                            long bytesRead = 0;
                            var totalSpectraWritten = 0;

                            var previousLine = string.Empty;
                            while (true)
                            {
                                var spectrumData = GetNextMGFSpectrum(srMgfFile, ref previousLine, ref bytesRead, out var scanNumber);
                                if (spectrumData.Count == 0)
                                {
                                    break;
                                }

                                var nextWriter = swWriters.Dequeue();
                                foreach (var dataLine in spectrumData)
                                {
                                    nextWriter.Writer.WriteLine(dataLine);
                                }

                                nextWriter.SpectraWritten += 1;
                                totalSpectraWritten += 1;

                                swScanToPartMapFile.WriteLine(scanNumber + '\t' + totalSpectraWritten + '\t' + nextWriter.PartNumber + '\t' + nextWriter.SpectraWritten);

                                swWriters.Enqueue(nextWriter);

                                if (DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 5)
                                {
                                    dtLastProgress = DateTime.UtcNow;
                                    var percentComplete = bytesRead / (float)srMgfFile.BaseStream.Length * 100;
                                    if (percentComplete > 100)
                                        percentComplete = 100;
                                    OnProgressUpdate("Splitting MGF file", (int)percentComplete);
                                }
                            }

                            // Close the writers
                            // In addition, delete any output files that did not have any spectra written to them
                            totalSpectraWritten = 0;

                            while (swWriters.Count > 0)
                            {
                                var nextWriter = swWriters.Dequeue();
                                nextWriter.Writer.Close();

                                if (nextWriter.SpectraWritten == 0)
                                {
                                    Thread.Sleep(50);
                                    nextWriter.OutputFile.Delete();
                                    lstSplitMgfFiles.Remove(nextWriter.OutputFile);
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
                        }


                    }
                }

                return lstSplitMgfFiles;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SplitMgfFile: " + ex.Message);
                return new List<FileInfo>();
            }


        }

        private List<string> GetNextMGFSpectrum(StreamReader srMgfFile, ref string previousLine, ref long bytesRead, out int scanNumber)
        {

            var spectrumFound = false;
            var spectrumData = new List<string>();
            scanNumber = 0;

            if (srMgfFile.EndOfStream)
                return spectrumData;

            string dataLine;
            if (string.IsNullOrWhiteSpace(previousLine))
            {
                dataLine = srMgfFile.ReadLine();
                bytesRead += 2;
            }
            else
            {
                dataLine = string.Copy(previousLine);
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

                if (srMgfFile.EndOfStream)
                    break;

                dataLine = srMgfFile.ReadLine();
                bytesRead += 2;
            }

            return spectrumData;

        }

    }
}
