using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace AnalysisManagerMODPlusPlugin
{
    public class MODPlusResultsReader
    {
        /// <summary>
        /// Data lines for the current scan
        /// </summary>
        public List<string> CurrentScanData => mCurrentScanData;

        /// <summary>
        /// Currently available scan number and charge
        /// For example if scan 1000 and charge 2, will be 1000.02
        /// Or if scan 1000 and charge 4, will be 1000.04
        /// </summary>
        /// <remarks>-1 if no more scans remain</remarks>
        public double CurrentScanChargeCombo => mCurrentScanChargeCombo;

        public FileInfo ResultFile => mResultFile;

        public bool SpectrumAvailable => mSpectrumAvailable;

        private double mCurrentScanChargeCombo;

        private List<string> mCurrentScanData;

        private string mSavedLine;

        private bool mSpectrumAvailable;

        private readonly Regex mExtractChargeAndScan;

        private readonly StreamReader mReader;

        private readonly FileInfo mResultFile;

        /// <summary>
        /// Constructor
        /// </summary>
        public MODPlusResultsReader(string datasetName, FileInfo modPlusResultsFile)
        {
            mResultFile = modPlusResultsFile;

            // This RegEx is used to parse out the charge and scan number from the current spectrum
            // LineFormat (where \t is tab)
            // >>MGFFilePath \t MGFScanIndex \t ScanNumber \t ParentMZ \t Charge \t MGFScanHeader

            // Example lines:
            // >>E:\DMS_WorkDir\O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28_Part4.mgf	522	0	841.5054	2	O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28.4165.4165.
            // >>E:\DMS_WorkDir\O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28_Part4.mgf	524	0	1037.5855	2	O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28.4181.4181.2

            // Notice that some lines have MGFScanHeaders of
            //   Charge<Tab>Dataset.StartScan.EndScan.
            // while others have
            //   Charge<Tab>Dataset.StartScan.EndScan.Charge

            mExtractChargeAndScan = new Regex(@"\t(\d+)\t" + datasetName + @"\.(\d+)\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            mReader = new StreamReader(new FileStream(modPlusResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            mCurrentScanData = new List<string>();
            mSavedLine = string.Empty;

            ReadNextSpectrum();
        }

        public bool ReadNextSpectrum()
        {
            mSpectrumAvailable = false;
            mCurrentScanChargeCombo = -1;

            if (mReader.EndOfStream)
            {
                return false;
            }

            string dataLine;
            if (!string.IsNullOrEmpty(mSavedLine))
            {
                dataLine = mSavedLine;
                mSavedLine = string.Empty;
            }
            else
            {
                dataLine = mReader.ReadLine();
            }

            mCurrentScanData.Clear();

            var startScanFound = false;

            while (true)
            {
                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    if (dataLine.StartsWith(">>"))
                    {
                        if (startScanFound)
                        {
                            // This is the second time we've encountered ">>" in this function
                            // Cache the line so it can be used the next time ReadNextSpectrum is called
                            mSavedLine = dataLine;
                            mSpectrumAvailable = true;
                            return true;
                        }

                        startScanFound = true;

                        var reMatch = mExtractChargeAndScan.Match(dataLine);

                        mCurrentScanChargeCombo = 0;

                        var scan = 0;
                        var scanMatched = false;

                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].Value, out var charge);
                            if (int.TryParse(reMatch.Groups[2].Value, out scan))
                            {
                                mCurrentScanChargeCombo = scan + charge / 100.0;
                                scanMatched = true;
                            }
                            else
                            {
                                mCurrentScanChargeCombo = 0;
                            }
                        }

                        // Replace the file path in this line with a generic path of "E:\DMS_WorkDir\"
                        // In addition, update the scan number if it is 0
                        // And, remove "_Part#" from the filename

                        // For example, change from
                        // >>E:\DMS_WorkDir3\DatasetX_Part3.mgf	51	0	1481.7382	3	DatasetX.592.592.
                        // to
                        // >>E:\DMS_WorkDir\DatasetX.mgf	51	592	1481.7382	3	DatasetX.592.592.

                        try
                        {
                            var dataColumns = dataLine.Split('\t');
                            if (dataColumns.Length > 3)
                            {
                                var mgfFilePath = dataColumns[0].TrimStart('>');
                                var fiMgfFileLocal = new FileInfo(mgfFilePath);

                                if (fiMgfFileLocal.Name.Length > 0)
                                {
                                    // Reconstruct dataLine

                                    mgfFilePath = Path.Combine("E:\\DMS_WorkDir\\", fiMgfFileLocal.Name);

                                    dataLine = ">>" + mgfFilePath + "\t" + dataColumns[1] + "\t";

                                    if (scanMatched && dataColumns[2] == "0")
                                    {
                                        dataLine += scan;
                                    }
                                    else
                                    {
                                        dataLine += dataColumns[2];
                                    }

                                    // Add the remaining columns
                                    for (var colIndex = 3; colIndex <= dataColumns.Length - 1; colIndex++)
                                    {
                                        dataLine += "\t" + dataColumns[colIndex];
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // Text parsing error
                            // Do not reconstruct dataLine
                        }
                    }
                    mCurrentScanData.Add(dataLine);
                }

                if (mReader.EndOfStream)
                {
                    if (mCurrentScanData.Count > 0)
                    {
                        mSpectrumAvailable = true;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }

                dataLine = mReader.ReadLine();
            }
        }
    }
}
