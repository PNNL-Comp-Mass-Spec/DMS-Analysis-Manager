
using PRISM;
using System;
using System.IO;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Scan stats generator
    /// </summary>
    public class clsScanStatsGenerator : EventNotifier
    {
        /// <summary>
        /// Debug level
        /// </summary>
        private readonly int mDebugLevel;

        /// <summary>
        /// MSFileInfoScanner DLL path
        /// </summary>
        private readonly string mMSFileInfoScannerDLLPath;

        /// <summary>
        /// MS File Info Scanner
        /// </summary>
        private MSFileInfoScannerInterfaces.iMSFileInfoScanner mMSFileInfoScanner;

        /// <summary>
        /// Most recent error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Number of errors reported by MSFileInfoScanner
        /// </summary>
        public int MSFileInfoScannerErrorCount { get; private set; }

        /// <summary>
        /// When ScanStart is > 0, will start processing at the specified scan number
        /// </summary>
        public int ScanStart { get; set; }

        /// <summary>
        /// When ScanEnd is > 0, will stop processing at the specified scan number
        /// </summary>
        public int ScanEnd { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="msFileInfoScannerDLLPath"></param>
        /// <param name="debugLevel"></param>
        public clsScanStatsGenerator(string msFileInfoScannerDLLPath, int debugLevel)
        {
            mMSFileInfoScannerDLLPath = msFileInfoScannerDLLPath;
            mDebugLevel = debugLevel;

            ErrorMessage = string.Empty;
            ScanStart = 0;
            ScanEnd = 0;
        }

        /// <summary>
        /// Create the ScanStats file for the given dataset file
        /// </summary>
        /// <param name="inputFilePath">Dataset file</param>
        /// <param name="outputDirectoryPath">Output directory</param>
        /// <remarks>Will list DatasetID as 0 in the output file</remarks>
        // ReSharper disable once UnusedMember.Global
        public bool GenerateScanStatsFile(string inputFilePath, string outputDirectoryPath)
        {
            return GenerateScanStatsFile(inputFilePath, outputDirectoryPath, 0);
        }

        /// <summary>
        /// Create the ScanStats file for the given dataset file
        /// </summary>
        /// <param name="inputFilePath">Dataset file</param>
        /// <param name="outputDirectoryPath">Output directory</param>
        /// <param name="datasetID">Dataset ID</param>
        /// <returns>True if successful, false if an error</returns>
        public bool GenerateScanStatsFile(string inputFilePath, string outputDirectoryPath, int datasetID)
        {
            try
            {
                MSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);
                RegisterEvents(mMSFileInfoScanner);

                mMSFileInfoScanner.ErrorEvent += MSFileInfoScanner_ErrorEvent;

                mMSFileInfoScanner.Options.CheckFileIntegrity = false;
                mMSFileInfoScanner.Options.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.Options.CreateScanStatsFile = true;
                mMSFileInfoScanner.Options.SaveLCMS2DPlots = false;
                mMSFileInfoScanner.Options.SaveTICAndBPIPlots = false;
                mMSFileInfoScanner.Options.CheckCentroidingStatus = false;

                mMSFileInfoScanner.Options.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.Options.DatasetID = datasetID;

                if (ScanStart > 0 || ScanEnd > 0)
                {
                    mMSFileInfoScanner.Options.ScanStart = ScanStart;
                    mMSFileInfoScanner.Options.ScanEnd = ScanEnd;
                }

                var success = mMSFileInfoScanner.ProcessMSFileOrDirectory(inputFilePath, outputDirectoryPath);

                if (success)
                    return true;

                ErrorMessage = "Error generating ScanStats file using " + inputFilePath;
                var msgAddnl = mMSFileInfoScanner.GetErrorMessage();

                if (!string.IsNullOrEmpty(msgAddnl))
                {
                    ErrorMessage = ErrorMessage + ": " + msgAddnl;
                }
                return false;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in GenerateScanStatsFile: " + ex.Message;
                return false;
            }
        }

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string msFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.MSFileInfoScanner";

            MSFileInfoScannerInterfaces.iMSFileInfoScanner msFileInfoScanner = null;

            try
            {
                if (!File.Exists(msFileInfoScannerDLLPath))
                {
                    var msg = "DLL not found: " + msFileInfoScannerDLLPath;
                    OnErrorEvent(msg);
                }
                else
                {
                    var newInstance = LoadObject(MsDataFileReaderClass, msFileInfoScannerDLLPath);
                    if (newInstance != null)
                    {
                        msFileInfoScanner = (MSFileInfoScannerInterfaces.iMSFileInfoScanner)newInstance;
                        var msg = "Loaded MSFileInfoScanner from " + msFileInfoScannerDLLPath;
                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent(msg);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception loading class " + MsDataFileReaderClass, ex);
            }

            return msFileInfoScanner;
        }

        private object LoadObject(string className, string dllFilePath)
        {
            try
            {
                // Dynamically load the specified class from dllFilePath
                var assembly = System.Reflection.Assembly.LoadFrom(dllFilePath);
                var dllType = assembly.GetType(className, false, true);
                var newInstance = Activator.CreateInstance(dllType);
                return newInstance;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception loading DLL " + dllFilePath, ex);
                return null;
            }
        }

        private void MSFileInfoScanner_ErrorEvent(string message, Exception ex)
        {
            ErrorMessage = message;
            MSFileInfoScannerErrorCount++;
        }
    }
}