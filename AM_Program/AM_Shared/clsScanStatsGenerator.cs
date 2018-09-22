
using System;
using System.IO;
using PRISM;

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
        /// <remarks></remarks>
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
        /// <param name="outputFolderPath">Output folder</param>
        /// <returns></returns>
        /// <remarks>Will list DatasetID as 0 in the output file</remarks>
        public bool GenerateScanStatsFile(string inputFilePath, string outputFolderPath)
        {
            return GenerateScanStatsFile(inputFilePath, outputFolderPath, 0);
        }

        /// <summary>
        /// Create the ScanStats file for the given dataset file
        /// </summary>
        /// <param name="inputFilePath">Dataset file</param>
        /// <param name="outputFolderPath">Output folder</param>
        /// <param name="datasetID">Dataset ID</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool GenerateScanStatsFile(string inputFilePath, string outputFolderPath, int datasetID)
        {

            try
            {
                MSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);
                RegisterEvents(mMSFileInfoScanner);

                mMSFileInfoScanner.ErrorEvent += MSFileInfoScanner_ErrorEvent;

                mMSFileInfoScanner.CheckFileIntegrity = false;
                mMSFileInfoScanner.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.CreateScanStatsFile = true;
                mMSFileInfoScanner.SaveLCMS2DPlots = false;
                mMSFileInfoScanner.SaveTICAndBPIPlots = false;
                mMSFileInfoScanner.CheckCentroidingStatus = false;

                mMSFileInfoScanner.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.DatasetIDOverride = datasetID;

                if (ScanStart > 0 || ScanEnd > 0)
                {
                    mMSFileInfoScanner.ScanStart = ScanStart;
                    mMSFileInfoScanner.ScanEnd = ScanEnd;
                }

                var success = mMSFileInfoScanner.ProcessMSFileOrFolder(inputFilePath, outputFolderPath);

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
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

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
                var assem = System.Reflection.Assembly.LoadFrom(dllFilePath);
                var dllType = assem.GetType(className, false, true);
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
            MSFileInfoScannerErrorCount += 1;
        }

    }

}