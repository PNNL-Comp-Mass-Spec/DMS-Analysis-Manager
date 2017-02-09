
using System;
using System.IO;

namespace AnalysisManagerBase
{
    public class clsScanStatsGenerator
    {

        protected int mDebugLevel;
        protected string mErrorMessage;

        protected string mMSFileInfoScannerDLLPath;
        private MSFileInfoScannerInterfaces.iMSFileInfoScanner mMSFileInfoScanner;

        protected int mMSFileInfoScannerErrorCount;
        public string ErrorMessage => mErrorMessage;

        public int MSFileInfoScannerErrorCount => mMSFileInfoScannerErrorCount;

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

            mErrorMessage = string.Empty;
            ScanStart = 0;
            ScanEnd = 0;
        }

        /// <summary>
        /// Create the ScanStats file for the given dataset file
        /// </summary>
        /// <param name="strInputFilePath">Dataset file</param>
        /// <param name="strOutputFolderPath">Output folder</param>
        /// <returns></returns>
        /// <remarks>Will list DatasetID as 0 in the output file</remarks>
        public bool GenerateScanStatsFile(string strInputFilePath, string strOutputFolderPath)
        {
            return GenerateScanStatsFile(strInputFilePath, strOutputFolderPath, 0);
        }

        /// <summary>
        /// Create the ScanStats file for the given dataset file
        /// </summary>
        /// <param name="strInputFilePath">Dataset file</param>
        /// <param name="strOutputFolderPath">Output folder</param>
        /// <param name="intDatasetID">Dataset ID</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool GenerateScanStatsFile(string strInputFilePath, string strOutputFolderPath, int intDatasetID)
        {

            try
            {
                mMSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class					
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);

                mMSFileInfoScanner.ErrorEvent += mMSFileInfoScanner_ErrorEvent;
                mMSFileInfoScanner.MessageEvent += mMSFileInfoScanner_MessageEvent;

                mMSFileInfoScanner.CheckFileIntegrity = false;
                mMSFileInfoScanner.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.CreateScanStatsFile = true;
                mMSFileInfoScanner.SaveLCMS2DPlots = false;
                mMSFileInfoScanner.SaveTICAndBPIPlots = false;
                mMSFileInfoScanner.CheckCentroidingStatus = false;

                mMSFileInfoScanner.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.DatasetIDOverride = intDatasetID;

                if (ScanStart > 0 | ScanEnd > 0)
                {
                    mMSFileInfoScanner.ScanStart = ScanStart;
                    mMSFileInfoScanner.ScanEnd = ScanEnd;
                }

                var success = mMSFileInfoScanner.ProcessMSFileOrFolder(strInputFilePath, strOutputFolderPath);

                if (success)
                    return true;

                mErrorMessage = "Error generating ScanStats file using " + strInputFilePath;
                var strMsgAddnl = mMSFileInfoScanner.GetErrorMessage();

                if (!string.IsNullOrEmpty(strMsgAddnl))
                {
                    mErrorMessage = mErrorMessage + ": " + strMsgAddnl;
                }
                return false;

            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in GenerateScanStatsFile: " + ex.Message;
                return false;
            }

        }

        protected MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string strMSFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

            MSFileInfoScannerInterfaces.iMSFileInfoScanner objMSFileInfoScanner = null;

            try
            {
                if (!File.Exists(strMSFileInfoScannerDLLPath))
                {
                    var msg = "DLL not found: " + strMSFileInfoScannerDLLPath;
                    Console.WriteLine(msg);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                }
                else
                {
                    var obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath);
                    if (obj != null)
                    {
                        objMSFileInfoScanner = (MSFileInfoScannerInterfaces.iMSFileInfoScanner)obj;
                        var msg = "Loaded MSFileInfoScanner from " + strMSFileInfoScannerDLLPath;
                        if (mDebugLevel >= 2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                var msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
                Console.WriteLine(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }

            return objMSFileInfoScanner;
        }

        protected object LoadObject(string className, string strDLLFilePath)
        {
            try
            {
                // Dynamically load the specified class from strDLLFilePath
                var assem = System.Reflection.Assembly.LoadFrom(strDLLFilePath);
                var dllType = assem.GetType(className, false, true);
                var obj = Activator.CreateInstance(dllType);
                return obj;
            }
            catch (Exception ex)
            {
                var msg = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message;
                Console.WriteLine(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return null;
            }
        }

        protected void mMSFileInfoScanner_ErrorEvent(string Message)
        {
            mMSFileInfoScannerErrorCount += 1;
            var msg = "MSFileInfoScanner error: " + Message;
            Console.WriteLine(msg);
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }

        protected void mMSFileInfoScanner_MessageEvent(string Message)
        {
            if (mDebugLevel >= 3)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " + Message);
            }
        }

    }   

}