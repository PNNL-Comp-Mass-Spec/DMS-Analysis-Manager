using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    public class clsDeconToolsQCPlotsGenerator
    {
        private readonly int mDebugLevel;
        private string mErrorMessage;
        private readonly string mMSFileInfoScannerDLLPath;

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner mMSFileInfoScanner;
        private int mMSFileInfoScannerErrorCount;

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        public int MSFileInfoScannerErrorCount
        {
            get { return mMSFileInfoScannerErrorCount; }
        }

        public clsDeconToolsQCPlotsGenerator(string MSFileInfoScannerDLLPath, int DebugLevel)
        {
            mMSFileInfoScannerDLLPath = MSFileInfoScannerDLLPath;
            mDebugLevel = DebugLevel;

            mErrorMessage = string.Empty;
        }

        public bool CreateQCPlots(string strInputFilePath, string strOutputFolderPath)
        {
            bool blnSuccess = false;

            try
            {
                mMSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);
                mMSFileInfoScanner.CheckFileIntegrity = false;
                mMSFileInfoScanner.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.CreateScanStatsFile = false;
                mMSFileInfoScanner.SaveLCMS2DPlots = true;
                mMSFileInfoScanner.SaveTICAndBPIPlots = true;
                mMSFileInfoScanner.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.ErrorEvent += mMSFileInfoScanner_ErrorEvent;
                mMSFileInfoScanner.MessageEvent += mMSFileInfoScanner_MessageEvent;

                blnSuccess = mMSFileInfoScanner.ProcessMSFileOrFolder(strInputFilePath, strOutputFolderPath);

                if (!blnSuccess)
                {
                    mErrorMessage = "Error generating QC Plots using " + strInputFilePath;
                    string strMsgAddnl = mMSFileInfoScanner.GetErrorMessage();

                    if (!string.IsNullOrEmpty(strMsgAddnl))
                    {
                        mErrorMessage = mErrorMessage + ": " + strMsgAddnl;
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in CreateQCPlots: " + ex.Message;
                return false;
            }

            return blnSuccess;
        }

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string strMSFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

            MSFileInfoScannerInterfaces.iMSFileInfoScanner objMSFileInfoScanner = null;
            string msg = null;

            try
            {
                if (!File.Exists(strMSFileInfoScannerDLLPath))
                {
                    msg = "DLL not found: " + strMSFileInfoScannerDLLPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                }
                else
                {
                    var obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath);
                    if (obj != null)
                    {
                        objMSFileInfoScanner = (MSFileInfoScannerInterfaces.iMSFileInfoScanner) obj;
                        msg = "Loaded MSFileInfoScanner from " + strMSFileInfoScannerDLLPath;
                        if (mDebugLevel >= 2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }

            return objMSFileInfoScanner;
        }

        private object LoadObject(string className, string strDLLFilePath)
        {
            object obj = null;
            try
            {
                // Dynamically load the specified class from strDLLFilePath
                var assem = System.Reflection.Assembly.LoadFrom(strDLLFilePath);
                Type dllType = assem.GetType(className, false, true);
                obj = Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                string msg = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }
            return obj;
        }

        private void mMSFileInfoScanner_ErrorEvent(string Message)
        {
            mMSFileInfoScannerErrorCount += 1;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner error: " + Message);
        }

        private void mMSFileInfoScanner_MessageEvent(string Message)
        {
            if (mDebugLevel >= 3)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " + Message);
            }
        }
    }
}
