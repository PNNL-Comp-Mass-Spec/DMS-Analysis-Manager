using System;
using System.IO;
using System.Threading;
using PRISM;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    public class clsDeconToolsQCPlotsGenerator : clsEventNotifier
    {
        private const int MAX_RUNTIME_HOURS = 5;

        private readonly int mDebugLevel;
        private string mErrorMessage;
        private readonly string mMSFileInfoScannerDLLPath;

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner mMSFileInfoScanner;
        private int mMSFileInfoScannerErrorCount;

        private string mInputFilePath;
        private string mOutputFolderPath;
        private bool mSuccess;

        public string ErrorMessage => mErrorMessage;

        public int MSFileInfoScannerErrorCount => mMSFileInfoScannerErrorCount;

        public clsDeconToolsQCPlotsGenerator(string msFileInfoScannerDLLPath, int debugLevel)
        {
            mMSFileInfoScannerDLLPath = msFileInfoScannerDLLPath;
            mDebugLevel = debugLevel;

            mErrorMessage = string.Empty;
        }

        public bool CreateQCPlots(string inputFilePath, string outputFolderPath)
        {

            try
            {
                mMSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);
                RegisterEvents(mMSFileInfoScanner);

                mMSFileInfoScanner.CheckFileIntegrity = false;
                mMSFileInfoScanner.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.CreateScanStatsFile = false;
                mMSFileInfoScanner.SaveLCMS2DPlots = true;
                mMSFileInfoScanner.SaveTICAndBPIPlots = true;
                mMSFileInfoScanner.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.PlotWithPython = true;

                mInputFilePath = inputFilePath;
                mOutputFolderPath = outputFolderPath;
                mSuccess = false;

                var thread = new Thread(ProcessMSFileOrFolderThread);

                thread.SetApartmentState(ApartmentState.STA);
                thread.Start();

                var startTime = DateTime.UtcNow;

                while (true)
                {
                    // Wait for 2 seconds
                    thread.Join(2000);

                    // Check whether the thread is still running
                    if (!thread.IsAlive)
                        break;

                    // Check whether the thread has been running too long
                    if (!(DateTime.UtcNow.Subtract(startTime).TotalHours > MAX_RUNTIME_HOURS))
                        continue;

                    OnErrorEvent("MSFileInfoScanner has run for over " + MAX_RUNTIME_HOURS + " hours; aborting");

                    try
                    {
                        thread.Abort();
                    }
                    catch
                    {
                        // Ignore errors here;
                    }

                    break;
                }


                if (!mSuccess)
                {
                    mErrorMessage = "Error generating QC Plots using " + inputFilePath;
                    var msgAddnl = mMSFileInfoScanner.GetErrorMessage();

                    if (!string.IsNullOrEmpty(msgAddnl))
                    {
                        mErrorMessage = mErrorMessage + ": " + msgAddnl;
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in CreateQCPlots: " + ex.Message;
                return false;
            }

            return mSuccess;
        }

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string msFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

            MSFileInfoScannerInterfaces.iMSFileInfoScanner msFileInfoScanner = null;
            string msg;

            try
            {
                if (!File.Exists(msFileInfoScannerDLLPath))
                {
                    msg = "DLL not found: " + msFileInfoScannerDLLPath;
                    OnErrorEvent(msg);
                }
                else
                {
                    var obj = LoadObject(MsDataFileReaderClass, msFileInfoScannerDLLPath);
                    if (obj != null)
                    {
                        msFileInfoScanner = (MSFileInfoScannerInterfaces.iMSFileInfoScanner) obj;
                        msg = "Loaded MSFileInfoScanner from " + msFileInfoScannerDLLPath;
                        if (mDebugLevel >= 2)
                        {
                            OnDebugEvent(msg);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
                OnErrorEvent(msg, ex);
            }

            return msFileInfoScanner;
        }

        private object LoadObject(string className, string dllFilePath)
        {
            object obj = null;
            try
            {
                // Dynamically load the specified class from dllFilePath
                var assem = System.Reflection.Assembly.LoadFrom(dllFilePath);
                var dllType = assem.GetType(className, false, true);
                obj = Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                var msg = "Exception loading DLL " + dllFilePath + ": " + ex.Message;
                OnErrorEvent(msg, ex);
            }
            return obj;
        }

        private void ProcessMSFileOrFolderThread()
        {
            mSuccess = mMSFileInfoScanner.ProcessMSFileOrFolder(mInputFilePath, mOutputFolderPath);
        }

    }
}
