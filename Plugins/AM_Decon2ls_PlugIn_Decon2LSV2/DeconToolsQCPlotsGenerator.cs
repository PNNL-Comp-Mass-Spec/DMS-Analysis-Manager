using System;
using System.IO;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    public class DeconToolsQCPlotsGenerator : EventNotifier
    {
        // Ignore Spelling: Decon, Deisotoped

        private const int MAX_RUNTIME_HOURS = 5;

        private readonly int mDebugLevel;
        private readonly IJobParams mJobParams;
        private readonly string mMSFileInfoScannerDLLPath;

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner mMSFileInfoScanner;
        private string mInputFilePath;
        private string mOutputFolderPath;
        private bool mSuccess;

        public string ErrorMessage { get; private set; }

        public int MSFileInfoScannerErrorCount { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="msFileInfoScannerDLLPath"></param>
        /// <param name="debugLevel"></param>
        /// <param name="jobParams"></param>
        public DeconToolsQCPlotsGenerator(string msFileInfoScannerDLLPath, int debugLevel, IJobParams jobParams)
        {
            mMSFileInfoScannerDLLPath = msFileInfoScannerDLLPath;
            mDebugLevel = debugLevel;
            mJobParams = jobParams;

            ErrorMessage = string.Empty;
        }

        public bool CreateQCPlots(string inputFilePath, string outputFolderPath)
        {
            try
            {
                MSFileInfoScannerErrorCount = 0;

                // Initialize the MSFileScanner class
                mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath);
                RegisterEvents(mMSFileInfoScanner);

                mMSFileInfoScanner.Options.CheckFileIntegrity = false;
                mMSFileInfoScanner.Options.CreateDatasetInfoFile = false;
                mMSFileInfoScanner.Options.CreateScanStatsFiles = false;
                mMSFileInfoScanner.Options.CreateEmptyScanStatsFiles = false;
                mMSFileInfoScanner.Options.SaveLCMS2DPlots = true;
                mMSFileInfoScanner.Options.SaveTICAndBPIPlots = true;
                mMSFileInfoScanner.Options.UpdateDatasetStatsTextFile = false;
                mMSFileInfoScanner.Options.PlotWithPython = true;

                var maxChargeToPlot = mJobParams.GetJobParameter("MaxChargeToPlot", mMSFileInfoScanner.LCMS2DPlotOptions.MaxChargeToPlot);
                var maxMonoMassForDeisotopedPlot = mJobParams.GetJobParameter("MaxMonoMassForDeisotopedPlot", 0);

                mMSFileInfoScanner.LCMS2DPlotOptions.MaxChargeToPlot = maxChargeToPlot;

                if (maxMonoMassForDeisotopedPlot > 0)
                {
                    mMSFileInfoScanner.LCMS2DPlotOptions.MaxMonoMassForDeisotopedPlot = maxMonoMassForDeisotopedPlot;
                }

                mInputFilePath = inputFilePath;
                mOutputFolderPath = outputFolderPath;
                mSuccess = false;

                var thread = new System.Threading.Thread(ProcessMSFileOrFolderThread);

                thread.SetApartmentState(System.Threading.ApartmentState.STA);
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
                    ErrorMessage = "Error generating QC Plots using " + inputFilePath;
                    var msgAdditional = mMSFileInfoScanner.GetErrorMessage();

                    if (!string.IsNullOrEmpty(msgAdditional))
                    {
                        ErrorMessage = ErrorMessage + ": " + msgAdditional;
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception in CreateQCPlots: " + ex.Message;
                return false;
            }

            return mSuccess;
        }

        private MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string msFileInfoScannerDLLPath)
        {
            const string MsDataFileReaderClass = "MSFileInfoScanner.MSFileInfoScanner";

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

                        if (mDebugLevel >= 1)
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
            try
            {
                // Dynamically load the specified class from dllFilePath
                var assembly = System.Reflection.Assembly.LoadFrom(dllFilePath);
                var dllType = assembly.GetType(className, false, true);
                return Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception loading DLL " + dllFilePath, ex);
                return null;
            }
        }

        private void ProcessMSFileOrFolderThread()
        {
            mSuccess = mMSFileInfoScanner.ProcessMSFileOrDirectory(mInputFilePath, mOutputFolderPath);
        }
    }
}
