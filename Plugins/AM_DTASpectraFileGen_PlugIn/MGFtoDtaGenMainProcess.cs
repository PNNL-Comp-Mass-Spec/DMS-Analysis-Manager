// This class creates DTA files using a MGF file
//
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Started November 2005
// Re-worked in 2012 to use MascotGenericFileToDTA.dll

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;

namespace DTASpectraFileGen
{
    public class MGFtoDtaGenMainProcess : DtaGen
    {
        private System.Threading.Thread mDTAFileCreationThread;
        private MascotGenericFileToDTA.clsMGFtoDTA mMGFtoDTA;

        // DTA generation options
        private int mScanStart;
        private int mScanStop;
        private float mMWLower;

        public override void Setup(SpectraFileProcessorParams initParams, AnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            mDtaToolNameLoc = Path.Combine(Global.GetAppDirectoryPath(), "MascotGenericFileToDTA.dll");
        }

        public override ProcessStatus Start()
        {
            mStatus = ProcessStatus.SF_STARTING;

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                mResults = ProcessResults.SF_FAILURE;
                mStatus = ProcessStatus.SF_ERROR;
                return mStatus;
            }

            // Make the DTA files (the process runs in a separate thread)
            try
            {
                mDTAFileCreationThread = new System.Threading.Thread(MakeDTAFilesThreaded);
                mDTAFileCreationThread.Start();
                mStatus = ProcessStatus.SF_RUNNING;
            }
            catch (Exception ex)
            {
                mErrMsg = "Error calling MakeDTAFilesFromMGF: " + ex.Message;
                mStatus = ProcessStatus.SF_ERROR;
            }

            return mStatus;
        }

        private bool VerifyMGFFileExists(string workDir, string datasetName)
        {
            // Verifies a .mgf file exists in specified directory
            if (File.Exists(Path.Combine(workDir, datasetName + AnalysisResources.DOT_MGF_EXTENSION)))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "Data file " + datasetName + ".mgf not found in working directory";
            return false;
        }

        protected override bool InitSetup()
        {
            // Verifies all necessary files exist in the specified locations

            // Do tests specified in base class
            if (!base.InitSetup())
                return false;

            // MGF data file exists?
            if (!VerifyMGFFileExists(mWorkDir, mDatasetName))
                return false;    // Error message handled by VerifyMGFFileExists

            // If we got to here, there was no problem
            return true;
        }

        private void MakeDTAFilesThreaded()
        {
            mStatus = ProcessStatus.SF_RUNNING;

            if (!MakeDTAFilesFromMGF())
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
            }

            if (mStatus == ProcessStatus.SF_ABORTING)
            {
                mResults = ProcessResults.SF_ABORTED;
            }
            else if (mStatus == ProcessStatus.SF_ERROR)
            {
                mResults = ProcessResults.SF_FAILURE;
            }
            else
            {
                // Verify at least one DTA file was created
                if (!VerifyDtaCreation())
                {
                    mResults = ProcessResults.SF_NO_FILES_CREATED;
                }
                else
                {
                    mResults = ProcessResults.SF_SUCCESS;
                }

                mStatus = ProcessStatus.SF_COMPLETE;
            }
        }

        private bool MakeDTAFilesFromMGF()
        {
            // Get the parameters from the various setup files
            var mgfFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MGF_EXTENSION);
            mScanStart = mJobParams.GetJobParameter("ScanStart", 0);
            mScanStop = mJobParams.GetJobParameter("ScanStop", 0);
            mMWLower = mJobParams.GetJobParameter("MWStart", 0);
            // mMWUpper = mJobParams.GetJobParameter("MWStop", 0);

            // Run the MGF to DTA converter
            if (!ConvertMGFtoDTA(mgfFilePath, mWorkDir))
            {
                // Note that ConvertMGFtoDTA will have updated mErrMsg with the error message
                mResults = ProcessResults.SF_FAILURE;
                mStatus = ProcessStatus.SF_ERROR;
                return false;
            }

            if (mAbortRequested)
            {
                mStatus = ProcessStatus.SF_ABORTING;
            }

            // We got this far, everything must have worked
            return mStatus != ProcessStatus.SF_ABORTING && mStatus != ProcessStatus.SF_ERROR;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool ConvertMGFtoDTA(string inputFilePathFull, string outputFolderPath)
        {
            if (mDebugLevel > 0)
            {
                OnDebugEvent("Converting .MGF file to _DTA.txt");
            }

            mMGFtoDTA = new MascotGenericFileToDTA.clsMGFtoDTA
            {
                CreateIndividualDTAFiles = false,
                GuesstimateChargeForAllSpectra = mJobParams.GetJobParameter("GuesstimateChargeForAllSpectra", false),
                ForceChargeAddnForPredefined2PlusOr3Plus = mJobParams.GetJobParameter("ForceChargeAddnForPredefined2PlusOr3Plus", false),
                FilterSpectra = mJobParams.GetJobParameter("FilterSpectra", false),
                LogMessagesToFile = false,
                MaximumIonsPerSpectrum = mJobParams.GetJobParameter("MaximumIonsPerSpectrum", 0),
                ScanToExportMinimum = mScanStart,
                ScanToExportMaximum = mScanStop,
                MinimumParentIonMZ = mMWLower
            };
            mMGFtoDTA.ErrorEvent += MGFtoDTA_ErrorEvent;

            // Value between 0 and 100
            mMGFtoDTA.ThresholdIonPctForSingleCharge = mJobParams.GetJobParameter("ThresholdIonPctForSingleCharge",
                (int)mMGFtoDTA.ThresholdIonPctForSingleCharge);

            // Value between 0 and 100
            mMGFtoDTA.ThresholdIonPctForDoubleCharge = mJobParams.GetJobParameter("ThresholdIonPctForDoubleCharge",
                (int)mMGFtoDTA.ThresholdIonPctForDoubleCharge);

            var success = mMGFtoDTA.ProcessFile(inputFilePathFull, outputFolderPath);

            if (!success && string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = mMGFtoDTA.GetErrorMessage();
            }

            mSpectraFileCount = mMGFtoDTA.SpectraCountWritten;
            mProgress = 95;

            return success;
        }

        private bool VerifyDtaCreation()
        {
            // Verify that the _DTA.txt file was created and is not empty
            var cdtaFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.CDTA_EXTENSION));

            if (!cdtaFile.Exists)
            {
                mErrMsg = "_DTA.txt file not created";
                return false;
            }

            if (cdtaFile.Length == 0)
            {
                mErrMsg = "_DTA.txt file is empty";
                return false;
            }

            return true;
        }

        private void MGFtoDTA_ErrorEvent(string message)
        {
            if (string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = "MGFtoDTA_Error: " + message;
            }
            else if (mErrMsg.Length < 300)
            {
                mErrMsg += "; MGFtoDTA_Error: " + message;
            }
        }
    }
}
