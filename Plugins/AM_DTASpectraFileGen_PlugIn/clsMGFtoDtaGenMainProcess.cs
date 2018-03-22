// This class creates DTA files using a MGF file
//
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Started November 2005
// Re-worked in 2012 to use MascotGenericFileToDTA.dll

using System;
using System.IO;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    public class clsMGFtoDtaGenMainProcess : clsDtaGen
    {
        #region "Constants"

        #endregion

        #region "Module variables"

        private System.Threading.Thread m_thThread;
        private MascotGenericFileToDTA.clsMGFtoDTA mMGFtoDTA;

        // DTA generation options
        private int mScanStart;
        private int mScanStop;
        private float mMWLower;

        #endregion

        public override void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            m_DtaToolNameLoc = Path.Combine(clsGlobal.GetAppFolderPath(), "MascotGenericFileToDTA.dll");
        }

        public override ProcessStatus Start()
        {
            m_Status = ProcessStatus.SF_STARTING;

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                m_Results = ProcessResults.SF_FAILURE;
                m_Status = ProcessStatus.SF_ERROR;
                return m_Status;
            }

            // Make the DTA files (the process runs in a separate thread)
            try
            {
                m_thThread = new System.Threading.Thread(MakeDTAFilesThreaded);
                m_thThread.Start();
                m_Status = ProcessStatus.SF_RUNNING;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error calling MakeDTAFilesFromMGF: " + ex.Message;
                m_Status = ProcessStatus.SF_ERROR;
            }

            return m_Status;
        }

        private bool VerifyMGFFileExists(string WorkDir, string DSName)
        {
            // Verifies a .mgf file exists in specfied directory
            if (File.Exists(Path.Combine(WorkDir, DSName + clsAnalysisResources.DOT_MGF_EXTENSION)))
            {
                m_ErrMsg = "";
                return true;
            }

            m_ErrMsg = "Data file " + DSName + ".mgf not found in working directory";
            return false;
        }

        protected override bool InitSetup()
        {
            // Verifies all necessary files exist in the specified locations

            // Do tests specfied in base class
            if (!base.InitSetup())
                return false;

            // MGF data file exists?
            if (!VerifyMGFFileExists(m_WorkDir, m_Dataset))
                return false;    // Error message handled by VerifyMGFFileExists

            // If we got to here, there was no problem
            return true;
        }

        private void MakeDTAFilesThreaded()
        {
            m_Status = ProcessStatus.SF_RUNNING;
            if (!MakeDTAFilesFromMGF())
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
            }

            if (m_Status == ProcessStatus.SF_ABORTING)
            {
                m_Results = ProcessResults.SF_ABORTED;
            }
            else if (m_Status == ProcessStatus.SF_ERROR)
            {
                m_Results = ProcessResults.SF_FAILURE;
            }
            else
            {
                // Verify at least one dta file was created
                if (!VerifyDtaCreation())
                {
                    m_Results = ProcessResults.SF_NO_FILES_CREATED;
                }
                else
                {
                    m_Results = ProcessResults.SF_SUCCESS;
                }

                m_Status = ProcessStatus.SF_COMPLETE;
            }
        }

        private bool MakeDTAFilesFromMGF()
        {
            // Get the parameters from the various setup files
            var mgfFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MGF_EXTENSION);
            mScanStart = m_JobParams.GetJobParameter("ScanStart", 0);
            mScanStop = m_JobParams.GetJobParameter("ScanStop", 0);
            mMWLower = m_JobParams.GetJobParameter("MWStart", 0);
            // mMWUpper = m_JobParams.GetJobParameter("MWStop", 0);

            // Run the MGF to DTA converter
            if (!ConvertMGFtoDTA(mgfFilePath, m_WorkDir))
            {
                // Note that ConvertMGFtoDTA will have updated m_ErrMsg with the error message
                m_Results = ProcessResults.SF_FAILURE;
                m_Status = ProcessStatus.SF_ERROR;
                return false;
            }

            if (m_AbortRequested)
            {
                m_Status = ProcessStatus.SF_ABORTING;
            }

            // We got this far, everything must have worked
            return m_Status != ProcessStatus.SF_ABORTING && m_Status != ProcessStatus.SF_ERROR;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This functon is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertMGFtoDTA(string strInputFilePathFull, string strOutputFolderPath)
        {
            if (m_DebugLevel > 0)
            {
                OnDebugEvent("Converting .MGF file to _DTA.txt");
            }

            mMGFtoDTA = new MascotGenericFileToDTA.clsMGFtoDTA
            {
                CreateIndividualDTAFiles = false,
                GuesstimateChargeForAllSpectra = m_JobParams.GetJobParameter("GuesstimateChargeForAllSpectra", false),
                ForceChargeAddnForPredefined2PlusOr3Plus = m_JobParams.GetJobParameter("ForceChargeAddnForPredefined2PlusOr3Plus", false),
                FilterSpectra = m_JobParams.GetJobParameter("FilterSpectra", false),
                LogMessagesToFile = false,
                MaximumIonsPerSpectrum = m_JobParams.GetJobParameter("MaximumIonsPerSpectrum", 0),
                ScanToExportMinimum = mScanStart,
                ScanToExportMaximum = mScanStop,
                MinimumParentIonMZ = mMWLower
            };
            mMGFtoDTA.ErrorEvent += mMGFtoDTA_ErrorEvent;

            // Value between 0 and 100
            mMGFtoDTA.ThresholdIonPctForSingleCharge = m_JobParams.GetJobParameter("ThresholdIonPctForSingleCharge",
                (int)mMGFtoDTA.ThresholdIonPctForSingleCharge);

            // Value between 0 and 100
            mMGFtoDTA.ThresholdIonPctForDoubleCharge = m_JobParams.GetJobParameter("ThresholdIonPctForDoubleCharge",
                (int)mMGFtoDTA.ThresholdIonPctForDoubleCharge);

            var blnSuccess = mMGFtoDTA.ProcessFile(strInputFilePathFull, strOutputFolderPath);

            if (!blnSuccess && string.IsNullOrEmpty(m_ErrMsg))
            {
                m_ErrMsg = mMGFtoDTA.GetErrorMessage();
            }

            m_SpectraFileCount = mMGFtoDTA.SpectraCountWritten;
            m_Progress = 95;

            return blnSuccess;
        }

        private bool VerifyDtaCreation()
        {
            // Verify that the _DTA.txt file was created and is not empty
            var fiCDTAFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_DTA.txt"));

            if (!fiCDTAFile.Exists)
            {
                m_ErrMsg = "_DTA.txt file not created";
                return false;
            }

            if (fiCDTAFile.Length == 0)
            {
                m_ErrMsg = "_DTA.txt file is empty";
                return false;
            }

            return true;
        }

        private void mMGFtoDTA_ErrorEvent(string strMessage)
        {
            if (string.IsNullOrEmpty(m_ErrMsg))
            {
                m_ErrMsg = "MGFtoDTA_Error: " + strMessage;
            }
            else if (m_ErrMsg.Length < 300)
            {
                m_ErrMsg += "; MGFtoDTA_Error: " + strMessage;
            }
        }
    }
}
