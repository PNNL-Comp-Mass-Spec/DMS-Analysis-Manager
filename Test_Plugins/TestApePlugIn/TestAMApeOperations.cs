using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_Ape_PlugIn;
using System.IO;

namespace TestApePlugIn
{
    class TestAMApeOperations
    {
        //-------------------------------- IMPROV
        string mWorkDir;
        string mLogFilename;

        public bool Test_GetImprovResults()
        {
            var mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "Job", "678425" },
                { "ApeOperations",	"GetImprovResults" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv" },
                { "DataPackageSourceFolderName", "ImportFiles" },
                { "ResultsBaseName", "Results" },
                { "OutputFolderName", "IPV201110280919_Auto678425" },
                { "StepInputFolderName", "step_1_APE" },
                { "Step", "1" },
                { "StepTool", "APE" },
                { "StepOutputFolderName", "Step_1_APE" },
                { "ApeMTSServer", "Albert" },
                { "ApeMTSDatabase", "MT_Sea_Sediments_SBI_P590" },
//                { "ApeMTSDatabase", "MT_SeaSediments_ERB_P744" },
                { "ImprovMinPMTQuality", "1.0" },
                { "ImprovMSGFThreshold", "1" },
                { "DataPackageID", "167" }
            };
            var mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" }
            };
            var m_mgrParams = new MgrParamsStub(mMgrParms);
            var m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            var ops = new ApeAMOperations(m_jobParams, m_mgrParams);
            var bSuccess = TestRunOperation(ops, "GetImprovResults");

            return bSuccess;

        }

        //-------------------------------- Spectral Counting

        public bool Test_RunWorkflow()
        {
            var mJobParms = new Dictionary<string, string>() {
                {"ApeOperations", "RunWorkflow"},
                {"ApeWorkflowStart", "1"},
                {"ApeWorkflowEnd", "124"},
                {"DataPackageID", "158"},
                {"transferFolderPath", @"C:\Users\d3m480\Desktop\MAC Issues"}, //"\\protoapps\DataPkgs\Public\2011\158_MAC_Test_Data_Package"},
                {"AnalysisType", "SpectralCounting"},
                {"Job", "678426"},
                {"Step", "2"},
                {"StepTool", "APE"},
                {"InputFolderName", ""},
                {"OutputFolderName", "SCo201111041420_Auto678426"},
                {"SharedResultsFolders", ""},
                {"StepOutputFolderName", "Step_2_APE"},
                {"StepInputFolderName", "Step_1_Mage"},
                {"ApeWorkflowStepList", "Create Indexes"},//, Compile Datasets Calculate 10 Percent FDR"}, //1-10,11-68,69-123"}Create Indexes, Compile Datasets, Calculate 10 Percent FDR, Calculate 5 Percent FDR, Calculate 1 Percent FDR, Calculate 0pt1 Percent FDR
                {"ApeCompactDatabase", "True"},
                {"ApeWorkflowName", "20111121_APE_MSGF_CS_Tryp_SpectralCount_WF.xml"},
                {"ResultsBaseName", "Results"}
            };
            var mMgrParms = new Dictionary<string, string>() {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" }
            };
            var m_mgrParams = new MgrParamsStub(mMgrParms);
            var m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            var ops = new ApeAMOperations(m_jobParams, m_mgrParams);
            var bSuccess = TestRunOperation(ops, "RunWorkflow");

            return bSuccess;

        }

        public bool Test_GetQRollupResults()
        {
            var mJobParms = new Dictionary<string, string>() {
                { "Job", "678425" },
                { "ApeOperations",	"GetQRollupResults" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv" },
                { "DataPackageSourceFolderName", "ImportFiles" },
                { "ResultsBaseName", "Results" },
                { "OutputFolderName", "IPV201110280919_Auto678425" },
                { "StepInputFolderName", "step_1_APE" },
                { "Step", "1" },
                { "StepTool", "APE" },
                { "StepOutputFolderName", "Step_1_APE" },
                //{ "QRollupMTSServer", "Albert" },
                //{ "QRollupMTSDatabase", "MT_Synechococcus_PCC7002_ABPP_P751" },
                { "QRollupMTSServer", "Elmer" },
                { "QRollupMTSDatabase", "MT_Human_AF_Plasma_P769" },
                //MT_Human_AF_Plasma_P769
                { "ImprovMinPMTQuality", "1.0" },
                { "DataPackageID", "397" }
            };
            var mMgrParms = new Dictionary<string, string>() {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" }
            };
            var m_mgrParams = new MgrParamsStub(mMgrParms);
            var m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            var ops = new ApeAMOperations(m_jobParams, m_mgrParams);
            var bSuccess = TestRunOperation(ops, "GetQRollupResults");

            return bSuccess;

        }


        private bool TestRunOperation(ApeAMOperations ops, string operationName)
        {
            //Change the name of the log file for the local log file to the plugin log filename
            var LogFileName = Path.Combine(mWorkDir, "Ape_Log"); //m_WorkDir, "Ape_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            LogTools.ChangeLogFileName(LogFileName);

            var bSuccess = ops.RunApeOperations(operationName);

            // Change the name of the log file back to the analysis manager log file
            LogFileName = mLogFilename;
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            LogTools.ChangeLogFileName(LogFileName);

            return bSuccess;
        }

    }
}
