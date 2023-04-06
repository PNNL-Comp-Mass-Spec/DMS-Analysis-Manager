using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_Ape_PlugIn;

namespace TestApePlugIn
{
    class TestToolRunnerApe
    {
        private const int DEBUG_LEVEL = 1;
        private const string WORK_DIR = @"C:\DMS_WorkDir";
        private const string STEP_TOOL_NAME = "TestToolRunnerApe";

        private readonly Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", DEBUG_LEVEL.ToString() },
                { "workdir",  WORK_DIR},
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
                { "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" }
            };

        private readonly Dictionary<string, string> mRunWorkflowJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                //{ "DatasetName", "Aggregation" },
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
                { "ImprovMTSServer", "Albert" },
                { "ImprovMTSDatabase", "MT_SeaSediments_ERB_P744" },
                { "ImprovMinPMTQuality", "1.0" },
                { "DataPackageID", "159" }
            };

        public CloseOutType TestRunWorkflow()
        {
            var apeResourcer = new AnalysisResourcesApe();
            var apeToolRunner = new AnalysisToolRunnerApe();
            var summaryFile = new SummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mRunWorkflowJobParms);
            var statusFile = new StatusFileStub();

            var myEMSLUtilities = new MyEMSLUtilities(DEBUG_LEVEL, WORK_DIR);

            apeResourcer.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, myEMSLUtilities);
            var eResult = apeResourcer.GetResources();

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                return eResult;

            apeToolRunner.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, summaryFile, myEMSLUtilities);
            eResult =  apeToolRunner.RunTool();

            return eResult;

        }


    }
}
