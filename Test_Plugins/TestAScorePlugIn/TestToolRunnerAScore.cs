using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_AScore_PlugIn;


namespace TestAScorePlugIn {

    class TestToolRunnerAScore {

        private Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" }
            };

        private Dictionary<string, string> mRunWorkflowJobParms = new Dictionary<string, string>() {
                //{ "DatasetNum", "Aggregation" },
                { "Job", "678425" },
                { "AScoreOperations",	"GetImprovResults" },
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

        public void TestRunAScore() {
            clsAnalysisResourcesAScore ascoreResourcer = new clsAnalysisResourcesAScore();
            clsAnalysisToolRunnerAScore ascoreToolRunner = new clsAnalysisToolRunnerAScore();

            MgrParamsStub mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub jobParams = new JobParamsStub(mRunWorkflowJobParms);
            StatusFileStub statusFile = new StatusFileStub();

            ascoreResourcer.Setup(mgrParams, jobParams);
            ascoreResourcer.GetResources();

            ascoreToolRunner.Setup(mgrParams, jobParams, statusFile);
            ascoreToolRunner.RunTool();

        }


    }
}
