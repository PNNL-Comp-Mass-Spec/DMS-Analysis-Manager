using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_Mage_PlugIn;


namespace TestMagePlugIn {

    class TestToolRunnerMage {

        private Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "MgrName", "Test_harness" }
            };

        private Dictionary<string, string> mIMPROVJobParms = new Dictionary<string, string>() {
                { "DatasetNum", "Aggregation" },
                { "Job", "000" },

                { "MageOperations",	"ImportDataPackageFiles" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv" },
                { "DataPackageSourceFolderName", "ImportFiles" },
                { "ResultsBaseName", "Results" },
                { "OutputFolderName", "IPV201110210922_Auto678423" },
                { "StepInputFolderName", "step_1_APE" },
                { "MageFDRFiles", "Iteration_Table.txt,T_FDR_1percent.txt,T_FDR_0pt1percent.txt,T_FDR_5percent.txt,T_FDR_10percent.txt" }
            };

        public void TestIMPROVJob() {
            clsAnalysisResourcesMage mageResourcer = new clsAnalysisResourcesMage();
            clsAnalysisToolRunnerMage mageToolRunner = new clsAnalysisToolRunnerMage();

            MgrParamsStub mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub jobParams = new JobParamsStub(mIMPROVJobParms);
            StatusFileStub statusFile = new StatusFileStub();

            mageResourcer.Setup(mgrParams, jobParams);
            mageResourcer.GetResources();

            mageToolRunner.Setup(mgrParams, jobParams, statusFile);
            mageToolRunner.RunTool();

        }


    }
}
