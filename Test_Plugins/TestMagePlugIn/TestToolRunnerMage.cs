using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_Mage_PlugIn;


namespace TestMagePlugIn {

    class TestToolRunnerMage
    {

        private const int DEBUG_LEVEL = 1;
        private const string WORK_DIR = @"C:\DMS_WorkDir";

        private readonly Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", DEBUG_LEVEL.ToString() },
                { "workdir", WORK_DIR },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
                { "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" }

            };

        private readonly Dictionary<string, string> mIMPROVJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "DatasetNum", "Aggregation" },
                { "Job", "000" },
                { "Step", "1" },
                { "StepTool", "Mage" },

                { "MageOperations",	"ImportDataPackageFiles" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv" },
                { "DataPackageSourceFolderName", "ImportFiles" },
                { "ResultsBaseName", "Results" },
                { "OutputFolderName", "IPV201110210922_Auto678423" },
                { "StepInputFolderName", "step_1_APE" },
                { "StepOutputFolderName", "Step_2_Mage" },
                { "MageFDRFiles", "Iteration_Table.txt,T_FDR_1percent.txt,T_FDR_0pt1percent.txt,T_FDR_5percent.txt,T_FDR_10percent.txt" }
            };

        public CloseOutType TestIMPROVJob()
        {
            var mageResourcer = new clsAnalysisResourcesMage();
            var mageToolRunner = new clsAnalysisToolRunnerMage();
            var summaryFile = new clsSummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mIMPROVJobParms);
            var statusFile = new StatusFileStub();
            var myEmslUtils = new clsMyEMSLUtilities(DEBUG_LEVEL, WORK_DIR);

            mageResourcer.Setup(mgrParams, jobParams, statusFile, myEmslUtils);
            var eResult = mageResourcer.GetResources();

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                return eResult;

            mageToolRunner.Setup(mgrParams, jobParams, statusFile, summaryFile, myEmslUtils);
            eResult = mageToolRunner.RunTool();

            return eResult;

        }


    }
}
