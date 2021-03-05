using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManagerMultiAlign_AggregatorPlugIn;

namespace TestMultiAlignPlugIn
{
    class TestToolRunnerMultiAlign
    {
        private const int DEBUG_LEVEL = 1;
        private const string WORK_DIR = @"C:\DMS_WorkDir";
        private const string STEP_TOOL_NAME = "TestToolRunnerApe";

        private readonly Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", DEBUG_LEVEL.ToString() },
                { "workdir",  WORK_DIR},
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
                { "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" },
                { "MultiAlignProgLoc", @"C:\DMS_Programs\MultiAlign\" },
                { "StepTool_ParamFileStoragePath_MultiAlign", @"\\gigasax\DMS_Parameter_Files\MultiAlign\"}
            };

        private readonly Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "Job", "520598" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\157_Johns_Test_Package_for_MultiAlign" },
                { "ResultsBaseName", "Results" },
                { "OutputFolderName", "MAA201005260748_Auto520598" },
                { "StepInputFolderName", "" },
                { "Step", "1" },
                { "StepTool", "MultiAlign" },
                { "StepOutputFolderName", "Step_1_MULTIALIGN" },
                { "DatasetNum", "Aggregation" },
                { "DataPackageID", "157" },
                { "AMTDB", "MT_Human_Sarcopenia_P724" },
                { "AMTDBServer", "Elmer" },
                { "AlignmentDataset", "" },
                { "MultiAlignParamFilename", "cluster_16ppm0.014net0.3dt_matchamts_25ppm0.035net3msecdt-ims-stac.xml" },
                { "MultiAlignSearchType", "_LCMSFeatures.txt" }
        };

        public CloseOutType TestRunMultiAlign()
        {
            var multialignResourcer = new AnalysisResourcesMultiAlignAggregator();
            var multialignToolRunner = new AnalysisToolRunnerMultiAlignAggregator();
            var summaryFile = new SummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mJobParms);
            var statusFile = new StatusFileStub();

            var myEMSLUtilities = new MyEMSLUtilities(DEBUG_LEVEL, WORK_DIR);

            multialignResourcer.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, myEMSLUtilities);
            var eResult = multialignResourcer.GetResources();

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                return eResult;

            multialignToolRunner.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, summaryFile, myEMSLUtilities);
            eResult = multialignToolRunner.RunTool();

            return eResult;
        }


    }
}
