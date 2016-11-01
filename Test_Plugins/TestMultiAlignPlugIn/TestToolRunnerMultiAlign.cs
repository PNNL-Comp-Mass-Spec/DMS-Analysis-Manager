using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_MultiAlign_Aggregator_PlugIn;


namespace TestMultiAlignPlugIn {

    class TestToolRunnerMultiAlign {

        private Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
                { "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" },
                { "MultiAlignProgLoc", @"C:\DMS_Programs\MultiAlign\" },
                { "StepTool_ParamFileStoragePath_MultiAlign", @"\\gigasax\DMS_Parameter_Files\MultiAlign\"}
            };
    
        private Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

        public IJobParams.CloseOutType TestRunMultiAlign()
        {
            clsAnalysisResourcesMultiAlignAggregator multialignResourcer = new clsAnalysisResourcesMultiAlignAggregator();
            clsAnalysisToolRunnerMultiAlignAggregator multialignToolRunner = new clsAnalysisToolRunnerMultiAlignAggregator();
            clsSummaryFile summaryFile = new clsSummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mJobParms);
            StatusFileStub statusFile = new StatusFileStub();
            IJobParams.CloseOutType eResult;

            multialignResourcer.Setup(mgrParams, jobParams);
            eResult = multialignResourcer.GetResources();

            if (eResult != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                return eResult;

            multialignToolRunner.Setup(mgrParams, jobParams, statusFile, ref summaryFile);
            eResult = multialignToolRunner.RunTool();

            return eResult;
        }


    }
}
