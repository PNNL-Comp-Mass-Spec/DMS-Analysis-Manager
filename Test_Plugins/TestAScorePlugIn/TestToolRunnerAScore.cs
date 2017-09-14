using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_AScore_PlugIn;

namespace TestAScorePlugIn {

    class TestToolRunnerAScore {
        private const int DEBUG_LEVEL = 1;
        private const string WORK_DIR = @"C:\DMS_WorkDir";
        private const string STEP_TOOL_NAME = "TestToolRunnerApe";

        private readonly Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", DEBUG_LEVEL.ToString() },
                { "workdir",  WORK_DIR},
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
                { "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" },
                { "zipprogram", @"C:\PKWare\Pkzipc\Pkzipc.exe" },
                { "StepTool_ParamFileStoragePath_AScore", @"\\gigasax\DMS_Parameter_Files\AScore"}
            };

        private readonly Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "Job", "520598" },
                { "AScoreOperations",	"GetImprovResults" },
                { "transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\162_Test_DatapackegeJosh" },
                { "DataPackageSourceFolderName", "ImportFiles" },
                { "ResultsBaseName", "Results" },
                { "ExtractionType", "Sequest First Hits"},
                { "OutputFolderName", "PZX201005260748_Auto520598" },
                { "StepInputFolderName", "" },
                { "AScoreCIDParamFile", "itraq_ascore_cid.par" },
                { "AScoreETDParamFile", "" },
                { "AScoreHCDParamFile", "" },
                { "Step", "1" },
                { "StepTool", "ASCORE" },
                { "StepOutputFolderName", "Step_1_ASCORE" },
                { "DatasetNum", "Aggregation" },
                { "TargetJobFileList", "sequest:_syn.txt:copy,sequest:_fht.txt:copy,sequest:_dta.zip:copy,masic_finnigan:_reporterions.txt:copy,masic_finnigan:_ScanStatsEx.txt:copy" },
                { "DataPackageID", "162" },

                { "AScoreParamFilename", "parameterFileForGmax.xml" },
                { "AScoreSearchType", "sequest" }
            };

        public CloseOutType TestRunAScore()
        {
            var ascoreResourcer = new clsAnalysisResourcesAScore();
            var ascoreToolRunner = new clsAnalysisToolRunnerAScore();
            var summaryFile = new clsSummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mJobParms);
            var statusFile = new StatusFileStub();

            var myEMSLUtilities = new clsMyEMSLUtilities(DEBUG_LEVEL, WORK_DIR);

            ascoreResourcer.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, myEMSLUtilities);
            var eResult = ascoreResourcer.GetResources();

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                return eResult;

            ascoreToolRunner.Setup(STEP_TOOL_NAME, mgrParams, jobParams, statusFile, summaryFile, myEMSLUtilities);
            eResult = ascoreToolRunner.RunTool();

            return eResult;
        }


    }
}
