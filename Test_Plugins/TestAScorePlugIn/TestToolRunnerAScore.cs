using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_AScore_PlugIn;


namespace TestAScorePlugIn {

    class TestToolRunnerAScore {

		private Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
				{ "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
				{ "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" },
				{ "zipprogram", @"C:\PKWare\Pkzipc\Pkzipc.exe" },
                { "StepTool_ParamFileStoragePath_AScore", @"\\gigasax\DMS_Parameter_Files\AScore"}
            };
	
		private Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

		public IJobParams.CloseOutType TestRunAScore()
		{
            clsAnalysisResourcesAScore ascoreResourcer = new clsAnalysisResourcesAScore();
            clsAnalysisToolRunnerAScore ascoreToolRunner = new clsAnalysisToolRunnerAScore();
			clsSummaryFile summaryFile = new clsSummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
			IJobParams jobParams = new JobParamsStub(mJobParms);
            StatusFileStub statusFile = new StatusFileStub();
			IJobParams.CloseOutType eResult;

            ascoreResourcer.Setup(mgrParams, jobParams);
			eResult = ascoreResourcer.GetResources();

			if (eResult != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				return eResult;

            ascoreToolRunner.Setup(mgrParams, jobParams, statusFile, ref summaryFile);						
            eResult = ascoreToolRunner.RunTool();

			return eResult;
        }


    }
}
