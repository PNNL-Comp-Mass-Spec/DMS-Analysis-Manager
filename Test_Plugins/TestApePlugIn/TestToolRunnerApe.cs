using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_Ape_PlugIn;


namespace TestApePlugIn {

    class TestToolRunnerApe {

		private Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "connectionstring", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
				{ "brokerconnectionstring", "Data Source=gigasax;Initial Catalog=DMS_Pipeline_Test;Integrated Security=SSPI;" },
				{ "MgrCnfgDbConnectStr", "Data Source=ProteinSeqs;Initial Catalog=Manager_Control;Integrated Security=SSPI;" }
            };

		private Dictionary<string, string> mRunWorkflowJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                //{ "DatasetNum", "Aggregation" },
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

		public IJobParams.CloseOutType TestRunWorkflow()
		{
            clsAnalysisResourcesApe apeResourcer = new clsAnalysisResourcesApe();
            clsAnalysisToolRunnerApe apeToolRunner = new clsAnalysisToolRunnerApe();
			clsSummaryFile summaryFile = new clsSummaryFile();

            IMgrParams mgrParams = new MgrParamsStub(mMgrParms);
            IJobParams jobParams = new JobParamsStub(mRunWorkflowJobParms);
            StatusFileStub statusFile = new StatusFileStub();
			IJobParams.CloseOutType eResult;

            apeResourcer.Setup(ref mgrParams, ref jobParams);
			eResult = apeResourcer.GetResources();

			if (eResult != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				return eResult;

            apeToolRunner.Setup(mgrParams, jobParams, statusFile, ref summaryFile);
            eResult =  apeToolRunner.RunTool();

		    return eResult;

        }


    }
}
