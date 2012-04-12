using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_AScore_PlugIn;
using System.IO;

namespace TestAScorePlugIn {

    class TestAMAScore {

        //-------------------------------- PHOSPHO
        string mWorkDir;
        string mLogFilename;
        /// <summary>
        /// 
        /// </summary>
        public void Test_RunAScore()
        {
			Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

			Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "AScoreprogloc", @"C:\ToolsApplications\AScore\AScore_Console.exe" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "ConnectionString", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "zipprogram", @"C:\PKWare\Pkzipc\Pkzipc.exe" },
                { "MgrName", "Test_harness" },
                { "StepTool_ParamFileStoragePath_AScore", @"\\gigasax\DMS_Parameter_Files\AScore"}

            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            //Change the name of the log file for the local log file to the plugin log filename
            String LogFileName = Path.Combine(mWorkDir, "AScore_Log"); //m_WorkDir, "AScore_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            clsAScoreMage dvas = new clsAScoreMage(m_jobParams, m_mgrParams);
            dvas.Run();

            // Change the name of the log file back to the analysis manager log file
            LogFileName = mLogFilename;
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

        }

    }
}
