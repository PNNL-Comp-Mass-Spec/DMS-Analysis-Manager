using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_AScore_PlugIn;
using System.IO;

namespace TestAScorePlugIn {

    class TestAMAScore {

        //-------------------------------- PHOSPHO
        /// <summary>
        /// 
        /// </summary>
        public void Test_RunAScore()
        {
            var dctJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

            var dctMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "0" },
                { "AScoreprogloc", @"C:\ToolsApplications\AScore\AScore_Console.exe" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "ConnectionString", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "zipprogram", @"C:\PKWare\Pkzipc\Pkzipc.exe" },
                { "MgrName", "Test_harness" },
                { "StepTool_ParamFileStoragePath_AScore", @"\\gigasax\DMS_Parameter_Files\AScore"}

            };
            var mgrParams = new MgrParamsStub(dctMgrParms);
            var jobParams = new JobParamsStub(dctJobParms);
            
            string workDir = mgrParams.GetParam("workdir");
            string logFilenameSaved = mgrParams.GetParam("logfilename");

            var ionicZipTools = new clsIonicZipTools(1, workDir);

            //Change the name of the log file for the local log file to the plugin log filename
            String logFileName = Path.Combine(workDir, "AScore_Log");
            log4net.GlobalContext.Properties["LogName"] = logFileName;
            clsLogTools.ChangeLogFileName(logFileName);

            var ascoreMage = new clsAScoreMagePipeline(jobParams, mgrParams, ionicZipTools);
            ascoreMage.Run();

            // Change the name of the log file back to the analysis manager log file
            log4net.GlobalContext.Properties["LogName"] = logFilenameSaved;
            clsLogTools.ChangeLogFileName(logFilenameSaved);

        }

    }
}
