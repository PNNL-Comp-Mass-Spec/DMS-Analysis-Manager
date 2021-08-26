using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManagerMultiAlign_AggregatorPlugIn;
using System.IO;

namespace TestMultiAlignPlugIn
{
    class TestAMMultiAlign
    {
        //-------------------------------- MultiAlign
        string mWorkDir;
        string mLogFilename;

        /// <summary>
        /// Runs MultiAlign
        /// </summary>
        /// <returns>Error message; empty string if no error</returns>
        public string Test_RunMultiAlign()
        {
            var mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

            var mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "5" },
                { "MultiAlignProgLoc", @"C:\DMS_Programs\MultiAlign\" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "ConnectionString", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "StepTool_ParamFileStoragePath_MultiAlign", @"\\gigasax\DMS_Parameter_Files\MultiAlign\"}

            };
            var m_mgrParams = new MgrParamsStub(mMgrParms);
            var m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            //Change the name of the log file for the local log file to the plugin log filename
            var LogFileName = Path.Combine(mWorkDir, "MultiAlign_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            LogTools.ChangeLogFileName(LogFileName);

            var statusFile = new StatusFileStub();

            var multiAlignMage = new MultiAlignMage(m_jobParams, m_mgrParams, statusFile);
            var multiAlignConsolePath = m_mgrParams.GetParam("MultiAlignProgLoc");
            multiAlignConsolePath = Path.Combine(multiAlignConsolePath, "MultiAlignConsole.exe");

            var success = multiAlignMage.Run(multiAlignConsolePath);

            // Change the name of the log file back to the analysis manager log file
            LogFileName = mLogFilename;
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            LogTools.ChangeLogFileName(LogFileName);

            if (success)
                return string.Empty;

            if (string.IsNullOrEmpty(multiAlignMage.Message))
                return "Unknown error running Multialign";

            return "Error running Multialign: " + multiAlignMage.Message;
        }
    }
}
