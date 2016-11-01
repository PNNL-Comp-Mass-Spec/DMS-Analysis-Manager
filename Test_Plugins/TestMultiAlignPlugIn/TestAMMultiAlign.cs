using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using AnalysisManager_MultiAlign_Aggregator_PlugIn;
using System.IO;

namespace TestMultiAlignPlugIn {

    class TestAMMultiAlign {

        //-------------------------------- MultiAlign
        string mWorkDir;
        string mLogFilename;

        /// <summary>
        /// Runs MultiAlign
        /// </summary>
        /// <returns>Error message; empty string if no error</returns>
        public string Test_RunMultiAlign()
        {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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

            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                { "debuglevel", "5" },
                { "MultiAlignProgLoc", @"C:\DMS_Programs\MultiAlign\" },
                { "workdir", @"C:\DMS_WorkDir" },
                { "logfilename", "AM_AnalysisManager_Log" },
                { "ConnectionString", "Data Source=gigasax;Initial Catalog=DMS5_T3;Integrated Security=SSPI;" },
                { "MgrName", "Test_harness" },
                { "StepTool_ParamFileStoragePath_MultiAlign", @"\\gigasax\DMS_Parameter_Files\MultiAlign\"}

            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            mWorkDir = m_mgrParams.GetParam("workdir");
            mLogFilename = m_mgrParams.GetParam("logfilename");

            //Change the name of the log file for the local log file to the plugin log filename
            String LogFileName = Path.Combine(mWorkDir, "MultiAlign_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            clsMultiAlignMage oMultiAlignMage = new clsMultiAlignMage(m_jobParams, m_mgrParams);
            string sMultiAlignConsolePath = m_mgrParams.GetParam("MultiAlignProgLoc");
            sMultiAlignConsolePath = System.IO.Path.Combine(sMultiAlignConsolePath, "MultiAlignConsole.exe");

            bool bSuccess = oMultiAlignMage.Run(sMultiAlignConsolePath);

            // Change the name of the log file back to the analysis manager log file
            LogFileName = mLogFilename;
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            if (bSuccess)
                return string.Empty;
            else
            {
                if (string.IsNullOrEmpty(oMultiAlignMage.Message))
                    return "Unknown error running Multialign";
                else
                    return "Error running Multialign: " + oMultiAlignMage.Message;

            }

        }
    }
}
