using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_Mage_PlugIn;
using System.IO;

namespace TestMagePlugIn {

    class TestAMMageOperations {

        private string mWorkDir = @"C:\DMS_WorkDir";
        private string mLocalDataFolder = @"C:\data";
        private string mDMSConnectionString = "Data Source=gigasax;Initial Catalog=DMS5_T3;integrated security=SSPI";

        //--[IMPROV]------------------------------ 

        /// <summary>
        /// 
        /// </summary>
        public void Test_ImportDataPackageFiles() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "159"},
                {"DataPackageSourceFolderName", "ImportFiles"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv"},
                {"OutputFolderName", "IPV201110210922_Auto678423"},
                {"StepInputFolderName", "step_1_APE"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "ImportDataPackageFiles");
        }

        //--[Spectral Counting]------------------------------ 

        /// <summary>
        /// 
        /// </summary>
        public void Test_ImportFDRTables() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"MageFDRFiles", "Iteration_Table.txt,T_FDR_1percent.txt,T_FDR_0pt1percent.txt,T_FDR_5percent.txt,T_FDR_10percent.txt"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", mLocalDataFolder},
                {"OutputFolderName", "xx"},
                {"StepInputFolderName", ""},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "GetFDRTables");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Test_GetFactors() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "158"},
                {"FactorsSource", "FactorsFromDataPackageID"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", mLocalDataFolder},
                {"OutputFolderName", "xx"},
                {"StepInputFolderName", ""},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
           };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "GetFactors");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Test_ExtractFromJobs() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "158"},
                {"ExtractionSource", "JobsFromDataPackageID"}, 
                {"ExtractionType", "Sequest First Hits" },
                {"KeepAllResults", "Yes" },
                {"ResultFilterSetID", "All Pass" },
                {"MSGFCutoff", "All Pass" },
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", mLocalDataFolder},
                {"OutputFolderName", "xx"},
                {"StepInputFolderName", ""},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
           };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "ExtractFromJobs");
        }

        //--[ITRAQ]------------------------------ 

        /// <summary>
        /// 
        /// </summary>
        public void Test_ImportFirstHits() {

            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "161"},
                {"FirstHitsSource", "JobsFromDataPackageIDForTool"}, 
                {"ResultsBaseName", "Results"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "ImportFirstHits");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Test_ImportReporterIons() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "161"},
                {"ReporterIonSource", "JobsFromDataPackageIDForTool"}, 
                {"ResultsBaseName", "Results"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            TestRunOperation(ops, "ImportReporterIons");
        }

        private void TestRunOperation(MageAMOperations ops, string operationName) {
            String LogFileName = Path.Combine(mWorkDir, "Mage_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);
            
            ops.RunMageOperation(operationName);
        }

    }
}
