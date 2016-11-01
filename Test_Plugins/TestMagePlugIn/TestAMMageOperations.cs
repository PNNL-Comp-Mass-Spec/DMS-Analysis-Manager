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

        public bool Test_ImportImprovClusterFiles() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"DataPackageID", "167"},
                {"DataPackageSourceFolderName", "ImportFiles"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", @"\\protoapps\DataPkgs\Public\2012\167_Test_Package_For_John"},
                {"OutputFolderName", "IPV201205180754_Auto678460"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase){
                {"workdir", mWorkDir}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "ImportIMPROVClusterFiles");
            return bSuccess;
        }
        /// <summary>
        /// 
        /// </summary>
        public bool Test_ImportDataPackageFiles() {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"DataPackageID", "159"},
                {"DataPackageSourceFolderName", "ImportFiles"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv"},
                {"OutputFolderName", "IPV201110210922_Auto678423"},
                {"StepInputFolderName", "step_1_APE"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase){
                {"workdir", mWorkDir}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "ImportDataPackageFiles");
            return bSuccess;
        }

        //--[Spectral Counting]------------------------------ 

        /// <summary>
        /// 
        /// </summary>
        public bool Test_ImportFDRTables()
        {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"MageFDRFiles", "Iteration_Table.txt,T_FDR_1percent.txt,T_FDR_0pt1percent.txt,T_FDR_5percent.txt,T_FDR_10percent.txt"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", mLocalDataFolder},
                {"OutputFolderName", "xx"},
                {"StepInputFolderName", ""},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"workdir", mWorkDir}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "GetFDRTables");
            return bSuccess;
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Test_GetFactors()
        {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"DataPackageID", "158"},
                {"FactorsSource", "FactorsFromDataPackageID"},
                {"ResultsBaseName", "Results"},
                {"transferFolderPath", mLocalDataFolder},
                {"OutputFolderName", "xx"},
                {"StepInputFolderName", ""},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
           };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "GetFactors");
            return bSuccess;
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Test_ExtractFromJobs()
        {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
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
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
           };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "ExtractFromJobs");
            return bSuccess;
        }

        //--[ITRAQ]------------------------------ 

        /// <summary>
        /// 
        /// </summary>
        public bool Test_ImportFirstHits()
        {

            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"DataPackageID", "161"},
                {"FirstHitsSource", "JobsFromDataPackageIDForTool"}, 
                {"ResultsBaseName", "Results"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "ImportFirstHits");
            return bSuccess;
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Test_ImportReporterIons()
        {
            Dictionary<string, string> mJobParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"DataPackageID", "161"},
                {"ReporterIonSource", "JobsFromDataPackageIDForTool"}, 
                {"ResultsBaseName", "Results"},
            };
            Dictionary<string, string> mMgrParms = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase) {
                {"workdir", mWorkDir},
                {"ConnectionString", mDMSConnectionString}
            };
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);
            bool bSuccess = TestRunOperation(ops, "ImportReporterIons");
            return bSuccess;
        }

        private bool TestRunOperation(MageAMOperations ops, string operationName) {
            String LogFileName = Path.Combine(mWorkDir, "Mage_Log");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            clsLogTools.ChangeLogFileName(LogFileName);

            bool bSuccess = ops.RunMageOperation(operationName);
            return bSuccess;
        }


    }
}
