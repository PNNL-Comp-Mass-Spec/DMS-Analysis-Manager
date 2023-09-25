using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_Mage_PlugIn;
using System.IO;
using Mage;

namespace TestMagePlugIn {

    public class TestITRAQOperations {

        private Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
                {"DataPackageID", "161"},
                {"ExtractionSource", "JobsFromDataPackageIDForTool"},
 //               {"Tool", "MASIC_Finnigan"},

                {"transferFolderPath", @"\\protoapps\DataPkgs\Public\2011\159_MAC_Test_Data_Package_For_Improv"},
                {"DataPackageSourceFolderName", "ImportFiles"},
                {"ResultsBaseName", "Results"},
                {"OutputFolderName", "IPV201110210922_Auto678423"},
                {"StepInputFolderName", "step_1_APE"},
                {"MageFDRFiles", "Iteration_Table.txt,T_FDR_1percent.txt,T_FDR_0pt1percent.txt,T_FDR_5percent.txt,T_FDR_10percent.txt"}
 //               { "FileNameSelector", "" },
 //               { "FileSelectorMode", "RegEx" },
 //               { "IncludeFilesOrFolders", "File" },
 //               { "RecursiveSearch", "No" },
//                { "DBTableName", "" }
            };

        private Dictionary<string, string> mMgrParms = new Dictionary<string, string>() {
                {"workdir", @"C:\DMS_WorkDir"},
                {"ConnectionString", "Data Source=gigasax;Initial Catalog=DMS5_T3;integrated security=SSPI"}
            };

        public void TestReporterIonImport() {
            MgrParamsStub m_mgrParams = new MgrParamsStub(mMgrParms);
            JobParamsStub m_jobParams = new JobParamsStub(mJobParms);

            MageAMOperations ops = new MageAMOperations(m_jobParams, m_mgrParams);

            ops.ImportFirstHits();
            ops.ImportReporterIons();
        }

    }
}
