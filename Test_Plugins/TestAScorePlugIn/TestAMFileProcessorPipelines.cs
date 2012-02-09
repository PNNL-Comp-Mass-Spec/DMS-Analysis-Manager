using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManager_Mage_PlugIn;
using System.IO;

namespace TestMagePlugIn {

    class TestAMFileProcessorPipelines {

        private Dictionary<string, string> mJobParms = new Dictionary<string, string>() {
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
                {"workdir", @"C:\DMS_WorkDir"}
            };

        public void Test_ImportDataPackageFiles() {
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(new JobParamsStub(mJobParms), new MgrParamsStub(mMgrParms));
            string dataPackageStorageFolderRoot = mageObj.RequireJobParam("transferFolderPath");
            string inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, "");
        }

        public void Test_ImportFDRTables() {
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(new JobParamsStub(mJobParms), new MgrParamsStub(mMgrParms));
            string inputFolderPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            string inputfileList = mageObj.GetJobParam("MageFDRFiles");
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, inputfileList);
        }

    }
}
