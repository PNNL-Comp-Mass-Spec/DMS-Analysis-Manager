using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;
using AnalysisManager_Mage_PlugIn;

namespace TestMagePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            TestToolRunnerMage ttr = new TestToolRunnerMage();

            AnalysisManagerBase.IJobParams.CloseOutType eResult;
            eResult = ttr.TestIMPROVJob();

            System.Windows.Forms.MessageBox.Show("Test complete: " + eResult.ToString());
        }

        private void Test_ImportDataPackageFiles_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ImportDataPackageFiles();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_ImportImprovClusterFiles_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ImportImprovClusterFiles();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_ImportFDRTables_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ImportFDRTables();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_GetFactors_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_GetFactors();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_ImportFirstHits_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ImportFirstHits();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_ImportReporterIons_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ImportReporterIons();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_ExtractFromJobs_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            bool bSuccess = tpp.Test_ExtractFromJobs();

            if (bSuccess)
                System.Windows.Forms.MessageBox.Show("Test complete");
            else
                System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Junk_Click(object sender, EventArgs e) {
            Junk junk = new Junk();
            junk.TestAlias();
        }

     }
}
