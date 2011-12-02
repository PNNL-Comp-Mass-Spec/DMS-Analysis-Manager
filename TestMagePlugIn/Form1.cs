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
            ttr.TestIMPROVJob();
        }

        private void Test_ImportDataPackageFiles_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_ImportDataPackageFiles();
        }

        private void Test_ImportFDRTables_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_ImportFDRTables();
        }

        private void Test_GetFactors_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_GetFactors();
        }

        private void Test_ImportFirstHits_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_ImportFirstHits();
        }

        private void Test_ImportReporterIons_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_ImportReporterIons();
        }

        private void Test_ExtractFromJobs_Click(object sender, EventArgs e) {
            TestAMMageOperations tpp = new TestAMMageOperations();
            tpp.Test_ExtractFromJobs();
        }

        private void Junk_Click(object sender, EventArgs e) {
            Junk junk = new Junk();
            junk.TestAlias();
        }



    }
}
