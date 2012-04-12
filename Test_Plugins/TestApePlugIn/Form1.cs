using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;
using AnalysisManager_Ape_PlugIn;

namespace TestApePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            TestToolRunnerApe ttr = new TestToolRunnerApe();

			AnalysisManagerBase.IJobParams.CloseOutType eResult;
			eResult = ttr.TestRunWorkflow();

			System.Windows.Forms.MessageBox.Show("Test complete: " + eResult.ToString());
        }

        private void Test_RunWorkflow_Click(object sender, EventArgs e)
        {
            TestAMApeOperations tpp = new TestAMApeOperations();
			bool bSuccess = tpp.Test_RunWorkflow();

			if (bSuccess)
				System.Windows.Forms.MessageBox.Show("Test complete");
			else
				System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_GetImprovResults_Click(object sender, EventArgs e)
        {
            TestAMApeOperations tpp = new TestAMApeOperations();
			bool bSuccess = tpp.Test_GetImprovResults();

			if (bSuccess)
				System.Windows.Forms.MessageBox.Show("Test complete");
			else
				System.Windows.Forms.MessageBox.Show("Test failed");
        }

        private void Test_GetQRollupResults_Click(object sender, EventArgs e)
        {
            TestAMApeOperations tpp = new TestAMApeOperations();
			bool bSuccess = tpp.Test_GetQRollupResults();

			if (bSuccess)
				System.Windows.Forms.MessageBox.Show("Test complete");
			else
				System.Windows.Forms.MessageBox.Show("Test failed");
        }


    }
}
