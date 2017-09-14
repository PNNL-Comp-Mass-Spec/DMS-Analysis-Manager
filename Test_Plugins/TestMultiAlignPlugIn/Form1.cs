using System;
using System.Windows.Forms;
using AnalysisManagerBase;

namespace TestMultiAlignPlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            var ttr = new TestToolRunnerMultiAlign();

            var eResult = ttr.TestRunMultiAlign();

            System.Windows.Forms.MessageBox.Show("Test complete: " + eResult);
        }

        private void Test_GetMultiAlignResults_Click(object sender, EventArgs e)
        {
            var tpp = new TestAMMultiAlign();
            var sErrorMessage = tpp.Test_RunMultiAlign();

            if (string.IsNullOrEmpty(sErrorMessage))
                MessageBox.Show("Test complete");
            else
                MessageBox.Show("Test failed: " + sErrorMessage);
        }


    }
}
