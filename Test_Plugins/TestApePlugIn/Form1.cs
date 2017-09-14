using System;
using System.Windows.Forms;

namespace TestApePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            var ttr = new TestToolRunnerApe();

            var eResult = ttr.TestRunWorkflow();

            MessageBox.Show("Test complete: " + eResult);
        }

        private void Test_RunWorkflow_Click(object sender, EventArgs e)
        {
            var tpp = new TestAMApeOperations();
            var bSuccess = tpp.Test_RunWorkflow();

            if (bSuccess)
                MessageBox.Show("Test complete");
            else
                MessageBox.Show("Test failed");
        }

        private void Test_GetImprovResults_Click(object sender, EventArgs e)
        {
            var tpp = new TestAMApeOperations();
            var bSuccess = tpp.Test_GetImprovResults();

            if (bSuccess)
                MessageBox.Show("Test complete");
            else
                MessageBox.Show("Test failed");
        }

        private void Test_GetQRollupResults_Click(object sender, EventArgs e)
        {
            var tpp = new TestAMApeOperations();
            var bSuccess = tpp.Test_GetQRollupResults();

            if (bSuccess)
                MessageBox.Show("Test complete");
            else
                MessageBox.Show("Test failed");
        }


    }
}
