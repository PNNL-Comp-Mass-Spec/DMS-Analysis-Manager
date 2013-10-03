using System;
using System.Windows.Forms;

namespace TestAScorePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            TestToolRunnerAScore ttr = new TestToolRunnerAScore();

			AnalysisManagerBase.IJobParams.CloseOutType eResult;
			eResult = ttr.TestRunAScore();

			System.Windows.Forms.MessageBox.Show("Test complete: " + eResult.ToString());
        }

        private void Test_GetAScoreResults_Click(object sender, EventArgs e)
        {
            TestAMAScore tpp = new TestAMAScore();
            tpp.Test_RunAScore();
			System.Windows.Forms.MessageBox.Show("Test complete");
        }


    }
}
