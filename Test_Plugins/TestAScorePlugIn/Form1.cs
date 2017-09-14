using System;
using System.Windows.Forms;

namespace TestAScorePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            var ttr = new TestToolRunnerAScore();

            var eResult = ttr.TestRunAScore();

            MessageBox.Show("Test complete: " + eResult);
        }

        private void Test_GetAScoreResults_Click(object sender, EventArgs e)
        {
            var tpp = new TestAMAScore();
            tpp.Test_RunAScore();
            MessageBox.Show("Test complete");
        }


    }
}
