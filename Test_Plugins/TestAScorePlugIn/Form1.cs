using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;
using AnalysisManager_AScore_PlugIn;

namespace TestAScorePlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            TestToolRunnerAScore ttr = new TestToolRunnerAScore();
            ttr.TestRunAScore();
        }

        private void Test_GetAScoreResults_Click(object sender, EventArgs e)
        {
            TestAMAScore tpp = new TestAMAScore();
            tpp.Test_RunAScore();
        }


    }
}
