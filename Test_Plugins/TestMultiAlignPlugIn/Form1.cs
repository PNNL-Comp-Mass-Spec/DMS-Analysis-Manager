using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;
using AnalysisManager_MultiAlign_Aggregator_PlugIn;

namespace TestMultiAlignPlugIn {
    public partial class Form1 : Form {
        public Form1() {
            InitializeComponent();
        }

        private void Test_Tool_Runner_Click(object sender, EventArgs e) {
            TestToolRunnerMultiAlign ttr = new TestToolRunnerMultiAlign();
            ttr.TestRunAScore();
        }

        private void Test_GetMultiAlignResults_Click(object sender, EventArgs e)
        {
            TestAMMultiAlign tpp = new TestAMMultiAlign();
            tpp.Test_RunMultiAlign();
        }


    }
}
