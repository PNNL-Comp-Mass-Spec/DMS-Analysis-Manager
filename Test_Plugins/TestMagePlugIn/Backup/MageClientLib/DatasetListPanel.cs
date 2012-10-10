using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class DatasetListPanel : UserControl {
                public event ActionCommand OnAction;
 
        public DatasetListPanel() {
            InitializeComponent();
        }

        public string DatasetName {
            get { return DatasetCtl.Text; }
        }

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_dataset_factor_summary");
            }
        }

        private void button2_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_factors");
            }
        }
        private void button4_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_factors_crosstab");
            }
        }

        private void button3_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_dataset_metadata");
            }
        }

    
    }
}
