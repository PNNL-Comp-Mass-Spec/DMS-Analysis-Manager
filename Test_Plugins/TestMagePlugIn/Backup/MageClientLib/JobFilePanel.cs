using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class JobFilePanel : UserControl {
                public event ActionCommand OnAction;

        public JobFilePanel() {
            InitializeComponent();
        }

        public string[] FileSelectors {
            get {
                return FileSelectorsCtl.Text.Split(';');
            }
        }

        private void button2_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_selected_job_files");
            }
        }

        private void button3_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_all_job_files");
            }
        }


    }
}
