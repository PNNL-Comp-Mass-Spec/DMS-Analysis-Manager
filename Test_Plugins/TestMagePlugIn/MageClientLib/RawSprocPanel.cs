using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class RawSprocPanel : UserControl {

        public event ActionCommand OnAction;

        public RawSprocPanel() {
            InitializeComponent();
        }

#region Properties

        public string ServerName { get { return ServerNameCtl.Text; } }

        public string DatabaseName { get { return DatabaseNameCtl.Text; } }

        public string SprocName { get { return SprocNameCtl.Text; } }

        public Dictionary<string, string> SprocArgs {
            get {
                Dictionary<string, string> result = new Dictionary<string, string>();
                string parmText = ParamCtl.Text;
                string[] args = parmText.Split(',');
                foreach (string arg in args) {
                    string[] fields = arg.Split('=');
                    result.Add(fields[0].Trim(), fields[1].Trim());
                }
                return result;
            }
        }

#endregion

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("show_sproc_in_list_display");
            }
        }


    }
}
