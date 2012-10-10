using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class RawSQLPanel : UserControl {
        public event ActionCommand OnAction;

        public RawSQLPanel() {
            InitializeComponent();
        }

#region Properties

        public string ServerName { get { return ServerNameCtl.Text; } }

        public string DatabaseName { get { return DatabaseNameCtl.Text; } }

        public string SQL { get { return SqlCtl.Text; } }

#endregion

        private void button1_Click(object sender, EventArgs e) {
            // show
            if (OnAction != null) {
                OnAction("show_query_in_list_display");
            }
        }


    }
}
