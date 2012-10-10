using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {
    public delegate void ActionCommand(string mode);

    public partial class StatusPanel : UserControl {
        public event ActionCommand OnAction;

        public StatusPanel() {
            InitializeComponent();
        }

        public void SetStatusMessage(string Message) {
            StatusMessageCtl.Text = Message;
        }

        private void CancelCtl_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("cancel_operation");
            }
        }
    }
}
