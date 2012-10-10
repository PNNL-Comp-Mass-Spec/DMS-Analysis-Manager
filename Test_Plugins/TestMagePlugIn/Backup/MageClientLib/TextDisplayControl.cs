using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class TextDisplayControl : UserControl {

        #region Properties

        public string Contents {
            get { return MainTextCtl.Text; }
            set { MainTextCtl.Text = value; }
        }

        public string[] Lines {
            get { return MainTextCtl.Lines; }
        }

        #endregion

        public TextDisplayControl() {
            InitializeComponent();
        }
    }
}
