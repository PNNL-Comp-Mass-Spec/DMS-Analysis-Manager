using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using MageClientLib;
using System.IO;

namespace MageManifest {

    public partial class InputFilePanel : UserControl {

        public event ActionCommand OnAction;

        #region Member Variables

        #endregion

        #region Properties

        public string FilePath {
            get { return FilePathCtl.Text; }
            set { FilePathCtl.Text = value; }
        }

        #endregion

        public InputFilePanel() {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("read_file_content");
            }
        }

        private void button2_Click(object sender, EventArgs e) {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK) {
                FilePathCtl.Text = openFileDialog1.FileName;
                //string dirName = Path.GetDirectoryName(openFileDialog1.FileName);
            }
        }
    }

}
