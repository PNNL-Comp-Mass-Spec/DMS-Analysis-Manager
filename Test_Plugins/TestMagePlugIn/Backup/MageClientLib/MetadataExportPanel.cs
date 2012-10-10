using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class MetadataExportPanel : UserControl {
        public event ActionCommand OnAction;

        public MetadataExportPanel() {
            InitializeComponent();
        }

        #region Properties

        public string OutputTableName { get { return OutputTableNameCtl.Text; } }

        public string OutputFilePath { get { return OutputFilePathCtl.Text; } }

        public string OutputFormat { get { return FormatTypeCtl.Text; } }

        public string OutputType { get { return OutputTypeCtl.Text; } }

        #endregion

        private void button2_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("save_metadata");
            }
        }

        private void FormatTypeCtl_SelectedIndexChanged(object sender, EventArgs e) {
            // FUTURE: use this to change output file name?
        }

        private void button1_Click(object sender, EventArgs e) {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.RestoreDirectory = true;
            openFileDialog1.CheckFileExists = false;
            if (openFileDialog1.ShowDialog() == DialogResult.OK) {
                OutputFilePathCtl.Text = openFileDialog1.FileName;
                //string dirName = Path.GetDirectoryName(openFileDialog1.FileName);
            }

        }
    }
}
