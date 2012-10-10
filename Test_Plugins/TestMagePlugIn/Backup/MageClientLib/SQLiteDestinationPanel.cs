using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class SQLiteDestinationPanel : UserControl {
        public event ActionCommand OnAction;

        public SQLiteDestinationPanel() {
            InitializeComponent();
        }

        public string DatabaseName {
            get { return DatabaseNameCtl.Text;} set { DatabaseNameCtl.Text = value; }
        }

        public string TableName {
            get { return TableNameCtl.Text; }
            set { TableNameCtl.Text = value; } 
        }

        private void button3_Click(object sender, EventArgs e) {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();
            openFileDialog1.RestoreDirectory = true;
            if (openFileDialog1.ShowDialog() == DialogResult.OK) {
                DatabaseNameCtl.Text = openFileDialog1.FileName;
                //string dirName = Path.GetDirectoryName(openFileDialog1.FileName);
            }
        }

    }
}
