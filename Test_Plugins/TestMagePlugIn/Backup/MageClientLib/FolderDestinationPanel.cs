using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class FolderDestinationPanel : UserControl {
       public event ActionCommand OnAction;

        public FolderDestinationPanel() {
            InitializeComponent();
        }

        #region Member Variables

        #endregion

        #region Properties

        public string OutputFolder {
            get { return OutputFolderCtl.Text; }
            set { OutputFolderCtl.Text = value;  }
        }

        #endregion


        private void button1_Click(object sender, EventArgs e) {
            FolderBrowserDialog browse = new FolderBrowserDialog();
            browse.ShowNewFolderButton = true;
            browse.Description = "Please select a folder";
            browse.RootFolder = Environment.SpecialFolder.MyComputer; //Environment.SpecialFolder.MyComputer; //Environment.SpecialFolder.DesktopDirectory;
            //            browse.SelectedPath = browse.SelectedPath;
            if (browse.ShowDialog() == DialogResult.OK) {
                OutputFolderCtl.Text = browse.SelectedPath;
            } 
        }

    }
}
