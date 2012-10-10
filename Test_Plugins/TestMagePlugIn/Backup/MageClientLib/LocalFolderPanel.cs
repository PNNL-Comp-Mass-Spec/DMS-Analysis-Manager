using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class LocalFolderPanel : UserControl {

        public event ActionCommand OnAction;

        #region Member Variables

        #endregion

        #region Properties

        public string FileNameFilter {
            get { return LocalFileNameFilterCtl.Text; }
            set { LocalFileNameFilterCtl.Text = value; }
        }

        public string Folder {
            get { return LocalDirectoryCtl.Text; }
            set { LocalDirectoryCtl.Text = value; }
        }

        #endregion


        public LocalFolderPanel() {
            InitializeComponent();
        }

        private void button2_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("process_local_folder");
            }
        }

        private void button1_Click(object sender, EventArgs e) {
            FolderBrowserDialog browse = new FolderBrowserDialog();
            browse.ShowNewFolderButton = true;
            browse.Description = "Please select a folder";
            browse.RootFolder = Environment.SpecialFolder.MyComputer; //Environment.SpecialFolder.DesktopDirectory;
//            browse.SelectedPath = browse.SelectedPath;
            if (browse.ShowDialog() == DialogResult.OK) {
                LocalDirectoryCtl.Text = browse.SelectedPath;
            } 
        }

    }
}
