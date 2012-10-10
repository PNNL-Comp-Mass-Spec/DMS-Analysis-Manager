using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class FileContentFilterPanel : UserControl {
        public event ActionCommand OnAction;

        #region Member Variables

        #endregion

        #region Properties

        public string OutputFolder {
            get { return OutputFolderCtl.Text; }
            set { OutputFolderCtl.Text = value; }
        }

        public string FilterSelection {
            get { return FilterSelectionCtl.Text; }
        }

        public string FilterSelectionItems {
            set { FilterSelectionCtl.Items.AddRange(value.Split('|')); }
        }

        #endregion


        public FileContentFilterPanel() {
            InitializeComponent();
        }

        private void button3_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("filter_selected_files");
            }
        }

        private void button1_Click(object sender, EventArgs e) {
            FolderBrowserDialog browse = new FolderBrowserDialog();
            browse.ShowNewFolderButton = true;
            browse.Description = "Please select a folder";
            browse.RootFolder = Environment.SpecialFolder.MyComputer; //Environment.SpecialFolder.DesktopDirectory;
            //            browse.SelectedPath = browse.SelectedPath;
            if (browse.ShowDialog() == DialogResult.OK) {
                OutputFolderCtl.Text = browse.SelectedPath;
            } 

        }
    }
}
