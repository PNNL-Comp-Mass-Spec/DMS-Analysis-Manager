using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;

namespace MageClientLib {

    public partial class FileProcessingPanel : UserControl, IModuleParameters {

        public FileProcessingPanel() {
            InitializeComponent();
            this.button2.Enabled = false;
        }

        public event ActionCommand OnAction;

        #region Member Variables

        private Dictionary<string, Dictionary<string, string>> mParameters = new Dictionary<string, Dictionary<string, string>>();

        #endregion

        #region Properties

        // return class name for selected filter
        public string SelectedFilterClassName {
            get { return ModuleDiscovery.SelectedFilterClassName(FilterSelectionCtl.Text); }
        }

        #endregion

        #region IModuleParameters Members

        public Dictionary<string, string> GetParameters() {
            if (mParameters.ContainsKey(FilterSelectionCtl.Text)) {
                return mParameters[FilterSelectionCtl.Text];
            } else {
                return null;
            }
        }

        public void SetParameters(Dictionary<string, string> paramList) {
            mParameters[FilterSelectionCtl.Text] = paramList;
            // FUTURE: set individual controls from items in the list
        }

        #endregion


        private void button3_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                string command = (FilterSelectionCtl.Text == "Copy Files") ? "copy_selected_files" : "process_selected_files";
                OnAction(command);
            }
        }

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                string command = (FilterSelectionCtl.Text == "Copy Files") ? "copy_all_files" : "process_all_files";
                OnAction(command);
            }
        }

        #region Support functions

        // discover filter modules and their associated parameter panels
        // and set up the necessary internal properties, components, and variables
        public void SetupFilters() {
            foreach (string label in ModuleDiscovery.FilterLabels) {
                FilterSelectionCtl.Items.Add(label);
            }
        }

        // see if there is a parameter panel associated with the currently selected filter
        // and, if there is one, present it to the user and save its returned parameter values
        private void GetFilterParams() {
            string FilterLabel = FilterSelectionCtl.Text;
            string panelName = ModuleDiscovery.GetParameterPanelForFilter(FilterLabel);
            if (panelName != "") {
                // create an instance of the parameter panel
                Type modType = ModuleDiscovery.GetModuleTypeFromClassName(panelName);
                Form paramForm = (Form)Activator.CreateInstance(modType);

                // need reference that lets us access its parameters
                IModuleParameters iPar = (IModuleParameters)paramForm;

                // initialize its current parameter values
                if (mParameters.ContainsKey(FilterLabel)) {
                    iPar.SetParameters(mParameters[FilterLabel]);
                }
                // popup the parameter panel and save its parameter values
                if (paramForm.ShowDialog() == DialogResult.OK) {
                    mParameters[FilterLabel] = iPar.GetParameters();
                }
            }
        }


        #endregion

        // bring up parameter panel associated with currently selected filter
        // (if there is such a panel)
        private void button2_Click(object sender, EventArgs e) {
            GetFilterParams();
        }

        // enable and disable the button that brings up parameter panel 
        // that is associated with currently selected filter
        // according to whether or not such a panel exists
        private void FilterSelectionCtl_SelectedIndexChanged(object sender, EventArgs e) {
            string panelName = ModuleDiscovery.GetParameterPanelForFilter(FilterSelectionCtl.Text);
            if (panelName != "") {
                this.button2.Enabled = true;
            } else {
                this.button2.Enabled = false;
            }

        }

    }
}
