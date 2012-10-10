using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;

namespace LoadableModuleExample {

    [MageAttributes("FilterPanel", "Bogus", "Bogus Filter", "Parameters for Bogus Filter")]
    public partial class BogusFilterPanel : Form, IModuleParameters {

        Dictionary<string, string> mParameters = new Dictionary<string, string>();

        #region IModuleParameters Members

        public Dictionary<string, string> GetParameters() {
            mParameters["XCorrThreshold"] = XCorrThresholdCtl.Text;
            return mParameters;
        }

        public void SetParameters(Dictionary<string, string> paramList) {
            XCorrThresholdCtl.Text = paramList["XCorrThreshold"];
        }

        #endregion

        public BogusFilterPanel() {
            InitializeComponent();
        }
    }
}
