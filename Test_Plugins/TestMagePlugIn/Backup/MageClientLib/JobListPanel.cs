using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class JobListPanel : UserControl {

        public event ActionCommand OnAction;

        #region Properties

        public string BaseSQL { get; set; }

        public string SQL { get { return GetSQL(); } }

        #endregion

        public JobListPanel() {
            InitializeComponent();
            BaseSQL = "SELECT * FROM V_Mage_Analysis_Jobs ";
        }

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_jobs");
            }
        }

        private string GetSQL() {
            string sql = BaseSQL;

            string glue = " WHERE ";
            if (DatasetCtl.Text != "") {
                sql += glue + string.Format(" {0} LIKE '%{1}%' ", "Dataset", DatasetCtl.Text);
                glue = "AND ";
            }
            if (ToolCtl.Text != "") {
                sql += glue + string.Format(" {0} LIKE '%{1}%' ", "Tool", ToolCtl.Text);
                glue = "AND ";
            }
            if (SettingsFileCtl.Text != "") {
                sql += glue + string.Format(" {0} LIKE '%{1}%' ", "Settings_File", SettingsFileCtl.Text);
                glue = "AND ";
            }
            if (ParameterFileCtl.Text != "") {
                sql += glue + string.Format(" {0} LIKE '%{1}%' ", "Parameter_File", ParameterFileCtl.Text);
                glue = "AND ";
            }
            return sql;
        }
    }
}
