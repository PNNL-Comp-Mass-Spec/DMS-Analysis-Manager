using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class JobIDListPanel : UserControl {

        public event ActionCommand OnAction;

        #region Properties

        public string BaseSQL { get; set; }

        public string SQL { get { return GetSQL(); } }

        #endregion

        public JobIDListPanel() {
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
            if (JobListCtl.Text != "") {
                sql += glue + string.Format(" {0} IN ({1}) ", "Job", JobListCtl.Text);
                glue = "AND ";
            }
            return sql;
        }

        private void JobListCtl_TextChanged(object sender, EventArgs e) {
            string s = JobListCtl.Text;
            JobListCtl.Text = s.Replace(Environment.NewLine, ", ").TrimEnd(',', ' ');
        }

    }
}
