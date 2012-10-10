using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {

    public partial class JobDataPackagePanel : UserControl {

        public event ActionCommand OnAction;

        #region Properties

        public string BaseSQL { get; set; }

        public string SQL { get { return GetSQL(); } }

        #endregion

        public JobDataPackagePanel() {
            InitializeComponent();
            BaseSQL = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs ";
        }

        private void button1_Click(object sender, EventArgs e) {
            if (OnAction != null) {
                OnAction("get_jobs");
            }
        }

        private string GetSQL() {
            string sql = BaseSQL;
            string glue = " WHERE ";
            if (DataPackageIDCtl.Text != "") {
                sql += glue + string.Format(" {0} = {1} ", "Data_Package_ID", DataPackageIDCtl.Text);
                glue = "AND ";
            }
            return sql;
        }

    }
}
