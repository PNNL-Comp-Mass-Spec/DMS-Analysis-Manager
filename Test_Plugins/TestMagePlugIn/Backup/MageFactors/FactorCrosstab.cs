using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;

namespace MageFactors {

    class FactorCrosstab : BaseModule {

        private int DatasetIdx = 0;
        private int DatasetIDIdx = 1;
        private int FactorIdx = 2;
        private int ValueIdx = 3;

        private Dictionary<string, string> datasets = new Dictionary<string, string>();
        private Dictionary<string, Dictionary<string, string>> factors = new Dictionary<string, Dictionary<string, string>>();

        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
//        public override event StatusMessageUpdated OnStatusMessageUpdated;

        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                string dataset = vals[DatasetIdx].ToString();
                string dataset_id = vals[DatasetIDIdx].ToString();
                string factor = vals[FactorIdx].ToString();
                string value = vals[ValueIdx].ToString();

                datasets[dataset_id] = dataset;
                if (!factors.ContainsKey(factor)) {
                    factors[factor] = new Dictionary<string, string>();
                }
                factors[factor].Add(dataset_id, value);
            } else {
                if (ColumnDefAvailable != null) {
                    ColumnDefAvailable(MakeColDef("Dataset", "15", "text"));
                    ColumnDefAvailable(MakeColDef("Dataset_ID", "15", "text"));
                    foreach (string fac in factors.Keys) {
                        ColumnDefAvailable(MakeColDef(fac, "15", "text"));
                    }
                    ColumnDefAvailable(null);
                }
                if (DataRowAvailable != null) {
                    foreach (string d_id in datasets.Keys) {
                        List<string> ol = new List<string>();
                        ol.Add(datasets[d_id]);
                        ol.Add(d_id);
                        foreach (string fac in factors.Keys) {
                            string s = "";
                            if (factors[fac].ContainsKey(d_id)) {
                                s = factors[fac][d_id];
                            }
                            ol.Add(s);
                        }
                        DataRowAvailable(ol.ToArray(), ref this.stop);
                    }
                    DataRowAvailable(null, ref this.stop);
                }
            }
        }

    }
}
