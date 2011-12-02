using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Mage filter module that adds and "Alias" column to data stream
    /// </summary>
    class ModuleAddAlias : ContentFilter {

        private int aliasColIdx;
        private int datqasetIDIdx;
        private int datasetIdx;
        private Dictionary<string, string> datasetAliases = new Dictionary<string, string>();

        public override void Prepare() {
            base.Prepare();
            OutputColumnList = "Dataset, Dataset_ID, Alias|+|text, *";
        }

        protected override void ColumnDefsFinished() {
            aliasColIdx = OutputColumnPos["Alias"];
            datqasetIDIdx = OutputColumnPos["Dataset_ID"];
            datasetIdx = OutputColumnPos["Dataset"];
        }

        protected override bool CheckFilter(ref object[] vals) {
            if (OutputColumnDefs != null) {
                object[] outRow = MapDataRow(vals);
                string dataset = outRow[datasetIdx].ToString();
                outRow[aliasColIdx] = MakeAlias(dataset);
                vals = outRow;
            }
            return true;
        }

        private string MakeAlias(string dataset) {
            string alias = dataset;
            if (datasetAliases.ContainsKey(dataset)) {
                alias = datasetAliases[dataset];
            } else {
                // find or make and remember a unique short label for this dataset
                for (int width = 8; width < dataset.Length; width++) {
                    string candidateAlias = dataset.Substring(0, width);
                    if (candidateAlias.Last() == '_') {
                        continue;
                    }
                    if (!datasetAliases.ContainsValue(candidateAlias)) {
                        alias = candidateAlias;
                        datasetAliases.Add(dataset, alias);
                        break;
                    }
                }
            }
            return alias;
        }

        public void SetFactors(SimpleSink factorsObj) {
            // FUTURE: This code is under construction
 /*           // set up column indexes
            int dIdx = factorsObj.ColumnIndex["Dataset"];
            int iIdx = factorsObj.ColumnIndex["Dataset_ID"];

            // get non-duplicate set of dataset names
            HashSet<string> uniqueNameSet = new HashSet<string>();
            foreach (Object[] row in factorsObj.Rows) {
                uniqueNameSet.Add(row[dIdx].ToString());
            }

            // get sortable list of unique dataset names
            List<string> nameList = new List<string>(uniqueNameSet);
            nameList.Sort();
            Dictionary<string, string> nameLookup = new Dictionary<string, string>();
            foreach (string name in nameList) {
                nameLookup.Add(name, "");
            }

            // build lookup of unique short names for each dataset using sorted list of names
            for(int currentNameIdx = 0; currentNameIdx < uniqueNameSet.Count - 1; currentNameIdx++) {
                int adjacentNameIdx = currentNameIdx + 1;
                int width = GetUniqueWidth(nameList[currentNameIdx], nameList[adjacentNameIdx]);
                if(nameLookup[nameList[currentNameIdx]].Length < width+1) {
                    nameLookup[nameList[currentNameIdx]] = nameList[currentNameIdx].Substring(0, width + 1);
                }
                if (nameLookup[nameList[adjacentNameIdx]].Length < width + 1) {
                    nameLookup[nameList[adjacentNameIdx]] = nameList[adjacentNameIdx].Substring(0, width + 1);
                }
            }

            foreach (string name in nameLookup.Keys) {
                Console.WriteLine(string.Format("{0} -> {1}", name, nameLookup[name]));
            }

            // get list of datasets indexed by dataset ID
            Dictionary<string, string> datasetNames = new Dictionary<string, string>();
            foreach (Object[] row in factorsObj.Rows) {
                string datasetID = row[iIdx].ToString();
                string dataset = row[dIdx].ToString();
                datasetNames.Add(datasetID, dataset);
            }
*/
        }

        // get the minimum number of initial characters necessary to distinquish s1 from s2
        private int GetUniqueWidth(string s1, string s2) {
            int width = 0;
            int len = (s1.Length > s2.Length)?s2.Length:s1.Length;
            for (int i = 0; i < len; i++) {
                if (s1.ElementAt(i) != s2.ElementAt(i)) {
                    width = i;
                    break;
                }
            }
            return width;
        }

    }
}
