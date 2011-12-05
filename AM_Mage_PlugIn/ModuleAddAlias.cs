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

        /// <summary>
        /// Get alias for given dataset.
        /// Use existing lookup value, if one exists,
        /// otherwise create a suitable alias and save it to the lookup
        /// </summary>
        /// <param name="dataset">Dataset Name</param>
        /// <returns>Alias for dataset name</returns>
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

        /// <summary>
        /// Use contents of Mage simple sink object to build 
        /// a lookup table of dataset names to short label equivalents
        /// </summary>
        /// <param name="factorsObj">Mage simple sink object holding </param>
        public void SetupAliasLookup(SimpleSink factorsObj, bool stripPrefix = false) {
           // set up column indexes
           int dIdx = factorsObj.ColumnIndex["Dataset"];

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
            // set mode to use this lookup
            if (nameLookup.Count > 0) {
                datasetAliases = nameLookup;
                if (stripPrefix) StripOffCommonPrefix();
            }
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

        /// <summary>
        /// Go through alias lookup table and strip off 
        /// any prefix characters that are shared by all the aliases
        /// </summary>
        private void StripOffCommonPrefix() {
            // find width of common prefix for all alias values
            int width = 1;
            bool matched = true;
            while (matched) {
                string candidatePrefix = "";
                foreach (string alias in datasetAliases.Values) {
                    if (string.IsNullOrEmpty(candidatePrefix)) {
                        candidatePrefix = alias.Substring(0, width);
                    } else {
                        if(candidatePrefix != alias.Substring(0, width)) {
                            matched = false;
                            break;
                        }
                    }
                }
                if (matched) {
                    width++;
                } else {
                    break;
                }
            }
            // strip off common prefix (if there was one)
            if (width > 1) {
                int start = width - 1;
                string[] datasets = datasetAliases.Keys.ToArray();
                foreach (string dataset in datasets) {
                    string alias = datasetAliases[dataset];
                    string strippedAlias = alias.Substring(start);
                    datasetAliases[dataset] = strippedAlias;
                }
            }
        }

    }
}
