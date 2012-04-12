using System;
using System.Collections.Generic;
using System.Linq;
using Mage;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Mage filter module that adds and "Alias" column to data stream
    /// </summary>
    public class ModuleAddAlias : ContentFilter {

        // Column indexes
        private int aliasColIdx;
        private int datasetIdx;

        // lookup table for dataset aliases
        private Dictionary<string, string> datasetAliases = new Dictionary<string, string>();

        public override void Prepare() {
            base.Prepare();
            // make sure alias column is in output (in case it isn't in input)
            OutputColumnList = "Dataset, Dataset_ID, Alias|+|text, *";
         }

        protected override void ColumnDefsFinished() {
            aliasColIdx = OutputColumnPos["Alias"];
            datasetIdx = OutputColumnPos["Dataset"];
        }

        // handle a data row - make sure alias field has an appropriate value
        protected override bool CheckFilter(ref object[] vals) {
            if (OutputColumnDefs != null) {
                object[] outRow = MapDataRow(vals);
                string dataset = outRow[datasetIdx].ToString();
                outRow[aliasColIdx] = LookupAlias(dataset);
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
        private string LookupAlias(string dataset) {
            string alias = dataset;
            if (datasetAliases.ContainsKey(dataset)) {
                alias = datasetAliases[dataset];
            } else {
                alias = MakeAlias(dataset);
            }
            return alias;
        }

        /// <summary>
        ///  make and remember a unique short label for this dataset
        /// </summary>
        /// <param name="dataset"></param>
        /// <returns></returns>
        private string MakeAlias(string dataset) {
            string alias = dataset;
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
            return alias;
        }

        /// <summary>
        /// Use contents of Mage simple sink object to build 
        /// a lookup table of dataset names to short label equivalents
        /// </summary>
        /// <param name="factorsObj">Mage simple sink object holding </param>
        public void SetupAliasLookup(SimpleSink factorsObj, bool PadAliases = true, bool StripPrefix = false) {

            // get non-duplicate set of dataset names
            HashSet<string> uniqueNameSet = new HashSet<string>();
            int dIdx = factorsObj.ColumnIndex["Dataset"];
            foreach (Object[] row in factorsObj.Rows) {
                uniqueNameSet.Add(row[dIdx].ToString());
            }

            // build alias lookup table for names
            Dictionary<string, string> nameLookup = BuildAliasLookupTable(uniqueNameSet, PadAliases);

            // set up to use the lookup table
            if (nameLookup.Count > 0) {
                if (StripPrefix) StripOffCommonPrefix(nameLookup);
                datasetAliases = nameLookup;
            }
        }

        /// <summary>
        /// Builds a lookup table of aliases given a unique set of names
        /// </summary>
        /// <param name="uniqueNameSet">unique set of names to build alias lookup from</param>
        /// <returns>Lookup table of aliases for names, indexed by name</returns>
        public static Dictionary<string, string> BuildAliasLookupTable(HashSet<string> uniqueNameSet, bool padAliasWidth = true) {

            // get sorted list of unique dataset names
            List<string> nameList = new List<string>(uniqueNameSet);
            nameList.Sort();

            // create lookup tables for alias widths and alaises
            Dictionary<string, string> nameLookup = new Dictionary<string, string>();
            Dictionary<string, int> aliasWidths = new Dictionary<string, int>();
            foreach (string name in nameList) {
                nameLookup.Add(name, "");
                aliasWidths.Add(name, 0);
            }

            // first, build lookup of aiias widths
            int maxWidth = 0;
            for (int currentNameIdx = 0; currentNameIdx < uniqueNameSet.Count - 1; currentNameIdx++) {
                // we examine current name and adjacent name in sorted list
                int adjacentNameIdx = currentNameIdx + 1;
                string currentName = nameList[currentNameIdx];
                string adjacentName = nameList[adjacentNameIdx];

                // check overlap of current name with adjacent name in list
                int width = GetUniqueWidth(currentName, adjacentName);
                maxWidth = (width > maxWidth) ? width : maxWidth;

                // set width for alias for current name
                if (aliasWidths[currentName] < width + 1) {
                    aliasWidths[currentName] = width + 1;
                }
                // set width for alias for adjacent name
                if (aliasWidths[adjacentName] < width + 1) {
                    aliasWidths[adjacentName] = width + 1;
                }
            }

            // next set aliases using previously calculated widths
            foreach (string name in nameList) {
                if (padAliasWidth) {
                    nameLookup[name] = name.Substring(0, maxWidth + 1);
                } else {
                    nameLookup[name] = name.Substring(0, aliasWidths[name]);
                }
            }
            return nameLookup;
        }

        // get the minimum number of initial characters necessary to distinquish s1 from s2
        private static int GetUniqueWidth(string s1, string s2) {
            int width = 0;
            int len = (s1.Length > s2.Length) ? s2.Length : s1.Length;
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
        public static void StripOffCommonPrefix(Dictionary<string, string> nameLookup) {
            // find width of common prefix for all alias values
            int width = 1;
            bool matched = true;
            while (matched) {
                string candidatePrefix = "";
                foreach (string alias in nameLookup.Values) {
                    if (string.IsNullOrEmpty(candidatePrefix)) {
                        candidatePrefix = alias.Substring(0, width);
                    } else {
                        if (candidatePrefix != alias.Substring(0, width)) {
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
				foreach (string dataset in nameLookup.Keys)
				{
                    string alias = nameLookup[dataset];
                    string strippedAlias = alias.Substring(start);
                    nameLookup[dataset] = strippedAlias;
                }
            }
        }

    }
}
