using Mage;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Mage filter module that adds and "Alias" column to data stream
    /// </summary>
    public class ModuleAddAlias : ContentFilter
    {
        // Ignore Spelling: defs, Mage

        // Column indexes
        private int _aliasColIdx;
        private int _datasetIdx;

        // Lookup table for dataset aliases
        private Dictionary<string, string> _datasetAliases = new();

        public override void Prepare()
        {
            base.Prepare();
            // Make sure alias column is in output (in case it isn't in input)
            OutputColumnList = "Dataset, Dataset_ID, Alias|+|text, *";
        }

        protected override void ColumnDefsFinished()
        {
            _aliasColIdx = OutputColumnPos["Alias"];
            _datasetIdx = OutputColumnPos["Dataset"];
        }

        /// <summary>
        /// Handle a data row: make sure alias field has an appropriate value
        /// </summary>
        /// <param name="values">Values</param>
        /// <returns></returns>
        protected override bool CheckFilter(ref string[] values)
        {
            if (OutputColumnDefs != null)
            {
                var outRow = MapDataRow(values);
                var dataset = outRow[_datasetIdx];
                outRow[_aliasColIdx] = LookupAlias(dataset);
                values = outRow;
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
        private string LookupAlias(string dataset)
        {
            // ReSharper disable once CanSimplifyDictionaryLookupWithTryGetValue
            return _datasetAliases.ContainsKey(dataset) ? _datasetAliases[dataset] : MakeAlias(dataset);
        }

        /// <summary>
        ///  make and remember a unique short label for this dataset
        /// </summary>
        /// <param name="dataset"></param>
        private string MakeAlias(string dataset)
        {
            var alias = dataset;

            for (var width = 8; width < dataset.Length; width++)
            {
                var candidateAlias = dataset.Substring(0, width);

                if (candidateAlias.Last() == '_')
                {
                    continue;
                }
                if (!_datasetAliases.ContainsValue(candidateAlias))
                {
                    alias = candidateAlias;
                    _datasetAliases.Add(dataset, alias);
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
        /// <param name="padAliases">When true, pad aliases</param>
        /// <param name="stripPrefix">When true, strip prefixes</param>
        public void SetupAliasLookup(SimpleSink factorsObj, bool padAliases = true, bool stripPrefix = false)
        {
            // Get non-duplicate set of dataset names
            var uniqueNameSet = new HashSet<string>();
            var dIdx = factorsObj.ColumnIndex["Dataset"];

            foreach (var row in factorsObj.Rows)
            {
                uniqueNameSet.Add(row[dIdx]);
            }

            // Build alias lookup table for names
            var nameLookup = BuildAliasLookupTable(uniqueNameSet, padAliases);

            // Set up to use the lookup table
            if (nameLookup.Count > 0)
            {
                if (stripPrefix)
                    StripOffCommonPrefix(nameLookup);
                _datasetAliases = nameLookup;
            }
        }

        /// <summary>
        /// Builds a lookup table of aliases given a unique set of names
        /// </summary>
        /// <param name="uniqueNameSet">unique set of names to build alias lookup from</param>
        /// <param name="padAliasWidth"> </param>
        /// <returns>Lookup table of aliases for names, indexed by name</returns>
        public static Dictionary<string, string> BuildAliasLookupTable(HashSet<string> uniqueNameSet, bool padAliasWidth = true)
        {
            // Get sorted list of unique dataset names
            var nameList = new List<string>(uniqueNameSet);
            nameList.Sort();

            // Create lookup tables for alias widths and aliases
            var nameLookup = new Dictionary<string, string>();
            var aliasWidths = new Dictionary<string, int>();

            foreach (var name in nameList)
            {
                nameLookup.Add(name, string.Empty);
                aliasWidths.Add(name, 0);
            }

            // First, build lookup of alias widths
            var maxWidth = 0;

            for (var currentNameIdx = 0; currentNameIdx < uniqueNameSet.Count - 1; currentNameIdx++)
            {
                // We examine current name and adjacent name in sorted list
                var adjacentNameIdx = currentNameIdx + 1;
                var currentName = nameList[currentNameIdx];
                var adjacentName = nameList[adjacentNameIdx];

                // Check overlap of current name with adjacent name in list
                var width = GetUniqueWidth(currentName, adjacentName);
                maxWidth = (width > maxWidth) ? width : maxWidth;

                // Set width for alias for current name
                if (aliasWidths[currentName] < width + 1)
                {
                    aliasWidths[currentName] = width + 1;
                }
                // Set width for alias for adjacent name
                if (aliasWidths[adjacentName] < width + 1)
                {
                    aliasWidths[adjacentName] = width + 1;
                }
            }

            // Next set aliases using previously calculated widths
            foreach (var name in nameList)
            {
                if (padAliasWidth)
                {
                    nameLookup[name] = name.Substring(0, maxWidth + 1);
                }
                else
                {
                    nameLookup[name] = name.Substring(0, aliasWidths[name]);
                }
            }
            return nameLookup;
        }

        /// <summary>
        /// Get the minimum number of initial characters necessary to distinguish the two strings
        /// </summary>
        /// <param name="s1">String 1</param>
        /// <param name="s2">String 2</param>
        /// <returns>Minimum number of characters</returns>
        private static int GetUniqueWidth(string s1, string s2)
        {
            var width = 0;
            var len = (s1.Length > s2.Length) ? s2.Length : s1.Length;

            for (var i = 0; i < len; i++)
            {
                if (s1[i] != s2[i])
                {
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
        public static void StripOffCommonPrefix(Dictionary<string, string> nameLookup)
        {
            // Find width of common prefix for all alias values
            var width = 1;

            while (true)
            {
                var candidatePrefix = string.Empty;
                var matched = true;

                foreach (var alias in nameLookup.Values)
                {
                    if (string.IsNullOrEmpty(candidatePrefix))
                    {
                        candidatePrefix = alias.Substring(0, width);
                    }
                    else
                    {
                        if (candidatePrefix != alias.Substring(0, width))
                        {
                            matched = false;
                            break;
                        }
                    }
                }
                if (matched)
                {
                    width++;
                }
                else
                {
                    break;
                }
            }

            // Strip off common prefix (if there was one)
            if (width > 1)
            {
                var start = width - 1;

                foreach (var dataset in nameLookup.Keys)
                {
                    var alias = nameLookup[dataset];
                    nameLookup[dataset] = alias.Substring(start);
                }
            }
        }
    }
}
