using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Utility for creating a map of full length dataset names to abbreviated dataset names
    /// </summary>
    public static class DatasetNameMapUtility
    {
        private static string CombineDatasetNameParts(
           string datasetName,
           IReadOnlyList<string> datasetNameParts,
           int partCountToUse,
           int minimumLength = 0,
           int maximumLengthThreshold = 0)
        {
            if (partCountToUse >= datasetNameParts.Count)
                return datasetName;

            var nextIndex = 0;
            var combinedName = new StringBuilder();

            while (nextIndex < datasetNameParts.Count)
            {
                if (nextIndex >= partCountToUse)
                {
                    if (maximumLengthThreshold == 0 && minimumLength == 0)
                    {
                        // Minimum or maximum length not defined, and we've used the required number of parts
                        break;
                    }

                    var tooShort = minimumLength > 0 && combinedName.ToString().Length < minimumLength;

                    if (maximumLengthThreshold > 0)
                    {
                        // Maximum length defined
                        if (!tooShort && combinedName.ToString().Length + datasetNameParts[nextIndex].Length > maximumLengthThreshold)
                        {
                            // Adding the next part will result in the total length exceeding the maximum
                            // Do not add any more name parts
                            break;
                        }
                    }

                    if (!tooShort)
                    {
                        // Minimum length defined and the name is now long enough
                        break;
                    }
                }

                combinedName.Append(datasetNameParts[nextIndex]);
                nextIndex++;
            }

            return combinedName.ToString();
        }

        /// <summary>
        /// Examine the names in datasetNames
        /// Create a mapping from full name to abbreviated name
        /// </summary>
        /// <param name="datasetNames">List of dataset names</param>
        /// <param name="longestCommonBaseName">Output: longest common base name</param>
        /// <param name="warnings">Output: warning messages</param>
        /// <returns>Dictionary where keys are dataset names and values are abbreviated names</returns>
        public static Dictionary<string, string> GetDatasetNameMap(
            SortedSet<string> datasetNames,
            out string longestCommonBaseName,
            out List<string> warnings)
        {
            warnings = new List<string>();

            var datasetNameParts = new Dictionary<string, List<string>>();
            var maxPartCount = 0;
            var splitChars = new[] { '_', '-' };

            foreach (var datasetName in datasetNames)
            {
                var nameParts = new List<string>();
                var startIndex = 0;

                while (startIndex < datasetName.Length)
                {
                    var matchIndex = datasetName.IndexOfAny(splitChars, startIndex + 1);

                    if (matchIndex < 0)
                    {
                        nameParts.Add(datasetName.Substring(startIndex));
                        break;
                    }

                    nameParts.Add(datasetName.Substring(startIndex, matchIndex - startIndex));
                    startIndex = matchIndex;
                }

                datasetNameParts.Add(datasetName, nameParts);

                maxPartCount = Math.Max(maxPartCount, nameParts.Count);
            }

            if (datasetNameParts.Count == 0)
            {
                longestCommonBaseName = string.Empty;
                return new Dictionary<string, string>();
            }

            var candidateBaseNames = new SortedSet<string>();

            var partCountToUse = 1;

            var datasetNameKeys = datasetNameParts.Keys.ToList();

            while (partCountToUse <= maxPartCount)
            {
                candidateBaseNames.Clear();
                candidateBaseNames.Add(CombineDatasetNameParts(datasetNameKeys[0], datasetNameParts[datasetNameKeys[0]], partCountToUse));

                for (var i = 1; i < datasetNameKeys.Count; i++)
                {
                    var baseNameToAdd = CombineDatasetNameParts(datasetNameKeys[i], datasetNameParts[datasetNameKeys[i]], partCountToUse);

                    if (candidateBaseNames.Contains(baseNameToAdd))
                    {
                        // Name collision found
                        break;
                    }

                    candidateBaseNames.Add(baseNameToAdd);
                }

                if (candidateBaseNames.Count == datasetNameKeys.Count)
                    break;

                partCountToUse++;
            }

            var baseDatasetNames = new SortedSet<string>();

            // Dictionary where keys are dataset names and values are abbreviated names
            var baseNameByDatasetName = new Dictionary<string, string>();

            if (candidateBaseNames.Count == datasetNameKeys.Count)
            {
                // Can use a subsection of the dataset name(s)
                // Combine subsections to create the base name for each dataset
                foreach (var item in datasetNameParts)
                {
                    var baseNameToAdd = CombineDatasetNameParts(item.Key, item.Value, partCountToUse, 12, 25);
                    baseNameByDatasetName.Add(item.Key, baseNameToAdd);

                    if (baseDatasetNames.Contains(baseNameToAdd))
                    {
                        warnings.Add(string.Format(
                            "Warning: baseDatasetNames already contains: {0}\nLogic error for dataset {1}",
                            baseNameToAdd, item.Key));

                        continue;
                    }

                    baseDatasetNames.Add(baseNameToAdd);
                }
            }
            else
            {
                // Not able to shorten the dataset names since they are too similar
                // Use full dataset names
                foreach (var item in datasetNameParts)
                {
                    baseNameByDatasetName.Add(item.Key, item.Key);
                    baseDatasetNames.Add(item.Key);
                }
            }

            longestCommonBaseName = LongestCommonStringFromStart(baseNameByDatasetName.Values.ToList());
            longestCommonBaseName = longestCommonBaseName.TrimEnd('_', '-');

            if (longestCommonBaseName.Length > 7 && (
                longestCommonBaseName.EndsWith("_0") ||
                longestCommonBaseName.EndsWith("_f")))
            {
                longestCommonBaseName = longestCommonBaseName.Substring(0, longestCommonBaseName.Length - 2);
            }

            return baseNameByDatasetName;
        }

        /// <summary>
        /// Find the longest string of letters in common at the start of the items
        /// </summary>
        /// <param name="items">List of strings</param>
        /// <param name="caseSensitive">If true, use case-sensitive comparisons</param>
        public static string LongestCommonStringFromStart(List<string> items, bool caseSensitive = false)
        {
            if (items.Count == 0)
                return string.Empty;

            if (items.Count == 1)
                return items.First();

            var comparisonType = caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

            var longestCommonString = items[0] ?? string.Empty;

            foreach (var item in items.Skip(1))
            {
                longestCommonString = LongestCommonStringFromStart(longestCommonString, item, comparisonType);

                if (longestCommonString.Length == 0)
                {
                    // The items do not all start with the same characters
                    return string.Empty;
                }
            }

            return longestCommonString;
        }

        /// <summary>
        /// Find the longest string of letters in common between string1 and string 2
        /// </summary>
        /// <param name="string1">First string</param>
        /// <param name="string2">Second string</param>
        /// <param name="comparisonType">Comparison type enum</param>
        public static string LongestCommonStringFromStart(string string1, string string2, StringComparison comparisonType = StringComparison.OrdinalIgnoreCase)
        {
            if (string2.Length < string1.Length)
            {
                // Swap strings so that string2 has the longer string
                (string1, string2) = (string2, string1);
            }

            for (var length = string1.Length; length > 0; length--)
            {
                var startOfString = string1.Substring(0, length);

                if (string2.StartsWith(startOfString, comparisonType))
                {
                    return string1.Substring(0, length);
                }
            }

            return string.Empty;
        }
    }
}
