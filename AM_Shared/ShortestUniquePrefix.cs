using System;
using System.Collections.Generic;

namespace AnalysisManagerBase
{
    /// <summary>
    /// This class has a method for finding the shortest unique prefix given a list of strings
    /// </summary>
    public class ShortestUniquePrefix
    {
        private string ExpandToNextPunctuation(string word, string abbreviation, char[] punctuationToFind, int maximumCharsToAdd = 15)
        {
            if (!word.StartsWith(abbreviation))
            {
                // This is unexpected
                return abbreviation;
            }

            if (word.Length == abbreviation.Length)
            {
                return abbreviation;
            }

            // Look for the next punctuation mark
            var matchIndex = word.IndexOfAny(punctuationToFind, abbreviation.Length);

            if (matchIndex < 0)
            {
                // Match not found; possibly add the rest of the string
                return word.Length - abbreviation.Length <= maximumCharsToAdd
                    ? word
                    : abbreviation;
            }

            if (matchIndex == abbreviation.Length)
            {
                // The abbreviation already has a punctuation mark after it
                return abbreviation;
            }

            if (matchIndex - abbreviation.Length > maximumCharsToAdd)
            {
                // This would add too many characters; just use the unique name as-is
                return abbreviation;
            }

            return word.Substring(0, matchIndex);
        }

        /// <summary>
        /// Given a list of strings, determine the shortest unique prefix for each string, optionally expanding abbreviations to the next space or punctuation mark
        /// </summary>
        /// <remarks>Based on code at https://www.geeksforgeeks.org/find-shortest-unique-prefix-every-word-given-list-set-2-using-sorting/</remarks>
        /// <param name="wordList">List of words</param>
        /// <param name="expandResultsToNextPunctuation">When true, expand the abbreviations to include text up until the next punctuation mark (or end of string if not found)</param>
        /// <param name="punctuationList">Punctuation marks to look for (defaults to underscore, dash, and space)</param>
        /// <returns>Dictionary where keys are the original words and values are the shortened version of each word</returns>
        public Dictionary<string, string> GetShortestUniquePrefix(IReadOnlyList<string> wordList, bool expandResultsToNextPunctuation = false, string punctuationList = "_- ")
        {
            var resultDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            switch (wordList.Count)
            {
                case 0:
                    return resultDictionary;

                case 1:
                    resultDictionary.Add(wordList[0], wordList[0]);
                    return resultDictionary;
            }

            var uniqueList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            // Assure that the word list does not have any duplicate words

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var item in wordList)
            {
                if (uniqueList.Contains(item))
                    continue;

                uniqueList.Add(item);
            }

            var sortedList = new List<string>();
            sortedList.AddRange(uniqueList);

            // Assure that the list is sorted
            sortedList.Sort();

            var size = sortedList.Count;

            var resultList = new string[size];

            // Compare the first string with its only right neighbor
            var j = 0;

            while (j < Math.Min(sortedList[0].Length - 1, sortedList[1].Length - 1))
            {
                if (sortedList[0][j] == sortedList[1][j])
                    j++;
                else
                    break;
            }

            resultList[0] = sortedList[0].Substring(0, j + 1);

            var targetIndex = 1;

            // Store the unique prefix of sortedList[1] from its left neighbor
            var tempPrefix = sortedList[1].Substring(0, j + 1);

            for (var i = 1; i < size - 1; i++)
            {
                // Compute common prefix of sortedList[i] unique from its right neighbor
                j = 0;

                while (j < Math.Min(sortedList[i].Length - 1, sortedList[i + 1].Length - 1))
                {
                    if (sortedList[i][j] == sortedList[i + 1][j])
                        j++;
                    else
                        break;
                }

                var newPrefix = sortedList[i].Substring(0, j + 1);

                // Compare the new prefix with previous prefix
                if (tempPrefix.Length > newPrefix.Length)
                    resultList[targetIndex] = tempPrefix;
                else
                    resultList[targetIndex] = newPrefix;

                targetIndex++;

                // Store the prefix of sortedList[i+1] unique from its left neighbor
                tempPrefix = sortedList[i + 1].Substring(0, j + 1);
            }

            // Compute the unique prefix for the last string in sorted array
            j = 0;
            var secondLast = sortedList[size - 2];

            var last = sortedList[size - 1];

            while (j < Math.Min(secondLast.Length - 1, last.Length - 1))
            {
                if (secondLast[j] == last[j])
                    j++;
                else
                    break;
            }

            resultList[targetIndex] = last.Substring(0, j + 1);

            if (string.IsNullOrEmpty(punctuationList))
                punctuationList = "_- ";

            var punctuationToFind = punctuationList.ToCharArray();

            for (var i = 0; i < size; i++)
            {
                if (expandResultsToNextPunctuation)
                {
                    // If the original word has additional characters then a punctuation mark after the shortened name, add the extra characters for readability
                    resultDictionary.Add(sortedList[i], ExpandToNextPunctuation(sortedList[i], resultList[i], punctuationToFind));
                    continue;
                }

                resultDictionary.Add(sortedList[i], resultList[i]);
            }

            return resultDictionary;
        }
    }
}
