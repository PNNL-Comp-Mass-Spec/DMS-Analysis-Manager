using System;
using System.Collections.Generic;
using AnalysisManagerBase.DataFileTools;
using NUnit.Framework;

namespace AnalysisManagerTest
{
    internal class DatasetNameMapTests
    {
        // ReSharper disable StringLiteralTypo

        [Test]
        [TestCase("", "")]
        [TestCase(" ", " ")]
        [TestCase("QC_Shew_16_01", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14")]
        [TestCase("QC_Shew_16_01-15f", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "QC_Shew_16_01-15f_09_15Nov16_Tiger_16-02-15")]
        [TestCase("QC_Shew_16_01-15f", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "QC_Shew_16_01-15f_10_15Nov16_Tiger_16-02-14")]
        [TestCase("QC_Shew_16_01-15f", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "QC_Shew_16_01-15f_09_15Nov16_Tiger_16-02-15", "QC_Shew_16_01-15f_10_15Nov16_Tiger_16-02-14")]
        [TestCase("QC", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "QC_PP_MCF-7_21_01_d_22Apr21_Rage_Rep-21-02-06")]
        [TestCase("QC", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "QC_Shew_16_01-15f_09_15Nov16_Tiger_16-02-15", "QC_PP_MCF-7_21_01_d_22Apr21_Rage_Rep-21-02-06")]
        [TestCase("QC", "QC_Shew_16_01-15f_09_15Nov16_Tiger_16-02-15", "QC_Mam_19_01_a_22Apr21_Rage_Rep-21-02-06")]
        [TestCase("QC", "QC_PP_MCF-7_21_01_d_22Apr21_Rage_Rep-21-02-06", "QC_Mam_19_01_a_22Apr21_Rage_Rep-21-02-06")]
        [TestCase("", "QC_Shew_16_01-15f_08_4Nov16_Tiger_16-02-14", "AALEDTLAETEAR_MSonly")]
        [TestCase("AALEDTLAETEAR_MS", "AALEDTLAETEAR_MSMSonly", "AALEDTLAETEAR_MSonly")]
        [TestCase("Acetamiprid_rep1_1000V", "Acetamiprid_rep1_1000V", "Acetamiprid_rep1_1000V_neg")]
        [TestCase("Acetamiprid_rep", "Acetamiprid_rep1_1000V", "Acetamiprid_rep1_1000V_neg", "Acetamiprid_rep2_1000V", "Acetamiprid_rep2_1000V_neg")]
        [TestCase("Acetamiprid_rep2_1", "Acetamiprid_rep2_1000V", "Acetamiprid_rep2_1000V_neg", "Acetamiprid_rep2_1100V", "Acetamiprid_rep2_1100V_neg")]
        // ReSharper restore StringLiteralTypo
        public void TestGetDatasetNameMap(string expectedLongestCommonString, params string[] datasets)
        {
            var datasetNames = new SortedSet<string>();

            foreach (var item in datasets)
            {
                if (item.Length > 0)
                    datasetNames.Add(item);
            }

            var baseDatasetNames = DatasetNameMapUtility.GetDatasetNameMap(datasetNames, out var longestCommonString, out var warnings);

            foreach (var warning in warnings)
            {
                Console.WriteLine(warning);
                Console.WriteLine();
            }

            foreach (var item in baseDatasetNames)
            {
                Console.WriteLine("{0} =>", item.Key);
                Console.WriteLine(item.Value);
                Console.WriteLine();
            }

            Console.WriteLine("Longest common string:");
            Console.WriteLine(longestCommonString);

            Assert.That(longestCommonString, Is.EqualTo(expectedLongestCommonString));

            Assert.That(warnings, Has.Count.EqualTo(0), "Warnings were reported");
        }

        [Test]
        [TestCase("", "")]
        [TestCase(" ", " ")]
        [TestCase("dataset1", "dataset1")]
        [TestCase("dataset", "dataset1", "dataset2")]
        [TestCase("dataset", "dataset1", "dataset2", "dataset3")]
        [TestCase("data", "dataset1", "data2", "dataset3")]
        [TestCase("data", "dataset1", "data2", "dataset3", "data2", "dataset1")]
        [TestCase("da", "dataset", "data", "daily")]
        [TestCase("da", "data", "dataset", "daily")]
        [TestCase("da", "daily", "data", "dataset")]
        [TestCase("da", "daily", "dataset", "data")]
        [TestCase("d", "dataset", "data", "diamond")]
        [TestCase("", "dataset", "crystal", "daily")]
        public void TestLongestCommonStringFromStart(string expectedResult, params string[] itemNames)
        {
            var items = new List<string>();

            foreach (var item in itemNames)
            {
                if (item.Length > 0)
                    items.Add(item);
            }

            var result = DatasetNameMapUtility.LongestCommonStringFromStart(items);

            Console.WriteLine("Longest common string: ");
            Console.WriteLine(result);
            Console.WriteLine();

            foreach (var item in items)
            {
                Console.WriteLine(item);
            }

            Assert.That(result, Is.EqualTo(expectedResult));
        }
    }
}
