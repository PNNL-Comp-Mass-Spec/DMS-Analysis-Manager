using System.Collections.Generic;

namespace AnalysisManagerNOMAnnotationPlugin
{
    /// <summary>
    /// Natural organic matter statistics
    /// </summary>
    internal class NaturalOrganicMatterStats
    {
        /// <summary>
        /// Integer-based metrics
        /// </summary>
        public Dictionary<string, int> IntegerMetrics { get; set; }

        /// <summary>
        /// Double-based metrics
        /// </summary>
        public Dictionary<string, double> NumericMetrics { get; set; }

        /// <summary>
        /// Scan number
        /// </summary>
        public int ScanNumber { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="scanNumber">Scan number</param>
        public NaturalOrganicMatterStats(int scanNumber)
        {
            IntegerMetrics = new Dictionary<string, int>();
            NumericMetrics = new Dictionary<string, double>();
            ScanNumber = scanNumber;
        }

        /// <summary>
        /// Deep clone the source stats
        /// </summary>
        /// <param name="sourceStats">Source stats</param>
        /// <returns>Natural organic matter stats instance</returns>
        public NaturalOrganicMatterStats Clone(NaturalOrganicMatterStats sourceStats)
        {
            var clonedStats = new NaturalOrganicMatterStats(sourceStats.ScanNumber);

            foreach (var metric in sourceStats.IntegerMetrics)
            {
                clonedStats.IntegerMetrics.Add(metric.Key, metric.Value);
            }

            foreach (var metric in sourceStats.NumericMetrics)
            {
                clonedStats.NumericMetrics.Add(metric.Key, metric.Value);
            }

            return clonedStats;
        }
    }
}
