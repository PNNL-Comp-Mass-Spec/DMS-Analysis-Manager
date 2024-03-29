using System;
using System.Collections.Generic;
using System.IO;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Provides tools for creating an analysis job summary file
    /// </summary>
    public class SummaryFile
    {
        private readonly List<string> mLines = new();

        /// <summary>
        /// Clears summary file data
        /// </summary>
        public void Clear()
        {
            mLines.Clear();
        }

        /// <summary>
        /// Writes the summary file to the specified location
        /// </summary>
        /// <param name="analysisSummaryFilePath">Full path of summary file to create</param>
        /// <returns>True if success, false if an error</returns>
        public bool SaveSummaryFile(string analysisSummaryFilePath)
        {
            try
            {
                using var writer = new StreamWriter(new FileStream(analysisSummaryFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                foreach (var outLine in mLines)
                {
                    writer.WriteLine(outLine);
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Adds a line of data to summary file
        /// </summary>
        /// <param name="line">Data to be added</param>
        public void Add(string line)
        {
            mLines.Add(line);
        }
    }
}