
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


namespace AnalysisManagerBase
{
    /// <summary>
    /// Provides tools for creating an analysis job summary file
    /// </summary>
    public class clsSummaryFile
    {

        #region "Module Variables"

        private readonly List<string> mLines = new List<string>();

        #endregion

        #region "Methods"

        /// <summary>
        /// Clears summary file data
        /// </summary>
        /// <remarks></remarks>
        public void Clear()
        {
            mLines.Clear();
        }

        /// <summary>
        /// Writes the summary file to the specified location
        /// </summary>
        /// <param name="AnalysisSummaryFilePath">Full path of summary file to create</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool SaveSummaryFile(string AnalysisSummaryFilePath)
        {
            try
            {
                using (var writer = new StreamWriter(new FileStream(AnalysisSummaryFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var outLine in mLines)
                    {
                        writer.WriteLine(outLine);
                    }
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
        /// <remarks></remarks>
        public void Add(string line)
        {
            mLines.Add(line);
        }

        #endregion

    }

}