﻿using Mage;
using System;
using System.Collections.Generic;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// Simple Mage FileContentProcessor module
    /// that imports the contents of files that it receives via standard tabular input
    /// to the given SQLite database table
    /// </summary>
    [Obsolete("Unused")]
    public class MageFileImport : FileContentProcessor
    {
        public string DBTableName { get; set; }
        public string DBFilePath { get; set; }
        public string ImportColumnList { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MageFileImport()
        {
            SourceDirectoryColumnName = "Directory";
            SourceFileColumnName = "Name";
            OutputDirectoryPath = "ignore";
            OutputFileName = "ignore";
        }

        // import contents of given file to SQLite database table
        protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, Dictionary<string, string> context)
        {
            if (string.IsNullOrEmpty(ImportColumnList))
            {
                AScoreMagePipeline.ImportFileToSQLite(sourcePath, DBFilePath, DBTableName);
            }
            else
            {
                AScoreMagePipeline.ImportFileToSQLiteWithColumnMods(sourcePath, DBFilePath, DBTableName, ImportColumnList, context);
            }
        }
    }
}
