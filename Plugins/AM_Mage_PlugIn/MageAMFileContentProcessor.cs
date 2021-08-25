using Mage;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Subclass of the Mage FileContentProcessor class
    /// that performs processes specific to the MAC Mage plug-in
    /// </summary>
    public class MageAMFileContentProcessor : FileContentProcessor
    {
        // Ignore Spelling: Mage

        /// <summary>
        /// Specific set of allowed file names.
        /// </summary>
        /// <remarks>
        /// If not null, processing will be restricted to only files in list (as tracked by property FileNameList)
        /// </remarks>
        private HashSet<string> _fileNameSet;

        public MageAMFileProcessingPipelines FilePipeline { get; }

        /// <summary>
        /// Name of the table in SQLite database that receives the results
        /// </summary>
        /// <remarks>If blank, table name will be constructed from source file name</remarks>
        public string DBTableName { get; set; }

        /// <summary>
        /// Name of the operation to be performed on the file contents
        /// </summary>
        public string Operation { get; set; }

        /// <summary>
        /// Specific list of allowed file names.
        /// </summary>
        /// <remarks>If not null, processing will be restricted to only files in list</remarks>
        public string FileNameList { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filePipeline"></param>
        public MageAMFileContentProcessor(MageAMFileProcessingPipelines filePipeline)
        {
            SourceDirectoryColumnName = "Directory";
            SourceFileColumnName = "Name";
            OutputDirectoryPath = "ignore";
            OutputFileName = "ignore";
            FilePipeline = filePipeline;
            Operation = "SimpleImport";
        }

        /// <summary>
        /// Do necessary setup before pipeline runs
        /// </summary>
        public override void Prepare()
        {
            base.Prepare();
            InitializeFileNameSet();
        }

        /// <summary>
        /// Override of base class stub that performs one of the predefined processes on the contents of the given file
        /// </summary>
        /// <param name="sourceFile">Name of the file to be processed</param>
        /// <param name="sourcePath">Full path to directory that contains the file to be processed</param>
        /// <param name="destPath"></param>
        /// <param name="context">Additional metadata about file to be processed</param>
        protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, Dictionary<string, string> context)
        {
            if (_fileNameSet?.Contains(sourceFile) == false)
                return;

            string dbFilePath;
            string workingFilePath;
            switch (Operation)
            {
                case "CopyAndImport":
                    workingFilePath = Path.Combine(FilePipeline.WorkingDir, sourceFile);
                    File.Copy(sourcePath, workingFilePath, true);
                    dbFilePath = FilePipeline.GetResultsDBFilePath();
                    FilePipeline.ImportFileToSQLite(workingFilePath, dbFilePath, DBTableName);
                    break;
                case "SimpleImport":
                    dbFilePath = FilePipeline.GetResultsDBFilePath();
                    FilePipeline.ImportFileToSQLite(sourcePath, dbFilePath, DBTableName);
                    break;
                case "AddDatasetIDToImport":
                    dbFilePath = FilePipeline.GetResultsDBFilePath();
                    const string columnList = "Dataset_ID|+|int, *";
                    FilePipeline.ImportFileToSQLiteWithColumnMods(sourcePath, dbFilePath, DBTableName, columnList, context);
                    break;
                case "IMPROVClusterImport":
                    workingFilePath = Path.Combine(FilePipeline.WorkingDir, sourceFile);
                    File.Copy(sourcePath, workingFilePath, true);
                    dbFilePath = FilePipeline.GetResultsDBFilePath();
                    FilePipeline.ImportImprovClusterFileToSQLite(workingFilePath, dbFilePath, DBTableName);
                    break;
            }
        }

        /// <summary>
        /// Convert comma-delimited list of item into a trimmed hash set
        /// </summary>
        /// <param name="delimitedList"></param>
        private HashSet<string> ConvertListToSet(string delimitedList)
        {
            var set = new HashSet<string>();
            var items = delimitedList.Split(',');
            foreach (var item in items)
            {
                set.Add(item.Trim());
            }
            return set;
        }

        /// <summary>
        /// Populate (or clear) set of permissible file names
        /// </summary>
        private void InitializeFileNameSet()
        {
            _fileNameSet = string.IsNullOrEmpty(FileNameList) ? null : ConvertListToSet(FileNameList);
        }
    }
}
