using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Mage;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn {

    public class MageAMFileProcessingPipelines : MageAMPipelineBase {

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        public MageAMFileProcessingPipelines(IJobParams jobParms, IMgrParams mgrParms)
            : base(jobParms, mgrParms) {
        }

 
        #endregion

        /// <summary>
        /// Copy the results SQLite database file produced by the previous job step (if it exists)
        /// to the working directory
        /// </summary>
        protected void GetPriorResultsToWorkDir() {
            string dataPackageFolderPath = Path.Combine(RequireJobParam("transferFolderPath"), RequireJobParam("OutputFolderName"));

            string priorResultsDBFilePath = Path.Combine(dataPackageFolderPath, RequireJobParam("StepInputFolderName"), mResultsDBFileName);

            if (File.Exists(priorResultsDBFilePath)) {
                File.Copy(priorResultsDBFilePath, mWorkingDir);
            }
        }

        /// <summary>
        /// Import the contents of files in the given source folder that pass the given name filter
        /// into the results SQLite database
        /// </summary>
        public void ImportFilesToSQLite() {
            GetPriorResultsToWorkDir();

            string dataPackageStorageFolderRoot = RequireJobParam("transferFolderPath");
            string inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, RequireJobParam("DataPackageSourceFolderName"));
            string dbFilePath = Path.Combine(mWorkingDir, mResultsDBFileName);
            string dbTableName = GetJobParam("DBTableName");


            FileListFilter reader = new FileListFilter();
            reader.AddFolderPath(inputFolderPath);
            reader.FileNameSelector = GetJobParam("FileNameSelector");
            reader.FileSelectorMode = GetJobParam("FileSelectorMode", "RegEx");
            reader.IncludeFilesOrFolders = GetJobParam("IncludeFilesOrFolders", "File");
            reader.RecursiveSearch = GetJobParam("RecursiveSearch", "No");

            SimpleSink fileList = new SimpleSink();

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("GetFileListPipeline", reader, fileList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            // reader.FileTypeColumnName = "Item";
            int folderIdx = fileList.ColumnIndex[reader.SourceFolderColumnName];
            int fileIdx = fileList.ColumnIndex[reader.FileColumnName];
            foreach (Object[] row in fileList.Rows) {
                string inputFilePath = Path.Combine(row[folderIdx].ToString(), row[fileIdx].ToString());
                ImportFileToSQLite(inputFilePath, dbFilePath, dbTableName);
            }
        }

        /// <summary>
        /// Import the contents of the given file into the given table in the given results SQLite database
        /// </summary>
        /// <param name="inputFilePath"></param>
        /// <param name="dbFilePath"></param>
        /// <param name="dbTableName"></param>
        public void ImportFileToSQLite(string inputFilePath, string dbFilePath, string dbTableName) {
            DelimitedFileReader reader = new DelimitedFileReader();
            reader.FilePath = inputFilePath;

            SQLiteWriter writer = new SQLiteWriter();
            string tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, writer);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
       }


    }
}
