using System;
using System.IO;
using System.Linq;
using AnalysisManager_RepoPkgr_PlugIn;
using Mage;

namespace AnalysisManager_RepoPkgr_Plugin
{
    /// Handler that provides pre-packaged Mage pipelines
    /// that do the heavy lifting tasks that get data package items,
    /// find associated files, and copy them to repo cache folders    
    public class MageRepoPkgrPipelines
    {
        #region Properties
        /// <summary>
        /// Data package that supplies items for repo cache (must be set by client before use)
        /// </summary>
        public string DataPkgId { get; set; }

        /// <summary>
        /// Path to root folder of repo cache (must be set by client before use)
        /// </summary>
        public string OutputResultsFolderPath { get; set; }

        /// <summary>
        /// object containing definitions of database queries to use (must be set by client before use)
        /// </summary>
        public QueryDefinitions QueryDefs { get; set; }

        // objects that retain intermediate results from latest pipeline run

        /// <summary>
        /// List of data package items
        /// </summary>
        public SimpleSink DataPackageItems { get; set; }

        /// <summary>
        /// List of associated files that were found for items in DataPackageItems
        /// </summary>
        public SimpleSink AssociatedFiles { get; set; }

        /// <summary>
        /// List of files that were copied to repo cache
        /// </summary>
        public SimpleSink ManifestForCopy { get; set; }

        /// <summary>
        /// Controls whether or not a metadata file will be written for copied files
        /// </summary>
        private bool EnableMetadataFile { get; set; }

        #endregion

        #region Constructors

        public MageRepoPkgrPipelines()
        {
            EnableMetadataFile = true;
        }

        #endregion

        #region Pipeline_Methods

        /// <summary>
        /// Copy given set of files for given set of items from data package
        /// to the given subfolder in the repo cache folder.
        /// Query the database (using the given query templage and (optional) secondary filter value) to get items from data package, 
        /// then search for files associated with those items using the given file name filter,
        /// and then copy the files to the given subfolder in the repo cache 
        /// (include metadata file (based on data package items) as well)
        /// </summary>
        /// <param name="queryTmpltName">Name of query template to use to get items from data package</param>
        /// <param name="filter">value to filter query on (ignore if blank)</param>
        /// <param name="fileNameSelector">Mage file filter to select specific files to copy</param>
        /// <param name="outputSubfolderName">Subfolder in repo package cache folder to copy files into</param>
        /// <param name="prefixCol">Name of column to use as prefix for output file name (ignore if blank)</param>
        public void GetItemsToRepoPkg(string queryTmpltName, string filter, string fileNameSelector, string outputSubfolderName, string prefixCol)
        {
            DataPackageItems = null;
            AssociatedFiles = null;
            ManifestForCopy = null;
            DataPackageItems = GetDataPackageItemList(queryTmpltName, filter);
            GetFilesToRepoPkg(fileNameSelector, outputSubfolderName, prefixCol);
        }

        /// <summary>
        /// Copy given set of files for given set of items from data package
        /// using existing list of data package items.
        /// Search for files associated with those items using the given file name filter,
        /// and then copy the files to the given subfolder in the repo cache 
        /// (include metadata file (based on data package items) as well)
        /// </summary>
        /// <param name="fileNameSelector">Mage file filter to select specific files to copy</param>
        /// <param name="outputSubfolderName">Subfolder in repo package cache folder to copy files into</param>
        /// <param name="prefixCol">Name of column to use as prefix for output file name (ignore if blank)</param>
        public void GetFilesToRepoPkg(string fileNameSelector, string outputSubfolderName, string prefixCol)
        {
            AssociatedFiles = null;
            ManifestForCopy = null;
            AssociatedFiles = GetFileSearchResults(DataPackageItems, fileNameSelector);
            CopyFilesToRepoPkg(outputSubfolderName, prefixCol);
        }

        /// <summary>
        /// Copy given set of files for given set of items from data package
        /// using existing list of files and existing list of data package items.
        /// Copy the files to the given subfolder in the repo cache 
        /// (include metadata file (based on data package items) as well)
        /// </summary>
        /// <param name="outputSubfolderName">Subfolder in repo package cache folder to copy files into</param>
        /// <param name="prefixCol">Name of column to use as prefix for output file name (ignore if blank)</param>
        public void CopyFilesToRepoPkg(string outputSubfolderName, string prefixCol)
        {
            ManifestForCopy = null;
            ManifestForCopy = CopyFiles(AssociatedFiles, Path.Combine(OutputResultsFolderPath, outputSubfolderName), prefixCol);
            WriteMetadata(DataPackageItems, outputSubfolderName);
        }

        /// <summary>
        /// Determine if there are any duplicates in the file name column
        /// </summary>
        /// <param name="outputFileColumnName">Name of the column that contains file name</param>
        /// <returns></returns>
        private bool CheckForDuplicateFileNames(string outputFileColumnName)
        {
            var fileNameCol = AssociatedFiles.ColumnIndex[outputFileColumnName];
            var fileNames = AssociatedFiles.Rows.Select(x => x[fileNameCol]);
            var groupedFileNames = fileNames.GroupBy(y => y).Select(g => new { Value = g.Key, Count = g.Count() });
            var duplicates = groupedFileNames.Where(z => z.Count > 1);
            return (duplicates.Any());
        }


        /// <summary>
        /// Write the contents of the given metadata to a file in 
        /// the appropriate target folder.  Metadata file name is based 
        /// on the outputSubFolder name. If outputSubfolderName is
        /// a nested partial path, name will be based on last segment.
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="outputSubfolderName"></param>
        private void WriteMetadata(SimpleSink metadata, string outputSubfolderName)
        {
            if (!EnableMetadataFile)
                return;
            if (metadata.Rows.Count == 0)
                return;
            var subfolders = outputSubfolderName.Split(new[] { Path.DirectorySeparatorChar });
            var metadataFolderPath = Path.Combine(OutputResultsFolderPath, outputSubfolderName);
            var metadataFileName = string.Format("{0}_metadata.txt", subfolders.Last());

            var fileWriter = new DelimitedFileWriter { FilePath = Path.Combine(metadataFolderPath, metadataFileName) };
            var pl = ProcessingPipeline.Assemble("WriteMetadataFile", new SinkWrapper(metadata), fileWriter);
            pl.RunRoot(null);
        }

        /// <summary>
        /// Mage pipeline that get results of query built from given template
        /// and returns them in SimpleSink object
        /// </summary>
        /// <param name="queryTmpltName">Name of the query template to use</param>
        /// <param name="filter">(optional) Value to use for supplemental filter (only effective if one is defined for query template)</param>
        /// <returns>SimpleSink object containing results of query</returns>
        private SimpleSink GetDataPackageItemList(string queryTmpltName, string filter = "")
        {
            var result = new SimpleSink();
            var cnStr = QueryDefs.GetCnStr(queryTmpltName);
            var sqlText = QueryDefs.GetQueryTmplt(queryTmpltName).Sql(DataPkgId, filter);
            var pl = ProcessingPipeline.Assemble("GetDataPackageItems",
                                        new MSSQLReader { ConnectionString = cnStr, SQLText = sqlText }, result);
            ConnectEventHandlersToPipeline(pl);
            pl.RunRoot(null);
            return result;
        }

        /// <summary>
        /// Simple Mage pipeline that returns results of seaching 
        /// the given set of input folders for files matching the given selector pattern
        /// </summary>
        /// <param name="searchList">SimpleSink object holding list of folders to search (plus optional metadata)</param>
        /// <param name="fileNameSelector">Pattern to use to select files</param>
        /// <returns></returns>
        private SimpleSink GetFileSearchResults(SimpleSink searchList, string fileNameSelector)
        {
            var sourceFolders = new SinkWrapper(searchList);
            var fileSearcher = new FileListFilter
            {
                FileNameSelector = fileNameSelector,
                FileSelectorMode = "FileSearch",
                OutputColumnList = "Item|+|text, File|+|text, File_Size_KB|+|text, File_Date|+|text, Folder, *"
            };
            var results = new SimpleSink();
            var pl = ProcessingPipeline.Assemble("FileSearchPipeline", sourceFolders, fileSearcher, results);
            ConnectEventHandlersToPipeline(pl);
            pl.RunRoot(null);
            return results;
        }

        /// <summary>
        /// Mage pipeline that copies files in the given source list to the given output folder
        /// A disambiguating prefix will be applied to file names if one is supplied and there are any duplicate file names.
        /// </summary>
        /// <param name="sourceObject">SimpleSink object containing list of files to copy</param>
        /// <param name="outputFolder">Full path the output folder</param>
        /// <param name="prefixCol">Name of column in sourceObject to apply to output files as prefix (optional)</param>
        /// <returns></returns>
        private SimpleSink CopyFiles(SimpleSink sourceObject, string outputFolder, string prefixCol = "")
        {
            var result = new SimpleSink();
            if (sourceObject.Rows.Count == 0)
                return result;
            if (!Directory.Exists(outputFolder))
            {
                Directory.CreateDirectory(outputFolder);
            }
            var source = new SinkWrapper(sourceObject);
            var copier = new FileCopy { OutputFolderPath = outputFolder, OverwriteExistingFiles = true };
            if (!string.IsNullOrEmpty(prefixCol) && CheckForDuplicateFileNames(copier.OutputFileColumnName))
            {
                copier.ColumnToUseForPrefix = prefixCol;
                copier.PrefixLeader = prefixCol;
                copier.ApplyPrefixToFileName = "Yes";
            }
            var pl = ProcessingPipeline.Assemble("FileCopyPipeline", sourceObject, copier, result);
            ConnectEventHandlersToPipeline(pl);
            pl.RunRoot(null);
            return result;
        }

        #endregion

        #region Pipeline_Run_Event_Handlers

        /// <summary>
        /// Wire up the internal event handlers to the given Mage pipeline
        /// </summary>
        /// <param name="pl"></param>
        private void ConnectEventHandlersToPipeline(ProcessingPipeline pl)
        {
            pl.OnStatusMessageUpdated += HandlePipelineUpdate;
            pl.OnWarningMessageUpdated += HandlePipelineWarning;
            pl.OnRunCompleted += HandlePipelineCompletion;
        }

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args)
        {
            //	todo Log pipeline event message?
            Console.WriteLine(args.Message);
        }

        private void HandlePipelineWarning(object sender, MageStatusEventArgs args)
        {
            //	todo Log pipeline event message?
            Console.WriteLine("Warning: " + args.Message);
        }

        private void HandlePipelineCompletion(object sender, MageStatusEventArgs args)
        {
            //	todo Log pipeline event message?
            Console.WriteLine(args.Message);

        }

        #endregion

    }
}
