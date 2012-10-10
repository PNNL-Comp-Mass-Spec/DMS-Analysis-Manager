using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using log4net;

// this module searches a list of folder paths for files and compares the file names
// against a set of file selection criteria and accumulates an internal list of files that pass,
// and outputs the selected files (and their folder path) via standard tablular output
//
// this module can receive the list of source folders either via its HandleDataRow listener
// (it will accumulate the list into an internal file path buffer and then use it to look for files)
// or it may be run as a source module after one or more source folders are specified by
// setting the "FolderPath" property/parameter
//
// this module uses output column definitions
// the internal defaults will provide a functional minimum even if the 
// "OutputColumnList" property is not set by the client, but if it is
// it must include a new column definition for the column specified by the "FileColumnName" property

namespace Mage {

    class FileListFilter : BaseModule {
        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        #region Member Variables

        // buffer that accumulates a row of output fields for each input row
        // received via standard tabular input or via the "FolderPath" property
        // it includes the folder path column to be searched for files
        // so it also functions as an internal file path buffer 
        private List<object[]> OutputBuffer = new List<object[]>();

        // list of regular expresssions to use to select files
        List<Regex> fileNameSpecs = new List<Regex>();

        #endregion

        #region "Properties

        // the name of the input column that contains the folder path to search for files
        public string SourceFolderColumnName { get; set; }

        // the name of the output column that will contain the file name
        public string FileColumnName { get; set; }

        // the name of the input column that contains the "ID" value
        public string IDColumnName { get; set; }

        // setting this property adds the file name selector to the internal list of selectors
        public string FileNameSelector {
            set {
                try {
                    Regex rx = new Regex(value, RegexOptions.IgnoreCase);
                    fileNameSpecs.Add(rx);
                } catch (Exception e) {
                    traceLog.Error(e.Message);
                    throw new Exception("Problem with file selector:" + e.Message);
                }
            }
        }

        // setting this property adds the file path to the internal file path buffer
        // (necessary if Run will be called instead of processing via standard tabular input)
        public string FolderPath {
            set {
                OutputBuffer.Add(new object[] { "", value });
            }
        }

        #endregion

        #region Constructors

        public FileListFilter() {
            FileColumnName = "File";
            SourceFolderColumnName = "Folder";
            OutputColumnList = string.Format("{0}|+|text, {1}|+|text", FileColumnName, SourceFolderColumnName);
            IDColumnName = "";
        }

        #endregion

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;

        // called when this module functions as source module
        // (requires that optional property FolderPath be set)
        public override void Run(object state) {
            if (ColumnDefAvailable != null) {
                SetUpOutputColumns();
                ExportColumnDefs();
            }
            if (DataRowAvailable != null) {
                SearchFoldersAndOutputFiles();
            }
        }

        // receive storage folder path as column in data row, 
        // and save it and the ID column value to our local folder path buffer
        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                OutputBuffer.Add(MapDataRow(vals));
            } else {
                // if we have subscribers, do the file lookup and tell them about it
                if (DataRowAvailable != null) {
                    SearchFoldersAndOutputFiles();
                }
            }
        }

        public override void HandleColumnDef(Dictionary<string, string> columnDef) {
            // build lookup of column index by column name
            base.HandleColumnDef(columnDef);
            if (columnDef == null) {
                // end of column definitions from our source,
                // now tell our subscribers what columns to expect from us
                if (ColumnDefAvailable != null) {
                    ExportColumnDefs();
                }
            }
        }

        #endregion

        #region Functions for Output Columns


        // tell our subscribers what columns to expect from us
        // which will be an internal (filename) column
        // followed by any input columns that are passed through to output
        private void ExportColumnDefs() {
            foreach (Dictionary<string, string> cd in OutputColumnDefs) {
                ColumnDefAvailable(cd);
            }
            ColumnDefAvailable(null);
        }


        #endregion

        #region Private Functions

        // go through each folder that we accumulated in our internal buffer
        // and search the folder for files that satisfy file name filter,
        // and output them to our listeners
        private void SearchFoldersAndOutputFiles() {
            int pathColIndx = OutputColumnPos[SourceFolderColumnName];
            int fileNameOutColIndx = OutputColumnPos[FileColumnName];

            // go through each folder that we accumulated in our internal buffer
            foreach (object[] fields in OutputBuffer) {
                if (this.stop) break;
                string path = (string)fields[pathColIndx];
                traceLog.Debug("FileListFilter:Searching folder " + path);
                UpdateStatus("FileListFilter:Searching folder " + path);
                try {
                    List<string> fileNames = GetFileNamesFromSourceFolder(path);
                    if (fileNames.Count > 0) {
                        // search the folder for files that satisfy file name filter,
                        foreach (string fn in fileNames) {
                            // inform our subscribers about the file we found
                            //object[] myRow = new object[] { folderRef.Key, fn, folderRef.Value };
                            fields[fileNameOutColIndx] = fn;
                            DataRowAvailable(fields, ref this.stop);
                        }
                    } else {
                        // output record that says we didn't find any files
                        // FUTURE: make reporting jobs with no files found as an option set by parameter
                        fields[fileNameOutColIndx] = "--No Files Found--";
                        DataRowAvailable(fields, ref this.stop);
                    }
                } catch (Exception e) {
                    // output record that says we had problem accessing files
                    fields[fileNameOutColIndx] = "--Error: " + e.Message;
                    traceLog.Error(e.Message);
                    DataRowAvailable(fields, ref this.stop);
                    this.stop = true;
                }
            }
            // inform our subscribers that all data has been sent
            DataRowAvailable(null, ref this.stop);
        }

        // search files in folder and return list of files whose names satisfy the selection criteria
        private List<string> GetFileNamesFromSourceFolder(string folderPath) {
            List<string> fileNames = new List<string>();
            FileInfo[] fiArr = null;
            try {
                // Get list of all files in the folder.
                DirectoryInfo di = new DirectoryInfo(folderPath);
                fiArr = di.GetFiles();
            } catch (Exception e) {
                throw new Exception("Problem finding files in fodler:" + e.Message + " -- " + folderPath);

            }

            // find files that meet selection criteria.
            foreach (FileInfo fri in fiArr) {
                if (fileNameSpecs.Count == 0) {
                    fileNames.Add(fri.Name);
                } else {
                    foreach (Regex rx in fileNameSpecs) {
                        Match m = rx.Match(fri.Name);
                        if (m.Success) {
                            fileNames.Add(fri.Name);
                            break;
                        }
                    }
                }
            }
            return fileNames;
        }

        private void UpdateStatus(string message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(message);
            }
        }

        #endregion


    }
}
