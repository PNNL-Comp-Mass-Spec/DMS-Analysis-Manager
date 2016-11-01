using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

// module that provides base functions for processing one or more input files 
//
// it expects to receive path information for files via its standard tabular input
//
// each row of standard tabular input will contain information for a single file 
// (parameters SourceFileColumnName and SourceFolderColumnName) define which
// columns in stardard input contain the folder and name of the input file.
//
// the OutputFolderPath parameter tells this module where to put results files
// 
// this module outputs a record of each file processed on stardard tabular output

namespace Mage {

    // delegate for a function that returns and output file name for a given input file name and parameters
    public delegate string OutputFileNamer(string sourceFile, Dictionary<string, int> fieldPos, object[] fields);

    public class FileContentProcessor : BaseModule {

        #region Member Variables

        // delegate that this module calls to get output file name
        private OutputFileNamer GetOutputFileName = null;

        #endregion

        #region Functions Available to Clients

        // define a delegate function that will generate output file name 
        public void SetOutputFileNamer(OutputFileNamer namer) {
            GetOutputFileName = namer;
        }

        #endregion


        #region Properties

        // path to the folder into which the 
        // processed input file contents will be saved as an output file
        // (required by subclasses that create result files)
        public string OutputFolderPath { get; set; }

        // name of the column in the standard tabular input
        // that contains the input folder path
        // (optional - defaults to "Folder")
        public string SourceFolderColumnName { get; set; }

        // name of the column in the standard tabular input
        // that contains the input file name
        // optional - defaults to "File")
        public string SourceFileColumnName { get; set; }

        // the name of the output column that will contain the file name
        public string FileColumnName { get; set; }

        #endregion

        #region Constructors

        public FileContentProcessor() {
            SourceFolderColumnName = "Folder";
            SourceFileColumnName = "File";
            FileColumnName = "File";

            OutputColumnList = string.Format("{0}|+|text, {1}", FileColumnName, SourceFolderColumnName);
            GetOutputFileName = GetDefaultOutputFileName;
        }

        #endregion

        #region IBaseModule Members

        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;

        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                string sourceFolder = vals[InputColumnPos[SourceFolderColumnName]].ToString();
                string sourceFile = vals[InputColumnPos[SourceFileColumnName]].ToString();
                string sourcePath = Path.GetFullPath(Path.Combine(sourceFolder, sourceFile));
                string destFolder = OutputFolderPath;
                string destFile = GetOutputFileName(sourceFile, InputColumnPos, vals);
                string destPath = Path.GetFullPath(Path.Combine(destFolder, destFile));

                // process file
                ProcessFile(sourceFile, sourcePath, destPath, ref stop);

                if (DataRowAvailable != null) {
                    object[] outRow = MapDataRow(vals);
                    int fileNameOutColIndx = OutputColumnPos[FileColumnName];
                    outRow[fileNameOutColIndx] = destFile;
                    DataRowAvailable(outRow, ref this.stop);
                }
            } else {
                if (DataRowAvailable != null) {
                    DataRowAvailable(null, ref this.stop);
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

        #region Overrides

        // this function should be overriden by subclasses to do the actual processing
        protected virtual void ProcessFile(string sourceFile, string sourcePath, string destPath, ref bool stop) {
        }

        #endregion

        #region Utility functions

        // default output file renamer
        protected string GetDefaultOutputFileName(string sourceFile, Dictionary<string, int> fieldPos, object[] fields) {
            return sourceFile;
        }

        #endregion

        #region Functions for Output Columns

        // tell our subscribers what columns to expect from us
        // which will be information about the files processed
        // pluss any input columns that are passed through to output
        private void ExportColumnDefs() {
            foreach (Dictionary<string, string> cd in OutputColumnDefs) {
                ColumnDefAvailable(cd);
            }
            ColumnDefAvailable(null);
        }

        #endregion

        #region Event Handlers

        protected void UpdateStatus(string message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(message);
            }
        }

        #endregion
    }
}
