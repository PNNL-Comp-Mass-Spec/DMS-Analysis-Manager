using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

// this module copies one or more input files to an output folder
//
// its FileContentProcessor base class provides the basic functionality
//
// the OutputMode parameter tells this module whether or not to append a prefix to
// each output file name to avoid name collisions when input files can come from
// more than one input folder
//
// if IDColumnName parameter is set, it specifies a column in the standard input data
// whose value should be used in the prefix.  Otherwise the prefix is generated.
//

namespace Mage {

    class FileCopy : FileContentProcessor {

        #region Member Variables

        private int tagIndex = 0; // used to provide unique prefix for duplicate file names

        #endregion

        #region Properties

        // name of column to be used for output file name prefix
        // (optional)
        public string IDColumnName { get; set; }

        public string OutputMode { set; get; }


        #endregion

        #region Constructors

        public FileCopy() {
            IDColumnName = "";
            OutputMode = "";
            SetOutputFileNamer(new OutputFileNamer(GetDestFile));
        }

        #endregion

        #region Overrides

        protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, ref bool stop) {
            try {
                UpdateStatus("Start Copy->" + sourceFile);
                File.Copy(sourcePath, destPath);
                UpdateStatus("Done->" + sourceFile);
            } catch (Exception e) {
                UpdateStatus("FAILED->" + e.Message + " -- " + sourceFile);
                throw new Exception("File copy failed:" + e.Message + " -- " + sourceFile);
            }
        }

        // determine the name to be used for the destination file
        protected string GetDestFile(string sourceFile, Dictionary<string, int> fieldPos, object[] fields) {
            string prefix = "";
            if (InputColumnPos.ContainsKey(IDColumnName)) {
                prefix = IDColumnName + "_" + fields[fieldPos[IDColumnName]].ToString();
            } else {
                prefix = "Tag_" + (tagIndex++).ToString();
            }

            if (OutputMode == "Prefix") {
                return prefix + "_" + sourceFile;
            } else {
                return sourceFile;
            }
        }

        #endregion
    }
}
