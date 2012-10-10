using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Mage {

    class DelimitedFileWriter : BaseModule {

        #region Member Variables

        private StreamWriter mOutFile = null;

        #endregion

        #region Properties

        public string Delimiter { get; set; }

        public string FilePath { get; set; }

        public string Header { get; set; }

        #endregion

        #region Constructors

        public DelimitedFileWriter() {
            Delimiter = "\t";
            Header = "Yes";
        }
        #endregion

        #region IBaseModule Members
        //        public override event Mage.DataRowHandler DataRowAvailable;
        //        public override event Mage.ColumnDefHandler ColumnDefAvailable;
        //        public override event Mage.StatusMessageUpdated OnStatusMessageUpdated;

        public override void Prepare() {
            mOutFile = new StreamWriter(FilePath);
        }

        public override void Cleanup() {
            base.Cleanup();
            if (mOutFile != null) {
                mOutFile.Close();
            }
        }

        public override void HandleColumnDef(Dictionary<string, string> columnDef) {
            base.HandleColumnDef(columnDef);
            if (columnDef == null && Header == "Yes") {
                OutputHeader();
            }
        }

        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                OutputDataRow(vals);
            } else {
                mOutFile.Close();
            }
        }

        #endregion

        #region Support Functions

        private void OutputHeader() {
            List<string> h = new List<string>();
            // use our output column definitions, if we have them
            // otherwise just use the input column definitions
            if (OutputColumnDefs != null) {
                foreach (Dictionary<string, string> col in OutputColumnDefs) {
                    h.Add(col["Name"]);
                }
            } else {
                foreach (Dictionary<string, string> col in InputColumnDefs) {
                    h.Add(col["Name"]);
                }
            }
            mOutFile.WriteLine(string.Join(Delimiter, h.ToArray()));
        }

        private void OutputDataRow(object[] vals) {
            string delim = "";
            // remap results according to our output column definitions, if we have them
            // otherwise just use the as-delivered format
            object[] outRow = vals;
            if (OutputColumnDefs != null) {
                outRow = MapDataRow(vals);
            }
            foreach (object obj in outRow) {
                mOutFile.Write(delim + ((obj != null)?obj.ToString():""));
                delim = Delimiter;
            }
            mOutFile.WriteLine();
        }

        #endregion
    }
}
