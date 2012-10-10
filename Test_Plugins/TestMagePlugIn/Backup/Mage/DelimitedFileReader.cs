using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Mage {

    public class DelimitedFileReader : BaseModule {

        #region Member Variables

        private StreamReader mFileReader = null;

        private bool doHeaderLine = true;

        private char[] mDelimiter = new char[] { '\t' };

        #endregion

        #region Properties

        public string Delimiter {
            get { return mDelimiter.ToString(); }
            set { mDelimiter = value.ToCharArray(); }
        }

        public string FilePath { get; set; }

        public string Header { get; set; }

        #endregion

        #region Constructors

        public DelimitedFileReader() {
            //            Delimiter = "\t";
            Header = "Yes";
        }

        #endregion

        #region IBaseModule Members
        public override event Mage.DataRowHandler DataRowAvailable;
        public override event Mage.ColumnDefHandler ColumnDefAvailable;
        public override event Mage.StatusMessageUpdated OnStatusMessageUpdated;

        public override void Prepare() {
        }

        public override void Cleanup() {
            base.Cleanup();
            if (mFileReader != null) {
                mFileReader.Close();
            }
        }

        public override void Run(object state) {
            doHeaderLine = (Header == "Yes");
            OutputFileContents();
        }

        #endregion

        #region Support Functions

        private void OutputFileContents() {
            mFileReader = new StreamReader(FilePath);
            string line;
            while ((line = mFileReader.ReadLine()) != null) {
                if (stop) break;
                string[] fields = line.Split(mDelimiter);
                if (doHeaderLine) {
                    doHeaderLine = false;
                    OutputHeaderLine(fields);
                } else {
                    OutputDataLine(fields);
                }
            }
            OutputDataLine(null);
            mFileReader.Close();
        }

        private void OutputHeaderLine(string[] fields) {
            // output the column definitions
            if (ColumnDefAvailable != null) {
                foreach (string field in fields) {
                    Dictionary<string, string> colDef = new Dictionary<string, string>();
                    colDef["Name"] = field;
                    colDef["Type"] = "text";
                    colDef["Size"] = "10";
                    ColumnDefAvailable(colDef);
                }
                ColumnDefAvailable(null);
            }
        }

        private void OutputDataLine(string[] fields) {
            if (DataRowAvailable != null) {
                DataRowAvailable(fields, ref stop);
            }
        }

        private void UpdateStatus(string message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(message);
            }
        }

        #endregion
    }
}
