using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;

// this is a pipeline module 
// that can serve the contents of a TextDisplayControl to standard tabular output

namespace MageClientLib {

    public class TDPipelineSource : BaseModule {

        #region Member Variables

        // object whose data we are serving
        private TextDisplayControl myTextControl = null;

        // delimiter for parsing text into tabular format
        private char[] mDelimiter = new char[] { '\t' };

        private bool doHeaderLine = true;

        #endregion

        #region Constructors

        public TDPipelineSource(TextDisplayControl lc) {
            myTextControl = lc;
            Header = "Yes";
        }

        private List<List<string>> RowBuffer = new List<List<string>>();

        #endregion

        #region Properties

        public string Delimiter {
            get { return mDelimiter.ToString(); }
            set { mDelimiter = value.ToCharArray(); }
        }

        public string Header { get; set; }


        #endregion

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        //        public override event StatusMessageUpdated OnStatusMessageUpdated;


        public override void Run(object state) {
            doHeaderLine = (Header == "Yes");
            OutputRowsFromList();
        }

        #endregion

        #region Private Functions

        private void OutputRowsFromList() {
            foreach(string line in myTextControl.Lines) {
                if (line == "") continue;
                string[] fields = line.Split(mDelimiter);
                if (doHeaderLine) {
                    doHeaderLine = false;
                    OutputHeaderLine(fields);
                } else {
                    OutputDataLine(fields);
                }
            }
            OutputDataLine(null);
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

        #endregion

    }

}
