using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;

// processes input rows from standard tabular input
// and passes only selected ones to standard tabular output
// it is meant to be the base clase for subclasses that actually do the filtering

namespace Mage {

    public class ContentFilter : BaseModule {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        static ContentFilter() {
            ModuleFunction = "ContentFilter";
        }

        private int totalRowsCounter = 0;
        private int passedRowsCounter = 0;
        private int reportRowBlockSize = 1000;

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;

        // let base class processess columns for us
        // and pass the appropriate definitions to our listeners
        public override void HandleColumnDef(Dictionary<string, string> columnDef) {
            base.HandleColumnDef(columnDef);
            if (columnDef == null) {
                if (ColumnDefAvailable != null) {
                    List<Dictionary<string, string>> cd = (OutputColumnDefs != null) ? OutputColumnDefs : InputColumnDefs;
                    foreach (Dictionary<string, string> col in cd) {
                        ColumnDefAvailable(col);
                    }
                    ColumnDefAvailable(null);
                }

                totalRowsCounter = 0;
                passedRowsCounter = 0;
                ColumnDefsFinished();
            }
        }

        // check each input row against the filter and pass on the
        // rows that are accepted
        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                if (DataRowAvailable != null) {

                    // report progress
                    if (++totalRowsCounter % reportRowBlockSize == 0) {
                        if (OnStatusMessageUpdated != null) {
                            OnStatusMessageUpdated("Processed " + totalRowsCounter.ToString() + " total rows, passed " + passedRowsCounter.ToString());
                        }
                    }

                    // do filtering here
                    if (CheckFilter(ref vals)) {
                        passedRowsCounter++;
                        DataRowAvailable(vals, ref stop);
                    }
                }
            } else {
                if (DataRowAvailable != null) {
                    DataRowAvailable(vals, ref stop);
                }
            }
        }

        #endregion

        #region Filtering Functions

        // this function should be overriden by subclasses to do the actual filtering
        protected virtual bool CheckFilter(ref object[] vals) {
            bool accepted = false;

            return accepted;
        }

        // called when all column definitions are complete
        // this function can be overridden by subclasses to set up processing
        protected virtual void ColumnDefsFinished() {

        }

        // allows a content filter module to provide a file nane conversion
        public virtual string RenameOutputFile(string sourceFile, Dictionary<string, int> fieldPos, object[] fields) {
            return sourceFile;
        }

        #endregion
    }


}
