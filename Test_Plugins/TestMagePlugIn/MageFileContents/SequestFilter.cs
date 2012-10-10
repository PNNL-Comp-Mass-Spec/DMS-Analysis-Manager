using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;
using log4net;

namespace MageFileContents {

    // this is an example of a simple filter module
    // that is derived from a generic ContentFilter class

    class SequestFilter : ContentFilter {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        // indexes into the row field data array
        private int xCorrIdx = 0;

        // this is called for each row that is being subjected to filtering
        // the fields array contains value of each column for the row
        // the column index of each field can be looked up by field name in columnPos[]
        protected override bool CheckFilter(ref object[] fields) {
            bool accepted = false;

            // example of filtering out low XCorr
            double v = 0;
            double.TryParse((string)fields[xCorrIdx], out v);
            if (v > 2.0) {
                accepted = true;
            }
            return accepted;
        }

        // this is called when all the field column definitions are complete
        // it can be used to set up precalulated indexes for the fields 
        // so that we don't have to look them up by name for each row sent to filter
        // (or it can be ignored)
        protected override void ColumnDefsFinished() {
            // set up indexes into fields array
            xCorrIdx = this.InputColumnPos["XCorr"];
        }

    }

}
