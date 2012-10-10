using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Mage;

namespace MageFileContents {

    class XT2FHTFilter : ContentFilter {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        // this is called for each row that is being subjected to filtering
        // the fields array contains value of each column for the row
        // the column index of each field can be looked up by field name in columnPos[]
        protected override bool CheckFilter(ref object[] fields) {
            bool accepted = false;

            if (OutputColumnDefs != null) {
                object[] outRow = MapDataRow(fields);
                fields = outRow;
            }
            accepted = true;

            return accepted;
        }

    }
}
