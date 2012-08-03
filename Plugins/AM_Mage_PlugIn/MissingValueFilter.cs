using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;

namespace AnalysisManager_Mage_PlugIn {

    public class MissingValueFilter : ContentFilter {

        private int _fillColIdx;
        private string _rememberedFillValue = "";

        public string FillColumnName { get; set; }

        protected override void ColumnDefsFinished() {
            _fillColIdx = InputColumnPos[FillColumnName];
        }

        // handle a data row - make sure alias field has an appropriate value
        protected override bool CheckFilter(ref object[] vals) {

            var val = vals[_fillColIdx].ToString();
            if (string.IsNullOrEmpty(val)) {
                vals[_fillColIdx] = _rememberedFillValue;
            } else {
                _rememberedFillValue = val;
            }

            if (OutputColumnDefs != null) {
                object[] outRow = MapDataRow(vals);
                vals = outRow;
            }
            return true;
        }

    }
}
