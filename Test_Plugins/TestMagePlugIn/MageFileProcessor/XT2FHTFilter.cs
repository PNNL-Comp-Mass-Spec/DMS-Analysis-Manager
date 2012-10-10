using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Mage;

namespace MageFileProcessor {

    [MageAttributes("Filter", "XT2FHT", "Ascore XT2FHT", "Convert XT results files to SEQUEST FHT format")]
    class XT2FHTFilter : ContentFilter {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        // this is called for each row that is being subjected to filtering
        // the fields array contains value of each column for the row
        // the column index of each field can be looked up by field name in columnPos[]
        // to prevent the row from being sent to the ouput, return false
        protected override bool CheckFilter(ref object[] fields) {
            bool accepted = false;

            if (OutputColumnDefs != null) {
                object[] outRow = MapDataRow(fields);
                fields = outRow;
            }
            accepted = true;

            return accepted;
        }

        // this filter module sets up its own column remapping
        public override void Prepare() {
            base.Prepare();
            OutputColumnList = GetFilterColumnMap();
        }

        // this is a function that returns an output column map
        private string GetFilterColumnMap() {
            List<string> colMapfields = new List<string>();
            colMapfields.Add("HitNum|+|text");
            colMapfields.Add("ScanNum|Scan");
            colMapfields.Add("ScanCount|+|text");
            colMapfields.Add("ChargeState|Charge");
            colMapfields.Add("MH|Peptide_MH");
            colMapfields.Add("XCorr|+|text");
            colMapfields.Add("DelCn|+|text");
            colMapfields.Add("Sp|+|text");
            colMapfields.Add("Reference|+|text");
            colMapfields.Add("MultiProtein|Multiple_Protein_Count");
            colMapfields.Add("Peptide|Peptide_Sequence");
            colMapfields.Add("DelCn2|DeltaCn2");
            colMapfields.Add("RankSp|+|text");
            colMapfields.Add("RankXc|+|text");
            colMapfields.Add("DelM|Delta_Mass");
            colMapfields.Add("XcRatio|+|text");
            colMapfields.Add("PassFilt|+|text");
            colMapfields.Add("MScore|+|text");
            colMapfields.Add("NumTrypticEnds|+|text");
            return string.Join(", ", colMapfields.ToArray());
        }

        // delegate that handles renaming of source file to output file 
        public override string RenameOutputFile(string sourceFile, Dictionary<string, int> fieldPos, object[] fields) {
            return sourceFile.Replace("_xt", "_fht");
        }

    }
}
