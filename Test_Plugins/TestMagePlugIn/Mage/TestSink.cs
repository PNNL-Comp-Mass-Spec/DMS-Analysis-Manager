using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mage {

    class TestSink : BaseModule {

        #region IBaseModule Members
        //        public override event DataRowHandler DataRowAvailable;
        //        public override event ColumnDefHandler ColumnDefAvailable;
        //        public override event StatusMessageUpdated OnStatusMessageUpdated;


        public override void Prepare() {
            // nothing to do here
        }

        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                foreach (object obj in vals) {
                    System.Console.Write(obj.ToString() + "|");
                }
                System.Console.WriteLine();
            }
        }

        public override void HandleColumnDef(Dictionary<string, string> columnDef) {
            if (columnDef != null) {
                Console.WriteLine("-- Columns ---");
                foreach (KeyValuePair<string, string> def in columnDef) {
                    System.Console.WriteLine("Key:" + def.Key + " -> " + def.Value);
                }
            }
        }

        public override void Run(object state) {
            throw new NotImplementedException();
        }

        #endregion
    }
}
