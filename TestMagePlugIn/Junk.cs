using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManager_Mage_PlugIn;
using System.Data;
using Mage;

namespace TestMagePlugIn {

    class Junk {

        public void TestAlias() {
            HashSet<string> dSet = new HashSet<string>() {
                "DTRA_iTRAQ_0_10Gy_C1_5Nov10_Griffin_10-06-15",
                "DTRA_iTRAQ_0_10Gy_C1_R2_5Nov10_Griffin_10-06-16",
                "DTRA_iTRAQ_0_10Gy_C2_5Nov10_Griffin_10-06-15",
                "DTRA_iTRAQ_0_10Gy_C2_R2_5Nov10_Griffin_10-06-16",
                "DTRA_iTRAQ_0_10Gy_C3_5Nov10_Griffin_10-06-15",
                "DTRA_iTRAQ_0_10Gy_C3_R2_5Nov10_Griffin_10-06-16",
            };

            Dictionary<string, string> lookup;

            lookup = ModuleAddAlias.BuildAliasLookupTable(dSet);

            Console.WriteLine("------");
            foreach (string name in lookup.Keys) {
                Console.WriteLine(string.Format("{0} -> {1}", name, lookup[name]));
            }

            ModuleAddAlias.StripOffCommonPrefix(lookup);

            Console.WriteLine("------");
            foreach (string name in lookup.Keys) {
                Console.WriteLine(string.Format("{0} -> {1}", name, lookup[name]));
            }

        }
/*
        #region DataTable stuff

        private void TestDataTableWriter() {
            DataTable table = GetTable(); // Get the test DataTable object
            WriteDataTableToSQLiteTable(table);
        }

        public void WriteDataTableToSQLiteTable(DataTable table) {

            // make the Mage module that will source the .Net DataTable object
            MageDataTableSource source = new MageDataTableSource();
            source.SourceTable = table;

            // make the Mage module that will write data to SQLite database
            SQLiteWriter writer = new SQLiteWriter();
            writer.DbPath = @"C:\DMS_WorkDir\TableTest.db3";
            writer.TableName = "test";

            // build and run Mage pipeline
            ProcessingPipeline.Assemble("Test_Pipeline", source, writer).RunRoot(null); 
        }

        private DataTable GetTable() {
            DataTable table = new DataTable(); // New data table.
            table.Columns.Add("Dosage", typeof(int)); // Add five columns.
            table.Columns.Add("Drug", typeof(string));
            table.Columns.Add("Patient", typeof(string));
            table.Columns.Add("Date", typeof(DateTime));
            table.Rows.Add(15, "Abilify", "Jacob", DateTime.Now); // Add five data rows.
            table.Rows.Add(40, "Accupril", "Emma", DateTime.Now);
            table.Rows.Add(40, "Accutane", "Michael", DateTime.Now);
            table.Rows.Add(20, "Aciphex", "Ethan", DateTime.Now);
            table.Rows.Add(45, "Actos", "Emily", DateTime.Now);
            return table; // Return reference.
        }

        #endregion
 */
    }
}
