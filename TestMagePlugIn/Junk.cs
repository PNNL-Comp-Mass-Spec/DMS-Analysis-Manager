using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManager_Mage_PlugIn;


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
            foreach(string name in lookup.Keys) {
                Console.WriteLine(string.Format("{0} -> {1}", name, lookup[name]));
            }

            ModuleAddAlias.StripOffCommonPrefix(lookup);

            Console.WriteLine("------");
            foreach (string name in lookup.Keys) {
                Console.WriteLine(string.Format("{0} -> {1}", name, lookup[name]));
            }

        }
    }
}
