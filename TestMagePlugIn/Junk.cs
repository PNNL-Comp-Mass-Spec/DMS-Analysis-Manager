using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestMagePlugIn {

    class Junk {

        public void TestAlias() {
            List<string> dList = new List<string> {
                "DTRA_iTRAQ_0_10Gy_C3_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C2_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C2_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C1_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C1_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C3_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C3_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C3_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C2_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C2_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C2_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C1_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C1_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C3_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C2_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C1_R2_5Nov10_Griffin_10-06-16", 
                "DTRA_iTRAQ_0_10Gy_C1_5Nov10_Griffin_10-06-15", 
                "DTRA_iTRAQ_0_10Gy_C3_R2_5Nov10_Griffin_10-06-16"
            };

            // find length of shortest item
            int pShortest = 0;
            foreach (string dItem in dList) {
                int len = dItem.Length;
                if (pShortest == 0) {
                    pShortest = len;
                }
                if (dItem.Length < pShortest) {
                    pShortest = len;
                }
            }

            // find largest common prefix
            int pSize = 1;
            bool same = true;
            string prefix = "";
            while (same) {
                string pTemp = "";
                foreach (string dItem in dList) {
                    string tmp = dItem.Substring(0, pSize);
                    char c = dItem.ElementAt(pSize);
                    if (string.IsNullOrEmpty(pTemp)) {
                        pTemp = tmp;
                    } else {
                        if (tmp != pTemp) {
                            same = false;
                            break;
                        }
                    }
                }
                if (same) {
                    pSize++;
                    prefix = pTemp;
                }
                if (pSize > pShortest) {
                    break;
                }
            }
            Console.WriteLine("boink");

        }
    }
}
