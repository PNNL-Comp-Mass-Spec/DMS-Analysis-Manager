using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

// FUTURE: need property to tell accumulator whether or not to accept new column defs.
namespace MageClientLib {

    public delegate void ItemBlockRetrieved(ListViewItem[] itemBlock, ref bool stop);
    public delegate void ColumnBlockRetrieved(ColumnHeader[] columnBlock);

    // accumulates chunks of ListView item and column data
    // from individual row data which is received via callback
    // and passes on chunked data via delegate
    public class LVAccumulator {

        #region Events for ListDisplay listeners to register for

        public event ItemBlockRetrieved OnItemBlockRetrieved;
        public event ColumnBlockRetrieved OnColumnBlockRetrieved;

        #endregion

        #region Member Variables

        private List<ListViewItem> itemAccumulator = new List<ListViewItem>();
        private List<ColumnHeader> columnAccumulator = new List<ColumnHeader>();
        private List<Dictionary<string, string>> columnDefs = new List<Dictionary<string, string>>();

        #endregion

        #region Properties

        // number of data rows in an item block
        public int ItemBlockSize { get; set; }

        public List<Dictionary<string, string>> ColumnDefs {
            get {
                return columnDefs;
            }
        }

        #endregion

        #region Constructors

        public LVAccumulator() {
            ItemBlockSize = 1000;
        }

        #endregion

        #region Utility functions

        public void Clear() {
            itemAccumulator.Clear();
            columnAccumulator.Clear();
        }

        #endregion

        #region "Handlers for reader events"

        // receive data row, convert to ListView item, and add to accumulator
        public void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                ListViewItem lvi = null;
                for (int i = 0; i < vals.Length; i++) {
                    object val = vals[i];
                    string s = (val != null) ? val.ToString() : "-";
                    if (i == 0) {
                        lvi = new ListViewItem(val.ToString());
                    } else {
                        lvi.SubItems.Add(s);
                    }
                }
                this.itemAccumulator.Add(lvi);
            }
            if (itemAccumulator.Count == this.ItemBlockSize || vals == null) {
                if (OnItemBlockRetrieved != null) {
                    OnItemBlockRetrieved((ListViewItem[])itemAccumulator.ToArray(), ref stop);
                }
                this.itemAccumulator.Clear();
            }
            if (vals == null && OnItemBlockRetrieved != null) {
                OnItemBlockRetrieved(null, ref stop);
            }
        }

        public void HandleColumnDef(Dictionary<string, string> columnDef) {
            if (columnDef != null) {
                columnDefs.Add(columnDef); // remember the original details
                ColumnHeader ch = new ColumnHeader();

                // sort out column display size
                string colSize = columnDef["Size"];
                int colSizeToUse = 6;
                if (colSize != null && colSize.Length > 0) {
                    int w = int.Parse(colSize);
                    w = (w < 6) ? 6 : w;
                    w = (w > 20) ? 20 : w;
                    colSizeToUse = w;
                }

                int pixels = colSizeToUse * 10;
                ch.Text = columnDef["Name"];
                ch.Name = columnDef["Name"];
                ch.Width = pixels;
                string colType = columnDef["Type"];
                ch.Tag = colType;
                columnAccumulator.Add(ch);
            } else {
                if (this.OnColumnBlockRetrieved != null) {
                    OnColumnBlockRetrieved((ColumnHeader[])this.columnAccumulator.ToArray());
                }
            }
        }
        #endregion
    }
}
