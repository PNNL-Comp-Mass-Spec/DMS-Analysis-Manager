using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;

// this is a pipeline module 
// that can serve the contents of a ListDisplayControl to standard tabular output

namespace MageClientLib {

    public class LVPipelineSource : BaseModule {
        // adapter for making rows in a ListDisplayControl object
        // available via Mage pipeline data source module connections

        #region Member Variables

        // object whose data we are serving
        private ListDisplayControl myListControl = null;

        public enum Modes { All, Selected }

        #endregion

        #region Constructors

        public LVPipelineSource(ListDisplayControl lc, Modes mode) {
            myListControl = lc;
            if (lc.List.Items.Count == 0) {
                throw new Exception("There are no items to process");
            }
            if (mode == LVPipelineSource.Modes.Selected && lc.List.SelectedItems.Count == 0) {
                throw new Exception("There are no items selected to process");
            }
            GetRowsFromList(mode);
        }

        private List<List<string>> RowBuffer = new List<List<string>>();

        #endregion

        #region Properties

        public bool Stop { get { return stop; } set { stop = value; } }

        #endregion

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
//        public override event StatusMessageUpdated OnStatusMessageUpdated;


        public override void Run(object state) {
            OutputListItems();
        }

        #endregion

        #region Private Functions

        private void GetRowsFromList(Modes mode) {
            switch (mode) {
                case Modes.All:
                    foreach (ListViewItem item in myListControl.List.Items) {
                        List<string> row = new List<string>();
                        foreach (ListViewItem.ListViewSubItem subItem in item.SubItems) {
                            row.Add(subItem.Text);
                        }
                        RowBuffer.Add(row);
                    }
                    break;
                case Modes.Selected:
                    foreach (ListViewItem item in myListControl.List.SelectedItems) {
                        List<string> row = new List<string>();
                        foreach (ListViewItem.ListViewSubItem subItem in item.SubItems) {
                            row.Add(subItem.Text);
                        }
                        RowBuffer.Add(row);
                    }
                    break;
            }
        }

        private void OutputListItems() {

            // output the original column definitions for the list control
            // from its accumulator
            if (ColumnDefAvailable != null) {
                foreach (Dictionary<string, string> colDef in myListControl.Accumulator.ColumnDefs) {
                    ColumnDefAvailable(colDef);
                }
                ColumnDefAvailable(null);
            }

            // output the rows from the list control according to current mode setting
            if (DataRowAvailable != null) {
                foreach (List<string> row in RowBuffer) {
                    if (stop) break;
                    DataRowAvailable(row.ToArray(), ref stop);
                }
                DataRowAvailable(null, ref stop);
            }
        }

        #endregion

    }
}
