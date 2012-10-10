using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;

namespace MageClientLib {

    public partial class ListDisplayControl : UserControl {

        #region "Delegate Functions"

        // callback for accessing fields from worker thread
        private delegate void ColumnBlockCallback(ColumnHeader[] columnBlock);
        private delegate void ItemBlockCallback(ListViewItem[] itemBlock);
        public delegate void NoticeCallback(string text);

        // callback for activation event
        public delegate void ActivationCallback(UserControl control, bool active);

        #endregion

        #region "Member Variables"

        protected enum eSqlDataColTypes {
            text,
            numInt,
            numFloat,
            date,
            binary
        }

        // Generic data type of each column
        private List<eSqlDataColTypes> mListViewColTypes = new List<eSqlDataColTypes>();

        // Convience array listing column names
        private List<string> mListViewColNames = new List<string>();

        // Sort info
        int mListViewSortColIndex = -1;
        bool mListViewSortAscending = true;

        #endregion

        #region Properties

        public string Notice { get { return this.lblNotice.Text; } set { this.lblNotice.Text = value; } }
        public string PageTitle { get { return this.lblPageTitle.Text; } set { this.lblPageTitle.Text = value; } }

        public LVAccumulator Accumulator { get; set; }

        public ListView List {
            get {
                return lvQueryResults;
            }
        }

        #endregion

        #region Constructors

        public ListDisplayControl() {
            InitializeComponent();
        }

        #endregion

        public void Clear() {
            lvQueryResults.Items.Clear();
            lvQueryResults.Columns.Clear();
            lvQueryResults.Update();
            lblNotice.Text = "";
        }

        #region "LVAccumulator Functions"
        // an external module (usually an LVAccumulator objet) provides
        // a block of column definitions and blocks of row items for the 
        // list view at the heart of this user control.
        // These blocks are delivered by the external module via events
        // that are connected to the HandleLVItemBlock and HandleColumnBlock
        // delegates.  Since the external object is usually running in its
        // own thread, there are appropriate helper functions to handle the
        // transfer of received objects to the actual list view control
        // running in the UI thread.

        // create a new LVAccumulator for this user control 
        // and wire up its events to this user control's event handlers
        public LVAccumulator MakeAccumulator() {
            Accumulator = new LVAccumulator();
            Accumulator.OnItemBlockRetrieved += this.HandleLVItemBlock;
            Accumulator.OnColumnBlockRetrieved += this.HandleColumnBlock;
            return Accumulator;
        }

        // this is a delegate that is called with a block of ListView items
        public void HandleLVItemBlock(ListViewItem[] itemBlock, ref bool stop) {
            NoticeCallback ncb = UpdateNoticeFld;
            if (itemBlock != null) {
                ItemBlockCallback lcb = UpdateListViewItems;
                Invoke(lcb, new object[] { itemBlock });
                Invoke(ncb, new object[] { "." });
            } else {
                Invoke(ncb, new object[] { string.Empty });
            }
        }

        // this is a delegate that is called with definitions for ListView columns
        public void HandleColumnBlock(ColumnHeader[] columnBlock) {
            //            lvQueryResults.Columns.AddRange(columnBlock);
            ColumnBlockCallback cb = UpdateListViewColumns;
            Invoke(cb, new object[] { columnBlock });
        }

        // target of invoke
        private void UpdateListViewColumns(ColumnHeader[] columnBlock) {
            lvQueryResults.Columns.AddRange(columnBlock);

            // Determine the data types for each column
            mListViewColTypes.Clear();
            mListViewColNames.Clear();

            // Parse out the data type from the .Tag member of each column
            for (int i = 0; i < lvQueryResults.Columns.Count; i++) {

                mListViewColNames.Add(lvQueryResults.Columns[i].Name);

                string colType = lvQueryResults.Columns[i].Tag.ToString();

                switch (colType) {
                    case "bit":
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                        // Integer number
                        mListViewColTypes.Add(eSqlDataColTypes.numInt);
                        break;

                    case "decimal":
                    case "real":
                    case "float":
                    case "numeric":
                    case "smallmoney":
                    case "money":
                        // Non-integer number
                        mListViewColTypes.Add(eSqlDataColTypes.numFloat);
                        break;

                    case "char":
                    case "varchar":
                    case "text":
                    case "nchar":
                    case "nvarchar":
                    case "ntext":
                    case "uniqueidentifier":
                    case "xml":
                        // Text-based data type
                        mListViewColTypes.Add(eSqlDataColTypes.text);
                        break;

                    case "date":
                    case "datetime":
                    case "datetime2":
                    case "smalldatetime":
                    case "time":
                    case "datetimeoffset":
                        // Date data type
                        mListViewColTypes.Add(eSqlDataColTypes.date);
                        break;

                    case "binary":
                    case "varbinary":
                    case "image":
                        mListViewColTypes.Add(eSqlDataColTypes.binary);
                        break;

                    default:
                        // Unknown data type
                        // If the data type contains "date" or "time", then treat as datetime
                        if (colType.Contains("date") || colType.Contains("time"))
                            mListViewColTypes.Add(eSqlDataColTypes.date);
                        else
                            // Assume text
                            mListViewColTypes.Add(eSqlDataColTypes.text);
                        break;
                }

            }
        }

        // target of invoke
        private void UpdateListViewItems(ListViewItem[] itemBlock) {
            int i = lvQueryResults.Items.Count;
            lvQueryResults.Items.AddRange(itemBlock);
            if (i > 0) lvQueryResults.Update();
        }

        // target of invoke
        private void UpdateNoticeFld(string text) {
            if (text != null && text == ".") {
                lblNotice.Text += ".";
                lblNotice.Update();
            } else {
                string strStatus;

                strStatus = lvQueryResults.Items.Count.ToString() + " row";
                if (lvQueryResults.Items.Count != 1)
                    strStatus += "s";

                if (text != null && text.Length > 0)
                    strStatus = text + "; " + strStatus;

                lblNotice.Text = strStatus;
            }
        }

        #endregion

        #region "Control Handlers"

        private void lvQueryResults_ColumnClicked(object sender, System.Windows.Forms.ColumnClickEventArgs e) {
            int intColIndex = e.Column;
            string strSortInfo;
            bool SortNumeric = false;
            bool SortDate = false;

            if (mListViewSortColIndex == intColIndex)
                // User clicked the same column; reverse the sort order
                mListViewSortAscending = !mListViewSortAscending;
            else
                // User clicked a new column, change the column sort index
                mListViewSortColIndex = intColIndex;

            switch (mListViewColTypes[intColIndex]) {
                case eSqlDataColTypes.numInt:
                case eSqlDataColTypes.numFloat:
                    SortNumeric = true;
                    break;
                case eSqlDataColTypes.date:
                    SortDate = true;
                    break;
            }


            strSortInfo = "Sort " + lvQueryResults.Columns[intColIndex].Text;

            if (!mListViewSortAscending)
                strSortInfo += " desc";

            if (SortNumeric)
                strSortInfo += " (numeric)";

            if (SortDate)
                strSortInfo += " (date)";

            //--           AddToMessageQueue(strSortInfo, (float)0.1);

            lvQueryResults.ListViewItemSorter = new ListViewItemComparer(e.Column, mListViewSortAscending, SortNumeric, SortDate);

            lvQueryResults.Update();

        }

        #endregion

        #region "Setup Functions"

        // create a context menu for the ListView and
        // add the given menu items to it
        public void SetPopupMenu(ToolStripItem[] items) {
            ContextMenuStrip mPopupMenu = new ContextMenuStrip();
            mPopupMenu.Items.AddRange(items.ToArray());
            lvQueryResults.ContextMenuStrip = mPopupMenu;
        }

        #endregion

        #region Pipeline Helper Functions

        public static void ConnectToPipeline(string title, ProcessingPipeline pipeline, string mod, ListDisplayControl lc, int blkSz) {
            lc.PageTitle = title;
            ConnectToPipeline(pipeline, mod, lc, blkSz);
        }

        public static void ConnectToPipeline(ProcessingPipeline pipeline, string mod, ListDisplayControl lc, int blkSz) {
            // connect list display control to pipeline via an accumulator object
            lc.Clear();
            LVAccumulator lva = lc.MakeAccumulator();
            pipeline.ConnectExternalModule(mod, lva.HandleColumnDef, lva.HandleDataRow);
            lva.ItemBlockSize = blkSz;
//            lc.PageTitle = mod;
        }


        #endregion

    }
}
