using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using log4net;
using System.Data;

namespace Mage {

    class SQLiteReader : BaseModule {
        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        #region Member Variables

        public int CommandTimeoutSeconds = 15;
        public string StatusMessage = string.Empty;

        DateTime startTime;
        DateTime stopTime;
        TimeSpan duration;

        private SQLiteConnection cn;

        #endregion

        #region Constructors

        public SQLiteReader() {
        }

        #endregion

        #region Properties
        /*
        public string server {
            get;
            set;
        }
*/
        public string database {
            get;
            set;
        }

        public string sqlText {
            get;
            set;
        }

        #endregion

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;

        public override void Run(Object state) {
            try {
                Connect();
                Access();
            } finally {
                Close();
            }
        }

        #endregion

        #region Private Functions

        private void Access() {
            SQLiteCommand cmd = new SQLiteCommand(this.sqlText, this.cn);
            cmd.CommandTimeout = CommandTimeoutSeconds;

            SQLiteDataReader myReader = cmd.ExecuteReader();
            GetData(myReader);
        }

        private void Connect() {
            SQLiteConnectionStringBuilder builder = new SQLiteConnectionStringBuilder();
            builder.DataSource = this.database;
            /*
                        if (password != null) {
                            builder.Password = password;
                        }
                        //builder.PageSize = 4096
                        //builder.UseUTF16Encoding = True
            */
            string connstring = builder.ConnectionString;
            this.cn = new SQLiteConnection(connstring);
            this.cn.Open();
        }

        private void Close() {
            cn.Close();
        }

        public void GetData(IDataReader myReader) {
            StatusMessage = string.Empty;

            if (myReader == null) { // Something went wrong
                UpdateStatusMessage("Error: SqlDataReader object is null");
                return;
            }

            OutputColumnDefinitions(myReader); // if ColumnDefAvailable

            int totalRows = 0;
            OutputDataRows(myReader, ref stop, ref totalRows);

            stopTime = DateTime.Now;
            duration = stopTime - startTime;
            traceLog.Info("SQLiteReader.GetData --> Get data finish (" + duration + ") [" + totalRows.ToString() + "]:" + sqlText);

            //Always close the DataReader
            myReader.Close();

        }

        private void OutputDataRows(IDataReader myReader, ref bool stop, ref int totalRows) {
            startTime = DateTime.Now;
            traceLog.Debug("SQLiteReader.GetData --> Get data start:" + sqlText);
            while (myReader.Read()) {
                if (this.DataRowAvailable != null) {
                    object[] a = new object[myReader.FieldCount];
                    myReader.GetValues(a);
                    DataRowAvailable(a, ref stop);
                    totalRows++;
                    if (stop) break;
                }
            }

            if (this.DataRowAvailable != null && !stop) {
                DataRowAvailable(null, ref stop);
            }
        }

        private void OutputColumnDefinitions(IDataReader myReader) {
            // if anyone is registered as listening for ColumnDefAvailable events, make it happen for them
            if (ColumnDefAvailable != null) {
                startTime = DateTime.Now;
                traceLog.Debug("SQLiteReader.GetData --> Get column info start:" + sqlText);
                // Determine the column names and column data types (

                // Get list of fields in result set and process each field
                DataTable schemaTable = myReader.GetSchemaTable();
                foreach (DataRow drField in schemaTable.Rows) {
                    Dictionary<string, string> columnDef = GetColumnInfo(drField);
                    foreach (DataColumn column in schemaTable.Columns) {
                        columnDef.Add(column.ColumnName, drField[column].ToString());
                    }
                    if (columnDef["Hidden"] == "No") {
                        // pass information about this column to the listeners
                        ColumnDefAvailable(columnDef);
                    } else {
                        // Column is marked as hidden; do not process it
                        UpdateStatusMessage("Skipping hidden column [" + columnDef["Name"] + "]");
                    }
                }

                // Signal that all columns have been read
                ColumnDefAvailable(null);
                stopTime = DateTime.Now;
                duration = stopTime - startTime;
                traceLog.Info("SQLiteReader.GetData --> Get column info finish (" + duration + "):" + sqlText);
            }
        }

        protected Dictionary<string, string> GetColumnInfo(DataRow drField) {
            // add the canonical column definition fields to column definition

            Dictionary<string, string> columnDef = new Dictionary<string, string>();
            columnDef["Name"] = drField["ColumnName"].ToString();
            columnDef["Type"] = drField["DataTypeName"].ToString();
            columnDef["Size"] = drField["ColumnSize"].ToString();

            string colHidden = drField["IsHidden"].ToString();
            if (colHidden == null || colHidden.Length == 0 || colHidden.ToLower() == "false" || colHidden == "") {
                columnDef["Hidden"] = "No";
            } else {
                columnDef["Hidden"] = "Yes";
            }

            return columnDef;
        }

        protected void UpdateStatusMessage(string message) {
            StatusMessage = message;
            if (OnStatusMessageUpdated != null)
                OnStatusMessageUpdated(message);

        }

        #endregion

    }
}
// Numeric types:   bit, tinyint, smallint, int, bigint, decimal, real, float, numeric, smallmoney, money
// String types:    char, varchar, text, nchar, nvarchar, ntext, uniqueidentifier, xml
// Datetime types:  date, datetime, datetime2, smalldatetime, time, datetimeoffset
// Binary types:    binary, varbinary, image

/*
 ---column---
AllowDBNull = False
BaseCatalogName = main
BaseColumnName = ID
BaseSchemaName = 
BaseServerName = 
BaseTableName = T_Data_Package
ColumnName = ID
ColumnOrdinal = 0
ColumnSize = 8
DataType = System.Int64
DataTypeName = integer
DefaultValue = 
IsAliased = False
IsAutoIncrement = False
IsExpression = False
IsHidden = False
IsKey = False
IsLong = False
IsReadOnly = False
IsRowVersion = False
IsUnique = False
NumericPrecision = 19
NumericScale = 0
ProviderSpecificDataType = 
ProviderType = 12
--
CollationType = BINARY
         */
