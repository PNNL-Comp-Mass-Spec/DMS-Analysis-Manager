using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using log4net;
using System.Data;

namespace Mage {

    public class MSSQLReader : BaseModule {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        #region member variables

        private SqlConnection cn;
        public string ConnectionString = "Data Source=@server@;Initial Catalog=@database@;integrated security=SSPI";

        public int CommandTimeoutSeconds = 15;

        public string StatusMessage = string.Empty;

        DateTime startTime;
        DateTime stopTime;
        TimeSpan duration;

        Dictionary<string, string> parms = new Dictionary<string, string>();

        #endregion

        #region Properties

        public string server { get; set; }

        public string database { get; set; }

        public string sqlText { get; set; }

        public string sprocName { get; set; }

        public void AddParm(string name, string value) {
            parms.Add(name, value);
        }

        #endregion

        #region Constructors

        public MSSQLReader() {
            sprocName = "";
        }

        public MSSQLReader(string server, string database) {
            this.server = server;
            this.database = database;
        }

        #endregion

        #region IBaseModule Members
        public override event DataRowHandler DataRowAvailable;
        public override event ColumnDefHandler ColumnDefAvailable;
        public override event StatusMessageUpdated OnStatusMessageUpdated;

        public override void Run(Object state) {
            try {
                Connect();
                if (sprocName != "") {
                    GetDataFromDatabaseSproc();
                } else {
                    GetDataFromDatabaseQuery();
                }
            } finally {
                Close();
            }
        }

        #endregion

        #region Private Functions

        private void Connect() {
            string cnStr = this.ConnectionString.Replace("@server@", this.server);
            cnStr = cnStr.Replace("@database@", this.database);
            cn = new SqlConnection();
            cn.ConnectionString = cnStr;
            cn.Open();
        }

        private void Close() {
            cn.Close();
        }

        // run SQL query against database
        private void GetDataFromDatabaseQuery() {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = this.cn;
            cmd.CommandText = this.sqlText;
            cmd.CommandTimeout = CommandTimeoutSeconds;

            SqlDataReader myReader = cmd.ExecuteReader();
            GetData(myReader);
        }

        public void GetDataFromDatabaseSproc() {
            SqlCommand myCmd = GetSprocCmd(sprocName, parms);
            SqlDataReader myReader = myCmd.ExecuteReader();
            GetData(myReader);
        }


        public string GetName() {
            return this.sqlText;
        }

        public void GetData(IDataReader myReader) {
            StatusMessage = string.Empty;

            if (myReader == null) {
                // Something went wrong
                UpdateStatusMessage("Error: SqlDataReader object is null");
                return;
            }

            OutputColumnDefinitions(myReader);

            int totalRows = 0;
            OutputDataRows(myReader, ref stop, ref totalRows);

            stopTime = DateTime.Now;
            duration = stopTime - startTime;
            traceLog.Info("MSSQLReader.GetData --> Get data finish (" + duration + ") [" + totalRows.ToString() + "]:" + sqlText);

            //Always close the DataReader
            myReader.Close();
        }

        private void OutputDataRows(IDataReader myReader, ref bool stop, ref int totalRows) {
            // now do all the rows - if anyone is registered as wanting them
            if (this.DataRowAvailable != null) {
                startTime = DateTime.Now;
                traceLog.Debug("MSSQLReader.GetData --> Get data start:" + sqlText);
                while (myReader.Read()) {
                    object[] a = new object[myReader.FieldCount];
                    myReader.GetValues(a);
                    DataRowAvailable(a, ref stop);
                    totalRows++;
                    if (stop) break;
                }
            }

            // Signal listeners that all data rows have been read
            if (this.DataRowAvailable != null && !stop) {
                DataRowAvailable(null, ref stop);
            }
        }

        private void OutputColumnDefinitions(IDataReader myReader) {
            // if anyone is registered as listening for ColumnDefAvailable events, make it happen for them
            if (ColumnDefAvailable != null) {
                startTime = DateTime.Now;
                traceLog.Debug("MSSQLReader.GetData --> Get column info start:" + sqlText);
                // Determine the column names and column data types (

                // Get list of fields in result set and process each field
                DataTable schemaTable = myReader.GetSchemaTable();
                foreach (DataRow drField in schemaTable.Rows) {
                    // initialize column definition with canonical fields
                    Dictionary<string, string> columnDef = GetColumnInfo(drField);
                    // now add native fields to column definition
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
                traceLog.Info("MSSQLReader.GetData --> Get column info finish (" + duration + "):" + sqlText);
            } // if ColumnDefAvailable
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

        #region Code for stored procedures

        // return a SqlCommand suitable for calling the given stored procedure
        // with the given argument values
        private SqlCommand GetSprocCmd(string sprocName, Dictionary<string, string> parms) {

            // start the SqlCommand that we are building up for the sproc
            SqlCommand builtCmd = new SqlCommand();
            builtCmd.Connection = cn;
            try {
                // query the database to get argument definitions for the given stored procedure
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = cn;
                string sqlText = string.Format("SELECT * FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = '{0}'", sprocName);
                cmd.CommandText = sqlText;
                //
                SqlDataReader rdr = cmd.ExecuteReader();

                // column positions for the argument data we need
                int namIdx = rdr.GetOrdinal("PARAMETER_NAME");
                int typIdx = rdr.GetOrdinal("DATA_TYPE");
                int modIdx = rdr.GetOrdinal("PARAMETER_MODE");
                int sizIdx = rdr.GetOrdinal("CHARACTER_MAXIMUM_LENGTH");

                // more stuff for the SqlCommand being built
                builtCmd.CommandType = CommandType.StoredProcedure;
                builtCmd.CommandText = sprocName;
                builtCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
                builtCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

                // loop through all the arguments and add a parameter for each one
                // the the SqlCommand being built
                while (rdr.Read()) {
                    object[] a = new object[rdr.FieldCount];
                    rdr.GetValues(a);
                    string argName = a[namIdx].ToString();
                    string argType = a[typIdx].ToString();
                    string argMode = a[modIdx].ToString();
                    switch (argType) {
                        case "int":
                            builtCmd.Parameters.Add(new SqlParameter(argName, SqlDbType.Int));
                            builtCmd.Parameters[argName].Direction = (argMode == "INOUT") ? ParameterDirection.Output : ParameterDirection.Input;
                            if (parms.ContainsKey(argName)) {
                                int val;
                                Int32.TryParse(parms[argName], out val);
                                builtCmd.Parameters[argName].Value = val;
                            }
                            break;
                        case "varchar":
                            Int32 size = (Int32)a[sizIdx];
                            builtCmd.Parameters.Add(new SqlParameter(argName, SqlDbType.VarChar, size));
                            builtCmd.Parameters[argName].Direction = (argMode == "INOUT") ? ParameterDirection.Output : ParameterDirection.Input;
                            if (parms.ContainsKey(argName)) {
                                builtCmd.Parameters[argName].Value = parms[argName];
                            }
                            break;
                        // FUTURE: Add code for more data types
                        default:
                            Console.WriteLine("Couldn't figure out " + argName);
                            break;
                    }
                }
                rdr.Close();
            } catch (Exception e) {
                Console.WriteLine(e.Message);
            }
            return builtCmd;
        }
        #endregion

    }
}

// Numeric types:   bit, tinyint, smallint, int, bigint, decimal, real, float, numeric, smallmoney, money
// String types:    char, varchar, text, nchar, nvarchar, ntext, uniqueidentifier, xml
// Datetime types:  date, datetime, datetime2, smalldatetime, time, datetimeoffset
// Binary types:    binary, varbinary, image

/*
---column def object ---
AllowDBNull = False
BaseCatalogName = 
BaseColumnName = Job
BaseSchemaName = 
BaseServerName = 
BaseTableName = 
ColumnName = Job
ColumnOrdinal = 0
ColumnSize = 4
DataType = System.Int32
DataTypeName = int
IsAliased = 
IsAutoIncrement = False
IsColumnSet = False
IsExpression = 
IsHidden = 
IsIdentity = False
IsKey = 
IsLong = False
IsReadOnly = False
IsRowVersion = False
IsUnique = False
NonVersionedProviderType = 8
NumericPrecision = 10
NumericScale = 255
ProviderSpecificDataType = System.Data.SqlTypes.SqlInt32
ProviderType = 8
--
UdtAssemblyQualifiedName = 
XmlSchemaCollectionDatabase = 
XmlSchemaCollectionName = 
XmlSchemaCollectionOwningSchema = 
 */
