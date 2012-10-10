using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using System.Data.SQLite;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;

namespace Mage {
    class SQLiteWriter : BaseModule {
        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog"); // traceLog.Debug

        #region Member Variables

        // buffer for accumulating rows into output block
        private List<object[]> mRows = new List<object[]>();

        // description of table we will be inserting rows into
        private TableSchema mSchema = null;

        // connection to SQLite database 
        private SQLiteConnection mConn = null;

        private int mRowsAccumulated = 0;

        private int mBlockSize = 1000;

        #endregion

        #region Properties

        public string TableName { get; set; }

        public string DbPath { get; set; }

        public string DbPassword { get; set; }

        // number of input rows that are grouped into SQLite transaction blocks
        public string BlockSize {
            get { return mBlockSize.ToString(); }
            set {
                int val = 0;
                if (int.TryParse(value, out val)) {
                    mBlockSize = val;
                }
            }
        }

        #endregion

        #region Constructors

        public SQLiteWriter() {
        }

        #endregion


        #region IBaseModule Members

        public override event StatusMessageUpdated OnStatusMessageUpdated;

        public override void Cleanup() {
            base.Cleanup();
            CloseDBConnection();
        }

        public override void SetParameters(List<KeyValuePair<string, string>> parameters) {
            base.SetParameters(parameters);
        }

        // build definition of output columns
        public override void HandleColumnDef(Dictionary<string, string> columnDef) {
            base.HandleColumnDef(columnDef);
            // build lookup of column index by column name
            if (columnDef == null) {
                // make table schema
                List<Dictionary<string, string>> colDefs = (OutputColumnDefs == null) ? InputColumnDefs : OutputColumnDefs;
                mSchema = MakeTableSchema(colDefs);
                // create db and table in database
                CreateTableInDatabase();
            }
        }

        // receive data row, add to accumulator, write to SQLite when buffer is full, or reader finishes
        public override void HandleDataRow(object[] vals, ref bool stop) {
            if (vals != null) {
                if (++mRowsAccumulated < mBlockSize) {
                    // accumulate row
                    if (OutputColumnDefs != null) {
                        mRows.Add(MapDataRow(vals));
                    } else {
                        mRows.Add(vals);
                    }
                } else {
                    mRowsAccumulated = 0;
                    // do trasaction block against SQLite database
                    CopyTabularDataRowsToSQLiteDB();
                    mRows.Clear();
                }
            } else {
                if (mRowsAccumulated > 0) {
                    // do trasaction block against SQLite database
                    CopyTabularDataRowsToSQLiteDB();
                }
            }
        }

        #endregion

        #region Helper Functions

        TableSchema MakeTableSchema(List<Dictionary<string, string>> colDefs) {
            TableSchema ts = new TableSchema();
            ts.TableName = TableName;
            ts.Columns = new List<ColumnSchema>();
            foreach (Dictionary<string, string> colDef in colDefs) {
                ColumnSchema cs = new ColumnSchema();
                cs.ColumnName = colDef["Name"];
                string type = colDef["Type"];
                if (type.Contains("char")) {
                    cs.ColumnType = "text";
                } else {
                    cs.ColumnType = type;
                }
                ts.Columns.Add(cs);
            }
            return ts;
        }

        private void UpdateStatus(string message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(message);
            }
        }

        #endregion

        #region "Top level SQLite Stuff"

        private void AssureDBConnection() {
            if (mConn == null) {
                string sqliteConnString = CreateSQLiteConnectionString(DbPath, DbPassword);
                mConn = new SQLiteConnection(sqliteConnString);
                mConn.Open();
            }
        }
        private void CloseDBConnection() {
            if (mConn != null) {
                mConn.Close();
            }
        }

        private void CreateTableInDatabase() {
            // create the target file if it doesn't exist.
            if (!File.Exists(DbPath)) {
                CreateSQLiteDatabaseOnly(DbPath);
                //File.Delete(DbPath)
            }
            // Connect to the database
            AssureDBConnection();
            try {
                AddSQLiteTable();
            } catch (Exception ex) {
                traceLog.Debug("AddSQLiteTable failed: " + ex.Message);
            }
            traceLog.Debug("added schema for SQLite table [" + mSchema.TableName + "]");
        }


        private void CopyTabularDataRowsToSQLiteDB() {
            traceLog.Debug("preparing to insert tablular data ...");

            AssureDBConnection();
            SQLiteTransaction tx = mConn.BeginTransaction();
            try {
                string tableQuery = BuildSqlServerTableQuery(mSchema);
                traceLog.Debug("Starting to insert block of rows for table [" + mSchema.TableName + "]");

                SQLiteCommand insert = BuildSQLiteInsert(mSchema);

                foreach (object[] row in mRows) {
                    insert.Connection = mConn;
                    insert.Transaction = tx;
                    List<string> pnames = new List<string>();
                    for (int j = 0; j <= mSchema.Columns.Count - 1; j++) {
                        string pname = "@" + GetNormalizedName(mSchema.Columns[j].ColumnName, pnames);
                        insert.Parameters[pname].Value = CastValueForColumn(row[j], mSchema.Columns[j]);
                        pnames.Add(pname);
                    }
                    insert.ExecuteNonQuery();
                }// foreach

                tx.Commit();

                traceLog.Debug("finished inserting block of rows for table [" + mSchema.TableName + "]");
            } catch (Exception ex) {
                tx.Rollback();
                traceLog.Debug("unexpected exception: " + ex.Message);
                UpdateStatus("unexpected exception: " + ex.Message);
            }
        }

        #endregion

        #region "General SQLite Stuff"

        // Creates the CREATE TABLE DDL for SQLite and a specific table.
        private void AddSQLiteTable() {
            // Prepare a CREATE TABLE DDL statement
            string stmt = BuildCreateTableQuery(mSchema);

            traceLog.Info(System.Environment.NewLine + System.Environment.NewLine + stmt + System.Environment.NewLine + System.Environment.NewLine);

            // Execute the query in order to actually create the table.
            AssureDBConnection();
            SQLiteCommand cmd = new SQLiteCommand(stmt, mConn);
            cmd.ExecuteNonQuery();
        }

        // returns the CREATE TABLE DDL for creating the SQLite table 
        // from the specified table schema object.
        private string BuildCreateTableQuery(TableSchema ts) {
            StringBuilder sb = new StringBuilder();

            sb.Append("CREATE TABLE [" + ts.TableName + "] (" + System.Environment.NewLine);

            bool pkey = false;
            for (int i = 0; i <= ts.Columns.Count - 1; i++) {
                ColumnSchema col = ts.Columns[i];
                string cline = BuildColumnStatement(col, ts, ref pkey);
                sb.Append(cline);
                if (i < ts.Columns.Count - 1) {
                    sb.Append("," + System.Environment.NewLine);
                }
            }
            sb.Append(System.Environment.NewLine);
            sb.Append(");" + System.Environment.NewLine);

            string query = sb.ToString();
            return query;
        }

        /// Used when creating the CREATE TABLE DDL. Creates a single row
        /// for the specified column.
        private string BuildColumnStatement(ColumnSchema col, TableSchema ts, ref bool pkey) {
            StringBuilder sb = new StringBuilder();
            sb.Append("\t" + "\"" + col.ColumnName + "\"" + "\t" + "\t");

            if (col.ColumnType == "int") {
                sb.Append("integer");
            } else {
                sb.Append(col.ColumnType);
            }
            //End If
            if (!col.IsNullable) {
                sb.Append(" NOT NULL");
            }

            string defval = StripParens(col.DefaultValue);
            defval = DiscardNational(defval);
            //traceLog.Debug(("DEFAULT VALUE BEFORE [" & col.DefaultValue & "] AFTER [") + defval & "]")
            if (defval != string.Empty && defval.ToUpper().Contains("GETDATE")) {
                traceLog.Debug("converted SQL Server GETDATE() to CURRENT_TIMESTAMP for column [" + col.ColumnName + "]");
                sb.Append(" DEFAULT (CURRENT_TIMESTAMP)");
            } else if (defval != string.Empty && IsValidDefaultValue(defval)) {
                sb.Append(" DEFAULT " + defval);
            }

            return sb.ToString();
        }

        // Creates SQLite connection string from the specified DB file path.
        private string CreateSQLiteConnectionString(string sqlitePath, string password) {
            SQLiteConnectionStringBuilder builder = new SQLiteConnectionStringBuilder();
            builder.DataSource = sqlitePath;
            if (password != null) {
                builder.Password = password;
            }
            //builder.PageSize = 4096
            //builder.UseUTF16Encoding = True
            string connstring = builder.ConnectionString;

            return connstring;
        }

        // Creates the SQLite database from the schema read from the SQL Server.
        private void CreateSQLiteDatabaseOnly(string sqlitePath) {
            traceLog.Debug("Creating SQLite database...");

            // Create the SQLite database file
            SQLiteConnection.CreateFile(sqlitePath);

            traceLog.Debug("SQLite file was created successfully at [" + sqlitePath + "]");

        }

        // Builds a SELECT query for a specific table. Needed in the process of copying rows
        // from the SQL Server database to the SQLite database.
        private string BuildSqlServerTableQuery(TableSchema ts) {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            for (int i = 0; i <= ts.Columns.Count - 1; i++) {
                sb.Append("[" + ts.Columns[i].ColumnName + "]");
                if (i < ts.Columns.Count - 1) {
                    sb.Append(", ");
                }
            }
            // for
            sb.Append(" FROM [" + ts.TableName + "]");
            return sb.ToString();
        }

        // Creates a command object needed to insert values into a specific SQLite table.
        private SQLiteCommand BuildSQLiteInsert(TableSchema ts) {
            SQLiteCommand res = new SQLiteCommand();

            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO [" + ts.TableName + "] (");
            for (int i = 0; i <= ts.Columns.Count - 1; i++) {
                sb.Append("[" + ts.Columns[i].ColumnName + "]");
                if (i < ts.Columns.Count - 1) {
                    sb.Append(", ");
                }
            }
            // for
            sb.Append(") VALUES (");

            List<string> pnames = new List<string>();
            for (int i = 0; i <= ts.Columns.Count - 1; i++) {
                string pname = "@" + GetNormalizedName(ts.Columns[i].ColumnName, pnames);
                sb.Append(pname);
                if (i < ts.Columns.Count - 1) {
                    sb.Append(", ");
                }

                DbType dbType = GetDbTypeOfColumn(ts.Columns[i]);
                SQLiteParameter prm = new SQLiteParameter(pname, dbType, ts.Columns[i].ColumnName);
                res.Parameters.Add(prm);

                // Remember the parameter name in order to avoid duplicates
                pnames.Add(pname);
            }
            // for
            sb.Append(")");
            res.CommandText = sb.ToString();
            res.CommandType = CommandType.Text;
            return res;
        }

        // Used in order to avoid breaking naming rules (e.g., when a table has
        // a name in SQL Server that cannot be used as a basis for a matching index
        // name in SQLite).
        private string GetNormalizedName(string str, List<string> names) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i <= str.Length - 1; i++) {
                if (Char.IsLetterOrDigit(str[i]) || str[i] == '_') {
                    sb.Append(str[i]);
                } else {
                    sb.Append("_");
                }
            }
            // for
            // Avoid returning duplicate name
            if (names.Contains(sb.ToString())) {
                return GetNormalizedName(sb.ToString() + "_", names);
            } else {
                return sb.ToString();
            }
        }

        // Used in order to adjust the value received from SQL Server for the SQLite database.
        private object CastValueForColumn(object val, ColumnSchema columnSchema) {
            if (val is DBNull) {
                return null;
            }

            DbType dt = GetDbTypeOfColumn(columnSchema);

            switch (dt) {
                case DbType.Int32:
                    if (val == "") {
                        return null;
                    }
                    if (val is short) {
                        return Convert.ToInt32(Convert.ToInt16(val));
                    }
                    if (val is byte) {
                        return Convert.ToInt32(Convert.ToByte(val));
                    }
                    if (val is long) {
                        return Convert.ToInt32(Convert.ToInt64(val));
                    }
                    if (val is decimal) {
                        return Convert.ToInt32(Convert.ToDecimal(val));
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.Int16:
                    if (val is int) {
                        return Convert.ToInt16(Convert.ToInt32(val));
                    }
                    if (val is byte) {
                        return Convert.ToInt16(Convert.ToByte(val));
                    }
                    if (val is long) {
                        return Convert.ToInt16(Convert.ToInt64(val));
                    }
                    if (val is decimal) {
                        return Convert.ToInt16(Convert.ToDecimal(val));
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.Int64:
                    if (val is int) {
                        return Convert.ToInt64(Convert.ToInt32(val));
                    }
                    if (val is short) {
                        return Convert.ToInt64(Convert.ToInt16(val));
                    }
                    if (val is byte) {
                        return Convert.ToInt64(Convert.ToByte(val));
                    }
                    if (val is decimal) {
                        return Convert.ToInt64(Convert.ToDecimal(val));
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.Single:
                    if (val is double) {
                        return Convert.ToSingle(Convert.ToDouble(val));
                    }
                    if (val is decimal) {
                        return Convert.ToSingle(Convert.ToDecimal(val));
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.Double:
                    if (val is float) {
                        return Convert.ToDouble(Convert.ToSingle(val));
                    }
                    if (val is double) {
                        return Convert.ToDouble(val);
                    }
                    if (val is decimal) {
                        return Convert.ToDouble(Convert.ToDecimal(val));
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.String:
                    if (val is Guid) {
                        return ((Guid)val).ToString();
                    }
                    break; // TODO: might not be correct. Was : Exit Select

                case DbType.Binary:
                case DbType.Boolean:
                case DbType.DateTime:
                    break; // TODO: might not be correct. Was : Exit Select
                default:

                    traceLog.Error("argument exception - illegal database type");
                    throw new ArgumentException("Illegal database type [" + Enum.GetName(typeof(DbType), dt) + "]");
            }
            // switch
            return val;
        }

        /// Matches SQL Server types to general DB types
        private DbType GetDbTypeOfColumn(ColumnSchema cs) {
            if (cs.ColumnType == "tinyint") {
                return DbType.Byte;
            }
            if (cs.ColumnType == "int") {
                return DbType.Int32;
            }
            if (cs.ColumnType == "smallint") {
                return DbType.Int16;
            }
            if (cs.ColumnType == "bigint") {
                return DbType.Int64;
            }
            if (cs.ColumnType == "bit") {
                return DbType.Boolean;
            }
            if (cs.ColumnType == "nvarchar" || cs.ColumnType == "varchar" || cs.ColumnType == "text" || cs.ColumnType == "ntext") {
                return DbType.String;
            }
            if (cs.ColumnType == "float") {
                return DbType.Double;
            }
            if (cs.ColumnType == "real") {
                return DbType.Single;
            }
            if (cs.ColumnType == "blob") {
                return DbType.Binary;
            }
            if (cs.ColumnType == "numeric") {
                return DbType.Double;
            }
            if (cs.ColumnType == "timestamp" || cs.ColumnType == "datetime") {
                return DbType.DateTime;
            }
            if (cs.ColumnType == "nchar" || cs.ColumnType == "char") {
                return DbType.String;
            }
            if (cs.ColumnType == "uniqueidentifier") {
                return DbType.String;
            }
            if (cs.ColumnType == "xml") {
                return DbType.String;
            }
            if (cs.ColumnType == "sql_variant") {
                return DbType.Object;
            }
            if (cs.ColumnType == "integer") {
                return DbType.Int64;
            }
            if (cs.ColumnType == "double") {
                return DbType.Double;
            }

            traceLog.Error("GetDbTypeOfColumn: illegal db type found");
            throw new ApplicationException("GetDbTypeOfColumn: Illegal DB type found (" + cs.ColumnType + ")");
        }

        // Strip any parentheses from the string.
        private string StripParens(string value) {
            Regex rx = new Regex("\\(([^\\)]*)\\)");
            Match m = rx.Match(value);
            if (!m.Success) {
                return value;
            } else {
                return StripParens(m.Groups[1].Value);
            }
        }

        // Check if the DEFAULT clause is valid by SQLite standards
        private bool IsValidDefaultValue(string value) {
            if (IsSingleQuoted(value)) {
                return true;
            }

            double testnum = 0;
            if (!double.TryParse(value, out testnum)) {
                return false;
            }
            return true;
        }

        private bool IsSingleQuoted(string value) {
            value = value.Trim();
            if (value.StartsWith("'") && value.EndsWith("'")) {
                return true;
            }
            return false;
        }

        // Discards the national prefix if exists (e.g., N'sometext') which is not supported in SQLite.
        private string DiscardNational(string value) {
            Regex rx = new Regex("N\\'([^\\']*)\\'");
            Match m = rx.Match(value);
            if (m.Success) {
                return m.Groups[1].Value;
            } else {
                return value;
            }
        }

        #endregion

        #region "Internal classes for SQLite

        private class ColumnSchema {
            public string ColumnName = "";
            public string ColumnType = "";
            public bool IsNullable = true;
            public string DefaultValue = "";
            public bool IsIdentity = false;
            public bool IsCaseSensitivite = false; // null??
        }

        private class TableSchema {
            public string TableName = "";
            public List<ColumnSchema> Columns = null;
        }
        #endregion

    }


}
