using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;

namespace AnalysisManagerBase
{
    public class clsSqLiteUtilities
    {

        /// <summary>
        /// Clones a database, optionally skipping tables in list tablesToSkip
        /// </summary>
        /// <param name="sourceDBPath">Source database path</param>
        /// <param name="targetDBPath">Target database path</param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>If the target database already exists, then missing tables (and data) will be appended to the file</remarks>
        public bool CloneDB(string sourceDBPath, string targetDBPath)
        {
            const bool appendToExistingDB = true;
            var tablesToSkip = new List<string>();
            return CloneDB(sourceDBPath, targetDBPath, appendToExistingDB, tablesToSkip);
        }

        /// <summary>
        /// Clones a database, optionally skipping tables in list tablesToSkip
        /// </summary>
        /// <param name="sourceDBPath">Source database path</param>
        /// <param name="targetDBPath">Target database path</param>
        /// <param name="appendToExistingDB">Behavior when the target DB exists; if True, then missing tables will be appended to the database; if False, then the target DB will be deleted</param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneDB(string sourceDBPath, string targetDBPath, bool appendToExistingDB)
        {
            var tablesToSkip = new List<string>();
            return CloneDB(sourceDBPath, targetDBPath, appendToExistingDB, tablesToSkip);
        }

        /// <summary>
        /// Clones a database, optionally skipping tables in list tablesToSkip
        /// </summary>
        /// <param name="sourceDBPath">Source database path</param>
        /// <param name="targetDBPath">Target database path</param>
        /// <param name="appendToExistingDB">Behavior when the target DB exists; if True, then missing tables will be appended to the database; if False, then the target DB will be deleted</param>
        /// <param name="tablesToSkip">A list of table names (e.g. Frame_Scans) that should not be copied.</param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneDB(string sourceDBPath, string targetDBPath, bool appendToExistingDB, List<string> tablesToSkip)
        {

            var currentTable = string.Empty;
            var appendingToExistingDB = false;


            try
            {
                using (var cnSourceDB = new SQLiteConnection("Data Source = " + sourceDBPath))
                {
                    cnSourceDB.Open();

                    // Get list of tables in source DB					
                    var dctTableInfo = GetDBObjects(cnSourceDB, "table");

                    // Delete the "sqlite_sequence" database from dctTableInfo if present
                    if (dctTableInfo.ContainsKey("sqlite_sequence"))
                    {
                        dctTableInfo.Remove("sqlite_sequence");
                    }

                    // Get list of indices in source DB
                    Dictionary<string, string> dctIndexToTableMap;
                    var dctIndexInfo = GetDBObjects(cnSourceDB, "index", out dctIndexToTableMap);

                    if (File.Exists(targetDBPath))
                    {
                        if (appendToExistingDB)
                        {
                            appendingToExistingDB = true;
                        }
                        else
                        {
                            File.Delete(targetDBPath);
                        }
                    }

                    try
                    {
                        var sTargetConnectionString = ("Data Source = " + targetDBPath) + "; Version=3; DateTimeFormat=Ticks;";
                        var cnTargetDB = new SQLiteConnection(sTargetConnectionString);

                        cnTargetDB.Open();
                        var cmdTargetDB = cnTargetDB.CreateCommand();


                        Dictionary<string, string> dctExistingTables;
                        if (appendingToExistingDB)
                        {
                            // Lookup the table names that already exist in the target
                            dctExistingTables = GetDBObjects(cnTargetDB, "table");
                        }
                        else
                        {
                            dctExistingTables = new Dictionary<string, string>();
                        }

                        // Create each table
                        foreach (var kvp in dctTableInfo)
                        {
                            if (!string.IsNullOrEmpty(kvp.Value))
                            {
                                if (dctExistingTables.ContainsKey(kvp.Key))
                                {
                                    if (!tablesToSkip.Contains(kvp.Key))
                                    {
                                        tablesToSkip.Add(kvp.Key);
                                    }
                                }
                                else
                                {
                                    currentTable = string.Copy(kvp.Key);
                                    cmdTargetDB.CommandText = kvp.Value;
                                    cmdTargetDB.ExecuteNonQuery();
                                }
                            }
                        }

                        foreach (var kvp in dctIndexInfo)
                        {
                            if (!string.IsNullOrEmpty(kvp.Value))
                            {
                                var createIndex = true;

                                if (appendingToExistingDB)
                                {
                                    string indexTargetTable;
                                    if (dctIndexToTableMap.TryGetValue(kvp.Key, out indexTargetTable))
                                    {
                                        if (dctExistingTables.ContainsKey(indexTargetTable))
                                        {
                                            createIndex = false;
                                        }
                                    }
                                }

                                if (createIndex)
                                {
                                    currentTable = kvp.Key + " (create index)";
                                    cmdTargetDB.CommandText = kvp.Value;
                                    cmdTargetDB.ExecuteNonQuery();
                                }
                            }
                        }

                        try
                        {
                            cmdTargetDB.CommandText = ("ATTACH DATABASE '" + sourceDBPath) + "' AS SourceDB;";
                            cmdTargetDB.ExecuteNonQuery();

                            // Populate each table
                            foreach (var kvp in dctTableInfo)
                            {
                                currentTable = string.Copy(kvp.Key);

                                if (!tablesToSkip.Contains(currentTable))
                                {
                                    var sSql = "INSERT INTO main." + currentTable + " SELECT * FROM SourceDB." + currentTable + ";";

                                    cmdTargetDB.CommandText = sSql;
                                    cmdTargetDB.ExecuteNonQuery();
                                }
                            }

                            currentTable = "(DETACH DATABASE)";

                            // Detach the source DB
                            cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';";
                            cmdTargetDB.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Error copying data into cloned database, table " + currentTable, ex);
                        }

                        cmdTargetDB.Dispose();

                        cnTargetDB.Close();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Error initializing cloned database", ex);
                    }

                    cnSourceDB.Close();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error cloning database", ex);
            }

            return true;
        }

        public bool CopySqliteTable(string sourceDBPath, string tableName, string targetDBPath)
        {


            try
            {
                using (var cnSourceDB = new SQLiteConnection("Data Source = " + sourceDBPath))
                {
                    cnSourceDB.Open();
                    var cmdSourceDB = cnSourceDB.CreateCommand();

                    // Lookup up the table creation Sql
                    var sql = "SELECT sql FROM main.sqlite_master WHERE name = '" + tableName + "'";
                    cmdSourceDB.CommandText = sql;

                    var result = cmdSourceDB.ExecuteScalar();
                    if (result == null || ReferenceEquals(result, DBNull.Value))
                    {
                        throw new Exception("Source file " + Path.GetFileName(sourceDBPath) + " does not have table " + tableName);
                    }

                    var tableCreateSql = result.ToString();

                    // Look for any indices on this table
                    Dictionary<string, string> dctIndexToTableMap;
                    var dctIndexInfo = GetDBObjects(cnSourceDB, "index", out dctIndexToTableMap, tableName);

                    // Connect to the target database
                    using (var cnTarget = new SQLiteConnection("Data Source = " + targetDBPath))
                    {
                        cnTarget.Open();
                        var cmdTargetDB = cnTarget.CreateCommand();

                        // Attach the source database to the target
                        cmdTargetDB.CommandText = "ATTACH DATABASE '" + sourceDBPath + "' AS SourceDB;";
                        cmdTargetDB.ExecuteNonQuery();

                        using (var transaction = cnTarget.BeginTransaction())
                        {

                            // Create the target table
                            cmdTargetDB.CommandText = tableCreateSql;
                            cmdTargetDB.ExecuteNonQuery();

                            // Copy the data
                            sql = "INSERT INTO main." + tableName + " SELECT * FROM SourceDB." + tableName + ";";
                            cmdTargetDB.CommandText = sql;
                            cmdTargetDB.ExecuteNonQuery();

                            // Create any indices
                            foreach (var item in dctIndexInfo)
                            {
                                cmdTargetDB.CommandText = item.Value;
                                cmdTargetDB.ExecuteNonQuery();
                            }

                            transaction.Commit();
                        }

                        // Detach the source DB
                        cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';";
                        cmdTargetDB.ExecuteNonQuery();

                        cmdTargetDB.Dispose();

                        cnTarget.Close();
                    }

                    cnSourceDB.Close();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error copying table to new database: " + ex.Message, ex);
            }

            return true;
        }

        private Dictionary<string, string> GetDBObjects(SQLiteConnection cnDatabase, string objectType)
        {
            var tableName = string.Empty;
            Dictionary<string, string> dctIndexToTableMap;
            return GetDBObjects(cnDatabase, objectType, out dctIndexToTableMap, tableName);
        }

        private Dictionary<string, string> GetDBObjects(SQLiteConnection cnDatabase, string objectType, out Dictionary<string, string> dctIndexToTableMap)
        {
            var tableName = string.Empty;
            return GetDBObjects(cnDatabase, objectType, out dctIndexToTableMap, tableName);
        }

        /// <summary>
        /// Looks up the object names and object creation sql for objects of the specified type
        /// </summary>
        /// <param name="cnDatabase">Database connection object</param>
        /// <param name="objectType">Should be 'table' or 'index'</param>
        /// <param name="dctIndexToTableMap">Output parameter, only used if objectType is "index"; Keys are Index names and Values are the names of the tables that the indices apply to</param>
        /// <param name="tableNameFilter">Optional table name to filter on (useful when looking for indices that refer to a given table)</param>
        /// <returns>Dictionary where Keys are the object names and Values are the Sql to create that object</returns>
        private Dictionary<string, string> GetDBObjects(
            SQLiteConnection cnDatabase, string 
            objectType, 
            out Dictionary<string, string> dctIndexToTableMap, 
            string tableNameFilter)
        {

            var dctObjects = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            dctIndexToTableMap = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            var cmd = new SQLiteCommand(cnDatabase);

            var sql = "SELECT name, sql, tbl_name FROM main.sqlite_master WHERE type='" + objectType + "'";

            if (!string.IsNullOrWhiteSpace(tableNameFilter))
            {
                sql += " and tbl_name = '" + tableNameFilter + "'";
            }
            sql += " ORDER BY NAME;";

            cmd.CommandText = sql;

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    dctObjects.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["sql"]));

                    if (objectType == "index")
                    {
                        dctIndexToTableMap.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["tbl_name"]));
                    }
                }
            }

            return dctObjects;
        }
    }
}
