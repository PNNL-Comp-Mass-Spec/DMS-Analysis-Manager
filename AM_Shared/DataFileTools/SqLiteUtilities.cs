using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// SQLite utilities
    /// </summary>
    public class SqLiteUtilities
    {
        // Ignore Spelling: sqlite, sql, tbl

        /// <summary>
        /// Clones a database, optionally skipping tables in list tablesToSkip
        /// </summary>
        /// <remarks>If the target database already exists, missing tables (and data) will be appended to the file</remarks>
        /// <param name="sourceDBPath">Source database path</param>
        /// <param name="targetDBPath">Target database path</param>
        /// <returns>True if success, false if a problem</returns>
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
        /// <param name="appendToExistingDB">
        /// Behavior when the target DB exists; if true, missing tables will be appended to the database; if false, the target DB will be deleted
        /// </param>
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
        /// <param name="appendToExistingDB">
        /// Behavior when the target DB exists; if true, missing tables will be appended to the database; if false, the target DB will be deleted
        /// </param>
        /// <param name="tablesToSkip">A list of table names (e.g. Frame_Scans) that should not be copied.</param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneDB(string sourceDBPath, string targetDBPath, bool appendToExistingDB, List<string> tablesToSkip)
        {
            var currentTable = string.Empty;
            var appendingToExistingDB = false;

            try
            {
                using var sourceDB = new SQLiteConnection("Data Source = " + sourceDBPath);

                sourceDB.Open();

                // Get list of tables in source DB
                var tableInfo = GetDBObjects(sourceDB, "table");

                // Delete the "sqlite_sequence" database from tableInfo if present
                if (tableInfo.ContainsKey("sqlite_sequence"))
                {
                    tableInfo.Remove("sqlite_sequence");
                }

                // Get list of indices in source DB
                var indexInfo = GetDBObjects(sourceDB, "index", out var indexToTableMap);

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
                    var targetConnectionString = "Data Source = " + targetDBPath + "; Version=3; DateTimeFormat=Ticks;";
                    var targetDB = new SQLiteConnection(targetConnectionString);

                    targetDB.Open();
                    var cmdTargetDB = targetDB.CreateCommand();

                    Dictionary<string, string> existingTables;

                    if (appendingToExistingDB)
                    {
                        // Lookup the table names that already exist in the target
                        existingTables = GetDBObjects(targetDB, "table");
                    }
                    else
                    {
                        existingTables = new Dictionary<string, string>();
                    }

                    // Create each table
                    foreach (var kvp in tableInfo)
                    {
                        if (!string.IsNullOrEmpty(kvp.Value))
                        {
                            if (existingTables.ContainsKey(kvp.Key))
                            {
                                if (!tablesToSkip.Contains(kvp.Key))
                                {
                                    tablesToSkip.Add(kvp.Key);
                                }
                            }
                            else
                            {
                                currentTable = kvp.Key;
                                cmdTargetDB.CommandText = kvp.Value;
                                cmdTargetDB.ExecuteNonQuery();
                            }
                        }
                    }

                    foreach (var kvp in indexInfo)
                    {
                        if (string.IsNullOrEmpty(kvp.Value))
                            continue;

                        var createIndex = true;

                        if (appendingToExistingDB && indexToTableMap.TryGetValue(kvp.Key, out var indexTargetTable))
                        {
                            if (existingTables.ContainsKey(indexTargetTable))
                            {
                                createIndex = false;
                            }
                        }

                        if (!createIndex)
                            continue;

                        currentTable = kvp.Key + " (create index)";
                        cmdTargetDB.CommandText = kvp.Value;
                        cmdTargetDB.ExecuteNonQuery();
                    }

                    try
                    {
                        cmdTargetDB.CommandText = "ATTACH DATABASE '" + sourceDBPath + "' AS SourceDB;";
                        cmdTargetDB.ExecuteNonQuery();

                        // Populate each table
                        foreach (var kvp in tableInfo)
                        {
                            currentTable = kvp.Key;

                            if (tablesToSkip.Contains(currentTable))
                                continue;

                            var sql = "INSERT INTO main." + currentTable + " SELECT * FROM SourceDB." + currentTable + ";";

                            cmdTargetDB.CommandText = sql;
                            cmdTargetDB.ExecuteNonQuery();
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

                    targetDB.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception("Error initializing cloned database", ex);
                }

                sourceDB.Close();
            }
            catch (Exception ex)
            {
                throw new Exception("Error cloning database", ex);
            }

            return true;
        }

        /// <summary>
        /// Copy a SQLite table from one file to another
        /// </summary>
        /// <param name="sourceDBPath">Source SQLite file path</param>
        /// <param name="tableName">Table name</param>
        /// <param name="targetDBPath">Target SQLite file path</param>
        public bool CopySqliteTable(string sourceDBPath, string tableName, string targetDBPath)
        {
            try
            {
                using var sourceDB = new SQLiteConnection("Data Source = " + sourceDBPath);

                sourceDB.Open();
                var cmdSourceDB = sourceDB.CreateCommand();

                // Lookup up the table creation SQL
                var sql = "SELECT sql FROM main.sqlite_master WHERE name = '" + tableName + "'";
                cmdSourceDB.CommandText = sql;

                var result = cmdSourceDB.ExecuteScalar();

                if (result == null || ReferenceEquals(result, DBNull.Value))
                {
                    throw new Exception("Source file " + Path.GetFileName(sourceDBPath) + " does not have table " + tableName);
                }

                var tableCreateSql = result.ToString();

                // Look for any indices on this table
                var indexInfo = GetDBObjects(sourceDB, "index", out _, tableName);

                // Connect to the target database
                using (var targetDB = new SQLiteConnection("Data Source = " + targetDBPath))
                {
                    targetDB.Open();
                    var cmdTargetDB = targetDB.CreateCommand();

                    // Attach the source database to the target
                    cmdTargetDB.CommandText = "ATTACH DATABASE '" + sourceDBPath + "' AS SourceDB;";
                    cmdTargetDB.ExecuteNonQuery();

                    using (var transaction = targetDB.BeginTransaction())
                    {
                        // Create the target table
                        cmdTargetDB.CommandText = tableCreateSql;
                        cmdTargetDB.ExecuteNonQuery();

                        // Copy the data
                        sql = "INSERT INTO main." + tableName + " SELECT * FROM SourceDB." + tableName + ";";
                        cmdTargetDB.CommandText = sql;
                        cmdTargetDB.ExecuteNonQuery();

                        // Create any indices
                        foreach (var item in indexInfo)
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
                }

                sourceDB.Close();
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
            return GetDBObjects(cnDatabase, objectType, out _, tableName);
        }

        private Dictionary<string, string> GetDBObjects(SQLiteConnection cnDatabase, string objectType, out Dictionary<string, string> indexToTableMap)
        {
            var tableName = string.Empty;
            return GetDBObjects(cnDatabase, objectType, out indexToTableMap, tableName);
        }

        /// <summary>
        /// Looks up the object names and object creation sql for objects of the specified type
        /// </summary>
        /// <param name="cnDatabase">Database connection object</param>
        /// <param name="objectType">Should be 'table' or 'index'</param>
        /// <param name="indexToTableMap">Output parameter, only used if objectType is "index"; Keys are Index names and Values are the names of the tables that the indices apply to</param>
        /// <param name="tableNameFilter">Optional table name to filter on (useful when looking for indices that refer to a given table)</param>
        /// <returns>Dictionary where Keys are the object names and Values are the Sql to create that object</returns>
        private Dictionary<string, string> GetDBObjects(
            SQLiteConnection cnDatabase, string
            objectType,
            out Dictionary<string, string> indexToTableMap,
            string tableNameFilter)
        {
            var dbObjects = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            indexToTableMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var cmd = new SQLiteCommand(cnDatabase);

            var sql = "SELECT name, sql, tbl_name FROM main.sqlite_master WHERE type='" + objectType + "'";

            if (!string.IsNullOrWhiteSpace(tableNameFilter))
            {
                sql += " and tbl_name = '" + tableNameFilter + "'";
            }
            sql += " ORDER BY NAME;";

            cmd.CommandText = sql;

            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                dbObjects.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["sql"]));

                if (objectType == "index")
                {
                    indexToTableMap.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["tbl_name"]));
                }
            }

            return dbObjects;
        }
    }
}
