Option Strict On

Imports System.Collections.Generic
Imports System.IO
Imports System.Linq
Imports System.Text
Imports System.Data.SQLite

Public Class clsSqLiteUtilities

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
    End Sub

    ''' <summary>
    ''' Clones a database, optionally skipping tables in list tablesToSkip
    ''' </summary>
    ''' <param name="sourceDBPath">Source database path</param>
    ''' <param name="targetDBPath">Target database path</param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>If the target database already exists, then missing tables (and data) will be appended to the file</remarks>
    Public Function CloneDB(sourceDBPath As String, targetDBPath As String) As Boolean
        Const appendToExistingDB = True
        Dim tablesToSkip = New List(Of String)()
        Return CloneDB(sourceDBPath, targetDBPath, appendToExistingDB, tablesToSkip)
    End Function

    ''' <summary>
    ''' Clones a database, optionally skipping tables in list tablesToSkip
    ''' </summary>
    ''' <param name="sourceDBPath">Source database path</param>
    ''' <param name="targetDBPath">Target database path</param>
    ''' <param name="appendToExistingDB">Behavior when the target DB exists; if True, then missing tables will be appended to the database; if False, then the target DB will be deleted</param>
    ''' <returns>True if success, false if a problem</returns>
    Public Function CloneDB(sourceDBPath As String, targetDBPath As String, appendToExistingDB As Boolean) As Boolean
        Dim tablesToSkip = New List(Of String)()
        Return CloneDB(sourceDBPath, targetDBPath, appendToExistingDB, tablesToSkip)
    End Function

    ''' <summary>
    ''' Clones a database, optionally skipping tables in list tablesToSkip
    ''' </summary>
    ''' <param name="sourceDBPath">Source database path</param>
    ''' <param name="targetDBPath">Target database path</param>
    ''' <param name="appendToExistingDB">Behavior when the target DB exists; if True, then missing tables will be appended to the database; if False, then the target DB will be deleted</param>
    ''' <param name="tablesToSkip">A list of table names (e.g. Frame_Scans) that should not be copied.</param>
    ''' <returns>True if success, false if a problem</returns>
    Public Function CloneDB(sourceDBPath As String, targetDBPath As String, appendToExistingDB As Boolean, ByRef tablesToSkip As List(Of String)) As Boolean

        Dim currentTable As String = String.Empty
        Dim appendingToExistingDB = False

        Try

            Using cnSourceDB = New SQLiteConnection("Data Source = " & sourceDBPath)
                cnSourceDB.Open()

                ' Get list of tables in source DB					
                Dim dctTableInfo As Dictionary(Of String, String) = GetDBObjects(cnSourceDB, "table")

                ' Delete the "sqlite_sequence" database from dctTableInfo if present
                If dctTableInfo.ContainsKey("sqlite_sequence") Then
                    dctTableInfo.Remove("sqlite_sequence")
                End If

                ' Get list of indices in source DB
                Dim dctIndexToTableMap As Dictionary(Of String, String) = Nothing
                Dim dctIndexInfo As Dictionary(Of String, String) = GetDBObjects(cnSourceDB, "index", dctIndexToTableMap)

                If File.Exists(targetDBPath) Then
                    If appendToExistingDB Then
                        appendingToExistingDB = True
                    Else
                        File.Delete(targetDBPath)
                    End If
                End If

                Try
                    Dim sTargetConnectionString As String = ("Data Source = " & targetDBPath) + "; Version=3; DateTimeFormat=Ticks;"
                    Dim cnTargetDB As New SQLiteConnection(sTargetConnectionString)

                    cnTargetDB.Open()
                    Dim cmdTargetDB As SQLiteCommand = cnTargetDB.CreateCommand()


                    Dim dctExistingTables As Dictionary(Of String, String)
                    If appendingToExistingDB Then
                        ' Lookup the table names that already exist in the target
                        dctExistingTables = GetDBObjects(cnTargetDB, "table")
                    Else
                        dctExistingTables = New Dictionary(Of String, String)()
                    End If

                    ' Create each table
                    For Each kvp As KeyValuePair(Of String, String) In dctTableInfo
                        If Not String.IsNullOrEmpty(kvp.Value) Then
                            If dctExistingTables.ContainsKey(kvp.Key) Then
                                If Not tablesToSkip.Contains(kvp.Key) Then
                                    tablesToSkip.Add(kvp.Key)
                                End If
                            Else
                                currentTable = String.Copy(kvp.Key)
                                cmdTargetDB.CommandText = kvp.Value
                                cmdTargetDB.ExecuteNonQuery()
                            End If
                        End If
                    Next

                    For Each kvp As KeyValuePair(Of String, String) In dctIndexInfo
                        If Not String.IsNullOrEmpty(kvp.Value) Then
                            Dim createIndex = True

                            If appendingToExistingDB Then
                                Dim indexTargetTable As String = String.Empty
                                If dctIndexToTableMap.TryGetValue(kvp.Key, indexTargetTable) Then
                                    If dctExistingTables.ContainsKey(indexTargetTable) Then
                                        createIndex = False
                                    End If
                                End If
                            End If

                            If createIndex Then
                                currentTable = kvp.Key + " (create index)"
                                cmdTargetDB.CommandText = kvp.Value
                                cmdTargetDB.ExecuteNonQuery()
                            End If
                        End If
                    Next

                    Try
                        cmdTargetDB.CommandText = ("ATTACH DATABASE '" & sourceDBPath) + "' AS SourceDB;"
                        cmdTargetDB.ExecuteNonQuery()

                        ' Populate each table
                        For Each kvp As KeyValuePair(Of String, String) In dctTableInfo
                            currentTable = String.Copy(kvp.Key)

                            If Not tablesToSkip.Contains(currentTable) Then
                                Dim sSql As String = "INSERT INTO main." & currentTable & " SELECT * FROM SourceDB." & currentTable + ";"

                                cmdTargetDB.CommandText = sSql
                                cmdTargetDB.ExecuteNonQuery()
                            End If
                        Next

                        currentTable = "(DETACH DATABASE)"

                        ' Detach the source DB
                        cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';"
                        cmdTargetDB.ExecuteNonQuery()
                    Catch ex As Exception
                        Throw New Exception("Error copying data into cloned database, table " & currentTable, ex)
                    End Try

                    cmdTargetDB.Dispose()

                    cnTargetDB.Close()
                Catch ex As Exception
                    Throw New Exception("Error initializing cloned database", ex)
                End Try

                cnSourceDB.Close()
            End Using
        Catch ex As Exception
            Throw New Exception("Error cloning database", ex)
        End Try

        Return True
    End Function

    Public Function CopySqliteTable(sourceDBPath As String, tableName As String, targetDBPath As String) As Boolean

        Try

            Using cnSourceDB = New SQLiteConnection("Data Source = " & sourceDBPath)
                cnSourceDB.Open()
                Dim cmdSourceDB As SQLiteCommand = cnSourceDB.CreateCommand()

                ' Lookup up the table creation Sql
                Dim sql As String = "SELECT sql FROM main.sqlite_master WHERE name = '" & tableName & "'"
                cmdSourceDB.CommandText = sql

                Dim result As Object = cmdSourceDB.ExecuteScalar()
                If result Is Nothing OrElse result Is DBNull.Value Then
                    Throw New Exception("Source file " + Path.GetFileName(sourceDBPath) + " does not have table " & tableName)
                End If

                Dim tableCreateSql As String = result.ToString()

                ' Look for any indices on this table
                Dim dctIndexToTableMap As Dictionary(Of String, String) = Nothing
                Dim dctIndexInfo As Dictionary(Of String, String) = GetDBObjects(cnSourceDB, "index", dctIndexToTableMap, tableName)

                ' Connect to the target database
                Using cnTarget = New SQLiteConnection("Data Source = " & targetDBPath)
                    cnTarget.Open()
                    Dim cmdTargetDB As SQLiteCommand = cnTarget.CreateCommand()

                    ' Attach the source database to the target
                    cmdTargetDB.CommandText = "ATTACH DATABASE '" & sourceDBPath & "' AS SourceDB;"
                    cmdTargetDB.ExecuteNonQuery()

                    Using transaction As SQLiteTransaction = cnTarget.BeginTransaction()

                        ' Create the target table
                        cmdTargetDB.CommandText = tableCreateSql
                        cmdTargetDB.ExecuteNonQuery()

                        ' Copy the data
                        sql = "INSERT INTO main." & tableName & " SELECT * FROM SourceDB." & tableName + ";"
                        cmdTargetDB.CommandText = sql
                        cmdTargetDB.ExecuteNonQuery()

                        ' Create any indices
                        For Each item In dctIndexInfo
                            cmdTargetDB.CommandText = item.Value
                            cmdTargetDB.ExecuteNonQuery()
                        Next

                        transaction.Commit()
                    End Using

                    ' Detach the source DB
                    cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';"
                    cmdTargetDB.ExecuteNonQuery()

                    cmdTargetDB.Dispose()

                    cnTarget.Close()
                End Using

                cnSourceDB.Close()
            End Using
        Catch ex As Exception
            Throw New Exception("Error copying table to new database: " + ex.Message, ex)
        End Try

        Return True
    End Function

    Private Function GetDBObjects(cnDatabase As SQLiteConnection, objectType As String) As Dictionary(Of String, String)
        Dim tableName As String = String.Empty
        Dim dctIndexToTableMap As Dictionary(Of String, String) = Nothing
        Return GetDBObjects(cnDatabase, objectType, dctIndexToTableMap, tableName)
    End Function

    Private Function GetDBObjects(cnDatabase As SQLiteConnection, objectType As String, ByRef dctIndexToTableMap As Dictionary(Of String, String)) As Dictionary(Of String, String)
        Dim tableName As String = String.Empty
        Return GetDBObjects(cnDatabase, objectType, dctIndexToTableMap, tableName)
    End Function

    ''' <summary>
    ''' Looks up the object names and object creation sql for objects of the specified type
    ''' </summary>
    ''' <param name="cnDatabase">Database connection object</param>
    ''' <param name="objectType">Should be 'table' or 'index'</param>
    ''' <param name="dctIndexToTableMap">Output parameter, only used if objectType is "index"; Keys are Index names and Values are the names of the tables that the indices apply to</param>
    ''' <param name="tableNameFilter">Optional table name to filter on (useful when looking for indices that refer to a given table)</param>
    ''' <returns>Dictionary object where Keys are the object names and Values are the Sql to create that object</returns>
    Private Function GetDBObjects(cnDatabase As SQLiteConnection, objectType As String, ByRef dctIndexToTableMap As Dictionary(Of String, String), tableNameFilter As String) As Dictionary(Of String, String)

        Dim dctObjects = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
        dctIndexToTableMap = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

        Dim cmd As New SQLiteCommand(cnDatabase)

        Dim sql As String = "SELECT name, sql, tbl_name FROM main.sqlite_master WHERE type='" & objectType & "'"

        If Not String.IsNullOrWhiteSpace(tableNameFilter) Then
            sql &= " and tbl_name = '" & tableNameFilter & "'"
        End If
        sql &= " ORDER BY NAME;"

        cmd.CommandText = sql

        Using reader As SQLiteDataReader = cmd.ExecuteReader()
            While reader.Read()
                dctObjects.Add(Convert.ToString(reader("Name")), Convert.ToString(reader("sql")))

                If objectType = "index" Then
                    dctIndexToTableMap.Add(Convert.ToString(reader("Name")), Convert.ToString(reader("tbl_name")))
                End If
            End While
        End Using

        Return dctObjects
    End Function
End Class
