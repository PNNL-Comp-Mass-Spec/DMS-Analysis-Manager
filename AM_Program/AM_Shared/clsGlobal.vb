'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/20/2007
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading
Imports System.Text.RegularExpressions
Imports System.Runtime.InteropServices
Imports System.Data.SqlClient
Imports System.Security.Cryptography

Public Class clsGlobal

#Region "Constants"
	Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Const XML_FILENAME_PREFIX As String = "JobParameters_"
	Public Const XML_FILENAME_EXTENSION As String = "xml"

	Public Const STEPTOOL_PARAMFILESTORAGEPATH_PREFIX As String = "StepTool_ParamFileStoragePath_"

	Public Const SERVER_CACHE_HASHCHECK_FILE_SUFFIX As String = ".hashcheck"

#End Region

#Region "Enums"
	Public Enum eAnalysisResourceOptions
		OrgDbRequired = 0
	End Enum
#End Region

#Region "Module variables"
    Declare Auto Function GetDiskFreeSpaceEx Lib "kernel32.dll" (
      lpRootPathName As String,
      ByRef lpFreeBytesAvailable As Long,
      ByRef lpTotalNumberOfBytes As Long,
      ByRef lpTotalNumberOfFreeBytes As Long) As Integer
#End Region

#Region "Methods"
    ''' <summary>
    ''' Appends a string to a job comment string
    ''' </summary>
    ''' <param name="baseComment">Comment currently in job params</param>
    ''' <param name="addnlComment">Comment to be appened</param>
    ''' <returns>String containing both comments</returns>
    ''' <remarks></remarks>
    Public Shared Function AppendToComment(baseComment As String, addnlComment As String) As String

        'Appends a comment string to an existing comment string

        If String.IsNullOrWhiteSpace(baseComment) Then
            Return addnlComment
        Else
            ' Append a semicolon to InpComment, but only if it doesn't already end in a semicolon
            If Not baseComment.TrimEnd(" "c).EndsWith(";"c) Then
                baseComment &= "; "
            End If

            Return baseComment & addnlComment
        End If

    End Function

    ''' <summary>
    '''Examines intCount to determine which string to return
    ''' </summary>
    ''' <param name="intCount"></param>
    ''' <param name="strTextIfOneItem"></param>
    ''' <param name="strTextIfZeroOrMultiple"></param>
    ''' <returns>Returns strTextIfOneItem if intCount is 1; otherwise, returns strTextIfZeroOrMultiple</returns>
    ''' <remarks></remarks>
    Public Shared Function CheckPlural(intCount As Integer, strTextIfOneItem As String, strTextIfZeroOrMultiple As String) As String

        If intCount = 1 Then
            Return strTextIfOneItem
        Else
            Return strTextIfZeroOrMultiple
        End If

    End Function

    ''' <summary>
    ''' Collapse an array of items to a tab-delimited list
    ''' </summary>
    ''' <param name="strItems"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function CollapseLine(strItems() As String) As String
        If strItems Is Nothing OrElse strItems.Length = 0 Then
            Return String.Empty
        Else
            Return CollapseList(strItems.ToList())
        End If
    End Function

    ''' <summary>
    ''' Collapse a list of items to a tab-delimited list
    ''' </summary>
    ''' <param name="lstFields"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function CollapseList(lstFields As List(Of String)) As String

        Return FlattenList(lstFields, ControlChars.Tab)

    End Function

    ''' <summary>
    ''' Decrypts password received from ini file
    ''' </summary>
    ''' <param name="enPwd">Encoded password</param>
    ''' <returns>Clear text password</returns>
    Public Shared Function DecodePassword(enPwd As String) As String
        ' Decrypts password received from ini file
        ' Password was created by alternately subtracting or adding 1 to the ASCII value of each character

        ' Convert the password string to a character array
        Dim pwdChars As Char() = enPwd.ToCharArray()
        Dim pwdBytes = New List(Of Byte)
        Dim pwdCharsAdj = New List(Of Char)

        For i = 0 To pwdChars.Length - 1
            pwdBytes.Add(Convert.ToByte(pwdChars(i)))
        Next

        ' Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string

        For byteCntr = 0 To pwdBytes.Count - 1
            If (byteCntr Mod 2) = 0 Then
                pwdBytes(byteCntr) += CByte(1)
            Else
                pwdBytes(byteCntr) -= CByte(1)
            End If
            pwdCharsAdj.Add(Convert.ToChar(pwdBytes(byteCntr)))
        Next

        Return String.Join("", pwdCharsAdj)

    End Function

    ''' <summary>
    ''' Flatten a list of items into a single string, with items separated by chDelimiter
    ''' </summary>
    ''' <param name="lstItems"></param>
    ''' <param name="chDelimiter"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function FlattenList(lstItems As List(Of String), chDelimiter As Char) As String

        If lstItems Is Nothing OrElse lstItems.Count = 0 Then
            Return String.Empty
        Else
            Return String.Join(chDelimiter, lstItems)
        End If

    End Function

    ''' <summary>
    ''' Returns the directory in which the entry assembly (typically the Program .exe file) resides 
    ''' </summary>
    ''' <returns>Full directory path</returns>
    Public Shared Function GetAppFolderPath() As String

        Static strAppFolderPath As String = String.Empty

        If String.IsNullOrEmpty(strAppFolderPath) Then
            Dim objAssembly As Reflection.Assembly
            objAssembly = Reflection.Assembly.GetEntryAssembly()

            Dim fiAssemblyFile As FileInfo
            fiAssemblyFile = New FileInfo(objAssembly.Location)

            strAppFolderPath = fiAssemblyFile.DirectoryName
        End If

        Return strAppFolderPath

    End Function

    ''' <summary>
    ''' Returns the version string of the entry assembly (typically the Program .exe file)
    ''' </summary>
    ''' <returns>Assembly version, e.g. 1.0.4482.23831</returns>
    Public Shared Function GetAssemblyVersion() As String
        Dim objEntryAssembly As Reflection.Assembly
        objEntryAssembly = Reflection.Assembly.GetEntryAssembly()

        Return GetAssemblyVersion(objEntryAssembly)

    End Function

    ''' <summary>
    ''' Returns the version string of the specified assembly
    ''' </summary>
    ''' <returns>Assembly version, e.g. 1.0.4482.23831</returns>
    Public Shared Function GetAssemblyVersion(objAssembly As Reflection.Assembly) As String
        ' objAssembly.FullName typically returns something like this:
        ' AnalysisManagerProg, Version=2.3.4479.23831, Culture=neutral, PublicKeyToken=null
        ' 
        ' the goal is to extract out the text after Version= but before the next comma

        Dim reGetVersion = New Regex("version=([0-9.]+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Dim reMatch As Match
        Dim strVersion As String

        strVersion = objAssembly.FullName

        reMatch = reGetVersion.Match(strVersion)

        If reMatch.Success Then
            strVersion = reMatch.Groups(1).Value
        End If

        Return strVersion

    End Function

    ''' <summary>
    ''' Runs the specified Sql query
    ''' </summary>
    ''' <param name="sqlStr">Sql query</param>
    ''' <param name="connectionString">Connection string</param>
    ''' <param name="callingFunction">Name of the calling function</param>
    ''' <param name="retryCount">Number of times to retry (in case of a problem)</param>
    ''' <param name="dtResults">Datatable (Output Parameter)</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Uses a timeout of 30 seconds</remarks>
    Public Shared Function GetDataTableByQuery(
      sqlStr As String,
      connectionString As String,
      callingFunction As String,
      retryCount As Short,
      <Out()> ByRef dtResults As DataTable) As Boolean

        Const timeoutSeconds = 30

        Return GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, dtResults, timeoutSeconds)

    End Function

    ''' <summary>
    ''' Runs the specified Sql query
    ''' </summary>
    ''' <param name="sqlStr">Sql query</param>
    ''' <param name="connectionString">Connection string</param>
    ''' <param name="callingFunction">Name of the calling function</param>
    ''' <param name="retryCount">Number of times to retry (in case of a problem)</param>
    ''' <param name="dtResults">Datatable (Output Parameter)</param>
    ''' <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Public Shared Function GetDataTableByQuery(
      sqlStr As String,
      connectionString As String,
      callingFunction As String,
      retryCount As Short,
      <Out()> ByRef dtResults As DataTable,
      timeoutSeconds As Integer) As Boolean

        Dim cmd = New SqlCommand(sqlStr)
        cmd.CommandType = CommandType.Text

        Return GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, dtResults, timeoutSeconds)

    End Function

    ''' <summary>
    ''' Runs the stored procedure or database query defined by "cmd"
    ''' </summary>
    ''' <param name="cmd">SqlCommand object (query or stored procedure)</param>
    ''' <param name="connectionString">Connection string</param>
    ''' <param name="callingFunction">Name of the calling function</param>
    ''' <param name="retryCount">Number of times to retry (in case of a problem)</param>
    ''' <param name="dtResults">Datatable (Output Parameter)</param>
    ''' <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Public Shared Function GetDataTableByCmd(
      cmd As SqlCommand,
      connectionString As String,
      callingFunction As String,
      retryCount As Short,
      <Out()> ByRef dtResults As DataTable,
      timeoutSeconds As Integer) As Boolean

        Dim strMsg As String

        If cmd Is Nothing Then Throw New ArgumentException("cmd is undefined")
        If String.IsNullOrEmpty(connectionString) Then Throw New ArgumentException("ConnectionString argument cannot be empty")
        If String.IsNullOrEmpty(callingFunction) Then callingFunction = "UnknownCaller"
        If retryCount < 1 Then retryCount = 1
        If timeoutSeconds < 5 Then timeoutSeconds = 5

        While retryCount > 0
            Try
                Using Cn = New SqlConnection(connectionString)

                    cmd.Connection = Cn
                    cmd.CommandTimeout = timeoutSeconds

                    Using Da = New SqlDataAdapter(cmd)
                        Using Ds = New DataSet
                            Da.Fill(Ds)
                            dtResults = Ds.Tables(0)
                        End Using
                    End Using
                End Using
                Return True
            Catch ex As Exception
                retryCount -= 1S
                If cmd.CommandType = CommandType.StoredProcedure Then
                    strMsg = callingFunction & "; Exception running stored procedure " & cmd.CommandText
                ElseIf cmd.CommandType = CommandType.TableDirect Then
                    strMsg = callingFunction & "; Exception querying table " & cmd.CommandText
                Else
                    strMsg = callingFunction & "; Exception querying database"
                End If

                strMsg &= ": " + ex.Message + "; ConnectionString: " + connectionString
                strMsg &= ", RetryCount = " + retryCount.ToString

                If cmd.CommandType = CommandType.Text Then
                    strMsg &= ", Query = " + cmd.CommandText
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
                Thread.Sleep(5000)              'Delay for 5 second before trying again
            End Try
        End While

        dtResults = Nothing
        Return False

    End Function

    ''' <summary>
    ''' Run a query against a SQL Server database
    ''' </summary>
    ''' <param name="sqlQuery">Query to run</param>
    ''' <param name="connectionString">Connection string</param>
    ''' <param name="lstResults">Results (first row only if multiple rows)</param>
    ''' <param name="callingFunction">Name of the calling function (for logging purposes)</param>
    ''' <param name="retryCount">Number of times to retry (in case of a problem)</param>
    ''' <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>
    ''' Null values are converted to empty strings
    ''' Numbers are converted to their string equivalent
    ''' Use the GetDataTable functions in this class if you need to retain numeric values or null values
    ''' </remarks>
    Public Shared Function GetQueryResultsTopRow(
      sqlQuery As String,
      connectionString As String,
      <Out()> ByRef lstResults As List(Of String),
      callingFunction As String,
      Optional retryCount As Short = 3,
      Optional timeoutSeconds As Integer = 5) As Boolean

        Dim lstResultTable As List(Of List(Of String)) = Nothing

        Dim success = GetQueryResults(sqlQuery, connectionString, lstResultTable, callingFunction, retryCount, timeoutSeconds, maxRowsToReturn:=1)

        If success Then
            lstResults = lstResultTable.FirstOrDefault()
            If lstResults Is Nothing Then lstResults = New List(Of String)
            Return True
        Else
            lstResults = New List(Of String)
            Return False
        End If
    End Function

    ''' <summary>
    ''' Run a query against a SQL Server database, return the results as a list of strings
    ''' </summary>
    ''' <param name="sqlQuery">Query to run</param>
    ''' <param name="connectionString">Connection string</param>
    ''' <param name="lstResults">Results (list of list of strings)</param>
    ''' <param name="callingFunction">Name of the calling function (for logging purposes)</param>
    ''' <param name="retryCount">Number of times to retry (in case of a problem)</param>
    ''' <param name="timeoutSeconds">Query timeout (in seconds); minimum is 5 seconds; suggested value is 30 seconds</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>
    ''' Null values are converted to empty strings
    ''' Numbers are converted to their string equivalent
    ''' Use the GetDataTable functions in this class if you need to retain numeric values or null values
    ''' </remarks>
    Public Shared Function GetQueryResults(
      sqlQuery As String,
      connectionString As String,
      <Out()> ByRef lstResults As List(Of List(Of String)),
      callingFunction As String,
      Optional retryCount As Short = 3,
      Optional timeoutSeconds As Integer = 5,
      Optional maxRowsToReturn As Integer = 0) As Boolean

        If retryCount < 1 Then retryCount = 1
        If timeoutSeconds < 5 Then timeoutSeconds = 5

        lstResults = New List(Of List(Of String))

        While retryCount > 0
            Try
                Using dbConnection = New SqlConnection(connectionString)
                    Using cmd = New SqlCommand(sqlQuery, dbConnection)

                        cmd.CommandTimeout = timeoutSeconds

                        dbConnection.Open()

                        Dim reader = cmd.ExecuteReader()

                        While reader.Read()
                            Dim lstCurrentRow = New List(Of String)

                            For columnIndex = 0 To reader.FieldCount - 1
                                Dim value = reader.GetValue(columnIndex)

                                If DBNull.Value.Equals(value) Then
                                    lstCurrentRow.Add(String.Empty)
                                Else
                                    lstCurrentRow.Add(value.ToString())
                                End If

                            Next

                            lstResults.Add(lstCurrentRow)

                            If maxRowsToReturn > 0 AndAlso lstResults.Count >= maxRowsToReturn Then
                                Exit While
                            End If
                        End While

                    End Using
                End Using

                Return True

            Catch ex As Exception
                retryCount -= 1S
                Dim errorMessage = "Exception querying database: " + ex.Message + "; ConnectionString: " + connectionString
                errorMessage &= ", RetryCount = " + retryCount.ToString
                errorMessage &= ", Query " & sqlQuery

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
                Thread.Sleep(5000)              'Delay for 5 second before trying again
            End Try
        End While

        Return False

    End Function

    ''' <summary>
    ''' Parses the .StackTrace text of the given expression to return a compact description of the current stack
    ''' </summary>
    ''' <param name="objException"></param>
    ''' <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
    ''' <remarks></remarks>
    Public Shared Function GetExceptionStackTrace(objException As Exception) As String

        Return PRISM.Logging.Utilities.GetExceptionStackTrace(objException)

    End Function

    Public Shared Function GetKeyValueSetting(strText As String) As KeyValuePair(Of String, String)

        Dim strKey As String = String.Empty
        Dim strValue As String = String.Empty

        If Not String.IsNullOrWhiteSpace(strText) Then
            strText = strText.Trim()

            If Not strText.StartsWith("#") AndAlso strText.Contains("="c) Then

                Dim intCharIndex As Integer
                intCharIndex = strText.IndexOf("=", StringComparison.Ordinal)

                If intCharIndex > 0 Then
                    strKey = strText.Substring(0, intCharIndex).Trim()
                    If intCharIndex < strText.Length - 1 Then
                        strValue = strText.Substring(intCharIndex + 1).Trim()
                    Else
                        strValue = String.Empty
                    End If
                End If
            End If

        End If

        Return New KeyValuePair(Of String, String)(strKey, strValue)
    End Function

    ''' <summary>
    ''' Compare two strings (not case sensitive)
    ''' </summary>
    ''' <param name="strText1"></param>
    ''' <param name="strText2"></param>
    ''' <returns>True if they match; false if not</returns>
    ''' <remarks></remarks>
    Public Shared Function IsMatch(strText1 As String, strText2 As String) As Boolean
        If String.Compare(strText1, strText2, True) = 0 Then
            Return True
        Else
            Return False
        End If
    End Function

    ''' <summary>
    ''' Returns true if the file is _.swp or starts with a . and ends with .swp
    ''' </summary>
    ''' <param name="filePath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function IsVimSwapFile(filePath As String) As Boolean

        Dim fileName = Path.GetFileName(filePath)

        If fileName.ToLower() = "_.swp" OrElse fileName.StartsWith(".") AndAlso fileName.ToLower().EndsWith(".swp") Then
            Return True
        Else
            Return False
        End If
    End Function

    ''' <summary>
    ''' Parses the headers in strHeaderLine to look for the names specified in lstHeaderNames
    ''' </summary>
    ''' <param name="strHeaderLine"></param>
    ''' <param name="lstHeaderNames"></param>
    ''' <returns>Dictionary with the header names and 0-based column index</returns>
    ''' <remarks>Header names not found in strHeaderLine will have an index of -1</remarks>
    Public Shared Function ParseHeaderLine(strHeaderLine As String, lstHeaderNames As List(Of String), isCaseSensitive As Boolean) As Dictionary(Of String, Integer)
        Dim dctHeaderMapping = New Dictionary(Of String, Integer)

        Dim lstColumns = strHeaderLine.Split(ControlChars.Tab).ToList()

        For Each headerName In lstHeaderNames
            Dim colIndex As Integer = -1

            If isCaseSensitive Then
                colIndex = lstColumns.IndexOf(headerName)
            Else
                For i = 0 To lstColumns.Count - 1
                    If IsMatch(lstColumns(i), headerName) Then
                        colIndex = i
                        Exit For
                    End If
                Next
            End If

            dctHeaderMapping.Add(headerName, colIndex)
        Next

        Return dctHeaderMapping

    End Function

    ''' <summary>
    ''' Examines strPath to look for spaces
    ''' </summary>
    ''' <param name="strPath"></param>
    ''' <returns>strPath as-is if no spaces, otherwise strPath surrounded by double quotes </returns>
    ''' <remarks></remarks>
    Public Shared Function PossiblyQuotePath(strPath As String) As String
        If String.IsNullOrWhiteSpace(strPath) Then
            Return String.Empty
        Else

            If strPath.Contains(" ") Then
                If Not strPath.StartsWith("""") Then
                    strPath = """" & strPath
                End If

                If Not strPath.EndsWith("""") Then
                    strPath &= """"
                End If
            End If

            Return strPath

        End If
    End Function

    ''' <summary>
    ''' Converts a string value to a boolean equivalent
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <returns></returns>
    ''' <remarks>Returns false if an exception</remarks>
    Public Shared Function CBoolSafe(Value As String) As Boolean
        Dim blnValue As Boolean

        Try
            If String.IsNullOrEmpty(Value) Then
                blnValue = False
            Else
                blnValue = CBool(Value)
            End If
        Catch ex As Exception
            blnValue = False
        End Try

        Return blnValue

    End Function

    ''' <summary>
    ''' Converts a string value to a boolean equivalent
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <param name="blnDefaultValue">Boolean value to return if Value is empty or an exception occurs</param>
    ''' <returns></returns>
    ''' <remarks>Returns false if an exception</remarks>
    Public Shared Function CBoolSafe(Value As String, blnDefaultValue As Boolean) As Boolean
        Dim blnValue As Boolean

        Try
            If String.IsNullOrEmpty(Value) Then
                blnValue = blnDefaultValue
            Else
                blnValue = CBool(Value)
            End If
        Catch ex As Exception
            blnValue = blnDefaultValue
        End Try

        Return blnValue

    End Function

    ''' <summary>
    ''' Converts Value to an integer
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <param name="intDefaultValue">Integer to return if Value is not numeric</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function CIntSafe(Value As String, intDefaultValue As Integer) As Integer
        Dim intValue As Integer

        Try
            If String.IsNullOrEmpty(Value) Then
                intValue = intDefaultValue
            Else
                intValue = CInt(Value)
            End If
        Catch ex As Exception
            intValue = intDefaultValue
        End Try

        Return intValue

    End Function

    ''' <summary>
    ''' Converts Value to an single (aka float)
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <param name="sngDefaultValue">Single to return if Value is not numeric</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function CSngSafe(Value As String, sngDefaultValue As Single) As Single
        Dim sngValue As Single

        Try
            If String.IsNullOrEmpty(Value) Then
                sngValue = sngDefaultValue
            Else
                sngValue = CSng(Value)
            End If

        Catch ex As Exception
            sngValue = sngDefaultValue
        End Try

        Return sngValue

    End Function

    ''' <summary>
    ''' Copies file SourceFilePath to folder TargetFolder, renaming it to TargetFileName.
    ''' However, if file TargetFileName already exists, then that file will first be backed up
    ''' Furthermore, up to VersionCountToKeep old versions of the file will be kept
    ''' </summary>
    ''' <param name="SourceFilePath"></param>
    ''' <param name="TargetFolder"></param>
    ''' <param name="TargetFileName"></param>
    ''' <param name="VersionCountToKeep">Maximum backup copies of the file to keep; must be 9 or less</param>
    ''' <returns>True if Success, false if failure </returns>
    ''' <remarks></remarks>
    Public Shared Function CopyAndRenameFileWithBackup(
      SourceFilePath As String,
      TargetFolder As String,
      TargetFileName As String,
      VersionCountToKeep As Integer) As Boolean

        Dim ioSrcFile As FileInfo
        Dim ioFileToRename As FileInfo

        Dim strBaseName As String
        Dim strBaseNameCurrent As String

        Dim strNewFilePath As String
        Dim strExtension As String

        Dim intRevision As Integer

        Try
            ioSrcFile = New FileInfo(SourceFilePath)
            If Not ioSrcFile.Exists Then
                ' Source file not found
                Return False
            Else
                strBaseName = Path.GetFileNameWithoutExtension(TargetFileName)
                strExtension = Path.GetExtension(TargetFileName)
                If String.IsNullOrEmpty(strExtension) Then
                    strExtension = ".bak"
                End If
            End If

            If VersionCountToKeep > 9 Then VersionCountToKeep = 9
            If VersionCountToKeep < 0 Then VersionCountToKeep = 0

            ' Backup any existing copies of strTargetFilePath
            For intRevision = VersionCountToKeep - 1 To 0 Step -1
                Try
                    strBaseNameCurrent = String.Copy(strBaseName)
                    If intRevision > 0 Then
                        strBaseNameCurrent &= "_" & intRevision.ToString
                    End If
                    strBaseNameCurrent &= strExtension

                    ioFileToRename = New FileInfo(Path.Combine(TargetFolder, strBaseNameCurrent))
                    strNewFilePath = Path.Combine(TargetFolder, strBaseName & "_" & (intRevision + 1).ToString & strExtension)

                    ' Confirm that strNewFilePath doesn't exist; delete it if it does
                    If File.Exists(strNewFilePath) Then
                        File.Delete(strNewFilePath)
                    End If

                    ' Rename the current file to strNewFilePath
                    If ioFileToRename.Exists Then
                        ioFileToRename.MoveTo(strNewFilePath)
                    End If

                Catch ex As Exception
                    ' Ignore errors here; we'll continue on with the next file
                End Try

            Next intRevision

            strNewFilePath = Path.Combine(TargetFolder, TargetFileName)

            ' Now copy the file from SourceFilePath to strNewFilePath
            ioSrcFile.CopyTo(strNewFilePath, True)

        Catch ex As Exception
            ' Ignore errors here
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Converts an database field value to a string, checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function DbCStr(InpObj As Object) As String

        'If input object is DbNull, returns "", otherwise returns String representation of object
        If InpObj Is DBNull.Value Then
            Return String.Empty
        Else
            Return CStr(InpObj)
        End If

    End Function

    ''' <summary>
    ''' Converts an database field value to a single, checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks>An exception will be thrown if the value is not numeric</remarks>
    Public Shared Function DbCSng(InpObj As Object) As Single

        'If input object is DbNull, returns 0.0, otherwise returns Single representation of object
        If InpObj Is DBNull.Value Then
            Return 0.0
        Else
            Return CSng(InpObj)
        End If

    End Function

    ''' <summary>
    ''' Converts an database field value to a double, checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks>An exception will be thrown if the value is not numeric</remarks>
    Public Shared Function DbCDbl(InpObj As Object) As Double

        'If input object is DbNull, returns 0.0, otherwise returns Double representation of object
        If InpObj Is DBNull.Value Then
            Return 0.0
        Else
            Return CDbl(InpObj)
        End If

    End Function

    ''' <summary>
    ''' Converts an database field value to an integer (int32), checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks>An exception will be thrown if the value is not numeric</remarks>
    Public Shared Function DbCInt(InpObj As Object) As Integer

        'If input object is DbNull, returns 0, otherwise returns Integer representation of object
        If InpObj Is DBNull.Value Then
            Return 0
        Else
            Return CInt(InpObj)
        End If

    End Function

    ''' <summary>
    ''' Converts an database field value to a long integer (int64), checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks>An exception will be thrown if the value is not numeric</remarks>
    Public Shared Function DbCLng(InpObj As Object) As Long

        'If input object is DbNull, returns 0, otherwise returns Integer representation of object
        If InpObj Is DBNull.Value Then
            Return 0
        Else
            Return CLng(InpObj)
        End If

    End Function

    ''' <summary>
    ''' Converts an database field value to a decimal, checking for null values
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns></returns>
    ''' <remarks>An exception will be thrown if the value is not numeric</remarks>
    Public Shared Function DbCDec(InpObj As Object) As Decimal

        'If input object is DbNull, returns 0, otherwise returns Decimal representation of object
        If InpObj Is DBNull.Value Then
            Return 0
        Else
            Return CDec(InpObj)
        End If

    End Function

    Private Shared Function ByteArrayToString(arrInput() As Byte) As String
        ' Converts a byte array into a hex string

        Dim strOutput As New Text.StringBuilder(arrInput.Length)

        For i = 0 To arrInput.Length - 1
            strOutput.Append(arrInput(i).ToString("X2"))
        Next

        Return strOutput.ToString().ToLower()

    End Function

    ''' <summary>
    ''' Computes the MD5 hash for a file
    ''' </summary>
    ''' <param name="strPath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeFileHashMD5(strPath As String) As String

        Dim hashValue As String

        ' open file (as read-only)
        Using objReader As Stream = New FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
            ' Hash contents of this stream
            hashValue = ComputeMD5Hash(objReader)
        End Using

        Return hashValue

    End Function

    ''' <summary>
    ''' Computes the MD5 hash for a string
    ''' </summary>
    ''' <param name="text"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeStringHashMD5(text As String) As String

        Dim hashValue = ComputeMD5Hash(New MemoryStream(System.Text.Encoding.UTF8.GetBytes(text)))

        Return hashValue

    End Function

    ''' <summary>
    ''' Computes the SHA-1 hash for a file
    ''' </summary>
    ''' <param name="strPath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeFileHashSha1(strPath As String) As String

        Dim hashValue As String

        ' open file (as read-only)
        Using objReader As Stream = New FileStream(strPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
            ' Hash contents of this stream
            hashValue = ComputeSha1Hash(objReader)
        End Using

        Return hashValue

    End Function

    ''' <summary>
    ''' Computes the SHA-1 hash for a string
    ''' </summary>
    ''' <param name="text"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeStringHashSha1(text As String) As String

        Dim hashValue = ComputeSha1Hash(New MemoryStream(System.Text.Encoding.UTF8.GetBytes(text)))

        Return hashValue

    End Function

    ''' <summary>
    ''' Computes the MD5 hash of a given stream
    ''' </summary>
    ''' <param name="data"></param>
    ''' <returns>MD5 hash, as a string</returns>
    ''' <remarks></remarks>
    Private Shared Function ComputeMD5Hash(data As Stream) As String

        Dim objMD5 As New Security.Cryptography.MD5CryptoServiceProvider
        Return ComputeHash(objMD5, data)

    End Function

    ''' <summary>
    ''' Computes the SHA-1 hash of a given stream
    ''' </summary>
    ''' <param name="data"></param>
    ''' <returns>SHA1 hash, as a string</returns>
    ''' <remarks></remarks>
    Private Shared Function ComputeSha1Hash(data As Stream) As String

        Dim objSha1 As New Security.Cryptography.SHA1CryptoServiceProvider
        Return ComputeHash(objSha1, data)

    End Function

    ''' <summary>
    ''' Use the given hash algorithm to compute a hash of the data stream
    ''' </summary>
    ''' <param name="hasher"></param>
    ''' <param name="data"></param>
    ''' <returns>Hash string</returns>
    ''' <remarks></remarks>
    Private Shared Function ComputeHash(hasher As HashAlgorithm, data As Stream) As String
        ' hash contents of this stream
        Dim arrHash = hasher.ComputeHash(data)

        ' Return the hash, formatted as a string
        Return ByteArrayToString(arrHash)

    End Function

    ''' <summary>
    ''' Creates a .hashcheck file for the specified file
    ''' The file will be created in the same folder as the data file, and will contain size, modification_date_utc, and hash
    ''' </summary>
    ''' <param name="strDataFilePath"></param>
    ''' <param name="blnComputeMD5Hash">If True, then computes the MD5 hash</param>
    ''' <returns>The full path to the .hashcheck file; empty string if a problem</returns>
    ''' <remarks></remarks>
    Public Shared Function CreateHashcheckFile(strDataFilePath As String, blnComputeMD5Hash As Boolean) As String

        Dim strMD5Hash As String

        If Not File.Exists(strDataFilePath) Then Return String.Empty

        If blnComputeMD5Hash Then
            strMD5Hash = ComputeFileHashMD5(strDataFilePath)
        Else
            strMD5Hash = String.Empty
        End If

        Return CreateHashcheckFile(strDataFilePath, strMD5Hash)

    End Function

    ''' <summary>
    ''' Creates a .hashcheck file for the specified file
    ''' The file will be created in the same folder as the data file, and will contain size, modification_date_utc, and hash
    ''' </summary>
    ''' <param name="strDataFilePath"></param>
    ''' <param name="strMD5Hash"></param>
    ''' <returns>The full path to the .hashcheck file; empty string if a problem</returns>
    ''' <remarks></remarks>
    Public Shared Function CreateHashcheckFile(strDataFilePath As String, strMD5Hash As String) As String

        Dim fiDataFile As FileInfo
        Dim strHashFilePath As String

        fiDataFile = New FileInfo(strDataFilePath)

        If Not fiDataFile.Exists Then Return String.Empty

        strHashFilePath = fiDataFile.FullName & SERVER_CACHE_HASHCHECK_FILE_SUFFIX
        If String.IsNullOrWhiteSpace(strMD5Hash) Then strMD5Hash = String.Empty

        Using swOutFile = New StreamWriter(New FileStream(strHashFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swOutFile.WriteLine("# Hashcheck file created " & DateTime.Now().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT))
            swOutFile.WriteLine("size=" & fiDataFile.Length)
            swOutFile.WriteLine("modification_date_utc=" & fiDataFile.LastWriteTimeUtc.ToString("yyyy-MM-dd hh:mm:ss tt"))
            swOutFile.WriteLine("hash=" & strMD5Hash)
        End Using

        Return strHashFilePath

    End Function

    ''' <summary>
    ''' Compares two files, byte-by-byte
    ''' </summary>
    ''' <param name="strFilePath1">Path to the first file</param>
    ''' <param name="strFilePath2">Path to the second file</param>
    ''' <returns>True if the files match; false if they don't match; also returns false if either file is missing</returns>
    ''' <remarks></remarks>
    Public Shared Function FilesMatch(strFilePath1 As String, strFilePath2 As String) As Boolean

        Dim fiFile1 As FileInfo
        Dim fiFile2 As FileInfo

        Try
            fiFile1 = New FileInfo(strFilePath1)
            fiFile2 = New FileInfo(strFilePath2)

            If Not fiFile1.Exists OrElse Not fiFile2.Exists Then
                Return False
            ElseIf fiFile1.Length <> fiFile2.Length Then
                Return False
            End If

            Using srFile1 = New BinaryReader(New FileStream(fiFile1.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                Using srFile2 = New BinaryReader(New FileStream(fiFile2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                    While srFile1.BaseStream.Position < fiFile1.Length
                        If srFile1.ReadByte <> srFile2.ReadByte Then
                            Return False
                        End If
                    End While
                End Using
            End Using

            Return True

        Catch ex As Exception
            ' Ignore errors here
            Console.WriteLine("Error in clsGlobal.FilesMatch: " & ex.Message)
        End Try

        Return False

    End Function

    ''' <summary>
    ''' Determines free disk space for the disk where the given directory resides.  Supports both fixed drive letters and UNC paths (e.g. \\Server\Share\)
    ''' </summary>
    ''' <param name="strDirectoryPath"></param>
    ''' <param name="lngFreeBytesAvailableToUser"></param>
    ''' <param name="lngTotalDriveCapacityBytes"></param>
    ''' <param name="lngTotalNumberOfFreeBytes"></param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks></remarks>
    Private Shared Function GetDiskFreeSpace(
        strDirectoryPath As String,
        <Out()> ByRef lngFreeBytesAvailableToUser As Int64,
        <Out()> ByRef lngTotalDriveCapacityBytes As Int64,
        <Out()> ByRef lngTotalNumberOfFreeBytes As Int64) As Boolean

        Dim intResult As Integer

        intResult = GetDiskFreeSpaceEx(strDirectoryPath, lngFreeBytesAvailableToUser, lngTotalDriveCapacityBytes, lngTotalNumberOfFreeBytes)

        If intResult = 0 Then
            Return False
        Else
            Return True
        End If

    End Function

    ''' <summary>
    ''' Replaces text in a string, ignoring case
    ''' </summary>
    ''' <param name="strTextToSearch"></param>
    ''' <param name="strTextToFind"></param>
    ''' <param name="strReplacementText"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ReplaceIgnoreCase(strTextToSearch As String, strTextToFind As String, strReplacementText As String) As String

        Dim intCharIndex As Integer
        intCharIndex = strTextToSearch.ToLower().IndexOf(strTextToFind.ToLower(), StringComparison.Ordinal)

        If intCharIndex < 0 Then
            Return strTextToSearch
        Else
            Dim strNewText As String
            If intCharIndex = 0 Then
                strNewText = String.Empty
            Else
                strNewText = strTextToSearch.Substring(0, intCharIndex)
            End If

            strNewText &= strReplacementText

            If intCharIndex + strTextToFind.Length < strTextToSearch.Length Then
                strNewText &= strTextToSearch.Substring(intCharIndex + strTextToFind.Length)
            End If

            Return strNewText
        End If

    End Function

    ''' <summary>
    ''' Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number.  If 
    ''' </summary>
    ''' <param name="filePath1">First file</param>
    ''' <param name="filePath2">Second file</param>
    ''' <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function TextFilesMatch(filePath1 As String, filePath2 As String, ignoreWhitespace As Boolean) As Boolean

        Const comparisonStartLine = 0
        Const comparisonEndLine = 0

        Return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, Nothing)

    End Function


    ''' <summary>
    ''' Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number.  If 
    ''' </summary>
    ''' <param name="filePath1">First file</param>
    ''' <param name="filePath2">Second file</param>
    ''' <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
    ''' <param name="comparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
    ''' <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function TextFilesMatch(
     filePath1 As String, filePath2 As String,
     comparisonStartLine As Integer, comparisonEndLine As Integer,
     ignoreWhitespace As Boolean) As Boolean

        Return TextFilesMatch(filePath1, filePath2, comparisonStartLine, comparisonEndLine, ignoreWhitespace, Nothing)

    End Function

    ''' <summary>
    ''' Compares two files line-by-line.  If comparisonStartLine is > 0, then ignores differences up until the given line number. 
    ''' </summary>
    ''' <param name="filePath1">First file</param>
    ''' <param name="filePath2">Second file</param>
    ''' <param name="comparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
    ''' <param name="comparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
    ''' <param name="ignoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
    ''' <param name="lstLineIgnoreRegExSpecs">List of RegEx match specs that indicate lines to ignore</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function TextFilesMatch(
      filePath1 As String, filePath2 As String,
      comparisonStartLine As Integer, comparisonEndLine As Integer,
      ignoreWhitespace As Boolean,
      lstLineIgnoreRegExSpecs As List(Of Regex)) As Boolean

        Dim strLineIn1 As String
        Dim strLineIn2 As String

        Dim chWhiteSpaceChars() As Char
        Dim intLineNumber = 0

        ReDim chWhiteSpaceChars(1)
        chWhiteSpaceChars(0) = ControlChars.Tab
        chWhiteSpaceChars(1) = " "c

        Try
            Using srFile1 = New StreamReader(New FileStream(filePath1, FileMode.Open, FileAccess.Read, FileShare.Read))
                Using srFile2 = New StreamReader(New FileStream(filePath2, FileMode.Open, FileAccess.Read, FileShare.Read))

                    Do While Not srFile1.EndOfStream
                        strLineIn1 = srFile1.ReadLine
                        intLineNumber += 1

                        If comparisonEndLine > 0 AndAlso intLineNumber > comparisonEndLine Then
                            ' No need to compare further; files match up to this point
                            Exit Do
                        End If

                        If Not srFile2.EndOfStream Then
                            strLineIn2 = srFile2.ReadLine

                            If intLineNumber >= comparisonStartLine Then
                                If ignoreWhitespace Then
                                    strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
                                    strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)
                                End If

                                If strLineIn1 <> strLineIn2 Then
                                    ' Lines don't match; are we ignoring both of them?
                                    If TextFilesMatchIgnoreLine(strLineIn1, lstLineIgnoreRegExSpecs) AndAlso
                                       TextFilesMatchIgnoreLine(strLineIn2, lstLineIgnoreRegExSpecs) Then
                                        ' Ignoring both lines
                                    Else
                                        ' Files do not match
                                        Return False
                                    End If
                                End If
                            End If
                            Continue Do
                        End If

                        ' File1 has more lines than file2

                        If Not ignoreWhitespace Then
                            ' Files do not match
                            Return False
                        End If

                        ' Ignoring whitespace
                        ' If file1 only has blank lines from here on out, then the files match; otherwise, they don't
                        ' See if the remaining lines are blank
                        Do
                            If strLineIn1.Length <> 0 Then
                                If Not TextFilesMatchIgnoreLine(strLineIn1, lstLineIgnoreRegExSpecs) Then
                                    ' Files do not match
                                    Return False
                                End If
                            End If

                            If srFile1.EndOfStream Then
                                Exit Do
                            End If

                            strLineIn1 = srFile1.ReadLine
                            strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
                        Loop

                        Exit Do

                    Loop

                    If Not srFile2.EndOfStream Then
                        ' File2 has more lines than file1
                        If Not ignoreWhitespace Then
                            ' Files do not match
                            Return False
                        End If

                        ' Ignoring whitespace
                        ' If file2 only has blank lines from here on out, then the files match; otherwise, they don't
                        ' See if the remaining lines are blank
                        Do
                            strLineIn2 = srFile2.ReadLine
                            strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)

                            If strLineIn2.Length <> 0 Then
                                If Not TextFilesMatchIgnoreLine(strLineIn2, lstLineIgnoreRegExSpecs) Then
                                    ' Files do not match
                                    Return False
                                End If
                            End If
                        Loop While Not srFile2.EndOfStream

                    End If

                End Using
            End Using

            Return True

        Catch ex As Exception
            ' Error occurred
            Return False
        End Try

    End Function

    Protected Shared Function TextFilesMatchIgnoreLine(strText As String, lstLineIgnoreRegExSpecs As List(Of Regex)) As Boolean

        If Not lstLineIgnoreRegExSpecs Is Nothing Then
            For Each matchSpec In lstLineIgnoreRegExSpecs
                If matchSpec.Match(strText).Success Then
                    ' Line matches; ignore it
                    Return True
                End If
            Next
        End If

        Return False

    End Function

    ''' <summary>
    ''' Change the host name in the given share path to use a different host
    ''' </summary>
    ''' <param name="sharePath"></param>
    ''' <param name="newHostName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function UpdateHostName(sharePath As String, newHostName As String) As String

        If Not newHostName.StartsWith("\\") Then
            Throw New NotSupportedException("\\ not found at the start of newHostName (" & newHostName & "); The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\")
        End If

        If Not newHostName.EndsWith("\") Then
            newHostName &= "\"
        End If

        If Not sharePath.StartsWith("\\") Then
            Throw New NotSupportedException("\\ not found at the start of sharePath (" & sharePath & "); The UpdateHostName function only works with UNC paths, e.g. \\ServerName\Share\")
        End If

        Dim slashLoc = sharePath.IndexOf("\", 3, StringComparison.Ordinal)

        If slashLoc < 0 Then
            Throw New Exception("Backslash not found after the 3rd character in SharePath, " & sharePath)
        End If

        Dim sharePathNew = newHostName & sharePath.Substring(slashLoc + 1)

        Return sharePathNew

    End Function

    ''' <summary>
    ''' Returns True if the computer name is Pub-1000 or higher
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function UsingVirtualMachineOnPIC() As Boolean
        Dim rePub1000 = New Regex("Pub-1\d{3,}", RegexOptions.IgnoreCase)

        If rePub1000.IsMatch(Environment.MachineName) Then
            ' The Memory performance counters are not available on Windows instances running under VMWare on PIC
            Return True
        Else
            Return False
        End If

    End Function

    ''' <summary>
    ''' Looks for a .hashcheck file for the specified data file
    ''' If found, opens the file and reads the stored values: size, modification_date_utc, and hash
    ''' Next compares the stored values to the actual values
    ''' Checks file size and file date, but does not compute the hash
    ''' </summary>
    ''' <param name="strDataFilePath">Data file to check.</param>
    ''' <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
    ''' <param name="strErrorMessage"></param>
    ''' <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
    ''' <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
    Public Shared Function ValidateFileVsHashcheck(strDataFilePath As String, strHashFilePath As String, <Out()> ByRef strErrorMessage As String) As Boolean
        Return ValidateFileVsHashcheck(strDataFilePath, strHashFilePath, strErrorMessage, blnCheckDate:=True, blnComputeHash:=False, blnCheckSize:=True)
    End Function

    ''' <summary>
    ''' Looks for a .hashcheck file for the specified data file
    ''' If found, opens the file and reads the stored values: size, modification_date_utc, and hash
    ''' Next compares the stored values to the actual values
    ''' Checks file size, plus optionally date and hash
    ''' </summary>
    ''' <param name="strDataFilePath">Data file to check.</param>
    ''' <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
    ''' <param name="strErrorMessage"></param>
    ''' <param name="blnCheckDate">If True, then compares UTC modification time; times must agree within 2 seconds</param>
    ''' <param name="blnComputeHash"></param>
    ''' <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
    ''' <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
    Public Shared Function ValidateFileVsHashcheck(strDataFilePath As String, strHashFilePath As String, <Out()> ByRef strErrorMessage As String, blnCheckDate As Boolean, blnComputeHash As Boolean) As Boolean
        Return ValidateFileVsHashcheck(strDataFilePath, strHashFilePath, strErrorMessage, blnCheckDate, blnComputeHash, blnCheckSize:=True)
    End Function

    ''' <summary>
    ''' Looks for a .hashcheck file for the specified data file
    ''' If found, opens the file and reads the stored values: size, modification_date_utc, and hash
    ''' Next compares the stored values to the actual values
    ''' </summary>
    ''' <param name="strDataFilePath">Data file to check.</param>
    ''' <param name="strHashFilePath">Hashcheck file for the given data file (auto-defined if blank)</param>
    ''' <param name="strErrorMessage"></param>
    ''' <param name="blnCheckDate">If True, then compares UTC modification time; times must agree within 2 seconds</param>
    ''' <param name="blnComputeHash"></param>
    ''' <param name="blnCheckSize"></param>
    ''' <returns>True if the hashcheck file exists and the actual file matches the expected values; false if a mismatch or a problem</returns>
    ''' <remarks>The .hashcheck file has the same name as the data file, but with ".hashcheck" appended</remarks>
    Public Shared Function ValidateFileVsHashcheck(strDataFilePath As String, strHashFilePath As String, <Out()> ByRef strErrorMessage As String, blnCheckDate As Boolean, blnComputeHash As Boolean, blnCheckSize As Boolean) As Boolean

        Dim blnValidFile = False
        strErrorMessage = String.Empty

        Dim lngExpectedFileSizeBytes As Int64 = 0
        Dim strExpectedHash As String = String.Empty
        Dim dtExpectedFileDate As DateTime = DateTime.MinValue

        Try

            Dim fiDataFile = New FileInfo(strDataFilePath)

            If String.IsNullOrEmpty(strHashFilePath) Then strHashFilePath = fiDataFile.FullName & SERVER_CACHE_HASHCHECK_FILE_SUFFIX
            Dim fiHashCheck = New FileInfo(strHashFilePath)

            If Not fiDataFile.Exists Then
                strErrorMessage = "Data file not found at " & fiDataFile.FullName
                Return False
            End If

            If Not fiHashCheck.Exists Then
                strErrorMessage = "Data file at " & fiDataFile.FullName & " does not have a corresponding .hashcheck file named " & fiHashCheck.Name
                Return False
            End If

            ' Read the details in the HashCheck file
            Dim strLineIn As String
            Dim strSplitLine As String()

            Using srInfile = New StreamReader(New FileStream(fiHashCheck.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                While Not srInfile.EndOfStream
                    strLineIn = srInfile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) AndAlso Not strLineIn.StartsWith("#"c) AndAlso strLineIn.Contains("="c) Then
                        strSplitLine = strLineIn.Split("="c)

                        If strSplitLine.Count >= 2 Then

                            ' Set this to true for now
                            blnValidFile = True

                            Select Case strSplitLine(0).ToLower()
                                Case "size"
                                    Int64.TryParse(strSplitLine(1), lngExpectedFileSizeBytes)
                                Case "modification_date_utc"
                                    DateTime.TryParse(strSplitLine(1), dtExpectedFileDate)
                                Case "hash"
                                    strExpectedHash = String.Copy(strSplitLine(1))

                            End Select
                        End If
                    End If

                End While
            End Using

            If blnCheckSize AndAlso fiDataFile.Length <> lngExpectedFileSizeBytes Then
                strErrorMessage = "File size mismatch: expecting " & lngExpectedFileSizeBytes.ToString("#,##0") & " but computed " & fiDataFile.Length.ToString("#,##0")
                Return False
            End If

            ' Only compare dates if we are not comparing hash values
            If Not blnComputeHash AndAlso blnCheckDate Then
                If Math.Abs(fiDataFile.LastWriteTimeUtc.Subtract(dtExpectedFileDate).TotalSeconds) > 2 Then
                    strErrorMessage = "File modification date mismatch: expecting " & dtExpectedFileDate.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) & " UTC but actually " & fiDataFile.LastWriteTimeUtc.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT) & " UTC"
                    Return False
                End If
            End If

            If blnComputeHash Then
                ' Compute the hash of the file
                Dim strActualHash As String
                strActualHash = ComputeFileHashMD5(strDataFilePath)

                If strActualHash <> strExpectedHash Then
                    strErrorMessage = "Hash mismatch: expecting " & strExpectedHash & " but computed " & strActualHash
                    Return False
                End If
            End If


        Catch ex As Exception
            Console.WriteLine("Error in ValidateFileVsHashcheck: " & ex.Message)
        End Try

        Return blnValidFile

    End Function

    Public Shared Function ValidateFreeDiskSpace(directoryDescription As String, directoryPath As String, minFreeSpaceMB As Integer, eLogLocationIfNotFound As clsLogTools.LoggerTypes, <Out()> ByRef errorMessage As String) As Boolean

        Dim diDirectory As DirectoryInfo
        Dim diDrive As DriveInfo
        Dim freeSpaceMB As Double

        errorMessage = String.Empty

        diDirectory = New DirectoryInfo(directoryPath)
        If Not diDirectory.Exists Then
            ' Example error message: Organism DB directory not found: G:\DMS_Temp_Org
            errorMessage = directoryDescription & " not found: " & directoryPath
            clsLogTools.WriteLog(eLogLocationIfNotFound, clsLogTools.LogLevels.ERROR, errorMessage)
            Return False
        End If

        If diDirectory.Root.FullName.StartsWith("\\") OrElse Not diDirectory.Root.FullName.Contains(":") Then
            ' Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll
            Dim lngFreeBytesAvailableToUser As Long
            Dim lngTotalNumberOfBytes As Long
            Dim lngTotalNumberOfFreeBytes As Long

            If GetDiskFreeSpace(diDirectory.FullName, lngFreeBytesAvailableToUser, lngTotalNumberOfBytes, lngTotalNumberOfFreeBytes) Then
                freeSpaceMB = lngTotalNumberOfFreeBytes / 1024.0 / 1024.0
            Else
                freeSpaceMB = 0
            End If

        Else
            ' Directory is a local drive; can query with .NET
            diDrive = New DriveInfo(diDirectory.Root.FullName)
            freeSpaceMB = diDrive.TotalFreeSpace / 1024.0 / 1024.0
        End If


        If freeSpaceMB < minFreeSpaceMB Then
            ' Example error message: Organism DB directory drive has less than 6858 MB free: 5794 MB
            errorMessage = directoryDescription & " drive has less than " & minFreeSpaceMB.ToString & " MB free: " & CInt(freeSpaceMB).ToString() & " MB"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Return False
        Else
            Return True
        End If

    End Function
#End Region
End Class


