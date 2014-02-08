'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/20/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading
Imports System.Text.RegularExpressions
Imports System.Runtime.InteropServices

Public Class clsGlobal

#Region "Constants"
	Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Const XML_FILENAME_PREFIX As String = "JobParameters_"
	Public Const XML_FILENAME_EXTENSION As String = "xml"

	Public Const STEPTOOL_PARAMFILESTORAGEPATH_PREFIX As String = "StepTool_ParamFileStoragePath_"

	Public Const SERVER_CACHE_HASHCHECK_FILE_SUFFIX As String = ".hashcheck"

#End Region

#Region "Methods"
	''' <summary>
	''' Appends a string to a job comment string
	''' </summary>
	''' <param name="InpComment">Comment currently in job params</param>
	''' <param name="NewComment">Comment to be appened</param>
	''' <returns>String containing both comments</returns>
	''' <remarks></remarks>
	Public Shared Function AppendToComment(ByVal InpComment As String, ByVal NewComment As String) As String

		'Appends a comment string to an existing comment string

		If String.IsNullOrWhiteSpace(InpComment) Then
			Return NewComment
		Else
			' Append a semicolon to InpComment, but only if it doesn't already end in a semicolon
			If Not InpComment.TrimEnd(" "c).EndsWith(";"c) Then
				InpComment &= "; "
			End If

			Return InpComment & NewComment
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
	Public Shared Function CheckPlural(ByVal intCount As Integer, ByVal strTextIfOneItem As String, ByVal strTextIfZeroOrMultiple As String) As String

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
	Public Shared Function CollapseLine(ByRef strItems() As String) As String
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
	Public Shared Function DecodePassword(ByVal enPwd As String) As String
		' Decrypts password received from ini file
		' Password was created by alternately subtracting or adding 1 to the ASCII value of each character

		' Convert the password string to a character array
		Dim pwdChars As Char() = enPwd.ToCharArray()
		Dim pwdBytes = New List(Of Byte)
		Dim pwdCharsAdj = New List(Of Char)

		For i As Integer = 0 To pwdChars.Length - 1
			pwdBytes.Add(Convert.ToByte(pwdChars(i)))
		Next

		' Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string

		For byteCntr As Integer = 0 To pwdBytes.Count - 1
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
	Public Shared Function FlattenList(ByVal lstItems As List(Of String), ByVal chDelimiter As Char) As String

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
	Public Shared Function GetAssemblyVersion(ByRef objAssembly As Reflection.Assembly) As String
		' objAssembly.FullName typically returns something like this:
		' AnalysisManagerProg, Version=2.3.4479.23831, Culture=neutral, PublicKeyToken=null
		' 
		' the goal is to extract out the text after Version= but before the next comma

		Dim reGetVersion As Regex = New Regex("version=([0-9.]+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
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
	''' <param name="SqlStr"></param>
	''' <param name="RetryCount"></param>
	''' <param name="ConnectionString"></param>
	''' <param name="Dt">Datatable (Output Parameter)</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Public Shared Function GetDataTableByQuery(
	  ByVal SqlStr As String,
	  ByVal ConnectionString As String,
	  ByVal CallingFunction As String,
	  ByVal RetryCount As Short,
	  <Out()> ByRef Dt As DataTable) As Boolean

		Dim strMsg As String

		If String.IsNullOrEmpty(SqlStr) Then Throw New ArgumentException("SqlStr argument cannot be empty")
		If String.IsNullOrEmpty(ConnectionString) Then Throw New ArgumentException("ConnectionString argument cannot be empty")
		If String.IsNullOrEmpty(CallingFunction) Then CallingFunction = "UnknownCaller"
		If RetryCount < 1 Then RetryCount = 1

		While RetryCount > 0
			Try
				Using Cn As SqlClient.SqlConnection = New SqlClient.SqlConnection(ConnectionString)
					Using Da As SqlClient.SqlDataAdapter = New SqlClient.SqlDataAdapter(SqlStr.ToString(), Cn)
						Using Ds As DataSet = New DataSet
							Da.Fill(Ds)
							Dt = Ds.Tables(0)
						End Using
					End Using
				End Using
				Return True
			Catch ex As Exception
				RetryCount -= 1S
				strMsg = CallingFunction & "; Exception querying database: " + ex.Message + "; ConnectionString: " + ConnectionString
				strMsg &= ", RetryCount = " + RetryCount.ToString
				strMsg &= ", Query = " + SqlStr
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
				Thread.Sleep(5000)				'Delay for 5 second before trying again
			End Try
		End While

		Dt = Nothing
		Return False

	End Function

	''' <summary>
	''' Parses the .StackTrace text of the given expression to return a compact description of the current stack
	''' </summary>
	''' <param name="objException"></param>
	''' <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
	''' <remarks></remarks>
	Public Shared Function GetExceptionStackTrace(ByVal objException As Exception) As String
		Const REGEX_FUNCTION_NAME As String = "at ([^(]+)\("
		Const REGEX_FILE_NAME As String = "in .+\\(.+)"

		Dim intIndex As Integer

		Dim lstFunctions As List(Of String) = New List(Of String)

		Dim strCurrentFunction As String
		Dim strFinalFile As String = String.Empty

		Dim strLine As String
		Dim strStackTrace As String

		Dim reFunctionName As New Regex(REGEX_FUNCTION_NAME, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
		Dim reFileName As New Regex(REGEX_FILE_NAME, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
		Dim objMatch As Match

		' Process each line in objException.StackTrace
		' Populate strFunctions() with the function name of each line
		Using trTextReader As StringReader = New StringReader(objException.StackTrace)

			Do While trTextReader.Peek > -1
				strLine = trTextReader.ReadLine()

				If Not String.IsNullOrEmpty(strLine) Then
					strCurrentFunction = String.Empty

					objMatch = reFunctionName.Match(strLine)
					If objMatch.Success AndAlso objMatch.Groups.Count > 1 Then
						strCurrentFunction = objMatch.Groups(1).Value
					Else
						' Look for the word " in "
						intIndex = strLine.ToLower().IndexOf(" in ", StringComparison.Ordinal)
						If intIndex = 0 Then
							' " in" not found; look for the first space after startIndex 4
							intIndex = strLine.IndexOf(" ", 4, StringComparison.Ordinal)
						End If
						If intIndex = 0 Then
							' Space not found; use the entire string
							intIndex = strLine.Length - 1
						End If

						If intIndex > 0 Then
							strCurrentFunction = strLine.Substring(0, intIndex)
						End If

					End If

					If Not String.IsNullOrEmpty(strCurrentFunction) Then
						lstFunctions.Add(strCurrentFunction)
					End If

					If strFinalFile.Length = 0 Then
						' Also extract the file name where the Exception occurred
						objMatch = reFileName.Match(strLine)
						If objMatch.Success AndAlso objMatch.Groups.Count > 1 Then
							strFinalFile = objMatch.Groups(1).Value
						End If
					End If

				End If
			Loop

		End Using

		strStackTrace = String.Empty
		For intIndex = lstFunctions.Count - 1 To 0 Step -1
			If strStackTrace.Length = 0 Then
				strStackTrace = "Stack trace: " & lstFunctions(intIndex)
			Else
				strStackTrace &= "-:-" & lstFunctions(intIndex)
			End If
		Next intIndex

		If Not String.IsNullOrEmpty(strStackTrace) AndAlso Not String.IsNullOrWhiteSpace(strFinalFile) Then
			strStackTrace &= " in " & strFinalFile
		End If

		Return strStackTrace

	End Function

	''' <summary>
	''' Parses the headers in strHeaderLine to look for the names specified in lstHeaderNames
	''' </summary>
	''' <param name="strHeaderLine"></param>
	''' <param name="lstHeaderNames"></param>
	''' <returns>Dictionary with the header names and 0-based column index</returns>
	''' <remarks>Header names not found in strHeaderLine will have an index of -1</remarks>
	Public Shared Function ParseHeaderLine(ByVal strHeaderLine As String, ByVal lstHeaderNames As List(Of String), ByVal isCaseSensitive As Boolean) As Dictionary(Of String, Integer)
		Dim dctHeaderMapping = New Dictionary(Of String, Integer)

		Dim lstColumns = strHeaderLine.Split(ControlChars.Tab).ToList()

		For Each headerName In lstHeaderNames
			Dim colIndex As Integer = -1

			If isCaseSensitive Then
				colIndex = lstColumns.IndexOf(headerName)
			Else
				For i As Integer = 0 To lstColumns.Count - 1
					If String.Equals(lstColumns(i), headerName, StringComparison.CurrentCultureIgnoreCase) Then
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
	Public Shared Function PossiblyQuotePath(ByVal strPath As String) As String
		If String.IsNullOrEmpty(strPath) Then
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
	Public Shared Function CBoolSafe(ByVal Value As String) As Boolean
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
	Public Shared Function CBoolSafe(ByVal Value As String, ByVal blnDefaultValue As Boolean) As Boolean
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
	Public Shared Function CIntSafe(ByVal Value As String, ByVal intDefaultValue As Integer) As Integer
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
	Public Shared Function CSngSafe(ByVal Value As String, ByVal sngDefaultValue As Single) As Single
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
	Public Shared Function CopyAndRenameFileWithBackup( _
	  ByVal SourceFilePath As String, _
	  ByVal TargetFolder As String, _
	  ByVal TargetFileName As String, _
	  ByVal VersionCountToKeep As Integer) As Boolean

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
	Public Shared Function DbCStr(ByVal InpObj As Object) As String

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
	Public Shared Function DbCSng(ByVal InpObj As Object) As Single

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
	Public Shared Function DbCDbl(ByVal InpObj As Object) As Double

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
	Public Shared Function DbCInt(ByVal InpObj As Object) As Integer

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
	Public Shared Function DbCLng(ByVal InpObj As Object) As Long

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
	Public Shared Function DbCDec(ByVal InpObj As Object) As Decimal

		'If input object is DbNull, returns 0, otherwise returns Decimal representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CDec(InpObj)
		End If

	End Function

	''' <summary>
	''' Method for executing a db stored procedure when a data table is not returned
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
	''' <param name="sErrorMessage">Error message (output)</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks>No logging is performed by this procedure</remarks>
	Public Shared Function ExecuteSP(ByRef SpCmd As SqlClient.SqlCommand, ByVal ConnStr As String, ByVal MaxRetryCount As Integer, ByRef sErrorMessage As String) As Integer
		Const TimeoutSeconds As Integer = 30
		Return ExecuteSP(SpCmd, ConnStr, MaxRetryCount, TimeoutSeconds, sErrorMessage)
	End Function

	''' <summary>
	''' Method for executing a db stored procedure when a data table is not returned
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
	''' <param name="TimeoutSeconds">Database timeout length (seconds)</param>
	''' <param name="sErrorMessage">Error message (output)</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks>No logging is performed by this procedure</remarks>
	Public Shared Function ExecuteSP(ByRef SpCmd As SqlClient.SqlCommand, ByVal ConnStr As String, ByVal MaxRetryCount As Integer, ByVal TimeoutSeconds As Integer, ByRef sErrorMessage As String) As Integer

		Dim ResCode As Integer = -9999	'If this value is in error msg, then exception occurred before ResCode was set			
		Dim RetryCount As Integer = MaxRetryCount
		Dim blnDeadlockOccurred As Boolean

		sErrorMessage = String.Empty
		If RetryCount < 1 Then
			RetryCount = 1
		End If

		If TimeoutSeconds = 0 Then TimeoutSeconds = 30
		If TimeoutSeconds < 10 Then TimeoutSeconds = 10

		While RetryCount > 0	'Multiple retry loop for handling SP execution failures
			blnDeadlockOccurred = False
			Try
				Using Cn As SqlClient.SqlConnection = New SqlClient.SqlConnection(ConnStr)

					Cn.Open()

					SpCmd.Connection = Cn
					SpCmd.CommandTimeout = TimeoutSeconds
					SpCmd.ExecuteNonQuery()

					ResCode = CInt(SpCmd.Parameters("@Return").Value)

				End Using

				sErrorMessage = String.Empty

				Exit While
			Catch ex As Exception
				RetryCount -= 1
				sErrorMessage = "clsGlobal.ExecuteSP(), exception calling stored procedure " & SpCmd.CommandText & ", " & ex.Message
				sErrorMessage &= ". ResCode = " & ResCode.ToString & ". Retry count = " & RetryCount.ToString
				sErrorMessage &= "; " & GetExceptionStackTrace(ex)
				Console.WriteLine(sErrorMessage)
				If ex.Message.StartsWith("Could not find stored procedure " & SpCmd.CommandText) Then
					Exit While
				ElseIf ex.Message.Contains("was deadlocked") Then
					blnDeadlockOccurred = True
				End If
			End Try

			If RetryCount > 0 Then
				Thread.Sleep(20000)	'Wait 20 seconds before retrying
			End If
		End While

		If RetryCount < 1 Then
			'Too many retries, log and return error
			sErrorMessage = "Excessive retries"
			If blnDeadlockOccurred Then
				sErrorMessage &= " (including deadlock)"
			End If
			sErrorMessage &= " executing SP " & SpCmd.CommandText
			Console.WriteLine(sErrorMessage)
			If blnDeadlockOccurred Then
				Return clsDBTask.RET_VAL_DEADLOCK
			Else
				Return clsDBTask.RET_VAL_EXCESSIVE_RETRIES
			End If
		End If

		Return ResCode

	End Function

	Private Shared Function ByteArrayToString(ByVal arrInput() As Byte) As String
		' Converts a byte array into a hex string

		Dim strOutput As New Text.StringBuilder(arrInput.Length)

		For i As Integer = 0 To arrInput.Length - 1
			strOutput.Append(arrInput(i).ToString("X2"))
		Next

		Return strOutput.ToString().ToLower

	End Function

	''' <summary>
	''' Computes the MD5 hash for a file
	''' </summary>
	''' <param name="strPath"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ComputeFileHashMD5(ByVal strPath As String) As String
		' Calculates the MD5 hash of a given file
		' Code from Tim Hastings, at http://www.nonhostile.com/page000017.asp

		Dim objMD5 As New Security.Cryptography.MD5CryptoServiceProvider
		Dim arrHash() As Byte

		' open file (as read-only)
		Using objReader As Stream = New FileStream(strPath, FileMode.Open, FileAccess.Read)
			' hash contents of this stream
			arrHash = objMD5.ComputeHash(objReader)
		End Using

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
	Public Shared Function CreateHashcheckFile(ByVal strDataFilePath As String, ByVal blnComputeMD5Hash As Boolean) As String

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
	Public Shared Function CreateHashcheckFile(ByVal strDataFilePath As String, ByVal strMD5Hash As String) As String

		Dim fiDataFile As FileInfo
		Dim strHashFilePath As String

		fiDataFile = New FileInfo(strDataFilePath)

		If Not fiDataFile.Exists Then Return String.Empty

		strHashFilePath = fiDataFile.FullName & SERVER_CACHE_HASHCHECK_FILE_SUFFIX
		If String.IsNullOrWhiteSpace(strMD5Hash) Then strMD5Hash = String.Empty

		Using swOutFile As StreamWriter = New StreamWriter(New FileStream(strHashFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
			swOutFile.WriteLine("# Hashcheck file created " & DateTime.Now().ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT))
			swOutFile.WriteLine("size=" & fiDataFile.Length)
			swOutFile.WriteLine("modification_date_utc=" & fiDataFile.LastWriteTimeUtc.ToString("yyyy-MM-dd hh:mm:ss tt"))
			swOutFile.WriteLine("hash=" & strMD5Hash)
		End Using

		Return strHashFilePath

	End Function

	''' <summary>
	''' Computes the Sha-1 hash for a file
	''' </summary>
	''' <param name="strPath"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ComputeFileHashSha1(ByVal strPath As String) As String
		' Calculates the Sha-1 hash of a given file

		Dim objSha1 As New Security.Cryptography.SHA1CryptoServiceProvider
		Dim arrHash() As Byte

		' open file (as read-only)
		Using objReader As Stream = New FileStream(strPath, FileMode.Open, FileAccess.Read)
			' hash contents of this stream
			arrHash = objSha1.ComputeHash(objReader)
		End Using

		' Return the hash, formatted as a string
		Return ByteArrayToString(arrHash)

	End Function

	''' <summary>
	''' Compares two files, byte-by-byte
	''' </summary>
	''' <param name="strFilePath1">Path to the first file</param>
	''' <param name="strFilePath2">Path to the second file</param>
	''' <returns>True if the files match; false if they don't match; also returns false if either file is missing</returns>
	''' <remarks></remarks>
	Public Shared Function FilesMatch(ByVal strFilePath1 As String, ByVal strFilePath2 As String) As Boolean

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

			Using srFile1 As BinaryReader = New BinaryReader(New FileStream(fiFile1.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
				Using srFile2 As BinaryReader = New BinaryReader(New FileStream(fiFile2.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
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
	''' Replaces text in a string, ignoring case
	''' </summary>
	''' <param name="strTextToSearch"></param>
	''' <param name="strTextToFind"></param>
	''' <param name="strReplacementText"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ReplaceIgnoreCase(ByVal strTextToSearch As String, strTextToFind As String, strReplacementText As String) As String

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
	''' Returns True if the computer name is Pub-1000 or higher
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function UsingVirtualMachineOnPIC() As Boolean
		Dim rePub1000 As Regex = New Regex("Pub-1\d{3,}", RegexOptions.IgnoreCase)

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
	Public Shared Function ValidateFileVsHashcheck(ByVal strDataFilePath As String, ByVal strHashFilePath As String, ByRef strErrorMessage As String) As Boolean
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
	Public Shared Function ValidateFileVsHashcheck(ByVal strDataFilePath As String, ByVal strHashFilePath As String, ByRef strErrorMessage As String, ByVal blnCheckDate As Boolean, ByVal blnComputeHash As Boolean) As Boolean
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
	Public Shared Function ValidateFileVsHashcheck(ByVal strDataFilePath As String, ByVal strHashFilePath As String, ByRef strErrorMessage As String, ByVal blnCheckDate As Boolean, ByVal blnComputeHash As Boolean, ByVal blnCheckSize As Boolean) As Boolean

		Dim blnValidFile As Boolean = False
		strErrorMessage = String.Empty

		Dim lngExpectedFileSizeBytes As Int64 = 0
		Dim strExpectedHash As String = String.Empty
		Dim dtExpectedFileDate As DateTime = DateTime.MinValue

		Try

			Dim fiDataFile As FileInfo = New FileInfo(strDataFilePath)

			If String.IsNullOrEmpty(strHashFilePath) Then strHashFilePath = fiDataFile.FullName & SERVER_CACHE_HASHCHECK_FILE_SUFFIX
			Dim fiHashCheck As FileInfo = New FileInfo(strHashFilePath)

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

			Using srInfile As StreamReader = New StreamReader(New FileStream(fiHashCheck.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
				While srInfile.Peek > -1
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

#End Region
End Class


