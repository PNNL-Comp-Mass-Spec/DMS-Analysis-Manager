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
Imports PRISM.Files.clsFileTools

Public Class clsGlobal

#Region "Constants"
	Public Const LOG_LOCAL_ONLY As Boolean = True
	Public Const LOG_DATABASE As Boolean = False
	Public Const XML_FILENAME_PREFIX As String = "JobParameters_"
	Public Const XML_FILENAME_EXTENSION As String = "xml"

	Public Const STEPTOOL_PARAMFILESTORAGEPATH_PREFIX As String = "StepTool_ParamFileStoragePath_"
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
	''' Returns the directory in which the entry assembly (typically the Program .exe file) residues 
	''' </summary>
	''' <returns>Full directory path</returns>
	Public Shared Function GetAppFolderPath() As String

		Static strAppFolderPath As String = String.Empty

		If String.IsNullOrEmpty(strAppFolderPath) Then
			Dim objAssembly As System.Reflection.Assembly
			objAssembly = System.Reflection.Assembly.GetEntryAssembly()

			Dim fiAssemblyFile As System.IO.FileInfo
			fiAssemblyFile = New System.IO.FileInfo(objAssembly.Location)

			strAppFolderPath = fiAssemblyFile.DirectoryName
		End If

		Return strAppFolderPath

	End Function

	''' <summary>
	''' Returns the version string of the entry assembly (typically the Program .exe file)
	''' </summary>
	''' <returns>Assembly version, e.g. 1.0.4482.23831</returns>
	Public Shared Function GetAssemblyVersion() As String
		Dim objEntryAssembly As System.Reflection.Assembly
		objEntryAssembly = System.Reflection.Assembly.GetEntryAssembly()

		Return GetAssemblyVersion(objEntryAssembly)

	End Function

	''' <summary>
	''' Returns the version string of the specified assembly
	''' </summary>
	''' <returns>Assembly version, e.g. 1.0.4482.23831</returns>
	Public Shared Function GetAssemblyVersion(ByRef objAssembly As System.Reflection.Assembly) As String
		' objAssembly.FullName typically returns something like this:
		' AnalysisManagerProg, Version=2.3.4479.23831, Culture=neutral, PublicKeyToken=null
		' 
		' the goal is to extract out the text after Version= but before the next comma

		Dim reGetVersion As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("version=([0-9.]+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim reMatch As System.Text.RegularExpressions.Match
		Dim strVersion As String

		strVersion = objAssembly.FullName

		reMatch = reGetVersion.Match(strVersion)

		If reMatch.Success Then
			strVersion = reMatch.Groups(1).Value
		End If

		Return strVersion

	End Function

	''' <summary>
	''' Parses the .StackTrace text of the given expression to return a compact description of the current stack
	''' </summary>
	''' <param name="objException"></param>
	''' <returns>String similar to "Stack trace: clsCodeTest.Test-:-clsCodeTest.TestException-:-clsCodeTest.InnerTestException in clsCodeTest.vb:line 86"</returns>
	''' <remarks></remarks>
	Public Shared Function GetExceptionStackTrace(ByVal objException As System.Exception) As String
		Const REGEX_FUNCTION_NAME As String = "at ([^(]+)\("
		Const REGEX_FILE_NAME As String = "in .+\\(.+)"

		Dim trTextReader As System.IO.StringReader
		Dim intIndex As Integer

		Dim intFunctionCount As Integer = 0
		Dim strFunctions() As String

		Dim strCurrentFunction As String
		Dim strFinalFile As String = String.Empty

		Dim strLine As String = String.Empty
		Dim strStackTrace As String = String.Empty

		Dim reFunctionName As New System.Text.RegularExpressions.Regex(REGEX_FUNCTION_NAME, System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim reFileName As New System.Text.RegularExpressions.Regex(REGEX_FILE_NAME, System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim objMatch As System.Text.RegularExpressions.Match

		' Process each line in objException.StackTrace
		' Populate strFunctions() with the function name of each line
		trTextReader = New System.IO.StringReader(objException.StackTrace)

		intFunctionCount = 0
		ReDim strFunctions(9)

		Do While trTextReader.Peek >= 0
			strLine = trTextReader.ReadLine

			If Not strLine Is Nothing AndAlso strLine.Length > 0 Then
				strCurrentFunction = String.Empty

				objMatch = reFunctionName.Match(strLine)
				If objMatch.Success AndAlso objMatch.Groups.Count > 1 Then
					strCurrentFunction = objMatch.Groups(1).Value
				Else
					' Look for the word " in "
					intIndex = strLine.ToLower.IndexOf(" in ")
					If intIndex = 0 Then
						' " in" not found; look for the first space after startIndex 4
						intIndex = strLine.IndexOf(" ", 4)
					End If
					If intIndex = 0 Then
						' Space not found; use the entire string
						intIndex = strLine.Length - 1
					End If

					If intIndex > 0 Then
						strCurrentFunction = strLine.Substring(0, intIndex)
					End If

				End If

				If Not strCurrentFunction Is Nothing AndAlso strCurrentFunction.Length > 0 Then
					If intFunctionCount >= strFunctions.Length Then
						' Reserve more space in strFunctions()
						ReDim Preserve strFunctions(strFunctions.Length * 2 - 1)
					End If

					strFunctions(intFunctionCount) = strCurrentFunction
					intFunctionCount += 1
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

		strStackTrace = String.Empty
		For intIndex = intFunctionCount - 1 To 0 Step -1
			If Not strFunctions(intIndex) Is Nothing Then
				If strStackTrace.Length = 0 Then
					strStackTrace = "Stack trace: " & strFunctions(intIndex)
				Else
					strStackTrace &= "-:-" & strFunctions(intIndex)
				End If
			End If
		Next intIndex

		If Not strStackTrace Is Nothing AndAlso strFinalFile.Length > 0 Then
			strStackTrace &= " in " & strFinalFile
		End If

		Return strStackTrace

	End Function

	Public Shared Function PossiblyQuotePath(strPath As String) As String
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

	Public Shared Function CBoolSafe(ByVal Value As String) As Boolean
		Dim blnValue As Boolean = False

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

	Public Shared Function CBoolSafe(ByVal Value As String, ByVal blnDefaultValue As Boolean) As Boolean
		Dim blnValue As Boolean = False

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

		Dim ioSrcFile As System.IO.FileInfo
		Dim ioFileToRename As System.IO.FileInfo

		Dim strBaseName As String
		Dim strBaseNameCurrent As String

		Dim strNewFilePath As String
		Dim strExtension As String

		Dim intRevision As Integer

		Try
			ioSrcFile = New System.IO.FileInfo(SourceFilePath)
			If Not ioSrcFile.Exists Then
				' Source file not found
				Return False
			Else
				strBaseName = System.IO.Path.GetFileNameWithoutExtension(TargetFileName)
				strExtension = System.IO.Path.GetExtension(TargetFileName)
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

					ioFileToRename = New System.IO.FileInfo(System.IO.Path.Combine(TargetFolder, strBaseNameCurrent))
					strNewFilePath = System.IO.Path.Combine(TargetFolder, strBaseName & "_" & (intRevision + 1).ToString & strExtension)

					' Confirm that strNewFilePath doesn't exist; delete it if it does
					If System.IO.File.Exists(strNewFilePath) Then
						System.IO.File.Delete(strNewFilePath)
					End If

					' Rename the current file to strNewFilePath
					If ioFileToRename.Exists Then
						ioFileToRename.MoveTo(strNewFilePath)
					End If

				Catch ex As Exception
					' Ignore errors here; we'll continue on with the next file
				End Try

			Next intRevision

			strNewFilePath = System.IO.Path.Combine(TargetFolder, TargetFileName)

			' Now copy the file from SourceFilePath to strNewFilePath
			ioSrcFile.CopyTo(strNewFilePath, True)

		Catch ex As Exception
			' Ignore errors here
		End Try

		Return True

	End Function

	Public Shared Function DbCStr(ByVal InpObj As Object) As String

		'If input object is DbNull, returns "", otherwise returns String representation of object
		If InpObj Is DBNull.Value Then
			Return ""
		Else
			Return CStr(InpObj)
		End If

	End Function

	Public Shared Function DbCSng(ByVal InpObj As Object) As Single

		'If input object is DbNull, returns 0.0, otherwise returns Single representation of object
		If InpObj Is DBNull.Value Then
			Return 0.0
		Else
			Return CSng(InpObj)
		End If

	End Function

	Public Shared Function DbCDbl(ByVal InpObj As Object) As Double

		'If input object is DbNull, returns 0.0, otherwise returns Double representation of object
		If InpObj Is DBNull.Value Then
			Return 0.0
		Else
			Return CDbl(InpObj)
		End If

	End Function

	Public Shared Function DbCInt(ByVal InpObj As Object) As Integer

		'If input object is DbNull, returns 0, otherwise returns Integer representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CInt(InpObj)
		End If

	End Function

	Public Shared Function DbCLng(ByVal InpObj As Object) As Long

		'If input object is DbNull, returns 0, otherwise returns Integer representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CLng(InpObj)
		End If

	End Function

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
	Public Shared Function ExecuteSP(ByRef SpCmd As System.Data.SqlClient.SqlCommand, ByVal ConnStr As String, ByVal MaxRetryCount As Integer, ByRef sErrorMessage As String) As Integer
		Dim TimeoutSeconds As Integer = 30
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
	Public Shared Function ExecuteSP(ByRef SpCmd As System.Data.SqlClient.SqlCommand, ByVal ConnStr As String, ByVal MaxRetryCount As Integer, ByVal TimeoutSeconds As Integer, ByRef sErrorMessage As String) As Integer

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
				Using Cn As System.Data.SqlClient.SqlConnection = New System.Data.SqlClient.SqlConnection(ConnStr)

					Cn.Open()

					SpCmd.Connection = Cn
					SpCmd.CommandTimeout = TimeoutSeconds
					SpCmd.ExecuteNonQuery()

					ResCode = CInt(SpCmd.Parameters("@Return").Value)

				End Using

				sErrorMessage = String.Empty

				Exit While
			Catch ex As System.Exception
				RetryCount -= 1
				sErrorMessage = "clsGlobal.ExecuteSP(), exception calling stored procedure " & SpCmd.CommandText & ", " & ex.Message
				sErrorMessage &= ". ResCode = " & ResCode.ToString & ". Retry count = " & RetryCount.ToString
				sErrorMessage &= "; " & clsGlobal.GetExceptionStackTrace(ex)
				Console.WriteLine(sErrorMessage)
				If ex.Message.StartsWith("Could not find stored procedure " & SpCmd.CommandText) Then
					Exit While
				ElseIf ex.Message.Contains("was deadlocked") Then
					blnDeadlockOccurred = True
				End If
			End Try

			If RetryCount > 0 Then
				System.Threading.Thread.Sleep(20000)	'Wait 20 seconds before retrying
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

		Dim strOutput As New System.Text.StringBuilder(arrInput.Length)

		For i As Integer = 0 To arrInput.Length - 1
			strOutput.Append(arrInput(i).ToString("X2"))
		Next

		Return strOutput.ToString().ToLower

	End Function

	Public Shared Function ComputeFileHashMD5(ByVal strPath As String) As String
		' Calculates the MD5 hash of a given file
		' Code from Tim Hastings, at http://www.nonhostile.com/page000017.asp

		Dim objReader As System.IO.Stream
		Dim objMD5 As New System.Security.Cryptography.MD5CryptoServiceProvider
		Dim arrHash() As Byte

		' open file (as read-only)
		objReader = New System.IO.FileStream(strPath, IO.FileMode.Open, IO.FileAccess.Read)

		' hash contents of this stream
		arrHash = objMD5.ComputeHash(objReader)

		' Cleanup the objects
		objReader.Close()
		objReader = Nothing
		objMD5 = Nothing

		' Return the hash, formatted as a string
		Return ByteArrayToString(arrHash)

	End Function

	Public Shared Function ComputeFileHashSha1(ByVal strPath As String) As String
		' Calculates the Sha-1 hash of a given file

		Dim objReader As System.IO.Stream
		Dim objSha1 As New System.Security.Cryptography.SHA1CryptoServiceProvider
		Dim arrHash() As Byte

		' open file (as read-only)
		objReader = New System.IO.FileStream(strPath, IO.FileMode.Open, IO.FileAccess.Read)

		' hash contents of this stream
		arrHash = objSha1.ComputeHash(objReader)

		' Cleanup the objects
		objReader.Close()
		objReader = Nothing
		objSha1 = Nothing

		' Return the hash, formatted as a string
		Return ByteArrayToString(arrHash)

	End Function

#End Region

End Class


