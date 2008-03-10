'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/20/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files.clsFileTools
Imports PRISM.Logging

Namespace AnalysisManagerBase

	Public Class clsGlobal

		'*********************************************************************************************************
		'Contains functions/variables common to all parts of the Analysis Manager
		'*********************************************************************************************************

#Region "Constants"
		Public Const LOG_LOCAL_ONLY As Boolean = True
		Public Const LOG_DATABASE As Boolean = False
#End Region

#Region "Module variables"
		Public Shared AppFilePath As String = ""
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

			If InpComment Is Nothing OrElse InpComment = "" Then
				Return NewComment
			Else
				Return InpComment & "; " & NewComment
			End If

		End Function

		''' <summary>
		''' Deletes all files in working directory
		''' </summary>
		''' <param name="WorkDir">ull path to working directory</param>
		''' <param name="MyLogger">Logging object for error recording</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Public Shared Function CleanWorkDir(ByVal WorkDir As String, ByVal MyLogger As ILogger) As Boolean

			'Cleans all files out of the working directory
			Dim FoundFiles() As String
			Dim FoundFolders() As String
			Dim DumName As String

			WorkDir = CheckTerminator(WorkDir)

			FoundFiles = Directory.GetFiles(WorkDir)
			FoundFolders = Directory.GetDirectories(WorkDir)

			'Try to ensure there are no open objects with file handles
			GC.Collect()
			GC.WaitForPendingFinalizers()
			System.Threading.Thread.Sleep(10000)		'Move this to before GC after troubleshooting complete

			'Delete the files
			Try
				For Each DumName In FoundFiles
					'Verify file is not set to readonly
					File.SetAttributes(DumName, File.GetAttributes(DumName) And (Not FileAttributes.ReadOnly))
					File.Delete(DumName)
				Next
			Catch Ex As Exception
				MyLogger.PostEntry("clsGlobal.ClearWorkDir(), Error deleting files in working directory: " & Ex.Message, _
				 ILogger.logMsgType.logError, True)
				Return False
			End Try

			'Delete the folders
			Try
				For Each DumName In FoundFolders
					Directory.Delete(DumName, True)
				Next
			Catch Ex As Exception
				MyLogger.PostEntry("Error deleting folders in working directory: " & Ex.Message, _
				 ILogger.logMsgType.logError, True)
				Return False
			End Try

			Return True

		End Function

		''' <summary>
		''' Creates a dummy file in the application directory to be used for controlling job request bypass
		''' </summary>
		''' <remarks></remarks>
		Public Shared Sub CreateStatusFlagFile()

			Dim ExeFi As New FileInfo(AppFilePath)
			Dim TestFileFi As New FileInfo(Path.Combine(ExeFi.DirectoryName, "FlagFile.txt"))
			Dim Sw As StreamWriter = TestFileFi.AppendText()

			Sw.WriteLine(Now().ToString)
			Sw.Flush()
			Sw.Close()

			Sw = Nothing
			TestFileFi = Nothing
			ExeFi = Nothing

		End Sub

		''' <summary>
		''' Deletes the flag file
		''' </summary>
		''' <param name="MyLogger">Logging object for error recording</param>
		''' <remarks></remarks>
		Public Shared Sub DeleteStatusFlagFile(ByVal MyLogger As ILogger)

			'Deletes the job request control flag file
			Dim ExeFi As New FileInfo(AppFilePath)
			Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

			Try
				If File.Exists(TestFile) Then
					File.Delete(TestFile)
				End If
			Catch Err As Exception
				MyLogger.PostEntry("DeleteStatusFlagFile, " & Err.Message, ILogger.logMsgType.logError, True)
			End Try

		End Sub

		''' <summary>
		''' Determines if flag file exists in application directory
		''' </summary>
		''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
		''' <remarks></remarks>
		Public Shared Function DetectStatusFlagFile() As Boolean

			'Returns True if job request control flag file exists
			Dim ExeFi As New FileInfo(AppFilePath)
			Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

			Return File.Exists(TestFile)

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
#End Region

	End Class

End Namespace
