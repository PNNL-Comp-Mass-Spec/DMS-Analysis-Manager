'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/20/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files.clsFileTools

Namespace AnalysisManagerBase

	Public Class clsGlobal

		'*********************************************************************************************************
		'Contains functions/variables common to all parts of the Analysis Manager
		'*********************************************************************************************************

#Region "Constants"
		Public Const LOG_LOCAL_ONLY As Boolean = True
        Public Const LOG_DATABASE As Boolean = False
        Public Const XML_FILENAME_PREFIX As String = "JobParameters_"
        Public Const XML_FILENAME_EXTENSION As String = "xml"
        Public Const ERROR_DELETING_FILES_FILENAME As String = "Error_Deleting_Files_Please_Delete_Me.txt"
#End Region

#Region "Module variables"
		Public Shared AppFilePath As String = ""
		Public Shared FilesToDelete As New List(Of String)				' List of file names to NOT move to the result folder; this list is used by both MoveResultFiles() and RemoveNonResultFiles()
		Public Shared m_FilesToDeleteExt As New List(Of String)			' List of file extensions to NOT move to the result folder; Comparison test uses "TmpFile.ToLower.EndsWith(m_FilesToDeleteExt(x).ToLower)"
		Public Shared m_ExceptionFiles As New List(Of String)			' List of file names that WILL be moved to the result folder, even if they are in FilesToDelete or m_FilesToDeleteExt; Comparison test uses "TmpFile.ToLower.Contains(m_ExceptionFiles(x).ToLower)"
		Public Shared m_Completions_Msg As String = ""
        Public Shared m_ServerFilesToDelete As New List(Of String)      ' List of file names to remove from the server
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

            If InpComment Is Nothing OrElse InpComment.Trim(" "c) = String.Empty Then
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

		''' <summary>
		''' Deletes files in specified directory that have been previously flagged as not wanted in results folder
		''' </summary>
		''' <param name="WorkDir">Full path to work directory</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
        Public Shared Function RemoveNonResultFiles(ByVal WorkDir As String, ByVal DebugLevel As Integer) As Boolean

            Dim FileToDelete As String = ""

            Try
                'Log status
                If DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Remove Non Result Files in " & WorkDir & "; FilesToDelete contains " & FilesToDelete.Count.ToString & " entries")
                End If

                For Each FileName As String In FilesToDelete
                    FileToDelete = Path.GetFileName(FileName)
                    If DebugLevel >= 5 Then 'Log file to be deleted
                        If (Not FileToDelete.ToLower.Contains(".dta")) And (Not FileToDelete.ToLower.Contains(".out")) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting " & FileToDelete)
                        End If
                    End If
                    FileToDelete = Path.Combine(WorkDir, FileToDelete)
                    If File.Exists(FileToDelete) Then
                        'Verify file is not set to readonly, then delete it
                        File.SetAttributes(FileToDelete, File.GetAttributes(FileToDelete) And (Not FileAttributes.ReadOnly))
                        File.Delete(FileToDelete)
                    End If
                Next
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsGlobal.RemoveNonResultFiles(), Error deleting file " & FileToDelete & ":" & ex.Message)
                'Create a flag file if there was trouble deleting the files
                CreateErrorDeletingFilesFlagFile()
                'Even if an exception occurred, return true since the results were already copied back to the server
                Return True
            End Try

            Return True

        End Function

        ''' <summary>
        ''' Deletes files in specified directory that have been previously flagged as not wanted in results folder
        ''' </summary>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Public Shared Function RemoveNonResultServerFiles(ByVal DebugLevel As Integer) As Boolean

            Dim FileToDelete As String = ""

            Try
                'Log status
                If DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Remove Files from Transfer folder on server; m_ServerFilesToDelete contains " & m_ServerFilesToDelete.Count.ToString & " entries")
                End If

                For Each FileToDelete In m_ServerFilesToDelete
                    If DebugLevel >= 4 Then  'Log file to be deleted
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting " & FileToDelete)
                    End If
                    If File.Exists(FileToDelete) Then
                        'Verify file is not set to readonly, then delete it
                        File.SetAttributes(FileToDelete, File.GetAttributes(FileToDelete) And (Not FileAttributes.ReadOnly))
                        File.Delete(FileToDelete)
                    End If
                Next
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsGlobal.RemoveNonResultServerFiles(), Error deleting file " & FileToDelete & ":" & ex.Message)
                'Even if an exception occurred, return true since the results were already copied back to the server
                Return True
            End Try

            Return True

        End Function

		''' <summary>
		''' Deletes the entries from the three String Lists used to track which files to delete or keep when packaging the results
		''' </summary>
		''' <remarks></remarks>
		Public Shared Sub ResetFilesToDeleteOrKeep()
			FilesToDelete.Clear()
			m_FilesToDeleteExt.Clear()
			m_ExceptionFiles.Clear()
		End Sub

		''' <summary>
		''' Deletes all files in working directory
		''' </summary>
		''' <param name="WorkDir">Full path to working directory</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
        Public Shared Function CleanWorkDir(ByVal WorkDir As String) As Boolean

            Dim FoundFiles() As String
            Dim FoundFolders() As String
            Dim DumName As String

            WorkDir = CheckTerminator(WorkDir)

            FoundFiles = Directory.GetFiles(WorkDir)
            FoundFolders = Directory.GetDirectories(WorkDir)

            'Try to ensure there are no open objects with file handles
            GC.Collect()
            GC.WaitForPendingFinalizers()
            System.Threading.Thread.Sleep(10000)        'Move this to before GC after troubleshooting complete

            'Delete the files
            Try
                For Each DumName In FoundFiles
                    'Verify file is not set to readonly
                    File.SetAttributes(DumName, File.GetAttributes(DumName) And (Not FileAttributes.ReadOnly))
                    File.Delete(DumName)
                Next
            Catch Ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsGlobal.ClearWorkDir(), Error deleting files in working directory " & WorkDir & ": " & Ex.Message)
                Return False
            End Try

            'Delete the folders
            Try
                For Each DumName In FoundFolders
                    Directory.Delete(DumName, True)
                Next
            Catch Ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting folders in working directory " & WorkDir & ": " & Ex.Message)
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

            Sw.WriteLine(System.DateTime.Now().ToString)
			Sw.Flush()
			Sw.Close()

			Sw = Nothing
			TestFileFi = Nothing
			ExeFi = Nothing

		End Sub

		''' <summary>
		''' Deletes the flag file
		''' </summary>
        ''' <remarks></remarks>
        Public Shared Sub DeleteStatusFlagFile()

            'Deletes the job request control flag file
            Dim ExeFi As New FileInfo(AppFilePath)
            Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, "FlagFile.txt")

            Try
                If File.Exists(TestFile) Then
                    File.Delete(TestFile)
                End If
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteStatusFlagFile, " & Err.Message)
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

		Public Shared Function CBoolSafe(ByVal Value As String) As Boolean
			Dim blnValue As Boolean = False

			Try
				blnValue = CBool(Value)
			Catch ex As Exception
				blnValue = False
			End Try

			Return blnValue

		End Function

        Public Shared Function CBoolSafe(ByVal Value As String, ByVal blnDefaultValue As Boolean) As Boolean
            Dim blnValue As Boolean = False

            Try
                blnValue = CBool(Value)
            Catch ex As Exception
                blnValue = blnDefaultValue
            End Try

            Return blnValue

        End Function


        Public Shared Function CIntSafe(ByVal Value As String, ByVal intDefaultValue As Integer) As Integer
            Dim intValue As Integer

            Try
                intValue = CInt(Value)
            Catch ex As Exception
                intValue = intDefaultValue
            End Try

            Return intValue

        End Function

        Public Shared Function CSngSafe(ByVal Value As String, ByVal sngDefaultValue As Single) As Single
            Dim sngValue As Single

            Try
                sngValue = CSng(Value)
            Catch ex As Exception
                sngValue = sngDefaultValue
            End Try

            Return sngValue

        End Function

        ''' <summary>
        ''' Constructs a description of the given job using the job number, step tool name, and dataset name
        ''' </summary>
        ''' <param name="JobStartTimeStamp">Time job started</param>
        ''' <param name="Job">Job name</param>
        ''' <param name="Dataset">Dataset name</param>
        ''' <param name="ToolName">Tool name (or step tool name)</param>
        ''' <returns>Info string, similar to: Job 375797; DataExtractor (XTandem), Step 4; QC_Shew_09_01_b_pt5_25Mar09_Griffin_09-02-03; 3/26/2009 3:17:57 AM</returns>
        ''' <remarks></remarks>
        Public Shared Function ConstructMostRecentJobInfoText(ByVal JobStartTimeStamp As String, ByVal Job As Integer, ByVal Dataset As String, ByVal ToolName As String) As String

            Try
                If JobStartTimeStamp Is Nothing Then JobStartTimeStamp = String.Empty
                If ToolName Is Nothing Then ToolName = "??"
                If Dataset Is Nothing Then Dataset = "??"

                Return "Job " & Job.ToString & "; " & ToolName & "; " & Dataset & "; " & JobStartTimeStamp
            Catch ex As Exception
                ' Error combining the terms; return an empty string
                Return String.Empty
            End Try

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
					If strExtension Is Nothing OrElse strExtension.Length = 0 Then
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

        ''' <summary>
        ''' Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
        ''' </summary>
        ''' <remarks></remarks>
        Public Shared Sub CreateErrorDeletingFilesFlagFile()

            Dim ExeFi As New FileInfo(AppFilePath)
            Dim TestFileFi As New FileInfo(Path.Combine(ExeFi.DirectoryName, ERROR_DELETING_FILES_FILENAME))
            Dim Sw As StreamWriter = TestFileFi.AppendText()

            Sw.WriteLine(System.DateTime.Now().ToString)
            Sw.Flush()
            Sw.Close()

            Sw = Nothing
            TestFileFi = Nothing
            ExeFi = Nothing

        End Sub

        ''' <summary>
        ''' Deletes the error deleting files flag file
        ''' </summary>
        ''' <remarks></remarks>
        Public Shared Sub DeleteErrorDeletingFilesFlagFile()

            'Deletes the job request control flag file
            Dim ExeFi As New FileInfo(AppFilePath)
            Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, ERROR_DELETING_FILES_FILENAME)

            Try
                If File.Exists(TestFile) Then
                    File.Delete(TestFile)
                End If
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteStatusFlagFile, " & Err.Message)
            End Try

        End Sub

        ''' <summary>
        ''' Determines if error deleting files flag file exists in application directory
        ''' </summary>
        ''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
        ''' <remarks></remarks>
        Public Shared Function DetectErrorDeletingFilesFlagFile() As Boolean

            'Returns True if job request control flag file exists
            Dim ExeFi As New FileInfo(AppFilePath)
            Dim TestFile As String = Path.Combine(ExeFi.DirectoryName, ERROR_DELETING_FILES_FILENAME)

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

        ''' <summary>
        ''' Given a log file with a name like AnalysisMgr_03-25-2009.txt, returns the log file name for the previous day
        ''' </summary>
        ''' <param name="strLogFilePath"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Shared Function DecrementLogFilePath(ByVal strLogFilePath As String) As String

            Dim reLogFileName As System.Text.RegularExpressions.Regex
            Dim objMatch As System.Text.RegularExpressions.Match

            Dim intYear As Integer
            Dim intMonth As Integer
            Dim intDay As Integer
            Dim strPreviousLogFilePath As String = String.Empty

            Try
                reLogFileName = New System.Text.RegularExpressions.Regex("(.+_)(\d+)-(\d+)-(\d+).\S+", System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)

                objMatch = reLogFileName.Match(strLogFilePath)

                If objMatch.Success AndAlso objMatch.Groups.Count >= 4 Then
                    intMonth = CInt(objMatch.Groups(2).Value)
                    intDay = CInt(objMatch.Groups(3).Value)
                    intYear = CInt(objMatch.Groups(4).Value)

                    Dim dtCurrentDate As System.DateTime
                    Dim dtNewDate As System.DateTime

                    dtCurrentDate = System.DateTime.Parse(intYear & "-" & intMonth & "-" & intDay)
                    dtNewDate = dtCurrentDate.Subtract(New System.TimeSpan(1, 0, 0, 0))

                    strPreviousLogFilePath = objMatch.Groups(1).Value & dtNewDate.ToString("MM-dd-yyyy") & System.IO.Path.GetExtension(strLogFilePath)
                End If

            Catch ex As Exception
                Console.WriteLine("Error in DecrementLogFilePath: " & ex.Message)
            End Try

            Return strPreviousLogFilePath

        End Function

        ''' <summary>
        ''' Parses the log files for this manager to determine the recent error messages, returning up to intErrorMessageCountToReturn of them
        ''' Will use objLogger to determine the most recent log file
        ''' Also examines the message info stored in objLogger
        ''' Lastly, if strMostRecentJobInfo is empty, then will update it with info on the most recent job started
        ''' </summary>
        ''' <param name="intErrorMessageCountToReturn">Maximum number of error messages to return</param>
        ''' <param name="strMostRecentJobInfo">Info on the most recent job started by this manager</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function DetermineRecentErrorMessages(ByRef m_MgrSettings As clsAnalysisMgrSettings, ByVal intErrorMessageCountToReturn As Integer, _
                                                            ByRef strMostRecentJobInfo As String) As String()

            ' This regex will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
            Const ERROR_MATCH_REGEX As String = "^([^,]+),(.+), Error, *$"

            ' This regex looks for information on a job starting
            Const JOB_START_REGEX As String = "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool (.+), Normal"

            ' The following effectively defines the number of days in the past to search when finding recent errors
            Const MAX_LOG_FILES_TO_SEARCH As Integer = 5

            Dim blnLoggerReportsError As Boolean
            Dim strLogFilePath As String
            Dim intLogFileCountProcessed As Integer

            Dim srInFile As System.IO.StreamReader

            Dim reErrorLine As System.Text.RegularExpressions.Regex
            Dim reJobStartLine As System.Text.RegularExpressions.Regex

            Dim objMatch As System.Text.RegularExpressions.Match

            Dim qErrorMsgQueue As System.Collections.Queue
            Dim htUniqueErrorMessages As System.Collections.Hashtable

            ' Note that strRecentErrorMessages() and dtRecentErrorMessageDates() are parallel arrays
            Dim intRecentErrorMessageCount As Integer
            Dim strRecentErrorMessages() As String = New String() {}
            Dim dtRecentErrorMessageDates() As DateTime

            Dim strLineIn As String

            Dim blnCheckForMostRecentJob As Boolean
            Dim strMostRecentJobInfoFromLogs As String

            Dim strTimestamp As String
            Dim strErrorMessageClean As String

            Try
                If strMostRecentJobInfo Is Nothing Then strMostRecentJobInfo = String.Empty
                strMostRecentJobInfoFromLogs = String.Empty

                'If objLogger Is Nothing Then
                '    intRecentErrorMessageCount = 0
                '    ReDim strRecentErrorMessages(-1)
                'Else
                If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

                intRecentErrorMessageCount = 0
                ReDim strRecentErrorMessages(intErrorMessageCountToReturn - 1)
                ReDim dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)

                ' Initialize the RegEx that splits out the timestamp from the error message
                reErrorLine = New System.Text.RegularExpressions.Regex(ERROR_MATCH_REGEX, System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
                reJobStartLine = New System.Text.RegularExpressions.Regex(JOB_START_REGEX, System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)

                ' Initialize the queue that holds recent error messages
                qErrorMsgQueue = New System.Collections.Queue(intErrorMessageCountToReturn)

                ' Initialize the hashtable to hold the error messages, but without date stamps
                htUniqueErrorMessages = New System.Collections.Hashtable

                ' Examine the most recent error reported by objLogger
                strLineIn = clsLogTools.MostRecentErrorMessage
                If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then
                    blnLoggerReportsError = True
                Else
                    blnLoggerReportsError = False
                End If


                strLogFilePath = GetRecentLogFilename(m_MgrSettings)
                If intErrorMessageCountToReturn > 1 OrElse Not blnLoggerReportsError Then

                    ' Recent error message reported by objLogger is empty or intErrorMessageCountToReturn is greater than one
                    ' Open log file strLogFilePath to find the most recent error messages
                    ' If not enough error messages are found, we will look through previous log files

                    intLogFileCountProcessed = 0
                    blnCheckForMostRecentJob = True

                    Do While qErrorMsgQueue.Count < intErrorMessageCountToReturn AndAlso intLogFileCountProcessed < MAX_LOG_FILES_TO_SEARCH

                        If System.IO.File.Exists(strLogFilePath) Then
                            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                            If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

                            Do While srInFile.Peek >= 0
                                strLineIn = srInFile.ReadLine

                                If Not strLineIn Is Nothing Then
                                    objMatch = reErrorLine.Match(strLineIn)

                                    If objMatch.Success Then
                                        DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
                                    End If

                                    If blnCheckForMostRecentJob Then
                                        objMatch = reJobStartLine.Match(strLineIn)
                                        If objMatch.Success Then
                                            Try
                                                strMostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(objMatch.Groups(1).Value, _
                                                                                                              CInt(objMatch.Groups(2).Value), _
                                                                                                              objMatch.Groups(3).Value, _
                                                                                                              objMatch.Groups(4).Value)
                                            Catch ex As Exception
                                                ' Ignore errors here
                                            End Try
                                        End If
                                    End If
                                End If
                            Loop

                            srInFile.Close()

                            If blnCheckForMostRecentJob AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
                                ' We determine the most recent job; no need to check other log files
                                blnCheckForMostRecentJob = False
                            End If

                        Else
                            ' Log file not found; that's OK, we'll decrement the name by one day and keep checking
                        End If

                        ' Increment the log file counter, regardless of whether or not the log file was found
                        intLogFileCountProcessed += 1

                        If qErrorMsgQueue.Count < intErrorMessageCountToReturn Then
                            ' We still haven't found intErrorMessageCountToReturn error messages
                            ' Keep checking older log files as long as qErrorMsgQueue.Count < intErrorMessageCountToReturn

                            ' Decrement the log file path by one day
                            strLogFilePath = DecrementLogFilePath(strLogFilePath)
                            If strLogFilePath Is Nothing OrElse strLogFilePath = String.Empty Then
                                Exit Do
                            End If
                        End If
                    Loop

                End If

                If blnLoggerReportsError Then
                    ' Append the error message reported by the Logger to the error message queue (treating it as the newest error)
                    strLineIn = clsLogTools.MostRecentErrorMessage
                    objMatch = reErrorLine.Match(strLineIn)

                    If objMatch.Success Then
                        DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
                    End If
                End If


                ' Populate strRecentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
                Do While qErrorMsgQueue.Count > 0
                    strErrorMessageClean = CStr(qErrorMsgQueue.Dequeue())

                    ' Find the newest timestamp for this message
                    If htUniqueErrorMessages.ContainsKey(strErrorMessageClean) Then
                        strTimestamp = CStr(htUniqueErrorMessages(strErrorMessageClean))
                    Else
                        ' This code should not be reached
                        strTimestamp = ""
                    End If

                    If intRecentErrorMessageCount >= strRecentErrorMessages.Length Then
                        ' Need to reserve more memory; this is unexpected
                        ReDim Preserve strRecentErrorMessages(strRecentErrorMessages.Length * 2 - 1)
                        ReDim Preserve dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)
                    End If

                    strRecentErrorMessages(intRecentErrorMessageCount) = strTimestamp & ", " & strErrorMessageClean.TrimStart(" "c)

                    Try
                        dtRecentErrorMessageDates(intRecentErrorMessageCount) = CDate(strTimestamp)
                    Catch ex As Exception
                        ' Error converting date;
                        dtRecentErrorMessageDates(intRecentErrorMessageCount) = System.DateTime.MinValue
                    End Try

                    intRecentErrorMessageCount += 1
                Loop

                If intRecentErrorMessageCount < strRecentErrorMessages.Length Then
                    ' Shrink the arrays
                    ReDim Preserve strRecentErrorMessages(intRecentErrorMessageCount - 1)
                    ReDim Preserve dtRecentErrorMessageDates(intRecentErrorMessageCount - 1)
                End If

                If intRecentErrorMessageCount > 1 Then
                    ' Sort the arrays by descending date
                    Array.Sort(dtRecentErrorMessageDates, strRecentErrorMessages)
                    Array.Reverse(dtRecentErrorMessageDates)
                    Array.Reverse(strRecentErrorMessages)
                End If

                If strMostRecentJobInfo.Length = 0 Then
                    If Not strMostRecentJobInfoFromLogs Is Nothing AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
                        ' Update strMostRecentJobInfo
                        strMostRecentJobInfo = strMostRecentJobInfoFromLogs
                    End If
                End If

            Catch ex As Exception
                ' Ignore errors here
                Try
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in clsGlobal.DetermineRecentErrorMessages: " & ex.Message)
                Catch ex2 As Exception
                    ' Ignore errors logging the error
                End Try
            End Try

            Return strRecentErrorMessages

        End Function

        Protected Shared Sub DetermineRecentErrorCacheError(ByRef objMatch As System.Text.RegularExpressions.Match, _
                                                            ByVal strErrorMessage As String, _
                                                            ByRef htUniqueErrorMessages As System.Collections.Hashtable, _
                                                            ByRef qErrorMsgQueue As System.Collections.Queue, _
                                                            ByVal intMaxErrorMessageCountToReturn As Integer)

            Dim strTimestamp As String
            Dim strErrorMessageClean As String
            Dim strQueuedError As String

            Dim blnAddItemToQueue As Boolean
            Dim objItem As Object

            ' See if this error is present in htUniqueErrorMessages yet
            ' If it is present, update the timestamp in htUniqueErrorMessages
            ' If not present, queue it

            If objMatch.Groups.Count >= 2 Then
                strTimestamp = objMatch.Groups(1).Value
                strErrorMessageClean = objMatch.Groups(2).Value
            Else
                ' Regex didn't match; this is unexpected
                strTimestamp = System.DateTime.MinValue.ToString()
                strErrorMessageClean = strErrorMessage
            End If

            ' Check whether strErrorMessageClean is in the hash table
            objItem = htUniqueErrorMessages.Item(strErrorMessageClean)
            If Not objItem Is Nothing Then
                ' The error message is present
                ' Update the timestamp associated with strErrorMessageClean if the time stamp is newer than the stored one
                Try
                    If System.DateTime.Parse(strTimestamp) > System.DateTime.Parse(CStr(objItem)) Then
                        htUniqueErrorMessages(strErrorMessageClean) = strTimestamp
                    End If
                Catch ex As Exception
                    ' Date comparison failed; leave the existing timestamp unchanged
                End Try

            Else
                ' The error message is not present
                htUniqueErrorMessages.Add(strErrorMessageClean, strTimestamp)
            End If

            If Not qErrorMsgQueue.Contains(strErrorMessageClean) Then
                ' Queue this message
                ' However, if we already have intErrorMessageCountToReturn messages queued, then dequeue the oldest one

                If qErrorMsgQueue.Count < intMaxErrorMessageCountToReturn Then
                    qErrorMsgQueue.Enqueue(strErrorMessageClean)
                Else
                    ' Too many queued messages, so remove oldest one
                    ' However, only do this if the new error message has a timestamp newer than the oldest queued message
                    '  (this is a consideration when processing multiple log files)

                    blnAddItemToQueue = True

                    strQueuedError = CStr(qErrorMsgQueue.Peek())

                    ' Get the timestamp associated with strQueuedError, as tracked by the hashtable
                    objItem = htUniqueErrorMessages.Item(strQueuedError)
                    If objItem Is Nothing Then
                        ' The error message is not in the hashtable; this is unexpected
                    Else
                        ' Compare the queued error's timestamp with the timestamp of the new error message
                        Try
                            If System.DateTime.Parse(CStr(objItem)) >= System.DateTime.Parse(strTimestamp) Then
                                ' The queued error message's timestamp is equal to or newer than the new message's timestamp
                                ' Do not add the new item to the queue
                                blnAddItemToQueue = False
                            End If
                        Catch ex As Exception
                            ' Date comparison failed; Do not add the new item to the queue
                            blnAddItemToQueue = False
                        End Try
                    End If

                    If blnAddItemToQueue Then
                        qErrorMsgQueue.Dequeue()
                        qErrorMsgQueue.Enqueue(strErrorMessageClean)
                    End If

                End If
            End If

        End Sub

        Public Shared Function GetJobParameter(ByRef objJobParams As IJobParams, ByVal strParameterName As String, ByVal ValueIfMissing As Boolean) As Boolean

            Dim strValue As String

            Try
                strValue = objJobParams.GetParam(strParameterName)

                If strValue Is Nothing OrElse strValue.Length = 0 Then
                    Return ValueIfMissing
                End If

            Catch ex As Exception
                Return ValueIfMissing
            End Try

            ' Note: if strValue is not True or False, this will throw an exception; the calling procedure will need to handle that exception
            Return CBool(strValue)

        End Function

        Public Shared Function GetJobParameter(ByRef objJobParams As IJobParams, ByVal strParameterName As String, ByVal ValueIfMissing As String) As String

            Dim strValue As String

            Try
                strValue = objJobParams.GetParam(strParameterName)

                If strValue Is Nothing OrElse strValue.Length = 0 Then
                    Return ValueIfMissing
                End If

            Catch ex As Exception
                Return ValueIfMissing
            End Try

            Return strValue
        End Function

        Public Shared Function GetJobParameter(ByRef objJobParams As IJobParams, ByVal strParameterName As String, ByVal ValueIfMissing As Integer) As Integer
            Dim strValue As String

            Try
                strValue = objJobParams.GetParam(strParameterName)

                If strValue Is Nothing OrElse strValue.Length = 0 Then
                    Return ValueIfMissing
                End If

            Catch ex As Exception
                Return ValueIfMissing
            End Try

            ' Note: if strValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
            Return CInt(strValue)

        End Function

        Public Shared Function GetJobParameter(ByRef objJobParams As IJobParams, ByVal strParameterName As String, ByVal ValueIfMissing As Short) As Short
            Return CShort(GetJobParameter(objJobParams, strParameterName, CInt(ValueIfMissing)))
        End Function

        Public Shared Function GetRecentLogFilename(ByVal m_MgrSettings As clsAnalysisMgrSettings) As String
            Dim strAppFolderPath As String
            Dim lastFilename As String = String.Empty
            Dim DSFiles() As String = Nothing
            Dim TmpFile As String = String.Empty
            Dim x As Integer
            Dim Files() As String

            Try
                ' Obtain a list of log files
                strAppFolderPath = System.IO.Path.GetDirectoryName(System.Windows.Forms.Application.ExecutablePath)

                Files = Directory.GetFiles(strAppFolderPath, m_MgrSettings.GetParam("logfilename") & "*.txt")

                ' Change the file names to lowercase (to assure that the sorting works)
                For x = 0 To Files.Length - 1
                    Files(x) = Files(x).ToLower
                Next

                ' Sort the files by filename
                Array.Sort(Files)

                ' Return the last filename in the list
                lastFilename = Files(Files.Length - 1)

            Catch ex As Exception
                Return String.Empty
            End Try

            Return lastFilename
        End Function

#End Region

	End Class

End Namespace
