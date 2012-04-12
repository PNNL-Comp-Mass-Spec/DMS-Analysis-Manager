Option Strict On

Imports AnalysisManagerBase

Public Class clsCleanupMgrErrors

#Region "Constants"

	Protected Const SP_NAME_REPORTMGRCLEANUP As String = "ReportManagerErrorCleanup"
	Public Const FLAG_FILE_NAME As String = "flagFile.txt"
	Public Const DECON_SERVER_FLAG_FILE_NAME As String = "flagFile_Svr.txt"
	Public Const ERROR_DELETING_FILES_FILENAME As String = "Error_Deleting_Files_Please_Delete_Me.txt"

	Public Enum eCleanupModeConstants
		Disabled = 0
		CleanupOnce = 1
		CleanupAlways = 2
	End Enum

	Public Enum eCleanupActionCodeConstants
		Start = 1
		Success = 2
		Fail = 3
	End Enum
#End Region

#Region "Class wide Variables"
	Protected mInitialized As Boolean = False

	Protected mMgrConfigDBConnectionString As String = String.Empty
	Protected mManagerName As String = String.Empty

	Protected mMgrFolderPath As String = String.Empty
	Protected mWorkingDirPath As String = String.Empty

#End Region

	Public Sub New(ByVal strMgrConfigDBConnectionString As String, _
		  ByVal strManagerName As String, _
		  ByVal strMgrFolderPath As String, _
		  ByVal strWorkingDirPath As String)

		If strMgrConfigDBConnectionString Is Nothing OrElse strMgrConfigDBConnectionString.Length = 0 Then
			Throw New Exception("Manager config DB connection string is not defined")
		ElseIf strManagerName Is Nothing OrElse strManagerName.Length = 0 Then
			Throw New Exception("Manager name is not defined")
		Else
			mMgrConfigDBConnectionString = String.Copy(strMgrConfigDBConnectionString)
			mManagerName = String.Copy(strManagerName)

			mMgrFolderPath = strMgrFolderPath
			mWorkingDirPath = strWorkingDirPath

			mInitialized = True
		End If

	End Sub

	Public Function AutoCleanupManagerErrors(ByVal eManagerErrorCleanupMode As eCleanupModeConstants, ByVal DebugLevel As Integer) As Boolean
		Dim blnSuccess As Boolean
		Dim strFailureMessage As String = String.Empty

		If Not mInitialized Then Return False

		If eManagerErrorCleanupMode <> eCleanupModeConstants.Disabled Then

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Attempting to automatically clean the work directory")

			' Call SP ReportManagerErrorCleanup @ActionCode=1
			ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start)

			' Delete all folders and subfolders in work folder
			blnSuccess = CleanWorkDir(mWorkingDirPath, 1, strFailureMessage)

			If Not blnSuccess Then
				If strFailureMessage Is Nothing OrElse strFailureMessage.Length = 0 Then
					strFailureMessage = "unable to clear work directory"
				End If
			Else
				' If successful, then deletes flag files: flagfile.txt and flagFile_Svr.txt
				blnSuccess = DeleteDeconServerFlagFile(DebugLevel)

				If Not blnSuccess Then
					strFailureMessage = "error deleting " & DECON_SERVER_FLAG_FILE_NAME
				Else
					blnSuccess = DeleteStatusFlagFile(DebugLevel)
					If Not blnSuccess Then
						strFailureMessage = "error deleting " & FLAG_FILE_NAME
					End If
				End If
			End If


			' If successful, then call SP with ReportManagerErrorCleanup @ActionCode=2 
			'    otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

			If blnSuccess Then
				ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success)
			Else
				ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, strFailureMessage)
			End If

		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Deletes all files in working directory (using a 10 second holdoff after calling GC.Collect)
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function CleanWorkDir() As Boolean
		Return CleanWorkDir(mWorkingDirPath, 10, "")
	End Function

	''' <summary>
	''' Deletes all files in working directory
	''' </summary>
	''' <param name="HoldoffSeconds">Number of seconds to wait after calling GC.Collect() and GC.WaitForPendingFinalizers()</param>
	''' <param name="strFailureMessage">Error message (output)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function CleanWorkDir(ByVal HoldoffSeconds As Single, ByRef strFailureMessage As String) As Boolean
		Return CleanWorkDir(mWorkingDirPath, 10, "")
	End Function


	''' <summary>
	''' Deletes all files in working directory (using a 10 second holdoff after calling GC.Collect)
	''' </summary>
	''' <param name="WorkDir">Full path to working directory</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Shared Function CleanWorkDir(ByVal WorkDir As String) As Boolean
		Return CleanWorkDir(WorkDir, 10, "")
	End Function

	''' <summary>
	''' Deletes all files in working directory
	''' </summary>
	''' <param name="WorkDir">Full path to working directory</param>
	''' <param name="HoldoffSeconds">Number of seconds to wait after calling GC.Collect() and GC.WaitForPendingFinalizers()</param>
	''' <param name="strFailureMessage">Error message (output)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Shared Function CleanWorkDir(ByVal WorkDir As String, ByVal HoldoffSeconds As Single, ByRef strFailureMessage As String) As Boolean

		Dim diWorkFolder As System.IO.DirectoryInfo
		Dim HoldoffMilliseconds As Integer

		Dim strCurrentFile As String = String.Empty
		Dim strCurrentSubfolder As String = String.Empty

		strFailureMessage = String.Empty

		Try
			HoldoffMilliseconds = CInt(HoldoffSeconds * 1000)
			If HoldoffMilliseconds < 100 Then HoldoffMilliseconds = 100
			If HoldoffMilliseconds > 300000 Then HoldoffMilliseconds = 300000
		Catch ex As Exception
			HoldoffMilliseconds = 10000
		End Try

		'Try to ensure there are no open objects with file handles
		GC.Collect()
		GC.WaitForPendingFinalizers()
		System.Threading.Thread.Sleep(HoldoffMilliseconds)

		diWorkFolder = New System.IO.DirectoryInfo(WorkDir)

		'Delete the files
		Try
			For Each fiFile As System.IO.FileInfo In diWorkFolder.GetFiles()
				Try
					fiFile.Delete()
				Catch ex As Exception
					' Make sure the readonly bit is not set
					' The manager will try to delete the file the next time is starts
					fiFile.Attributes = fiFile.Attributes And (Not System.IO.FileAttributes.ReadOnly)
				End Try
			Next
		Catch Ex As Exception
			strFailureMessage = "Error deleting files in working directory"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ClearWorkDir(), " & strFailureMessage & " " & WorkDir, Ex)
			Console.WriteLine(strFailureMessage & ": " & Ex.Message)
			Return False
		End Try

		'Delete the sub directories
		Try
			For Each diSubDirectory As System.IO.DirectoryInfo In diWorkFolder.GetDirectories
				diSubDirectory.Delete(True)
			Next
		Catch Ex As Exception
			strFailureMessage = "Error deleting subfolder " & strCurrentSubfolder
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage & " in working directory", Ex)
			Console.WriteLine(strFailureMessage & ": " & Ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
	''' </summary>
	''' <remarks></remarks>
	Public Sub CreateErrorDeletingFilesFlagFile()

		Try
			Dim strPath As String = System.IO.Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)
			Using Sw As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strPath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))
				Sw.WriteLine(System.DateTime.Now().ToString())
				Sw.Flush()
			End Using

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating " & ERROR_DELETING_FILES_FILENAME & ": " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Creates a dummy file in the application directory to be used for controlling job request bypass
	''' </summary>
	''' <remarks></remarks>
	Public Sub CreateStatusFlagFile()

		Try
			Dim strPath As String = System.IO.Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)
			Using Sw As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strPath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))
				Sw.WriteLine(System.DateTime.Now().ToString())
				Sw.Flush()
			End Using

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating " & FLAG_FILE_NAME & ": " & ex.Message)
		End Try

	End Sub


	''' <summary>
	''' Deletes the Decon2LS OA Server flag file
	''' </summary>
	''' <returns>True if no flag file exists or if file was successfully deleted</returns>
	''' <remarks></remarks>
	Public Function DeleteDeconServerFlagFile(ByVal DebugLevel As Integer) As Boolean

		'Deletes the job request control flag file
		Dim strFlagFilePath As String = System.IO.Path.Combine(mMgrFolderPath, DECON_SERVER_FLAG_FILE_NAME)

		Return DeleteFlagFile(strFlagFilePath, DebugLevel)

	End Function


	''' <summary>
	''' Deletes the file given by strFlagFilePath
	''' </summary>
	''' <param name="strFlagFilePath">Full path to the file to delete</param>
	''' <returns>True if no flag file exists or if file was successfully deleted</returns>
	''' <remarks></remarks>
	Protected Function DeleteFlagFile(ByVal strFlagFilePath As String, ByVal intDebugLevel As Integer) As Boolean

		Try
			If System.IO.File.Exists(strFlagFilePath) Then

				Try
					' DeleteFileWithRetries will throw an exception if it cannot delete the file
					' Thus, need to wrap it with an Exception handler

					If AnalysisManagerBase.clsAnalysisToolRunnerBase.DeleteFileWithRetries(strFlagFilePath, intDebugLevel) Then
						Return True
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting file " & strFlagFilePath)
						Return False
					End If

				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteFlagFile", ex)
					Return False
				End Try

			End If

			Return True

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteFlagFile", ex)
			Return False
		End Try

	End Function

	''' <summary>
	''' Deletes the analysis manager flag file
	''' </summary>
	''' <returns>True if no flag file exists or if file was successfully deleted</returns>
	''' <remarks></remarks>
	Public Function DeleteStatusFlagFile(ByVal DebugLevel As Integer) As Boolean

		'Deletes the job request control flag file
		Dim strFlagFilePath As String = System.IO.Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)

		Return DeleteFlagFile(strFlagFilePath, DebugLevel)

	End Function


	''' <summary>
	''' Determines if error deleting files flag file exists in application directory
	''' </summary>
	''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
	''' <remarks></remarks>
	Public Function DetectErrorDeletingFilesFlagFile() As Boolean

		'Returns True if job request control flag file exists
		Dim TestFile As String = System.IO.Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)

		Return System.IO.File.Exists(TestFile)

	End Function



	''' <summary>
	''' Determines if flag file exists in application directory
	''' </summary>
	''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
	''' <remarks></remarks>
	Public Function DetectStatusFlagFile() As Boolean

		'Returns True if job request control flag file exists
		Dim TestFile As String = System.IO.Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)

		Return System.IO.File.Exists(TestFile)

	End Function




	''' <summary>
	''' Deletes the error deleting files flag file
	''' </summary>
	''' <remarks></remarks>
	Public Sub DeleteErrorDeletingFilesFlagFile()

		'Deletes the job request control flag file
		Dim TestFile As String = System.IO.Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)

		Try
			If System.IO.File.Exists(TestFile) Then
				System.IO.File.Delete(TestFile)
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteStatusFlagFile", ex)
		End Try

	End Sub

	Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants)
		ReportManagerErrorCleanup(eMgrCleanupActionCode, String.Empty)
	End Sub

	Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants, ByVal strFailureMessage As String)

		Dim MyConnection As System.Data.SqlClient.SqlConnection
		Dim MyCmd As New System.Data.SqlClient.SqlCommand
		Dim RetVal As Integer

		Try
			If strFailureMessage Is Nothing Then strFailureMessage = String.Empty

			MyConnection = New System.Data.SqlClient.SqlConnection(mMgrConfigDBConnectionString)
			MyConnection.Open()

			'Set up the command object prior to SP execution
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SP_NAME_REPORTMGRCLEANUP
				.Connection = MyConnection

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

				.Parameters.Add(New SqlClient.SqlParameter("@ManagerName", SqlDbType.VarChar, 128))
				.Parameters.Item("@ManagerName").Direction = ParameterDirection.Input
				.Parameters.Item("@ManagerName").Value = mManagerName

				.Parameters.Add(New SqlClient.SqlParameter("@State", SqlDbType.Int))
				.Parameters.Item("@State").Direction = ParameterDirection.Input
				.Parameters.Item("@State").Value = eMgrCleanupActionCode

				.Parameters.Add(New SqlClient.SqlParameter("@FailureMsg", SqlDbType.VarChar, 512))
				.Parameters.Item("@FailureMsg").Direction = ParameterDirection.Input
				.Parameters.Item("@FailureMsg").Value = strFailureMessage

				.Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
				.Parameters.Item("@message").Direction = ParameterDirection.Output
				.Parameters.Item("@message").Value = String.Empty
			End With

			'Execute the SP
			RetVal = MyCmd.ExecuteNonQuery

		Catch ex As System.Exception
			If mMgrConfigDBConnectionString Is Nothing Then mMgrConfigDBConnectionString = String.Empty
			Dim strErrorMessage As String = "Exception calling " & SP_NAME_REPORTMGRCLEANUP & " in ReportManagerErrorCleanup with connection string " & mMgrConfigDBConnectionString
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage & ex.Message)
		End Try

	End Sub

End Class
