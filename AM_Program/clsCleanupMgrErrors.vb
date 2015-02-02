Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Security.AccessControl

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

        If String.IsNullOrEmpty(strMgrConfigDBConnectionString) Then
            Throw New Exception("Manager config DB connection string is not defined")
        ElseIf String.IsNullOrEmpty(strManagerName) Then
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
                If String.IsNullOrEmpty(strFailureMessage) Then
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
	''' Deletes all files in working directory (using a 10 second holdoff after calling GC.Collect via PRISM.Processes.clsProgRunner.GarbageCollectNow)
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function CleanWorkDir() As Boolean
		Return CleanWorkDir(mWorkingDirPath, 10, "")
	End Function

	''' <summary>
	''' Deletes all files in working directory
	''' </summary>
	''' <param name="HoldoffSeconds">Number of seconds to wait after calling PRISM.Processes.clsProgRunner.GarbageCollectNow()</param>
	''' <param name="strFailureMessage">Error message (output)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function CleanWorkDir(ByVal HoldoffSeconds As Single, ByRef strFailureMessage As String) As Boolean
		Return CleanWorkDir(mWorkingDirPath, 10, "")
	End Function


	''' <summary>
	''' Deletes all files in working directory (using a 10 second holdoff after calling GC.Collect via PRISM.Processes.clsProgRunner.GarbageCollectNow)
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
	''' <param name="HoldoffSeconds">Number of seconds to wait after calling PRISM.Processes.clsProgRunner.GarbageCollectNow()</param>
	''' <param name="strFailureMessage">Error message (output)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Shared Function CleanWorkDir(ByVal WorkDir As String, ByVal HoldoffSeconds As Single, ByRef strFailureMessage As String) As Boolean

		Dim diWorkFolder As DirectoryInfo
		Dim HoldoffMilliseconds As Integer

		If Environment.MachineName.ToLower.StartsWith("monroe") AndAlso HoldoffSeconds > 1 Then HoldoffSeconds = 1

		strFailureMessage = String.Empty

		Try
			HoldoffMilliseconds = CInt(HoldoffSeconds * 1000)
			If HoldoffMilliseconds < 100 Then HoldoffMilliseconds = 100
			If HoldoffMilliseconds > 300000 Then HoldoffMilliseconds = 300000
		Catch ex As Exception
			HoldoffMilliseconds = 10000
		End Try

		'Try to ensure there are no open objects with file handles
		PRISM.Processes.clsProgRunner.GarbageCollectNow()
		Threading.Thread.Sleep(HoldoffMilliseconds)

		' Delete all of the files and folders in the work directory
		diWorkFolder = New DirectoryInfo(WorkDir)
		If Not DeleteFilesWithRetry(diWorkFolder) Then
			Return False
		Else
			Return True
		End If

	End Function

	Protected Shared Function DeleteFilesWithRetry(ByVal diWorkFolder As DirectoryInfo) As Boolean

		Dim failedDeleteCount = 0

		'Delete the files
		Try
			For Each fiFile In diWorkFolder.GetFiles()
				Try
					fiFile.Delete()
				Catch ex As Exception
					' Make sure the readonly bit is not set
					If (fiFile.IsReadOnly) Then
						Dim attributes = fiFile.Attributes
						fiFile.Attributes = attributes And (Not FileAttributes.ReadOnly)

						Try
							' Retry the delete
							fiFile.Delete()
						Catch ex2 As Exception
							Dim strFailureMessage As String = "Error deleting file " & fiFile.FullName & ": " & ex2.Message
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
							Console.WriteLine(strFailureMessage)
							failedDeleteCount += 1
						End Try
					Else
						Dim strFailureMessage As String = "Error deleting file " & fiFile.FullName & ": " & ex.Message
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
						Console.WriteLine(strFailureMessage)
						failedDeleteCount += 1
					End If
				End Try
			Next

			'Delete the sub directories
			For Each diSubDirectory In diWorkFolder.GetDirectories
				If DeleteFilesWithRetry(diSubDirectory) Then
					' Remove the folder if it is empty
					diSubDirectory.Refresh()
					If diSubDirectory.GetFileSystemInfos().Count = 0 Then
						Try
							diSubDirectory.Delete()
						Catch ex As IOException
							' Try re-applying the permissions							

							Dim FolderAcl As New DirectorySecurity
							Dim currentUser = Environment.UserDomainName & "\" & Environment.UserName

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "IOException deleting " & diSubDirectory.FullName & "; will try granting modify access to user " & currentUser)
							FolderAcl.AddAccessRule(New FileSystemAccessRule(currentUser, FileSystemRights.Modify, InheritanceFlags.ContainerInherit Or InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow))

							Try
								' To remove existing permissions, use this: FolderAcl.SetAccessRuleProtection(True, False) 

								' Add the new access rule
								diSubDirectory.SetAccessControl(FolderAcl)

								' Make sure the readonly flag is not set (it's likely not even possible for a folder to have a readonly flag set, but it doesn't hurt to check)
								diSubDirectory.Refresh()
								Dim attributes = diSubDirectory.Attributes
								If ((attributes And FileAttributes.ReadOnly) = FileAttributes.ReadOnly) Then
									diSubDirectory.Attributes = attributes And (Not FileAttributes.ReadOnly)
								End If

								Try
									' Retry the delete
									diSubDirectory.Delete()
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updated permissions, then successfully deleted the folder")
								Catch ex3 As Exception
									Dim strFailureMessage As String = "Error deleting folder " & diSubDirectory.FullName & ": " & ex3.Message
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
									Console.WriteLine(strFailureMessage)
									failedDeleteCount += 1
								End Try

							Catch ex2 As Exception
								Dim strFailureMessage As String = "Error updating permissions for folder " & diSubDirectory.FullName & ": " & ex2.Message
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
								Console.WriteLine(strFailureMessage)
								failedDeleteCount += 1
							End Try

						Catch ex As Exception
							Dim strFailureMessage As String = "Error deleting folder " & diSubDirectory.FullName & ": " & ex.Message
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
							Console.WriteLine(strFailureMessage)
							failedDeleteCount += 1
						End Try
					End If
				Else
					Dim strFailureMessage = "Error deleting working directory subfolder " & diSubDirectory.FullName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage)
					Console.WriteLine(strFailureMessage)
					failedDeleteCount += 1
				End If
			Next

		Catch ex As Exception
			Dim strFailureMessage As String = "Error deleting files/folders in " & diWorkFolder.FullName
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strFailureMessage, ex)
			Console.WriteLine(strFailureMessage & ": " & ex.Message)
			Return False
		End Try

		If failedDeleteCount = 0 Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Creates a dummy file in the application directory when a error has occurred when trying to delete non result files
	''' </summary>
	''' <remarks></remarks>
	Public Sub CreateErrorDeletingFilesFlagFile()

		Try
			Dim strPath As String = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)
			Using Sw As StreamWriter = New StreamWriter(New FileStream(strPath, FileMode.Append, FileAccess.Write, FileShare.Read))
				Sw.WriteLine(DateTime.Now().ToString())
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
			Dim strPath As String = Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)
			Using Sw As StreamWriter = New StreamWriter(New FileStream(strPath, FileMode.Append, FileAccess.Write, FileShare.Read))
				Sw.WriteLine(DateTime.Now().ToString())
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
		Dim strFlagFilePath As String = Path.Combine(mMgrFolderPath, DECON_SERVER_FLAG_FILE_NAME)

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
			If File.Exists(strFlagFilePath) Then

				Try
					' DeleteFileWithRetries will throw an exception if it cannot delete the file
					' Thus, need to wrap it with an Exception handler

					If clsAnalysisToolRunnerBase.DeleteFileWithRetries(strFlagFilePath, intDebugLevel) Then
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
		Dim strFlagFilePath As String = Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)

		Return DeleteFlagFile(strFlagFilePath, DebugLevel)

	End Function


	''' <summary>
	''' Determines if error deleting files flag file exists in application directory
	''' </summary>
	''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
	''' <remarks></remarks>
	Public Function DetectErrorDeletingFilesFlagFile() As Boolean

		'Returns True if job request control flag file exists
		Dim TestFile As String = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)

		Return File.Exists(TestFile)

	End Function



	''' <summary>
	''' Determines if flag file exists in application directory
	''' </summary>
	''' <returns>TRUE if flag file exists; FALSE otherwise</returns>
	''' <remarks></remarks>
	Public Function DetectStatusFlagFile() As Boolean

		'Returns True if job request control flag file exists
		Dim TestFile As String = Path.Combine(mMgrFolderPath, FLAG_FILE_NAME)

		Return File.Exists(TestFile)

	End Function




	''' <summary>
	''' Deletes the error deleting files flag file
	''' </summary>
	''' <remarks></remarks>
	Public Sub DeleteErrorDeletingFilesFlagFile()

		'Deletes the job request control flag file
		Dim TestFile As String = Path.Combine(mMgrFolderPath, ERROR_DELETING_FILES_FILENAME)

		Try
			If File.Exists(TestFile) Then
				File.Delete(TestFile)
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeleteStatusFlagFile", ex)
		End Try

	End Sub

	Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants)
		ReportManagerErrorCleanup(eMgrCleanupActionCode, String.Empty)
	End Sub

	Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants, ByVal strFailureMessage As String)

		Dim MyConnection As SqlClient.SqlConnection
		Dim MyCmd As New SqlClient.SqlCommand

		Try
			If strFailureMessage Is Nothing Then strFailureMessage = String.Empty

			MyConnection = New SqlClient.SqlConnection(mMgrConfigDBConnectionString)
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
			MyCmd.ExecuteNonQuery()

		Catch ex As Exception
			If mMgrConfigDBConnectionString Is Nothing Then mMgrConfigDBConnectionString = String.Empty
			Dim strErrorMessage As String = "Exception calling " & SP_NAME_REPORTMGRCLEANUP & " in ReportManagerErrorCleanup with connection string " & mMgrConfigDBConnectionString
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage & ex.Message)
		End Try

	End Sub

End Class
