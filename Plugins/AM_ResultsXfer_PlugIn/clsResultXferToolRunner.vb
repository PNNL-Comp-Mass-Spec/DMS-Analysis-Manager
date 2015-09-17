'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/30/2008
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Threading

''' <summary>
''' Derived class for performing analysis results transfer
''' </summary>
''' <remarks></remarks>
Public Class clsResultXferToolRunner
	Inherits clsAnalysisToolRunnerBase

#Region "Methods"
	''' <summary>
	''' Runs the results transfer tool
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim Result As IJobParams.CloseOutType

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the AnalysisManager version info in the database
			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining AnalysisManager version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Transfer the results
			Result = PerformResultsXfer()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error calling PerformResultsXfer"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			DeleteTransferFolderIfEmpty()

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

		Catch ex As Exception
			m_message = "Error in ResultsXferPlugin->RunTool: " & ex.Message
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'If we got to here, everything worked, so exit
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function ChangeFolderPathsToLocal(ByVal serverName As String, ByRef transferFolderPath As String, ByRef datasetStoragePath As String) As Boolean

		Dim connectionString = m_mgrParams.GetParam("connectionstring")

		Dim datasetStorageVolServer = LookupLocalPath(serverName, datasetStoragePath, "raw-storage", connectionString)
		If String.IsNullOrWhiteSpace(datasetStorageVolServer) Then
			m_message = "Unable to determine the local drive letter for " + Path.Combine("\\" & serverName, datasetStoragePath)
			Return False
		Else
			datasetStoragePath = datasetStorageVolServer
		End If

		Dim transferVolServer = LookupLocalPath(serverName, transferFolderPath, "results_transfer", connectionString)
		If String.IsNullOrWhiteSpace(transferVolServer) Then
			m_message = "Unable to determine the local drive letter for " + Path.Combine("\\" & serverName, transferFolderPath)
			Return False
		Else
			transferFolderPath = transferVolServer
		End If

		Return True
	End Function

	Protected Sub DeleteTransferFolderIfEmpty()

		Dim Msg As String = ""

		' If there are no more folders or files in the dataset folder in the xfer directory, then delete the folder
		' Note that another manager might be simultaneously examining this folder to see if it's empty
		' If that manager deletes this folder first, then an exception could occur in this manager
		' Thus, we will log any exceptions that occur, but we won't treat them as a job failure

		Try
			Dim diTransferFolder = New DirectoryInfo(Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_Dataset))

			If diTransferFolder.Exists AndAlso diTransferFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Count = 0 Then
				' Dataset folder in transfer folder is empty; delete it
				Try
					If m_DebugLevel >= 3 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting empty dataset folder in transfer directory: " & diTransferFolder.FullName)
					End If

					diTransferFolder.Delete()
				Catch ex As Exception
					' Log this exception, but don't treat it is a job failure
					Msg = "clsResultXferToolRunner.RunTool(); Exception deleting dataset folder " & _
					  m_jobParams.GetParam("DatasetFolderName") & " in xfer folder(another results manager may have deleted it): " & ex.Message
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
				End Try
			Else
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset folder in transfer directory still has files/folders; will not delete: " & diTransferFolder.FullName)
				End If
			End If

		Catch ex As Exception
			' Log this exception, but don't treat it is a job failure
			Msg = "clsResultXferToolRunner.RunTool(); Exception looking for dataset folder " & _
			  m_jobParams.GetParam("DatasetFolderName") & " in xfer folder (another results manager may have deleted it): " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
		End Try

	End Sub

	Private Function GetMachineNameFromPath(ByVal uncFolderPath As String) As String
		Dim charIndex = uncFolderPath.IndexOf("\"c, 2)

		If charIndex < 0 OrElse Not uncFolderPath.StartsWith("\\") Then
			Return String.Empty
		End If
		
		Dim machineName = uncFolderPath.Substring(2, charIndex - 2)
		Return machineName

	End Function

    Protected Function LookupLocalPath(
       ByVal serverName As String,
       ByVal uncFolderPath As String,
       ByVal folderFunction As String,
       ByVal connectionString As String) As String

        Const retryCount As Short = 3
        Dim strMsg As String

        If Not uncFolderPath.StartsWith("\\") Then
            ' Not a network path; cannot convert
            Return String.Empty
        End If

        ' Remove the server name from the start of folderPath
        ' For example, change 
        '   from: \\proto-6\LTQ_Orb_3\2013_2
        '   to:   LTQ_Orb_3\2013_2

        ' First starting from index 2 in the string, find the next slash
        Dim charIndex = uncFolderPath.IndexOf("\"c, 2)

        If charIndex < 0 Then
            ' Match not found
            Return String.Empty
        End If

        uncFolderPath = uncFolderPath.Substring(charIndex + 1)

        ' Make sure folderPath does not end in a slash
        If uncFolderPath.EndsWith("\"c) Then
            uncFolderPath = uncFolderPath.TrimEnd("\"c)
        End If

        Dim sbSql = New Text.StringBuilder

        ' Query V_Storage_Path_Export for the local volume name of the given path
        '
        sbSql.Append(" SELECT TOP 1 VolServer, [Path]")
        sbSql.Append(" FROM V_Storage_Path_Export")
        sbSql.Append(" WHERE (MachineName = '" + serverName + "') AND")
        sbSql.Append("       ([Path] = '" + uncFolderPath + "' OR")
        sbSql.Append("        [Path] = '" + uncFolderPath + "\')")
        sbSql.Append(" ORDER BY CASE WHEN [Function] = '" + folderFunction + "' THEN 1 ELSE 2 END, ID DESC")

        ' Get a table to hold the results of the query
        Dim dt As DataTable = Nothing
        Dim blnSuccess = clsGlobal.GetDataTableByQuery(sbSql.ToString(), connectionString, "LookupLocalPath", retryCount, dt)

        If Not blnSuccess Then
            strMsg = "LookupLocalPath; Excessive failures attempting to retrieve folder info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
            Return String.Empty
        End If

        For Each curRow As DataRow In dt.Rows
            Dim volServer = clsGlobal.DbCStr(curRow("VolServer"))
            Dim localFolderPath = Path.Combine(volServer, uncFolderPath)
            Return localFolderPath
        Next

        ' No data was returned
        strMsg = "LookupLocalPath; could not resolve a local volume name for path '" + uncFolderPath + "' on server " + serverName
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
        Return String.Empty

    End Function
	
	''' <summary>
	''' Moves files from one local directory to another local directory
	''' </summary>
	''' <param name="sourceFolderpath"></param>
	''' <param name="targetFolderPath"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function MoveFilesLocally(ByVal sourceFolderpath As String, ByVal targetFolderPath As String, ByVal overwriteExisting As Boolean) As Boolean

		Dim success = True
		Dim errorCount = 0
		Dim errorMessage = String.Empty

		Try
			If sourceFolderpath.StartsWith("\\") Then
				m_message = "MoveFilesLocally cannot be used with files on network shares; " & sourceFolderpath
				Return False
			End If

			If targetFolderPath.StartsWith("\\") Then
				m_message = "MoveFilesLocally cannot be used with files on network shares; " & targetFolderPath
				Return False
			End If

			Dim diSourceFolder = New DirectoryInfo(sourceFolderpath)
			Dim diTargetFolder = New DirectoryInfo(targetFolderPath)

			If Not diTargetFolder.Exists Then diTargetFolder.Create()

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Moving files locally to " & diTargetFolder.FullName)
			End If

			For Each fiSourceFile In diSourceFolder.GetFiles()
				Try
					Dim fiTargetFile = New FileInfo(Path.Combine(diTargetFolder.FullName, fiSourceFile.Name))

					If fiTargetFile.Exists Then
						If Not overwriteExisting Then
							If m_DebugLevel >= 2 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Skipping existing file: " & fiTargetFile.FullName)
							End If
							Continue For
						End If
						fiTargetFile.Delete()
					End If

					fiSourceFile.MoveTo(fiTargetFile.FullName)

				Catch ex As Exception
					errorCount += 1
					If errorCount = 1 Then
						errorMessage = "Error moving file " & fiSourceFile.Name & ": " & ex.Message
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error moving file " & fiSourceFile.Name & ": " & ex.Message)
					success = False
				End Try

			Next

			If errorCount > 0 Then
				m_message = clsGlobal.AppendToComment(m_message, errorMessage)
			End If

			' Recursively call this function for each subdirectory
			For Each diSubFolder In diSourceFolder.GetDirectories()
				Dim subDirSuccess = MoveFilesLocally(diSubFolder.FullName, Path.Combine(diTargetFolder.FullName, diSubFolder.Name), overwriteExisting)
				If Not subDirSuccess Then
					success = False
				End If
			Next

			' Delete this folder if it is empty
			diSourceFolder.Refresh()
			If diSourceFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Count = 0 Then
				diSourceFolder.Delete()
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error moving directory " & sourceFolderpath & ": " & ex.Message)
			success = False
		End Try

		Return success

	End Function

	''' <summary>
	''' Performs the results transfer
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure></returns>
	''' <remarks></remarks>
	Protected Overridable Function PerformResultsXfer() As IJobParams.CloseOutType

		Dim Msg As String
		Dim FolderToMove As String
		Dim DatasetDir As String
		Dim TargetDir As String
		Dim diDatasetFolder As DirectoryInfo

		' Set this to True to overwrite existing results folders
		Const blnOverwriteExisting As Boolean = True

		Dim transferFolderPath = m_jobParams.GetParam("transferFolderPath")
		Dim datasetStoragePath = m_jobParams.GetParam("DatasetStoragePath")

		' Check whether the transfer folder and the dataset folder reside on the same server as this manager
		Dim serverName = Environment.MachineName
		Dim movingLocalFiles As Boolean = False

		If String.Compare(GetMachineNameFromPath(transferFolderPath), serverName, True) = 0 AndAlso
		   String.Compare(GetMachineNameFromPath(datasetStoragePath), serverName, True) = 0 Then
			' Update the paths to use local file paths instead of network share paths

			If Not ChangeFolderPathsToLocal(serverName, transferFolderPath, datasetStoragePath) Then
				If String.IsNullOrWhiteSpace(m_message) Then m_message = "Unknown error calling ChangeFolderPathsToLocal"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			movingLocalFiles = True
		End If

		' Verify input folder exists in storage server xfer folder
		FolderToMove = Path.Combine(transferFolderPath, m_jobParams.GetParam("DatasetFolderName"))
		FolderToMove = Path.Combine(FolderToMove, m_jobParams.GetParam("InputFolderName"))

		If Not Directory.Exists(FolderToMove) Then
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); results folder " & FolderToMove & " not found"
			m_message = clsGlobal.AppendToComment(m_message, "results folder not found")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		ElseIf m_DebugLevel >= 4 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder to move: " & FolderToMove)
		End If

		' Verify dataset folder exists on storage server
		' If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
		DatasetDir = Path.Combine(datasetStoragePath, m_jobParams.GetParam("DatasetFolderName"))
		diDatasetFolder = New DirectoryInfo(DatasetDir)
		If Not diDatasetFolder.Exists Then
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); dataset folder " & DatasetDir & " not found; will attempt to make it"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)

			Try
				Dim diParentFolder As DirectoryInfo
				diParentFolder = diDatasetFolder.Parent

				If Not diParentFolder.Exists Then
					' Parent folder doesn't exist; try to go up one more level and create the parent

					If Not diParentFolder.Parent Is Nothing Then
						' Parent of the parent exist; try to create the parent folder
						diParentFolder.Create()

						' Wait 500 msec then verify that the folder was created
						Thread.Sleep(500)
						diParentFolder.Refresh()
						diDatasetFolder.Refresh()
					End If
				End If

				If diParentFolder.Exists Then
					' Parent folder exists; try to create the dataset folder
					diDatasetFolder.Create()

					' Wait 500 msec then verify that the folder now exists
					Thread.Sleep(500)
					diDatasetFolder.Refresh()

					If Not diDatasetFolder.Exists Then
						' Creation of the dataset folder failed; unable to continue
						Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": folder creation failed for unknown reason"
						m_message = clsGlobal.AppendToComment(m_message, "error trying to create missing dataset folder")
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Else
					Msg = "clsResultXferToolRunner.PerformResultsXfer(); parent folder not found: " & diDatasetFolder.Parent.FullName & "; unable to continue"
					m_message = clsGlobal.AppendToComment(m_message, "parent folder not found: " & diDatasetFolder.Parent.FullName)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			Catch ex As Exception
				Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": " & ex.Message
				m_message = clsGlobal.AppendToComment(m_message, "exception trying to create missing dataset folder")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try


		ElseIf m_DebugLevel >= 4 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset folder path: " & DatasetDir)
		End If

		TargetDir = Path.Combine(DatasetDir, m_jobParams.GetParam("inputfoldername"))


		' Determine if output folder already exists on storage server
		If Directory.Exists(TargetDir) Then
			If blnOverwriteExisting Then
				Msg = "Warning: overwriting existing results folder: " & TargetDir
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
				Msg = String.Empty
			Else
				Msg = "clsResultXferToolRunner.PerformResultsXfer(); destination directory " & DatasetDir & " already exists"
				m_message = clsGlobal.AppendToComment(m_message, "results folder already exists at destination and overwrite is disabled")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		' Move the directory
		Try
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Moving '" & FolderToMove & "' to '" & TargetDir & "'")
			End If

			If movingLocalFiles Then
				Dim success = MoveFilesLocally(FolderToMove, TargetDir, blnOverwriteExisting)
				If Not success Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				' Call MoveDirectory, which will copy the files using locks
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Using m_FileTools.MoveDirectory to copy files to " & TargetDir)
				End If
				ResetTimestampForQueueWaitTimeLogging()
				m_FileTools.MoveDirectory(FolderToMove, TargetDir, blnOverwriteExisting, m_mgrParams.GetParam("MgrName", "Undefined-Manager"))
			End If
			
		Catch ex As Exception
			Msg = "clsResultXferToolRunner.PerformResultsXfer(); Exception moving results folder " & FolderToMove & ": " & ex.Message
			m_message = clsGlobal.AppendToComment(m_message, "exception moving results folder")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim strAppFolderPath As String = clsGlobal.GetAppFolderPath()

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the Analysis Manager
		If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "AnalysisManagerProg") Then
			Return False
		End If

		' Lookup the version of AnalysisManagerResultsXferPlugin
		If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "AnalysisManagerResultsXferPlugin") Then
			Return False
		End If

		' Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsXferPlugin.dll in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerProg.exe")))
		ioToolFiles.Add(New FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerResultsXferPlugin.dll")))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

#End Region

End Class
