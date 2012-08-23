'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerDataImport
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for importing data files from an external source into a job folder
	'*********************************************************************************************************

#Region "Module Variables"
	Protected mSourceFiles As System.Collections.Generic.List(Of System.IO.FileInfo)

#End Region

#Region "Methods"
	''' <summary>
	''' Runs DataImport tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim blnMoveFilesAfterImport As Boolean
		Dim blnSuccess As Boolean
		Dim result As IJobParams.CloseOutType

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		
			' Store the AnalysisManagerDataImportPlugIn version info in the database
			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining AnalysisManagerDataImportPlugIn version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If


			' Import the files
			blnSuccess = PerformDataImport()
			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unknown error calling PerformDataImport"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		   '2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			result = MakeResultsFolder()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MoveResultFiles()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				m_message = "Error moving files into results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Delete two auto-generated files from the Results Folder since they're not necessary to keep
			System.Threading.Thread.Sleep(500)
			DeleteFileFromResultFolder("DataImport_AnalysisSummary.txt")
			DeleteFileFromResultFolder("JobParameters_" & m_JobNum & ".xml")

			result = CopyResultsFolderToServer()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return result
			End If

			blnMoveFilesAfterImport = m_jobParams.GetJobParameter("MoveFilesAfterImport", True)
			If blnMoveFilesAfterImport Then
				MoveImportedFiles()
			End If


		Catch ex As Exception
			m_message = "Error in DataImportPlugin->RunTool: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try


		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Sub DeleteFileFromResultFolder(strFileName As String)
		Dim fiFileToDelete As System.IO.FileInfo

		Try
			fiFileToDelete = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, System.IO.Path.Combine(m_ResFolderName, strFileName)))
			If fiFileToDelete.Exists Then fiFileToDelete.Delete()
		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub
	''' <summary>
	''' Move the files from the source folder to a new subfolder below the source folder
	''' </summary>
	''' <returns></returns>
	''' <remarks>The name of the new subfolder comes from m_ResFolderName</remarks>
	Protected Function MoveImportedFiles() As Boolean

		Dim strTargetFolder As String = "??"
		Dim strTargetFilePath As String = "??"
		Dim fiTargetFolder As System.IO.DirectoryInfo

		Try
			If mSourceFiles Is Nothing OrElse mSourceFiles.Count = 0 Then
				' Nothing to do
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "mSourceFiles is empty; nothing for MoveImportedFiles to do")
				Return True
			End If

			strTargetFolder = System.IO.Path.Combine(mSourceFiles(0).DirectoryName, m_ResFolderName)
			fiTargetFolder = New System.IO.DirectoryInfo(strTargetFolder)
			If fiTargetFolder.Exists Then
				' Need to rename the target folder
				fiTargetFolder.MoveTo(fiTargetFolder.FullName & "_" & System.DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss"))
				fiTargetFolder = New System.IO.DirectoryInfo(strTargetFolder)
			End If

			If Not fiTargetFolder.Exists Then
				fiTargetFolder.Create()
			End If

			For Each fiFile As System.IO.FileInfo In mSourceFiles
				Try
					strTargetFilePath = System.IO.Path.Combine(strTargetFolder, fiFile.Name)
					fiFile.MoveTo(strTargetFilePath)
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error moving file " & fiFile.Name & " to " & strTargetFilePath & ": " & ex.Message)
					Return False
				End Try
			Next

		Catch ex As Exception
			m_message = "Error moving files to " & strTargetFolder & ":" & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Import files from the source share to the analysis job folder
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function PerformDataImport() As Boolean

		Const MATCH_ALL_FILES As String = "*"

		Dim strSharePath As String
		Dim strDataImportFolder As String
		Dim strSourceFileSpec As String
		Dim strSourceFolderPath As String

		Dim strMessage As String

		Dim fiSourceShare As System.IO.DirectoryInfo
		Dim fiSourceFolder As System.IO.DirectoryInfo

		Try
			mSourceFiles = New System.Collections.Generic.List(Of System.IO.FileInfo)

			strSharePath = m_jobParams.GetJobParameter("DataImportSharePath", "")

			' If the user specifies a DataImportFolder using the "Special Processing" field of an analysis job, then the folder name will be stored in section "JobParameters"
			strDataImportFolder = m_jobParams.GetJobParameter("JobParameters", "DataImportFolder", "")

			If String.IsNullOrEmpty(strDataImportFolder) Then
				' If the user specifies a DataImportFolder using the SettingsFile for an analysis job, then the folder name will be stored in section "JobParameters"
				strDataImportFolder = m_jobParams.GetJobParameter("DataImport", "DataImportFolder", "")
			End If

			If String.IsNullOrEmpty(strDataImportFolder) Then
				' strDataImportFolder is still empty, look for a parameter named DataImportFolder in any section
				strDataImportFolder = m_jobParams.GetJobParameter("DataImportFolder", "")
			End If


			strSourceFileSpec = m_jobParams.GetJobParameter("DataImportFileMask", "")
			If String.IsNullOrEmpty(strSourceFileSpec) Then strSourceFileSpec = MATCH_ALL_FILES

			If String.IsNullOrEmpty(strSharePath) Then
				m_message = "DataImportSharePath not defined in the settings file for this analysis job (" & m_jobParams.GetJobParameter("SettingsFileName", "??") & ")"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If String.IsNullOrEmpty(strDataImportFolder) Then
				strMessage = "DataImportFolder not defined in the Special_Processing parameters or the settings file for this job; will assume the input folder is the dataset name"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_message)
				strDataImportFolder = String.Copy(m_Dataset)
			End If

			fiSourceShare = New System.IO.DirectoryInfo(strSharePath)
			If Not fiSourceShare.Exists Then
				m_message = "Data Import Share not found: " & fiSourceShare.FullName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			strSourceFolderPath = System.IO.Path.Combine(fiSourceShare.FullName, strDataImportFolder)
			fiSourceFolder = New System.IO.DirectoryInfo(strSourceFolderPath)
			If Not fiSourceFolder.Exists Then
				m_message = "Data Import Folder not found: " & fiSourceFolder.FullName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Copy files from the source folder to the working directory
			mSourceFiles.Clear()
			For Each fiFile As System.IO.FileInfo In fiSourceFolder.GetFiles(strSourceFileSpec)
				Try
					fiFile.CopyTo(System.IO.Path.Combine(m_WorkDir, fiFile.Name))
					mSourceFiles.Add(fiFile)
				Catch ex As Exception
					m_message = "Error copying file " & fiFile.Name & ": " & ex.Message
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End Try
			Next

			If mSourceFiles.Count = 0 Then
				If strSourceFileSpec = MATCH_ALL_FILES Then
					m_message = "Source folder was empty; nothing to copy: " & fiSourceFolder.FullName
				Else
					m_message = "No files matching " & strSourceFileSpec & " were found in the source folder: " & fiSourceFolder.FullName
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False

			Else
				strMessage = "Copied " & mSourceFiles.Count & " file"
				If mSourceFiles.Count > 1 Then strMessage &= "s"
				strMessage &= " from " & fiSourceFolder.FullName

				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
				End If

			End If

		Catch ex As Exception
			m_message = "Error importing data files: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End Try

		Return True

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

		' Lookup the version of AnalysisManagerDataImportPlugIn
		Try
			Dim oAssemblyName As System.Reflection.AssemblyName
			oAssemblyName = System.Reflection.Assembly.Load("AnalysisManagerDataImportPlugIn").GetName

			Dim strNameAndVersion As String
			strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AnalysisManagerDataImportPlugIn: " & ex.Message)
			Return False
		End Try

		' Store the path to AnalysisManagerDataImportPlugIn.dll in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(strAppFolderPath, "AnalysisManagerDataImportPlugIn.dll")))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function


#End Region

End Class
