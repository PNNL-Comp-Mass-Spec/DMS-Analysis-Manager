Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports ParamFileGenerator.MakeParams

Public Class clsAnalysisResources
	Implements IAnalysisResources

#Region "Constants"
	Protected Const COPY_ORG_DB_TRUE As Integer = 1
	Protected Const COPY_ORG_DB_FALSE As Integer = 0
#End Region

#Region "Module variables"
	' access to the job parameters
	Protected m_jobParams As IJobParams

	' access to the logger
	Protected m_logger As ILogger

	' access to mgr parameters
	Protected m_mgrParams As IMgrParams

	' properties to get files
	Protected m_WorkingDir As String

	'Convenient place for frequently used job number
	Protected m_JobNum As String

	'Convenient place for frequently used machine name
	Protected m_MachName As String

	' for posting a general explanation for external consumption
	Protected m_message As String

	'Debug output control
	Protected m_DebugLevel As Short
#End Region

	Public Sub New()

	End Sub

	Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger) Implements IAnalysisResources.Setup
		m_mgrParams = mgrParams
		m_logger = logger
		m_jobParams = jobParams
		m_JobNum = m_jobParams.GetParam("jobNum")
		m_MachName = m_mgrParams.GetParam("programcontrol", "machname")
		m_DebugLevel = CShort(m_mgrParams.GetParam("programcontrol", "debuglevel"))
	End Sub

	' explanation of what happened to last operation this class performed
	Public Overridable ReadOnly Property Message() As String Implements IAnalysisResources.Message
		Get
			Return m_message
		End Get
	End Property

	Public Overridable Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

		Dim ParamFile As String = m_jobParams.GetParam("parmFileName")
		Dim SettingsFile As String = m_jobParams.GetParam("settingsFileName")
		Dim OrgDB As String = m_jobParams.GetParam("organismDBName")
		Dim msgstr As String
		'Dim TmpDirArray() As String
		'Dim TmpFilArray() As String

		m_WorkingDir = m_mgrParams.GetParam("commonfileandfolderlocations", "WorkDir")

		'Make log entry about starting resource retrieval
		m_logger.PostEntry(m_MachName & ": Retrieving files, job " & m_JobNum, _
		ILogger.logMsgType.logNormal, LOG_DATABASE)

		'Copy OrgDB file, if specified
		'
		'IMPORTANT: The OrgDB MUST be processed before the param file because
		'	for some analysis tools OrgDB processing generates data that feeds into param file processing
		If CInt(m_jobParams.GetParam("OrgDbReqd")) = COPY_ORG_DB_TRUE Then
			If Not RetrieveOrgDB(m_mgrParams.GetParam("commonfileandfolderlocations", "orgdbdir")) Then
				msgstr = "Error copying OrgDB file, job " & m_JobNum
				m_logger.PostEntry(msgstr, ILogger.logMsgType.logError, LOG_DATABASE)
				'				m_message = AppendToComment(m_jobParams.GetParam("comment"), "Error copying OrgDB file")
				m_message = "Error copying OrgDB file"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		'Copy param file, if one is specified
		If ParamFile.ToLower <> "na" Then
			If Not RetrieveParamFile(ParamFile, m_jobParams.GetParam("parmFileStoragePath"), m_WorkingDir) Then
				msgstr = "Error copying param file, job " & m_JobNum
				m_logger.PostEntry(msgstr, ILogger.logMsgType.logError, LOG_DATABASE)
				'				m_message = AppendToComment(m_jobParams.GetParam("comment"), "Error copying param file")
				m_message = "Error copying param file"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		'Copy settings file, if specified
		If SettingsFile.ToLower <> "na" Then
			If Not CopyFileToWorkDir(SettingsFile, m_jobParams.GetParam("settingsFileStoragePath"), m_WorkingDir) Then
				msgstr = "Error copying settings file, job " & m_JobNum
				m_logger.PostEntry(msgstr, ILogger.logMsgType.logError, LOG_DATABASE)
				'				m_message = AppendToComment(m_jobParams.GetParam("comment"), "Error copying settings file")
				m_message = "Error copying settings file"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		'Copy spectra file(s)
		If Not RetrieveSpectra(m_jobParams.GetParam("RawDataType"), m_WorkingDir) Then
			msgstr = "Error copying spectra, job " & m_JobNum
			m_logger.PostEntry(msgstr, ILogger.logMsgType.logError, LOG_DATABASE)
			'			m_message = AppendToComment(m_jobParams.GetParam("comment"), "Error copying spectra")
			m_message = "Error copying spectra"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'You got to here, everything must be cool!
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function CopyFileToWorkDir(ByVal InpFile As String, ByVal InpFolder As String, _
	ByVal OutDir As String) As Boolean

		'Copies specified file from storage server to local working directory
		Dim SourceFile As String

		SourceFile = Path.Combine(InpFolder, InpFile)

		'Verify source file exists
		If Not File.Exists(SourceFile) Then
			Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File not found: " & SourceFile
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return False
		End If

		Try
			File.Copy(SourceFile, Path.Combine(OutDir, InpFile), True)
			If m_DebugLevel > 3 Then
				Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File copied: " & SourceFile
				m_logger.PostEntry(msg, ILogger.logMsgType.logDebug, True)
			End If
		Catch CopyError As Exception
			m_logger.PostError("Error copying file " & SourceFile, CopyError, LOG_DATABASE)
			Return False
		End Try

		'If we got to here, then everything worked
		Return True

	End Function

	Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, ByVal InpFolder As String, _
	ByVal OutDir As String) As Boolean

		'Copies specified file from storage server to local working directory, renames destination with dataset name

		Dim SourceFile As String

		SourceFile = Path.Combine(InpFolder, InpFile)

		'Verify source file exists
		If Not File.Exists(SourceFile) Then
			Return False
		End If

		Dim Fi As New FileInfo(SourceFile)
		Dim TargetName As String = m_jobParams.GetParam("datasetNum") & Fi.Extension

		Try
			File.Copy(SourceFile, Path.Combine(OutDir, TargetName), True)
		Catch CopyError As Exception
			m_logger.PostError("Error copying file " & SourceFile, CopyError, LOG_DATABASE)
			Return False
		End Try

		'If we got to here, then everything worked
		Return True

	End Function

	Protected Overridable Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean

		'Determines if specified org db file already exists in OrgDBPath. If so, it compares with
		'	master version and copies master if different
		'NOTE: This function only uses file file size/date to verify file validity. For Sequest and XTandem processing
		'	override this method to use the hash functions provided by Ken's dll

		Dim FiSource As FileInfo
		Dim FiDest As FileInfo
		Dim SourceFile As String
		Dim DestFile As String
		'		Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("commonfileandfolderlocations", "orgdbdir")
		Dim LegacyOrgDbPath As String = m_jobParams.GetParam("organismDBStoragePath")
		Dim LegacyOrdDbName As String = m_jobParams.GetParam("organismDBName")

		'Verify source exists
		SourceFile = Path.Combine(LegacyOrgDbPath, LegacyOrdDbName)
		If Not Directory.Exists(SourceFile) Then
			Return False
		End If

		DestFile = Path.Combine(LocalOrgDBFolder, LegacyOrdDbName)

		If File.Exists(DestFile) Then		'Destination file already exists, verify size and date
			FiSource = New FileInfo(SourceFile)
			FiDest = New FileInfo(DestFile)
			If (FiSource.Length <> FiDest.Length) Or (FiSource.LastWriteTime <> FiDest.LastWriteTime) Then
				'A copy of the specified file exists, but it doesn't match size and/or date stamp
				If CopyFileToWorkDir(LegacyOrdDbName, LegacyOrgDbPath, LocalOrgDBFolder) Then
					Return True
				Else
					'There was a problem
					Return False
				End If
			Else
				'File exists and is current
				Return True
			End If
		Else		 'Destination file didn't exist, so just copy over from source
			If CopyFileToWorkDir(LegacyOrdDbName, LegacyOrgDbPath, LocalOrgDBFolder) Then
				Return True
			Else
				Return False
			End If
		End If

	End Function

	Protected Overridable Function RetrieveParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
	ByVal WorkDir As String) As Boolean

		'Provides a parameter file copy function that can be overridden by the Sequest analysis classes

		If CopyFileToWorkDir(ParamFileName, ParamFilePath, WorkDir) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overridable Function RetrieveSettingsFile(ByVal SettingFileName As String, ByVal SettingFilePath As String, _
	ByVal WorkDir As String) As Boolean

		'Provides a settings file copy function that can be overridden by subclasses

		If CopyFileToWorkDir(SettingFileName, SettingFilePath, WorkDir) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overridable Function RetrieveSpectra(ByVal RawDataType As String, ByVal WorkDir As String) As Boolean

		'Retrieves the spectra file(s) based on raw data type and puts them in the working directory
		Dim OpResult As Boolean = False

		m_logger.PostEntry("Retrieving spectra file(s)", ILogger.logMsgType.logNormal, True)

		Select Case RawDataType.ToLower
			Case "dot_d_folders"			'Agilent ion trap data
				If RetrieveMgfFile(WorkDir, True) Then OpResult = True

			Case "dot_wiff_files"			'Agilent/QSTAR TOF data
				If RetrieveDotWiffFile(WorkDir) Then OpResult = True

			Case "zipped_s_folders"			'FTICR data
				If RetrieveSFolders(WorkDir) Then OpResult = True

			Case "dot_raw_files"			'Finnigan ion trap/LTQ-FT data
				If RetrieveDotRawFile(WorkDir) Then OpResult = True

			Case "dot_raw_folder"			'Micromass QTOF data
				If RetrieveDotRawFolder(WorkDir) Then OpResult = True

			Case Else			'Something bad has happened if we ever get to here
				m_logger.PostEntry("Invalid data type specified: " & RawDataType, ILogger.logMsgType.logError, True)
		End Select

		'Return the result of the spectra retrieval
		Return OpResult

	End Function

	Protected Overridable Function RetrieveDotRawFile(ByVal WorkDir As String) As Boolean

		'Retrieves a .raw file for the analysis job in progress
		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ServerPath As String = Path.Combine(m_jobParams.GetParam("datasetFolderStoragePath"), DSName)

		If CopyFileToWorkDir(DSName & ".raw", ServerPath, WorkDir) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overridable Function RetrieveDotWiffFile(ByVal WorkDir As String) As Boolean

		'Retrieves a .wiff file for the analysis job in progress
		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ServerPath As String = Path.Combine(m_jobParams.GetParam("datasetFolderStoragePath"), DSName)

		If CopyFileToWorkDir(DSName & ".wiff", ServerPath, WorkDir) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overridable Function RetrieveMgfFile(ByVal WorkDir As String, ByVal GetCdfAlso As Boolean) As Boolean

		'Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress.
		'Data files are in a subfolder off of the main dataset folder
		'Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ServerPath As String = m_jobParams.GetParam("datasetFolderStoragePath")
		Dim DSFolders() As String
		Dim DSFiles() As String
		Dim DSFileInfo As FileInfo
		Dim DSFolderPath As String
		Dim DumFolder As String
		Dim DumFile As String
		Dim FileFound As Boolean = False
		Dim DataFolderPath As String

		'Set up the file paths
		DSFolderPath = Path.Combine(ServerPath, DSName)

		'Get a list of the subfolders in the dataset folder
		DSFolders = Directory.GetDirectories(DSFolderPath)
		'Go through the folders looking for a file with a ".mgf" extension
		For Each DumFolder In DSFolders
			If FileFound Then Exit For
			DSFiles = Directory.GetFiles(DumFolder, "*.mgf")
			If DSFiles.GetLength(0) = 1 Then
				'Correct folder has been found
				DataFolderPath = DumFolder
				FileFound = True
				Exit For
			End If
		Next DumFolder

		'Exit if no data file was found
		If Not FileFound Then Return False

		'Do the copy
		If Not CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir) Then Return False

		'If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
		If Not GetCdfAlso Then Return True

		DSFiles = Directory.GetFiles(DataFolderPath, "*.cdf")
		If DSFiles.GetLength(0) <> 1 Then
			'Incorrect number of .cdf files found
			Return False
		End If

		'Copy the .cdf file that was found
		If CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overridable Function RetrieveDotRawFolder(ByVal WorkDir As String) As Boolean

		'Copies a .raw datafolder from the Micromass TOF datafile to the working directory
		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ServerPath As String = m_jobParams.GetParam("datasetFolderStoragePath")

		'Set up the file paths
		Dim DSFolderPath As String = Path.Combine(ServerPath, DSName)

		'Find the .raw folder in the dataset folder
		Dim RemFolders() As String = Directory.GetDirectories(DSFolderPath, "*.raw")
		If RemFolders.GetLength(0) <> 1 Then Return False

		'Do the copy
		Try
			CopyDirectory(Path.Combine(DSFolderPath, RemFolders(0)), Path.Combine(WorkDir, DSName & ".raw"))
			Return True
		Catch ex As Exception
			Dim MsgStr As String = "Error copying folder " & DSFolderPath & " to working directory: " & ex.Message
			m_logger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
			Return False
		End Try

	End Function

	Private Function RetrieveSFolders(ByVal WorkDir As String) As Boolean

		'=====================================================================================================
		'Original version
		'=====================================================================================================
		''Unzips dataset folders to working directory
		'Dim DSName As String = m_jobParams.GetParam("datasetNum")
		'Dim ServerPath As String = m_jobParams.GetParam("datasetFolderStoragePath")
		'Dim ZipFiles() As String
		'Dim DSFolderPath As String
		'Dim UnZipper As ZipTools
		'Dim TargetFolder As String
		'Dim ZipFile As String

		'DSFolderPath = Path.Combine(ServerPath, DSName)

		''Verify dataset folder exists
		'If Not Directory.Exists(DSFolderPath) Then Return False

		''Get a listing of the zip files to process
		'ZipFiles = Directory.GetFiles(CheckTerminator(DSFolderPath), "s*.zip")
		'If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

		''Set up the unzipper
		'UnZipper = New PRISM.Files.ZipTools(WorkDir, m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"))

		''Create a dataset subdirectory under the working directory
		'Directory.CreateDirectory(Path.Combine(WorkDir, DSName))

		''Unzip each of the zip files to the working directory
		'For Each ZipFile In ZipFiles
		'	TargetFolder = Path.Combine(WorkDir, Path.Combine(DSName, _
		'	 Path.GetFileNameWithoutExtension(ZipFile)))
		'	Directory.CreateDirectory(TargetFolder)
		'	If Not UnZipper.UnzipFile("-dir=relative", ZipFile, TargetFolder) Then
		'		UnZipper = Nothing
		'		Return False
		'	End If
		'Next

		'UnZipper = Nothing
		'Return True

		'=====================================================================================================
		'Modified for local unzip and #ZipLib use
		'=====================================================================================================
		'Unzips dataset folders to working directory
		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ZipFiles() As String
		Dim DSWorkFolder As String
		Dim UnZipper As clsSharpZipWrapper
		Dim TargetFolder As String
		Dim ZipFile As String

		'Copy the zipped s-folders from archive to work directory
		If Not CopySFoldersToWorkDir(WorkDir) Then
			'Error messages have already been logged, so just exit
			Return False
		End If

		'Get a listing of the zip files to process
		ZipFiles = Directory.GetFiles(WorkDir, "s*.zip")
		If ZipFiles.GetLength(0) < 1 Then
			m_logger.PostEntry("No zipped s-folders found in working directory", ILogger.logMsgType.logError, True)
			Return False			'No zipped data files found
		End If

		''Set up the unzipper
		UnZipper = New clsSharpZipWrapper

		'Create a dataset subdirectory under the working directory
		DSWorkFolder = Path.Combine(WorkDir, DSName)
		Directory.CreateDirectory(DSWorkFolder)

		'Unzip each of the zip files to the working directory
		For Each ZipFile In ZipFiles
			If m_DebugLevel > 3 Then
				m_logger.PostEntry("Unzipping file " & ZipFile, ILogger.logMsgType.logDebug, True)
			End If
			Try
				TargetFolder = Path.Combine(DSWorkFolder, Path.GetFileNameWithoutExtension(ZipFile))
				Directory.CreateDirectory(TargetFolder)
				If Not UnZipper.ExtractAllFilesToOneFolder(ZipFile, TargetFolder) Then
					m_logger.PostEntry(UnZipper.ErrMsg, ILogger.logMsgType.logError, True)
					Return False
				End If
			Catch ex As Exception
				m_logger.PostEntry("Exception while unzipping s-folders: " & ex.Message, ILogger.logMsgType.logError, True)
				Return False
			End Try
		Next

		'Delete all s*.zip files in working directory
		For Each ZipFile In ZipFiles
			Try
				File.Delete(ZipFile)
			Catch ex As Exception
				m_logger.PostEntry("Exception deleting file " & ZipFile & " : " & ex.Message, ILogger.logMsgType.logError, True)
				Return False
			End Try
		Next

		'Got to here, so everything must have worked
		Return True

	End Function

	Protected Overridable Function SetBioworksVersion(ByVal InpVersion As String) As IGenerateFile.ParamFileType

		'Converts the setup file entry for the Bioworks version to a parameter type compatible with the
		'	parameter file generator dll
		Select Case InpVersion
			Case "20"
				Return IGenerateFile.ParamFileType.BioWorks_20
			Case "30"
				Return IGenerateFile.ParamFileType.BioWorks_30
			Case "31"
				Return IGenerateFile.ParamFileType.BioWorks_31
			Case "xtandem"
				Return IGenerateFile.ParamFileType.X_Tandem
			Case Else
				'If we get to here, there's a problem
				Return Nothing
		End Select

	End Function

	Private Function CopySFoldersToWorkDir(ByVal WorkDir As String) As Boolean

		'Copies the zipped s-folders to the working directory
		Dim DSName As String = m_jobParams.GetParam("datasetNum")
		Dim ServerPath As String = m_jobParams.GetParam("datasetFolderStoragePath")
		Dim ZipFiles() As String
		Dim DSFolderPath As String
		Dim ZippedFileName As String

		DSFolderPath = Path.Combine(ServerPath, DSName)

		'Verify dataset folder exists
		If Not Directory.Exists(DSFolderPath) Then Return False

		'Get a listing of the zip files to process
		ZipFiles = Directory.GetFiles(DSFolderPath, "s*.zip")
		If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

		'copy each of the s*.zip files to the working directory
		For Each ZipFile As String In ZipFiles
			ZippedFileName = Path.GetFileName(ZipFile)
			Try
				If m_DebugLevel > 3 Then
					m_logger.PostEntry("Copying file " & ZipFile & " to work directory", ILogger.logMsgType.logDebug, True)
				End If
				File.Copy(ZipFile, Path.Combine(WorkDir, ZippedFileName), False)
			Catch ex As Exception
				m_logger.PostEntry("Exception copying file " & ZipFile & " : " & ex.Message, ILogger.logMsgType.logError, True)
				Return False
			End Try
		Next

		'If we got to here, then copy was a success
		Return True

	End Function

End Class
