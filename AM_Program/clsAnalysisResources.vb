'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 05/14/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports PRISM.Files.ZipTools
Imports AnalysisManagerBase.clsGlobal
Imports ParamFileGenerator.MakeParams

Namespace AnalysisManagerBase

	Public Class clsAnalysisResources
		Implements IAnalysisResources

		'*********************************************************************************************************
		'Base class for job resource class
		'*********************************************************************************************************

#Region "Constants"
		Protected Const COPY_ORG_DB_TRUE As Integer = 1
		Protected Const COPY_ORG_DB_FALSE As Integer = 0

		Protected Const DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 15
		Protected Const DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 5
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

		'Array for receiving data from Resource class
		Protected m_DataFileList() As String
#End Region

#Region "Properties"
		' explanation of what happened to last operation this class performed
		Public Overridable ReadOnly Property Message() As String Implements IAnalysisResources.Message
			Get
				Return m_message
			End Get
		End Property

		Public ReadOnly Property DataFileList() As String() Implements IAnalysisResources.DataFileList
			Get
				Return m_DataFileList
			End Get
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <remarks>Does nothing at present</remarks>
		Public Sub New()

		End Sub

		''' <summary>
		''' Initialize class
		''' </summary>
		''' <param name="mgrParams">Manager parameter object</param>
		''' <param name="jobParams">Job parameter object</param>
		''' <param name="logger">Logging object</param>
		''' <remarks></remarks>
		Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger) Implements IAnalysisResources.Setup
			m_mgrParams = mgrParams
			m_logger = logger
			m_jobParams = jobParams
			m_JobNum = m_jobParams.GetParam("jobNum")
			m_MachName = m_mgrParams.GetParam("MgrName")
			m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel"))
		End Sub

		''' <summary>
		''' Gets all resources for an analysis job
		''' </summary>
		''' <returns>CloseOutType indicating success or failure</returns>
		''' <remarks></remarks>
		Public Overridable Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

			Dim ParamFile As String = m_jobParams.GetParam("parmFileName")
			Dim SettingsFile As String = m_jobParams.GetParam("settingsFileName")
			Dim OrgDB As String = m_jobParams.GetParam("organismDBName")
			Dim msgstr As String

			m_WorkingDir = m_mgrParams.GetParam("WorkDir")

			'Make log entry about starting resource retrieval
			m_logger.PostEntry(m_MachName & ": Retrieving files, job " & m_JobNum, _
			ILogger.logMsgType.logNormal, LOG_DATABASE)

			'Copy OrgDB file, if specified

			'IMPORTANT: The OrgDB MUST be processed before the param file because
			'	for some analysis tools OrgDB processing generates data that feeds into param file processing
			If CInt(m_jobParams.GetParam("OrgDbReqd")) = COPY_ORG_DB_TRUE Then
				If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then
					msgstr = "Error copying OrgDB file, job " & m_JobNum
					m_logger.PostEntry(msgstr, ILogger.logMsgType.logError, LOG_DATABASE)
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

		''' <summary>
		''' Copies specified file from storage server to local working directory
		''' </summary>
		''' <param name="InpFile">Name of file to copy</param>
		''' <param name="InpFolder">Path to folder where input file is located</param>
		''' <param name="OutDir">Destination directory for file copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Function CopyFileToWorkDir(ByVal InpFile As String, ByVal InpFolder As String, _
		 ByVal OutDir As String) As Boolean

			Dim SourceFile As String

			SourceFile = Path.Combine(InpFolder, InpFile)

			'Verify source file exists
			If Not FileExistsWithRetry(SourceFile) Then
				Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File not found: " & SourceFile
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return False
			End If

			If CopyFileWithRetry(SourceFile, Path.Combine(OutDir, InpFile), True) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File copied: " & SourceFile
					m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
				End If
				Return True
			Else
				m_logger.PostEntry("Error copying file " & SourceFile, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End If

		End Function

		''' <summary>
		''' Copies specified file from storage server to local working directory, renames destination with dataset name
		''' </summary>
		''' <param name="InpFile">Name of file to copy</param>
		''' <param name="InpFolder">Path to folder where input file is located</param>
		''' <param name="OutDir">Destination directory for file copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, ByVal InpFolder As String, _
		 ByVal OutDir As String) As Boolean

			Dim SourceFile As String

			SourceFile = Path.Combine(InpFolder, InpFile)

			'Verify source file exists
			If Not FileExistsWithRetry(SourceFile) Then
				Return False
			End If

			Dim Fi As New FileInfo(SourceFile)
			Dim TargetName As String = m_jobParams.GetParam("datasetNum") & Fi.Extension

			If CopyFileWithRetry(SourceFile, Path.Combine(OutDir, TargetName), True) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File copied: " & SourceFile
					m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
					Return True
				End If
			Else
				m_logger.PostEntry("Error copying file " & SourceFile, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End If

		End Function

		''' <summary>
		''' Determines if specified org db file already exists in OrgDBPath. If so, it compares with
		''' master version and copies master if different
		''' </summary>
		''' <param name="LocalOrgDBFolder">Path to local org db file storage</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks>Function only uses file file size/date to verify file validity</remarks>
		Protected Overridable Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean

			'Determines if specified org db file already exists in OrgDBPath. If so, it compares with
			'	master version and copies master if different
			'NOTE: This function only uses file file size/date to verify file validity. For Sequest and XTandem processing
			'	override this method to use the hash functions provided by Ken's dll

			Dim FiSource As FileInfo
			Dim FiDest As FileInfo
			Dim SourceFile As String
			Dim DestFile As String
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

		''' <summary>
		''' Copies analysis tool param file
		''' </summary>
		''' <param name="ParamFileName">Name of file to copy</param>
		''' <param name="ParamFilePath">Full path to param file</param>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
		 ByVal WorkDir As String) As Boolean

			'Provides a parameter file copy function that can be overridden by the Sequest analysis classes

			If CopyFileToWorkDir(ParamFileName, ParamFilePath, WorkDir) Then
				Return True
			Else
				Return False
			End If

		End Function

		''' <summary>
		''' Copies analysis tool settings file
		''' </summary>
		''' <param name="SettingFileName">Name of file to copy</param>
		''' <param name="SettingFilePath">Full path to param file</param>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveSettingsFile(ByVal SettingFileName As String, ByVal SettingFilePath As String, _
		 ByVal WorkDir As String) As Boolean

			If CopyFileToWorkDir(SettingFileName, SettingFilePath, WorkDir) Then
				Return True
			Else
				Return False
			End If

		End Function

		''' <summary>
		''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
		''' </summary>
		''' <param name="RawDataType">Type of data to copy</param>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveSpectra(ByVal RawDataType As String, ByVal WorkDir As String) As Boolean

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

		''' <summary>
		''' Retrieves a .raw file for the analysis job in progress
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveDotRawFile(ByVal WorkDir As String) As Boolean

			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim DataFileName As String = DSName & ".raw"
			Dim DSFolderPath As String = FindValidDatasetFolder(DSName, DataFileName)

			If CopyFileToWorkDir(DataFileName, DSFolderPath, WorkDir) Then
				Return True
			Else
				Return False
			End If

		End Function

		''' <summary>
		''' Retrieves a .wiff file for the analysis job in progress
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveDotWiffFile(ByVal WorkDir As String) As Boolean

			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim DataFileName As String = DSName & ".wiff"
			Dim DSFolderPath As String = FindValidDatasetFolder(DSName, DataFileName)

			If CopyFileToWorkDir(DataFileName, DSFolderPath, WorkDir) Then
				Return True
			Else
				Return False
			End If

		End Function

		''' <summary>
		''' Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <param name="GetCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveMgfFile(ByVal WorkDir As String, ByVal GetCdfAlso As Boolean) As Boolean

			'Data files are in a subfolder off of the main dataset folder
			'Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim ServerPath As String = FindValidDatasetFolder(DSName, "", "*.D")

			Dim DSFolders() As String
			Dim DSFiles() As String = Nothing
			Dim DumFolder As String
			Dim FileFound As Boolean = False
			Dim DataFolderPath As String = ""

			'Get a list of the subfolders in the dataset folder
			DSFolders = Directory.GetDirectories(ServerPath)
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

		''' <summary>
		''' Retrieves a .raw folder from Micromass TOF for the analysis job in progress
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function RetrieveDotRawFolder(ByVal WorkDir As String) As Boolean

			'Copies a .raw datafolder from the Micromass TOF datafile to the working directory
			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim ServerPath As String = FindValidDatasetFolder(DSName, "", "*.raw")

			'Find the .raw folder in the dataset folder
			Dim RemFolders() As String = Directory.GetDirectories(ServerPath, "*.raw")
			If RemFolders.GetLength(0) <> 1 Then Return False

			'Set up the file paths
			Dim DSFolderPath As String = Path.Combine(ServerPath, RemFolders(0))

			'Do the copy
			Try
				CopyDirectory(DSFolderPath, Path.Combine(WorkDir, DSName & ".raw"))
				Return True
			Catch ex As Exception
				Dim MsgStr As String = "Error copying folder " & DSFolderPath & " to working directory: " & ex.Message
				m_logger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End Try

		End Function

		''' <summary>
		''' Unzips dataset folders to working directory
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Private Function RetrieveSFolders(ByVal WorkDir As String) As Boolean

			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim ZipFiles() As String
			Dim DSWorkFolder As String
			Dim UnZipper As ZipTools
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

			'Create a dataset subdirectory under the working directory
			DSWorkFolder = Path.Combine(WorkDir, DSName)
			Directory.CreateDirectory(DSWorkFolder)

			''Set up the unzipper
			UnZipper = New ZipTools(DSWorkFolder, m_mgrParams.GetParam("zipprogram"))

			'Unzip each of the zip files to the working directory
			For Each ZipFile In ZipFiles
				If m_DebugLevel > 3 Then
					m_logger.PostEntry("Unzipping file " & ZipFile, ILogger.logMsgType.logDebug, True)
				End If
				Try
					TargetFolder = Path.Combine(DSWorkFolder, Path.GetFileNameWithoutExtension(ZipFile))
					Directory.CreateDirectory(TargetFolder)
					If Not UnZipper.UnzipFile("", ZipFile, TargetFolder) Then
						Dim ErrMsg As String = "Error unzipping file " & ZipFile
						m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
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

		'TODO: This function can be deleted (Ticket 556)
		Protected Overridable Function SetBioworksVersion(ByVal InpVersion As String) As IGenerateFile.ParamFileType

			'Converts the setup file entry for the Bioworks version to a parameter type compatible with the
			'	parameter file generator dll
			Select Case InpVersion.ToLower
				Case "20"
					Return IGenerateFile.ParamFileType.BioWorks_20
				Case "30"
					Return IGenerateFile.ParamFileType.BioWorks_30
				Case "31"
					Return IGenerateFile.ParamFileType.BioWorks_31
				Case "32"
					Return IGenerateFile.ParamFileType.BioWorks_32
				Case "current"
					Return IGenerateFile.ParamFileType.BioWorks_Current
				Case "xtandem"
					Return IGenerateFile.ParamFileType.X_Tandem
                Case "inspect"
                    Return IGenerateFile.ParamFileType.Inspect
                Case Else
                    'If we get to here, there's a problem
                    Return Nothing
            End Select

		End Function

		''' <summary>
		''' Copies the zipped s-folders to the working directory
		''' </summary>
		''' <param name="WorkDir">Destination directory for copy</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
		Private Function CopySFoldersToWorkDir(ByVal WorkDir As String) As Boolean

			'
			Dim DSName As String = m_jobParams.GetParam("datasetNum")
			Dim DSFolderPath As String = FindValidDatasetFolder(DSName, "s*.zip")

			Dim ZipFiles() As String
			Dim ZippedFileName As String

			'Verify dataset folder exists
			If Not Directory.Exists(DSFolderPath) Then Return False

			'Get a listing of the zip files to process
			ZipFiles = Directory.GetFiles(DSFolderPath, "s*.zip")
			If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

			'copy each of the s*.zip files to the working directory
			For Each ZipFile As String In ZipFiles
				ZippedFileName = Path.GetFileName(ZipFile)
				If m_DebugLevel > 3 Then
					m_logger.PostEntry("Copying file " & ZipFile & " to work directory", ILogger.logMsgType.logDebug, True)
				End If
				If CopyFileWithRetry(ZipFile, Path.Combine(WorkDir, ZippedFileName), False) Then
					Continue For
				Else
					m_logger.PostEntry("Error copying file " & ZipFile, ILogger.logMsgType.logError, True)
					Return False
				End If
			Next

			'If we got to here, everything worked
			Return True

		End Function

		''' <summary>
		''' Copies a file with retries in case of failure
		''' </summary>
		''' <param name="SrcFileName">Full path to source file</param>
		''' <param name="DestFileName">Full path to destination file</param>
		''' <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
		''' <returns>TRUE for success; FALSE for error</returns>
		''' <remarks>Logs copy errors</remarks>
		Private Function CopyFileWithRetry(ByVal SrcFileName As String, ByVal DestFileName As String, ByVal Overwrite As Boolean) As Boolean

			Dim RetryCount As Integer = 3

			While RetryCount > 0
				Try
					File.Copy(SrcFileName, DestFileName, Overwrite)
					'Copy must have worked, so return TRUE
					Return True
				Catch ex As Exception
					Dim ErrMsg As String = "Exception copying file " & SrcFileName & " to " & DestFileName & ": " & ex.Message
					ErrMsg &= " Retry Count = " & RetryCount.ToString
					m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
					RetryCount -= 1
					System.Threading.Thread.Sleep(15000)	'Wait 15 seconds before retrying
				End Try
			End While

			'If we got to here, there were too many failures
			If RetryCount < 1 Then
				m_logger.PostEntry("Excessive failures during file copy", ILogger.logMsgType.logError, True)
				Return False
			End If

		End Function

		''' <summary>
		''' Test for file existence with a retry loop in case of temporary glitch
		''' </summary>
		''' <param name="FileName"></param>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function FileExistsWithRetry(ByVal FileName As String) As Boolean
			Return FileExistsWithRetry(FileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS)
		End Function

		''' <summary>
		''' Test for file existence with a retry loop in case of temporary glitch
		''' </summary>
		''' <param name="FileName"></param>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function FileExistsWithRetry(ByVal FileName As String, ByVal RetryHoldoffSeconds As Integer) As Boolean

			Dim RetryCount As Integer = 3

			If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS
			If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

			While RetryCount > 0
				If File.Exists(FileName) Then
					Return True
				Else
					Dim ErrMsg As String = "File " & FileName & " not found. Retry count = " & RetryCount.ToString
					m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
					RetryCount -= 1
					System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
				End If
			End While

			'If we got to here, there were too many failures
			If RetryCount < 1 Then
				m_logger.PostEntry("File could not be found after multiple retries", ILogger.logMsgType.logError, True)
				Return False
			End If

		End Function

		''' <summary>
		''' Test for folder existence with a retry loop in case of temporary glitch
		''' </summary>
		''' <param name="FolderName">Folder name to look for</param>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function FolderExistsWithRetry(ByVal FolderName As String) As Boolean
			Return FolderExistsWithRetry(FolderName, DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS)
		End Function


		''' <summary>
		''' Test for folder existence with a retry loop in case of temporary glitch
		''' </summary>
		''' <param name="FolderName">Folder name to look for</param>
		''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer) As Boolean

			Dim RetryCount As Integer = 3

			If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS
			If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

			While RetryCount > 0
				If System.IO.Directory.Exists(FolderName) Then
					Return True
				Else
					Dim ErrMsg As String = "Folder " & FolderName & " not found. Retry count = " & RetryCount.ToString
					m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
					RetryCount -= 1
					System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
				End If
			End While

			'If we got to here, there were too many failures
			If RetryCount < 1 Then
				m_logger.PostEntry("Folder could not be found after multiple retries", ILogger.logMsgType.logError, True)
				Return False
			End If

		End Function

		''' <summary>
		''' Determines the most appropriate folder to use to obtain dataset files from
		''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
		''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
		''' </summary>
		''' <param name="DSName">Name of the dataset</param>
		''' <param name="FileNameToFind">Optional: Name of a file that must exist in the folder</param>
		''' <returns>Path to the most appropriate dataset folder</returns>
		''' <remarks></remarks>
		Private Function FindValidDatasetFolder(ByVal DSName As String, ByVal FileNameToFind As String) As String
			Return FindValidDatasetFolder(DSName, FileNameToFind, "")
		End Function

		''' <summary>
		''' Determines the most appropriate folder to use to obtain dataset files from
		''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
		''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
		''' </summary>
		''' <param name="DSName">Name of the dataset</param>
		''' <param name="FileNameToFind">Optional: Name of a file that must exist in the folder</param>
		''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the folder</param>
		''' <returns>Path to the most appropriate dataset folder</returns>
		''' <remarks></remarks>
		Private Function FindValidDatasetFolder(ByVal DSName As String, ByVal FileNameToFind As String, ByVal FolderNameToFind As String) As String

			Dim strBestPath As String = String.Empty
			Dim PathsToCheck() As String

			Dim intIndex As Integer
			Dim blnValidFolder As Boolean

			Dim objFolderInfo As System.IO.DirectoryInfo

			ReDim PathsToCheck(1)

			Try
				If FileNameToFind Is Nothing Then FileNameToFind = String.Empty

				PathsToCheck(0) = Path.Combine(m_jobParams.GetParam("DatasetStoragePathLocal"), DSName)
				PathsToCheck(1) = Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), DSName)

				strBestPath = PathsToCheck(0)
				For intIndex = 0 To PathsToCheck.Length - 1
					Try
						If m_DebugLevel > 3 Then
							Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder " & PathsToCheck(intIndex)
							m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
						End If

						' First check whether this folder exists
						' Using a 3 second holdoff between retries
						If FolderExistsWithRetry(PathsToCheck(intIndex), 3) Then
							If m_DebugLevel > 3 Then
								Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Folder found " & PathsToCheck(intIndex)
								m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
							End If

							' Folder was found
							blnValidFolder = True

							' Optionally look for FileNameToFind
							If FileNameToFind.Length > 0 Then

								If FileNameToFind.Contains("*") Then
									If m_DebugLevel > 3 Then
										Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for files matching " & FileNameToFind
										m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
									End If

									' Wildcard in the name
									' Look for any files matching FileNameToFind
									objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

									If objFolderInfo.GetFiles(FileNameToFind).Length = 0 Then
										blnValidFolder = False
									End If
								Else
									If m_DebugLevel > 3 Then
										Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for file named " & FileNameToFind
										m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
									End If

									' Look for file FileNameToFind in this folder
									' Note: Using a 1 second holdoff between retries
									If Not FileExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FileNameToFind), 1) Then
										blnValidFolder = False
									End If
								End If
							End If

							' Optionally look for FolderNameToFind
							If blnValidFolder AndAlso FolderNameToFind.Length > 0 Then
								If FolderNameToFind.Contains("*") Then
									If m_DebugLevel > 3 Then
										Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folders matching " & FolderNameToFind
										m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
									End If

									' Wildcard in the name
									' Look for any folders matching FolderNameToFind
									objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

									If objFolderInfo.GetDirectories(FolderNameToFind).Length = 0 Then
										blnValidFolder = False
									End If
								Else
									If m_DebugLevel > 3 Then
										Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder named " & FolderNameToFind
										m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
									End If

									' Look for folder FolderNameToFind in this folder
									' Note: Using a 1 second holdoff between retries
									If Not FolderExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FolderNameToFind), 1) Then
										blnValidFolder = False
									End If
								End If
							End If

							If blnValidFolder Then
								strBestPath = PathsToCheck(intIndex)

								If m_DebugLevel > 3 Then
									Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Valid dataset folder has been found:  " & strBestPath
									m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
								End If

								Exit For
							End If
						End If

					Catch ex As Exception
						Dim ErrMsg As String = "Exception looking for folder: " & PathsToCheck(intIndex)
						m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
					End Try
				Next intIndex

				If Not blnValidFolder Then
					Dim Msg As String = "Could not find a valid dataset folder, Job " & m_JobNum.ToString & ", Dataset " & DSName
					m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
				End If

			Catch ex As Exception
				Dim ErrMsg As String = "Exception looking for a valid dataset folder for dataset " & DSName
				m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
			End Try

			Return strBestPath

		End Function
#End Region

	End Class

End Namespace
