'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports PHRPReader

Public MustInherit Class clsAnalysisResources
	Implements IAnalysisResources

	'*********************************************************************************************************
	'Base class for job resource class
	'*********************************************************************************************************

#Region "Constants"
	Protected Const DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 15
	Protected Const DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 5
	Protected Const DEFAULT_MAX_RETRY_COUNT As Integer = 3

	Protected Const FASTA_GEN_TIMEOUT_INTERVAL_MINUTES As Integer = 65

	' Define the maximum file size to process using IonicZip; 
	'  the reason we don't want to process larger files is that IonicZip is 1.5x to 2x slower than PkZip
	'  For example, given a 1.9 GB _isos.csv file zipped to a 660 MB .Zip file:
	'   SharpZipLib unzips the file in 130 seconds
	'   WinRar      unzips the file in 120 seconds
	'   PKZipC      unzips the file in  84 seconds
	'
	' Re-tested on 1/7/2011 with a 611 MB file
	'   IonicZip    unzips the file in 70 seconds (reading/writing to the same drive)
	'   IonicZip    unzips the file in 62 seconds (reading/writing from different drives)
	'   WinRar      unzips the file in 36 seconds (reading/writing from different drives)
	'   PKZipC      unzips the file in 38 seconds (reading/writing from different drives)
	'
	' For smaller files, the speed differences are much less noticable

	Protected Const IONIC_ZIP_MAX_FILESIZE_MB As Integer = 1280

	' Note: All of the RAW_DATA_TYPE constants need to be all lowercase
	'
	Public Const RAW_DATA_TYPE_DOT_D_FOLDERS As String = "dot_d_folders"				'Agilent ion trap data, Agilent TOF data
	Public Const RAW_DATA_TYPE_ZIPPED_S_FOLDERS As String = "zipped_s_folders"			'FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
	Public Const RAW_DATA_TYPE_DOT_RAW_FOLDER As String = "dot_raw_folder"				'Micromass QTOF data
	Public Const RAW_DATA_TYPE_DOT_RAW_FILES As String = "dot_raw_files"				'Finnigan ion trap/LTQ-FT data
	Public Const RAW_DATA_TYPE_DOT_WIFF_FILES As String = "dot_wiff_files"				'Agilent/QSTAR TOF data
	Public Const RAW_DATA_TYPE_DOT_UIMF_FILES As String = "dot_uimf_files"				'IMS_UIMF (IMS_Agilent_TOF in DMS)
	Public Const RAW_DATA_TYPE_DOT_MZXML_FILES As String = "dot_mzxml_files"			'mzXML
	Public Const RAW_DATA_TYPE_DOT_MZML_FILES As String = "dot_mzml_files"				'mzML

	' 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser folder, and a XMASS_Method.m subfolder with file apexAcquisition.method
	' Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

	' 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D folder
	' Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
	' 15T datasets also have raw_data_type "bruker_ft"
	' Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
	Public Const RAW_DATA_TYPE_BRUKER_FT_FOLDER As String = "bruker_ft"

	' The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
	' Folder has a .EMF file and a single sub-folder that has an acqu file and fid file
	Public Const RAW_DATA_TYPE_BRUKER_MALDI_SPOT As String = "bruker_maldi_spot"

	' The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
	' Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
	Public Const RAW_DATA_TYPE_BRUKER_MALDI_IMAGING As String = "bruker_maldi_imaging"

	' The following is used by instrument Maxis_01
	' Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
	Public Const RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER As String = "bruker_tof_baf"

	Public Const RESULT_TYPE_SEQUEST As String = "Peptide_Hit"
	Public Const RESULT_TYPE_XTANDEM As String = "XT_Peptide_Hit"
	Public Const RESULT_TYPE_INSPECT As String = "IN_Peptide_Hit"
	Public Const RESULT_TYPE_MSGFDB As String = "MSG_Peptide_Hit"			' Used for MSGFDB and MSGF+
	Public Const RESULT_TYPE_MSALIGN As String = "MSA_Peptide_Hit"

	Public Const DOT_WIFF_EXTENSION As String = ".wiff"
	Public Const DOT_D_EXTENSION As String = ".d"
	Public Const DOT_RAW_EXTENSION As String = ".raw"
	Public Const DOT_UIMF_EXTENSION As String = ".uimf"
	Public Const DOT_MZXML_EXTENSION As String = ".mzxml"
	Public Const DOT_MZML_EXTENSION As String = ".mzml"

	Public Const DOT_MGF_EXTENSION As String = ".mgf"
	Public Const DOT_CDF_EXTENSION As String = ".cdf"

	Public Const STORAGE_PATH_INFO_FILE_SUFFIX As String = "_StoragePathInfo.txt"

	Public Const SCAN_STATS_FILE_SUFFIX As String = "_ScanStats.txt"
	Public Const SCAN_STATS_EX_FILE_SUFFIX As String = "_ScanStatsEx.txt"

	Public Const BRUKER_ZERO_SER_FOLDER As String = "0.ser"
	Public Const BRUKER_SER_FILE As String = "ser"

	Public Enum eRawDataTypeConstants
		Unknown = 0
		ThermoRawFile = 1
		UIMF = 2
		mzXML = 3
		mzML = 4
		AgilentDFolder = 5				' Agilent ion trap data, Agilent TOF data
		AgilentQStarWiffFile = 6
		MicromassRawFolder = 7			' Micromass QTOF data
		ZippedSFolders = 8				' FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
		BrukerFTFolder = 9				' .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
		BrukerMALDISpot = 10			' has a .EMF file and a single sub-folder that has an acqu file and fid file
		BrukerMALDIImaging = 11			' Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
		BrukerTOFBaf = 12				' Used by Maxis01; Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
	End Enum

#End Region

#Region "Structures"
	Public Structure udtDataPackageJobInfoType
		Public Job As Integer
		Public Dataset As String
		Public DatasetID As Integer
		Public Experiment As String
		Public Tool As String
		Public ResultType As String
		Public PeptideHitResultType As clsPHRPReader.ePeptideHitResultType
		Public SettingsFileName As String
		Public ParameterFileName As String
		Public OrganismDBName As String				' Generated Fasta File Name or legacy fasta file name; for jobs where ProteinCollectionList = 'na', this is the legacy fasta file name; otherwise, this is the generated fasta file name (or "na")
		Public LegacyFastaFileName As String
		Public ProteinCollectionList As String
		Public ProteinOptions As String
		Public ServerStoragePath As String
		Public ArchiveStoragePath As String
		Public ResultsFolderName As String
		Public DatasetFolderName As String
		Public SharedResultsFolder As String
		Public RawDataType As String
	End Structure
#End Region

#Region "Module variables"
	Protected m_jobParams As IJobParams
	Protected m_mgrParams As IMgrParams
	Protected m_WorkingDir As String
	Protected m_DatasetName As String
	Protected m_message As String
	Protected m_DebugLevel As Short
	Protected m_MgrName As String

	Protected m_GenerationStarted As Boolean = False
	Protected m_GenerationComplete As Boolean = False
	Protected m_FastaToolsCnStr As String = ""
	Protected m_FastaFileName As String = ""
	Protected m_FastaGenTimeOut As Boolean = False
	Protected m_FastaGenStartTime As DateTime = System.DateTime.UtcNow

	Protected WithEvents m_FastaTools As Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS
	Protected WithEvents m_FastaTimer As System.Timers.Timer
	Protected m_IonicZipTools As clsIonicZipTools

	Protected WithEvents m_FileTools As PRISM.Files.clsFileTools
	Private m_LastLockQueueWaitTimeLog As System.DateTime = System.DateTime.UtcNow
#End Region

#Region "Properties"
	' Explanation of what happened to last operation this class performed
	Public ReadOnly Property Message() As String Implements IAnalysisResources.Message
		Get
			Return m_message
		End Get
	End Property
#End Region

#Region "Event handlers"
	Private Sub m_FastaTools_FileGenerationStarted(ByVal taskMsg As String) Handles m_FastaTools.FileGenerationStarted

		m_GenerationStarted = True

	End Sub

	Private Sub m_FastaTools_FileGenerationCompleted(ByVal FullOutputPath As String) Handles m_FastaTools.FileGenerationCompleted

		m_FastaFileName = System.IO.Path.GetFileName(FullOutputPath)	  'Get the name of the fasta file that was generated
		m_GenerationComplete = True		'Set the completion flag

	End Sub

	Private Sub m_FastaTools_FileGenerationProgress(ByVal statusMsg As String, ByVal fractionDone As Double) Handles m_FastaTools.FileGenerationProgress
		Const MINIMUM_LOG_INTERVAL_SEC As Integer = 10
		Static dtLastLogTime As DateTime = System.DateTime.UtcNow.Subtract(New System.TimeSpan(1, 0, 0))
		Static dblFractionDoneSaved As Double = -1

		Dim blnForcelog As Boolean = False

		If m_DebugLevel >= 1 AndAlso statusMsg.Contains(Protein_Exporter.clsGetFASTAFromDMS.LOCK_FILE_PROGRESS_TEXT) Then
			blnForcelog = True
		End If

		If m_DebugLevel >= 3 OrElse blnForcelog Then
			' Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
			If blnForcelog OrElse _
			   System.DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC OrElse _
			   fractionDone - dblFractionDoneSaved >= 0.25 Then
				dtLastLogTime = System.DateTime.UtcNow
				dblFractionDoneSaved = fractionDone
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Generating Fasta file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg)
			End If
		End If

	End Sub

	Private Sub m_FastaTimer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_FastaTimer.Elapsed

		If System.DateTime.UtcNow.Subtract(m_FastaGenStartTime).TotalMinutes >= FASTA_GEN_TIMEOUT_INTERVAL_MINUTES Then
			m_FastaGenTimeOut = True	  'Set the timeout flag so an error will be reported
			m_GenerationComplete = True		'Set the completion flag so the fasta generation wait loop will exit
		End If

	End Sub
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
	''' <remarks></remarks>
	Public Overridable Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams) Implements IAnalysisResources.Setup

		m_mgrParams = mgrParams
		m_jobParams = jobParams

		m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel", 1))
		m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring")
		m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager")

		m_WorkingDir = m_mgrParams.GetParam("workdir")
		m_DatasetName = m_jobParams.GetParam("JobParameters", "DatasetNum")

		m_IonicZipTools = New clsIonicZipTools(m_DebugLevel, m_WorkingDir)

		ResetTimestampForQueueWaitTimeLogging()
		m_FileTools = New PRISM.Files.clsFileTools(m_MgrName, m_DebugLevel)

	End Sub

	Public MustOverride Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, ByVal InpFolder As String, ByVal OutDir As String) As Boolean
		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, False)
	End Function

	''' <summary>
	''' Copies the zipped s-folders to the working directory
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified files, and instead creates a series of files named s*.zip_StoragePathInfo.txt, and each file's first line will be the full path to the source file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function CopySFoldersToWorkDir(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim DSFolderPath As String = FindValidFolder(m_DatasetName, "s*.zip")

		Dim ZipFiles() As String
		Dim DestFilePath As String

		'Verify dataset folder exists
		If Not System.IO.Directory.Exists(DSFolderPath) Then Return False

		'Get a listing of the zip files to process
		ZipFiles = System.IO.Directory.GetFiles(DSFolderPath, "s*.zip")
		If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

		'copy each of the s*.zip files to the working directory
		For Each ZipFilePath As String In ZipFiles

			If m_DebugLevel > 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " + ZipFilePath + " to work directory")
			End If

			DestFilePath = System.IO.Path.Combine(WorkDir, System.IO.Path.GetFileName(ZipFilePath))

			If CreateStoragePathInfoOnly Then
				If Not CreateStoragePathInfoFile(ZipFilePath, DestFilePath) Then
					m_message = "Error creating storage path info file for " + ZipFilePath
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If
			Else
				If Not CopyFileWithRetry(ZipFilePath, DestFilePath, False) Then
					m_message = "Error copying file " + ZipFilePath
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If
			End If
		Next

		'If we got to here, everything worked
		Return True

	End Function

	''' <summary>
	''' Copies a file with retries in case of failure
	''' </summary>
	''' <param name="SrcFilePath">Full path to source file</param>
	''' <param name="DestFilePath">Full path to destination file</param>
	''' <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
	''' <returns>TRUE for success; FALSE for error</returns>
	''' <remarks>Logs copy errors</remarks>
	Private Function CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, ByVal Overwrite As Boolean) As Boolean
		Const RETRY_HOLDOFF_SECONDS As Integer = 15

		Dim RetryCount As Integer = 3

		While RetryCount > 0
			Try
				ResetTimestampForQueueWaitTimeLogging()
				If m_FileTools.CopyFileUsingLocks(SrcFilePath, DestFilePath, m_MgrName, Overwrite) Then
					Return True
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileUsingLocks returned false copying " & SrcFilePath & " to " & DestFilePath)
					Return False
				End If
			Catch ex As Exception
				Dim ErrMsg As String = "Exception copying file " + SrcFilePath + " to " + DestFilePath + ": " + _
				  ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex)

				ErrMsg &= " Retry Count = " + RetryCount.ToString
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
				RetryCount -= 1

				If Not Overwrite AndAlso System.IO.File.Exists(DestFilePath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Tried to overwrite an existing file when Overwrite = False: " + DestFilePath)
					Return False
				End If

				System.Threading.Thread.Sleep(RETRY_HOLDOFF_SECONDS * 1000)	   'Wait several seconds before retrying
			End Try
		End While

		'If we got to here, there were too many failures
		If RetryCount < 1 Then
			m_message = "Excessive failures during file copy"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Return False

	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, False)

	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <param name="CreateStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, _
	   ByVal InpFolder As String, _
	   ByVal OutDir As String, _
	   ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	   ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim SourceFile As String = String.Empty
		Dim DestFilePath As String = String.Empty

		Try
			SourceFile = System.IO.Path.Combine(InpFolder, InpFile)
			DestFilePath = System.IO.Path.Combine(OutDir, InpFile)

			'Verify source file exists
			If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
				m_message = "File not found: " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
				Return False
			End If

			If CreateStoragePathInfoOnly Then
				' Create a storage path info file
				Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
			End If

			If CopyFileWithRetry(SourceFile, DestFilePath, True) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File copied: " + SourceFile
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If
				Return True
			Else
				m_message = "Error copying file " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

		Catch ex As Exception
			If SourceFile Is Nothing Then SourceFile = InpFile
			If SourceFile Is Nothing Then SourceFile = "??"

			m_message = "Exception in CopyFileToWorkDir for " + SourceFile
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
		End Try

		Return False

	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String) As Boolean
		Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, False)
	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean
		Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, False)
	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory, renames destination with dataset name
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified file, and instead creates a file named FileName_StoragePathInfo.txt, and this file's first line will be the full path to the source file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal CreateStoragePathInfoOnly As Boolean) As Boolean


		Dim SourceFile As String = String.Empty
		Dim DestFilePath As String = String.Empty

		Try
			SourceFile = System.IO.Path.Combine(InpFolder, InpFile)

			'Verify source file exists
			If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
				m_message = "File not found: " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
				Return False
			End If

			Dim Fi As New System.IO.FileInfo(SourceFile)
			Dim TargetName As String = m_DatasetName + Fi.Extension
			DestFilePath = System.IO.Path.Combine(OutDir, TargetName)

			If CreateStoragePathInfoOnly Then
				' Create a storage path info file
				Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
			End If

			If CopyFileWithRetry(SourceFile, DestFilePath, True) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDirWithRename, File copied: " + SourceFile
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If
				Return True
			Else
				m_message = "Error copying file " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

		Catch ex As Exception
			If SourceFile Is Nothing Then SourceFile = InpFile
			If SourceFile Is Nothing Then SourceFile = "??"

			m_message = "Exception in CopyFileToWorkDirWithRename for " + SourceFile
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
		End Try

		Return False

	End Function

	''' <summary>
	''' Creates a Fasta file based on Ken's DLL
	''' </summary>
	''' <param name="DestFolder">Folder where file will be created</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function CreateFastaFile(ByVal DestFolder As String) As Boolean

		Dim HashString As String
		Dim OrgDBDescription As String = String.Empty

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating fasta file")
		End If

		'Instantiate fasta tool if not already done
		If m_FastaTools Is Nothing Then
			If m_FastaToolsCnStr = "" Then
				m_message = "Protein database connection string not specified"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Error in CreateFastaFile: " + m_message)
				Return False
			End If
			m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
		End If

		'Initialize fasta generation state variables
		m_GenerationStarted = False
		m_GenerationComplete = False

		'Set up variables for fasta creation call
		Dim LegacyFasta As String = m_jobParams.GetParam("LegacyFastaFileName")
		Dim CreationOpts As String = m_jobParams.GetParam("ProteinOptions")
		Dim CollectionList As String = m_jobParams.GetParam("ProteinCollectionList")

		If Not String.IsNullOrWhiteSpace(CollectionList) AndAlso Not CollectionList.ToLower() = "na" Then
			OrgDBDescription = "Protein collection: " + CollectionList + " with options " + CreationOpts
		ElseIf Not String.IsNullOrWhiteSpace(LegacyFasta) AndAlso Not LegacyFasta.ToLower() = "na" Then
			OrgDBDescription = "Legacy DB: " + LegacyFasta
		Else
			m_message = "Both the ProteinCollectionList and LegacyFastaFileName parameters are empty or 'na'; unable to obtain Fasta file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateFastaFile: " + m_message)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ProteinCollectionList=" + CollectionList + "; CreationOpts=" + CreationOpts + "; LegacyFasta=" + LegacyFasta)
		End If

		m_FastaTimer = New System.Timers.Timer
		m_FastaTimer.Interval = 5000
		m_FastaTimer.AutoReset = True

		' Note that m_FastaTools does not spawn a new thread
		'   Since it does not spawn a new thread, the while loop after this Try block won't actually get reached while m_FastaTools.ExportFASTAFile is running
		'   Furthermore, even if m_FastaTimer_Elapsed sets m_FastaGenTimeOut to True, this won't do any good since m_FastaTools.ExportFASTAFile will still be running
		m_FastaGenTimeOut = False
		m_FastaGenStartTime = System.DateTime.UtcNow
		Try
			m_FastaTimer.Start()
			HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, LegacyFasta, DestFolder)
		Catch Ex As Exception
			m_message = "Exception generating OrgDb file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception generating OrgDb file; " + OrgDBDescription + "; " + _
			 "; " + clsGlobal.GetExceptionStackTrace(Ex))
			Return False
		End Try

		'Wait for fasta creation to finish
		While Not (m_GenerationComplete Or m_FastaGenTimeOut)
			System.Threading.Thread.Sleep(2000)
		End While

		m_FastaTimer.Stop()
		If m_FastaGenTimeOut Then
			'Fasta generator hung - report error and exit
			m_message = "Timeout error while generating OrdDb file (" + FASTA_GEN_TIMEOUT_INTERVAL_MINUTES.ToString + " minutes have elapsed); " + OrgDBDescription
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If String.IsNullOrEmpty(HashString) Then
			' Fasta generator returned empty hash string
			m_message = "m_FastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " + OrgDBDescription
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If m_DebugLevel >= 1 Then
			' Log the name of the .Fasta file we're using
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Fasta generation complete, using database: " + m_FastaFileName)

			If m_DebugLevel >= 2 Then
				' Also log the file creation and modification dates
				Try
					Dim fiFastaFile As System.IO.FileInfo
					Dim strFastaFileMsg As String
					fiFastaFile = New System.IO.FileInfo(System.IO.Path.Combine(DestFolder, m_FastaFileName))

					strFastaFileMsg = "Fasta file last modified: " + GetHumanReadableTimeInterval(System.DateTime.UtcNow.Subtract(fiFastaFile.LastWriteTimeUtc)) + " ago at " + fiFastaFile.LastWriteTime.ToString()
					strFastaFileMsg &= "; file created: " + GetHumanReadableTimeInterval(System.DateTime.UtcNow.Subtract(fiFastaFile.CreationTimeUtc)) + " ago at " + fiFastaFile.CreationTime.ToString()
					strFastaFileMsg &= "; file size: " + fiFastaFile.Length.ToString() + " bytes"

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strFastaFileMsg)
				Catch ex As Exception
					' Ignore errors here
				End Try
			End If

		End If

		'If we got to here, everything worked OK
		Return True

	End Function

	''' <summary>
	''' Creates an XML formatted settings file based on data from broker
	''' </summary>
	''' <param name="FileText">String containing XML file contents</param>
	''' <param name="FileNamePath">Name of file to create</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>XML handling based on code provided by Matt Monroe</remarks>
	Private Function CreateSettingsFile(ByVal FileText As String, ByVal FileNamePath As String) As Boolean

		Dim objFormattedXMLWriter As New clsFormattedXMLWriter

		If Not objFormattedXMLWriter.WriteXMLToFile(FileText, FileNamePath) Then
			m_message = "Error creating settings file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " " + FileNamePath + ": " + objFormattedXMLWriter.ErrMsg)
			Return False
		Else
			Return True
		End If

	End Function

	''' <summary>
	''' Creates a file named DestFilePath but with "_StoragePathInfo.txt" appended to the name
	''' The file's contents is the path given by SourceFilePath
	''' </summary>
	''' <param name="SourceFilePath">The path to write to the StoragePathInfo file</param>
	''' <param name="DestFilePath">The path where the file would have been copied to</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CreateStoragePathInfoFile(ByVal SourceFilePath As String, ByVal DestFilePath As String) As Boolean

		Dim swOutFile As System.IO.StreamWriter
		Dim strInfoFilePath As String = String.Empty

		Try
			If SourceFilePath Is Nothing Or DestFilePath Is Nothing Then
				Return False
			End If

			strInfoFilePath = DestFilePath + STORAGE_PATH_INFO_FILE_SUFFIX

			swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strInfoFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

			swOutFile.WriteLine(SourceFilePath)
			swOutFile.Close()

		Catch ex As Exception
			m_message = "Exception in CreateStoragePathInfoFile for " + strInfoFilePath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Tries to delete the first file whose path is defined in strFilesToDelete
	''' If deletion succeeds, then removes the file from the queue
	''' </summary>
	''' <param name="strFilesToDelete">Queue of files to delete (full file paths)</param>
	''' <param name="strFileToQueueForDeletion">Optional: new file to add to the queue; blank to do nothing</param>
	''' <remarks></remarks>
	Protected Sub DeleteQueuedFiles(ByRef strFilesToDelete As System.Collections.Generic.Queue(Of String), ByVal strFileToQueueForDeletion As String)

		If strFilesToDelete.Count > 0 Then
			' Call the garbage collector, then try to delete the first queued file
			' Note, do not call WaitForPendingFinalizers since that could block this thread
			GC.Collect()

			Try
				Dim strFileToDelete As String
				strFileToDelete = strFilesToDelete.Peek()

				System.IO.File.Delete(strFileToDelete)

				' If we get here, then the delete succeeded, so we can dequeue the file
				strFilesToDelete.Dequeue()

			Catch ex As Exception
				' Exception deleting the file; ignore this error
			End Try

		End If

		If Not String.IsNullOrEmpty(strFileToQueueForDeletion) Then
			strFilesToDelete.Enqueue(strFileToQueueForDeletion)
		End If

	End Sub


	''' <summary>
	''' Test for file existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FileName"></param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Overloads Function FileExistsWithRetry(ByVal FileName As String, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

		Return FileExistsWithRetry(FileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS, eLogMsgTypeIfNotFound)

	End Function

	''' <summary>
	''' Test for file existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FileName"></param>
	''' <param name="RetryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Overloads Function FileExistsWithRetry(ByVal FileName As String, ByVal RetryHoldoffSeconds As Integer, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

		Dim RetryCount As Integer = 3

		If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS
		If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

		While RetryCount > 0
			If System.IO.File.Exists(FileName) Then
				Return True
			Else
				If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
					' Only log each failed attempt to find the file if eLogMsgTypeIfNotFound = ILogger.logMsgType.logError
					' Otherwise, we won't log each failed attempt
					Dim ErrMsg As String = "File " + FileName + " not found. Retry count = " + RetryCount.ToString
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, ErrMsg)
				End If
				RetryCount -= 1
				System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
			End If
		End While

		'If we got to here, there were too many failures
		If RetryCount < 1 Then
			m_message = "File " + FileName + " could not be found after multiple retries"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
			Return False
		End If

		Return False

	End Function

	''' <summary>
	''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
	''' </summary>
	''' <param name="FileName">Name of file to be retrieved</param>
	''' <param name="Unzip">TRUE if retrieved file should be unzipped after retrieval</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function FindAndRetrieveMiscFiles(ByVal FileName As String, ByVal Unzip As Boolean) As Boolean

		Return FindAndRetrieveMiscFiles(FileName, Unzip, True)
	End Function

	''' <summary>
	''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
	''' </summary>
	''' <param name="FileName">Name of file to be retrieved</param>
	''' <param name="Unzip">TRUE if retrieved file should be unzipped after retrieval</param>
	''' <param name="SearchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function FindAndRetrieveMiscFiles(ByVal FileName As String, ByVal Unzip As Boolean, ByVal SearchArchivedDatasetFolder As Boolean) As Boolean

		'Find file location
		Dim FolderName As String
		Dim CreateStoragePathInfoFile As Boolean = False

		' Look for the file in the various folders
		FolderName = FindDataFile(FileName, SearchArchivedDatasetFolder)

		' Exit if file was not found
		If String.IsNullOrEmpty(FolderName) Then
			' No folder found containing the specified file
			Return False
		End If

		' Copy the file
		If Not CopyFileToWorkDir(FileName, FolderName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoFile) Then
			Return False
		End If

		'Return or unzip file, as specified
		If Not Unzip Then Return True

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + FileName)
		If UnzipFileStart(System.IO.Path.Combine(m_WorkingDir, FileName), m_WorkingDir, "clsAnalysisResources.FindAndRetrieveMiscFiles", False) Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipped file " + FileName)
			End If
		End If


		Return True

	End Function

	''' <summary>
	''' Finds the server or archive folder where specified file is located
	''' </summary>
	''' <param name="FileToFind">Name of the file to search for</param>
	''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
	''' <remarks></remarks>
	Protected Function FindDataFile(ByVal FileToFind As String) As String
		Return FindDataFile(FileToFind, SearchArchivedDatasetFolder:=True)
	End Function

	''' <summary>
	''' Finds the server or archive folder where specified file is located
	''' </summary>
	''' <param name="FileToFind">Name of the file to search for</param>
	''' <param name="SearchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
	''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
	''' <remarks></remarks>
	Protected Function FindDataFile(ByVal FileToFind As String, ByVal SearchArchivedDatasetFolder As Boolean) As String
		Return FindDataFile(FileToFind, SearchArchivedDatasetFolder, LogFileNotFound:=True)
	End Function

	''' <summary>
	''' Finds the server or archive folder where specified file is located
	''' </summary>
	''' <param name="FileToFind">Name of the file to search for</param>
	''' <param name="SearchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
	''' <param name="LogFileNotFound">True if an error should be logged when a file is not found</param>
	''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
	''' <remarks></remarks>
	Protected Function FindDataFile(ByVal FileToFind As String, ByVal SearchArchivedDatasetFolder As Boolean, ByVal LogFileNotFound As Boolean) As String

		Dim FoldersToSearch As New System.Collections.Generic.List(Of String)
		Dim TempDir As String = String.Empty
		Dim FileFound As Boolean = False

		Dim strParentFolderPaths As System.Collections.Generic.List(Of String)
		Dim strDatasetFolderName As String
		Dim strInputFolderName As String

		Dim strSharedResultFolders As String

		Dim SharedResultFolderNames As New System.Collections.Generic.List(Of String)

		Try
			' Fill collection with possible folder locations
			' The order of searching is:
			'  a. Check the "inputFolderName" and then each of the Shared Results Folders in the Transfer folder
			'  b. Check the "inputFolderName" and then each of the Shared Results Folders in the Dataset folder
			'  c. Check the "inputFolderName" and then each of the Shared Results Folders in the Archived dataset folder
			'
			' Note that "SharedResultsFolders" will typically only contain one folder path, 
			'  but can contain a comma-separated list of folders

			strDatasetFolderName = m_jobParams.GetParam("DatasetFolderName")
			strInputFolderName = m_jobParams.GetParam("inputFolderName")
			strSharedResultFolders = m_jobParams.GetParam("SharedResultsFolders")

			If strSharedResultFolders.Contains(",") Then

				' Split on commas and populate SharedResultFolderNames
				For Each strItem As String In strSharedResultFolders.Split(","c)
					If strItem.Trim.Length > 0 Then
						SharedResultFolderNames.Add(strItem.Trim)
					End If
				Next

				' Reverse the list so that the last item in strSharedResultFolders is the first item in SharedResultFolderNames
				SharedResultFolderNames.Reverse()
			Else
				' Just one item in strSharedResultFolders
				SharedResultFolderNames.Add(strSharedResultFolders)
			End If

			strParentFolderPaths = New System.Collections.Generic.List(Of String)
			strParentFolderPaths.Add(m_jobParams.GetParam("transferFolderPath"))
			strParentFolderPaths.Add(m_jobParams.GetParam("DatasetStoragePath"))

			If SearchArchivedDatasetFolder Then
				strParentFolderPaths.Add(m_jobParams.GetParam("DatasetArchivePath"))
			End If

			For Each strParentFolderPath As String In strParentFolderPaths

				If Not String.IsNullOrEmpty(strParentFolderPath) Then
					If Not String.IsNullOrEmpty(strInputFolderName) Then
						FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strInputFolderName))	' Parent Folder / Dataset Folder / Input folder
					End If

					For Each strSharedFolderName As String In SharedResultFolderNames
						FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strSharedFolderName))			' Parent Folder / Dataset Folder /  Shared results folder
					Next

					FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, String.Empty))	' Parent Folder / Dataset Folder
				End If

			Next

			' Now search for FileToFind in each folder in FoldersToSearch
			For Each TempDir In FoldersToSearch
				Try
					If System.IO.Directory.Exists(TempDir) Then
						If System.IO.File.Exists(System.IO.Path.Combine(TempDir, FileToFind)) Then
							FileFound = True
							Exit For
						End If
					End If
				Catch ex As Exception
					' Exception checking TempDir; log an error, but continue checking the other folders in FoldersToSearch
					m_message = "Exception in FindDataFile looking for: " + FileToFind + " in " + TempDir + ": " + ex.Message
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End Try
			Next

			If FileFound Then
				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Data file found: " + FileToFind)
				End If
				Return TempDir
			Else
				' Data file not found
				' Log this as an error if SearchArchivedDatasetFolder=True
				' Log this as a warning if SearchArchivedDatasetFolder=False

				If LogFileNotFound Then
					If SearchArchivedDatasetFolder Then
						m_message = "Data file not found: " + FileToFind
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Data file not found (did not check archive): " + FileToFind)
					End If
				End If

				Return String.Empty
			End If

		Catch ex As Exception
			m_message = "Exception in FindDataFile looking for: " + FileToFind
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
		End Try

		' We'll only get here if an exception occurs
		Return String.Empty

	End Function

	Private Function FindDataFileAddFolder(ByVal strParentFolderPath As String, _
	   ByVal strDatasetFolderName As String, _
	   ByVal strInputFolderName As String) As String
		Dim strTargetFolderPath As String

		strTargetFolderPath = System.IO.Path.Combine(strParentFolderPath, strDatasetFolderName)
		If Not String.IsNullOrEmpty(strInputFolderName) Then
			strTargetFolderPath = System.IO.Path.Combine(strTargetFolderPath, strInputFolderName)
		End If

		Return strTargetFolderPath

	End Function

	''' <summary>
	''' Looks for file strFileName in strFolderPath or any of its subfolders
	''' The filename may contain a wildcard character, in which case the first match will be returned
	''' </summary>
	''' <param name="strFolderPath">Folder path to examine</param>
	''' <param name="strFileName">File name to find</param>
	''' <returns>Full path to the file, if found</returns>
	''' <remarks></remarks>
	Public Shared Function FindFileInDirectoryTree(ByVal strFolderPath As String, ByVal strFileName As String) As String
		Dim ioFolder As System.IO.DirectoryInfo
		Dim ioFile As System.IO.FileSystemInfo
		Dim ioSubFolder As System.IO.FileSystemInfo

		Dim strFilePathMatch As String = String.Empty

		ioFolder = New System.IO.DirectoryInfo(strFolderPath)

		If ioFolder.Exists Then
			' Examine the files for this folder
			For Each ioFile In ioFolder.GetFiles(strFileName)
				strFilePathMatch = ioFile.FullName
				Return strFilePathMatch
			Next

			' Match not found
			' Recursively call this function with the subdirectories in this folder

			For Each ioSubFolder In ioFolder.GetDirectories
				strFilePathMatch = FindFileInDirectoryTree(ioSubFolder.FullName, strFileName)
				If Not String.IsNullOrEmpty(strFilePathMatch) Then
					Return strFilePathMatch
				End If
			Next
		End If

		Return strFilePathMatch
	End Function

	Protected Function FindMZXmlFile() As String

		' Finds this dataset's .mzXML file
		Dim DatasetID As String = m_jobParams.GetParam("JobParameters", "DatasetID")
		Dim MSXmlFoldernameBase As String = "MSXML_Gen_1_"
		Dim MzXMLFilename As String = m_DatasetName + ".mzXML"
		Dim ServerPath As String


		Dim MaxRetryCount As Integer = 1

		Dim lstValuesToCheck As System.Collections.Generic.List(Of Integer)
		lstValuesToCheck = New System.Collections.Generic.List(Of Integer)

		' Initialize the values we'll look for
		lstValuesToCheck.Add(154)			' MSXML_Gen_1_154_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=250; MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(132)			' MSXML_Gen_1_132_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=150; MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(93)			' MSXML_Gen_1_93_DatasetID,    CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(126)			' MSXML_Gen_1_126_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML; ReAdW_Version=v2.1;
		lstValuesToCheck.Add(39)			' MSXML_Gen_1_39_DatasetID,    CentroidMSXML=False; MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;

		For Each intVersion As Integer In lstValuesToCheck

			Dim SourceFilePath As String = String.Empty

			Dim MSXmlFoldername As String
			MSXmlFoldername = MSXmlFoldernameBase + intVersion.ToString() + "_" + DatasetID

			' Look for the MSXmlFolder
			' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
			ServerPath = FindValidFolder(m_DatasetName, "", MSXmlFoldername, MaxRetryCount, False)

			If Not String.IsNullOrEmpty(ServerPath) Then

				Dim diFolderInfo As System.IO.DirectoryInfo
				diFolderInfo = New System.IO.DirectoryInfo(ServerPath)

				If diFolderInfo.Exists Then

					'See if the ServerPath folder actually contains a subfolder named MSXmlFoldername
					Dim diSubfolders() As System.IO.DirectoryInfo = diFolderInfo.GetDirectories(MSXmlFoldername)
					If diSubfolders.Length > 0 Then

						' MSXmlFolder found; return the path        
						Return System.IO.Path.Combine(diSubfolders(0).FullName, MzXMLFilename)

					End If

				End If

			End If

		Next

		Return String.Empty

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
	Protected Function FindValidFolder(ByVal DSName As String, ByVal FileNameToFind As String) As String

		Return FindValidFolder(DSName, FileNameToFind, "", DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True)

	End Function

	Protected Function FindValidFolder(ByVal DSName As String, _
	  ByVal FileNameToFind As String, _
	  ByVal FolderNameToFind As String) As String

		Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True)

	End Function

	Protected Function FindValidFolder(ByVal DSName As String, _
	  ByVal FileNameToFind As String, _
	  ByVal FolderNameToFind As String, _
	  ByVal MaxRetryCount As Integer) As String

		Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, MaxRetryCount, LogFolderNotFound:=True)

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by Job Param "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Optional: Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks></remarks>
	Protected Function FindValidFolder(ByVal DSName As String, _
	   ByVal FileNameToFind As String, _
	   ByVal FolderNameToFind As String, _
	   ByVal MaxRetryCount As Integer, _
	   ByVal LogFolderNotFound As Boolean) As String

		Dim strBestPath As String = String.Empty
		Dim PathsToCheck() As String

		Dim intIndex As Integer
		Dim blnValidFolder As Boolean
		Dim blnFileNotFoundEncountered As Boolean

		Dim objFolderInfo As System.IO.DirectoryInfo

		ReDim PathsToCheck(2)

		Try
			If FileNameToFind Is Nothing Then FileNameToFind = String.Empty
			If FolderNameToFind Is Nothing Then FolderNameToFind = String.Empty

			PathsToCheck(0) = System.IO.Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), DSName)
			PathsToCheck(1) = System.IO.Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), DSName)
			PathsToCheck(2) = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), DSName)

			blnFileNotFoundEncountered = False

			strBestPath = PathsToCheck(0)
			For intIndex = 0 To PathsToCheck.Length - 1
				Try
					If m_DebugLevel > 3 Then
						Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder " + PathsToCheck(intIndex)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
					End If

					' First check whether this folder exists
					' Using a 3 second holdoff between retries
					If FolderExistsWithRetry(PathsToCheck(intIndex), 3, MaxRetryCount, LogFolderNotFound) Then
						If m_DebugLevel > 3 Then
							Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Folder found " + PathsToCheck(intIndex)
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
						End If

						' Folder was found
						blnValidFolder = True

						' Optionally look for FileNameToFind
						If FileNameToFind.Length > 0 Then

							If FileNameToFind.Contains("*") Then
								If m_DebugLevel > 3 Then
									Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for files matching " + FileNameToFind
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
								End If

								' Wildcard in the name
								' Look for any files matching FileNameToFind
								objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

								If objFolderInfo.GetFiles(FileNameToFind).Length = 0 Then
									blnValidFolder = False
								End If
							Else
								If m_DebugLevel > 3 Then
									Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for file named " + FileNameToFind
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
								End If

								' Look for file FileNameToFind in this folder
								' Note: Using a 1 second holdoff between retries
								If Not FileExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FileNameToFind), 1, clsLogTools.LogLevels.WARN) Then
									blnValidFolder = False
								End If
							End If
						End If

						' Optionally look for FolderNameToFind
						If blnValidFolder AndAlso FolderNameToFind.Length > 0 Then
							If FolderNameToFind.Contains("*") Then
								If m_DebugLevel > 3 Then
									Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folders matching " + FolderNameToFind
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
								End If

								' Wildcard in the name
								' Look for any folders matching FolderNameToFind
								objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

								If objFolderInfo.GetDirectories(FolderNameToFind).Length = 0 Then
									blnValidFolder = False
								End If
							Else
								If m_DebugLevel > 3 Then
									Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder named " + FolderNameToFind
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
								End If

								' Look for folder FolderNameToFind in this folder
								' Note: Using a 1 second holdoff between retries
								If Not FolderExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FolderNameToFind), 1, MaxRetryCount, LogFolderNotFound) Then
									blnValidFolder = False
								End If
							End If
						End If

						If Not blnValidFolder Then
							blnFileNotFoundEncountered = True
						Else
							strBestPath = PathsToCheck(intIndex)

							If m_DebugLevel >= 4 OrElse m_DebugLevel >= 1 AndAlso blnFileNotFoundEncountered Then
								Dim Msg As String = "clsAnalysisResources.FindValidFolder, Valid dataset folder has been found:  " + strBestPath
								If FileNameToFind.Length > 0 Then
									Msg &= " (matched file " + FileNameToFind + ")"
								End If
								If FolderNameToFind.Length > 0 Then
									Msg &= " (matched folder " + FolderNameToFind + ")"
								End If
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
							End If

							Exit For
						End If
					Else
						blnFileNotFoundEncountered = True
					End If

				Catch ex As Exception
					m_message = "Exception looking for folder: " + PathsToCheck(intIndex) + "; " + clsGlobal.GetExceptionStackTrace(ex)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End Try
			Next intIndex

			If Not blnValidFolder Then
				m_message = "Could not find a valid dataset folder"
				If FileNameToFind.Length > 0 Then
					m_message &= " containing file " + FileNameToFind
				End If
				If LogFolderNotFound Then
					If m_DebugLevel >= 1 Then
						Dim Msg As String = m_message + ", Job " + m_jobParams.GetParam("StepParameters", "Job") + ", Dataset " + DSName
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
					End If
				End If
			End If

		Catch ex As Exception
			m_message = "Exception looking for a valid dataset folder"
			Dim ErrMsg As String = m_message + " for dataset " + DSName + "; " + clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
		End Try

		Return strBestPath

	End Function

	''' <summary>
	''' Test for folder existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FolderName">Folder name to look for</param>
	Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String) As Boolean
		Return FolderExistsWithRetry(FolderName, DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS, DEFAULT_MAX_RETRY_COUNT, True)
	End Function

	''' <summary>
	''' Test for folder existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FolderName">Folder name to look for</param>
	''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer) As Boolean
		Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, DEFAULT_MAX_RETRY_COUNT, True)
	End Function

	''' <summary>
	''' Test for folder existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FolderName">Folder name to look for</param>
	''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer, ByVal MaxRetryCount As Integer) As Boolean
		Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, MaxRetryCount, True)
	End Function

	''' <summary>
	''' Test for folder existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FolderName">Folder name to look for</param>
	''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String, _
	 ByVal RetryHoldoffSeconds As Integer, _
	 ByVal MaxRetryCount As Integer, _
	 ByVal LogFolderNotFound As Boolean) As Boolean

		Dim RetryCount As Integer

		If MaxRetryCount < 1 Then MaxRetryCount = 1
		If MaxRetryCount > 10 Then MaxRetryCount = 10
		RetryCount = MaxRetryCount

		If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS
		If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

		While RetryCount > 0
			If System.IO.Directory.Exists(FolderName) Then
				Return True
			Else
				If LogFolderNotFound Then
					If m_DebugLevel >= 2 OrElse m_DebugLevel >= 1 AndAlso RetryCount = 0 Then
						Dim ErrMsg As String = "Folder " + FolderName + " not found. Retry count = " + RetryCount.ToString
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, ErrMsg)
					End If
				End If
				RetryCount -= 1
				If RetryCount <= 0 Then
					Return False
				Else
					System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
				End If
			End If
		End While

		Return False

	End Function

	Protected Function GenerateScanStatsFile() As Boolean

		Dim strRawDataType As String
		Dim strInputFilePath As String

		Dim strMSFileInfoScannerDir As String
		Dim strMSFileInfoScannerDLLPath As String

		Dim objScanStatsGenerator As clsScanStatsGenerator
		Dim blnSuccess As Boolean

		strRawDataType = m_jobParams.GetParam("RawDataType")

		strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir")
		If String.IsNullOrEmpty(strMSFileInfoScannerDir) Then
			m_message = "Manager parameter 'MSFileInfoScannerDir' is not defined"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
			Return False
		End If

		strMSFileInfoScannerDLLPath = System.IO.Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll")
		If Not System.IO.File.Exists(strMSFileInfoScannerDLLPath) Then
			m_message = "File Not Found: " + strMSFileInfoScannerDLLPath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
			Return False
		End If

		' Confirm that this dataset is a Thermo .Raw file or a .UIMF file
		Select Case clsAnalysisResources.GetRawDataType(strRawDataType)
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				strInputFilePath = m_DatasetName + clsAnalysisResources.DOT_RAW_EXTENSION
			Case clsAnalysisResources.eRawDataTypeConstants.UIMF
				strInputFilePath = m_DatasetName + clsAnalysisResources.DOT_UIMF_EXTENSION
			Case Else
				m_message = "Invalid dataset type for auto-generating ScanStats.txt file: " + strRawDataType
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
				Return False
		End Select

		strInputFilePath = System.IO.Path.Combine(m_WorkingDir, strInputFilePath)

		If Not RetrieveSpectra(strRawDataType, m_WorkingDir) Then
			Dim strExtraMsg As String = m_message
			m_message = "Error retrieving spectra file"
			If Not String.IsNullOrWhiteSpace(strExtraMsg) Then
				m_message &= "; " + strExtraMsg
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message)
			Return False
		End If

		' Make sure the raw data file does not get copied to the results folder
		m_jobParams.AddResultFileToSkip(System.IO.Path.GetFileName(strInputFilePath))

		objScanStatsGenerator = New clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel)

		' Create the _ScanStats.txt and _ScanStatsEx.txt files
		blnSuccess = objScanStatsGenerator.GenerateScanStatsFile(strInputFilePath, m_WorkingDir)

		If blnSuccess Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generated ScanStats file using " + strInputFilePath)
			End If

			System.Threading.Thread.Sleep(500)
			Try
				System.IO.File.Delete(strInputFilePath)
			Catch ex As Exception
				' Ignore errors here
			End Try
		Else
			m_message = "Error generating ScanStats files with clsScanStatsGenerator"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, objScanStatsGenerator.ErrorMessage)
			If objScanStatsGenerator.MSFileInfoScannerErrorCount > 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner encountered " + objScanStatsGenerator.MSFileInfoScannerErrorCount.ToString() + " errors")
			End If
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Split apart coordinates that look like "R00X438Y093" into R, X, and Y
	''' </summary>
	''' <param name="strCoord"></param>
	''' <param name="R"></param>
	''' <param name="X"></param>
	''' <param name="Y"></param>
	''' <returns>True if success, false otherwise</returns>
	''' <remarks></remarks>
	Public Shared Function GetBrukerImagingFileCoords(ByVal strCoord As String, _
	  ByRef R As Integer, _
	  ByRef X As Integer, _
	  ByRef Y As Integer) As Boolean

		Static reRegExRXY As System.Text.RegularExpressions.Regex
		Static reRegExRX As System.Text.RegularExpressions.Regex

		Dim reMatch As System.Text.RegularExpressions.Match
		Dim blnSuccess As Boolean

		If reRegExRXY Is Nothing Then
			reRegExRXY = New System.Text.RegularExpressions.Regex("R(\d+)X(\d+)Y(\d+)", System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
			reRegExRX = New System.Text.RegularExpressions.Regex("R(\d+)X(\d+)", System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
		End If

		' Try to match names like R00X438Y093
		reMatch = reRegExRXY.Match(strCoord)

		blnSuccess = False

		If reMatch.Success Then
			' Match succeeded; extract out the coordinates
			If Integer.TryParse(reMatch.Groups.Item(1).Value, R) Then blnSuccess = True
			If Integer.TryParse(reMatch.Groups.Item(2).Value, X) Then blnSuccess = True
			Integer.TryParse(reMatch.Groups.Item(3).Value, Y)

		Else
			' Try to match names like R00X438
			reMatch = reRegExRX.Match(strCoord)

			If reMatch.Success Then
				If Integer.TryParse(reMatch.Groups.Item(1).Value, R) Then blnSuccess = True
				If Integer.TryParse(reMatch.Groups.Item(2).Value, X) Then blnSuccess = True
			End If
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Looks for job parameters BrukerMALDI_Imaging_StartSectionX and BrukerMALDI_Imaging_EndSectionX
	''' If defined, then populates StartSectionX and EndSectionX with the Start and End X values to filter on
	''' </summary>
	''' <param name="objJobParams"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function GetBrukerImagingSectionFilter(ByRef objJobParams As IJobParams, _
	  ByRef StartSectionX As Integer, _
	  ByRef EndSectionX As Integer) As Boolean

		Dim blnApplySectionFilter As Boolean

		Dim strParam As String

		blnApplySectionFilter = False
		StartSectionX = -1
		EndSectionX = Int32.MaxValue

		strParam = objJobParams.GetParam("MALDI_Imaging_StartSectionX")
		If Not String.IsNullOrEmpty(strParam) Then
			If Integer.TryParse(strParam, StartSectionX) Then
				blnApplySectionFilter = True
			End If
		End If

		strParam = objJobParams.GetParam("MALDI_Imaging_EndSectionX")
		If Not String.IsNullOrEmpty(strParam) Then
			If Integer.TryParse(strParam, EndSectionX) Then
				blnApplySectionFilter = True
			End If
		End If

		Return blnApplySectionFilter

	End Function

	''' <summary>
	''' Reports the amount of free memory on this computer (in MB)
	''' </summary>
	''' <returns>Free memory, in MB</returns>
	Public Shared Function GetFreeMemoryMB() As Single

		Static mFreeMemoryPerformanceCounter As System.Diagnostics.PerformanceCounter

		Dim sngFreeMemory As Single

		Try
			If mFreeMemoryPerformanceCounter Is Nothing Then
				mFreeMemoryPerformanceCounter = New System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
				mFreeMemoryPerformanceCounter.ReadOnly = True
			End If

			sngFreeMemory = mFreeMemoryPerformanceCounter.NextValue()

		Catch ex As Exception
			' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
			' A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group in "Local Users and Groups" on the machine showing this error.  
			' Alternatively, add the user to the "Administrators" group.
			' In either case, you will need to reboot the computer for the change to take effect
			If System.DateTime.Now().Hour = 0 And System.DateTime.Now().Minute <= 30 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " + ex.Message)
			End If
		End Try

		Return sngFreeMemory

	End Function


	''' <summary>
	''' Converts the given timespan to the total days, hours, minutes, or seconds as a string
	''' </summary>
	''' <param name="dtInterval">Timespan to convert</param>
	''' <returns>Timespan length in human readable form</returns>
	''' <remarks></remarks>
	Protected Function GetHumanReadableTimeInterval(ByVal dtInterval As System.TimeSpan) As String

		If dtInterval.TotalDays >= 1 Then
			' Report Days
			Return dtInterval.TotalDays.ToString("0.00") + " days"
		ElseIf dtInterval.TotalHours >= 1 Then
			' Report hours
			Return dtInterval.TotalHours.ToString("0.00") + " hours"
		ElseIf dtInterval.TotalMinutes >= 1 Then
			' Report minutes
			Return dtInterval.TotalMinutes.ToString("0.00") + " minutes"
		Else
			' Report seconds
			Return dtInterval.TotalSeconds.ToString("0.0") + " seconds"
		End If
	End Function

	Public Shared Function GetRawDataType(ByVal strRawDataType As String) As eRawDataTypeConstants

		If String.IsNullOrEmpty(strRawDataType) Then
			Return eRawDataTypeConstants.Unknown
		End If

		Select Case strRawDataType.ToLower()
			Case RAW_DATA_TYPE_DOT_D_FOLDERS
				Return eRawDataTypeConstants.AgilentDFolder
			Case RAW_DATA_TYPE_ZIPPED_S_FOLDERS
				Return eRawDataTypeConstants.ZippedSFolders
			Case RAW_DATA_TYPE_DOT_RAW_FOLDER
				Return eRawDataTypeConstants.MicromassRawFolder
			Case RAW_DATA_TYPE_DOT_RAW_FILES
				Return eRawDataTypeConstants.ThermoRawFile
			Case RAW_DATA_TYPE_DOT_WIFF_FILES
				Return eRawDataTypeConstants.AgilentQStarWiffFile
			Case RAW_DATA_TYPE_DOT_UIMF_FILES
				Return eRawDataTypeConstants.UIMF
			Case RAW_DATA_TYPE_DOT_MZXML_FILES
				Return eRawDataTypeConstants.mzXML
			Case RAW_DATA_TYPE_DOT_MZML_FILES
				Return eRawDataTypeConstants.mzML
			Case RAW_DATA_TYPE_BRUKER_FT_FOLDER
				Return eRawDataTypeConstants.BrukerFTFolder
			Case RAW_DATA_TYPE_BRUKER_MALDI_SPOT
				Return eRawDataTypeConstants.BrukerMALDISpot
			Case RAW_DATA_TYPE_BRUKER_MALDI_IMAGING
				Return eRawDataTypeConstants.BrukerMALDIImaging
			Case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER
				Return eRawDataTypeConstants.BrukerTOFBaf
			Case Else
				Return eRawDataTypeConstants.Unknown
		End Select

	End Function

	''' <summary>
	''' Lookups up dataset information the data package associated with this analysis job
	''' </summary>
	''' <param name="dctDataPackageJobs"></param>
	''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
	''' <remarks></remarks>
	Protected Function LoadDataPackageJobInfo(ByRef dctDataPackageJobs As Generic.Dictionary(Of Integer, udtDataPackageJobInfoType)) As Boolean

		Dim ConnectionString As String = m_mgrParams.GetParam("brokerconnectionstring")
		Dim DataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

		If DataPackageID < 0 Then
			Return False
		Else
			Return LoadDataPackageJobInfo(ConnectionString, DataPackageID, dctDataPackageJobs)
		End If
	End Function

	''' <summary>
	''' Lookups up dataset information for a data package
	''' </summary>
	''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
	''' <param name="DataPackageID">Data Package ID</param>
	''' <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function LoadDataPackageJobInfo(ByVal ConnectionString As String, DataPackageID As Integer, ByRef dctDataPackageJobs As Generic.Dictionary(Of Integer, udtDataPackageJobInfoType)) As Boolean

		'Requests Dataset information from a data package
		Dim RetryCount As Short = 3
		Dim strMsg As String

		If dctDataPackageJobs Is Nothing Then
			dctDataPackageJobs = New Generic.Dictionary(Of Integer, udtDataPackageJobInfoType)
		Else
			dctDataPackageJobs.Clear()
		End If

		Dim SqlStr As Text.StringBuilder = New Text.StringBuilder

		SqlStr.Append(" SELECT Job, Dataset, DatasetID, Experiment, Tool, ResultType, SettingsFileName, ParameterFileName,")
		SqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,")
		SqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType")
		SqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs")
		SqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())
		SqlStr.Append(" ORDER BY Dataset, Tool")

		Dim Dt As DataTable = Nothing

		'Get a table to hold the results of the query
		While RetryCount > 0
			Try
				Using Cn As System.Data.SqlClient.SqlConnection = New System.Data.SqlClient.SqlConnection(ConnectionString)
					Using Da As System.Data.SqlClient.SqlDataAdapter = New System.Data.SqlClient.SqlDataAdapter(SqlStr.ToString(), Cn)
						Using Ds As DataSet = New DataSet
							Da.Fill(Ds)
							Dt = Ds.Tables(0)
						End Using  'Ds
					End Using  'Da
				End Using  'Cn
				Exit While
			Catch ex As System.Exception
				RetryCount -= 1S
				strMsg = "clsAnalysisResources.LoadDataPackageJobInfo; Exception getting aggregate list from database: " + ex.Message + "; ConnectionString: " + ConnectionString
				strMsg &= ", RetryCount = " + RetryCount.ToString
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
				System.Threading.Thread.Sleep(5000)				'Delay for 5 second before trying again
			End Try
		End While

		'If loop exited due to errors, return false
		If RetryCount < 1 Then
			strMsg = "clsAnalysisResources.LoadDataPackageJobInfo; Excessive failures attempting to retrieve aggregate list from database"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
			Dt.Dispose()
			Return False
		End If

		'Verify at least one row returned
		If Dt.Rows.Count < 1 Then
			' No data was returned
			strMsg = "clsAnalysisResources.LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID.ToString()
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
			Dt.Dispose()
			Return False
		Else
			For Each CurRow As DataRow In Dt.Rows
				Dim udtDataPackageInfo As udtDataPackageJobInfoType = New udtDataPackageJobInfoType

				With udtDataPackageInfo
					.Job = clsGlobal.DbCInt(CurRow("Job"))
					.Dataset = clsGlobal.DbCStr(CurRow("Dataset"))
					.DatasetID = clsGlobal.DbCInt(CurRow("DatasetID"))
					.Experiment = clsGlobal.DbCStr(CurRow("Experiment"))
					.Tool = clsGlobal.DbCStr(CurRow("Tool"))
					.ResultType = clsGlobal.DbCStr(CurRow("ResultType"))
					.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(.ResultType)
					.SettingsFileName = clsGlobal.DbCStr(CurRow("SettingsFileName"))
					.ParameterFileName = clsGlobal.DbCStr(CurRow("ParameterFileName"))
					.OrganismDBName = clsGlobal.DbCStr(CurRow("OrganismDBName"))
					.ProteinCollectionList = clsGlobal.DbCStr(CurRow("ProteinCollectionList"))
					.ProteinOptions = clsGlobal.DbCStr(CurRow("ProteinOptions"))

					If String.IsNullOrWhiteSpace(.ProteinCollectionList) OrElse .ProteinCollectionList = "na" Then
						.LegacyFastaFileName = String.Copy(.OrganismDBName)
					Else
						.LegacyFastaFileName = "na"
					End If

					.ServerStoragePath = clsGlobal.DbCStr(CurRow("ServerStoragePath"))
					.ArchiveStoragePath = clsGlobal.DbCStr(CurRow("ArchiveStoragePath"))
					.ResultsFolderName = clsGlobal.DbCStr(CurRow("ResultsFolder"))
					.DatasetFolderName = clsGlobal.DbCStr(CurRow("DatasetFolder"))
					.SharedResultsFolder = clsGlobal.DbCStr(CurRow("SharedResultsFolder"))
					.RawDataType = clsGlobal.DbCStr(CurRow("RawDataType"))
				End With

				If Not dctDataPackageJobs.ContainsKey(udtDataPackageInfo.Job) Then
					dctDataPackageJobs.Add(udtDataPackageInfo.Job, udtDataPackageInfo)
				End If
			Next

			Dt.Dispose()
			Return True
		End If

	End Function

	Protected Function GetCurrentDatasetAndJobInfo() As udtDataPackageJobInfoType

		Dim udtDataPackageJobInfo As udtDataPackageJobInfoType = New udtDataPackageJobInfoType

		With udtDataPackageJobInfo
			.Job = m_jobParams.GetJobParameter("StepParameters", "Job", 0)
			.Dataset = m_jobParams.GetJobParameter("JobParameters", "DatasetNum", m_DatasetName)
			.DatasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0)
			.Experiment = String.Empty

			.Tool = m_jobParams.GetJobParameter("JobParameters", "ToolName", String.Empty)
			.ResultType = m_jobParams.GetJobParameter("JobParameters", "ResultType", String.Empty)
			.SettingsFileName = m_jobParams.GetJobParameter("JobParameters", "SettingsFileName", String.Empty)

			.ParameterFileName = m_jobParams.GetJobParameter("PeptideSearch", "ParmFileName", String.Empty)

			.LegacyFastaFileName = m_jobParams.GetJobParameter("PeptideSearch", "legacyFastaFileName", String.Empty)
			.OrganismDBName = String.Copy(.LegacyFastaFileName)

			.ProteinCollectionList = m_jobParams.GetJobParameter("PeptideSearch", "ProteinCollectionList", String.Empty)
			.ProteinOptions = m_jobParams.GetJobParameter("PeptideSearch", "ProteinOptions", String.Empty)

			.ServerStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetStoragePath", String.Empty)
			.ArchiveStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetArchivePath", String.Empty)
			.ResultsFolderName = m_jobParams.GetJobParameter("JobParameters", "inputFolderName", String.Empty)
			.DatasetFolderName = m_jobParams.GetJobParameter("JobParameters", "DatasetFolderName", String.Empty)
			.SharedResultsFolder = m_jobParams.GetJobParameter("JobParameters", "SharedResultsFolders", String.Empty)
			.RawDataType = m_jobParams.GetJobParameter("JobParameters", "RawDataType", String.Empty)
		End With

		Return udtDataPackageJobInfo

	End Function

	''' <summary>
	''' Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
	''' </summary>
	''' <param name="udtDataPackageJobInfo"></param>
	''' <returns></returns>
	''' <remarks> Does not override the job number</remarks>
	Protected Function OverrideCurrentDatasetAndJobInfo(ByVal udtDataPackageJobInfo As udtDataPackageJobInfoType) As Boolean

		If String.IsNullOrEmpty(udtDataPackageJobInfo.Dataset) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		' Update job params to have the details for the current dataset
		' This is required so that we can use FindDataFile to find the desired files
		If String.IsNullOrEmpty(udtDataPackageJobInfo.ServerStoragePath) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ServerStoragePath' not defined for job in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If String.IsNullOrEmpty(udtDataPackageJobInfo.ArchiveStoragePath) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If String.IsNullOrEmpty(udtDataPackageJobInfo.ResultsFolderName) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ResultsFolderName' not defined for job in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If String.IsNullOrEmpty(udtDataPackageJobInfo.DatasetFolderName) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'DatasetFolderName' not defined for job in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		With udtDataPackageJobInfo

			m_jobParams.AddDatasetInfo(.Dataset, .DatasetID)
			m_DatasetName = String.Copy(.Dataset)

			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetNum", .Dataset)
			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetID", .DatasetID.ToString())

			m_jobParams.AddAdditionalParameter("JobParameters", "ToolName", .Tool)
			m_jobParams.AddAdditionalParameter("JobParameters", "ResultType", .ResultType)
			m_jobParams.AddAdditionalParameter("JobParameters", "SettingsFileName", .SettingsFileName)

			m_jobParams.AddAdditionalParameter("PeptideSearch", "ParmFileName", .ParameterFileName)

			If String.IsNullOrWhiteSpace(.OrganismDBName) Then
				m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", "na")
			Else
				m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", .OrganismDBName)
			End If

			If String.IsNullOrWhiteSpace(.ProteinCollectionList) OrElse .ProteinCollectionList = "na" Then
				m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", .OrganismDBName)
			Else
				m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", "na")
			End If

			m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinCollectionList", .ProteinCollectionList)
			m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinOptions", .ProteinOptions)

			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetStoragePath", .ServerStoragePath)
			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetArchivePath", .ArchiveStoragePath)
			m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", .ResultsFolderName)
			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetFolderName", .DatasetFolderName)
			m_jobParams.AddAdditionalParameter("JobParameters", "SharedResultsFolders", .SharedResultsFolder)
			m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", .RawDataType)

		End With

		Return True

	End Function

	Protected Sub ResetTimestampForQueueWaitTimeLogging()
		m_LastLockQueueWaitTimeLog = System.DateTime.UtcNow
	End Sub

	''' <summary>
	''' Looks for the specified file in the given folder
	''' If present, returns the full path to the file
	''' If not present, looks for a file named FileName_StoragePathInfo.txt; if that file is found, opens the file and reads the path
	''' If the file isn't found (and the _StoragePathInfo.txt file isn't present), then returns an empty string
	''' </summary>
	''' <param name="FolderPath">The folder to look in</param>
	''' <param name="FileName">The file name to find</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ResolveStoragePath(ByVal FolderPath As String, ByVal FileName As String) As String

		Dim srInFile As System.IO.StreamReader
		Dim strPhysicalFilePath As String = String.Empty
		Dim strFilePath As String

		Dim strLineIn As String

		strFilePath = System.IO.Path.Combine(FolderPath, FileName)

		If System.IO.File.Exists(strFilePath) Then
			' The desired file is located in folder FolderPath
			strPhysicalFilePath = strFilePath
		Else
			' The desired file was not found
			strFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX

			If System.IO.File.Exists(strFilePath) Then
				' The _StoragePathInfo.txt file is present
				' Open that file to read the file path on the first line of the file

				srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

				strLineIn = srInFile.ReadLine
				strPhysicalFilePath = strLineIn

				srInFile.Close()
			End If
		End If

		Return strPhysicalFilePath

	End Function

	''' <summary>
	''' Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working folder
	''' If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
	''' If the file named _StoragePathInfo.txt isn't found, then looks for a ser file in the specified folder
	''' If found, returns the path to the ser file
	''' If not found, then looks for a 0.ser folder in the specified folder
	''' If found, returns the path to the 0.ser folder
	''' Otherwise, returns an empty string
	''' </summary>
	''' <param name="FolderPath">The folder to look in</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ResolveSerStoragePath(ByVal FolderPath As String) As String

		Dim ioFolder As System.IO.DirectoryInfo
		Dim ioFile As System.IO.FileInfo

		Dim srInFile As System.IO.StreamReader
		Dim strPhysicalFilePath As String = String.Empty
		Dim strFilePath As String

		Dim strLineIn As String

		strFilePath = System.IO.Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX)

		If System.IO.File.Exists(strFilePath) Then
			' The desired file is located in folder FolderPath
			' The _StoragePathInfo.txt file is present
			' Open that file to read the file path on the first line of the file

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

			strLineIn = srInFile.ReadLine
			strPhysicalFilePath = strLineIn

			srInFile.Close()
		Else
			' The desired file was not found

			' Look for a ser file in the dataset folder
			strPhysicalFilePath = System.IO.Path.Combine(FolderPath, BRUKER_SER_FILE)
			ioFile = New System.IO.FileInfo(strPhysicalFilePath)

			If Not ioFile.Exists Then
				' See if a folder named 0.ser exists in FolderPath
				strPhysicalFilePath = System.IO.Path.Combine(FolderPath, BRUKER_ZERO_SER_FOLDER)
				ioFolder = New System.IO.DirectoryInfo(strPhysicalFilePath)
				If Not ioFolder.Exists Then
					strPhysicalFilePath = ""
				End If
			End If

		End If

		Return strPhysicalFilePath

	End Function


	Public Function RetrieveAggregateFiles(ByVal FilesToRetrieveExt As String()) As Boolean

		Dim dctDataPackageJobs As Generic.Dictionary(Of Integer, udtDataPackageJobInfoType) = Nothing

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		Dim SourceFolderPath As String = "??"
		Dim SourceFilename As String = "??"
		Dim SplitString As String()
		Dim FilterValue As String

		Dim blnSuccess As Boolean = False

		Try
			If Not LoadDataPackageJobInfo(dctDataPackageJobs) Then
				m_message = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
				Return False
			End If
		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex)
			Return False
		End Try

		Try
			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			For Each udtItem As Generic.KeyValuePair(Of Integer, udtDataPackageJobInfoType) In dctDataPackageJobs

				If Not OverrideCurrentDatasetAndJobInfo(udtItem.Value) Then
					Return False
				End If

				FilterValue = udtItem.Value.SettingsFileName + udtItem.Value.ParameterFileName

				For Each FileNameExt As String In FilesToRetrieveExt
					SplitString = FileNameExt.Split(":"c)
					SourceFilename = udtItem.Value.Dataset + SplitString(1)
					If SplitString(0).ToLower() = udtItem.Value.Tool.ToLower() Then
						SourceFolderPath = FindDataFile(SourceFilename)
						If Not CopyFileToWorkDir(SourceFilename, SourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
							m_message = "CopyFileToWorkDir returned False for " + SourceFilename + " using folder " + SourceFolderPath
							If m_DebugLevel >= 1 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							End If
							Return False
						Else
							If m_DebugLevel >= 1 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " + SourceFilename + " from folder " + SourceFolderPath)
							End If
						End If

						If SourceFilename.ToLower.Contains(".zip") Then
							'Unzip file
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file: " + SourceFilename)
							If UnzipFileStart(System.IO.Path.Combine(m_WorkingDir, SourceFilename), m_WorkingDir, "clsAnalysisResources.RetrieveAggregateFiles", False) Then
								If m_DebugLevel >= 1 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
								End If
								m_jobParams.AddResultFileExtensionToSkip(SourceFilename)
								RetrieveAggregateFilesRename(m_WorkingDir, System.IO.Path.GetFileNameWithoutExtension(SourceFilename) + ".txt", FilterValue, SplitString(2))
							Else
								Return False
							End If

						End If

						'Rename the files where dataset name will cause collisions
						RetrieveAggregateFilesRename(m_WorkingDir, SourceFilename, FilterValue, SplitString(2))
					End If
				Next
			Next

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			blnSuccess = True

		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveAggregateFiles; Exception during copy of file: " + SourceFilename + " from folder " + SourceFolderPath, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Private Function RetrieveAggregateFilesRename(ByVal workDir As String, _
	 ByVal SourceFilename As String, _
	 ByVal filterValue As String, _
	 ByVal SaveFile As String) As Boolean
		Dim newFilename As String = ""
		Dim ext As String = ""
		Dim filenameNoExt As String = "'"

		Try
			Select Case m_jobParams.GetParam("StepTool").ToLower
				Case "phospho_fdr_aggregator"
					Dim fi As New System.IO.FileInfo(System.IO.Path.Combine(workDir, SourceFilename))
					ext = System.IO.Path.GetExtension(SourceFilename)
					filenameNoExt = System.IO.Path.GetFileNameWithoutExtension(SourceFilename)

					If filterValue.ToLower.Contains("_hcd") Then
						newFilename = filenameNoExt + "_hcd" + ext

					ElseIf filterValue.ToLower.Contains("_etd") Then
						newFilename = filenameNoExt + "_etd" + ext

					ElseIf filterValue.ToLower.Contains("_cid") Then
						newFilename = filenameNoExt + "_cid" + ext

					Else
						newFilename = SourceFilename
					End If

					If newFilename <> SourceFilename Then
						Dim intRetryCount As Integer
						Dim blnSuccess As Boolean = False
						Dim strExceptionMsg As String = "unknown reason"

						Do
							Try
								fi.MoveTo(System.IO.Path.Combine(workDir, newFilename))
								blnSuccess = True
							Catch ex As System.IO.IOException
								intRetryCount += 1
								If intRetryCount = 1 Then
									strExceptionMsg = String.Copy(ex.Message)
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to rename file " + fi.Name + " in folder " + workDir + "; will retry after garbage collection")
									PRISM.Processes.clsProgRunner.GarbageCollectNow()
									System.Threading.Thread.Sleep(1000)
								End If
							End Try
						Loop While Not blnSuccess And intRetryCount <= 1

						If Not blnSuccess Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMgrSettings.RetrieveAggregateFilesRename; Unable to rename file" + fi.Name + " to " + newFilename + " in folder " + workDir + ": " + strExceptionMsg)
							Return False
						End If
					End If

					If SaveFile.ToLower = "nocopy" Then
						m_jobParams.AddResultFileExtensionToSkip(newFilename)
					End If

					Return True

			End Select

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMgrSettings.RetrieveAggregateFilesRename; Exception during renaming of file: " + newFilename + " from folder " + workDir, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Function RetrieveDataPackagePeptideHitJobInfo(ByRef DataPackageID As Integer) As Generic.List(Of udtDataPackageJobInfoType)

		Dim ConnectionString As String = m_mgrParams.GetParam("brokerconnectionstring")
		DataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1)

		If DataPackageID < 0 Then
			m_message = "DataPackageID is not defined for this analysis job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
			Return New Generic.List(Of udtDataPackageJobInfoType)
		Else
			Return RetrieveDataPackagePeptideHitJobInfo(ConnectionString, DataPackageID)
		End If

	End Function

	Public Shared Function RetrieveDataPackagePeptideHitJobInfo(ByVal ConnectionString As String, ByVal DataPackageID As Integer) As Generic.List(Of udtDataPackageJobInfoType)

		Dim lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType)
		Dim dctDataPackageJobs As Generic.Dictionary(Of Integer, udtDataPackageJobInfoType)

		Dim strMsg As String

		' This list tracks the info for the jobs associated with this aggregation job's data package
		lstDataPackagePeptideHitJobs = New Generic.List(Of udtDataPackageJobInfoType)

		' This dictionary will track the jobs associated with this aggregation job's data package
		' Key is job number, value is an instance of udtDataPackageJobInfoType
		dctDataPackageJobs = New Generic.Dictionary(Of Integer, udtDataPackageJobInfoType)

		Try
			If Not LoadDataPackageJobInfo(ConnectionString, DataPackageID, dctDataPackageJobs) Then
				strMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMsg)
				Return lstDataPackagePeptideHitJobs
			End If
		Catch ex As System.Exception
			strMsg = "Exception calling LoadDataPackageJobInfo"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveDataPackagePeptideHitJobPHRPFiles; " & strMsg, ex)
			Return lstDataPackagePeptideHitJobs
		End Try

		Try
			For Each kvItem As Generic.KeyValuePair(Of Integer, udtDataPackageJobInfoType) In dctDataPackageJobs

				If kvItem.Value.PeptideHitResultType <> clsPHRPReader.ePeptideHitResultType.Unknown Then
					' Cache this job info in lstDataPackagePeptideHitJobs
					lstDataPackagePeptideHitJobs.Add(kvItem.Value)

				End If

			Next

		Catch ex As System.Exception
			strMsg = "Exception determining data package jobs for this aggregation job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveDataPackagePeptideHitJobInfo; " & strMsg, ex)
		End Try

		Return lstDataPackagePeptideHitJobs

	End Function

	''' <summary>
	''' Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
	''' </summary>
	''' <param name="blnRetrieveMzXMLFile">Set to True to retrieve the mzXML file (will create it from the .Raw file if the mzXML file wasn't found)</param>
	''' <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function RetrieveDataPackagePeptideHitJobPHRPFiles(ByVal blnRetrieveMzXMLFile As Boolean, ByRef lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType)) As Boolean

		Dim SourceFolderPath As String = "??"
		Dim SourceFilename As String = "??"
		Dim TargetFolderPath As String
		Dim DataPackageID As Integer = 0

		Dim blnFileCopied As Boolean
		Dim blnSuccess As Boolean = False
		Dim blnPrefixRequired As Boolean

		Dim dctInstrumentDataToRetrieve As Generic.Dictionary(Of udtDataPackageJobInfoType, String)

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		' This list tracks the info for the jobs associated with this aggregation job's data package
		If lstDataPackagePeptideHitJobs Is Nothing Then
			lstDataPackagePeptideHitJobs = New Generic.List(Of udtDataPackageJobInfoType)
		Else
			lstDataPackagePeptideHitJobs.Clear()
		End If

		' The keys in this dictionary are udtJobInfo entries; the values will be the path to the mzXML file, or an empty string if the .Raw file needs to be retrieved
		dctInstrumentDataToRetrieve = New Generic.Dictionary(Of udtDataPackageJobInfoType, String)

		Try
			lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(DataPackageID)

			If lstDataPackagePeptideHitJobs.Count = 0 Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Did not find any peptide hit jobs associated with this job's data package ID (" & DataPackageID & ")"
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
				Return False
			End If

		Catch ex As System.Exception
			m_message = "Exception calling RetrieveDataPackagePeptideHitJobInfo"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveDataPackagePeptideHitJobPHRPFiles; " & m_message, ex)
			Return False
		End Try

		Try

			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			For Each udtJobInfo As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs

				If Not OverrideCurrentDatasetAndJobInfo(udtJobInfo) Then
					' Error message has already been logged
					Return False
				End If

				If udtJobInfo.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PeptideHit ResultType not recognized for job " & udtJobInfo.Job & ": " & udtJobInfo.ResultType.ToString())
				Else
					Dim lstFilesToGet As Generic.List(Of String) = New Generic.List(Of String)
					Dim strSynopsisFileName As String
					Dim strSynopsisMSGFFileName As String
					Dim eLogMsgTypeIfNotFound As clsLogTools.LogLevels

					strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset)
					lstFilesToGet.Add(strSynopsisFileName)

					lstFilesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset))
					lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset))
					lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset))
					lstFilesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset))

					strSynopsisMSGFFileName = clsPHRPReader.GetMSGFFileName(strSynopsisFileName)
					lstFilesToGet.Add(strSynopsisMSGFFileName)

					SourceFolderPath = String.Empty

					For Each SourceFilename In lstFilesToGet

						If String.IsNullOrEmpty(SourceFolderPath) Then
							' Only use FindDataFile() for the first file in lstFilesToGet; we will assume the other files are in that folder
							SourceFolderPath = FindDataFile(SourceFilename)
						End If

						If SourceFilename = strSynopsisFileName Then
							' Check whether a synopsis file by this name has already been copied locally
							' If it has, then we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
							If IO.File.Exists(IO.Path.Combine(m_WorkingDir, SourceFilename)) Then
								blnPrefixRequired = True
							Else
								blnPrefixRequired = False
							End If
						End If

						If blnPrefixRequired Then
							TargetFolderPath = IO.Path.Combine(m_WorkingDir, "FileRename")
							If Not IO.Directory.Exists(TargetFolderPath) Then
								IO.Directory.CreateDirectory(TargetFolderPath)
							End If
						Else
							TargetFolderPath = String.Copy(m_WorkingDir)
						End If

						If SourceFilename = strSynopsisMSGFFileName Then
							' It's OK if the MSGF file doesn't exist
							eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG
						Else
							eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR
						End If

						blnFileCopied = CopyFileToWorkDir(SourceFilename, SourceFolderPath, TargetFolderPath, eLogMsgTypeIfNotFound)

						If Not blnFileCopied Then

							If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
								m_message = "CopyFileToWorkDir returned False for " + SourceFilename + " using folder " + SourceFolderPath
								If m_DebugLevel >= 1 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
								End If
								Return False
							End If

						Else
							If m_DebugLevel > 1 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " + SourceFilename + " from folder " + SourceFolderPath)
							End If

							If blnPrefixRequired Then
								Try
									Dim fiFileToRename As System.IO.FileInfo = New System.IO.FileInfo(IO.Path.Combine(TargetFolderPath, SourceFilename))
									Dim strFilePathWithPrefix As String = IO.Path.Combine(m_WorkingDir, "Job" & udtJobInfo.Job.ToString() & "_" & fiFileToRename.Name)

									Threading.Thread.Sleep(100)
									fiFileToRename.MoveTo(strFilePathWithPrefix)

									m_jobParams.AddResultFileToSkip(IO.Path.GetFileName(strFilePathWithPrefix))

								Catch ex As Exception
									m_message = "Exception renaming PHRP file " & SourceFilename & " for job " & udtJobInfo.Job & " (data package has multiple jobs for the same dataset)"
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
									Return False
								End Try

							Else
								m_jobParams.AddResultFileToSkip(SourceFilename)
							End If
						End If

					Next

					Dim strMzXMLFilePath As String = String.Empty

					' See if a .mzXML file already exists for this dataset
					strMzXMLFilePath = FindMZXmlFile()

					If String.IsNullOrEmpty(strMzXMLFilePath) Then
						' mzXML file not found
						If udtJobInfo.RawDataType = RAW_DATA_TYPE_DOT_RAW_FILES Then
							' Will need to retrieve the .Raw file for this dataset
							dctInstrumentDataToRetrieve.Add(udtJobInfo, String.Empty)
						Else
							m_message = "mzXML file not found for dataset " & udtJobInfo.Dataset & " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							Return False
						End If
					Else
						dctInstrumentDataToRetrieve.Add(udtJobInfo, strMzXMLFilePath)
					End If

				End If

			Next


			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			If blnRetrieveMzXMLFile Then
				blnSuccess = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve)
			Else
				blnSuccess = True
			End If


		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveDataPackagePeptideHitJobPHRPFiles; Exception during copy of file: " + SourceFilename + " from folder " + SourceFolderPath, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Protected Function RetrieveDataPackageMzXMLFiles(ByVal dctInstrumentDataToRetrieve As Generic.Dictionary(Of udtDataPackageJobInfoType, String)) As Boolean

		Dim blnSuccess As Boolean
		Dim intCurrentJob As Integer
		Dim lstDatasetsProcessed As Generic.SortedSet(Of String)

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		Try

			' Make sure we don't move the .mzXML file into the results folder
			m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)			' Raw file
			m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION)			' mzXML file

			lstDatasetsProcessed = New Generic.SortedSet(Of String)

			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			' All of the PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
			For Each kvItem As Generic.KeyValuePair(Of udtDataPackageJobInfoType, String) In dctInstrumentDataToRetrieve

				Dim strMzXMLFilePath As String = kvItem.Value
				intCurrentJob = kvItem.Key.Job

				' Skip this dataset if we already processed it
				If Not lstDatasetsProcessed.Contains(kvItem.Key.Dataset) Then

					If Not OverrideCurrentDatasetAndJobInfo(kvItem.Key) Then
						' Error message has already been logged
						Return False
					End If

					If String.IsNullOrEmpty(strMzXMLFilePath) Then
						blnSuccess = False
					Else
						' mzXML file exists; try to retrieve it
						blnSuccess = RetrieveMZXmlFileUsingSourceFile(m_WorkingDir, False, strMzXMLFilePath)
					End If

					If blnSuccess Then
						' .mzXML file found and copied locally
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied .mzXML file for job " & intCurrentJob & ": " & strMzXMLFilePath)
						End If
					Else
						' .mzXML file not found
						' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
						If Not RetrieveSpectra(kvItem.Key.RawDataType, m_WorkingDir) Then
							m_message = "Error occurred retrieving instrument data file for job " & intCurrentJob
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "RetrieveDataPackageMzXMLFiles, " & m_message)
							Return False
						End If
					End If

					lstDatasetsProcessed.Add(kvItem.Key.Dataset)
				End If
			Next

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			blnSuccess = True

		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.RetrieveDataPackageMzXMLFiles; Exception retrieving mzXML file or .Raw file for job " & intCurrentJob, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Retrieves file PNNLOmicsElementData.xml from the program directory of the program specified by strProgLocName
	''' </summary>
	''' <param name="strProgLocName"></param>
	''' <returns></returns>
	''' <remarks>strProgLocName is tyipcally DeconToolsProgLoc, LipidToolsProgLoc, or TargetedWorkflowsProgLoc</remarks>
	Protected Function RetrievePNNLOmicsResourceFiles(ByVal strProgLocName As String) As Boolean
		Const OMICS_ELEMENT_DATA_FILE As String = "PNNLOmicsElementData.xml"

		' Copy the PNNLOmicsElementData.xml file to the working directory
		Dim strProgLoc As String
		Dim fiSourceFile As System.IO.FileInfo

		Try
			strProgLoc = m_mgrParams.GetParam(strProgLocName)
			If String.IsNullOrEmpty(strProgLocName) Then
				m_message = "Manager parameter " + strProgLocName + " is not defined; cannot retrieve file " & OMICS_ELEMENT_DATA_FILE
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			fiSourceFile = New System.IO.FileInfo(IO.Path.Combine(strProgLoc, OMICS_ELEMENT_DATA_FILE))
			
			If Not fiSourceFile.Exists Then
				m_message = "PNNLOmics Element Data file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " at: " & fiSourceFile.FullName)
				Return False
			End If

			fiSourceFile.CopyTo(IO.Path.Combine(m_WorkingDir, OMICS_ELEMENT_DATA_FILE))

		Catch ex As Exception
			m_message = "Error copying " & OMICS_ELEMENT_DATA_FILE
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " to working directory: " + ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
	''' </summary>
	''' <param name="RawDataType">Type of data to copy</param>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveSpectra(ByVal RawDataType As String, ByVal WorkDir As String) As Boolean
		Return RetrieveSpectra(RawDataType, WorkDir, False)
	End Function

	''' <summary>
	''' Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <param name="FileExtension">File extension to match; must contain a period, for example ".raw"</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDatasetFile(ByVal WorkDir As String, _
	ByVal FileExtension As String, _
	ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim DataFileName As String = m_DatasetName + FileExtension
		Dim DSFolderPath As String = FindValidFolder(m_DatasetName, DataFileName)

		If CopyFileToWorkDir(DataFileName, DSFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
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
	Protected Function RetrieveMgfFile(ByVal WorkDir As String, _
	  ByVal GetCdfAlso As Boolean, _
	  ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		'Data files are in a subfolder off of the main dataset folder
		'Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

		Dim ServerPath As String = FindValidFolder(m_DatasetName, "", "*" + DOT_D_EXTENSION)

		Dim DSFolders() As String
		Dim DSFiles() As String = Nothing
		Dim DumFolder As String
		Dim FileFound As Boolean = False
		Dim DataFolderPath As String = ""

		'Get a list of the subfolders in the dataset folder
		DSFolders = System.IO.Directory.GetDirectories(ServerPath)
		'Go through the folders looking for a file with a ".mgf" extension
		For Each DumFolder In DSFolders
			If FileFound Then Exit For
			DSFiles = System.IO.Directory.GetFiles(DumFolder, "*" + DOT_MGF_EXTENSION)
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
		If Not CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then Return False

		'If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
		If Not GetCdfAlso Then Return True

		DSFiles = System.IO.Directory.GetFiles(DataFolderPath, "*" + DOT_CDF_EXTENSION)
		If DSFiles.GetLength(0) <> 1 Then
			'Incorrect number of .cdf files found
			Return False
		End If

		'Copy the .cdf file that was found
		If CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Looks for this dataset's mzXML file
	''' Hard-coded to look for certain folders with names like MSXML_Gen_1_154_DatasetID, MSXML_Gen_1_93_DatasetID, or MSXML_Gen_1_39_DatasetID (plus a few others)
	''' If the MSXML folder (or the .mzXML file) cannot be found, then returns False
	''' </summary>
	''' <param name="WorkDir"></param>
	''' <param name="CreateStoragePathInfoOnly"></param>
	''' <param name="SourceFilePath">Returns the full path to the file that was retrieved</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveMZXmlFile(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean, ByRef SourceFilePath As String) As Boolean

		SourceFilePath = FindMZXmlFile()

		If String.IsNullOrEmpty(SourceFilePath) Then
			Return False
		Else
			Return RetrieveMZXmlFileUsingSourceFile(WorkDir, CreateStoragePathInfoOnly, SourceFilePath)
		End If

	End Function

	Protected Function RetrieveMZXmlFileUsingSourceFile(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean, ByVal SourceFilePath As String) As Boolean

		Dim fiSourceFile As System.IO.FileInfo

		fiSourceFile = New System.IO.FileInfo(SourceFilePath)

		If fiSourceFile.Exists Then
			If CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
				Return True
			End If
		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MzXML file not found; will need to generate it: " + fiSourceFile.Name)
		End If

		Return False

	End Function

	''' <summary>
	''' Looks for this dataset's ScanStats files (previously created by MASIC)
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanStatsFiles(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim RetrieveSICStatsFile As Boolean = False
		Return RetrieveScanAndSICStatsFiles(WorkDir, RetrieveSICStatsFile, CreateStoragePathInfoOnly)

	End Function

	''' <summary>
	''' Looks for this dataset's MASIC results files
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(ByVal WorkDir As String, ByVal RetrieveSICStatsFile As Boolean, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim ServerPath As String
		Dim ScanStatsFilename As String

		Dim MaxRetryCount As Integer = 1

		' Look for the MASIC Results folder
		' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
		ScanStatsFilename = m_DatasetName + SCAN_STATS_FILE_SUFFIX
		ServerPath = FindValidFolder(m_DatasetName, "", "SIC*", MaxRetryCount, False)

		If String.IsNullOrEmpty(ServerPath) Then
			m_message = "Dataset folder path not defined"
		Else

			Dim diFolderInfo As System.IO.DirectoryInfo
			diFolderInfo = New System.IO.DirectoryInfo(ServerPath)

			If Not diFolderInfo.Exists Then
				m_message = "Dataset folder with MASIC files not found: " + diFolderInfo.FullName
			Else

				'See if the ServerPath folder actually contains a subfolder that starts with "SIC"
				Dim diSubfolders() As System.IO.DirectoryInfo = diFolderInfo.GetDirectories("SIC*")
				If diSubfolders.Length = 0 Then
					m_message = "Dataset folder does not contain any MASIC results folders: " + diFolderInfo.FullName
				Else
					' MASIC Results Folder Found
					' If more than one folder, then use the folder with the newest _ScanStats.txt file
					Dim diSourceFile As System.IO.FileInfo
					Dim dtNewestScanStatsFileDate As System.DateTime
					Dim strNewestScanStatsFilePath As String = String.Empty

					For Each diSubFolder As System.IO.DirectoryInfo In diSubfolders
						diSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(diSubFolder.FullName, ScanStatsFilename))
						If diSourceFile.Exists Then
							If String.IsNullOrEmpty(strNewestScanStatsFilePath) OrElse diSourceFile.LastWriteTimeUtc > dtNewestScanStatsFileDate Then
								strNewestScanStatsFilePath = diSourceFile.FullName
								dtNewestScanStatsFileDate = diSourceFile.LastWriteTimeUtc
							End If
						End If
					Next

					If String.IsNullOrEmpty(strNewestScanStatsFilePath) Then
						m_message = "MASIC ScanStats file not found below " + diFolderInfo.FullName
					Else
						Return RetrieveScanAndSICStatsFiles(WorkDir, System.IO.Path.GetDirectoryName(strNewestScanStatsFilePath), RetrieveSICStatsFile, CreateStoragePathInfoOnly)
					End If
				End If
			End If
		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "RetrieveScanAndSICStatsFiles: " + m_message)
		End If

		Return False

	End Function

	''' <summary>
	''' 
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="MASICResultsFolderPath">Source folder to copy files from</param>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(ByVal WorkDir As String, ByVal MASICResultsFolderPath As String, ByVal RetrieveSICStatsFile As Boolean, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim ScanStatsFilename As String

		Dim MaxRetryCount As Integer = 1

		' Copy the MASIC files from the MASIC results folder
		ScanStatsFilename = m_DatasetName + SCAN_STATS_FILE_SUFFIX

		If String.IsNullOrEmpty(MASICResultsFolderPath) Then
			m_message = "MASIC Results folder path not defined"
		Else

			Dim diFolderInfo As System.IO.DirectoryInfo
			Dim diSourceFile As System.IO.FileInfo
			diFolderInfo = New System.IO.DirectoryInfo(MASICResultsFolderPath)

			If Not diFolderInfo.Exists Then
				m_message = "MASIC Results folder not found: " + diFolderInfo.FullName
			Else

				diSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(MASICResultsFolderPath, ScanStatsFilename))

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying ScanStats.txt file: " + diSourceFile.FullName)
				End If

				If Not CopyFileToWorkDir(diSourceFile.Name, diSourceFile.Directory.FullName, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
					m_message = SCAN_STATS_FILE_SUFFIX + " file not found at " + diSourceFile.Directory.FullName
				Else

					' ScanStats File successfully copied
					' Also look for and copy the _ScanStatsEx.txt file

					If Not CopyFileToWorkDir(m_DatasetName + SCAN_STATS_EX_FILE_SUFFIX, diSourceFile.Directory.FullName, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
						m_message = "_ScanStatsEx.txt file not found at " + diSourceFile.Directory.FullName
					Else

						' ScanStatsEx file successfully copied

						If RetrieveSICStatsFile Then

							' Also look for and copy the _SICStats.txt file
							If Not CopyFileToWorkDir(m_DatasetName + "_SICStats.txt", diSourceFile.Directory.FullName, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
								m_message = "_SICStats.txt file not found at " + diSourceFile.Directory.FullName
							Else
								' All files successfully copied
								Return True
							End If

						Else
							' All files successfully copied
							Return True
						End If

					End If
				End If

			End If

		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveScanAndSICStatsFiles: " + m_message)
		End If

		Return False

	End Function


	''' <summary>
	''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
	''' </summary>
	''' <param name="RawDataType">Type of data to copy</param>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt, and this file's first line will be the full path to the spectrum file (or spectrum folder)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveSpectra(ByVal RawDataType As String, _
	  ByVal WorkDir As String, _
	  ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim blnSuccess As Boolean = False
		Dim StoragePath As String = m_jobParams.GetParam("DatasetStoragePath")
		Dim eRawDataType As eRawDataTypeConstants

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving spectra file(s)")

		eRawDataType = GetRawDataType(RawDataType)
		Select Case eRawDataType
			Case eRawDataTypeConstants.AgilentDFolder			'Agilent ion trap data

				If StoragePath.ToLower.Contains("Agilent_SL1".ToLower) OrElse _
				   StoragePath.ToLower.Contains("Agilent_XCT1".ToLower) Then
					' For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005, 
					'  we would pre-process the data beforehand to create MGF files
					' The following call can be used to retrieve the files
					blnSuccess = RetrieveMgfFile(WorkDir, True, CreateStoragePathInfoOnly)
				Else
					' DeconTools_V2 now supports reading the .D files directly
					' Call RetrieveDotDFolder() to copy the folder and all subfolders
					blnSuccess = RetrieveDotDFolder(WorkDir, CreateStoragePathInfoOnly, blnSkipBAFFiles:=True)
				End If

			Case eRawDataTypeConstants.AgilentQStarWiffFile			'Agilent/QSTAR TOF data
				blnSuccess = RetrieveDatasetFile(WorkDir, DOT_WIFF_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.ZippedSFolders			'FTICR data
				blnSuccess = RetrieveSFolders(WorkDir, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.ThermoRawFile			'Finnigan ion trap/LTQ-FT data
				blnSuccess = RetrieveDatasetFile(WorkDir, DOT_RAW_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.MicromassRawFolder			'Micromass QTOF data
				blnSuccess = RetrieveDotRawFolder(WorkDir, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.UIMF			'IMS UIMF data
				blnSuccess = RetrieveDatasetFile(WorkDir, DOT_UIMF_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.mzXML
				blnSuccess = RetrieveDatasetFile(WorkDir, DOT_MZXML_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.mzML
				blnSuccess = RetrieveDatasetFile(WorkDir, DOT_MZML_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
				' Call RetrieveDotDFolder() to copy the folder and all subfolders

				' Both the MSXml step tool and DeconTools require the .Baf file
				' We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, we need the file
				Dim blnSkipBAFFiles As Boolean
				blnSkipBAFFiles = False

				blnSuccess = RetrieveDotDFolder(WorkDir, CreateStoragePathInfoOnly, blnSkipBAFFiles)

			Case eRawDataTypeConstants.BrukerMALDIImaging
				blnSuccess = RetrieveBrukerMALDIImagingFolders(WorkDir, UnzipOverNetwork:=True)

			Case Else
				' RawDataType is not recognized or not supported by this function
				If eRawDataType = eRawDataTypeConstants.Unknown Then
					m_message = "Invalid data type specified: " + RawDataType
				Else
					m_message = "Data type " + RawDataType + " is not supported by the RetrieveSpectra function"
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
		End Select

		'Return the result of the spectra retrieval
		Return blnSuccess

	End Function

	''' <summary>
	''' Retrieves an Agilent or Bruker .D folder for the analysis job in progress
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotDFolder(ByVal WorkDir As String, _
	  ByVal CreateStoragePathInfoOnly As Boolean, _
	  ByVal blnSkipBAFFiles As Boolean) As Boolean
		Dim objFileNamesToSkip As List(Of String)

		objFileNamesToSkip = New List(Of String)
		If blnSkipBAFFiles Then
			objFileNamesToSkip.Add("analysis.baf")
		End If

		Return RetrieveDotXFolder(WorkDir, DOT_D_EXTENSION, CreateStoragePathInfoOnly, objFileNamesToSkip)
	End Function

	''' <summary>
	''' Retrieves a Micromass .raw folder for the analysis job in progress
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotRawFolder(ByVal WorkDir As String, _
	 ByVal CreateStoragePathInfoOnly As Boolean) As Boolean
		Return RetrieveDotXFolder(WorkDir, DOT_RAW_EXTENSION, CreateStoragePathInfoOnly, New List(Of String))
	End Function


	''' <summary>
	''' Retrieves a folder with a name like Dataset.D or Dataset.Raw
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <param name="FolderExtension">Extension on the folder; for example, ".D"</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotXFolder(ByVal WorkDir As String, _
	  ByVal FolderExtension As String, _
	  ByVal CreateStoragePathInfoOnly As Boolean, _
	  ByVal objFileNamesToSkip As List(Of String)) As Boolean

		'Copies a data folder ending in FolderExtension to the working directory

		If Not FolderExtension.StartsWith(".") Then
			FolderExtension = "." + FolderExtension
		End If
		Dim FolderExtensionWildcard As String = "*" + FolderExtension

		Dim ServerPath As String = FindValidFolder(m_DatasetName, "", FolderExtensionWildcard)
		Dim DestFolderPath As String

		'Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
		Dim RemFolders() As String = System.IO.Directory.GetDirectories(ServerPath, FolderExtensionWildcard)
		If RemFolders.GetLength(0) <> 1 Then Return False

		'Set up the file paths
		Dim DSFolderPath As String = System.IO.Path.Combine(ServerPath, RemFolders(0))

		'Do the copy
		Try
			DestFolderPath = System.IO.Path.Combine(WorkDir, m_DatasetName + FolderExtension)

			If CreateStoragePathInfoOnly Then
				If Not System.IO.Directory.Exists(DSFolderPath) Then
					m_message = "Source folder not found: " + DSFolderPath
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				Else
					CreateStoragePathInfoFile(DSFolderPath, DestFolderPath)
				End If
			Else
				' Copy the directory and all subdirectories
				' Skip any files defined by objFileNamesToSkip
				ResetTimestampForQueueWaitTimeLogging()
				m_FileTools.CopyDirectory(DSFolderPath, DestFolderPath, objFileNamesToSkip)
			End If

		Catch ex As Exception
			m_message = "Error copying folder " + DSFolderPath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " to working directory: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try

		' If we get here, all is fine
		Return True

	End Function

	''' <summary>
	''' Retrieves a data from a Bruker MALDI imaging dataset
	''' The data is stored as zip files with names like 0_R00X433.zip
	''' This data is unzipped into a subfolder in the Chameleon cached data folder
	''' </summary>
	''' <param name="WorkDir">Work directory for this manager; only used if UnzipOverNetwork is false</param>
	''' <param name="UnzipOverNetwork"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function RetrieveBrukerMALDIImagingFolders(ByVal WorkDir As String, _
	  ByVal UnzipOverNetwork As Boolean) As Boolean

		Const ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK As String = "*R*X*.zip"

		Dim ChameleonCachedDataFolder As String = m_mgrParams.GetParam("ChameleonCachedDataFolder")
		Dim diCachedDataFolder As System.IO.DirectoryInfo

		Dim ServerPath As String
		Dim strUnzipFolderPathBase As String = String.Empty

		Dim strFilesToDelete As New System.Collections.Generic.Queue(Of String)

		Dim strZipFilePathRemote As String = String.Empty
		Dim strZipFilePathToExtract As String

		Dim blnUnzipFile As Boolean

		Dim blnApplySectionFilter As Boolean
		Dim StartSectionX As Integer
		Dim EndSectionX As Integer

		Dim CoordR As Integer, CoordX As Integer, CoordY As Integer

		Try

			If String.IsNullOrEmpty(ChameleonCachedDataFolder) Then
				m_message = "Chameleon cached data folder not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to unzip MALDI imaging data")
				Return False
			Else
				' Delete any subfolders at ChameleonCachedDataFolder that do not have this dataset's name
				diCachedDataFolder = New System.IO.DirectoryInfo(ChameleonCachedDataFolder)
				If Not diCachedDataFolder.Exists Then
					m_message = "Chameleon cached data folder does not exist"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + diCachedDataFolder.FullName)
					Return False
				Else
					strUnzipFolderPathBase = System.IO.Path.Combine(diCachedDataFolder.FullName, m_DatasetName)
				End If

				For Each diSubFolder As System.IO.DirectoryInfo In diCachedDataFolder.GetDirectories()
					If diSubFolder.Name.ToLower <> m_DatasetName.ToLower Then
						' Delete this directory
						Try
							If m_DebugLevel >= 2 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting old dataset subfolder from chameleon cached data folder: " + diSubFolder.FullName)
							End If

							If m_mgrParams.GetParam("MgrName").ToLower.Contains("monroe") Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " Skipping delete since this is a development computer")
							Else
								diSubFolder.Delete(True)
							End If

						Catch ex As Exception
							m_message = "Error deleting cached subfolder"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " " + diSubFolder.FullName + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
							Return False
						End Try
					End If
				Next

				' Delete any .mis files that do not start with this dataset's name
				For Each fiFile As System.IO.FileInfo In diCachedDataFolder.GetFiles("*.mis")
					If System.IO.Path.GetFileNameWithoutExtension(fiFile.Name).ToLower <> m_DatasetName.ToLower Then
						fiFile.Delete()
					End If
				Next
			End If

		Catch ex As Exception
			m_message = "Error cleaning out old data from the Chameleon cached data folder"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try

		' See if any imaging section filters are defined
		blnApplySectionFilter = GetBrukerImagingSectionFilter(m_jobParams, StartSectionX, EndSectionX)

		' Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
		' If a matching folder isn't found, then ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"
		ServerPath = FindValidFolder(m_DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK)

		Try

			Dim MisFiles() As String
			Dim strImagingSeqFilePathFinal As String

			' Look for the .mis file (ImagingSequence file) 
			strImagingSeqFilePathFinal = System.IO.Path.Combine(diCachedDataFolder.FullName, m_DatasetName + ".mis")

			If Not System.IO.File.Exists(strImagingSeqFilePathFinal) Then

				' Copy the .mis file (ImagingSequence file) over from the storage server
				MisFiles = System.IO.Directory.GetFiles(ServerPath, "*.mis")

				If MisFiles.Length = 0 Then
					' No .mis files were found; unable to continue
					m_message = "ImagingSequence (.mis) file not found in dataset folder"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to process MALDI imaging data")
					Return False
				Else
					' We'll copy the first file in MisFiles(0)
					' Log a warning if we will be renaming the file

					If System.IO.Path.GetFileName(MisFiles(0)).ToLower <> System.IO.Path.GetFileName(strImagingSeqFilePathFinal).ToLower() Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Note: Renaming .mis file (ImagingSequence file) from " + System.IO.Path.GetFileName(MisFiles(0)) + " to " + System.IO.Path.GetFileName(strImagingSeqFilePathFinal))
					End If

					If Not CopyFileWithRetry(MisFiles(0), strImagingSeqFilePathFinal, True) Then
						' Abort processing
						m_message = "Error copying ImagingSequence (.mis) file"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to process MALDI imaging data")
						Return False
					End If

				End If
			End If

		Catch ex As Exception
			m_message = "Error obtaining ImagingSequence (.mis) file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try

		Try

			' Unzip each of the *R*X*.zip files to the Chameleon cached data folder

			' However, consider limits defined by job params BrukerMALDI_Imaging_StartSectionX and BrukerMALDI_Imaging_EndSectionX
			' when processing the files

			Dim ZipFiles() As String
			ZipFiles = System.IO.Directory.GetFiles(ServerPath, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK)

			For Each strZipFilePathRemote In ZipFiles

				If blnApplySectionFilter Then
					blnUnzipFile = False

					' Determine the R, X, and Y coordinates for this .Zip file
					If GetBrukerImagingFileCoords(strZipFilePathRemote, CoordR, CoordX, CoordY) Then
						' Compare to StartSectionX and EndSectionX
						If CoordX >= StartSectionX AndAlso CoordX <= EndSectionX Then
							blnUnzipFile = True
						End If
					End If
				Else
					blnUnzipFile = True
				End If

				' Open up the zip file over the network and get a listing of all of the files
				' If they already exist in the cached data folder, then there is no need to continue

				If blnUnzipFile Then

					' Set this to false for now
					blnUnzipFile = False

					Dim objZipfile As Ionic.Zip.ZipFile

					objZipfile = New Ionic.Zip.ZipFile(strZipFilePathRemote)

					For Each objEntry As Ionic.Zip.ZipEntry In objZipfile.Entries
						If Not objEntry.IsDirectory Then

							Dim strPathToCheck As String
							strPathToCheck = System.IO.Path.Combine(strUnzipFolderPathBase, objEntry.FileName.Replace("/"c, "\"c))

							If Not System.IO.File.Exists(strPathToCheck) Then
								blnUnzipFile = True
								Exit For
							End If
						End If
					Next
				End If

				If blnUnzipFile Then
					' Unzip the file to the Chameleon cached data folder
					' If UnzipOverNetwork=True, then we want to copy the file locally first

					If UnzipOverNetwork Then
						strZipFilePathToExtract = String.Copy(strZipFilePathRemote)
					Else
						Try

							' Copy the file to the work directory on the local computer
							strZipFilePathToExtract = System.IO.Path.Combine(WorkDir, System.IO.Path.GetFileName(strZipFilePathRemote))

							If m_DebugLevel >= 2 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying " + strZipFilePathRemote)
							End If

							If Not CopyFileWithRetry(strZipFilePathRemote, strZipFilePathToExtract, True) Then
								' Abort processing
								m_message = "Error copying Zip file"
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to process MALDI imaging data")
								Return False
							End If

						Catch ex As Exception
							m_message = "Error copying zipped instrument data"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ", file " + strZipFilePathRemote + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
							Return False
						End Try
					End If

					' Now use Ionic to unzip strZipFilePathLocal to the data cache folder
					' Do not overwrite existing files (assume they're already valid)
					Try

						Dim objZipfile As Ionic.Zip.ZipFile

						objZipfile = New Ionic.Zip.ZipFile(strZipFilePathToExtract)

						If m_DebugLevel >= 2 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping " + strZipFilePathToExtract)
						End If

						objZipfile.ExtractAll(strUnzipFolderPathBase, Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)
						objZipfile = Nothing

					Catch ex As Exception
						m_message = "Error extracting zipped instrument data"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ", file " + strZipFilePathToExtract + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
						Return False
					End Try

					If Not UnzipOverNetwork Then
						' Need to delete the zip file that we copied locally
						' However, Ionic may have a file handle open so we use a queue to keep track of files that need to be deleted

						DeleteQueuedFiles(strFilesToDelete, strZipFilePathToExtract)
					End If

				End If

			Next


			If Not UnzipOverNetwork Then
				Dim dtStartTime As System.DateTime = System.DateTime.UtcNow

				Do While strFilesToDelete.Count > 0
					' Try to process the files remaining in queue strFilesToDelete

					DeleteQueuedFiles(strFilesToDelete, String.Empty)

					If strFilesToDelete.Count > 0 Then
						If System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 20 Then
							' Stop trying to delete files; it's not worth continuing to try
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete all of the files in queue strFilesToDelete; Queue Length = " + strFilesToDelete.Count.ToString() + "; this warning can be safely ignored (function RetrieveBrukerMALDIImagingFolders)")
							Exit Do
						End If

						System.Threading.Thread.Sleep(500)
					End If
				Loop

			End If

		Catch ex As Exception
			m_message = "Error extracting zipped instrument data"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " from " + strZipFilePathRemote + "; " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try

		' If we get here, all is fine
		Return True

	End Function

	''' <summary>
	''' Unzips dataset folders to working directory
	''' </summary>
	''' <param name="WorkDir">Destination directory for copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function RetrieveSFolders(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim ZipFiles() As String
		Dim DSWorkFolder As String
		Dim UnZipper As clsIonicZipTools

		Dim SourceFilePath As String
		Dim TargetFolderPath As String

		Dim ZipFile As String

		Try

			'First Check for the existence of a 0.ser Folder
			'If 0.ser folder exists, then either store the path to the 0.ser folder in a StoragePathInfo file, or copy the 0.ser folder to the working directory
			Dim DSFolderPath As String = FindValidFolder(m_DatasetName, "", BRUKER_ZERO_SER_FOLDER)

			If Not String.IsNullOrEmpty(DSFolderPath) Then
				Dim diSourceFolder As System.IO.DirectoryInfo
				Dim diTargetFolder As System.IO.DirectoryInfo
				Dim fiFile As System.IO.FileInfo

				diSourceFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(DSFolderPath, BRUKER_ZERO_SER_FOLDER))

				If diSourceFolder.Exists Then
					If CreateStoragePathInfoOnly Then
						If CreateStoragePathInfoFile(diSourceFolder.FullName, WorkDir + "\") Then
							Return True
						Else
							Return False
						End If
					Else
						' Copy the 0.ser folder to the Work directory
						' First create the 0.ser subfolder
						diTargetFolder = System.IO.Directory.CreateDirectory(System.IO.Path.Combine(WorkDir, BRUKER_ZERO_SER_FOLDER))

						' Now copy the files from the source 0.ser folder to the target folder
						' Typically there will only be two files: ACQUS and ser
						For Each fiFile In diSourceFolder.GetFiles()
							If Not CopyFileToWorkDir(fiFile.Name, diSourceFolder.FullName, diTargetFolder.FullName) Then
								' Error has alredy been logged
								Return False
							End If
						Next

						Return True
					End If
				End If

			End If

			'If the 0.ser folder does not exist, unzip the zipped s-folders
			'Copy the zipped s-folders from archive to work directory
			If Not CopySFoldersToWorkDir(WorkDir, CreateStoragePathInfoOnly) Then
				'Error messages have already been logged, so just exit
				Return False
			End If

			If CreateStoragePathInfoOnly Then
				' Nothing was copied locally, so nothing to unzip
				Return True
			End If


			'Get a listing of the zip files to process
			ZipFiles = System.IO.Directory.GetFiles(WorkDir, "s*.zip")
			If ZipFiles.GetLength(0) < 1 Then
				m_message = "No zipped s-folders found in working directory"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False			'No zipped data files found
			End If

			'Create a dataset subdirectory under the working directory
			DSWorkFolder = System.IO.Path.Combine(WorkDir, m_DatasetName)
			System.IO.Directory.CreateDirectory(DSWorkFolder)

			'Set up the unzipper
			UnZipper = New clsIonicZipTools(m_DebugLevel, DSWorkFolder)

			'Unzip each of the zip files to the working directory
			For Each ZipFile In ZipFiles
				If m_DebugLevel > 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file " + ZipFile)
				End If
				Try
					TargetFolderPath = System.IO.Path.Combine(DSWorkFolder, System.IO.Path.GetFileNameWithoutExtension(ZipFile))
					System.IO.Directory.CreateDirectory(TargetFolderPath)

					SourceFilePath = System.IO.Path.Combine(WorkDir, System.IO.Path.GetFileName(ZipFile))

					If Not UnZipper.UnzipFile(SourceFilePath, TargetFolderPath) Then
						m_message = "Error unzipping file " + ZipFile
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return False
					End If
				Catch ex As Exception
					m_message = "Exception while unzipping s-folders"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
					Return False
				End Try
			Next

			'Delete all s*.zip files in working directory
			For Each ZipFile In ZipFiles
				Try
					System.IO.File.Delete(System.IO.Path.Combine(WorkDir, System.IO.Path.GetFileName(ZipFile)))
				Catch ex As Exception
					m_message = "Exception deleting file " + ZipFile
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " : " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
					Return False
				End Try
			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RetrieveSFolders: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try


		'Got to here, so everything must have worked
		Return True
	End Function

	''' <summary>
	''' Uses Ken's dll to create a fasta file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
	''' </summary>
	''' <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Stores the name of the FASTA file as a new job parameter named "generatedFastaName" in section "PeptideSearch"</remarks>
	Protected Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean

		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Obtaining org db file")
		End If

		'Make a new fasta file from scratch
		If Not CreateFastaFile(LocalOrgDBFolder) Then
			'There was a problem. Log entries in lower-level routines provide documentation
			Return False
		End If

		'Fasta file was successfully generated. Put the name of the generated fastafile in the
		'	job data class for other methods to use
		If Not m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName) Then
			m_message = "Error adding parameter 'generatedFastaName' to m_jobParams"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If


		'We got to here OK, so return
		Return True

	End Function

	''' <summary>
	''' Overrides base class version of the function to creates a Sequest params file compatible 
	'''	with the Bioworks version on this system. Uses ParamFileGenerator dll provided by Ken Auberry
	''' </summary>
	''' <param name="ParamFileName">Name of param file to be created</param>
	''' <param name="ParamFilePath">Param file storage path</param>
	''' <param name="WorkDir">Working directory on analysis machine</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability</remarks>
	Protected Function RetrieveGeneratedParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
	  ByVal WorkDir As String) As Boolean

		Dim ParFileGen As ParamFileGenerator.MakeParams.IGenerateFile = Nothing
		Dim blnSuccess As Boolean

		Try
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving parameter file")

			ParFileGen = New ParamFileGenerator.MakeParams.clsMakeParameterFile
			ParFileGen.TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")

			' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
			blnSuccess = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion(m_jobParams.GetParam("ToolName")), _
			 System.IO.Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName")), _
			 WorkDir, m_mgrParams.GetParam("connectionstring"), m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0))

			If blnSuccess Then
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Successfully retrieved param file: " + ParamFileName)
				End If

				Return True
			Else
				m_message = "Error converting param file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ParFileGen.LastError)
				Return False
			End If

		Catch ex As Exception
			If String.IsNullOrWhiteSpace(m_message) Then
				m_message = "Error retrieving parameter file"
			End If

			Dim Msg As String = m_message + ": " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

			If Not ParFileGen Is Nothing Then
				If Not String.IsNullOrWhiteSpace(ParFileGen.LastError) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error converting param file: " + ParFileGen.LastError)
				End If
			End If
			Return False
		End Try

	End Function

	''' <summary>
	''' This is just a generic function to copy files to the working directory
	'''	
	''' </summary>
	''' <param name="FileName">Name of file to be copied</param>
	''' <param name="FilePath">File storage path</param>
	''' <param name="WorkDir">Working directory on analysis machine</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	Protected Function RetrieveFile(ByVal FileName As String, ByVal FilePath As String, _
	  ByVal WorkDir As String) As Boolean

		'Copy the file
		If Not CopyFileToWorkDir(FileName, FilePath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
			Return False
		End If

		Return True

	End Function

	''' <summary>
	''' Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
	''' </summary>
	''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
	''' <returns>TRUE for success, FALSE for error</returns>
	''' <remarks></remarks>
	Public Function RetrieveDtaFiles(ByVal UnConcatenate As Boolean) As Boolean

		Dim SourceFileName As String
		Dim SourceFolderPath As String

		'Retrieve zipped DTA file
		SourceFileName = m_DatasetName + "_dta.zip"
		SourceFolderPath = FindDataFile(SourceFileName)

		If SourceFolderPath = "" Then
			' Couldn't find a folder with the _dta.zip file; how about the _dta.txt file?

			SourceFileName = m_DatasetName + "_dta.txt"
			SourceFolderPath = FindDataFile(SourceFileName)

			If SourceFolderPath = "" Then
				' No folder found containing the zipped DTA files; return False
				' (the FindDataFile procedure should have already logged an error)
				m_message = "Could not find " + SourceFileName
				Return False
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Warning: could not find the _dta.zip file, but was able to find " + SourceFileName + " in folder " + SourceFolderPath)

				'Copy the _dta.txt file
				If Not CopyFileToWorkDir(SourceFileName, SourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " + SourceFileName + " using folder " + SourceFolderPath)
					End If
					m_message = "Error copying " + SourceFileName
					Return False
				End If

			End If

		Else

			'Copy the _dta.zip file
			If Not CopyFileToWorkDir(SourceFileName, SourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " + SourceFileName + " using folder " + SourceFolderPath)
				End If
				m_message = "Error copying " + SourceFileName
				Return False
			Else
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " + SourceFileName + " from folder " + SourceFolderPath)
				End If
			End If

			'Unzip concatenated DTA file
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
			If UnzipFileStart(System.IO.Path.Combine(m_WorkingDir, SourceFileName), m_WorkingDir, "clsAnalysisResources.RetrieveDtaFiles", False) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
				End If
			End If

			' Delete the _DTA.zip file to free up some disk space
			System.Threading.Thread.Sleep(100)
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting the _DTA.zip file")
			End If

			Try
				System.IO.File.Delete(System.IO.Path.Combine(m_WorkingDir, SourceFileName))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting the _DTA.zip file: " + ex.Message)
			End Try

		End If

		'Unconcatenate DTA file if needed
		If UnConcatenate Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file")

			Dim fiSourceFile As System.IO.FileInfo
			fiSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkingDir, m_DatasetName + "_dta.txt"))

			If Not fiSourceFile.Exists Then
				m_message = "_DTA.txt file not found after unzipping"
				Return False
			ElseIf fiSourceFile.Length = 0 Then
				m_message = "_DTA.txt file is empty (zero-bytes)"
				Return False
			End If

			Dim FileSplitter As New clsSplitCattedFiles()
			FileSplitter.SplitCattedDTAsOnly(m_DatasetName, m_WorkingDir)

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Completed splitting concatenated DTA file")
			End If
		End If

		Return True

	End Function

	''' <summary>
	''' Retrieves zipped, concatenated OUT file, unzips, and splits into individual OUT files
	''' </summary>
	''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
	''' <returns>TRUE for success, FALSE for error</returns>
	''' <remarks></remarks>
	Protected Function RetrieveOutFiles(ByVal UnConcatenate As Boolean) As Boolean

		'Retrieve zipped OUT file
		Dim ZippedFileName As String = m_DatasetName + "_out.zip"
		Dim ZippedFolderName As String = FindDataFile(ZippedFileName)

		If ZippedFolderName = "" Then Return False 'No folder found containing the zipped OUT files
		'Copy the file
		If Not CopyFileToWorkDir(ZippedFileName, ZippedFolderName, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
			Return False
		End If

		'Unzip concatenated OUT file
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated OUT file")
		If UnzipFileStart(System.IO.Path.Combine(m_WorkingDir, ZippedFileName), m_WorkingDir, "clsAnalysisResources.RetrieveOutFiles", False) Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Concatenated OUT file unzipped")
			End If
		End If

		'Unconcatenate OUT file if needed
		If UnConcatenate Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated OUT file")

			Dim fiSourceFile As System.IO.FileInfo
			fiSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkingDir, m_DatasetName + "_out.txt"))

			If Not fiSourceFile.Exists Then
				m_message = "_OUT.txt file not found after unzipping"
				Return False
			ElseIf fiSourceFile.Length = 0 Then
				m_message = "_OUT.txt file is empty (zero-bytes)"
				Return False
			End If

			Dim FileSplitter As New clsSplitCattedFiles()
			FileSplitter.SplitCattedOutsOnly(m_DatasetName, m_WorkingDir)

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Completed splitting concatenated OUT file")
			End If
		End If

		Return True

	End Function

	''' <summary>
	''' Creates the specified settings file from db info
	''' </summary>
	''' <returns>TRUE if file created successfully; FALSE otherwise</returns>
	''' <remarks>Use this overload with jobs where settings file is retrieved from database</remarks>
	Protected Friend Function RetrieveSettingsFileFromDb() As Boolean

		Dim OutputFile As String = System.IO.Path.Combine(m_WorkingDir, m_jobParams.GetParam("SettingsFileName"))

		Return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile)

	End Function

	''' <summary>
	''' Specifies the Bioworks version for use by the Param File Generator DLL
	''' </summary>
	''' <param name="ToolName">Version specified in mgr config file</param>
	''' <returns>IGenerateFile.ParamFileType based on input version</returns>
	''' <remarks></remarks>
	Protected Function SetBioworksVersion(ByVal ToolName As String) As ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType

		Dim strToolNameLCase As String

		strToolNameLCase = ToolName.ToLower

		'Converts the setup file entry for the Bioworks version to a parameter type compatible with the
		'	parameter file generator dll
		Select Case strToolNameLCase
			Case "20"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_20
			Case "30"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_30
			Case "31"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_31
			Case "32"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_32
			Case "sequest"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_Current
			Case "xtandem"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.X_Tandem
			Case "inspect"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.Inspect
			Case "msgfplus"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSGFPlus
			Case "msalign"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSAlign
			Case Else
				' Did not find an exact match
				' Try a substring match
				If strToolNameLCase.Contains("sequest") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.BioWorks_Current
				ElseIf strToolNameLCase.Contains("xtandem") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.X_Tandem
				ElseIf strToolNameLCase.Contains("inspect") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.Inspect
				ElseIf strToolNameLCase.Contains("msgfplus") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSGFPlus
				ElseIf strToolNameLCase.Contains("msalign") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSAlign
				Else
					Return Nothing
				End If
		End Select

	End Function

	''' <summary>
	''' Unzips all files in the specified Zip file
	''' If the file is less than 1.25 GB in size (IONIC_ZIP_MAX_FILESIZE_MB) then uses Ionic.Zip
	''' Otherwise, uses PKZipC (provided PKZipC.exe exists)
	''' </summary>
	''' <param name="ZipFilePath">File to unzip</param>
	''' <param name="OutFolderPath">Target directory for the extracted files</param>
	''' <param name="CallingFunctionName">Calling function name (used for debugging purposes)</param>
	''' <param name="ForceExternalZipProgramUse">If True, then force use of PKZipC.exe</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function UnzipFileStart(ByVal ZipFilePath As String, _
	 ByVal OutFolderPath As String, _
	 ByVal CallingFunctionName As String, _
	 ByVal ForceExternalZipProgramUse As Boolean) As Boolean

		Dim fiFileInfo As System.IO.FileInfo
		Dim sngFileSizeMB As Single

		Dim blnUseExternalUnzipper As Boolean = False
		Dim blnSuccess As Boolean = False

		Dim strExternalUnzipperFilePath As String
		Dim strUnzipperName As String = String.Empty

		Dim dtStartTime As DateTime
		Dim dtEndTime As DateTime

		Try
			If ZipFilePath Is Nothing Then ZipFilePath = String.Empty

			If String.IsNullOrEmpty(CallingFunctionName) Then
				CallingFunctionName = "??"
			End If

			strExternalUnzipperFilePath = m_mgrParams.GetParam("zipprogram")
			If strExternalUnzipperFilePath Is Nothing Then strExternalUnzipperFilePath = String.Empty

			fiFileInfo = New System.IO.FileInfo(ZipFilePath)
			sngFileSizeMB = CSng(fiFileInfo.Length / 1024.0 / 1024)

			If Not fiFileInfo.Exists Then
				' File not found
				m_message = "Error unzipping '" + ZipFilePath + "': File not found (called from " + CallingFunctionName + ")"

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Use the external zipper if the file size is over IONIC_ZIP_MAX_FILESIZE_MB or if ForceExternalZipProgramUse = True
			' However, if the .Exe file for the external zipper is not found, then fall back to use Ionic.Zip
			If ForceExternalZipProgramUse OrElse sngFileSizeMB >= IONIC_ZIP_MAX_FILESIZE_MB Then
				If strExternalUnzipperFilePath.Length > 0 AndAlso _
				   strExternalUnzipperFilePath.ToLower <> "na" Then
					If System.IO.File.Exists(strExternalUnzipperFilePath) Then
						blnUseExternalUnzipper = True
					End If
				End If

				If Not blnUseExternalUnzipper Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "External zip program not found: " + strExternalUnzipperFilePath + "; will instead use Ionic.Zip")
				End If
			End If

			If blnUseExternalUnzipper Then
				strUnzipperName = System.IO.Path.GetFileName(strExternalUnzipperFilePath)

				Dim UnZipper As New PRISM.Files.ZipTools(OutFolderPath, strExternalUnzipperFilePath)

				dtStartTime = DateTime.UtcNow
				blnSuccess = UnZipper.UnzipFile("", ZipFilePath, OutFolderPath)
				dtEndTime = DateTime.UtcNow

				If blnSuccess Then
					m_IonicZipTools.ReportZipStats(fiFileInfo, dtStartTime, dtEndTime, False, strUnzipperName)
				Else
					m_message = "Error unzipping " + System.IO.Path.GetFileName(ZipFilePath) + " using " + strUnzipperName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, CallingFunctionName + ": " + m_message)
					UnZipper = Nothing
				End If
			Else
				' Use Ionic.Zip
				strUnzipperName = clsIonicZipTools.IONIC_ZIP_NAME
				m_IonicZipTools.DebugLevel = m_DebugLevel
				blnSuccess = m_IonicZipTools.UnzipFile(ZipFilePath, OutFolderPath)
			End If

		Catch ex As Exception
			m_message = "Exception while unzipping '" + ZipFilePath + "'"
			If Not String.IsNullOrEmpty(strUnzipperName) Then m_message &= " using " + strUnzipperName

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex))
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
	''' </summary>
	''' <param name="strSourceFilePath">Input _DTA.txt file to parse</param>
	''' <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
	''' <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
	''' <param name="strOutputFilePath">Output file path to use for the updated file; required if blnReplaceSourceFile=False; ignored if blnReplaceSourceFile=True</param>
	''' <returns>True if success; false if an error</returns>
	Public Function ValidateCDTAFileScanAndCSTags(ByVal strSourceFilePath As String, ByVal blnReplaceSourceFile As Boolean, ByVal blnDeleteSourceFileIfUpdated As Boolean, ByRef strOutputFilePath As String) As Boolean

		Dim strOutputFilePathTemp As String
		Dim strLineIn As String
		Dim strDTAHeader As String

		Dim intScanNumberStart As Integer
		Dim intScanNumberEnd As Integer
		Dim intScanCount As Integer
		Dim intCharge As Integer

		Dim blnValidScanInfo As Boolean = False
		Dim blnParentIonLineIsNext As Boolean = False
		Dim blnParentIonLineUpdated As Boolean = False

		Dim blnSuccess As Boolean = False

		' We use the DtaTextFileReader to parse out the scan and charge from the header line
		Dim objReader As MSDataFileReader.clsDtaTextFileReader

		Dim fiOriginalFile As System.IO.FileInfo
		Dim fiUpdatedFile As System.IO.FileInfo

		Try

			If String.IsNullOrEmpty(strSourceFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ValidateCDTAFileScanAndCSTags: strSourceFilePath is empty")
				Return False
			End If

			fiOriginalFile = New System.IO.FileInfo(strSourceFilePath)
			If Not fiOriginalFile.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ValidateCDTAFileScanAndCSTags: source file not found: " + strSourceFilePath)
				Return False
			End If

			If blnReplaceSourceFile Then
				strOutputFilePathTemp = strSourceFilePath + ".tmp"
			Else
				' strOutputFilePath must contain a valid file path
				If String.IsNullOrEmpty(strOutputFilePath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ValidateCDTAFileScanAndCSTags: variable strOutputFilePath must define a file path when blnReplaceSourceFile=False")
					Return False
				End If
				strOutputFilePathTemp = strOutputFilePath
			End If

			fiUpdatedFile = New System.IO.FileInfo(strOutputFilePathTemp)

			objReader = New MSDataFileReader.clsDtaTextFileReader(False)

			' Open the input file
			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(fiOriginalFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Create the output file
				Using swOutFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(fiUpdatedFile.FullName, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srInFile.Peek > -1
						strLineIn = srInFile.ReadLine()

						If String.IsNullOrEmpty(strLineIn) Then
							swOutFile.WriteLine()
						Else
							If strLineIn.StartsWith("="c) Then
								' Parse the DTA header line, for example:
								' =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

								' Remove the leading and trailing characters, then extract the scan and charge
								strDTAHeader = strLineIn.Trim(New Char() {"="c, " "c, ControlChars.Quote})
								blnValidScanInfo = objReader.ExtractScanInfoFromDtaHeader(strDTAHeader, intScanNumberStart, intScanNumberEnd, intScanCount, intCharge)

								blnParentIonLineIsNext = True

							ElseIf blnParentIonLineIsNext Then
								' strLineIn contains the parent ion line text

								' Construct the parent ion line to write out
								' Will contain the MH+ value of the parent ion (thus always the 1+ mass, even if actually a different charge)
								' Next contains the charge state, then scan= and cs= tags, for example:
								' 447.34573 1   scan=3 cs=1

								If Not strLineIn.Contains("scan=") Then
									' Append scan=x to the parent ion line
									strLineIn = strLineIn.Trim() + "   scan=" + intScanNumberStart.ToString()
									blnParentIonLineUpdated = True
								End If

								If Not strLineIn.Contains("cs=") Then
									' Append cs=y to the parent ion line
									strLineIn = strLineIn.Trim() + " cs=" + intCharge.ToString()
									blnParentIonLineUpdated = True
								End If

								blnParentIonLineIsNext = False

							End If

							swOutFile.WriteLine(strLineIn)

						End If
					Loop

				End Using
			End Using

			If blnParentIonLineUpdated Then
				System.Threading.Thread.Sleep(100)

				If blnReplaceSourceFile Then
					' Replace the original file with the new one
					Dim strOldFilePath As String
					Dim intAddon As Integer = 0

					Do
						strOldFilePath = fiOriginalFile.FullName + ".old"
						If intAddon > 0 Then
							strOldFilePath &= intAddon.ToString()
						End If
						intAddon += 1
					Loop While System.IO.File.Exists(strOldFilePath)

					fiOriginalFile.MoveTo(strOldFilePath)
					System.Threading.Thread.Sleep(100)

					fiUpdatedFile.MoveTo(strSourceFilePath)

					If blnDeleteSourceFileIfUpdated Then
						fiOriginalFile.Delete()
					End If

					blnSuccess = True
				Else
					' Directly wrote to the output file; nothing to rename
					blnSuccess = True
				End If
			Else
				' No changes were made; nothing to update
				' However, delete the new file we created
				fiUpdatedFile.Delete()
				blnSuccess = True
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateCDTAFileScanAndCSTags: " + ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function


	''' <summary>
	''' Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the first column
	''' </summary>
	''' <param name="strFilePath">Path to the file</param>
	''' <param name="strFileDescription">File description, e.g. Synopsis</param>
	''' <returns>True if the file has data; otherwise false</returns>
	''' <remarks></remarks>
	Public Shared Function ValidateFileHasData(ByVal strFilePath As String, ByVal strFileDescription As String, ByRef strErrorMessage As String) As Boolean
		Dim intNumericDataColIndex As Integer = 0
		Return ValidateFileHasData(strFilePath, strFileDescription, strErrorMessage, intNumericDataColIndex)
	End Function

	''' <summary>
	''' Validate that the specified file exists and has at least one tab-delimited row with a numeric value
	''' </summary>
	''' <param name="strFilePath">Path to the file</param>
	''' <param name="strFileDescription">File description, e.g. Synopsis</param>
	''' <param name="intNumericDataColIndex">Index of the numeric data column; use -1 to simply look for any text in the file</param>
	''' <returns>True if the file has data; otherwise false</returns>
	''' <remarks></remarks>
	Public Shared Function ValidateFileHasData(ByVal strFilePath As String, ByVal strFileDescription As String, ByRef strErrorMessage As String, ByVal intNumericDataColIndex As Integer) As Boolean

		Dim fiFileInfo As System.IO.FileInfo

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim dblValue As Double
		Dim blnDataFound As Boolean

		strErrorMessage = String.Empty

		Try
			fiFileInfo = New System.IO.FileInfo(strFilePath)

			If Not fiFileInfo.Exists Then
				strErrorMessage = strFileDescription + " file not found: " + fiFileInfo.Name
				Return False
			End If

			If fiFileInfo.Length = 0 Then
				strErrorMessage = strFileDescription + " file is empty (zero-bytes)"
				Return False
			End If

			' Open the file and confirm it has data rows
			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(fiFileInfo.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
				While srInFile.Peek > -1 And Not blnDataFound
					strLineIn = srInFile.ReadLine()
					If Not String.IsNullOrEmpty(strLineIn) Then
						If intNumericDataColIndex < 0 Then
							blnDataFound = True
						Else
							' Split on the tab character and check if the first column is numeric
							strSplitLine = strLineIn.Split(ControlChars.Tab)

							If Not strSplitLine Is Nothing AndAlso strSplitLine.Length > intNumericDataColIndex Then
								If Double.TryParse(strSplitLine(intNumericDataColIndex), dblValue) Then
									blnDataFound = True
								End If
							End If

						End If
					End If
				End While
			End Using

			If Not blnDataFound Then
				strErrorMessage = strFileDescription + " is empty (no data)"
			End If

		Catch ex As Exception
			strErrorMessage = "Exception validating " + strFileDescription + " file"
			Return False
		End Try

		Return blnDataFound

	End Function

	''' <summary>
	''' Validates that sufficient free memory is available to run Java
	''' </summary>
	''' <param name="strJavaMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) to reserve for Java</param>
	''' <param name="strStepToolName">Step tool name to use when posting log entries</param>
	''' <returns>True if sufficient free memory; false if not enough free memory</returns>
	''' <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.  
	''' These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline) </remarks>
	Protected Function ValidateFreeMemorySize(ByVal strJavaMemorySizeJobParamName As String, ByVal strStepToolName As String) As Boolean

		Dim blnLogFreeMemoryOnSuccess As Boolean = True
		Return ValidateFreeMemorySize(strJavaMemorySizeJobParamName, strStepToolName, blnLogFreeMemoryOnSuccess)

	End Function

	''' <summary>
	''' Validates that sufficient free memory is available to run Java
	''' </summary>
	''' <param name="strMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) that must be available on the system</param>
	''' <param name="strStepToolName">Step tool name to use when posting log entries</param>
	''' <param name="blnLogFreeMemoryOnSuccess">If True, then post a log entry if sufficient memory is, in fact, available</param>
	''' <returns>True if sufficient free memory; false if not enough free memory</returns>
	''' <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.  
	''' These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline) </remarks>

	Protected Function ValidateFreeMemorySize(ByVal strMemorySizeJobParamName As String, ByVal strStepToolName As String, ByVal blnLogFreeMemoryOnSuccess As Boolean) As Boolean
		Dim intFreeMemoryRequiredMB As Integer

		' Lookup parameter strMemorySizeJobParamName; assume 2000 MB if not defined
		intFreeMemoryRequiredMB = m_jobParams.GetJobParameter(strMemorySizeJobParamName, 2000)

		' Require intFreeMemoryRequiredMB be at least 0.5 GB
		If intFreeMemoryRequiredMB < 512 Then intFreeMemoryRequiredMB = 512

		If m_DebugLevel < 1 Then blnLogFreeMemoryOnSuccess = False

		Return ValidateFreeMemorySize(intFreeMemoryRequiredMB, strStepToolName, blnLogFreeMemoryOnSuccess)

	End Function

	Public Shared Function ValidateFreeMemorySize(ByVal intFreeMemoryRequiredMB As Integer, ByVal strStepToolName As String, ByVal blnLogFreeMemoryOnSuccess As Boolean) As Boolean
		Dim sngFreeMemoryMB As Single
		Dim strMessage As String

		sngFreeMemoryMB = GetFreeMemoryMB()

		If intFreeMemoryRequiredMB >= sngFreeMemoryMB Then
			strMessage = "Not enough free memory to run " + strStepToolName

			strMessage &= "; need " + intFreeMemoryRequiredMB.ToString() + " MB but system has " + sngFreeMemoryMB.ToString("0") + " MB available"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)

			Return False
		Else
			If blnLogFreeMemoryOnSuccess Then
				strMessage = strStepToolName + " will use " + intFreeMemoryRequiredMB.ToString() + " MB; system has " + sngFreeMemoryMB.ToString("0") + " MB available"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
			End If

			Return True
		End If
	End Function

#End Region

#Region "Event Handlers"
	Private Sub m_FileTools_WaitingForLockQueue(SourceFilePath As String, TargetFilePath As String, MBBacklogSource As Integer, MBBacklogTarget As Integer) Handles m_FileTools.WaitingForLockQueue
		If System.DateTime.UtcNow.Subtract(m_LastLockQueueWaitTimeLog).TotalSeconds >= 30 Then
			m_LastLockQueueWaitTimeLog = System.DateTime.UtcNow
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Waiting for lockfile queue to fall below threshold (clsAnalysisResources); SourceBacklog=" & MBBacklogSource & " MB, TargetBacklog=" & MBBacklogTarget & " MB, Source=" & SourceFilePath & ", Target=" & TargetFilePath)
			End If
		End If
	End Sub
#End Region

End Class


