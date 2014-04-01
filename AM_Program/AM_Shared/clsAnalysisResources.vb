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
Imports System.IO
Imports System.Runtime.InteropServices

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

	Public Const MYEMSL_PATH_FLAG As String = "\\MyEMSL"

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
	Public Const RESULT_TYPE_MODA As String = "MODa_Peptide_Hit"

	Public Const DOT_WIFF_EXTENSION As String = ".wiff"
	Public Const DOT_D_EXTENSION As String = ".d"
	Public Const DOT_RAW_EXTENSION As String = ".raw"
	Public Const DOT_UIMF_EXTENSION As String = ".uimf"
	Public Const DOT_MZXML_EXTENSION As String = ".mzXML"
	Public Const DOT_MZML_EXTENSION As String = ".mzML"

	Public Const DOT_MGF_EXTENSION As String = ".mgf"
	Public Const DOT_CDF_EXTENSION As String = ".cdf"

	Public Const STORAGE_PATH_INFO_FILE_SUFFIX As String = "_StoragePathInfo.txt"

	Public Const SCAN_STATS_FILE_SUFFIX As String = "_ScanStats.txt"
	Public Const SCAN_STATS_EX_FILE_SUFFIX As String = "_ScanStatsEx.txt"

	Public Const BRUKER_ZERO_SER_FOLDER As String = "0.ser"
	Public Const BRUKER_SER_FILE As String = "ser"
	Public Const BRUKER_FID_FILE As String = "fid"

	Public Const JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS As String = "PackedParam_DatasetFilePaths"
	Public Const JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES As String = "PackedParam_DatasetRawDataTypes"

	Public Const JOB_INFO_FILE_PREFIX As String = "JobInfoFile_Job"

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
		Public Instrument As String
		Public InstrumentGroup As String
		Public Experiment As String
		Public Experiment_Reason As String
		Public Experiment_Comment As String
		Public Experiment_Organism As String
		Public Experiment_NEWT_ID As Integer		' NEWT ID for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
		Public Experiment_NEWT_Name As String		' NEWT Name for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
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

	Public Structure udtDataPackageRetrievalOptionsType
		''' <summary>
		''' Set to true to create a text file for each job listing the full path to the files that would be retrieved for that job
		''' Example filename: FilePathInfo_Job950000.txt
		''' </summary>
		''' <remarks>No files are actually retrieved when this is set to True</remarks>
		Public CreateJobPathFiles As Boolean
		''' <summary>
		''' Set to true to obtain the mzXML file for the dataset associated with this job
		''' </summary>
		''' <remarks>If the .mzXML file does not exist, then retrieves the instrument data file (e.g. Thermo .raw file)</remarks>
		Public RetrieveMzXMLFile As Boolean
		''' <summary>
		''' Set to True to retrieve _DTA.txt files (the PRIDE Converter will convert these to .mgf files)
		''' </summary>
		''' <remarks></remarks>
		Public RetrieveDTAFiles As Boolean
		''' <summary>
		''' Set to True to obtain MSGF+ .mzID files
		''' </summary>
		''' <remarks></remarks>
		Public RetrieveMZidFiles As Boolean
		''' <summary>
		''' Set to True to obtain the _syn.txt file and related PHRP files
		''' </summary>
		''' <remarks></remarks>
		Public RetrievePHRPFiles As Boolean
	End Structure

	Public Structure udtAggregateFileProcessingType
		Public Filename As String
		Public FilterValue As String
		Public SaveMode As String
	End Structure

	Public Structure udtHPCOptionsType
		Public HeadNode As String
		Public UsingHPC As Boolean
		Public SharePath As String
		Public ResourceType As String
		' Obsolete parameter; no longer used: Public NodeGroup As String
		Public MinimumMemoryMB As Integer
		Public MinimumCores As Integer
		Public WorkDirPath As String
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

	Protected m_StatusTools As IStatusFile		' Might be nothing

	Protected m_GenerationStarted As Boolean = False
	Protected m_GenerationComplete As Boolean = False
	Protected m_FastaToolsCnStr As String = ""
	Protected m_FastaFileName As String = ""
	Protected m_FastaGenTimeOut As Boolean = False
	Protected m_FastaGenStartTime As DateTime = DateTime.UtcNow

	Protected WithEvents m_FastaTools As Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS
	Protected WithEvents m_FastaTimer As Timers.Timer
	Protected m_IonicZipTools As clsIonicZipTools

	Protected WithEvents m_FileTools As PRISM.Files.clsFileTools

	Protected WithEvents m_CDTAUtilities As clsCDTAUtilities

	Protected WithEvents m_SplitFastaFileUtility As clsSplitFastaFileUtilities

	Protected m_SplitFastaLastUpdateTime As DateTime
	Protected m_SplitFastaLastPercentComplete As Integer

	Protected WithEvents m_MyEMSLDatasetListInfo As MyEMSLReader.DatasetListInfo
	Protected m_RecentlyFoundMyEMSLFiles As List(Of MyEMSLReader.DatasetFolderOrFileInfo)

	Private m_LastLockQueueWaitTimeLog As DateTime = DateTime.UtcNow
	Private m_LockQueueWaitTimeStart As DateTime = DateTime.UtcNow

	Private m_LastMyEMSLProgressWriteTime As DateTime = DateTime.UtcNow

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

		m_FastaFileName = Path.GetFileName(FullOutputPath)	  'Get the name of the fasta file that was generated
		m_GenerationComplete = True		'Set the completion flag

	End Sub

	Private Sub m_FastaTools_FileGenerationProgress(ByVal statusMsg As String, ByVal fractionDone As Double) Handles m_FastaTools.FileGenerationProgress
		Const MINIMUM_LOG_INTERVAL_SEC As Integer = 10
		Static dtLastLogTime As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(1, 0, 0))
		Static dblFractionDoneSaved As Double = -1

		Dim blnForcelog = m_DebugLevel >= 1 AndAlso statusMsg.Contains(Protein_Exporter.clsGetFASTAFromDMS.LOCK_FILE_PROGRESS_TEXT)

		If m_DebugLevel >= 3 OrElse blnForcelog Then
			' Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
			If blnForcelog OrElse _
			   DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC OrElse _
			   fractionDone - dblFractionDoneSaved >= 0.25 Then
				dtLastLogTime = DateTime.UtcNow
				dblFractionDoneSaved = fractionDone
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Generating Fasta file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg)
			End If
		End If

	End Sub

	Private Sub m_FastaTimer_Elapsed(ByVal sender As Object, ByVal e As Timers.ElapsedEventArgs) Handles m_FastaTimer.Elapsed

		If DateTime.UtcNow.Subtract(m_FastaGenStartTime).TotalMinutes >= FASTA_GEN_TIMEOUT_INTERVAL_MINUTES Then
			m_FastaGenTimeOut = True	  'Set the timeout flag so an error will be reported
			m_GenerationComplete = True		'Set the completion flag so the fasta generation wait loop will exit
		End If

	End Sub
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <remarks></remarks>
	Public Sub New()
		m_CDTAUtilities = New clsCDTAUtilities
	End Sub

	''' <summary>
	''' Initialize class
	''' </summary>
	''' <param name="mgrParams">Manager parameter object</param>
	''' <param name="jobParams">Job parameter object</param>
	''' <remarks></remarks>
	Public Overridable Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams) Implements IAnalysisResources.Setup
		Dim statusTools As IStatusFile = Nothing
		Me.Setup(mgrParams, jobParams, statusTools)
	End Sub

	''' <summary>
	''' Initialize class
	''' </summary>
	''' <param name="mgrParams">Manager parameter object</param>
	''' <param name="jobParams">Job parameter object</param>
	''' <remarks></remarks>
	Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal statusTools As IStatusFile) Implements IAnalysisResources.Setup
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

		m_MyEMSLDatasetListInfo = New MyEMSLReader.DatasetListInfo()
		m_MyEMSLDatasetListInfo.AddDataset(m_DatasetName)

		m_RecentlyFoundMyEMSLFiles = New List(Of MyEMSLReader.DatasetFolderOrFileInfo)

		m_StatusTools = statusTools

	End Sub

	Public MustOverride Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

	Protected Function AddFileToMyEMSLDownloadQueue(ByVal encodedFilePath As String) As Boolean

		Dim myEMSLFileID As Int64 = MyEMSLReader.DatasetInfo.ExtractMyEMSLFileID(encodedFilePath)

		If myEMSLFileID > 0 Then

			Dim fileInfo As MyEMSLReader.ArchivedFileInfo = Nothing

			If GetCachedArchivedFileInfo(myEMSLFileID, fileInfo) Then
				m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(fileInfo)
				Return True
			Else
				m_message = "Cached ArchiveFileInfo does not contain MyEMSL File ID " & myEMSLFileID
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

		Else
			m_message = "MyEMSL File ID not found in path: " & encodedFilePath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

	End Function

	''' <summary>
	''' Appends file specified file path to the JobInfo file for the given Job
	''' </summary>
	''' <param name="intJob"></param>
	''' <param name="strFilePath"></param>
	''' <remarks></remarks>
	Protected Sub AppendToJobInfoFile(ByVal intJob As Integer, ByVal strFilePath As String)

		Dim strJobInfoFilePath As String = GetJobInfoFilePath(intJob)

		Using swJobInfoFile As StreamWriter = New StreamWriter(New FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
			swJobInfoFile.WriteLine(strFilePath)
		End Using

	End Sub

	''' <summary>
	''' Copies the zipped s-folders to the working directory
	''' </summary>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified files, and instead creates a series of files named s*.zip_StoragePathInfo.txt, and each file's first line will be the full path to the source file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function CopySFoldersToWorkDir(ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim DSFolderPath As String = FindValidFolder(m_DatasetName, "s*.zip", RetrievingInstrumentDataFolder:=True)

		Dim ZipFiles() As String
		Dim DestFilePath As String

		'Verify dataset folder exists
		If Not Directory.Exists(DSFolderPath) Then Return False

		'Get a listing of the zip files to process
		ZipFiles = Directory.GetFiles(DSFolderPath, "s*.zip")
		If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

		'copy each of the s*.zip files to the working directory
		For Each ZipFilePath As String In ZipFiles

			If m_DebugLevel > 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " + ZipFilePath + " to work directory")
			End If

			DestFilePath = Path.Combine(m_WorkingDir, Path.GetFileName(ZipFilePath))

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
		Const MaxCopyAttempts As Integer = 3
		Return CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, MaxCopyAttempts)
	End Function

	''' <summary>
	''' Copies a file with retries in case of failure
	''' </summary>
	''' <param name="SrcFilePath">Full path to source file</param>
	''' <param name="DestFilePath">Full path to destination file</param>
	''' <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
	''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
	''' <returns>TRUE for success; FALSE for error</returns>
	''' <remarks>Logs copy errors</remarks>
	Private Function CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, ByVal Overwrite As Boolean, ByVal MaxCopyAttempts As Integer) As Boolean

		Const RETRY_HOLDOFF_SECONDS As Integer = 15

		If MaxCopyAttempts < 1 Then MaxCopyAttempts = 1
		Dim RetryCount As Integer = MaxCopyAttempts

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

				If Not Overwrite AndAlso File.Exists(DestFilePath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Tried to overwrite an existing file when Overwrite = False: " + DestFilePath)
					Return False
				End If

				Threading.Thread.Sleep(RETRY_HOLDOFF_SECONDS * 1000)	   'Wait several seconds before retrying
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
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function CopyFileToWorkDir(ByVal InpFile As String, ByVal InpFolder As String, ByVal OutDir As String) As Boolean
		Const MaxCopyAttempts As Integer = 3
		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function CopyFileToWorkDir(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

		Const MaxCopyAttempts As Integer = 3
		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)

	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function CopyFileToWorkDir(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal MaxCopyAttempts As Integer) As Boolean

		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)

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
	''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function CopyFileToWorkDir(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Const MaxCopyAttempts As Integer = 3
		Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, CreateStoragePathInfoOnly, MaxCopyAttempts)

	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <param name="CreateStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
	''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function CopyFileToWorkDir(
	  ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal CreateStoragePathInfoOnly As Boolean, _
	  ByVal MaxCopyAttempts As Integer) As Boolean

		Dim SourceFile As String = String.Empty
		Dim DestFilePath As String

		Try

			If InpFolder.StartsWith(MYEMSL_PATH_FLAG) Then
				Return AddFileToMyEMSLDownloadQueue(InpFolder)
			End If

			SourceFile = Path.Combine(InpFolder, InpFile)
			DestFilePath = Path.Combine(OutDir, InpFile)

			'Verify source file exists
			Const HoldoffSeconds As Integer = 1
			Const MaxAttempts As Integer = 1
			If Not FileExistsWithRetry(SourceFile, HoldoffSeconds, eLogMsgTypeIfNotFound, MaxAttempts) Then
				m_message = "File not found: " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
				Return False
			End If

			If CreateStoragePathInfoOnly Then
				' Create a storage path info file
				Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
			End If

			If CopyFileWithRetry(SourceFile, DestFilePath, True, MaxCopyAttempts) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "CopyFileToWorkDir, File copied: " + SourceFile
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
	Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String) As Boolean
		Const MaxCopyAttempts As Integer = 3
		Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
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
	Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean
		Const MaxCopyAttempts As Integer = 3
		Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' ''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal MaxCopyAttempts As Integer) As Boolean
		Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
	End Function

	''' <summary>
	''' Copies specified file from storage server to local working directory, renames destination with dataset name
	''' </summary>
	''' <param name="InpFile">Name of file to copy</param>
	''' <param name="InpFolder">Path to folder where input file is located</param>
	''' <param name="OutDir">Destination directory for file copy</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified file, and instead creates a file named FileName_StoragePathInfo.txt, and this file's first line will be the full path to the source file</param>
	''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
	  ByVal InpFolder As String, _
	  ByVal OutDir As String, _
	  ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
	  ByVal CreateStoragePathInfoOnly As Boolean, _
	  ByVal MaxCopyAttempts As Integer) As Boolean


		Dim SourceFile As String = String.Empty
		Dim DestFilePath As String

		Try
			SourceFile = Path.Combine(InpFolder, InpFile)

			'Verify source file exists
			If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
				m_message = "File not found: " + SourceFile
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
				Return False
			End If

			Dim Fi As New FileInfo(SourceFile)
			Dim TargetName As String = m_DatasetName + Fi.Extension
			DestFilePath = Path.Combine(OutDir, TargetName)

			If CreateStoragePathInfoOnly Then
				' Create a storage path info file
				Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
			End If

			If CopyFileWithRetry(SourceFile, DestFilePath, True, MaxCopyAttempts) Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "CopyFileToWorkDirWithRename, File copied: " + SourceFile
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
		Dim OrgDBDescription As String

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating fasta file at " & DestFolder)
		End If

		If Not Directory.Exists(DestFolder) Then
			Directory.CreateDirectory(DestFolder)
		End If

		'Instantiate fasta tool if not already done
		If m_FastaTools Is Nothing Then
			If String.IsNullOrWhiteSpace(m_FastaToolsCnStr) Then
				m_message = "Protein database connection string not specified"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Error in CreateFastaFile: " + m_message)
				Return False
			End If
			m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
		End If

		'Initialize fasta generation state variables
		m_GenerationStarted = False
		m_GenerationComplete = False
		m_FastaFileName = String.Empty

		'Set up variables for fasta creation call
		Dim LegacyFasta As String = m_jobParams.GetParam("LegacyFastaFileName")
		Dim CreationOpts As String = m_jobParams.GetParam("ProteinOptions")
		Dim CollectionList As String = m_jobParams.GetParam("ProteinCollectionList")
		Dim usingLegacyFasta = False

		If Not String.IsNullOrWhiteSpace(CollectionList) AndAlso Not CollectionList.ToLower() = "na" Then
			OrgDBDescription = "Protein collection: " + CollectionList + " with options " + CreationOpts
		ElseIf Not String.IsNullOrWhiteSpace(LegacyFasta) AndAlso Not LegacyFasta.ToLower() = "na" Then
			OrgDBDescription = "Legacy DB: " + LegacyFasta
			usingLegacyFasta = True
		Else
			m_message = "Both the ProteinCollectionList and LegacyFastaFileName parameters are empty or 'na'; unable to obtain Fasta file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateFastaFile: " + m_message)
			Return False
		End If

		Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)
		Dim legacyFastaToUse As String

		If splitFastaEnabled Then

			If Not usingLegacyFasta Then
				m_message = "Cannot use protein collections when running a SplitFasta job; choose a Legacy fasta file instead"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Running a SplitFasta job; need to update the name of the fasta file to be of the form FastaFileName_NNx_nn.fasta
			' where NN is the number of total cloned steps and nn is this job's specific step number
			Dim numberOfClonedSteps As Integer

			legacyFastaToUse = GetSplitFastaFileName(m_jobParams, m_message, numberOfClonedSteps)

			If String.IsNullOrEmpty(legacyFastaToUse) Then
				' The error should have already been logged
				Return False
			End If

			OrgDBDescription = "Legacy DB: " + legacyFastaToUse

			' Lookup connection strings
			Dim proteinSeqsDBConnectionString = m_mgrParams.GetParam("fastacnstring")
			If String.IsNullOrWhiteSpace(proteinSeqsDBConnectionString) Then
				m_message = "Error in CreateFastaFile: manager parameter fastacnstring is not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			Dim dmsConnectionString = m_mgrParams.GetParam("connectionstring")
			If String.IsNullOrWhiteSpace(proteinSeqsDBConnectionString) Then
				m_message = "Error in CreateFastaFile: manager parameter connectionstring is not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			' Lookup the MSGFPlus Index Folder path
			Dim strMSGFPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", "\\Proto-7\MSGFPlus_Index_Files")
			If String.IsNullOrWhiteSpace(strMSGFPlusIndexFilesFolderPathLegacyDB) Then
				strMSGFPlusIndexFilesFolderPathLegacyDB = "\\Proto-7\MSGFPlus_Index_Files\Other"
			Else
				strMSGFPlusIndexFilesFolderPathLegacyDB = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other")
			End If

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Verifying that split fasta file exists: " & legacyFastaToUse)
			End If

			' Make sure the original fasta file has already been split into the appropriate number parts
			' and that DMS knows about them
			'
			m_SplitFastaFileUtility = New clsSplitFastaFileUtilities(dmsConnectionString, proteinSeqsDBConnectionString, numberOfClonedSteps)
			m_SplitFastaFileUtility.MSGFPlusIndexFilesFolderPathLegacyDB = strMSGFPlusIndexFilesFolderPathLegacyDB

			m_SplitFastaLastUpdateTime = DateTime.UtcNow
			m_SplitFastaLastPercentComplete = 0

			Dim success = m_SplitFastaFileUtility.ValidateSplitFastaFile(LegacyFasta, legacyFastaToUse)
			If Not success Then
				m_message = m_SplitFastaFileUtility.ErrorMessage
				Return False
			End If

		Else
			legacyFastaToUse = String.Copy(LegacyFasta)
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ProteinCollectionList=" + CollectionList + "; CreationOpts=" + CreationOpts + "; LegacyFasta=" + legacyFastaToUse)
		End If

		' Setup a timer to prevent an infinite loop if there's a fasta generation problem
		m_FastaTimer = New Timers.Timer
		m_FastaTimer.Interval = 5000
		m_FastaTimer.AutoReset = True

		' Note that m_FastaTools does not spawn a new thread
		'   Since it does not spawn a new thread, the while loop after this Try block won't actually get reached while m_FastaTools.ExportFASTAFile is running
		'   Furthermore, even if m_FastaTimer_Elapsed sets m_FastaGenTimeOut to True, this won't do any good since m_FastaTools.ExportFASTAFile will still be running
		m_FastaGenTimeOut = False
		m_FastaGenStartTime = DateTime.UtcNow
		Try
			m_FastaTimer.Start()
			HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, legacyFastaToUse, DestFolder)
		Catch Ex As Exception
			m_message = "Exception generating OrgDb file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception generating OrgDb file; " + OrgDBDescription + "; " + Ex.Message + "; " + clsGlobal.GetExceptionStackTrace(Ex))
			Return False
		End Try

		' Wait for fasta creation to finish
		While Not (m_GenerationComplete Or m_FastaGenTimeOut)
			Threading.Thread.Sleep(2000)
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

		If String.IsNullOrEmpty(m_FastaFileName) Then
			' Fasta generator never raised event FileGenerationCompleted
			m_message = "m_FastaTools did not raise event FileGenerationCompleted; unable to continue; " + OrgDBDescription
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Dim fiFastaFile As FileInfo
		Dim strFastaFileMsg As String
		fiFastaFile = New FileInfo(Path.Combine(DestFolder, m_FastaFileName))

		If m_DebugLevel >= 1 Then
			' Log the name of the .Fasta file we're using
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Fasta generation complete, using database: " + m_FastaFileName)

			If m_DebugLevel >= 2 Then
				' Also log the file creation and modification dates
				Try

					strFastaFileMsg = "Fasta file last modified: " + GetHumanReadableTimeInterval(DateTime.UtcNow.Subtract(fiFastaFile.LastWriteTimeUtc)) + " ago at " + fiFastaFile.LastWriteTime.ToString()
					strFastaFileMsg &= "; file created: " + GetHumanReadableTimeInterval(DateTime.UtcNow.Subtract(fiFastaFile.CreationTimeUtc)) + " ago at " + fiFastaFile.CreationTime.ToString()
					strFastaFileMsg &= "; file size: " + fiFastaFile.Length.ToString() + " bytes"

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strFastaFileMsg)
				Catch ex As Exception
					' Ignore errors here
				End Try
			End If

		End If

		' Create/Update the .LastUsed file for the newly created Fasta File
		Dim lastUsedFilePath = fiFastaFile.FullName & ".LastUsed"
		Try
			Using swLastUsedFile = New StreamWriter(New FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
				swLastUsedFile.WriteLine(DateTime.UtcNow.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT))
			End Using
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Warning: unable to create a new .LastUsed file at " & lastUsedFilePath & ": " & ex.Message)
		End Try

		' If we got to here, everything worked OK
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

		Dim strInfoFilePath As String = String.Empty

		Try
			If SourceFilePath Is Nothing Or DestFilePath Is Nothing Then
				Return False
			End If

			strInfoFilePath = DestFilePath + STORAGE_PATH_INFO_FILE_SUFFIX

			Using swOutFile = New StreamWriter(New FileStream(strInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
				swOutFile.WriteLine(SourceFilePath)
			End Using

		Catch ex As Exception
			m_message = "Exception in CreateStoragePathInfoFile for " + strInfoFilePath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)

			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Given two dates, returns the most recent date
	''' </summary>
	''' <param name="date1"></param>
	''' <param name="date2"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Shared Function DateMax(ByVal date1 As Date, ByVal date2 As Date) As Date
		If date1 > date2 Then
			Return date1
		Else
			Return date2
		End If
	End Function

	''' <summary>
	''' Tries to delete the first file whose path is defined in strFilesToDelete
	''' If deletion succeeds, then removes the file from the queue
	''' </summary>
	''' <param name="strFilesToDelete">Queue of files to delete (full file paths)</param>
	''' <param name="strFileToQueueForDeletion">Optional: new file to add to the queue; blank to do nothing</param>
	''' <remarks></remarks>
	Protected Sub DeleteQueuedFiles(ByRef strFilesToDelete As Queue(Of String), ByVal strFileToQueueForDeletion As String)

		If strFilesToDelete.Count > 0 Then
			' Call the garbage collector, then try to delete the first queued file
			' Note, do not call WaitForPendingFinalizers since that could block this thread
			' Thus, do not use PRISM.Processes.clsProgRunner.GarbageCollectNow
			GC.Collect()

			Try
				Dim strFileToDelete As String
				strFileToDelete = strFilesToDelete.Peek()

				File.Delete(strFileToDelete)

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
	Private Function FileExistsWithRetry(ByVal FileName As String, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

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
	Private Function FileExistsWithRetry(ByVal FileName As String, ByVal RetryHoldoffSeconds As Integer, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

		Const MaxAttempts As Integer = 3
		Return FileExistsWithRetry(FileName, RetryHoldoffSeconds, eLogMsgTypeIfNotFound, MaxAttempts)

	End Function

	''' <summary>
	''' Test for file existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FileName"></param>
	''' <param name="RetryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
	''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function FileExistsWithRetry(ByVal FileName As String, ByVal RetryHoldoffSeconds As Integer, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, ByVal MaxAttempts As Integer) As Boolean

		Dim RetryCount As Integer = MaxAttempts
		If RetryCount < 1 Then RetryCount = 1

		If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS
		If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

		While RetryCount > 0
			If File.Exists(FileName) Then
				Return True
			Else
				If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
					' Only log each failed attempt to find the file if eLogMsgTypeIfNotFound = ILogger.logMsgType.logError
					' Otherwise, we won't log each failed attempt
					Dim ErrMsg As String = "File " + FileName + " not found. Retry count = " + RetryCount.ToString
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, ErrMsg)
				End If
				RetryCount -= 1
				If RetryCount > 0 Then
					Threading.Thread.Sleep(New TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
				End If
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
		Const CreateStoragePathInfoFile As Boolean = False

		' Look for the file in the various folders
		FolderName = FindDataFile(FileName, SearchArchivedDatasetFolder)

		' Exit if file was not found
		If String.IsNullOrEmpty(FolderName) Then
			' No folder found containing the specified file
			Return False
		End If

		If FolderName.StartsWith(MYEMSL_PATH_FLAG) Then
			Return AddFileToMyEMSLDownloadQueue(FolderName)
		End If

		' Copy the file
		If Not CopyFileToWorkDir(FileName, FolderName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoFile) Then
			Return False
		End If

		'Return or unzip file, as specified
		If Not Unzip Then Return True

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + FileName)
		If UnzipFileStart(Path.Combine(m_WorkingDir, FileName), m_WorkingDir, "FindAndRetrieveMiscFiles", False) Then
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
	''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function FindDataFile(ByVal FileToFind As String) As String
		Return FindDataFile(FileToFind, SearchArchivedDatasetFolder:=True)
	End Function

	''' <summary>
	''' Finds the server or archive folder where specified file is located
	''' </summary>
	''' <param name="FileToFind">Name of the file to search for</param>
	''' <param name="SearchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
	''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
	''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
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
	''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
	Protected Function FindDataFile(ByVal FileToFind As String, ByVal SearchArchivedDatasetFolder As Boolean, ByVal LogFileNotFound As Boolean) As String

		Dim FoldersToSearch As New List(Of String)

		' ReSharper disable once RedundantAssignment
		Dim TempDir As String = String.Empty

		Dim FileFound As Boolean = False

		Dim strParentFolderPaths As List(Of String)
		Dim strDatasetFolderName As String
		Dim strInputFolderName As String

		Dim strSharedResultFolders As String

		Dim SharedResultFolderNames As New List(Of String)

		Try
			' Fill collection with possible folder locations
			' The order of searching is:
			'  a. Check the "inputFolderName" and then each of the Shared Results Folders in the Transfer folder
			'  b. Check the "inputFolderName" and then each of the Shared Results Folders in the Dataset folder
			'  c. Check the "inputFolderName" and then each of the Shared Results Folders in MyEMSL for this dataset
			'  d. Check the "inputFolderName" and then each of the Shared Results Folders in the Archived dataset folder
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

			strParentFolderPaths = New List(Of String)
			strParentFolderPaths.Add(m_jobParams.GetParam("transferFolderPath"))
			strParentFolderPaths.Add(m_jobParams.GetParam("DatasetStoragePath"))

			If SearchArchivedDatasetFolder Then
				strParentFolderPaths.Add(MYEMSL_PATH_FLAG)
				strParentFolderPaths.Add(m_jobParams.GetParam("DatasetArchivePath"))
			End If

			For Each strParentFolderPath As String In strParentFolderPaths

				If Not String.IsNullOrEmpty(strParentFolderPath) Then
					If Not String.IsNullOrEmpty(strInputFolderName) Then
						FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strInputFolderName))	' Parent Folder \ Dataset Folder \ Input folder
					End If

					For Each strSharedFolderName As String In SharedResultFolderNames
						FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strSharedFolderName))	' Parent Folder \ Dataset Folder \  Shared results folder
					Next

					FoldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, String.Empty))				' Parent Folder \ Dataset Folder
				End If

			Next

			' Now search for FileToFind in each folder in FoldersToSearch
			For Each TempDir In FoldersToSearch
				Try
					Dim diFolderToCheck = New DirectoryInfo(TempDir)

					If TempDir.StartsWith(MYEMSL_PATH_FLAG) Then

						If (Not m_MyEMSLDatasetListInfo.ContainsDataset(m_DatasetName)) Then
							m_MyEMSLDatasetListInfo.AddDataset(m_DatasetName)
						End If

						m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(FileToFind, diFolderToCheck.Name, m_DatasetName, recurse:=False)

						If m_RecentlyFoundMyEMSLFiles.Count > 0 Then
							FileFound = True

							' Include the MyEMSL FileID in TempDir so that it is available for downloading
							TempDir = MyEMSLReader.DatasetInfo.AppendMyEMSLFileID(TempDir, m_RecentlyFoundMyEMSLFiles.First().FileID)
							Exit For
						End If

					Else

						If diFolderToCheck.Exists Then
							If File.Exists(Path.Combine(TempDir, FileToFind)) Then
								FileFound = True
								Exit For
							End If
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

		strTargetFolderPath = Path.Combine(strParentFolderPath, strDatasetFolderName)
		If Not String.IsNullOrEmpty(strInputFolderName) Then
			strTargetFolderPath = Path.Combine(strTargetFolderPath, strInputFolderName)
		End If

		Return strTargetFolderPath

	End Function

	''' <summary>
	''' Looks for file strFileName in strFolderPath or any of its subfolders
	''' The filename may contain a wildcard character, in which case the first match will be returned
	''' </summary>
	''' <param name="strFolderPath">Folder path to examine</param>
	''' <param name="strFileName">File name to find</param>
	''' <returns>Full path to the file, if found; empty string if no match</returns>
	''' <remarks></remarks>
	Public Shared Function FindFileInDirectoryTree(ByVal strFolderPath As String, ByVal strFileName As String) As String
		Return FindFileInDirectoryTree(strFolderPath, strFileName, New SortedSet(Of String))
	End Function

	''' <summary>
	''' Looks for file strFileName in strFolderPath or any of its subfolders
	''' The filename may contain a wildcard character, in which case the first match will be returned
	''' </summary>
	''' <param name="strFolderPath">Folder path to examine</param>
	''' <param name="strFileName">File name to find</param>
	''' <param name="lstFolderNamesToSkip">List of folder names that should not be examined</param>
	''' <returns>Full path to the file, if found; empty string if no match</returns>
	''' <remarks></remarks>
	Public Shared Function FindFileInDirectoryTree(ByVal strFolderPath As String, ByVal strFileName As String, ByVal lstFolderNamesToSkip As SortedSet(Of String)) As String
		Dim ioFolder As DirectoryInfo
		Dim ioFile As FileSystemInfo
		Dim ioSubFolder As FileSystemInfo

		Dim strFilePathMatch As String

		ioFolder = New DirectoryInfo(strFolderPath)

		If ioFolder.Exists Then
			' Examine the files for this folder
			For Each ioFile In ioFolder.GetFiles(strFileName)
				strFilePathMatch = ioFile.FullName
				Return strFilePathMatch
			Next

			' Match not found
			' Recursively call this function with the subdirectories in this folder

			For Each ioSubFolder In ioFolder.GetDirectories
				If Not lstFolderNamesToSkip.Contains(ioSubFolder.Name) Then
					strFilePathMatch = FindFileInDirectoryTree(ioSubFolder.FullName, strFileName)
					If Not String.IsNullOrEmpty(strFilePathMatch) Then
						Return strFilePathMatch
					End If
				End If
			Next
		End If

		Return String.Empty

	End Function

	''' <summary>
	''' Determines the full path to the dataset file
	''' Returns a folder path for data that is stored in folders (e.g. .D folders)
	''' For instruments with multiple data folders, returns the path to the first folder
	''' For instrument with multiple zipped data files, returns the dataset folder path
	''' </summary>
	''' <param name="blnIsFolder">Output variable: true if the path returned is a folder path; false if a file</param>
	''' <returns>The full path to the dataset file or folder</returns>
	''' <remarks></remarks>
	Protected Function FindDatasetFileOrFolder(ByRef blnIsFolder As Boolean) As String

		Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
		Dim StoragePath As String = m_jobParams.GetParam("DatasetStoragePath")
		Dim eRawDataType As eRawDataTypeConstants
		Dim strFileOrFolderPath As String = String.Empty

		blnIsFolder = False

		eRawDataType = GetRawDataType(RawDataType)
		Select Case eRawDataType
			Case eRawDataTypeConstants.AgilentDFolder			'Agilent ion trap data

				If StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) OrElse _
				   StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()) Then
					' For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005, 
					'  we would pre-process the data beforehand to create MGF files
					' The following call can be used to retrieve the files
					strFileOrFolderPath = FindMGFFile()
				Else
					' DeconTools_V2 now supports reading the .D files directly
					' Call RetrieveDotDFolder() to copy the folder and all subfolders
					strFileOrFolderPath = FindDotDFolder()
					blnIsFolder = True
				End If

			Case eRawDataTypeConstants.AgilentQStarWiffFile			'Agilent/QSTAR TOF data
				strFileOrFolderPath = FindDatasetFile(DOT_WIFF_EXTENSION)

			Case eRawDataTypeConstants.ZippedSFolders			'FTICR data
				strFileOrFolderPath = FindSFolders()
				blnIsFolder = True

			Case eRawDataTypeConstants.ThermoRawFile			'Finnigan ion trap/LTQ-FT data
				strFileOrFolderPath = FindDatasetFile(DOT_RAW_EXTENSION)

			Case eRawDataTypeConstants.MicromassRawFolder			'Micromass QTOF data
				strFileOrFolderPath = FindDotRawFolder()
				blnIsFolder = True

			Case eRawDataTypeConstants.UIMF			'IMS UIMF data
				strFileOrFolderPath = FindDatasetFile(DOT_UIMF_EXTENSION)

			Case eRawDataTypeConstants.mzXML
				strFileOrFolderPath = FindDatasetFile(DOT_MZXML_EXTENSION)

			Case eRawDataTypeConstants.mzML
				strFileOrFolderPath = FindDatasetFile(DOT_MZML_EXTENSION)

			Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
				' Call RetrieveDotDFolder() to copy the folder and all subfolders

				' Both the MSXml step tool and DeconTools require the .Baf file
				' We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, so we need the file

				strFileOrFolderPath = FindDotDFolder()
				blnIsFolder = True

			Case eRawDataTypeConstants.BrukerMALDIImaging
				strFileOrFolderPath = FindBrukerMALDIImagingFolders()
				blnIsFolder = True

		End Select

		Return strFileOrFolderPath

	End Function

	''' <summary>
	''' Finds the dataset folder containing Bruker Maldi imaging .zip files
	''' </summary>
	''' <returns>The full path to the dataset folder</returns>
	''' <remarks></remarks>
	Public Function FindBrukerMALDIImagingFolders() As String

		Const ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK As String = "*R*X*.zip"

		' Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
		' If a matching folder isn't found, then ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"

		Dim DSFolderPath As String
		DSFolderPath = FindValidFolder(m_DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK, RetrievingInstrumentDataFolder:=True)
		If String.IsNullOrEmpty(DSFolderPath) Then Return String.Empty

		Return DSFolderPath

	End Function


	''' <summary>
	''' Finds a file named DatasetName.FileExtension
	''' </summary>
	''' <param name="FileExtension"></param>
	''' <returns>The full path to the folder; an empty string if no match</returns>
	''' <remarks></remarks>
	Protected Function FindDatasetFile(ByVal FileExtension As String) As String

		If Not FileExtension.StartsWith(".") Then
			FileExtension = "." + FileExtension
		End If

		Dim DataFileName As String = m_DatasetName + FileExtension
		Dim DSFolderPath As String = FindValidFolder(m_DatasetName, DataFileName)

		If Not String.IsNullOrEmpty(DSFolderPath) Then
			Return Path.Combine(DSFolderPath, DataFileName)
		Else
			Return String.Empty
		End If

	End Function

	''' <summary>
	''' Finds a .Raw folder below the dataset folder
	''' </summary>
	''' <returns>The full path to the folder; an empty string if no match</returns>
	''' <remarks></remarks>
	Protected Function FindDotDFolder() As String
		Return FindDotXFolder(DOT_D_EXTENSION)
	End Function

	''' <summary>
	''' Finds a .D folder below the dataset folder
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function FindDotRawFolder() As String
		Return FindDotXFolder(DOT_RAW_EXTENSION)
	End Function

	''' <summary>
	''' Finds a subfolder (typically Dataset.D or Dataset.Raw) below the dataset folder
	''' </summary>
	''' <param name="FolderExtension"></param>
	''' <returns>The full path to the folder; an empty string if no match</returns>
	''' <remarks></remarks>
	Protected Function FindDotXFolder(ByVal FolderExtension As String) As String

		If Not FolderExtension.StartsWith(".") Then
			FolderExtension = "." + FolderExtension
		End If

		Dim FileNameToFind As String = String.Empty
		Dim FolderExtensionWildcard As String = "*" + FolderExtension
		Dim ServerPath As String = FindValidFolder(m_DatasetName, FileNameToFind, FolderExtensionWildcard, RetrievingInstrumentDataFolder:=True)

		Dim diDatasetFolder As DirectoryInfo = New DirectoryInfo(ServerPath)

		'Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
		For Each diSubFolder As DirectoryInfo In diDatasetFolder.GetDirectories(FolderExtensionWildcard)
			Return diSubFolder.FullName
		Next

		' No match found
		Return String.Empty

	End Function

	''' <summary>
	''' Finds the dataset folder containing either a 0.ser subfolder or containing zipped S-folders
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function FindSFolders() As String

		' First Check for the existence of a 0.ser Folder
		Dim FileNameToFind As String = String.Empty
		Dim DSFolderPath As String = FindValidFolder(m_DatasetName, FileNameToFind, BRUKER_ZERO_SER_FOLDER, RetrievingInstrumentDataFolder:=True)

		If Not String.IsNullOrEmpty(DSFolderPath) Then
			Return Path.Combine(DSFolderPath, BRUKER_ZERO_SER_FOLDER)
		End If

		' The 0.ser folder does not exist; look for zipped s-folders
		DSFolderPath = FindValidFolder(m_DatasetName, "s*.zip", RetrievingInstrumentDataFolder:=True)

		Return DSFolderPath

	End Function

	''' <summary>
	''' Finds the best .mgf file for the current dataset
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function FindMGFFile() As String

		' Data files are in a subfolder off of the main dataset folder
		' Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

		Dim ServerPath As String = FindValidFolder(m_DatasetName, "", "*" + DOT_D_EXTENSION)

		Dim diServerFolder As DirectoryInfo = New DirectoryInfo(ServerPath)

		'Get a list of the subfolders in the dataset folder		
		'Go through the folders looking for a file with a ".mgf" extension
		For Each diSubFolder As DirectoryInfo In diServerFolder.GetDirectories()

			For Each fiFile As FileInfo In diSubFolder.GetFiles("*" + DOT_MGF_EXTENSION)
				' Return the first .mgf file that was found
				Return fiFile.FullName
			Next
		Next

		' No match was found
		Return String.Empty

	End Function

	''' <summary>
	''' Looks for the .mzXML file for this dataset
	''' </summary>
	''' <param name="strHashcheckFilePath">Output parameter: path to the hashcheck file if the .mzXML file was found in the MSXml cache</param>
	''' <returns>Full path to the file, if found; empty string if no match</returns>
	''' <remarks></remarks>
	Protected Function FindMZXmlFile(ByRef strHashcheckFilePath As String) As String

		' Finds this dataset's .mzXML file		
		Dim DatasetID As String = m_jobParams.GetParam("JobParameters", "DatasetID")

		Const MSXmlFoldernameBase As String = "MSXML_Gen_1_"
		Dim MzXMLFilename As String = m_DatasetName + ".mzXML"

		Const MaxRetryCount As Integer = 1

		Dim lstValuesToCheck As List(Of Integer)
		lstValuesToCheck = New List(Of Integer)

		' Initialize the values we'll look for
		' Note that these values are added to the list in the order of the preferred file to retrieve
		lstValuesToCheck.Add(154)			' MSXML_Gen_1_154_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=250; MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(132)			' MSXML_Gen_1_132_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=150; MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(93)			' MSXML_Gen_1_93_DatasetID,    CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;
		lstValuesToCheck.Add(126)			' MSXML_Gen_1_126_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML; ReAdW_Version=v2.1;
		lstValuesToCheck.Add(39)			' MSXML_Gen_1_39_DatasetID,    CentroidMSXML=False; MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;

		strHashcheckFilePath = String.Empty

		For Each intVersion As Integer In lstValuesToCheck

			Dim MSXmlFoldername As String
			MSXmlFoldername = MSXmlFoldernameBase + intVersion.ToString() + "_" + DatasetID

			' Look for the MSXmlFolder
			' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
			Dim ServerPath As String = FindValidFolder(m_DatasetName, "", MSXmlFoldername, MaxRetryCount, False, RetrievingInstrumentDataFolder:=False)

			If String.IsNullOrEmpty(ServerPath) Then
				Continue For
			End If

			If ServerPath.StartsWith(MYEMSL_PATH_FLAG) Then
				' File found in MyEMSL
				' Determine the MyEMSL FileID by searching for the expected file in m_RecentlyFoundMyEMSLFiles

				Dim myEmslFileID As Int64 = 0

				For Each udtArchivedFile In m_RecentlyFoundMyEMSLFiles
					Dim fiArchivedFile As New FileInfo(udtArchivedFile.FileInfo.RelativePathWindows)
					If String.Equals(fiArchivedFile.Name, MzXMLFilename, StringComparison.CurrentCultureIgnoreCase) Then
						myEmslFileID = udtArchivedFile.FileID
						Exit For
					End If
				Next

				If myEmslFileID > 0 Then
					Return Path.Combine(ServerPath, MSXmlFoldername, MyEMSLReader.DatasetInfo.AppendMyEMSLFileID(MzXMLFilename, myEmslFileID))
				End If
			Else

				' Due to quirks with how FindValidFolder behaves, we need to confirm that the mzXML file actually exists
				Dim diFolderInfo As DirectoryInfo
				diFolderInfo = New DirectoryInfo(ServerPath)

				If diFolderInfo.Exists Then

					'See if the ServerPath folder actually contains a subfolder named MSXmlFoldername
					Dim diSubfolders() As DirectoryInfo = diFolderInfo.GetDirectories(MSXmlFoldername)
					If diSubfolders.Length > 0 Then

						' MSXmlFolder found; return the path to the file     
						Return Path.Combine(diSubfolders(0).FullName, MzXMLFilename)

					End If

				End If

			End If

		Next

		' If we get here, then no match was found
		' Lookup the MSXML cache path (typically \\proto-6\MSXML_Cache\ )
		Dim strMSXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
		Dim diCacheFolder As DirectoryInfo = New DirectoryInfo(strMSXMLCacheFolderPath)

		If diCacheFolder.Exists Then
			' Look for the file in folders in the MSXML file cache

			Dim strDatasetStoragePath As String = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
			If String.IsNullOrEmpty(strDatasetStoragePath) Then strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")

			Dim strSubfolderToCheck As String
			Dim strMatchedFile As String
			Dim strYearQuarter As String = GetDatasetYearQuarter(strDatasetStoragePath)

			Dim strMSXmlGeneratorName As String = m_jobParams.GetJobParameter("MSXMLGenerator", String.Empty)
			If String.IsNullOrWhiteSpace(strMSXmlGeneratorName) Then
				strSubfolderToCheck = diCacheFolder.FullName
			Else
				strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(strMSXmlGeneratorName)
				strSubfolderToCheck = Path.Combine(diCacheFolder.FullName, strMSXmlGeneratorName, strYearQuarter)
			End If

			strMatchedFile = FindFileInDirectoryTree(strSubfolderToCheck, MzXMLFilename)

			If Not String.IsNullOrEmpty(strMatchedFile) Then
				' Match found; confirm that it has a .hashcheck file and that the information in the .hashcheck file matches the file

				Dim strErrorMessage As String = String.Empty
				strHashcheckFilePath = strMatchedFile & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX

				If clsGlobal.ValidateFileVsHashcheck(strMatchedFile, strHashcheckFilePath, strErrorMessage) Then

					Return strMatchedFile
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
					Return String.Empty
				End If

			End If

		End If

		Return String.Empty

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
	Protected Function FindValidFolder(ByVal DSName As String, ByVal FileNameToFind As String) As String

		Return FindValidFolder(DSName, FileNameToFind, "", DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True, RetrievingInstrumentDataFolder:=False)

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
	Protected Function FindValidFolder(ByVal DSName As String, ByVal FileNameToFind As String, RetrievingInstrumentDataFolder As Boolean) As String

		Return FindValidFolder(DSName, FileNameToFind, "", DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True, RetrievingInstrumentDataFolder:=RetrievingInstrumentDataFolder)

	End Function


	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
	Protected Function FindValidFolder(ByVal DSName As String,
	  ByVal FileNameToFind As String,
	  ByVal FolderNameToFind As String) As String

		Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True, RetrievingInstrumentDataFolder:=False)

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
	Protected Function FindValidFolder(ByVal DSName As String,
	  ByVal FileNameToFind As String,
	  ByVal FolderNameToFind As String,
	  ByVal RetrievingInstrumentDataFolder As Boolean) As String

		Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, DEFAULT_MAX_RETRY_COUNT, LogFolderNotFound:=True, RetrievingInstrumentDataFolder:=RetrievingInstrumentDataFolder)

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
	Protected Function FindValidFolder(ByVal DSName As String,
	  ByVal FileNameToFind As String,
	  ByVal FolderNameToFind As String,
	  ByVal MaxRetryCount As Integer) As String

		Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, MaxRetryCount, LogFolderNotFound:=True, RetrievingInstrumentDataFolder:=False)

	End Function

	''' <summary>
	''' Determines the most appropriate folder to use to obtain dataset files from
	''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
	''' If no folder is deemed valid, then returns the path defined by Job Param "DatasetStoragePath"
	''' </summary>
	''' <param name="DSName">Name of the dataset</param>
	''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
	Protected Function FindValidFolder(ByVal DSName As String,
	  ByVal FileNameToFind As String,
	  ByVal FolderNameToFind As String,
	  ByVal MaxRetryCount As Integer,
	  ByVal LogFolderNotFound As Boolean,
	  ByVal RetrievingInstrumentDataFolder As Boolean) As String

		Dim strBestPath As String = String.Empty
		Dim lstPathsToCheck = New List(Of String)

		Dim blnValidFolder As Boolean
		Dim blnFileNotFoundEncountered As Boolean

		Try
			If FileNameToFind Is Nothing Then FileNameToFind = String.Empty
			If FolderNameToFind Is Nothing Then FolderNameToFind = String.Empty

			Dim instrumentDataPurged = m_jobParams.GetJobParameter("InstrumentDataPurged", 0)

			If RetrievingInstrumentDataFolder AndAlso instrumentDataPurged <> 0 Then
				' The instrument data is purged and we're retrieving instrument data
				' Skip the primary dataset folder since the primary data files were most likely purged
			Else
				lstPathsToCheck.Add(Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), DSName))
			End If

			lstPathsToCheck.Add(MYEMSL_PATH_FLAG)	   ' \\MyEMSL
			lstPathsToCheck.Add(Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), DSName))
			lstPathsToCheck.Add(Path.Combine(m_jobParams.GetParam("transferFolderPath"), DSName))

			blnFileNotFoundEncountered = False

			strBestPath = lstPathsToCheck.First()
			For Each pathToCheck In lstPathsToCheck
				Try
					If m_DebugLevel > 3 Then
						Dim Msg As String = "FindValidDatasetFolder, Looking for folder " + pathToCheck
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
					End If

					If pathToCheck = MYEMSL_PATH_FLAG Then
						Const recurseMyEMSL As Boolean = False
						blnValidFolder = FindValidFolderMyEMSL(DSName, FileNameToFind, FolderNameToFind, False, recurseMyEMSL)
					Else
						blnValidFolder = FindValidFolderUNC(pathToCheck, FileNameToFind, FolderNameToFind, MaxRetryCount, LogFolderNotFound)
					End If

					If blnValidFolder Then
						strBestPath = String.Copy(pathToCheck)
					Else
						blnFileNotFoundEncountered = True
					End If

					If blnValidFolder Then Exit For

				Catch ex As Exception
					m_message = "Exception looking for folder: " + pathToCheck + "; " + clsGlobal.GetExceptionStackTrace(ex)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End Try
			Next

			If blnValidFolder Then

				If m_DebugLevel >= 4 OrElse m_DebugLevel >= 1 AndAlso blnFileNotFoundEncountered Then
					Dim Msg As String = "FindValidFolder, Valid dataset folder has been found:  " + strBestPath
					If FileNameToFind.Length > 0 Then
						Msg &= " (matched file " + FileNameToFind + ")"
					End If
					If FolderNameToFind.Length > 0 Then
						Msg &= " (matched folder " + FolderNameToFind + ")"
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

			Else
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
	''' Determines whether the folder specified by strPathToCheck is appropriate for retrieving dataset files
	''' </summary>
	''' <param name="DSName">Dataset name</param>
	''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <param name="Recurse">True to look for FileNameToFind in all subfolders of a dataset; false to only look in the primary dataset folder</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
	Private Function FindValidFolderMyEMSL(ByVal DSName As String, ByVal FileNameToFind As String, ByVal FolderNameToFind As String, ByVal LogFolderNotFound As Boolean, ByVal Recurse As Boolean) As Boolean

		If String.IsNullOrEmpty(FileNameToFind) Then FileNameToFind = "*"

		If m_DebugLevel > 3 Then
			Const Msg As String = "FindValidFolderMyEMSL, querying MyEMSL for this dataset's files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If

		If (Not m_MyEMSLDatasetListInfo.ContainsDataset(DSName)) Then
			m_MyEMSLDatasetListInfo.AddDataset(DSName)
		End If

		If String.IsNullOrEmpty(FolderNameToFind) Then
			' Simply look for the file
			m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(FileNameToFind, String.Empty, DSName, Recurse)
		Else
			' First look for the subfolder
			' If there are multiple matching subfolders, then choose the newest one
			' The entries in m_RecentlyFoundMyEMSLFiles will be folder entries where the "Filename" field is the folder name while the "SubDirPath" field is any parent folders above the found folder
			m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(FileNameToFind, FolderNameToFind, DSName, Recurse)
		End If

		If m_RecentlyFoundMyEMSLFiles.Count > 0 Then
			Return True
		Else
			If LogFolderNotFound Then
				Dim msg As String = "MyEMSL does not have any files for dataset " & DSName
				If Not String.IsNullOrEmpty(FileNameToFind) Then
					msg &= " and file " & FileNameToFind
				End If

				If Not String.IsNullOrEmpty(FolderNameToFind) Then
					msg &= " and subfolder " & FolderNameToFind
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
			End If
			Return False
		End If

	End Function

	''' <summary>
	''' Determines whether the folder specified by strPathToCheck is appropriate for retrieving dataset files
	''' </summary>
	''' <param name="PathToCheck">Path to examine</param>
	''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
	''' <param name="FolderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <returns>Path to the most appropriate dataset folder</returns>
	''' <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
	Private Function FindValidFolderUNC(
	  ByVal PathToCheck As String,
	  ByVal FileNameToFind As String,
	  ByVal FolderNameToFind As String,
	  ByVal MaxRetryCount As Integer,
	  ByVal LogFolderNotFound As Boolean) As Boolean

		' First check whether this folder exists
		' Using a 1 second holdoff between retries
		If Not FolderExistsWithRetry(PathToCheck, 1, MaxRetryCount, LogFolderNotFound) Then
			Return False
		End If

		' Folder was found
		Dim blnValidFolder = True

		If m_DebugLevel > 3 Then
			Dim Msg As String = "FindValidFolderUNC, Folder found " + PathToCheck
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
		End If

		' Optionally look for FileNameToFind
		If Not String.IsNullOrEmpty(FileNameToFind) Then

			If FileNameToFind.Contains("*") Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "FindValidFolderUNC, Looking for files matching " + FileNameToFind
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

				' Wildcard in the name
				' Look for any files matching FileNameToFind
				Dim objFolderInfo = New DirectoryInfo(PathToCheck)

				If objFolderInfo.GetFiles(FileNameToFind).Length = 0 Then
					blnValidFolder = False
				End If
			Else
				If m_DebugLevel > 3 Then
					Dim Msg As String = "FindValidFolderUNC, Looking for file named " + FileNameToFind
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

				' Look for file FileNameToFind in this folder
				' Note: Using a 1 second holdoff between retries
				If Not FileExistsWithRetry(Path.Combine(PathToCheck, FileNameToFind), 1, clsLogTools.LogLevels.WARN) Then
					blnValidFolder = False
				End If
			End If
		End If

		' Optionally look for FolderNameToFind
		If blnValidFolder AndAlso Not String.IsNullOrEmpty(FolderNameToFind) Then
			If FolderNameToFind.Contains("*") Then
				If m_DebugLevel > 3 Then
					Dim Msg As String = "FindValidFolderUNC, Looking for folders matching " + FolderNameToFind
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

				' Wildcard in the name
				' Look for any folders matching FolderNameToFind
				Dim objFolderInfo = New DirectoryInfo(PathToCheck)

				If objFolderInfo.GetDirectories(FolderNameToFind).Length = 0 Then
					blnValidFolder = False
				End If
			Else
				If m_DebugLevel > 3 Then
					Dim Msg As String = "FindValidFolderUNC, Looking for folder named " + FolderNameToFind
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

				' Look for folder FolderNameToFind in this folder
				' Note: Using a 1 second holdoff between retries
				If Not FolderExistsWithRetry(Path.Combine(PathToCheck, FolderNameToFind), 1, MaxRetryCount, LogFolderNotFound) Then
					blnValidFolder = False
				End If
			End If
		End If

		Return blnValidFolder

	End Function

	' Obsolete code:
	'
	' ''' <summary>
	' ''' Test for folder existence with a retry loop in case of temporary glitch
	' ''' </summary>
	' ''' <param name="FolderName">Folder name to look for</param>	
	'Private Function FolderExistsWithRetry(ByVal FolderName As String) As Boolean
	'	Return FolderExistsWithRetry(FolderName, DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS, DEFAULT_MAX_RETRY_COUNT, True)
	'End Function

	' ''' <summary>
	' ''' Test for folder existence with a retry loop in case of temporary glitch
	' ''' </summary>
	' ''' <param name="FolderName">Folder name to look for</param>
	' ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	'Private Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer) As Boolean
	'	Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, DEFAULT_MAX_RETRY_COUNT, True)
	'End Function

	' ''' <summary>
	' ''' Test for folder existence with a retry loop in case of temporary glitch
	' ''' </summary>
	' ''' <param name="FolderName">Folder name to look for</param>
	' ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	' ''' <param name="MaxRetryCount">Maximum number of attempts</param>
	'Private Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer, ByVal MaxRetryCount As Integer) As Boolean
	'	Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, MaxRetryCount, True)
	'End Function


	''' <summary>
	''' Test for folder existence with a retry loop in case of temporary glitch
	''' </summary>
	''' <param name="FolderName">Folder name to look for</param>
	''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
	''' <param name="MaxRetryCount">Maximum number of attempts</param>
	''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function FolderExistsWithRetry(ByVal FolderName As String, _
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
			If Directory.Exists(FolderName) Then
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
					Threading.Thread.Sleep(New TimeSpan(0, 0, RetryHoldoffSeconds))		'Wait RetryHoldoffSeconds seconds before retrying
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

		Dim blnSuccess As Boolean

		strRawDataType = m_jobParams.GetParam("RawDataType")

		strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir")
		If String.IsNullOrEmpty(strMSFileInfoScannerDir) Then
			m_message = "Manager parameter 'MSFileInfoScannerDir' is not defined"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
			Return False
		End If

		strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll")
		If Not File.Exists(strMSFileInfoScannerDLLPath) Then
			m_message = "File Not Found: " + strMSFileInfoScannerDLLPath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
			Return False
		End If

		' Confirm that this dataset is a Thermo .Raw file or a .UIMF file
		Select Case GetRawDataType(strRawDataType)
			Case eRawDataTypeConstants.ThermoRawFile
				strInputFilePath = m_DatasetName + DOT_RAW_EXTENSION
			Case eRawDataTypeConstants.UIMF
				strInputFilePath = m_DatasetName + DOT_UIMF_EXTENSION
			Case Else
				m_message = "Invalid dataset type for auto-generating ScanStats.txt file: " + strRawDataType
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GenerateScanStatsFile: " + m_message)
				Return False
		End Select

		strInputFilePath = Path.Combine(m_WorkingDir, strInputFilePath)

		If Not RetrieveSpectra(strRawDataType) Then
			Dim strExtraMsg As String = m_message
			m_message = "Error retrieving spectra file"
			If Not String.IsNullOrWhiteSpace(strExtraMsg) Then
				m_message &= "; " + strExtraMsg
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message)
			Return False
		End If

		If Not ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return False
		End If

		' Make sure the raw data file does not get copied to the results folder
		m_jobParams.AddResultFileToSkip(Path.GetFileName(strInputFilePath))

		Dim objScanStatsGenerator = New clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel)

		' Create the _ScanStats.txt and _ScanStatsEx.txt files
		blnSuccess = objScanStatsGenerator.GenerateScanStatsFile(strInputFilePath, m_WorkingDir)

		If blnSuccess Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generated ScanStats file using " + strInputFilePath)
			End If

			Threading.Thread.Sleep(125)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Try
				File.Delete(strInputFilePath)
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

		Static reRegExRXY As Text.RegularExpressions.Regex
		Static reRegExRX As Text.RegularExpressions.Regex

		Dim reMatch As Text.RegularExpressions.Match
		Dim blnSuccess As Boolean

		If reRegExRXY Is Nothing Then
			reRegExRXY = New Text.RegularExpressions.Regex("R(\d+)X(\d+)Y(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
			reRegExRX = New Text.RegularExpressions.Regex("R(\d+)X(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
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

	Protected Function GetCachedArchivedFileInfo(ByVal myEMSLFileID As Int64, ByRef fileInfoOut As MyEMSLReader.ArchivedFileInfo) As Boolean

		fileInfoOut = Nothing

		Dim fileInfoMatch = (
		  From item In m_RecentlyFoundMyEMSLFiles
		  Where item.FileID = myEMSLFileID
		  Select item.FileInfo).ToList()

		If fileInfoMatch.Count = 0 Then
			Return False
		Else
			fileInfoOut = fileInfoMatch.First()
			Return True
		End If

	End Function

	Protected Function GetCurrentDatasetAndJobInfo() As udtDataPackageJobInfoType

		Dim udtDataPackageJobInfo As udtDataPackageJobInfoType = New udtDataPackageJobInfoType

		With udtDataPackageJobInfo
			.Job = m_jobParams.GetJobParameter("StepParameters", "Job", 0)
			.Dataset = m_jobParams.GetJobParameter("JobParameters", "DatasetNum", m_DatasetName)
			.DatasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0)

			.Instrument = m_jobParams.GetJobParameter("JobParameters", "Instrument", String.Empty)
			.InstrumentGroup = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

			.Experiment = m_jobParams.GetJobParameter("JobParameters", "Experiment", String.Empty)
			.Experiment_Reason = String.Empty
			.Experiment_Comment = String.Empty
			.Experiment_Organism = String.Empty
			.Experiment_NEWT_ID = 0
			.Experiment_NEWT_Name = String.Empty

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
	''' Examines the folder tree in strFolderPath to find the a folder with a name like 2013_2
	''' </summary>
	''' <param name="strFolderPath"></param>
	''' <returns>Matching folder name if found, otherwise an empty string</returns>
	''' <remarks></remarks>
	Public Shared Function GetDatasetYearQuarter(ByVal strFolderPath As String) As String

		If String.IsNullOrEmpty(strFolderPath) Then
			Return String.Empty

		End If

		' Split strFolderPath on the path separator
		Dim lstFolders As List(Of String)
		Dim reYearQuarter As Text.RegularExpressions.Regex = New Text.RegularExpressions.Regex("[0-9]{4}_[0-9]{1,2}", Text.RegularExpressions.RegexOptions.Compiled)
		Dim reMatch As Text.RegularExpressions.Match

		lstFolders = strFolderPath.Split(Path.DirectorySeparatorChar).ToList()
		lstFolders.Reverse()

		For Each strFolder As String In lstFolders
			reMatch = reYearQuarter.Match(strFolder)
			If reMatch.Success Then
				Return reMatch.Value
			End If
		Next

		Return String.Empty

	End Function

	''' <summary>
	''' Reports the amount of free memory on this computer (in MB)
	''' </summary>
	''' <returns>Free memory, in MB</returns>
	Public Shared Function GetFreeMemoryMB() As Single

		Static mFreeMemoryPerformanceCounter As PerformanceCounter

		Dim sngFreeMemory As Single = 0
		Dim blnVirtualMachineOnPIC As Boolean = clsGlobal.UsingVirtualMachineOnPIC()

		Try
			If mFreeMemoryPerformanceCounter Is Nothing Then
				mFreeMemoryPerformanceCounter = New PerformanceCounter("Memory", "Available MBytes")
				mFreeMemoryPerformanceCounter.ReadOnly = True
			End If

			Dim intIterations As Integer = 0
			sngFreeMemory = 0
			Do While sngFreeMemory < Single.Epsilon AndAlso intIterations <= 3
				sngFreeMemory = mFreeMemoryPerformanceCounter.NextValue()
				If sngFreeMemory < Single.Epsilon Then
					' You sometimes have to call .NextValue() several times before it returns a useful number
					' Wait 1 second and then try again
					Threading.Thread.Sleep(1000)
				End If
				intIterations += 1
			Loop

		Catch ex As Exception
			' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
			' A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group in "Local Users and Groups" on the machine showing this error.  
			' Alternatively, add the user to the "Administrators" group.
			' In either case, you will need to reboot the computer for the change to take effect
			If Not blnVirtualMachineOnPIC AndAlso DateTime.Now().Hour = 0 AndAlso DateTime.Now().Minute <= 30 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " + ex.Message)
			End If
		End Try

		Try

			If sngFreeMemory < Single.Epsilon Then
				' The Performance counters are still reporting a value of 0 for available memory; use an alternate method

				If blnVirtualMachineOnPIC Then
					' The Memory performance counters are not available on Windows instances running under VMWare on PIC
				Else
					If DateTime.Now().Hour = 15 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Performance monitor reports 0 MB available; using alternate method: Devices.ComputerInfo().AvailablePhysicalMemory (this message is only logged between 3:00 pm and 4:00 pm)")
					End If
				End If

				sngFreeMemory = CSng(New Devices.ComputerInfo().AvailablePhysicalMemory / 1024.0 / 1024.0)

			End If

		Catch ex As Exception
			If DateTime.Now().Hour = 15 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error determining available memory using Devices.ComputerInfo().AvailablePhysicalMemory (this message is only logged between 3:00 pm and 4:00 pm): " + ex.Message)
			End If
		End Try

		Return sngFreeMemory

	End Function

	Protected Function GetJobInfoFilePath(ByVal intJob As Integer) As String
		Return GetJobInfoFilePath(intJob, m_WorkingDir)
	End Function

	Public Shared Function GetJobInfoFilePath(ByVal intJob As Integer, ByVal strWorkDirPath As String) As String
		Return Path.Combine(strWorkDirPath, JOB_INFO_FILE_PREFIX & intJob & ".txt")
	End Function

	''' <summary>
	''' Converts the given timespan to the total days, hours, minutes, or seconds as a string
	''' </summary>
	''' <param name="dtInterval">Timespan to convert</param>
	''' <returns>Timespan length in human readable form</returns>
	''' <remarks></remarks>
	Protected Function GetHumanReadableTimeInterval(ByVal dtInterval As TimeSpan) As String

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

	Public Shared Function GetHPCOptions(ByVal jobParams As IJobParams, ByVal managerName As String) As udtHPCOptionsType

		Dim stepTool = jobParams.GetJobParameter("StepTool", "Unknown_Tool")
		Dim bMSGFPlusHPC = False

		Dim udtHPCOptions = New udtHPCOptionsType

		udtHPCOptions.HeadNode = jobParams.GetJobParameter("HPCHeadNode", "")
		If stepTool.ToLower() = "MSGFPlus_HPC".ToLower() AndAlso String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode) Then
			' Run this job using HPC, despite the fact that the settings file does not have the HPC settings defined
			udtHPCOptions.HeadNode = "deception.pnl.gov"
			bMSGFPlusHPC = True
		End If

		udtHPCOptions.UsingHPC = Not String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode)

		udtHPCOptions.ResourceType = jobParams.GetJobParameter("HPCResourceType", "socket")
		' Obsolete parameter; no longer used: udtHPCOptions.NodeGroup = jobParams.GetJobParameter("HPCNodeGroup", "ComputeNodes")
		udtHPCOptions.SharePath = jobParams.GetJobParameter("HPCSharePath", "\\picfs\projects\DMS")
		udtHPCOptions.MinimumMemoryMB = jobParams.GetJobParameter("MinimumMemoryMB", 0)
		udtHPCOptions.MinimumCores = jobParams.GetJobParameter("MinimumCores", 0)

		If bMSGFPlusHPC AndAlso udtHPCOptions.MinimumMemoryMB <= 0 Then
			udtHPCOptions.MinimumMemoryMB = 28000
		End If

		If bMSGFPlusHPC AndAlso udtHPCOptions.MinimumCores <= 0 Then
			udtHPCOptions.MinimumCores = 16
		End If

		Dim mgrNameClean = String.Empty

		For charIndex = 0 To managerName.Length - 1
			If Path.GetInvalidFileNameChars.Contains(managerName.Chars(charIndex)) Then
				mgrNameClean &= "_"
			Else
				mgrNameClean &= managerName.Chars(charIndex)
			End If
		Next

		' Example WorkDirPath: \\picfs\projects\DMS\DMS_Work_Dir\Pub-60-3
		udtHPCOptions.WorkDirPath = Path.Combine(udtHPCOptions.SharePath, "DMS_Work_Dir", mgrNameClean)

		Return udtHPCOptions

	End Function

	''' <summary>
	''' Get the name of the split fasta file to use for this job
	''' </summary>
	''' <param name="jobParams"></param>
	''' <param name="errorMessage">Output parameter: error message</param>
	''' <returns>The name of the split fasta file to use</returns>
	''' <remarks>Returns an empty string if an error</remarks>
	Public Shared Function GetSplitFastaFileName(ByVal jobParams As IJobParams, <Out()> ByRef errorMessage As String) As String
		Dim numberOfClonedSteps As Integer = 0

		Return GetSplitFastaFileName(jobParams, errorMessage, numberOfClonedSteps)

	End Function

	''' <summary>
	''' Get the name of the split fasta file to use for this job
	''' </summary>
	''' <param name="jobParams"></param>
	''' <param name="errorMessage">Output parameter: error message</param>
	''' <param name="numberOfClonedSteps">Output parameter: total number of cloned steps</param>
	''' <returns>The name of the split fasta file to use</returns>
	''' <remarks>Returns an empty string if an error</remarks>
	Public Shared Function GetSplitFastaFileName(ByVal jobParams As IJobParams, <Out()> ByRef errorMessage As String, <Out()> ByRef numberOfClonedSteps As Integer) As String

		errorMessage = String.Empty
		numberOfClonedSteps = 0

		Dim legacyFastaFileName = jobParams.GetJobParameter("LegacyFastaFileName", "")
		If String.IsNullOrEmpty(legacyFastaFileName) Then
			errorMessage = "Parameter LegacyFastaFileName is empty for the job; cannot determine the SplitFasta file name for this job step"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			Return String.Empty
		End If

		numberOfClonedSteps = jobParams.GetJobParameter("NumberOfClonedSteps", 0)
		If numberOfClonedSteps = 0 Then
			errorMessage = "Settings file is missing parameter NumberOfClonedSteps; cannot determine the SplitFasta file name for this job step"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			Return String.Empty
		End If

		Dim iteration = GetSplitFastaIteration(jobParams, errorMessage)
		If iteration < 1 Then
			If String.IsNullOrEmpty(errorMessage) Then
				errorMessage = "GetSplitFastaIteration computed an iteration value of " & iteration & "; cannot determine the SplitFasta file name for this job step"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			End If
			Return String.Empty
		End If

		Dim fastaNameBase = Path.GetFileNameWithoutExtension(legacyFastaFileName)
		Dim splitFastaName = fastaNameBase & "_" & numberOfClonedSteps.ToString() & "x_"

		If numberOfClonedSteps < 10 Then
			splitFastaName &= iteration.ToString("0") & ".fasta"
		ElseIf numberOfClonedSteps < 100 Then
			splitFastaName &= iteration.ToString("00") & ".fasta"
		Else
			splitFastaName &= iteration.ToString("000") & ".fasta"
		End If

		Return splitFastaName

	End Function

	Public Shared Function GetSplitFastaIteration(ByVal jobParams As IJobParams, <Out()> ByRef errorMessage As String) As Integer

		errorMessage = String.Empty

		Dim cloneStepRenumStart = jobParams.GetJobParameter("CloneStepRenumberStart", 0)
		If cloneStepRenumStart = 0 Then
			errorMessage = "Settings file is missing parameter CloneStepRenumberStart; cannot determine the SplitFasta iteration value for this job step"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			Return 0
		End If

		Dim stepNumber = jobParams.GetJobParameter("StepParameters", "Step", 0)
		If stepNumber = 0 Then
			errorMessage = "Job parameter Step is missing; cannot determine the SplitFasta iteration value for this job step"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			Return 0
		End If

		Return stepNumber - cloneStepRenumStart + 1

	End Function

	Public Shared Function IsLockQueueLogMessageNeeded(ByRef dtLockQueueWaitTimeStart As DateTime, ByRef dtLastLockQueueWaitTimeLog As DateTime) As Boolean

		Dim intWaitTimeLogIntervalSeconds As Integer

		If dtLockQueueWaitTimeStart = DateTime.MinValue Then dtLockQueueWaitTimeStart = DateTime.UtcNow()

		Select Case DateTime.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes
			Case Is >= 30
				intWaitTimeLogIntervalSeconds = 240
			Case Is >= 15
				intWaitTimeLogIntervalSeconds = 120
			Case Is >= 5
				intWaitTimeLogIntervalSeconds = 60
			Case Else
				intWaitTimeLogIntervalSeconds = 30
		End Select

		If DateTime.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= intWaitTimeLogIntervalSeconds Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Lookups up dataset information the data package associated with this analysis job
	''' </summary>
	''' <param name="dctDataPackageJobs"></param>
	''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
	''' <remarks></remarks>
	Protected Function LoadDataPackageJobInfo(ByRef dctDataPackageJobs As Dictionary(Of Integer, udtDataPackageJobInfoType)) As Boolean

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
	Public Shared Function LoadDataPackageJobInfo(ByVal ConnectionString As String, DataPackageID As Integer, ByRef dctDataPackageJobs As Dictionary(Of Integer, udtDataPackageJobInfoType)) As Boolean

		'Requests Dataset information from a data package
		Const RetryCount As Short = 3
		Dim strMsg As String

		If dctDataPackageJobs Is Nothing Then
			dctDataPackageJobs = New Dictionary(Of Integer, udtDataPackageJobInfoType)
		Else
			dctDataPackageJobs.Clear()
		End If

		Dim SqlStr As Text.StringBuilder = New Text.StringBuilder

		SqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ")
		SqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
		SqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ")
		SqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,")
		SqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType")
		SqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs")
		SqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())
		SqlStr.Append(" ORDER BY Dataset, Tool")

		Dim Dt As DataTable = Nothing

		'Get a table to hold the results of the query
		Dim blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RetryCount, Dt)

		If Not blnSuccess Then
			strMsg = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve aggregate list from database"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
			Dt.Dispose()
			Return False
		End If

		'Verify at least one row returned
		If Dt.Rows.Count < 1 Then
			' No data was returned

			' If the data package exists and has datasets associated with it, then Log this as a warning but return true
			' Otherwise, log an error and return false

			SqlStr.Clear()
			SqlStr.Append(" SELECT Count(*) AS Datasets")
			SqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets")
			SqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())

			'Get a table to hold the results of the query
			blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RetryCount, Dt)
			If blnSuccess AndAlso Dt.Rows.Count > 0 Then
				For Each curRow As DataRow In Dt.Rows
					Dim datasetCount = clsGlobal.DbCInt(curRow(0))

					If datasetCount > 0 Then
						strMsg = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID & ", but it does have " & datasetCount & " dataset"
						If datasetCount > 1 Then strMsg &= "s"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMsg)
						Return True
					End If
				Next
			End If

			strMsg = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID.ToString()
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg)
			Return False
		Else
			For Each CurRow As DataRow In Dt.Rows
				Dim udtDataPackageInfo As udtDataPackageJobInfoType = New udtDataPackageJobInfoType

				With udtDataPackageInfo
					.Job = clsGlobal.DbCInt(CurRow("Job"))
					.Dataset = clsGlobal.DbCStr(CurRow("Dataset"))
					.DatasetID = clsGlobal.DbCInt(CurRow("DatasetID"))
					.Instrument = clsGlobal.DbCStr(CurRow("Instrument"))
					.InstrumentGroup = clsGlobal.DbCStr(CurRow("InstrumentGroup"))
					.Experiment = clsGlobal.DbCStr(CurRow("Experiment"))
					.Experiment_Reason = clsGlobal.DbCStr(CurRow("Experiment_Reason"))
					.Experiment_Comment = clsGlobal.DbCStr(CurRow("Experiment_Comment"))
					.Experiment_Organism = clsGlobal.DbCStr(CurRow("Organism"))
					.Experiment_NEWT_ID = clsGlobal.DbCInt(CurRow("Experiment_NEWT_ID"))
					.Experiment_NEWT_Name = clsGlobal.DbCStr(CurRow("Experiment_NEWT_Name"))
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

	''' <summary>
	''' Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
	''' </summary>
	''' <param name="udtDataPackageJobInfo"></param>
	''' <returns></returns>
	''' <remarks> Does not override the job number</remarks>
	Protected Function OverrideCurrentDatasetAndJobInfo(ByVal udtDataPackageJobInfo As udtDataPackageJobInfoType) As Boolean

		Dim blnAggregationJob As Boolean = False

		If String.IsNullOrEmpty(udtDataPackageJobInfo.Dataset) Then
			m_message = "OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job " & udtDataPackageJobInfo.Job & " in the data package"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		If String.Equals(udtDataPackageJobInfo.Dataset, "Aggregation", StringComparison.CurrentCultureIgnoreCase) Then
			blnAggregationJob = True
		End If

		If Not blnAggregationJob Then
			' Update job params to have the details for the current dataset
			' This is required so that we can use FindDataFile to find the desired files
			If String.IsNullOrEmpty(udtDataPackageJobInfo.ServerStoragePath) Then
				m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ServerStoragePath' not defined for job " & udtDataPackageJobInfo.Job & " in the data package"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If String.IsNullOrEmpty(udtDataPackageJobInfo.ArchiveStoragePath) Then
				m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " & udtDataPackageJobInfo.Job & " in the data package"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If String.IsNullOrEmpty(udtDataPackageJobInfo.ResultsFolderName) Then
				m_message = "OverrideCurrentDatasetAndJobInfo; Column 'ResultsFolderName' not defined for job " & udtDataPackageJobInfo.Job & " in the data package"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If String.IsNullOrEmpty(udtDataPackageJobInfo.DatasetFolderName) Then
				m_message = "OverrideCurrentDatasetAndJobInfo; Column 'DatasetFolderName' not defined for job " & udtDataPackageJobInfo.Job & " in the data package"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If
		End If


		With udtDataPackageJobInfo

			m_jobParams.AddDatasetInfo(.Dataset, .DatasetID)
			m_DatasetName = String.Copy(.Dataset)

			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetNum", .Dataset)
			m_jobParams.AddAdditionalParameter("JobParameters", "DatasetID", .DatasetID.ToString())

			m_jobParams.AddAdditionalParameter("JobParameters", "Instrument", .Instrument)
			m_jobParams.AddAdditionalParameter("JobParameters", "InstrumentGroup", .InstrumentGroup)

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

	Protected Function ProcessMyEMSLDownloadQueue(ByVal downloadFolderPath As String, ByVal folderLayout As MyEMSLReader.Downloader.DownloadFolderLayout) As Boolean

		If m_MyEMSLDatasetListInfo.FilesToDownload.Count = 0 Then
			' Nothing to download; that's OK
			Return True
		End If

		Dim success = m_MyEMSLDatasetListInfo.ProcessDownloadQueue(downloadFolderPath, folderLayout)

		If Not success Then
			If m_MyEMSLDatasetListInfo.ErrorMessages.Count > 0 Then
				m_message = "Error in ProcessMyEMSLDownloadQueue: " & m_MyEMSLDatasetListInfo.ErrorMessages.First()
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Else
				m_message = "Unknown error in ProcessMyEMSLDownloadQueue"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

		End If

		Return success

	End Function

	''' <summary>
	''' Purges old fasta files (and related suffix array files) from localOrgDbFolder
	''' </summary>
	''' <param name="localOrgDbFolder"></param>
	''' <param name="freeSpaceThresholdPercent">Value between 0 and 100</param>
	''' <remarks>Minimum allowed value for freeSpaceThresholdPercent is 1; maximum allowed value is 50</remarks>
	Protected Sub PurgeFastaFilesIfLowFreeSpace(ByVal localOrgDbFolder As String, ByVal freeSpaceThresholdPercent As Integer)

		If freeSpaceThresholdPercent < 1 Then freeSpaceThresholdPercent = 1
		If freeSpaceThresholdPercent > 50 Then freeSpaceThresholdPercent = 50

		Try

			Dim diOrgDbFolder = New DirectoryInfo(localOrgDbFolder)
			If diOrgDbFolder.FullName.Length <= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Org DB folder length is less than 3 characters; this is unexpected: " & diOrgDbFolder.FullName)
				Exit Sub
			End If

			Dim driveLetter = diOrgDbFolder.FullName.Substring(0, 2)
			If (Not driveLetter.EndsWith(":")) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Orb DB folder path does not have a colon; cannot query drive free space: " & diOrgDbFolder.FullName)
				Exit Sub
			End If

			Dim driveInfo = New DriveInfo(driveLetter)
			Dim percentFreeSpace As Double = driveInfo.AvailableFreeSpace / CDbl(driveInfo.TotalSize) * 100

			If (percentFreeSpace >= freeSpaceThresholdPercent) Then
				If m_DebugLevel >= 2 Then
					Dim freeSpaceMB = driveInfo.AvailableFreeSpace / 1024.0 / 1024.0
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Free space on " & driveInfo.Name & " (" & freeSpaceMB.ToString("#,##0") & " MB) is over " & freeSpaceThresholdPercent & "% of the total space; purge not required")
				End If
				Exit Sub
			End If

			If m_DebugLevel >= 1 Then
				Dim freeSpaceMB = driveInfo.AvailableFreeSpace / 1024.0 / 1024.0
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Free space on " & driveInfo.Name & " (" & freeSpaceMB.ToString("#,##0") & " MB) is " & freeSpaceThresholdPercent & "% of the total space; purge required since less than threshold of " & freeSpaceThresholdPercent & "%")
			End If

			Dim dctFastaFiles = New Dictionary(Of FileInfo, DateTime)

			For Each fiFile In diOrgDbFolder.GetFiles("*.fasta")
				If Not dctFastaFiles.ContainsKey(fiFile) Then
					Dim dtLastUsed As DateTime = DateMax(fiFile.LastWriteTimeUtc, fiFile.CreationTimeUtc)

					' Look for a .hashcheck file
					Dim lstHashCheckfiles = diOrgDbFolder.GetFiles(fiFile.Name & "*.hashcheck")
					If lstHashCheckfiles.Count > 0 Then
						dtLastUsed = DateMax(dtLastUsed, lstHashCheckfiles.First.LastWriteTimeUtc)
					End If

					' Look for a .LastUsed file
					Dim lstLastUsedFiles = diOrgDbFolder.GetFiles(fiFile.Name & ".LastUsed")
					If lstLastUsedFiles.Count > 0 Then
						dtLastUsed = DateMax(dtLastUsed, lstLastUsedFiles.First.LastWriteTimeUtc)

						Try
							' Read the date stored in the file
							Using srLastUsedfile = New StreamReader(New FileStream(lstLastUsedFiles.First.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
								If srLastUsedfile.Peek > -1 Then
									Dim strLastUseDate = srLastUsedfile.ReadLine()
									Dim dtLastUsedActual As DateTime
									If DateTime.TryParse(strLastUseDate, dtLastUsedActual) Then
										dtLastUsed = DateMax(dtLastUsed, dtLastUsedActual)
									End If
								End If
							End Using
						Catch ex As Exception
							' Ignore errors here
						End Try

					End If
					dctFastaFiles.Add(fiFile, dtLastUsed)
				End If
			Next

			Dim lstFastaFilesByLastUse = From item In dctFastaFiles Order By item.Value Select item.Key

			For Each fiFileToPurge In lstFastaFilesByLastUse
				' Abort this process if the LastUsed date of this file is less than 5 days old
				Dim dtLastUsed As DateTime
				If dctFastaFiles.TryGetValue(fiFileToPurge, dtLastUsed) Then
					If DateTime.UtcNow.Subtract(dtLastUsed).TotalDays < 5 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "All fasta files in " & localOrgDbFolder & " are less than 5 days old; will not purge any more files to free disk space")
						Exit For
					End If
				End If

				' Delete all files associated with this fasta file
				Dim baseName = Path.GetFileNameWithoutExtension(fiFileToPurge.Name)

				Dim lstFilesToDelete = New List(Of FileInfo)
				lstFilesToDelete.AddRange(diOrgDbFolder.GetFiles(baseName & ".*"))

				If m_DebugLevel >= 1 Then
					Dim fileText = lstFilesToDelete.Count & " file"
					If lstFilesToDelete.Count <> 1 Then
						fileText &= "s"
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting " & fileText & " associated with " & fiFileToPurge.FullName)
				End If

				Try
					For Each fiFileToDelete In lstFilesToDelete
						fiFileToDelete.Delete()
					Next
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in PurgeFastaFilesIfLowFreeSpace", ex)
				End Try

				' Re-check the disk free space
				percentFreeSpace = driveInfo.AvailableFreeSpace / CDbl(driveInfo.TotalSize) * 100
				Dim freeSpaceMB = driveInfo.AvailableFreeSpace / 1024.0 / 1024.0

				If (percentFreeSpace >= freeSpaceThresholdPercent) Then
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Free space on " & driveInfo.Name & " (" & freeSpaceMB.ToString("#,##0") & " MB) is now over " & freeSpaceThresholdPercent & "% of the total space")
					End If
					Exit Sub
				ElseIf m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Free space on " & driveInfo.Name & " (" & freeSpaceMB.ToString("#,##0") & " MB) is now " & freeSpaceThresholdPercent & "% of the total space")
				End If

			Next

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in PurgeFastaFilesIfLowFreeSpace", ex)
		End Try

	End Sub

	Protected Function RenameDuplicatePHRPFile(ByVal SourceFolderPath As String, ByVal SourceFilename As String, ByVal TargetFolderPath As String, ByVal strPrefixToAdd As String, ByVal intJob As Integer) As Boolean
		Try
			Dim fiFileToRename As FileInfo = New FileInfo(Path.Combine(SourceFolderPath, SourceFilename))
			Dim strFilePathWithPrefix As String = Path.Combine(TargetFolderPath, strPrefixToAdd & fiFileToRename.Name)

			Threading.Thread.Sleep(100)
			fiFileToRename.MoveTo(strFilePathWithPrefix)

			m_jobParams.AddResultFileToSkip(Path.GetFileName(strFilePathWithPrefix))

		Catch ex As Exception
			m_message = "Exception renaming PHRP file " & SourceFilename & " for job " & intJob & " (data package has multiple jobs for the same dataset)"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Sub ResetTimestampForQueueWaitTimeLogging()
		m_LastLockQueueWaitTimeLog = DateTime.UtcNow
		m_LockQueueWaitTimeStart = DateTime.UtcNow
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

		Dim srInFile As StreamReader
		Dim strPhysicalFilePath As String = String.Empty
		Dim strFilePath As String

		Dim strLineIn As String

		strFilePath = Path.Combine(FolderPath, FileName)

		If File.Exists(strFilePath) Then
			' The desired file is located in folder FolderPath
			strPhysicalFilePath = strFilePath
		Else
			' The desired file was not found
			strFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX

			If File.Exists(strFilePath) Then
				' The _StoragePathInfo.txt file is present
				' Open that file to read the file path on the first line of the file

				srInFile = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

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

		Dim ioFolder As DirectoryInfo
		Dim ioFile As FileInfo

		Dim srInFile As StreamReader
		Dim strPhysicalFilePath As String
		Dim strFilePath As String

		Dim strLineIn As String

		strFilePath = Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX)

		If File.Exists(strFilePath) Then
			' The desired file is located in folder FolderPath
			' The _StoragePathInfo.txt file is present
			' Open that file to read the file path on the first line of the file

			srInFile = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

			strLineIn = srInFile.ReadLine
			strPhysicalFilePath = strLineIn

			srInFile.Close()
		Else
			' The desired file was not found

			' Look for a ser file in the dataset folder
			strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_SER_FILE)
			ioFile = New FileInfo(strPhysicalFilePath)

			If Not ioFile.Exists Then
				' See if a folder named 0.ser exists in FolderPath
				strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_ZERO_SER_FOLDER)
				ioFolder = New DirectoryInfo(strPhysicalFilePath)
				If Not ioFolder.Exists Then
					strPhysicalFilePath = ""
				End If
			End If

		End If

		Return strPhysicalFilePath

	End Function

	''' <summary>
	''' Retrieve the files specified by the file processing options parameter
	''' </summary>
	''' <param name="FilesToRetrieveExt">File processing options, for example: sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy</param>
	''' <returns></returns>
	''' <remarks>This function is used by two plugins which, as of September 2013, are unused: PhosphoFDRAggregator and PRIDEMzXML</remarks>
	Protected Function RetrieveAggregateFiles(ByVal FilesToRetrieveExt As String()) As Boolean

		Dim dctDataPackageJobs As Dictionary(Of Integer, udtDataPackageJobInfoType) = Nothing

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType
		Dim blnSuccess As Boolean

		Try
			If Not LoadDataPackageJobInfo(dctDataPackageJobs) Then
				m_message = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
				Return False
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex)
			Return False
		End Try

		Try
			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			Dim lstFilesToUnzip = New List(Of udtAggregateFileProcessingType)

			For Each udtItem As KeyValuePair(Of Integer, udtDataPackageJobInfoType) In dctDataPackageJobs

				If Not OverrideCurrentDatasetAndJobInfo(udtItem.Value) Then
					Return False
				End If

				Dim FilterValue = udtItem.Value.SettingsFileName + udtItem.Value.ParameterFileName

				For Each FileNameExt As String In FilesToRetrieveExt
					Dim SplitString = FileNameExt.Split(":"c)
					Dim SourceFilename = udtItem.Value.Dataset + SplitString(1)
					Dim SourceFolderPath = "??"

					Try

						If String.Equals(SplitString(0), udtItem.Value.Tool, StringComparison.CurrentCultureIgnoreCase) Then
							SourceFolderPath = FindDataFile(SourceFilename)
							If String.IsNullOrEmpty(SourceFolderPath) Then
								m_message = "Could not find a valid folder with file " & SourceFilename
								If m_DebugLevel >= 1 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
								End If
								Return False
							End If

							If Not CopyFileToWorkDir(SourceFilename, SourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
								m_message = "CopyFileToWorkDir returned False for " + SourceFilename + " using folder " + SourceFolderPath
								If m_DebugLevel >= 1 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
								End If
								Return False
							End If

							If m_DebugLevel >= 1 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " + SourceFilename + " from folder " + SourceFolderPath)
							End If

							If SourceFilename.ToLower().EndsWith(".zip") Then
								' Need to unzip the file
								' However, if the file is in MyEMSL then we won't be able to unzip it until after the call to ProcessMyEMSLDownloadQueue

								Dim udtAggregateFile = New udtAggregateFileProcessingType
								udtAggregateFile.Filename = String.Copy(SourceFilename)
								udtAggregateFile.FilterValue = String.Copy(FilterValue)
								udtAggregateFile.SaveMode = SplitString(2)

								lstFilesToUnzip.Add(udtAggregateFile)
							Else
								'Rename the files where dataset name will cause collisions
								RetrieveAggregateFilesRename(SourceFilename, FilterValue, SplitString(2))
							End If

						End If

					Catch ex As Exception
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveAggregateFiles; Exception during copy of file: " + SourceFilename + " from folder " + SourceFolderPath, ex)
						Return False

					End Try

				Next
			Next

			If Not ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
				Return False
			End If

			For Each fileToUnzip In lstFilesToUnzip
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file: " + fileToUnzip.Filename)
				If UnzipFileStart(Path.Combine(m_WorkingDir, fileToUnzip.Filename), m_WorkingDir, "RetrieveAggregateFiles", False) Then
					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File unzipped: " & fileToUnzip.Filename)
					End If
					m_jobParams.AddResultFileExtensionToSkip(fileToUnzip.Filename)

					' Note: This assumes that the file we just unzipped was a text file
					RetrieveAggregateFilesRename(Path.GetFileNameWithoutExtension(fileToUnzip.Filename) + ".txt", fileToUnzip.FilterValue, fileToUnzip.SaveMode)
				Else
					Return False
				End If
			Next

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			blnSuccess = True

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in RetrieveAggregateFiles", ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Private Sub RetrieveAggregateFilesRename(ByVal SourceFilename As String, ByVal filterValue As String, ByVal SaveMode As String)

		Dim newFilename As String = ""

		Try
			Select Case m_jobParams.GetParam("StepTool").ToLower
				Case "phospho_fdr_aggregator"
					Dim fi As New FileInfo(Path.Combine(m_WorkingDir, SourceFilename))
					Dim ext = Path.GetExtension(SourceFilename)
					Dim filenameNoExt = Path.GetFileNameWithoutExtension(SourceFilename)

					If filterValue.ToLower().Contains("_hcd") Then
						newFilename = filenameNoExt + "_hcd" + ext

					ElseIf filterValue.ToLower().Contains("_etd") Then
						newFilename = filenameNoExt + "_etd" + ext

					ElseIf filterValue.ToLower().Contains("_cid") Then
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
								fi.MoveTo(Path.Combine(m_WorkingDir, newFilename))
								blnSuccess = True
							Catch ex As IOException
								intRetryCount += 1
								If intRetryCount = 1 Then
									strExceptionMsg = String.Copy(ex.Message)
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to rename file " + fi.Name + " in folder " + m_WorkingDir + "; will retry after garbage collection")
									PRISM.Processes.clsProgRunner.GarbageCollectNow()
									Threading.Thread.Sleep(1000)
								End If
							End Try
						Loop While Not blnSuccess And intRetryCount <= 1

						If Not blnSuccess Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMgrSettings.RetrieveAggregateFilesRename; Unable to rename file" + fi.Name + " to " + newFilename + " in folder " + m_WorkingDir + ": " + strExceptionMsg)
							Return
						End If
					End If

					If SaveMode.ToLower() = "nocopy" Then
						m_jobParams.AddResultFileExtensionToSkip(newFilename)
					End If

					Return

			End Select

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMgrSettings.RetrieveAggregateFilesRename; Exception during renaming of file: " + newFilename + " from folder " + m_WorkingDir, ex)
			Return
		End Try

		Return
	End Sub

	Protected Function RetrieveDataPackagePeptideHitJobInfo(<Out()> ByRef DataPackageID As Integer) As List(Of udtDataPackageJobInfoType)

		Dim lstAdditionalJobs = New List(Of udtDataPackageJobInfoType)
		Return RetrieveDataPackagePeptideHitJobInfo(DataPackageID, lstAdditionalJobs)

	End Function

	Protected Function RetrieveDataPackagePeptideHitJobInfo(<Out()> ByRef DataPackageID As Integer, <Out()> ByRef lstAdditionalJobs As List(Of udtDataPackageJobInfoType)) As List(Of udtDataPackageJobInfoType)

		Dim ConnectionString As String = m_mgrParams.GetParam("brokerconnectionstring")
		DataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1)

		If DataPackageID < 0 Then
			m_message = "DataPackageID is not defined for this analysis job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
			lstAdditionalJobs = New List(Of udtDataPackageJobInfoType)
			Return New List(Of udtDataPackageJobInfoType)
		Else
			Return RetrieveDataPackagePeptideHitJobInfo(ConnectionString, DataPackageID, lstAdditionalJobs)
		End If

	End Function

	Public Shared Function RetrieveDataPackagePeptideHitJobInfo(ByVal ConnectionString As String, ByVal DataPackageID As Integer) As List(Of udtDataPackageJobInfoType)

		Dim lstAdditionalJobs = New List(Of udtDataPackageJobInfoType)
		Return RetrieveDataPackagePeptideHitJobInfo(ConnectionString, DataPackageID, lstAdditionalJobs)
	End Function

	Public Shared Function RetrieveDataPackagePeptideHitJobInfo(ByVal ConnectionString As String, ByVal DataPackageID As Integer, <Out()> ByRef lstAdditionalJobs As List(Of udtDataPackageJobInfoType)) As List(Of udtDataPackageJobInfoType)

		Dim lstDataPackagePeptideHitJobs As List(Of udtDataPackageJobInfoType)
		Dim dctDataPackageJobs As Dictionary(Of Integer, udtDataPackageJobInfoType)

		Dim strMsg As String

		' This list tracks the info for the Peptide Hit jobs (e.g. MSGF+ or Sequest) associated with this aggregation job's data package
		lstDataPackagePeptideHitJobs = New List(Of udtDataPackageJobInfoType)

		' This list tracks the info for the non Peptide Hit jobs (e.g. DeconTools or MASIC) associated with this aggregation job's data package
		lstAdditionalJobs = New List(Of udtDataPackageJobInfoType)

		' This dictionary will track the jobs associated with this aggregation job's data package
		' Key is job number, value is an instance of udtDataPackageJobInfoType
		dctDataPackageJobs = New Dictionary(Of Integer, udtDataPackageJobInfoType)

		Try
			If Not LoadDataPackageJobInfo(ConnectionString, DataPackageID, dctDataPackageJobs) Then
				strMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMsg)
				Return lstDataPackagePeptideHitJobs
			End If
		Catch ex As Exception
			strMsg = "Exception calling LoadDataPackageJobInfo"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveDataPackagePeptideHitJobInfo; " & strMsg, ex)
			Return lstDataPackagePeptideHitJobs
		End Try

		Try
			For Each kvItem As KeyValuePair(Of Integer, udtDataPackageJobInfoType) In dctDataPackageJobs

				If kvItem.Value.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					lstAdditionalJobs.Add(kvItem.Value)
				Else
					' Cache this job info in lstDataPackagePeptideHitJobs
					lstDataPackagePeptideHitJobs.Add(kvItem.Value)
				End If

			Next

		Catch ex As Exception
			strMsg = "Exception determining data package jobs for this aggregation job"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveDataPackagePeptideHitJobInfo; " & strMsg, ex)
		End Try

		Return lstDataPackagePeptideHitJobs

	End Function

	''' <summary>
	''' Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
	''' Also creates a batch file that can be manually run to retrieve the instrument data files
	''' </summary>
	''' <param name="udtOptions">File retrieval options</param>
	''' <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDataPackagePeptideHitJobPHRPFiles(
	  ByVal udtOptions As udtDataPackageRetrievalOptionsType,
	  ByRef lstDataPackagePeptideHitJobs As List(Of udtDataPackageJobInfoType)) As Boolean

		Const progressPercentAtStart As Single = 0
		Const progressPercentAtFinish As Single = 20
		Return RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish)
	End Function

	''' <summary>
	''' Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
	''' Also creates a batch file that can be manually run to retrieve the instrument data files
	''' </summary>
	''' <param name="udtOptions">File retrieval options</param>
	''' <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
	''' <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
	''' <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDataPackagePeptideHitJobPHRPFiles(
	  ByVal udtOptions As udtDataPackageRetrievalOptionsType,
	  ByRef lstDataPackagePeptideHitJobs As List(Of udtDataPackageJobInfoType),
	  ByVal progressPercentAtStart As Single,
	  ByVal progressPercentAtFinish As Single) As Boolean

		Dim SourceFolderPath As String = "??"
		Dim SourceFilename As String = "??"
		Dim DataPackageID As Integer = 0

		Dim blnFileCopied As Boolean
		Dim blnSuccess As Boolean

		' The keys in this dictionary are udtJobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
		' The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
		Dim dctInstrumentDataToRetrieve As Dictionary(Of udtDataPackageJobInfoType, KeyValuePair(Of String, String))

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		' Keys in this dictionary are DatasetID, values are a command of the form "Copy \\Server\Share\Folder\Dataset.raw Dataset.raw"
		' Note that we're explicitly defining the target filename to make sure the case of the letters matches the dataset name's case
		Dim dctRawFileRetrievalCommands As Dictionary(Of Integer, String) = New Dictionary(Of Integer, String)

		' Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
		Dim dctDatasetRawFilePaths As Dictionary(Of String, String) = New Dictionary(Of String, String)

		' This list tracks the info for the jobs associated with this aggregation job's data package
		If lstDataPackagePeptideHitJobs Is Nothing Then
			lstDataPackagePeptideHitJobs = New List(Of udtDataPackageJobInfoType)
		Else
			lstDataPackagePeptideHitJobs.Clear()
		End If

		' The keys in this dictionary are udtJobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
		' The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
		dctInstrumentDataToRetrieve = New Dictionary(Of udtDataPackageJobInfoType, KeyValuePair(Of String, String))

		Try
			lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(DataPackageID)

			If lstDataPackagePeptideHitJobs.Count = 0 Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Did not find any peptide hit jobs associated with this job's data package ID (" & DataPackageID & ")"
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
				Return False
			End If

		Catch ex As Exception
			m_message = "Exception calling RetrieveDataPackagePeptideHitJobInfo"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveDataPackagePeptideHitJobPHRPFiles; " & m_message, ex)
			Return False
		End Try

		Try

			' Make sure the MyEMSL download queue is empty
			If m_MyEMSLDatasetListInfo.FilesToDownload.Count > 0 Then
				If Not ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
					Return False
				End If
			End If

			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			Dim intJobsProcessed As Integer = 0

			For Each udtJobInfo As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs

				If Not OverrideCurrentDatasetAndJobInfo(udtJobInfo) Then
					' Error message has already been logged
					Return False
				End If

				If udtJobInfo.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PeptideHit ResultType not recognized for job " & udtJobInfo.Job & ": " & udtJobInfo.ResultType.ToString())

				Else

					' Keys in this list are filenames; values are True if the file is required and False if not required
					Dim lstFilesToGet = New SortedList(Of String, Boolean)
					Dim LocalFolderPath As String
					Dim lstPendingFileRenames = New List(Of String)
					Dim strSynopsisFileName As String
					Dim strSynopsisMSGFFileName As String
					Dim eLogMsgTypeIfNotFound As clsLogTools.LogLevels
					Dim strMZidFilenameZip As String = String.Empty
					Dim strMZidFilenameGZip As String = String.Empty
					Dim blnPrefixRequired As Boolean

					strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset)
					strSynopsisMSGFFileName = clsPHRPReader.GetMSGFFileName(strSynopsisFileName)

					If udtOptions.RetrievePHRPFiles Then
						lstFilesToGet.Add(strSynopsisFileName, True)

						lstFilesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset), True)
						lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset), True)
						lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset), True)
						lstFilesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(udtJobInfo.PeptideHitResultType, udtJobInfo.Dataset), True)

						lstFilesToGet.Add(strSynopsisMSGFFileName, False)
					End If

					If udtOptions.RetrieveMZidFiles AndAlso udtJobInfo.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
						' Retrieve MSGF+ .mzID files
						' They will either be stored as .zip files or as .gz files
						strMZidFilenameZip = m_DatasetName & "_msgfplus.zip"
						strMZidFilenameGZip = m_DatasetName & "_msgfplus.mzid.gz"
						lstFilesToGet.Add(strMZidFilenameZip, False)
						lstFilesToGet.Add(strMZidFilenameGZip, False)
					End If

					SourceFolderPath = String.Empty

					' Check whether a synopsis file by this name has already been copied locally
					' If it has, then we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
					If File.Exists(Path.Combine(m_WorkingDir, strSynopsisFileName)) Then
						blnPrefixRequired = True

						LocalFolderPath = Path.Combine(m_WorkingDir, "FileRename")
						If Not Directory.Exists(LocalFolderPath) Then
							Directory.CreateDirectory(LocalFolderPath)
						End If

					Else
						blnPrefixRequired = False
						LocalFolderPath = String.Copy(m_WorkingDir)
					End If

					Dim swJobInfoFile As StreamWriter = Nothing
					If udtOptions.CreateJobPathFiles Then
						Dim strJobInfoFilePath As String = GetJobInfoFilePath(udtJobInfo.Job)
						swJobInfoFile = New StreamWriter(New FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
					End If

					For Each sourceFile In lstFilesToGet

						SourceFilename = sourceFile.Key

						' Typically only use FindDataFile() for the first file in lstFilesToGet; we will assume the other files are in that folder
						' However, if the file resides in MyEMSL then we need to call FindDataFile for every new file because FindDataFile will append the MyEMSL File ID for each file
						If String.IsNullOrEmpty(SourceFolderPath) OrElse SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
							SourceFolderPath = FindDataFile(SourceFilename)
						End If

						If Not sourceFile.Value Then
							' It's OK if this file doesn't exist, we'll just log a debug message
							eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG
						Else
							' This file must exist; log an error if it's not found
							eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR
						End If

						If udtOptions.CreateJobPathFiles And Not SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
							Dim strSourceFilePath As String = Path.Combine(SourceFolderPath, SourceFilename)
							If File.Exists(strSourceFilePath) Then
								swJobInfoFile.WriteLine(strSourceFilePath)
							Else
								If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
									m_message = "Required PHRP file not found: " & SourceFilename
									If SourceFilename.ToLower().EndsWith("_msgfplus.zip") Or SourceFilename.ToLower().EndsWith("_msgfplus.mzid.gz") Then
										m_message &= "; Confirm job used MSGF+ and not MSGFDB"
									End If
									If m_DebugLevel >= 1 Then
										clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Required PHRP file not found: " & strSourceFilePath)
									End If
									Return False
								End If
							End If

						Else
							' Note for files in MyEMSL, this call will simply add the file to the download queue; use ProcessMyEMSLDownloadQueue() to retrieve the file
							blnFileCopied = CopyFileToWorkDir(SourceFilename, SourceFolderPath, LocalFolderPath, eLogMsgTypeIfNotFound)

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
									lstPendingFileRenames.Add(SourceFilename)
								Else
									m_jobParams.AddResultFileToSkip(SourceFilename)
								End If
							End If
						End If

					Next sourceFile		' in lstFilesToGet

					If m_MyEMSLDatasetListInfo.FilesToDownload.Count > 0 Then
						' Some of the files were found in MyEMSL; download them now
						If Not ProcessMyEMSLDownloadQueue(LocalFolderPath, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
							Return False
						End If
					End If

					' Now perform any required file renames
					For Each SourceFilename In lstPendingFileRenames
						If Not RenameDuplicatePHRPFile(LocalFolderPath, SourceFilename, m_WorkingDir, "Job" & udtJobInfo.Job.ToString() & "_", udtJobInfo.Job) Then
							Return False
						End If
					Next

					If udtOptions.RetrieveDTAFiles Then
						If udtOptions.CreateJobPathFiles Then
							' Find the CDTA file
							Dim strErrorMessage As String = String.Empty
							Dim SourceCDTAFilePath As String
							SourceCDTAFilePath = FindCDTAFile(strErrorMessage)

							If String.IsNullOrEmpty(SourceCDTAFilePath) Then
								m_message = strErrorMessage
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
								Return False
							Else
								swJobInfoFile.WriteLine(SourceCDTAFilePath)
							End If
						Else
							If Not RetrieveDtaFiles() Then
								'Errors were reported in function call, so just return
								Return False
							End If
						End If
					End If

					If udtOptions.CreateJobPathFiles Then
						swJobInfoFile.Close()
					Else
						' Unzip the MZId file (if it exists)						
						If Not String.IsNullOrEmpty(strMZidFilenameZip) Or Not String.IsNullOrEmpty(strMZidFilenameGZip) Then

							Dim fiFileToUnzip = New FileInfo(Path.Combine(m_WorkingDir, strMZidFilenameZip))
							If fiFileToUnzip.Exists Then
								m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName)
							Else
								fiFileToUnzip = New FileInfo(Path.Combine(m_WorkingDir, strMZidFilenameGZip))
								If fiFileToUnzip.Exists Then
									m_IonicZipTools.GUnzipFile(fiFileToUnzip.FullName)
								Else
									m_message = "Could not find either the _msgfplus.zip file or the _msgfplus.mzid.gz file for dataset"
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
									Return False
								End If
							End If

							If blnPrefixRequired Then
								If Not RenameDuplicatePHRPFile(m_WorkingDir, m_DatasetName & "_msgfplus.mzid", m_WorkingDir, "Job" & udtJobInfo.Job.ToString() & "_", udtJobInfo.Job) Then
									Return False
								End If
							End If
						End If
					End If

				End If

				If udtOptions.RetrieveMzXMLFile Then
					' See if a .mzXML file already exists for this dataset
					Dim strMzXMLFilePath As String
					Dim strHashcheckFilePath As String = String.Empty

					strMzXMLFilePath = FindMZXmlFile(strHashcheckFilePath)

					If String.IsNullOrEmpty(strMzXMLFilePath) Then
						' mzXML file not found
						If udtJobInfo.RawDataType = RAW_DATA_TYPE_DOT_RAW_FILES Then
							' Will need to retrieve the .Raw file for this dataset
							dctInstrumentDataToRetrieve.Add(udtJobInfo, New KeyValuePair(Of String, String)(String.Empty, String.Empty))
						ElseIf udtOptions.RetrieveMzXMLFile Then
							m_message = "mzXML file not found for dataset " & udtJobInfo.Dataset & " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
							Return False
						End If
					Else
						dctInstrumentDataToRetrieve.Add(udtJobInfo, New KeyValuePair(Of String, String)(strMzXMLFilePath, strHashcheckFilePath))
					End If
				End If

				Dim blnIsFolder As Boolean = False
				Dim strRawFilePath As String
				strRawFilePath = FindDatasetFileOrFolder(blnIsFolder)

				If Not String.IsNullOrEmpty(strRawFilePath) Then
					If Not dctRawFileRetrievalCommands.ContainsKey(udtJobInfo.DatasetID) Then
						Dim strCopyCommand As String
						If blnIsFolder Then
							strCopyCommand = "copy " & strRawFilePath & " .\" & Path.GetFileName(strRawFilePath) & " /S /I"
						Else
							' Make sure the case of the filename matches the case of the dataset name
							' Also, make sure the extension is lowercase
							strCopyCommand = "copy " & strRawFilePath & " " & udtJobInfo.Dataset & Path.GetExtension(strRawFilePath).ToLower()
						End If
						dctRawFileRetrievalCommands.Add(udtJobInfo.DatasetID, strCopyCommand)
						dctDatasetRawFilePaths.Add(udtJobInfo.Dataset, strRawFilePath)
					End If
				End If

				intJobsProcessed += 1
				Dim sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(progressPercentAtStart, progressPercentAtFinish, intJobsProcessed, lstDataPackagePeptideHitJobs.Count)
				If Not m_StatusTools Is Nothing Then
					m_StatusTools.CurrentOperation = "RetrieveDataPackagePeptideHitJobPHRPFiles"
					m_StatusTools.UpdateAndWrite(sngProgress)
				End If

			Next udtJobInfo		' in lstDataPackagePeptideHitJobs

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			If dctRawFileRetrievalCommands.Count > 0 Then
				' Create a batch file with commands for retrieve the dataset files
				Dim strBatchFilePath As String
				strBatchFilePath = Path.Combine(m_WorkingDir, "RetrieveInstrumentData.bat")
				Using swOutfile As StreamWriter = New StreamWriter(New FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
					For Each item As String In dctRawFileRetrievalCommands.Values
						swOutfile.WriteLine(item)
					Next
				End Using

				' Store the dataset paths in a Packed Job Parameter
				StorePackedJobParameterDictionary(dctDatasetRawFilePaths, JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS)

			End If

			If udtOptions.RetrieveMzXMLFile Then
				' All of the PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
				' If udtOptions.CreateJobPathFiles = True then we will create StoragePathInfo files
				blnSuccess = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions)
			Else
				blnSuccess = True
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveDataPackagePeptideHitJobPHRPFiles; Exception during copy of file: " + SourceFilename + " from folder " + SourceFolderPath, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Retrieve the .mzXML files for the jobs in dctInstrumentDataToRetrieve
	''' </summary>
	''' <param name="dctInstrumentDataToRetrieve">The keys in this dictionary are JobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any); the KeyValuePair will have empty strings if the .Raw file needs to be retrieved</param>
	''' <param name="udtOptions">File retrieval options</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>If udtOptions.CreateJobPathFiles is True, then will create StoragePathInfo files for the .mzXML or .Raw files</remarks>
	Protected Function RetrieveDataPackageMzXMLFiles(
	  ByVal dctInstrumentDataToRetrieve As Dictionary(Of udtDataPackageJobInfoType, KeyValuePair(Of String, String)),
	  ByVal udtOptions As udtDataPackageRetrievalOptionsType) As Boolean

		Dim blnSuccess As Boolean
		Dim CreateStoragePathInfoOnly As Boolean

		Dim intCurrentJob As Integer
		Dim lstDatasetsProcessed As SortedSet(Of String)

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		Try

			' Make sure we don't move the .mzXML file into the results folder
			m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)			' Raw file
			m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)			' mzXML file

			If udtOptions.CreateJobPathFiles Then
				CreateStoragePathInfoOnly = True
			Else
				CreateStoragePathInfoOnly = False
			End If

			lstDatasetsProcessed = New SortedSet(Of String)

			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			Dim dctInstrumentDataUNC = New Dictionary(Of udtDataPackageJobInfoType, KeyValuePair(Of String, String))

			' First retrieve files from MyEMSL
			For Each kvItem As KeyValuePair(Of udtDataPackageJobInfoType, KeyValuePair(Of String, String)) In dctInstrumentDataToRetrieve
				' The key in kvMzXMLFileInfo is the path to the .mzXML file
				' The value in kvMzXMLFileInfo is the path to the .hashcheck file
				Dim kvMzXMLFileInfo As KeyValuePair(Of String, String) = kvItem.Value
				Dim strMzXMLFilePath As String = kvMzXMLFileInfo.Key

				If Not strMzXMLFilePath.StartsWith(MYEMSL_PATH_FLAG) Then
					dctInstrumentDataUNC.Add(kvItem.Key, kvItem.Value)
					Continue For
				End If


			Next

			' Next retrieve the remaining files
			For Each kvItem In dctInstrumentDataToRetrieve

				' The key in kvMzXMLFileInfo is the path to the .mzXML file
				' The value in kvMzXMLFileInfo is the path to the .hashcheck file
				Dim kvMzXMLFileInfo As KeyValuePair(Of String, String) = kvItem.Value
				Dim strMzXMLFilePath As String = kvMzXMLFileInfo.Key
				Dim strHashcheckFilePath As String = kvMzXMLFileInfo.Value

				intCurrentJob = kvItem.Key.Job

				If Not lstDatasetsProcessed.Contains(kvItem.Key.Dataset) Then

					If Not OverrideCurrentDatasetAndJobInfo(kvItem.Key) Then
						' Error message has already been logged
						Return False
					End If

					If String.IsNullOrEmpty(strMzXMLFilePath) Then
						' The .mzXML file was not found; we will need to obtain the .Raw file
						blnSuccess = False
					Else
						' mzXML file exists; either retrieve it or create a StoragePathInfo file
						blnSuccess = RetrieveMZXmlFileUsingSourceFile(CreateStoragePathInfoOnly, strMzXMLFilePath, strHashcheckFilePath)
					End If

					If blnSuccess Then
						' .mzXML file found and copied locally
						If m_DebugLevel >= 1 Then
							If udtOptions.CreateJobPathFiles Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, ".mzXML file found for job " & intCurrentJob & " at " & strMzXMLFilePath)
							Else
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied .mzXML file for job " & intCurrentJob & " from " & strMzXMLFilePath)
							End If

						End If
					Else
						' .mzXML file not found
						' Find or retrieve the .Raw file, which can be used to create the .mzXML file (the plugin will actually perform the work of converting the file; as an example, see the MSGF plugin)

						If Not RetrieveSpectra(kvItem.Key.RawDataType, CreateStoragePathInfoOnly) Then
							m_message = "Error occurred retrieving instrument data file for job " & intCurrentJob
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "RetrieveDataPackageMzXMLFiles, " & m_message)
							Return False
						End If

					End If

					lstDatasetsProcessed.Add(kvItem.Key.Dataset)
				End If

			Next kvItem

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

			blnSuccess = True

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveDataPackageMzXMLFiles; Exception retrieving mzXML file or .Raw file for job " & intCurrentJob, ex)
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
		Dim fiSourceFile As FileInfo

		Try
			strProgLoc = m_mgrParams.GetParam(strProgLocName)
			If String.IsNullOrEmpty(strProgLocName) Then
				m_message = "Manager parameter " + strProgLocName + " is not defined; cannot retrieve file " & OMICS_ELEMENT_DATA_FILE
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			fiSourceFile = New FileInfo(Path.Combine(strProgLoc, OMICS_ELEMENT_DATA_FILE))

			If Not fiSourceFile.Exists Then
				m_message = "PNNLOmics Element Data file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " at: " & fiSourceFile.FullName)
				Return False
			End If

			fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, OMICS_ELEMENT_DATA_FILE))

		Catch ex As Exception
			m_message = "Error copying " & OMICS_ELEMENT_DATA_FILE
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " to working directory: " + ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
	''' </summary>
	''' <param name="FileExtension">File extension to match; must contain a period, for example ".raw"</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDatasetFile(ByVal FileExtension As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim DatasetFilePath As String = FindDatasetFile(FileExtension)
		If String.IsNullOrEmpty(DatasetFilePath) Then
			Return False
		End If

		If (DatasetFilePath.StartsWith(MYEMSL_PATH_FLAG)) Then
			' Queue this file for download
			m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(m_RecentlyFoundMyEMSLFiles.First().FileInfo)
			Return True
		End If

		Dim fiDatasetFile As FileInfo = New FileInfo(DatasetFilePath)
		If Not fiDatasetFile.Exists Then
			m_message = "Source dataset file file not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiDatasetFile.FullName)
			Return False
		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving file " & fiDatasetFile.FullName)
		End If

		If CopyFileToWorkDir(fiDatasetFile.Name, fiDatasetFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress
	''' </summary>
	''' <param name="GetCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveMgfFile(ByVal GetCdfAlso As Boolean, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim strMGFFilePath As String
		Dim fiMGFFile As FileInfo

		strMGFFilePath = FindMGFFile()

		If String.IsNullOrEmpty(strMGFFilePath) Then
			m_message = "Source mgf file not found using FindMGFFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		fiMGFFile = New FileInfo(strMGFFilePath)
		If Not fiMGFFile.Exists Then
			m_message = "Source mgf file not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiMGFFile.FullName)
			Return False
		End If


		'Do the copy
		If Not CopyFileToWorkDirWithRename(fiMGFFile.Name, fiMGFFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly, MaxCopyAttempts:=3) Then Return False

		'If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
		If Not GetCdfAlso Then Return True

		For Each fiCDFFile As FileInfo In fiMGFFile.Directory.GetFiles("*" + DOT_CDF_EXTENSION)
			'Copy the .cdf file that was found
			If CopyFileToWorkDirWithRename(fiCDFFile.Name, fiCDFFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly, MaxCopyAttempts:=3) Then
				Return True
			Else
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error obtaining CDF file from " & fiCDFFile.FullName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				End If
				Return False
			End If
		Next

		' CDF file not found
		m_message = "CDF File not found"
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

		Return False

	End Function

	''' <summary>
	''' Looks for this dataset's mzXML file
	''' Looks in folders with names like MSXML_Gen_1_154_DatasetID, MSXML_Gen_1_93_DatasetID, or MSXML_Gen_1_39_DatasetID (plus a few others)
	''' Also examines \\proto-6\MSXML_Cache\
	''' If the .mzXML file cannot be found, then returns False
	''' </summary>
	''' <param name="CreateStoragePathInfoOnly"></param>
	''' <param name="SourceFilePath">Returns the full path to the file that was retrieved</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveMZXmlFile(ByVal CreateStoragePathInfoOnly As Boolean, ByRef SourceFilePath As String) As Boolean

		Dim strHashcheckFilePath As String = String.Empty
		SourceFilePath = FindMZXmlFile(strHashcheckFilePath)

		If String.IsNullOrEmpty(SourceFilePath) Then
			Return False
		Else
			Return RetrieveMZXmlFileUsingSourceFile(CreateStoragePathInfoOnly, SourceFilePath, strHashcheckFilePath)
		End If

	End Function

	Protected Function RetrieveMZXmlFileUsingSourceFile(ByVal CreateStoragePathInfoOnly As Boolean, ByVal SourceFilePath As String, ByVal HashcheckFilePath As String) As Boolean

		Dim fiSourceFile As FileInfo

		If SourceFilePath.StartsWith(MYEMSL_PATH_FLAG) Then
			Return AddFileToMyEMSLDownloadQueue(SourceFilePath)
		End If

		fiSourceFile = New FileInfo(SourceFilePath)

		If fiSourceFile.Exists Then
			If CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then

				If Not String.IsNullOrEmpty(HashcheckFilePath) AndAlso File.Exists(HashcheckFilePath) Then
					RetrieveMzXMLFileVerifyHash(fiSourceFile, HashcheckFilePath, CreateStoragePathInfoOnly)
				Else
					Return True
				End If

			End If
		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MzXML file not found; will need to generate it: " + fiSourceFile.Name)
		End If

		Return False

	End Function

	Protected Function RetrieveMzXMLFileVerifyHash(ByVal fiSourceFile As FileInfo, ByVal HashcheckFilePath As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim strTargetFilePath As String
		Dim strErrorMessage As String = String.Empty
		Dim blnComputeHash As Boolean

		If CreateStoragePathInfoOnly Then
			strTargetFilePath = fiSourceFile.FullName
			' Don't compute the hash, since we're accessing the file over the network
			blnComputeHash = False
		Else
			strTargetFilePath = Path.Combine(m_WorkingDir, fiSourceFile.Name)
			blnComputeHash = True
		End If

		If clsGlobal.ValidateFileVsHashcheck(strTargetFilePath, HashcheckFilePath, strErrorMessage, blnCheckDate:=True, blnComputeHash:=blnComputeHash) Then
			Return True
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MzXML file validation error in RetrieveMZXmlFileUsingSourceFile: " & strErrorMessage)

		Try
			If CreateStoragePathInfoOnly Then
				' Delete the local StoragePathInfo file
				Dim strStoragePathInfoFile As String = Path.Combine(m_WorkingDir, fiSourceFile.Name & STORAGE_PATH_INFO_FILE_SUFFIX)
				If File.Exists(strStoragePathInfoFile) Then
					File.Delete(strStoragePathInfoFile)
				End If
			Else
				' Delete the local file to force it to be re-generated
				File.Delete(strTargetFilePath)
			End If

		Catch ex As Exception
			' Ignore errors here
		End Try

		Try
			' Delete the remote mzXML file only if we computed the hash and we had a hash mismatch
			If blnComputeHash Then
				fiSourceFile.Delete()
			End If
		Catch ex As Exception
			' Ignore errors here
		End Try

		Return False

	End Function

	''' <summary>
	''' Looks for this dataset's ScanStats files (previously created by MASIC)
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanStatsFiles(ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Const RetrieveSICStatsFile As Boolean = False
		Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=True)

	End Function

	''' <summary>
	''' Looks for this dataset's ScanStats files (previously created by MASIC)
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="CreateStoragePathInfoOnly"></param>
	''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
	''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanStatsFiles(ByVal CreateStoragePathInfoOnly As Boolean, ByVal RetrieveScanStatsFile As Boolean, ByVal RetrieveScanStatsExFile As Boolean) As Boolean

		Const RetrieveSICStatsFile As Boolean = False
		Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile, RetrieveScanStatsExFile)

	End Function

	''' <summary>
	''' Looks for this dataset's MASIC results files
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(ByVal RetrieveSICStatsFile As Boolean, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean
		Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=True)
	End Function

	''' <summary>
	''' Looks for this dataset's MASIC results files
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
	''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(
	  ByVal RetrieveSICStatsFile As Boolean,
	  ByVal CreateStoragePathInfoOnly As Boolean,
	  ByVal RetrieveScanStatsFile As Boolean,
	  ByVal RetrieveScanStatsExFile As Boolean) As Boolean

		Dim lstNonCriticalFileSuffixes As List(Of String) = New List(Of String)
		Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile, RetrieveScanStatsExFile, lstNonCriticalFileSuffixes)

	End Function

	''' <summary>
	''' Looks for this dataset's MASIC results files
	''' Looks for the files in any SIC folder that exists for the dataset
	''' </summary>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
	''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(
	  ByVal RetrieveSICStatsFile As Boolean,
	  ByVal CreateStoragePathInfoOnly As Boolean,
	  ByVal RetrieveScanStatsFile As Boolean,
	  ByVal RetrieveScanStatsExFile As Boolean,
	  ByVal lstNonCriticalFileSuffixes As List(Of String)) As Boolean

		Dim ServerPath As String
		Dim ScanStatsFilename As String

		Dim BestScanStatsFileTransactionID As Int64 = 0

		Const MaxRetryCount As Integer = 1

		' Look for the MASIC Results folder
		' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
		ScanStatsFilename = m_DatasetName + SCAN_STATS_FILE_SUFFIX
		ServerPath = FindValidFolder(m_DatasetName, "", "SIC*", MaxRetryCount, LogFolderNotFound:=False, RetrievingInstrumentDataFolder:=False)

		If String.IsNullOrEmpty(ServerPath) Then
			m_message = "Dataset folder path not defined"
		Else

			If ServerPath.StartsWith(MYEMSL_PATH_FLAG) Then
				' Find the newest _ScanStats.txt file in MyEMSL
				Dim BestSICFolderName = String.Empty

				For Each myEmslFile In m_RecentlyFoundMyEMSLFiles
					If myEmslFile.IsFolder Then
						Continue For
					End If

					If String.Equals(myEmslFile.FileInfo.Filename, ScanStatsFilename, StringComparison.CurrentCultureIgnoreCase) AndAlso
					  myEmslFile.FileInfo.TransactionID > BestScanStatsFileTransactionID Then
						Dim fiScanStatsFile = New FileInfo(myEmslFile.FileInfo.RelativePathWindows)
						BestSICFolderName = fiScanStatsFile.Directory.Name
						BestScanStatsFileTransactionID = myEmslFile.FileInfo.TransactionID
					End If
				Next

				If BestScanStatsFileTransactionID = 0 Then
					m_message = "MASIC ScanStats file not found in the SIC results folder(s) in MyEMSL"
				Else
					Dim BestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, BestSICFolderName)
					Return RetrieveScanAndSICStatsFiles(BestSICFolderPath, RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile:=RetrieveScanStatsFile, RetrieveScanStatsExFile:=RetrieveScanStatsExFile, lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)
				End If
			Else
				Dim diFolderInfo As DirectoryInfo
				diFolderInfo = New DirectoryInfo(ServerPath)

				If Not diFolderInfo.Exists Then
					m_message = "Dataset folder with MASIC files not found: " + diFolderInfo.FullName
				Else

					' See if the ServerPath folder actually contains a subfolder that starts with "SIC"
					Dim diSubfolders() As DirectoryInfo = diFolderInfo.GetDirectories("SIC*")
					If diSubfolders.Length = 0 Then
						m_message = "Dataset folder does not contain any MASIC results folders: " + diFolderInfo.FullName
					Else
						' MASIC Results Folder Found
						' If more than one folder, then use the folder with the newest _ScanStats.txt file						
						Dim dtNewestScanStatsFileDate As DateTime
						Dim strNewestScanStatsFilePath As String = String.Empty

						For Each diSubFolder As DirectoryInfo In diSubfolders
							Dim fiSourceFile = New FileInfo(Path.Combine(diSubFolder.FullName, ScanStatsFilename))
							If fiSourceFile.Exists Then
								If String.IsNullOrEmpty(strNewestScanStatsFilePath) OrElse fiSourceFile.LastWriteTimeUtc > dtNewestScanStatsFileDate Then
									strNewestScanStatsFilePath = fiSourceFile.FullName
									dtNewestScanStatsFileDate = fiSourceFile.LastWriteTimeUtc
								End If
							End If
						Next

						If String.IsNullOrEmpty(strNewestScanStatsFilePath) Then
							m_message = "MASIC ScanStats file not found below " + diFolderInfo.FullName
						Else
							Dim fiSourceFile = New FileInfo(strNewestScanStatsFilePath)
							Dim BestSICFolderPath = fiSourceFile.Directory.FullName
							Return RetrieveScanAndSICStatsFiles(BestSICFolderPath, RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile:=RetrieveScanStatsFile, RetrieveScanStatsExFile:=RetrieveScanStatsExFile, lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)
						End If

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
	''' Retrieves the MASIC results for this dataset using the specified folder
	''' </summary>
	''' <param name="MASICResultsFolderPath">Source folder to copy files from</param>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
	''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	''' <remarks></remarks>
	Protected Function RetrieveScanAndSICStatsFiles(
	  ByVal MASICResultsFolderPath As String,
	  ByVal RetrieveSICStatsFile As Boolean,
	  ByVal CreateStoragePathInfoOnly As Boolean,
	  ByVal RetrieveScanStatsFile As Boolean,
	  ByVal RetrieveScanStatsExFile As Boolean) As Boolean

		Dim lstNonCriticalFileSuffixes As List(Of String) = New List(Of String)

		Return RetrieveScanAndSICStatsFiles(MASICResultsFolderPath, RetrieveSICStatsFile, CreateStoragePathInfoOnly, RetrieveScanStatsFile, RetrieveScanStatsExFile, lstNonCriticalFileSuffixes)
	End Function

	''' <summary>
	''' Retrieves the MASIC results for this dataset using the specified folder
	''' </summary>
	''' <param name="MASICResultsFolderPath">Source folder to copy files from</param>
	''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
	''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
	''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
	''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
	''' <param name="lstNonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
	''' <returns>True if the file was found and retrieved, otherwise False</returns>
	Protected Function RetrieveScanAndSICStatsFiles(
	  ByVal MASICResultsFolderPath As String,
	  ByVal RetrieveSICStatsFile As Boolean,
	  ByVal CreateStoragePathInfoOnly As Boolean,
	  ByVal RetrieveScanStatsFile As Boolean,
	  ByVal RetrieveScanStatsExFile As Boolean,
	  ByVal lstNonCriticalFileSuffixes As List(Of String)) As Boolean

		Const MaxCopyAttempts As Integer = 2

		' Copy the MASIC files from the MASIC results folder

		If String.IsNullOrEmpty(MASICResultsFolderPath) Then
			m_message = "MASIC Results folder path not defined"

		ElseIf MASICResultsFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then

			Dim diSICFolder = New DirectoryInfo(MASICResultsFolderPath)

			If RetrieveScanStatsFile Then
				' Look for and copy the _ScanStats.txt file
				If Not RetrieveSICFileMyEMSL(m_DatasetName + SCAN_STATS_FILE_SUFFIX, diSICFolder.Name, lstNonCriticalFileSuffixes) Then
					Return False
				End If
			End If

			If RetrieveScanStatsExFile Then
				' Look for and copy the _ScanStatsEx.txt file
				If Not RetrieveSICFileMyEMSL(m_DatasetName + SCAN_STATS_EX_FILE_SUFFIX, diSICFolder.Name, lstNonCriticalFileSuffixes) Then
					Return False
				End If
			End If


			If RetrieveSICStatsFile Then
				' Look for and copy the _SICStats.txt file
				If Not RetrieveSICFileMyEMSL(m_DatasetName + "_SICStats.txt", diSICFolder.Name, lstNonCriticalFileSuffixes) Then
					Return False
				End If
			End If

			' All files have been found
			' The calling process should download them using ProcessMyEMSLDownloadQueue()
			Return True

		Else

			Dim diFolderInfo As DirectoryInfo
			diFolderInfo = New DirectoryInfo(MASICResultsFolderPath)

			If Not diFolderInfo.Exists Then
				m_message = "MASIC Results folder not found: " + diFolderInfo.FullName
			Else

				If RetrieveScanStatsFile Then
					' Look for and copy the _ScanStats.txt file
					If Not RetrieveSICFileUNC(m_DatasetName + SCAN_STATS_FILE_SUFFIX, MASICResultsFolderPath, CreateStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
						Return False
					End If
				End If

				If RetrieveScanStatsExFile Then
					' Look for and copy the _ScanStatsEx.txt file
					If Not RetrieveSICFileUNC(m_DatasetName + SCAN_STATS_EX_FILE_SUFFIX, MASICResultsFolderPath, CreateStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
						Return False
					End If
				End If

				If RetrieveSICStatsFile Then
					' Look for and copy the _SICStats.txt file
					If Not RetrieveSICFileUNC(m_DatasetName + "_SICStats.txt", MASICResultsFolderPath, CreateStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
						Return False
					End If
				End If

				' All files successfully copied
				Return True

			End If

		End If

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveScanAndSICStatsFiles: " + m_message)
		End If

		Return False

	End Function

	Protected Function RetrieveSICFileMyEMSL(ByVal strFileToFind As String, ByVal strSICFolderName As String, ByVal lstNonCriticalFileSuffixes As List(Of String)) As Boolean

		m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(strFileToFind, strSICFolderName, m_DatasetName, recurse:=False)

		If m_RecentlyFoundMyEMSLFiles.Count > 0 Then
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Found MASIC results file in MyEMSL, " & Path.Combine(strSICFolderName, strFileToFind))
			End If

			m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(m_RecentlyFoundMyEMSLFiles.First().FileInfo)

		Else
			Dim blnIgnoreFile As Boolean
			blnIgnoreFile = SafeToIgnore(strFileToFind, lstNonCriticalFileSuffixes)

			If Not blnIgnoreFile Then
				m_message = strFileToFind + " not found in MyEMSL, subfolder " + strSICFolderName
				Return False
			End If
		End If

		Return True

	End Function

	Protected Function RetrieveSICFileUNC(ByVal strFileToFind As String, ByVal MASICResultsFolderPath As String, ByVal CreateStoragePathInfoOnly As Boolean, ByVal MaxCopyAttempts As Integer, ByVal lstNonCriticalFileSuffixes As List(Of String)) As Boolean

		Dim fiSourceFile = New FileInfo(Path.Combine(MASICResultsFolderPath, strFileToFind))

		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying MASIC results file: " + fiSourceFile.FullName)
		End If

		If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly, MaxCopyAttempts) Then
			Dim blnIgnoreFile As Boolean
			blnIgnoreFile = SafeToIgnore(fiSourceFile.Name, lstNonCriticalFileSuffixes)

			If Not blnIgnoreFile Then
				m_message = strFileToFind + " not found at " + fiSourceFile.Directory.FullName
				Return False
			End If
		End If

		Return True

	End Function

	''' <summary>
	''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
	''' </summary>
	''' <param name="RawDataType">Type of data to copy</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveSpectra(ByVal RawDataType As String) As Boolean
		Const CreateStoragePathInfoOnly As Boolean = False
		Return RetrieveSpectra(RawDataType, CreateStoragePathInfoOnly)
	End Function

	''' <summary>
	''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
	''' </summary>
	''' <param name="RawDataType">Type of data to copy</param>
	''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt, and this file's first line will be the full path to the spectrum file (or spectrum folder)</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveSpectra(ByVal RawDataType As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim blnSuccess As Boolean = False
		Dim StoragePath As String = m_jobParams.GetParam("DatasetStoragePath")
		Dim eRawDataType As eRawDataTypeConstants

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving spectra file(s)")

		eRawDataType = GetRawDataType(RawDataType)
		Select Case eRawDataType
			Case eRawDataTypeConstants.AgilentDFolder			'Agilent ion trap data

				If StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) OrElse _
				   StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()) Then
					' For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005, 
					'  we would pre-process the data beforehand to create MGF files
					' The following call can be used to retrieve the files
					blnSuccess = RetrieveMgfFile(GetCdfAlso:=True, CreateStoragePathInfoOnly:=CreateStoragePathInfoOnly)
				Else
					' DeconTools_V2 now supports reading the .D files directly
					' Call RetrieveDotDFolder() to copy the folder and all subfolders
					blnSuccess = RetrieveDotDFolder(CreateStoragePathInfoOnly, blnSkipBAFFiles:=True)
				End If

			Case eRawDataTypeConstants.AgilentQStarWiffFile			'Agilent/QSTAR TOF data
				blnSuccess = RetrieveDatasetFile(DOT_WIFF_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.ZippedSFolders			'FTICR data
				blnSuccess = RetrieveSFolders(CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.ThermoRawFile			'Finnigan ion trap/LTQ-FT data
				blnSuccess = RetrieveDatasetFile(DOT_RAW_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.MicromassRawFolder			'Micromass QTOF data
				blnSuccess = RetrieveDotRawFolder(CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.UIMF			'IMS UIMF data
				blnSuccess = RetrieveDatasetFile(DOT_UIMF_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.mzXML
				blnSuccess = RetrieveDatasetFile(DOT_MZXML_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.mzML
				blnSuccess = RetrieveDatasetFile(DOT_MZML_EXTENSION, CreateStoragePathInfoOnly)

			Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
				' Call RetrieveDotDFolder() to copy the folder and all subfolders

				' Both the MSXml step tool and DeconTools require the .Baf file
				' We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, we need the file
				' In contrast, ICR-2LS only needs the ser or FID file, plus the apexAcquisition.method file in the .md folder

				Dim blnSkipBAFFiles As Boolean = False

				Dim strStepTool = m_jobParams.GetJobParameter("StepTool", "Unknown")

				If strStepTool = "ICR2LS" Then
					blnSkipBAFFiles = True
				End If

				blnSuccess = RetrieveDotDFolder(CreateStoragePathInfoOnly, blnSkipBAFFiles)

			Case eRawDataTypeConstants.BrukerMALDIImaging
				blnSuccess = RetrieveBrukerMALDIImagingFolders(UnzipOverNetwork:=True)

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
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotDFolder(ByVal CreateStoragePathInfoOnly As Boolean, ByVal blnSkipBAFFiles As Boolean) As Boolean

		Dim objFileNamesToSkip As List(Of String)

		objFileNamesToSkip = New List(Of String)
		If blnSkipBAFFiles Then
			objFileNamesToSkip.Add("analysis.baf")
		End If

		Return RetrieveDotXFolder(DOT_D_EXTENSION, CreateStoragePathInfoOnly, objFileNamesToSkip)
	End Function

	''' <summary>
	''' Retrieves a Micromass .raw folder for the analysis job in progress
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotRawFolder(ByVal CreateStoragePathInfoOnly As Boolean) As Boolean
		Return RetrieveDotXFolder(DOT_RAW_EXTENSION, CreateStoragePathInfoOnly, New List(Of String))
	End Function

	''' <summary>
	''' Retrieves a folder with a name like Dataset.D or Dataset.Raw
	''' </summary>
	''' <param name="FolderExtension">Extension on the folder; for example, ".D"</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function RetrieveDotXFolder(
	  ByVal FolderExtension As String,
	  ByVal CreateStoragePathInfoOnly As Boolean,
	  ByVal objFileNamesToSkip As List(Of String)) As Boolean

		'Copies a data folder ending in FolderExtension to the working directory

		'Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
		Dim DSFolderPath As String = FindDotXFolder(FolderExtension)
		If String.IsNullOrEmpty(DSFolderPath) Then Return False

		'Do the copy
		Try
			Dim diSourceFolder As DirectoryInfo
			Dim DestFolderPath As String

			diSourceFolder = New DirectoryInfo(DSFolderPath)
			If Not diSourceFolder.Exists Then
				m_message = "Source dataset folder not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & diSourceFolder.FullName)
			End If

			DestFolderPath = Path.Combine(m_WorkingDir, diSourceFolder.Name)

			If CreateStoragePathInfoOnly Then
				If Not diSourceFolder.Exists Then
					m_message = "Source folder not found: " + diSourceFolder.FullName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				Else
					CreateStoragePathInfoFile(diSourceFolder.FullName, DestFolderPath)
				End If
			Else
				' Copy the directory and all subdirectories
				' Skip any files defined by objFileNamesToSkip
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving folder " & diSourceFolder.FullName)
				End If
				ResetTimestampForQueueWaitTimeLogging()
				m_FileTools.CopyDirectory(diSourceFolder.FullName, DestFolderPath, objFileNamesToSkip)
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
	''' <param name="UnzipOverNetwork"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function RetrieveBrukerMALDIImagingFolders(ByVal UnzipOverNetwork As Boolean) As Boolean

		Const ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK As String = "*R*X*.zip"

		Dim ChameleonCachedDataFolder As String = m_mgrParams.GetParam("ChameleonCachedDataFolder")
		Dim diCachedDataFolder As DirectoryInfo

		Dim ServerPath As String
		Dim strUnzipFolderPathBase As String

		Dim strFilesToDelete As New Queue(Of String)

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
				diCachedDataFolder = New DirectoryInfo(ChameleonCachedDataFolder)
				If Not diCachedDataFolder.Exists Then
					m_message = "Chameleon cached data folder does not exist"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + diCachedDataFolder.FullName)
					Return False
				Else
					strUnzipFolderPathBase = Path.Combine(diCachedDataFolder.FullName, m_DatasetName)
				End If

				For Each diSubFolder As DirectoryInfo In diCachedDataFolder.GetDirectories()
					If Not String.Equals(diSubFolder.Name, m_DatasetName, StringComparison.CurrentCultureIgnoreCase) Then
						' Delete this directory
						Try
							If m_DebugLevel >= 2 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting old dataset subfolder from chameleon cached data folder: " + diSubFolder.FullName)
							End If

							If m_mgrParams.GetParam("MgrName").ToLower().Contains("monroe") Then
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
				For Each fiFile As FileInfo In diCachedDataFolder.GetFiles("*.mis")
					If Not String.Equals(Path.GetFileNameWithoutExtension(fiFile.Name), m_DatasetName, StringComparison.CurrentCultureIgnoreCase) Then
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
		ServerPath = FindValidFolder(m_DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK, RetrievingInstrumentDataFolder:=True)

		Try

			Dim MisFiles() As String
			Dim strImagingSeqFilePathFinal As String

			' Look for the .mis file (ImagingSequence file) 
			strImagingSeqFilePathFinal = Path.Combine(diCachedDataFolder.FullName, m_DatasetName + ".mis")

			If Not File.Exists(strImagingSeqFilePathFinal) Then

				' Copy the .mis file (ImagingSequence file) over from the storage server
				MisFiles = Directory.GetFiles(ServerPath, "*.mis")

				If MisFiles.Length = 0 Then
					' No .mis files were found; unable to continue
					m_message = "ImagingSequence (.mis) file not found in dataset folder"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to process MALDI imaging data")
					Return False
				Else
					' We'll copy the first file in MisFiles(0)
					' Log a warning if we will be renaming the file

					If Not String.Equals(Path.GetFileName(MisFiles(0)), strImagingSeqFilePathFinal, StringComparison.CurrentCultureIgnoreCase) Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Note: Renaming .mis file (ImagingSequence file) from " + Path.GetFileName(MisFiles(0)) + " to " + Path.GetFileName(strImagingSeqFilePathFinal))
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
			ZipFiles = Directory.GetFiles(ServerPath, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK)

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
							strPathToCheck = Path.Combine(strUnzipFolderPathBase, objEntry.FileName.Replace("/"c, "\"c))

							If Not File.Exists(strPathToCheck) Then
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
							strZipFilePathToExtract = Path.Combine(m_WorkingDir, Path.GetFileName(strZipFilePathRemote))

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

						Using objZipfile = New Ionic.Zip.ZipFile(strZipFilePathToExtract)
							If m_DebugLevel >= 2 Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping " + strZipFilePathToExtract)
							End If

							objZipfile.ExtractAll(strUnzipFolderPathBase, Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)
						End Using

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
				Dim dtStartTime As DateTime = DateTime.UtcNow

				Do While strFilesToDelete.Count > 0
					' Try to process the files remaining in queue strFilesToDelete

					DeleteQueuedFiles(strFilesToDelete, String.Empty)

					If strFilesToDelete.Count > 0 Then
						If DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 20 Then
							' Stop trying to delete files; it's not worth continuing to try
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete all of the files in queue strFilesToDelete; Queue Length = " + strFilesToDelete.Count.ToString() + "; this warning can be safely ignored (function RetrieveBrukerMALDIImagingFolders)")
							Exit Do
						End If

						Threading.Thread.Sleep(500)
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
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function RetrieveSFolders(ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

		Dim ZipFiles() As String
		Dim DSWorkFolder As String
		Dim UnZipper As clsIonicZipTools

		Dim SourceFilePath As String
		Dim TargetFolderPath As String

		Dim ZipFile As String

		Try

			'First Check for the existence of a 0.ser Folder
			'If 0.ser folder exists, then either store the path to the 0.ser folder in a StoragePathInfo file, or copy the 0.ser folder to the working directory
			Dim DSFolderPath As String = FindValidFolder(m_DatasetName, "", BRUKER_ZERO_SER_FOLDER, RetrievingInstrumentDataFolder:=True)

			If Not String.IsNullOrEmpty(DSFolderPath) Then
				Dim diSourceFolder As DirectoryInfo
				Dim diTargetFolder As DirectoryInfo
				Dim fiFile As FileInfo

				diSourceFolder = New DirectoryInfo(Path.Combine(DSFolderPath, BRUKER_ZERO_SER_FOLDER))

				If diSourceFolder.Exists Then
					If CreateStoragePathInfoOnly Then
						If CreateStoragePathInfoFile(diSourceFolder.FullName, m_WorkingDir + "\") Then
							Return True
						Else
							Return False
						End If
					Else
						' Copy the 0.ser folder to the Work directory
						' First create the 0.ser subfolder
						diTargetFolder = Directory.CreateDirectory(Path.Combine(m_WorkingDir, BRUKER_ZERO_SER_FOLDER))

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
			If Not CopySFoldersToWorkDir(CreateStoragePathInfoOnly) Then
				'Error messages have already been logged, so just exit
				Return False
			End If

			If CreateStoragePathInfoOnly Then
				' Nothing was copied locally, so nothing to unzip
				Return True
			End If


			'Get a listing of the zip files to process
			ZipFiles = Directory.GetFiles(m_WorkingDir, "s*.zip")
			If ZipFiles.GetLength(0) < 1 Then
				m_message = "No zipped s-folders found in working directory"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False			'No zipped data files found
			End If

			'Create a dataset subdirectory under the working directory
			DSWorkFolder = Path.Combine(m_WorkingDir, m_DatasetName)
			Directory.CreateDirectory(DSWorkFolder)

			'Set up the unzipper
			UnZipper = New clsIonicZipTools(m_DebugLevel, DSWorkFolder)

			'Unzip each of the zip files to the working directory
			For Each ZipFile In ZipFiles
				If m_DebugLevel > 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file " + ZipFile)
				End If
				Try
					TargetFolderPath = Path.Combine(DSWorkFolder, Path.GetFileNameWithoutExtension(ZipFile))
					Directory.CreateDirectory(TargetFolderPath)

					SourceFilePath = Path.Combine(m_WorkingDir, Path.GetFileName(ZipFile))

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

			Threading.Thread.Sleep(125)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			'Delete all s*.zip files in working directory
			For Each ZipFile In ZipFiles
				Try
					File.Delete(Path.Combine(m_WorkingDir, Path.GetFileName(ZipFile)))
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

	Protected Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean
		Dim udtHPCOptions = New udtHPCOptionsType
		Return RetrieveOrgDB(LocalOrgDBFolder, udtHPCOptions)
	End Function

	''' <summary>
	''' Uses Ken's dll to create a fasta file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
	''' </summary>
	''' <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Stores the name of the FASTA file as a new job parameter named "generatedFastaName" in section "PeptideSearch"</remarks>
	Protected Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String, ByVal udtHPCOptions As udtHPCOptionsType) As Boolean

		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Obtaining org db file")
		End If

		Try
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

			If Not udtHPCOptions.UsingHPC Then
				' Delete old fasta files and suffix array files if getting low on disk space
				Const freeSpaceThresholdPercent As Integer = 20
				PurgeFastaFilesIfLowFreeSpace(LocalOrgDBFolder, freeSpaceThresholdPercent)
			End If


		Catch ex As Exception
			m_message = "Exception in RetrieveOrgDB: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in RetrieveOrgDB", ex)
			Return False
		End Try

		'We got to here OK, so return
		Return True

	End Function

	''' <summary>
	''' Overrides base class version of the function to creates a Sequest params file compatible 
	'''	with the Bioworks version on this System. Uses ParamFileGenerator dll provided by Ken Auberry
	''' </summary>
	''' <param name="ParamFileName">Name of param file to be created</param>
	''' <param name="ParamFilePath">Param file storage path</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability</remarks>
	Protected Function RetrieveGeneratedParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String) As Boolean

		Dim ParFileGen As ParamFileGenerator.MakeParams.IGenerateFile = Nothing
		Dim blnSuccess As Boolean

		Try
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving parameter file")

			ParFileGen = New ParamFileGenerator.MakeParams.clsMakeParameterFile
			ParFileGen.TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")

			' Note that job parameter "generatedFastaName" gets defined by RetrieveOrgDB
			' Furthermore, the full path to the fasta file is only necessary when creating Sequest parameter files
			Dim paramFileType = SetBioworksVersion(m_jobParams.GetParam("ToolName"))
			Dim fastaFilePath = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
			Dim connectionString = m_mgrParams.GetParam("connectionstring")
			Dim datasetID As Integer = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0)

			blnSuccess = ParFileGen.MakeFile(ParamFileName, paramFileType, fastaFilePath, m_WorkingDir, connectionString, datasetID)

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
	''' </summary>
	''' <param name="FileName">Name of file to be copied</param>
	''' <param name="FilePath">File storage path</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	Protected Function RetrieveFile(ByVal FileName As String, ByVal FilePath As String) As Boolean


		'Copy the file
		If Not CopyFileToWorkDir(FileName, FilePath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
			Return False
		End If

		Return True

	End Function

	''' <summary>
	''' This is just a generic function to copy files to the working directory
	'''	
	''' </summary>
	''' <param name="FileName">Name of file to be copied</param>
	''' <param name="FilePath">File storage path</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	Protected Function RetrieveFile(ByVal FileName As String, ByVal FilePath As String, ByVal MaxCopyAttempts As Integer) As Boolean

		'Copy the file
		If MaxCopyAttempts < 1 Then MaxCopyAttempts = 1
		If Not CopyFileToWorkDir(FileName, FilePath, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts) Then
			Return False
		End If

		Return True

	End Function


	''' <summary>
	''' Finds the _DTA.txt file for this dataset
	''' </summary>
	''' <returns>The path to the _dta.zip file (or _dta.txt file)</returns>
	''' <remarks></remarks>
	Protected Function FindCDTAFile(ByRef strErrorMessage As String) As String

		Dim SourceFileName As String
		Dim SourceFolderPath As String

		strErrorMessage = String.Empty

		'Retrieve zipped DTA file
		SourceFileName = m_DatasetName + "_dta.zip"
		SourceFolderPath = FindDataFile(SourceFileName)

		If Not String.IsNullOrEmpty(SourceFolderPath) Then
			If SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
				Return SourceFolderPath
			Else
				' Return the path to the _dta.zip file
				Return Path.Combine(SourceFolderPath, SourceFileName)
			End If
		End If

		' Couldn't find a folder with the _dta.zip file; how about the _dta.txt file?

		SourceFileName = m_DatasetName + "_dta.txt"
		SourceFolderPath = FindDataFile(SourceFileName)

		If String.IsNullOrEmpty(SourceFolderPath) Then
			' No folder found containing the zipped DTA files; return False
			' (the FindDataFile procedure should have already logged an error)
			strErrorMessage = "Could not find " + SourceFileName + " using FindDataFile"
			Return String.Empty
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Warning: could not find the _dta.zip file, but was able to find " + SourceFileName + " in folder " + SourceFolderPath)

			If SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
				Return SourceFolderPath
			Else
				' Return the path to the _dta.txt file
				Return Path.Combine(SourceFolderPath, SourceFileName)
			End If

		End If

	End Function

	''' <summary>
	''' Retrieves the _DTA.txt file (either zipped or unzipped).  
	''' </summary>
	''' <returns>TRUE for success, FALSE for error</returns>
	''' <remarks>If the _dta.zip or _dta.txt file already exists in the working folder then will not re-copy it from the remote folder</remarks>
	Public Function RetrieveDtaFiles() As Boolean

		Dim TargetZipFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName + "_dta.zip")
		Dim TargetCDTAFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName + "_dta.txt")

		If Not File.Exists(TargetCDTAFilePath) And Not File.Exists(TargetZipFilePath) Then

			Dim SourceFilePath As String
			Dim strErrorMessage As String = String.Empty

			' Find the CDTA file
			SourceFilePath = FindCDTAFile(strErrorMessage)

			If String.IsNullOrEmpty(SourceFilePath) Then
				m_message = strErrorMessage
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If SourceFilePath.StartsWith(MYEMSL_PATH_FLAG) Then
				If ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Downloaded " + m_MyEMSLDatasetListInfo.DownloadedFiles.First().Value.Filename + " from MyEMSL")
					End If
				Else
					Return False
				End If
			Else

				Dim fiSourceFile As FileInfo = New FileInfo(SourceFilePath)

				' Copy the file locally
				If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
					m_message = "Error copying " + fiSourceFile.Name
					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " + fiSourceFile.Name + " using folder " + fiSourceFile.Directory.FullName)
					End If
					Return False
				Else
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " + fiSourceFile.Name + " from folder " + fiSourceFile.FullName)
					End If
				End If
			End If

		End If

		If Not File.Exists(TargetCDTAFilePath) Then

			If Not File.Exists(TargetZipFilePath) Then
				m_message = Path.GetFileName(TargetZipFilePath) & " not found in the working directory"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; cannot unzip in RetrieveDtaFiles")
				Return False
			End If

			' Unzip concatenated DTA file
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
			If UnzipFileStart(TargetZipFilePath, m_WorkingDir, "RetrieveDtaFiles", False) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
				End If
			End If

			' Delete the _DTA.zip file to free up some disk space
			Threading.Thread.Sleep(100)
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting the _DTA.zip file")
			End If

			Try
				Threading.Thread.Sleep(125)
				PRISM.Processes.clsProgRunner.GarbageCollectNow()

				File.Delete(TargetZipFilePath)
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error deleting the _DTA.zip file: " + ex.Message)
			End Try

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
		If UnzipFileStart(Path.Combine(m_WorkingDir, ZippedFileName), m_WorkingDir, "RetrieveOutFiles", False) Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Concatenated OUT file unzipped")
			End If
		End If

		'Unconcatenate OUT file if needed
		If UnConcatenate Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated OUT file")

			Dim fiSourceFile As FileInfo
			fiSourceFile = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName + "_out.txt"))

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

		Dim OutputFile As String = Path.Combine(m_WorkingDir, m_jobParams.GetParam("SettingsFileName"))

		Return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile)

	End Function

	''' <summary>
	''' Returns True if the filename ends with any of the suffixes in lstNonCriticalFileSuffixes
	''' </summary>
	''' <param name="strFileName"></param>
	''' <param name="lstNonCriticalFileSuffixes"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SafeToIgnore(ByVal strFileName As String, ByVal lstNonCriticalFileSuffixes As List(Of String)) As Boolean

		If Not lstNonCriticalFileSuffixes Is Nothing Then

			strFileName = strFileName.ToLower()
			For Each strSuffix As String In lstNonCriticalFileSuffixes
				If strFileName.EndsWith(strSuffix.ToLower()) Then
					' It's OK that this file is missing
					Return True
				End If
			Next

		End If

		Return False

	End Function

	''' <summary>
	''' Specifies the Bioworks version for use by the Param File Generator DLL
	''' </summary>
	''' <param name="ToolName">Version specified in mgr config file</param>
	''' <returns>IGenerateFile.ParamFileType based on input version</returns>
	''' <remarks></remarks>
	Protected Function SetBioworksVersion(ByVal ToolName As String) As ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType

		Dim strToolNameLCase As String

		strToolNameLCase = ToolName.ToLower()

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
			Case "msalign_histone"
				Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSAlignHistone
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
				ElseIf strToolNameLCase.Contains("msalign_histone") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSAlignHistone
				ElseIf strToolNameLCase.Contains("msalign") Then
					Return ParamFileGenerator.MakeParams.IGenerateFile.ParamFileType.MSAlign
				Else
					Return Nothing
				End If
		End Select

	End Function

	''' <summary>
	''' Converts the dictionary items to a list of key/value pairs separated by an equals sign
	''' Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
	''' </summary>
	''' <param name="dctItems">Dictionary items to store as a packed job parameter</param>
	''' <param name="strParameterName">Packed job parameter name</param>
	''' <remarks></remarks>
	Protected Sub StorePackedJobParameterDictionary(ByVal dctItems As Dictionary(Of String, String), ByVal strParameterName As String)

		Dim lstItems As List(Of String) = New List(Of String)

		For Each item As KeyValuePair(Of String, String) In dctItems
			lstItems.Add(item.Key & "=" & item.Value)
		Next

		StorePackedJobParameterList(lstItems, strParameterName)

	End Sub

	''' <summary>
	''' Convert a string list to a packed job parameter (items are separated by tab characters)
	''' </summary>
	''' <param name="lstItems">List items to store as a packed job parameter</param>
	''' <param name="strParameterName">Packed job parameter name</param>
	''' <remarks></remarks>
	Protected Sub StorePackedJobParameterList(ByVal lstItems As List(Of String), ByVal strParameterName As String)

		m_jobParams.AddAdditionalParameter("JobParameters", strParameterName, clsGlobal.FlattenList(lstItems, ControlChars.Tab))

	End Sub

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

		Dim fiFileInfo As FileInfo
		Dim sngFileSizeMB As Single

		Dim blnUseExternalUnzipper As Boolean = False
		Dim blnSuccess As Boolean

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

			fiFileInfo = New FileInfo(ZipFilePath)
			sngFileSizeMB = CSng(fiFileInfo.Length / 1024.0 / 1024)

			If Not fiFileInfo.Exists Then
				' File not found
				m_message = "Error unzipping '" + ZipFilePath + "': File not found (called from " + CallingFunctionName + ")"

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			If ZipFilePath.ToLower().EndsWith(".gz") Then
				' This is a gzipped file
				' Use Ionic.Zip
				strUnzipperName = clsIonicZipTools.IONIC_ZIP_NAME
				m_IonicZipTools.DebugLevel = m_DebugLevel
				Return m_IonicZipTools.GUnzipFile(ZipFilePath, OutFolderPath)
			End If

			' Use the external zipper if the file size is over IONIC_ZIP_MAX_FILESIZE_MB or if ForceExternalZipProgramUse = True
			' However, if the .Exe file for the external zipper is not found, then fall back to use Ionic.Zip
			If ForceExternalZipProgramUse OrElse sngFileSizeMB >= IONIC_ZIP_MAX_FILESIZE_MB Then
				If strExternalUnzipperFilePath.Length > 0 AndAlso _
				   strExternalUnzipperFilePath.ToLower() <> "na" Then
					If File.Exists(strExternalUnzipperFilePath) Then
						blnUseExternalUnzipper = True
					End If
				End If

				If Not blnUseExternalUnzipper Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "External zip program not found: " + strExternalUnzipperFilePath + "; will instead use Ionic.Zip")
				End If
			End If

			If blnUseExternalUnzipper Then
				strUnzipperName = Path.GetFileName(strExternalUnzipperFilePath)

				Dim UnZipper As New PRISM.Files.ZipTools(OutFolderPath, strExternalUnzipperFilePath)

				dtStartTime = DateTime.UtcNow
				blnSuccess = UnZipper.UnzipFile("", ZipFilePath, OutFolderPath)
				dtEndTime = DateTime.UtcNow

				If blnSuccess Then
					m_IonicZipTools.ReportZipStats(fiFileInfo, dtStartTime, dtEndTime, False, strUnzipperName)
				Else
					m_message = "Error unzipping " + Path.GetFileName(ZipFilePath) + " using " + strUnzipperName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, CallingFunctionName + ": " + m_message)
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
	''' Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
	''' </summary>
	''' <param name="strWorkDir">Folder with the CDTA file</param>
	''' <param name="strInputFileName">CDTA filename</param>
	''' <returns>True if success; false if an error</returns>
	Protected Function ValidateCDTAFileRemoveSparseSpectra(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
		Dim blnSuccess As Boolean

		blnSuccess = m_CDTAUtilities.RemoveSparseSpectra(strWorkDir, strInputFileName)
		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
			m_message = "m_CDTAUtilities.RemoveSparseSpectra returned False"
		End If

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
	Protected Function ValidateCDTAFileScanAndCSTags(ByVal strSourceFilePath As String, ByVal blnReplaceSourceFile As Boolean, ByVal blnDeleteSourceFileIfUpdated As Boolean, ByRef strOutputFilePath As String) As Boolean

		Dim blnSuccess As Boolean

		blnSuccess = m_CDTAUtilities.ValidateCDTAFileScanAndCSTags(strSourceFilePath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, strOutputFilePath)
		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
			m_message = "m_CDTAUtilities.ValidateCDTAFileScanAndCSTags returned False"
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Condenses CDTA files that are over 2 GB in size
	''' </summary>
	''' <param name="strWorkDir"></param>
	''' <param name="strInputFileName"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ValidateCDTAFileSize(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
		Dim blnSuccess As Boolean

		blnSuccess = m_CDTAUtilities.ValidateCDTAFileSize(strWorkDir, strInputFileName)
		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
			m_message = "m_CDTAUtilities.ValidateCDTAFileSize returned False"
		End If

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
		Const intNumericDataColIndex As Integer = 0
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

		Dim fiFileInfo As FileInfo

		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim dblValue As Double
		Dim blnDataFound As Boolean

		strErrorMessage = String.Empty

		Try
			fiFileInfo = New FileInfo(strFilePath)

			If Not fiFileInfo.Exists Then
				strErrorMessage = strFileDescription + " file not found: " + fiFileInfo.Name
				Return False
			End If

			If fiFileInfo.Length = 0 Then
				strErrorMessage = strFileDescription + " file is empty (zero-bytes)"
				Return False
			End If

			' Open the file and confirm it has data rows
			Using srInFile As StreamReader = New StreamReader(New FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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

		Const blnLogFreeMemoryOnSuccess As Boolean = True
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

	Private Sub m_CDTAUtilities_ErrorEvent(ByVal ErrorMessage As String) Handles m_CDTAUtilities.ErrorEvent
		m_message = ErrorMessage
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage)
	End Sub

	Private Sub m_CDTAUtilities_InfoEvent(ByVal strMessage As String, ByVal DebugLevel As Integer) Handles m_CDTAUtilities.InfoEvent
		If m_DebugLevel >= DebugLevel Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
		End If
	End Sub

	Private Sub m_CDTAUtilities_ProgressEvent(ByVal taskDescription As String, ByVal percentComplete As Single) Handles m_CDTAUtilities.ProgressEvent

		Static dtLastUpdateTime As DateTime

		If m_DebugLevel >= 1 Then
			If m_DebugLevel = 1 AndAlso DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse _
			   m_DebugLevel > 1 AndAlso DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
				dtLastUpdateTime = DateTime.UtcNow

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... CDTAUtilities: " & percentComplete.ToString("0.00") & "% complete")
			End If
		End If

	End Sub

	Private Sub m_CDTAUtilities_WarningEvent(ByVal strMessage As String) Handles m_CDTAUtilities.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
	End Sub

	Private Sub m_FileTools_WaitingForLockQueue(SourceFilePath As String, TargetFilePath As String, MBBacklogSource As Integer, MBBacklogTarget As Integer) Handles m_FileTools.WaitingForLockQueue

		If IsLockQueueLogMessageNeeded(m_LockQueueWaitTimeStart, m_LastLockQueueWaitTimeLog) Then
			m_LastLockQueueWaitTimeLog = DateTime.UtcNow
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Waiting for lockfile queue to fall below threshold (clsAnalysisResources); SourceBacklog=" & MBBacklogSource & " MB, TargetBacklog=" & MBBacklogTarget & " MB, Source=" & SourceFilePath & ", Target=" & TargetFilePath)
			End If
		End If

	End Sub

	Private Sub m_SplitFastaFileUtility_ErrorEvent(strMessage As String) Handles m_SplitFastaFileUtility.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
	End Sub

	Private Sub m_SplitFastaFileUtility_ProgressUpdate(progressMessage As String, percentComplete As Integer) Handles m_SplitFastaFileUtility.ProgressUpdate

		If m_DebugLevel >= 1 Then
			If m_DebugLevel = 1 AndAlso DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 60 OrElse
			   m_DebugLevel > 1 AndAlso DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 20 OrElse
			   percentComplete = 100 And m_SplitFastaLastPercentComplete < 100 Then

				m_SplitFastaLastUpdateTime = DateTime.UtcNow
				m_SplitFastaLastPercentComplete = percentComplete

				If percentComplete > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & progressMessage & ", " & percentComplete & "% complete")
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... SplitFastaFile: " & progressMessage)
				End If

			End If
		End If
	End Sub

	Private Sub m_SplitFastaFileUtility_SplittingBaseFastafile(strBaseFastaFileName As String, numSplitParts As Integer) Handles m_SplitFastaFileUtility.SplittingBaseFastafile
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Splitting " & strBaseFastaFileName & " into " & numSplitParts & " parts")
	End Sub

#End Region

#Region "MyEMSL Event Handlers"

	Private Sub m_MyEMSLDatasetListInfo_ErrorEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles m_MyEMSLDatasetListInfo.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, e.Message)
	End Sub

	Private Sub m_MyEMSLDatasetListInfo_MessageEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles m_MyEMSLDatasetListInfo.MessageEvent
		Console.WriteLine(e.Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
	End Sub

	Private Sub m_MyEMSLDatasetListInfo_ProgressEvent(sender As Object, e As MyEMSLReader.ProgressEventArgs) Handles m_MyEMSLDatasetListInfo.ProgressEvent
		If DateTime.UtcNow.Subtract(m_LastMyEMSLProgressWriteTime).TotalMinutes > 0.2 Then
			m_LastMyEMSLProgressWriteTime = DateTime.UtcNow
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MyEMSL downloader: " & e.PercentComplete & "% complete")
		End If
	End Sub

	Private Sub m_MyEMSLDatasetListInfo_FileDownloadedEvent(sender As Object, e As MyEMSLReader.FileDownloadedEventArgs) Handles m_MyEMSLDatasetListInfo.FileDownloadedEvent

		If e.UnzipRequired Then
			Dim fiFileToUnzip = New FileInfo(Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename))

			If fiFileToUnzip.Exists AndAlso fiFileToUnzip.Extension.ToLower() = ".zip" Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + fiFileToUnzip.Name)
				m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath)
			End If
		End If

	End Sub
#End Region
End Class


