'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading

Public Class clsAnalysisToolRunnerBase
	Implements IToolRunner

	'*********************************************************************************************************
	'Base class for analysis tool runner
	'*********************************************************************************************************

#Region "Constants"
	Protected Const SP_NAME_SET_TASK_TOOL_VERSION As String = "SetStepTaskToolVersion"
	Public Const DATE_TIME_FORMAT As String = "yyyy-MM-dd hh:mm:ss tt"
	Public Const PVM_RESET_ERROR_MESSAGE As String = "Error resetting PVM"
#End Region

#Region "Module variables"
	'status tools
	Protected m_StatusTools As IStatusFile

	' access to the job parameters
	Protected m_jobParams As IJobParams

	' access to mgr parameters
	Protected m_mgrParams As IMgrParams

	' access to settings file parameters
	Protected m_settingsFileParams As New PRISM.Files.XmlSettingsFileAccessor

	' progress of run (in percent); This is a value between 0 and 100
	Protected m_progress As Single = 0

	'	status code
	Protected m_status As IStatusFile.EnumMgrStatus

	'DTA count for status report
	Protected m_DtaCount As Integer = 0

	' For posting a general explanation for external consumption
	Protected m_message As String = String.Empty

	Protected m_EvalCode As Integer = 0							' Can be used to pass codes regarding the results of this analysis back to the DMS_Pipeline DB
	Protected m_EvalMessage As String = String.Empty			' Can be used to pass information regarding the results of this analysis back to the DMS_Pipeline DB        

	'Debug level
	Protected m_DebugLevel As Short

	'Working directory, machine name (aka manager name), & job number (used frequently by subclasses)
	Protected m_WorkDir As String
	Protected m_MachName As String
	Protected m_JobNum As String
	Protected m_Dataset As String

	'Elapsed time information
	Protected m_StartTime As Date
	Protected m_StopTime As Date

	'Results folder name
	Protected m_ResFolderName As String

	'DLL file info
	Protected m_FileVersion As String
	Protected m_FileDate As String

	Protected m_IonicZipTools As clsIonicZipTools
	Protected WithEvents m_FileTools As PRISM.Files.clsFileTools

	Protected m_NeedToAbortProcessing As Boolean

	Protected m_SummaryFile As clsSummaryFile

	Private m_LastLockQueueWaitTimeLog As DateTime = DateTime.UtcNow

#End Region

#Region "Properties"

	''' <summary>
	''' Evaluation code to be reported to the DMS_Pipeline DB
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property EvalCode As Integer Implements IToolRunner.EvalCode
		Get
			Return m_EvalCode
		End Get
	End Property

	''' <summary>
	''' Evaluation message to be reported to the DMS_Pipeline DB
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property EvalMessage As String Implements IToolRunner.EvalMessage
		Get
			Return m_EvalMessage
		End Get
	End Property

	''' <summary>
	''' Publicly accessible results folder name and path
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property ResFolderName() As String Implements IToolRunner.ResFolderName
		Get
			Return m_ResFolderName
		End Get
	End Property

	''' <summary>
	''' Explanation of what happened to last operation this class performed
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property Message() As String Implements IToolRunner.Message
		Get
			Return m_message
		End Get
	End Property

	Public ReadOnly Property NeedToAbortProcessing() As Boolean Implements IToolRunner.NeedToAbortProcessing
		Get
			Return m_NeedToAbortProcessing
		End Get
	End Property

	' the state of completion of the job (as a percentage)
	Public ReadOnly Property Progress() As Single Implements IToolRunner.Progress
		Get
			Return m_progress
		End Get
	End Property
#End Region

#Region "Methods"

	''' <summary>
	''' Initializes class
	''' </summary>
	''' <param name="mgrParams">Object holding manager parameters</param>
	''' <param name="jobParams">Object holding job parameters</param>
	''' <param name="StatusTools">Object for status reporting</param>
	''' <remarks></remarks>
	Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
	  ByVal StatusTools As IStatusFile, ByRef SummaryFile As clsSummaryFile) Implements IToolRunner.Setup

		m_mgrParams = mgrParams
		m_jobParams = jobParams
		m_StatusTools = StatusTools
		m_WorkDir = m_mgrParams.GetParam("workdir")
		m_MachName = m_mgrParams.GetParam("MgrName")
		m_JobNum = m_jobParams.GetParam("StepParameters", "Job")
		m_Dataset = m_jobParams.GetParam("JobParameters", "DatasetNum")
		m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel", 1))
		m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription()

		m_SummaryFile = SummaryFile

		m_ResFolderName = m_jobParams.GetParam("OutputFolderName")

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.Setup()")
		End If

		m_IonicZipTools = New clsIonicZipTools(m_DebugLevel, m_WorkDir)

		ResetTimestampForQueueWaitTimeLogging()
		m_FileTools = New PRISM.Files.clsFileTools(m_MachName, m_DebugLevel)

		m_NeedToAbortProcessing = False

		m_message = String.Empty
		m_EvalCode = 0
		m_EvalMessage = String.Empty
	End Sub

	''' <summary>
	''' Calculates total run time for a job
	''' </summary>
	''' <param name="StartTime">Time job started</param>
	''' <param name="StopTime">Time of job completion</param>
	''' <returns>Total job run time (HH:MM)</returns>
	''' <remarks></remarks>
	Protected Function CalcElapsedTime(ByVal StartTime As DateTime, ByVal StopTime As DateTime) As String
		Dim dtElapsedTime As TimeSpan

		If StopTime < StartTime Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Stop time is less than StartTime; this is unexpected.  Assuming current time for StopTime")
			StopTime = DateTime.UtcNow
		End If

		If StopTime < StartTime OrElse StartTime = DateTime.MinValue Then
			Return String.Empty
		End If

		dtElapsedTime = StopTime.Subtract(StartTime)

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, StartTime = " & StartTime.ToString)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Stoptime = " & StopTime.ToString)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Hours = " & dtElapsedTime.Hours.ToString)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Minutes = " & dtElapsedTime.Minutes.ToString)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, Seconds = " & dtElapsedTime.Seconds.ToString)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "CalcElapsedTime, TotalMinutes = " & dtElapsedTime.TotalMinutes.ToString("0.00"))
		End If

		Return dtElapsedTime.Hours.ToString("###0") & ":" & dtElapsedTime.Minutes.ToString("00") & ":" & dtElapsedTime.Seconds.ToString("00")

	End Function

	''' <summary>
	''' Computes the incremental progress that has been made beyond CurrentTaskProgressAtStart, based on the number of items processed and the next overall progress level
	''' </summary>
	''' <param name="CurrentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
	''' <param name="CurrentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
	''' <param name="CurrentTaskItemsProcessed">Number of items processed so far during this subtask</param>
	''' <param name="CurrentTaskTotalItems">Total number of items to process during this subtask</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Shared Function ComputeIncrementalProgress(ByVal CurrentTaskProgressAtStart As Single, ByVal CurrentTaskProgressAtEnd As Single, ByVal CurrentTaskItemsProcessed As Integer, ByVal CurrentTaskTotalItems As Integer) As Single
		If CurrentTaskTotalItems < 1 Then
			Return CurrentTaskProgressAtStart
		ElseIf CurrentTaskItemsProcessed > CurrentTaskTotalItems Then
			Return CurrentTaskProgressAtEnd
		Else
			Return CSng(CurrentTaskProgressAtStart + (CurrentTaskItemsProcessed / CurrentTaskTotalItems) * (CurrentTaskProgressAtEnd - CurrentTaskProgressAtStart))
		End If
	End Function

	''' <summary>
	''' Copies a file (typically a mzXML file) to a server cache folder
	''' Will store the file in the subfolder strSubfolderInTarget and, below that, in a folder with a name like 2013_2, based on the DatasetStoragePath job parameter
	''' </summary>
	''' <param name="strCacheFolderPath">Cache folder base path</param>
	''' <param name="strSubfolderInTarget">Subfolder name to create below strCacheFolderPath (optional)</param>
	''' <param name="strSourceFilePath">Path to the data file</param>
	''' <param name="strDatasetYearQuarter">Dataset year quarter text (optional); example value is 2013_2; if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
	''' <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 300 GB</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CopyFileToServerCache(ByVal strCacheFolderPath As String, ByVal strSubfolderInTarget As String, ByVal strSourceFilePath As String, ByVal strDatasetYearQuarter As String, ByVal blnPurgeOldFilesIfNeeded As Boolean) As Boolean

		Dim blnSuccess As Boolean

		Try

			Dim diCacheFolder As DirectoryInfo = New DirectoryInfo(strCacheFolderPath)

			If Not diCacheFolder.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Cache folder not found: " & strCacheFolderPath)
			Else

				Dim diTargetFolder As DirectoryInfo

				' Define the target folder
				If String.IsNullOrEmpty(strSubfolderInTarget) Then
					diTargetFolder = diCacheFolder
				Else
					diTargetFolder = New DirectoryInfo(Path.Combine(diCacheFolder.FullName, strSubfolderInTarget))
					If Not diTargetFolder.Exists Then diTargetFolder.Create()
				End If

				If String.IsNullOrEmpty(strDatasetYearQuarter) Then
					' Determine the year_quarter text for this dataset
					Dim strDatasetStoragePath As String = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
					If String.IsNullOrEmpty(strDatasetStoragePath) Then strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")

					strDatasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(strDatasetStoragePath)
				End If

				If Not String.IsNullOrEmpty(strDatasetYearQuarter) Then
					diTargetFolder = New DirectoryInfo(Path.Combine(diTargetFolder.FullName, strDatasetYearQuarter))
					If Not diTargetFolder.Exists Then diTargetFolder.Create()
				End If

				m_jobParams.AddResultFileExtensionToSkip(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX)

				' Create the .hashcheck file
				Dim strHashcheckFilePath As String
				strHashcheckFilePath = clsGlobal.CreateHashcheckFile(strSourceFilePath, blnComputeMD5Hash:=True)

				If String.IsNullOrEmpty(strHashcheckFilePath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CopyFileToServerCache: Hashcheck file was not created")
					Return False
				End If

				Dim diTargetFile As FileInfo = New FileInfo(Path.Combine(diTargetFolder.FullName, Path.GetFileName(strSourceFilePath)))

				ResetTimestampForQueueWaitTimeLogging()
				blnSuccess = m_FileTools.CopyFileUsingLocks(strSourceFilePath, diTargetFile.FullName, m_MachName, True)

				If blnSuccess Then
					' Copy over the .Hashcheck file
					m_FileTools.CopyFile(strHashcheckFilePath, Path.Combine(diTargetFile.DirectoryName, Path.GetFileName(strHashcheckFilePath)), True)
				End If

				If blnSuccess AndAlso blnPurgeOldFilesIfNeeded Then
					Const intSpaceUsageThresholdGB As Integer = 300
					PurgeOldServerCacheFiles(strCacheFolderPath, intSpaceUsageThresholdGB)
				End If
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CopyFileToServerCache: " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Copies the .mzXML file to the MSXML_Cache
	''' </summary>
	''' <param name="strSourceFilePath"></param>
	''' <param name="strDatasetYearQuarter">Dataset year quarter text, e.g. 2013_2;  if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
	''' <param name="strMSXmlGeneratorName">Name of the MzXML generator, e.g. MSConvert</param>
	''' <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 300 GB</param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Protected Function CopyMzXMLFileToServerCache(ByVal strSourceFilePath As String, ByVal strDatasetYearQuarter As String, ByVal strMSXmlGeneratorName As String, ByVal blnPurgeOldFilesIfNeeded As Boolean) As Boolean

		Dim blnSuccess As Boolean

		Try

			Dim strMSXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)

			If String.IsNullOrEmpty(strMSXmlGeneratorName) Then
				strMSXmlGeneratorName = m_jobParams.GetJobParameter("MSXMLGenerator", String.Empty)

				If Not String.IsNullOrEmpty(strMSXmlGeneratorName) Then
					strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(strMSXmlGeneratorName)
				End If
			End If

			blnSuccess = CopyFileToServerCache(strMSXMLCacheFolderPath, strMSXmlGeneratorName, strSourceFilePath, strDatasetYearQuarter, blnPurgeOldFilesIfNeeded)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CopyMzXMLFileToServerCache: " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Copies the files from the results folder to the transfer folder on the server
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CopyResultsFolderToServer() As IJobParams.CloseOutType

		Dim ResultsFolderName As String
		Dim SourceFolderPath As String = String.Empty
		Dim TargetFolderPath As String

		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

		Dim strMessage As String
		Dim blnErrorEncountered As Boolean = False
		Dim intFailedFileCount As Integer = 0


		Const intRetryCount As Integer = 10
		Const intRetryHoldoffSeconds As Integer = 15
		Const blnIncreaseHoldoffOnEachRetry As Boolean = True

		Try

			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.DELIVERING_RESULTS, 0)

			ResultsFolderName = m_jobParams.GetParam("OutputFolderName")
			If String.IsNullOrEmpty(ResultsFolderName) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder name is not defined, job " & m_jobParams.GetParam("StepParameters", "Job"))
				m_message = "Results folder not found"
				' Without a source folder; there isn't much we can do
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			SourceFolderPath = Path.Combine(m_WorkDir, ResultsFolderName)

			'Verify the source folder exists
			If Not Directory.Exists(SourceFolderPath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder not found, job " & m_jobParams.GetParam("StepParameters", "Job") & ", folder " & SourceFolderPath)
				m_message = "Results folder not found"
				' Without a source folder; there isn't much we can do
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			TargetFolderPath = CreateRemoteTransferFolder(objAnalysisResults)
			If String.IsNullOrEmpty(TargetFolderPath) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating results folder in transfer directory: " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error creating dataset folder in transfer directory")
			If Not String.IsNullOrEmpty(SourceFolderPath) Then
				objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
			End If

			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Copy results folder to xfer folder
		' Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)
		Try

			' Copy all of the files and subdirectories in the local result folder to the target folder
			Dim eResult As IJobParams.CloseOutType

			' Copy the files and subfolders
			eResult = CopyResultsFolderRecursive(SourceFolderPath, SourceFolderPath, TargetFolderPath, _
			  objAnalysisResults, blnErrorEncountered, intFailedFileCount, _
			  intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying results folder to " & Path.GetPathRoot(TargetFolderPath) & " : " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error copying results folder to " & Path.GetPathRoot(TargetFolderPath))
			blnErrorEncountered = True
		End Try

		If blnErrorEncountered Then
			strMessage = "Error copying " & intFailedFileCount.ToString & " file"
			If intFailedFileCount <> 1 Then
				strMessage &= "s"
			End If
			strMessage &= " to transfer folder"
			m_message = clsGlobal.AppendToComment(m_message, strMessage)
			objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	''' <summary>
	''' Copies each of the files in the source folder to the target folder
	''' Uses CopyFileWithRetry to retry the copy up to intRetryCount times
	''' </summary>
	''' <param name="SourceFolderPath"></param>
	''' <param name="TargetFolderPath"></param>
	''' <remarks></remarks>
	Private Function CopyResultsFolderRecursive(ByVal RootSourceFolderPath As String, ByVal SourceFolderPath As String, ByVal TargetFolderPath As String, _
	  ByRef objAnalysisResults As clsAnalysisResults, _
	  ByRef blnErrorEncountered As Boolean, _
	  ByRef intFailedFileCount As Integer, _
	  ByVal intRetryCount As Integer, _
	  ByVal intRetryHoldoffSeconds As Integer, _
	  ByVal blnIncreaseHoldoffOnEachRetry As Boolean) As IJobParams.CloseOutType

		Dim objSourceFolderInfo As DirectoryInfo
		Dim objSourceFile As FileInfo
		Dim objTargetFile As FileInfo

		Dim htFilesToOverwrite As Hashtable

		Dim ResultFiles() As String
		Dim strSourceFileName As String
		Dim strTargetPath As String

		Dim strMessage As String

		Try
			htFilesToOverwrite = New Hashtable
			htFilesToOverwrite.Clear()

			If objAnalysisResults.FolderExistsWithRetry(TargetFolderPath) Then
				' The target folder already exists

				' Examine the files in the results folder to see if any of the files already exist in the transfer folder
				' If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)

				objSourceFolderInfo = New DirectoryInfo(SourceFolderPath)
				For Each objSourceFile In objSourceFolderInfo.GetFiles()
					If File.Exists(Path.Combine(TargetFolderPath, objSourceFile.Name)) Then
						objTargetFile = New FileInfo(Path.Combine(TargetFolderPath, objSourceFile.Name))

						If objSourceFile.LastWriteTimeUtc > objTargetFile.LastWriteTimeUtc Then
							strMessage = "File in transfer folder on server will be overwritten by newer file in results folder: " & objSourceFile.Name & "; new file date (UTC): " & objSourceFile.LastWriteTimeUtc.ToString() & "; old file date (UTC): " & objTargetFile.LastWriteTimeUtc.ToString()

							If objSourceFile.Name <> clsAnalysisJob.JobParametersFilename(m_JobNum) Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
							End If


							htFilesToOverwrite.Add(objSourceFile.Name.ToLower, 1)
						End If
					End If
				Next
			Else
				' Need to create the target folder
				Try
					objAnalysisResults.CreateFolderWithRetry(TargetFolderPath)
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating results folder in transfer directory, " & Path.GetPathRoot(TargetFolderPath) & ": " & ex.Message)
					m_message = clsGlobal.AppendToComment(m_message, "Error creating results folder in transfer directory, " & Path.GetPathRoot(TargetFolderPath))
					objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error comparing files in source folder to " & TargetFolderPath & ": " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error comparing files in source folder to transfer directory")
			objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Note: Entries in ResultFiles will have full file paths, not just file names
		ResultFiles = Directory.GetFiles(SourceFolderPath, "*")

		For Each FileToCopy As String In ResultFiles
			strSourceFileName = Path.GetFileName(FileToCopy)
			strTargetPath = Path.Combine(TargetFolderPath, strSourceFileName)

			Try
				If htFilesToOverwrite.Count > 0 AndAlso htFilesToOverwrite.Contains(strSourceFileName.ToLower) Then
					' Copy file and overwrite existing
					objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True, _
					 intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
				Else
					' Copy file only if it doesn't currently exist
					If Not File.Exists(strTargetPath) Then
						objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True, _
						 intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
					End If
				End If
			Catch ex As Exception
				' Continue copying files; we'll fail the results at the end of this function
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " CopyResultsFolderToServer: error copying " & Path.GetFileName(FileToCopy) & " to " & strTargetPath & ": " & ex.Message)
				blnErrorEncountered = True
				intFailedFileCount += 1
			End Try
		Next


		' Recursively call this function for each subfolder
		' If any of the subfolders have an error, we'll continue copying, but will set blnErrorEncountered to True
		Dim eResult As IJobParams.CloseOutType
		eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Dim diSourceFolder As DirectoryInfo
		Dim strTargetFolderPathCurrent As String
		diSourceFolder = New DirectoryInfo(SourceFolderPath)

		For Each objSubFolder As DirectoryInfo In diSourceFolder.GetDirectories()
			strTargetFolderPathCurrent = Path.Combine(TargetFolderPath, objSubFolder.Name)

			eResult = CopyResultsFolderRecursive(RootSourceFolderPath, objSubFolder.FullName, strTargetFolderPathCurrent, _
			 objAnalysisResults, blnErrorEncountered, intFailedFileCount, _
			 intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

		Next

		Return eResult

	End Function

	''' <summary>
	''' Determines the path to the remote transfer folder
	''' Creates the folder if it does not exist
	''' </summary>
	''' <returns>The full path to the remote transfer folder; an empty string if an error</returns>
	''' <remarks></remarks>
	Protected Function CreateRemoteTransferFolder(ByVal objAnalysisResults As clsAnalysisResults) As String

		Dim strRemoteTransferFolderPath As String
		Dim ResultsFolderName As String
		Dim TransferFolderPath As String

		ResultsFolderName = m_jobParams.GetParam("OutputFolderName")
		If String.IsNullOrEmpty(ResultsFolderName) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder name is not defined, job " & m_jobParams.GetParam("StepParameters", "Job"))
			m_message = "Results folder not found"
			' Without a source folder; there isn't much we can do
			Return String.Empty
		End If

		TransferFolderPath = m_jobParams.GetParam("transferFolderPath")

		' Verify transfer directory exists
		' First make sure TransferFolderPath is defined
		If String.IsNullOrEmpty(TransferFolderPath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Transfer folder path not defined; job param 'transferFolderPath' is empty")
			m_message = clsGlobal.AppendToComment(m_message, "Transfer folder path not defined")
			Return String.Empty
		End If

		' Now verify transfer directory exists
		Try
			objAnalysisResults.FolderExistsWithRetry(TransferFolderPath)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error verifying transfer directory, " & Path.GetPathRoot(TransferFolderPath) & ": " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error verifying transfer directory, " & Path.GetPathRoot(TransferFolderPath))
			Return String.Empty
		End Try

		'Determine if dataset folder in transfer directory already exists; make directory if it doesn't exist
		' First make sure "DatasetNum" is defined
		If String.IsNullOrEmpty(m_Dataset) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Dataset name is undefined, job " & m_jobParams.GetParam("StepParameters", "Job"))
			m_message = "Dataset name is undefined"
			Return String.Empty
		End If

		If m_Dataset.ToLower() = "Aggregation".ToLower() Then
			' Do not append "Aggregation" to the path since this is a generic dataset name applied to jobs that use Data Packages
			strRemoteTransferFolderPath = String.Copy(TransferFolderPath)
		Else
			' Append the dataset name to the transfer folder path
			strRemoteTransferFolderPath = Path.Combine(TransferFolderPath, m_Dataset)
		End If

		' Create the target folder if it doesn't exist
		Try
			objAnalysisResults.CreateFolderWithRetry(strRemoteTransferFolderPath, MaxRetryCount:=5, RetryHoldoffSeconds:=20, blnIncreaseHoldoffOnEachRetry:=True)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating dataset folder in transfer directory, " & Path.GetPathRoot(strRemoteTransferFolderPath) & ": " & ex.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error creating dataset folder in transfer directory, " & Path.GetPathRoot(strRemoteTransferFolderPath))
			Return String.Empty
		End Try

		' Now append the output folder name to strRemoteTransferFolderPath
		strRemoteTransferFolderPath = Path.Combine(strRemoteTransferFolderPath, ResultsFolderName)

		Return strRemoteTransferFolderPath

	End Function

	''' <summary>
	''' Makes up to 3 attempts to delete specified file
	''' </summary>
	''' <param name="FileNamePath">Full path to file for deletion</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Raises exception if error occurs</remarks>
	Public Function DeleteFileWithRetries(ByVal FileNamePath As String) As Boolean
		Return DeleteFileWithRetries(FileNamePath, m_DebugLevel, 3)
	End Function

	''' <summary>
	''' Makes up to 3 attempts to delete specified file
	''' </summary>
	''' <param name="FileNamePath">Full path to file for deletion</param>
	''' <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Raises exception if error occurs</remarks>
	Public Shared Function DeleteFileWithRetries(ByVal FileNamePath As String, ByVal intDebugLevel As Integer) As Boolean
		Return DeleteFileWithRetries(FileNamePath, intDebugLevel, 3)
	End Function

	''' <summary>
	''' Makes multiple tries to delete specified file
	''' </summary>
	''' <param name="FileNamePath">Full path to file for deletion</param>
	''' <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
	''' <param name="MaxRetryCount">Maximum number of deletion attempts</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Raises exception if error occurs</remarks>
	Public Shared Function DeleteFileWithRetries(ByVal FileNamePath As String, ByVal intDebugLevel As Integer, ByVal MaxRetryCount As Integer) As Boolean

		Dim RetryCount As Integer = 0
		Dim ErrType As AMFileNotDeletedAfterRetryException.RetryExceptionType

		If intDebugLevel > 4 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method")
		End If

		'Verify specified file exists
		If Not File.Exists(FileNamePath) Then
			'Throw an exception
			Throw New AMFileNotFoundException(FileNamePath, "Specified file not found")
		End If

		While RetryCount < MaxRetryCount
			Try
				File.Delete(FileNamePath)
				If intDebugLevel > 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit")
				End If
				Return True

			Catch Err1 As UnauthorizedAccessException
				'File may be read-only. Clear read-only flag and try again
				If intDebugLevel > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR1: " & Err1.Message)
					If Not Err1.InnerException Is Nothing Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err1.InnerException.Message)
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " may be read-only, attribute reset attempt #" & _
					 RetryCount.ToString)
				End If
				File.SetAttributes(FileNamePath, File.GetAttributes(FileNamePath) And (Not FileAttributes.ReadOnly))
				ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception
				RetryCount += 1

			Catch Err2 As IOException
				'If problem is locked file, attempt to fix lock and retry
				If intDebugLevel > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR2: " & Err2.Message)
					If Not Err2.InnerException Is Nothing Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err2.InnerException.Message)
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Error deleting file " & FileNamePath & ", attempt #" & RetryCount.ToString)
				End If
				ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception

				'Delay 2 seconds
				Thread.Sleep(2000)

				'Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd 
				PRISM.Processes.clsProgRunner.GarbageCollectNow()
				RetryCount += 1

			Catch Err3 As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting file, exception ERR3 " & FileNamePath & Err3.Message)
				Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
			End Try
		End While

		'If we got to here, then we've exceeded the max retry limit
		Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")

	End Function

	Protected Function DeleteRawDataFiles() As IJobParams.CloseOutType
		Dim RawDataType As String
		RawDataType = m_jobParams.GetParam("RawDataType")

		Return DeleteRawDataFiles(RawDataType)
	End Function

	Protected Function DeleteRawDataFiles(ByVal RawDataType As String) As IJobParams.CloseOutType
		Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants
		eRawDataType = clsAnalysisResources.GetRawDataType(RawDataType)

		Return DeleteRawDataFiles(eRawDataType)
	End Function

	Protected Function DeleteRawDataFiles(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As IJobParams.CloseOutType

		'Deletes the raw data files/folders from the working directory
		Dim IsFile As Boolean
		Dim IsNetworkDir As Boolean = False
		Dim FileOrFolderName As String = String.Empty

		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
				IsFile = True

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_WIFF_EXTENSION)
				IsFile = True

			Case clsAnalysisResources.eRawDataTypeConstants.UIMF
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_UIMF_EXTENSION)
				IsFile = True

			Case clsAnalysisResources.eRawDataTypeConstants.mzXML
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
				IsFile = True

			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)
				IsFile = True

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders

				Dim NewSourceFolder As String = clsAnalysisResources.ResolveSerStoragePath(m_WorkDir)
				'Check for "0.ser" folder
				If String.IsNullOrEmpty(NewSourceFolder) Then
					FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
					IsNetworkDir = False
				Else
					IsNetworkDir = True
				End If

				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder
				' Bruker_FT folders are actually .D folders
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot
				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''

				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging

				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''

				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
				IsFile = False

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf

				' BrukerTOFBaf folders are actually .D folders
				FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
				IsFile = False

			Case Else
				'Should never get this value
				m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " & eRawDataType.ToString()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select


		If IsFile Then
			'Data is a file, so use file deletion tools
			Try
				' DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
				' Thus, need to wrap it with an Exception handler

				If DeleteFileWithRetries(FileOrFolderName) Then
					Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				Else
					m_message = "Error deleting raw data file " & FileOrFolderName
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			Catch ex As Exception
				m_message = "Exception deleting raw data file " & FileOrFolderName & ": " & _
				ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		ElseIf IsNetworkDir Then
			'The files were on the network and do not need to be deleted

		Else
			'Use folder deletion tools
			Try
				Directory.Delete(FileOrFolderName, True)
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Catch ex As Exception
				m_message = "Exception deleting raw data folder " & FileOrFolderName & ": " & _
				 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Sub DeleteTemporaryfile(ByVal strFilePath As String)

		Try
			If File.Exists(strFilePath) Then
				File.Delete(strFilePath)
			End If
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception deleting temporary file " & strFilePath, ex)
		End Try

	End Sub

	''' <summary>
	''' Determine the path to the correct version of the step tool
	''' </summary>
	''' <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
	''' <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
	''' <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
	''' <returns>The path to the program, or an empty string if there is a problem</returns>
	''' <remarks></remarks>
	Protected Function DetermineProgramLocation(ByVal strStepToolName As String, _
	  ByVal strProgLocManagerParamName As String, _
	  ByVal strExeName As String) As String

		' Check whether the settings file specifies that a specific version of the step tool be used
		Dim strStepToolVersion As String = m_jobParams.GetParam(strStepToolName & "_Version")

		Return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strStepToolVersion)

	End Function

	''' <summary>
	''' Determine the path to the correct version of the step tool
	''' </summary>
	''' <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
	''' <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
	''' <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
	''' <param name="strStepToolVersion">Specific step tool version to use (will be the name of a subfolder located below the primary ProgLoc location)</param>
	''' <returns>The path to the program, or an empty string if there is a problem</returns>
	''' <remarks></remarks>
	Protected Function DetermineProgramLocation(ByVal strStepToolName As String, _
	   ByVal strProgLocManagerParamName As String, _
	   ByVal strExeName As String, _
	   ByVal strStepToolVersion As String) As String

		' Lookup the path to the folder that contains the Step tool
		Dim progLoc As String = m_mgrParams.GetParam(strProgLocManagerParamName)

		If String.IsNullOrWhiteSpace(progLoc) Then
			m_message = "Manager parameter " & strProgLocManagerParamName & " is not defined in the Manager Control DB"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return String.Empty
		End If

		' Check whether the settings file specifies that a specific version of the step tool be used
		If Not String.IsNullOrWhiteSpace(strStepToolVersion) Then

			' Specific version is defined; verify that the folder exists
			progLoc = Path.Combine(progLoc, strStepToolVersion)

			If Not Directory.Exists(progLoc) Then
				m_message = "Version-specific folder not found for " & strStepToolName
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
				Return String.Empty
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Using specific version of " & strStepToolName & ": " & progLoc)
			End If
		End If

		' Define the path to the .Exe, then verify that it exists
		progLoc = Path.Combine(progLoc, strExeName)

		If Not File.Exists(progLoc) Then
			m_message = "Cannot find " & strStepToolName & " program file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
			Return String.Empty
		End If

		Return progLoc

	End Function

	''' <summary>
	''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function GetCurrentMgrSettingsFromDB() As Boolean
		Return GetCurrentMgrSettingsFromDB(0)
	End Function

	''' <summary>
	''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
	''' </summary>
	''' <param name="intUpdateIntervalSeconds">The minimum number of seconds between updates; if fewer than intUpdateIntervalSeconds seconds have elapsed since the last call to this function, then no update will occur</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function GetCurrentMgrSettingsFromDB(ByVal intUpdateIntervalSeconds As Integer) As Boolean
		Return GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds, m_mgrParams, m_DebugLevel)
	End Function

	''' <summary>
	''' Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
	''' </summary>
	''' <param name="DebugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
	''' <returns>True for success; False for error</returns>
	''' <remarks></remarks>
	Public Shared Function GetCurrentMgrSettingsFromDB(ByVal intUpdateIntervalSeconds As Integer, _
	   ByRef objMgrParams As IMgrParams, _
	   ByRef DebugLevel As Short) As Boolean

		Dim MyConnection As SqlClient.SqlConnection
		Dim MyCmd As New SqlClient.SqlCommand
		Dim drSqlReader As SqlClient.SqlDataReader
		Dim ConnectionString As String

		Dim strParamName As String
		Dim strParamValue As String
		Dim intValueCountRead As Integer = 0

		Dim intNewDebugLevel As Short

		Static dtLastUpdateTime As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(1, 0, 0))

		Try

			If intUpdateIntervalSeconds > 0 AndAlso DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds < intUpdateIntervalSeconds Then
				Return True
			End If
			dtLastUpdateTime = DateTime.UtcNow

			If DebugLevel >= 5 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings from the Manager Control DB")
			End If

			ConnectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr")
			MyConnection = New SqlClient.SqlConnection(ConnectionString)
			MyConnection.Open()

			'Set up the command object prior to SP execution
			With MyCmd
				.CommandType = CommandType.Text
				.CommandText = "SELECT ParameterName, ParameterValue FROM V_MgrParams " & _
				   "WHERE ManagerName = '" & objMgrParams.GetParam("MgrName") & "' AND " & _
				  " ParameterName IN ('debuglevel')"

				.Connection = MyConnection
			End With

			'Execute the SP
			drSqlReader = MyCmd.ExecuteReader(CommandBehavior.CloseConnection)

			While drSqlReader.Read
				strParamName = drSqlReader.GetString(0)
				strParamValue = drSqlReader.GetString(1)

				If Not strParamName Is Nothing And Not strParamValue Is Nothing Then
					Select Case strParamName
						Case "debuglevel"
							intNewDebugLevel = Short.Parse(strParamValue)

							If DebugLevel > 0 AndAlso intNewDebugLevel <> DebugLevel Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Debug level changed from " & DebugLevel.ToString & " to " & intNewDebugLevel.ToString)
								DebugLevel = intNewDebugLevel
							End If
							intValueCountRead += 1
						Case Else
							' Unknown parameter
					End Select
				End If
			End While

			drSqlReader.Close()

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception getting current manager settings from the manager control DB: " & ex.Message)
		End Try

		If intValueCountRead > 0 Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Deterime the path to java.exe
	''' </summary>
	''' <returns>The path to the java.exe, or an empty string if the manager parameter is not defined or if java.exe does not exist</returns>
	''' <remarks></remarks>
	Protected Function GetJavaProgLoc() As String

		' JavaLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
		Dim javaProgLoc As String = m_mgrParams.GetParam("JavaLoc")

		If String.IsNullOrEmpty(javaProgLoc) Then
			m_message = "Parameter 'JavaLoc' not defined for this manager"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return String.Empty
		End If

		If Not File.Exists(javaProgLoc) Then
			m_message = "Cannot find Java: " & javaProgLoc
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return String.Empty
		End If

		Return javaProgLoc

	End Function

	''' <summary>
	''' Returns the full path to the program to use for converting a dataset to a .mzXML file
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function GetMSXmlGeneratorAppPath() As String

		Dim strMSXmlGeneratorExe As String = GetMSXmlGeneratorExeName()
		Dim strMSXmlGeneratorAppPath As String

		strMSXmlGeneratorAppPath = String.Empty
		If strMSXmlGeneratorExe.ToLower().Contains("readw") Then
			' ReadW
			' Note that msXmlGenerator will likely be ReAdW.exe
			strMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdW", "ReAdWProgLoc", strMSXmlGeneratorExe)

		ElseIf strMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
			' MSConvert
			Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")			' MSConvert.exe is stored in the ProteoWizard folder
			strMSXmlGeneratorAppPath = Path.Combine(ProteoWizardDir, strMSXmlGeneratorExe)

		Else
			m_message = "Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
		End If

		Return strMSXmlGeneratorAppPath

	End Function

	''' <summary>
	''' Returns the name of the .Exe to use to convert a dataset to a .mzXML file
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function GetMSXmlGeneratorExeName() As String
		' Determine the path to the XML Generator
		Dim strMSXmlGeneratorExe As String = m_jobParams.GetParam("MSXMLGenerator")			' ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)

		If String.IsNullOrEmpty(strMSXmlGeneratorExe) Then
			' Assume we're using MSConvert
			strMSXmlGeneratorExe = "MSConvert.exe"
		End If

		Return strMSXmlGeneratorExe
	End Function

	''' <summary>
	''' Decompresses the specified gzipped file
	''' Output folder is m_WorkDir
	''' </summary>
	''' <param name="GZipFilePath">File to decompress</param>
	''' <returns></returns>
	Public Function GUnzipFile(ByVal GZipFilePath As String) As Boolean
		Return GUnzipFile(GZipFilePath, m_WorkDir)
	End Function

	''' <summary>
	''' Decompresses the specified gzipped file
	''' </summary>
	''' <param name="GZipFilePath">File to unzip</param>
	''' <param name="TargetDirectory">Target directory for the extracted files</param>
	''' <returns></returns>
	Public Function GUnzipFile(ByVal GZipFilePath As String, ByVal TargetDirectory As String) As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel
		Return m_IonicZipTools.GUnzipFile(GZipFilePath, TargetDirectory)
	End Function

	''' <summary>
	''' Gzips SourceFilePath, creating a new file in the same folder, but with extension .gz appended
	''' </summary>
	''' <param name="SourceFilePath">Full path to the file to be zipped</param>
	''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
	''' <returns>True if success; false if an error</returns>
	Public Function GZipFile(ByVal SourceFilePath As String, ByVal DeleteSourceAfterZip As Boolean) As Boolean
		Dim blnSuccess As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel

		blnSuccess = m_IonicZipTools.GZipFile(SourceFilePath, DeleteSourceAfterZip)

		If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
			m_NeedToAbortProcessing = True
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Gzips SourceFilePath, creating a new file in TargetDirectoryPath; the file extension will be the original extension plus .gz
	''' </summary>
	''' <param name="SourceFilePath">Full path to the file to be zipped</param>
	''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
	''' <returns>True if success; false if an error</returns>
	Public Function GZipFile(ByVal SourceFilePath As String, ByVal TargetDirectoryPath As String, ByVal DeleteSourceAfterZip As Boolean) As Boolean

		Dim blnSuccess As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel

		blnSuccess = m_IonicZipTools.GZipFile(SourceFilePath, TargetDirectoryPath, DeleteSourceAfterZip)

		If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
			m_NeedToAbortProcessing = True
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Lookups up dataset information the data package associated with this analysis job
	''' </summary>
	''' <param name="dctDataPackageJobs"></param>
	''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
	''' <remarks></remarks>
	Protected Function LoadDataPackageJobInfo(ByRef dctDataPackageJobs As Dictionary(Of Integer, clsAnalysisResources.udtDataPackageJobInfoType)) As Boolean

		Dim ConnectionString As String = m_mgrParams.GetParam("brokerconnectionstring")
		Dim DataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

		If DataPackageID < 0 Then
			Return False
		Else
			Return clsAnalysisResources.LoadDataPackageJobInfo(ConnectionString, DataPackageID, dctDataPackageJobs)
		End If

	End Function

	''' <summary>
	''' Loads the job settings file
	''' </summary>
	''' <returns>TRUE for success, FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function LoadSettingsFile() As Boolean
		Dim fileName As String = m_jobParams.GetParam("settingsFileName")
		If fileName <> "na" Then
			Dim filePath As String = Path.Combine(m_WorkDir, fileName)
			If File.Exists(filePath) Then			 'XML tool Loadsettings returns True even if file is not found, so separate check reqd
				Return m_settingsFileParams.LoadSettings(filePath)
			Else
				Return False			'Settings file wasn't found
			End If
		Else
			Return True		  'Settings file wasn't required
		End If

	End Function

	''' <summary>
	''' Creates a results folder after analysis complete
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Function MakeResultsFolder() As IJobParams.CloseOutType

		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)

		'Makes results folder and moves files into it
		Dim ResFolderNamePath As String

		'Log status (both locally and in the DB)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MachName & ": Creating results folder, Job " & m_JobNum)
		ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)

		'make the results folder
		Try
			Directory.CreateDirectory(ResFolderNamePath)
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error making results folder, job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(Err))
			m_message = clsGlobal.AppendToComment(m_message, "Error making results folder")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Moves result files to the local results folder after tool has completed
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Function MoveResultFiles() As IJobParams.CloseOutType
		Const REJECT_LOGGING_THRESHOLD As Integer = 10
		Const ACCEPT_LOGGING_THRESHOLD As Integer = 50
		Const LOG_LEVEL_REPORT_ACCEPT_OR_REJECT As Integer = 5

		'Makes results folder and moves files into it
		Dim ResFolderNamePath As String = String.Empty
		Dim strTargetFilePath As String = String.Empty

		Dim Files() As String
		Dim TmpFile As String = String.Empty
		Dim TmpFileNameLcase As String
		Dim OkToMove As Boolean
		Dim strLogMessage As String

		Dim strExtension As String
		Dim dctRejectStats As Dictionary(Of String, Integer)
		Dim dctAcceptStats As Dictionary(Of String, Integer)
		Dim intCount As Integer

		Dim objExtension As Dictionary(Of String, Integer).Enumerator

		Dim blnErrorEncountered As Boolean = False

		'Move files into results folder
		Try
			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)
			ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)
			dctRejectStats = New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)
			dctAcceptStats = New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

			'Log status
			If m_DebugLevel >= 2 Then
				strLogMessage = "Move Result Files to " & ResFolderNamePath
				If m_DebugLevel >= 3 Then
					strLogMessage &= "; ResultFilesToSkip contains " & m_jobParams.ResultFilesToSkip.Count.ToString & " entries" & _
					   "; ResultFileExtensionsToSkip contains " & m_jobParams.ResultFileExtensionsToSkip.Count.ToString & " entries" & _
					   "; ResultFilesToKeep contains " & m_jobParams.ResultFilesToKeep.Count.ToString & " entries"
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strLogMessage)
			End If


			' Obtain a list of all files in the working directory
			Files = Directory.GetFiles(m_WorkDir, "*")

			' Check each file against m_jobParams.m_ResultFileExtensionsToSkip and m_jobParams.m_ResultFilesToKeep
			For Each TmpFile In Files
				If TmpFile = "IDPicker_AnalysisSummary.txt" Then
					Console.WriteLine("Check this file")
				End If

				OkToMove = True
				TmpFileNameLcase = Path.GetFileName(TmpFile).ToLower()

				' Check to see if the filename is defined in ResultFilesToSkip
				' Note that entries in ResultFilesToSkip are not case sensitive since they were instantiated using SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)
				If m_jobParams.ResultFilesToSkip.Contains(TmpFileNameLcase) Then
					' File found in the ResultFilesToSkip list; do not move it
					OkToMove = False
				End If

				If OkToMove Then
					' Check to see if the file ends with an entry specified in m_ResultFileExtensionsToSkip
					' Note that entries in m_ResultFileExtensionsToSkip can be extensions, or can even be partial file names, e.g. _peaks.txt
					For Each ext As String In m_jobParams.ResultFileExtensionsToSkip
						If TmpFileNameLcase.EndsWith(ext.ToLower()) Then
							OkToMove = False
							Exit For
						End If
					Next
				End If

				If Not OkToMove Then
					' Check to see if the file is a result file that got captured as a non result file
					If m_jobParams.ResultFilesToKeep.Contains(TmpFileNameLcase) Then
						OkToMove = True
					End If
				End If

				' Look for invalid characters in the filename
				'	(Required because extract_msn.exe sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in working directory) 
				' Note: now evaluating each character in the filename
				If OkToMove Then
					Dim intAscValue As Integer
					For Each chChar As Char In Path.GetFileName(TmpFile).ToCharArray
						intAscValue = Convert.ToInt32(chChar)
						If intAscValue <= 31 Or intAscValue >= 128 Then
							' Invalid character found
							OkToMove = False
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
							Exit For
						End If
					Next
				Else
					If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
						strExtension = Path.GetExtension(TmpFile)
						If dctRejectStats.TryGetValue(strExtension, intCount) Then
							dctRejectStats(strExtension) = intCount + 1
						Else
							dctRejectStats.Add(strExtension, 1)
						End If

						' Only log the first 10 times files of a given extension are rejected
						'  However, if a file was rejected due to invalid characters in the name, then we don't track that rejection with dctRejectStats
						If dctRejectStats(strExtension) <= REJECT_LOGGING_THRESHOLD Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected file:  " & TmpFile)
						End If
					End If
				End If

				'If valid file name, then move file to results folder
				If OkToMove Then
					If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
						strExtension = Path.GetExtension(TmpFile).ToLower
						If dctAcceptStats.TryGetValue(strExtension, intCount) Then
							dctAcceptStats(strExtension) = intCount + 1
						Else
							dctAcceptStats.Add(strExtension, 1)
						End If

						' Only log the first 50 times files of a given extension are accepted
						If dctAcceptStats(strExtension) <= ACCEPT_LOGGING_THRESHOLD Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
						End If
					End If

					Try
						strTargetFilePath = Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile))
						File.Move(TmpFile, strTargetFilePath)
					Catch ex As Exception
						Try
							' Move failed
							' Attempt to copy the file instead of moving the file
							File.Copy(TmpFile, strTargetFilePath, True)

							' If we get here, then the copy succeeded; the original file (in the work folder) will get deleted when the work folder is "cleaned" after the job finishes

						Catch ex2 As Exception
							' Copy also failed
							' Continue moving files; we'll fail the results at the end of this function
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " MoveResultFiles: error moving/copying file: " & TmpFile & ex.Message)
							blnErrorEncountered = True
						End Try
					End Try
				End If
			Next

			If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
				' Look for any extensions in dctAcceptStats that had over 50 accepted files
				objExtension = dctAcceptStats.GetEnumerator
				Do While objExtension.MoveNext
					If objExtension.Current.Value > ACCEPT_LOGGING_THRESHOLD Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted a total of " & objExtension.Current.Value & " files with extension " & objExtension.Current.Key)
					End If
				Loop

				' Look for any extensions in dctRejectStats that had over 10 rejected files
				objExtension = dctRejectStats.GetEnumerator
				Do While objExtension.MoveNext
					If objExtension.Current.Value > REJECT_LOGGING_THRESHOLD Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected a total of " & objExtension.Current.Value & " files with extension " & objExtension.Current.Key)
					End If
				Loop
			End If

		Catch Err As Exception
			If m_DebugLevel > 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results folder")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Tmpfile = " & TmpFile)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder name = " & Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile)))
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error moving results files, job " & m_JobNum & Err.Message)
			m_message = clsGlobal.AppendToComment(m_message, "Error moving results files")

			blnErrorEncountered = True
		End Try

		Try
			'Make the summary file
			OutputSummary(ResFolderNamePath)
		Catch ex As Exception
			' Ignore errors here
		End Try

		If blnErrorEncountered Then
			' Try to save whatever files were moved into the results folder
			Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
			objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

	End Function

	Public Shared Function NotifyMissingParameter(ByVal oJobParams As IJobParams, ByVal strParameterName As String) As String

		Dim strSettingsFile As String = oJobParams.GetJobParameter("SettingsFileName", "?UnknownSettingsFile?")
		Dim strToolName As String = oJobParams.GetJobParameter("ToolName", "?UnknownToolName?")

		Return "Settings file " & strSettingsFile & " for tool " & strToolName & " does not have parameter " & strParameterName & " defined"

	End Function

	''' <summary>
	''' Adds manager assembly data to job summary file
	''' </summary>
	''' <param name="OutputPath">Path to summary file</param>
	''' <remarks></remarks>
	Protected Sub OutputSummary(ByVal OutputPath As String)

		'Saves the summary file in the results folder
		Dim objAssemblyTools As clsAssemblyTools = New clsAssemblyTools

		objAssemblyTools.GetComponentFileVersionInfo(m_SummaryFile)

		m_SummaryFile.SaveSummaryFile(Path.Combine(OutputPath, m_jobParams.GetParam("StepTool") & "_AnalysisSummary.txt"))

	End Sub

	''' <summary>
	''' Adds double quotes around a path if it contains a space
	''' </summary>
	''' <param name="strPath"></param>
	''' <returns>The path (updated if necessary)</returns>
	''' <remarks></remarks>
	Public Shared Function PossiblyQuotePath(strPath As String) As String
		If String.IsNullOrEmpty(strPath) Then
			Return String.Empty
		Else

			If strPath.Contains(" ") Then
				If Not strPath.StartsWith("""") Then
					strPath = """" & strPath
				End If

				If Not strPath.EndsWith("""") Then
					strPath &= """"
				End If
			End If

			Return strPath

		End If
	End Function

	''' <summary>
	''' Determines the space usage of data files in the cache folder
	''' If usage is over intSpaceUsageThresholdGB, then deletes the oldest files until usage falls below intSpaceUsageThresholdGB
	''' </summary>
	''' <param name="strCacheFolderPath">Path to the file cache</param>
	''' <param name="intSpaceUsageThresholdGB">Maximum space usage (cannot be less than 1)</param>
	''' <remarks></remarks>
	Protected Sub PurgeOldServerCacheFiles(ByVal strCacheFolderPath As String, ByVal intSpaceUsageThresholdGB As Integer)

		Const PURGE_INTERVAL_MINUTES As Integer = 10
		Static dtLastCheck As DateTime = DateTime.UtcNow.AddMinutes(-PURGE_INTERVAL_MINUTES * 2)

		Dim diCacheFolder As DirectoryInfo
		Dim lstDataFiles As SortedList(Of DateTime, FileInfo) = New SortedList(Of DateTime, FileInfo)

		Dim dblTotalSizeMB As Double = 0

		Dim dblSizeDeletedMB As Double = 0
		Dim intFileDeleteCount As Integer = 0
		Dim intFileDeleteErrorCount As Integer = 0

		Dim dctErrorSummary As Dictionary(Of String, Integer) = New Dictionary(Of String, Integer)

		If intSpaceUsageThresholdGB < 1 Then intSpaceUsageThresholdGB = 1

		Try
			If DateTime.UtcNow.Subtract(dtLastCheck).TotalMinutes < PURGE_INTERVAL_MINUTES Then
				Exit Sub
			End If
			dtLastCheck = DateTime.UtcNow

			diCacheFolder = New DirectoryInfo(strCacheFolderPath)

			If diCacheFolder.Exists Then
				' Make a list of all of the files in diCacheFolder

				For Each fiItem As FileInfo In diCacheFolder.GetFiles("*.hashcheck", SearchOption.AllDirectories)

					If fiItem.FullName.ToLower().EndsWith(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.ToLower()) Then
						Dim strDataFilePath As String
						strDataFilePath = fiItem.FullName.Substring(0, fiItem.FullName.Length - clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.Length)

						Dim fiDataFile As FileInfo = New FileInfo(strDataFilePath)

						If fiDataFile.Exists Then
							lstDataFiles.Add(fiDataFile.LastWriteTimeUtc, fiDataFile)

							dblTotalSizeMB += fiDataFile.Length / 1024.0 / 1024.0
						End If
					End If
				Next
			End If

			If dblTotalSizeMB / 1024.0 > intSpaceUsageThresholdGB Then
				' Purge files until the space usage falls below the threshold

				For Each kvItem As KeyValuePair(Of DateTime, FileInfo) In lstDataFiles

					Try
						Dim strHashcheckPath As String
						Dim dblFileSizeMB As Double = kvItem.Value.Length / 1024.0 / 1024.0

						strHashcheckPath = kvItem.Value.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX
						dblTotalSizeMB -= dblFileSizeMB

						kvItem.Value.Delete()
						File.Delete(strHashcheckPath)

						dblSizeDeletedMB += dblFileSizeMB
						intFileDeleteCount += 1

					Catch ex As Exception
						' Keep track of the number of times we have an exception
						intFileDeleteErrorCount += 1

						Dim intOccurrences As Integer = 1
						Dim strExceptionName As String = ex.GetType.ToString()
						If dctErrorSummary.TryGetValue(strExceptionName, intOccurrences) Then
							dctErrorSummary(strExceptionName) = intOccurrences + 1
						Else
							dctErrorSummary.Add(strExceptionName, 1)
						End If

					End Try

					If dblTotalSizeMB / 1024.0 < intSpaceUsageThresholdGB Then
						Exit For
					End If
				Next

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Deleted " & intFileDeleteCount & " file(s) from " & strCacheFolderPath & ", recovering " & dblSizeDeletedMB.ToString("0.0") & " MB in disk space")

				If intFileDeleteErrorCount > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to delete " & intFileDeleteErrorCount & " file(s) from " & strCacheFolderPath)
					For Each kvItem As KeyValuePair(Of String, Integer) In dctErrorSummary
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  " & kvItem.Key & ": " & kvItem.Value)
					Next
				End If

			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in PurgeOldServerCacheFiles", ex)
		End Try
	End Sub

	''' <summary>
	''' Updates the dataset name to the final folder name in the transferFolderPath job parameter
	''' Updates the transfer folder path to remove the final folder
	''' </summary>
	''' <remarks></remarks>
	Protected Sub RedefineAggregationJobDatasetAndTransferFolder()

		Dim strTransferFolderPath As String = m_jobParams.GetParam("transferFolderPath")
		Dim diTransferFolder As New DirectoryInfo(strTransferFolderPath)

		m_Dataset = diTransferFolder.Name
		strTransferFolderPath = diTransferFolder.Parent.FullName
		m_jobParams.SetParam("JobParameters", "transferFolderPath", strTransferFolderPath)

	End Sub

	''' <summary>
	''' Extracts the contents of the Version= line in a Tool Version Info file
	''' </summary>
	''' <param name="strDLLFilePath"></param>
	''' <param name="strVersionInfoFilePath"></param>
	''' <param name="strVersion"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ReadVersionInfoFile(ByVal strDLLFilePath As String, ByVal strVersionInfoFilePath As String, ByRef strVersion As String) As Boolean

		' Open strVersionInfoFilePath and read the Version= line
		Dim srInFile As StreamReader
		Dim strLineIn As String
		Dim strKey As String
		Dim strValue As String
		Dim intEqualsLoc As Integer

		strVersion = String.Empty
		Dim blnSuccess As Boolean = False

		Try

			If Not File.Exists(strVersionInfoFilePath) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Version Info File not found: " & strVersionInfoFilePath)
				Return False
			End If

			srInFile = New StreamReader(New FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

			Do While srInFile.Peek > -1
				strLineIn = srInFile.ReadLine()

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					intEqualsLoc = strLineIn.IndexOf("="c)

					If intEqualsLoc > 0 Then
						strKey = strLineIn.Substring(0, intEqualsLoc)

						If intEqualsLoc < strLineIn.Length Then
							strValue = strLineIn.Substring(intEqualsLoc + 1)
						Else
							strValue = String.Empty
						End If

						Select Case strKey.ToLower()
							Case "filename"
							Case "path"
							Case "version"
								strVersion = String.Copy(strValue)
								If String.IsNullOrWhiteSpace(strVersion) Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Empty version line in Version Info file for " & Path.GetFileName(strDLLFilePath))
									blnSuccess = False
								Else
									blnSuccess = True
								End If
							Case "error"
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reported by DLLVersionInspector for " & Path.GetFileName(strDLLFilePath) & ": " & strValue)
								blnSuccess = False
							Case Else
								' Ignore the line
						End Select
					End If

				End If
			Loop

			srInFile.Close()

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading Version Info File for " & Path.GetFileName(strDLLFilePath), ex)
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Deletes files in specified directory that have been previously flagged as not wanted in results folder
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>List of files to delete is tracked via m_jobParams.ServerFilesToDelete; must store full file paths in ServerFilesToDelete</remarks>
	Public Function RemoveNonResultServerFiles() As Boolean

		Dim FileToDelete As String = ""

		Try
			'Log status
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Remove Files from the storage server; ServerFilesToDelete contains " & m_jobParams.ServerFilesToDelete.Count.ToString & " entries")
			End If

			For Each FileToDelete In m_jobParams.ServerFilesToDelete
				If m_DebugLevel >= 4 Then	 'Log file to be deleted
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting " & FileToDelete)
				End If

				If File.Exists(FileToDelete) Then
					'Verify file is not set to readonly, then delete it
					File.SetAttributes(FileToDelete, File.GetAttributes(FileToDelete) And (Not FileAttributes.ReadOnly))
					File.Delete(FileToDelete)
				End If
			Next
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsGlobal.RemoveNonResultServerFiles(), Error deleting file " & FileToDelete, ex)
			'Even if an exception occurred, return true since the results were already copied back to the server
			Return True
		End Try

		Return True

	End Function

	Protected Sub ResetTimestampForQueueWaitTimeLogging()
		m_LastLockQueueWaitTimeLog = DateTime.UtcNow
	End Sub

	''' <summary>
	''' Runs the analysis tool
	''' Major work is performed by overrides
	''' </summary>
	''' <returns>CloseoutType enum representing completion status</returns>
	''' <remarks></remarks>
	Public Overridable Function RunTool() As IJobParams.CloseOutType Implements IToolRunner.RunTool

		' Synchronize the stored Debug level with the value stored in the database
		GetCurrentMgrSettingsFromDB()

		'Make log entry
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_MachName & ": Starting analysis, job " & m_JobNum)

		'Start the job timer
		m_StartTime = DateTime.UtcNow

		'Remainder of method is supplied by subclasses

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Creates a Tool Version Info file
	''' </summary>
	''' <param name="strFolderPath"></param>
	''' <param name="strToolVersionInfo"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function SaveToolVersionInfoFile(ByVal strFolderPath As String, ByVal strToolVersionInfo As String) As Boolean
		Dim swToolVersionFile As StreamWriter
		Dim strToolVersionFilePath As String
		Dim strStepToolName As String

		Try
			strStepToolName = m_jobParams.GetParam("StepTool")
			If strStepToolName.ToLower().StartsWith("msgfplus") Then
				' For backwards compatibility, need to make sure the file does not start with "MSGFPlus" 
				strStepToolName = clsGlobal.ReplaceIgnoreCase(strStepToolName, "MSGFPlus", "MSGFDB")
			End If

			strToolVersionFilePath = Path.Combine(strFolderPath, "Tool_Version_Info_" & strStepToolName & ".txt")

			swToolVersionFile = New StreamWriter(New FileStream(strToolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

			swToolVersionFile.WriteLine("Date: " & DateTime.Now().ToString(DATE_TIME_FORMAT))
			swToolVersionFile.WriteLine("Dataset: " & m_Dataset)
			swToolVersionFile.WriteLine("Job: " & m_JobNum)
			swToolVersionFile.WriteLine("Step: " & m_jobParams.GetParam("StepParameters", "Step"))
			swToolVersionFile.WriteLine("Tool: " & m_jobParams.GetParam("StepTool"))
			swToolVersionFile.WriteLine("ToolVersionInfo:")

			swToolVersionFile.WriteLine(strToolVersionInfo.Replace("; ", ControlChars.NewLine))
			swToolVersionFile.Close()

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception saving tool version info: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Communicates with database to record the tool version(s) for the current step task
	''' </summary>
	''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
	''' <returns>True for success, False for failure</returns>
	''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
	Protected Function SetStepTaskToolVersion(ByVal strToolVersionInfo As String) As Boolean
		Return SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
	End Function

	''' <summary>
	''' Communicates with database to record the tool version(s) for the current step task
	''' </summary>
	''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
	''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
	''' <returns>True for success, False for failure</returns>
	''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
	Protected Function SetStepTaskToolVersion(ByVal strToolVersionInfo As String, _
	   ByVal ioToolFiles As List(Of FileInfo)) As Boolean

		Return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, True)
	End Function

	''' <summary>
	''' Communicates with database to record the tool version(s) for the current step task
	''' </summary>
	''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
	''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
	''' <param name="blnSaveToolVersionTextFile">if true, then creates a text file with the tool version information</param>
	''' <returns>True for success, False for failure</returns>
	''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
	Protected Function SetStepTaskToolVersion(ByVal strToolVersionInfo As String, _
	   ByVal ioToolFiles As List(Of FileInfo), _
	   ByVal blnSaveToolVersionTextFile As Boolean) As Boolean

		Dim strExeInfo As String = String.Empty
		Dim strToolVersionInfoCombined As String

		Dim Outcome As Boolean
		Dim ResCode As Integer

		If Not ioToolFiles Is Nothing Then

			For Each ioFileInfo As FileInfo In ioToolFiles
				Try
					If ioFileInfo.Exists Then
						strExeInfo = clsGlobal.AppendToComment(strExeInfo, ioFileInfo.Name & ": " & ioFileInfo.LastWriteTime.ToString(DATE_TIME_FORMAT))

						If m_DebugLevel >= 2 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "EXE Info: " & strExeInfo)
						End If

					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Tool file not found: " & ioFileInfo.FullName)
					End If

				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception looking up tool version file info: " & ex.Message)
				End Try
			Next
		End If

		' Append the .Exe info to strToolVersionInfo
		If String.IsNullOrEmpty(strExeInfo) Then
			strToolVersionInfoCombined = String.Copy(strToolVersionInfo)
		Else
			strToolVersionInfoCombined = clsGlobal.AppendToComment(strToolVersionInfo, strExeInfo)
		End If

		If blnSaveToolVersionTextFile Then
			SaveToolVersionInfoFile(m_WorkDir, strToolVersionInfoCombined)
		End If

		'Setup for execution of the stored procedure
		Dim MyCmd As New SqlClient.SqlCommand
		With MyCmd
			.CommandType = CommandType.StoredProcedure
			.CommandText = SP_NAME_SET_TASK_TOOL_VERSION

			.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
			.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

			.Parameters.Add(New SqlClient.SqlParameter("@job", SqlDbType.Int))
			.Parameters.Item("@job").Direction = ParameterDirection.Input
			.Parameters.Item("@job").Value = m_jobParams.GetJobParameter("StepParameters", "Job", 0)

			.Parameters.Add(New SqlClient.SqlParameter("@step", SqlDbType.Int))
			.Parameters.Item("@step").Direction = ParameterDirection.Input
			.Parameters.Item("@step").Value = m_jobParams.GetJobParameter("StepParameters", "Step", 0)

			.Parameters.Add(New SqlClient.SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900))
			.Parameters.Item("@ToolVersionInfo").Direction = ParameterDirection.Input
			.Parameters.Item("@ToolVersionInfo").Value = strToolVersionInfoCombined
		End With

		Dim objAnalysisTask As clsAnalysisJob
		Dim strBrokerConnStr As String = m_mgrParams.GetParam("brokerconnectionstring")

		objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

		'Execute the SP (retry the call up to 4 times)
		ResCode = objAnalysisTask.ExecuteSP(MyCmd, strBrokerConnStr, 4)

		If ResCode = 0 Then
			Outcome = True
		Else
			Dim Msg As String = "Error " & ResCode.ToString & " storing tool version for current processing step"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Outcome = False
		End If

		Return Outcome

	End Function

	''' <summary>
	''' Uses Reflection to determine the version info for an assembly already loaded in memory
	''' </summary>
	''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
	''' <param name="strAssemblyName">Assembly Name</param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
	Protected Function StoreToolVersionInfoForLoadedAssembly(ByRef strToolVersionInfo As String, ByVal strAssemblyName As String) As Boolean
		Return StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, strAssemblyName, blnIncludeRevision:=True)
	End Function

	''' <summary>
	''' Uses Reflection to determine the version info for an assembly already loaded in memory
	''' </summary>
	''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
	''' <param name="strAssemblyName">Assembly Name</param>
	''' <param name="blnIncludeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
	Protected Function StoreToolVersionInfoForLoadedAssembly(ByRef strToolVersionInfo As String, ByVal strAssemblyName As String, ByVal blnIncludeRevision As Boolean) As Boolean

		Try
			Dim oAssemblyName As Reflection.AssemblyName
			oAssemblyName = Reflection.Assembly.Load(strAssemblyName).GetName

			Dim strNameAndVersion As String
			If blnIncludeRevision Then
				strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
			Else
				strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.Major & "." & oAssemblyName.Version.Minor & "." & oAssemblyName.Version.Build
			End If

			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for " & strAssemblyName & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Determines the version info for a .NET DLL using reflection
	''' If reflection fails, then uses System.Diagnostics.FileVersionInfo
	''' </summary>
	''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
	''' <param name="strDLLFilePath">Path to the DLL</param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfoOneFile(ByRef strToolVersionInfo As String, ByVal strDLLFilePath As String) As Boolean

		Dim ioFileInfo As FileInfo
		Dim blnSuccess As Boolean

		Try
			ioFileInfo = New FileInfo(strDLLFilePath)

			If Not ioFileInfo.Exists Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "File not found by StoreToolVersionInfoOneFile: " & strDLLFilePath)
				Return False
			Else

				Dim oAssemblyName As Reflection.AssemblyName
				oAssemblyName = Reflection.Assembly.LoadFrom(ioFileInfo.FullName).GetName()

				Dim strNameAndVersion As String
				strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

				blnSuccess = True
			End If

		Catch ex As Exception
			' If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
			'  <startup useLegacyV2RuntimeActivationPolicy="true">
			'    <supportedRuntime version="v4.0" />
			'  </startup>
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for " & Path.GetFileName(strDLLFilePath) & ": " & ex.Message)
			blnSuccess = False
		End Try

		If Not blnSuccess Then
			blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strDLLFilePath)
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
	''' </summary>
	''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
	''' <param name="strDLLFilePath">Path to the DLL</param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfoViaSystemDiagnostics(ByRef strToolVersionInfo As String, ByVal strDLLFilePath As String) As Boolean
		Dim ioFileInfo As FileInfo
		Dim blnSuccess As Boolean

		Try
			ioFileInfo = New FileInfo(strDLLFilePath)

			If Not ioFileInfo.Exists Then
				m_message = "File not found by StoreToolVersionInfoViaSystemDiagnostics"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message & ": " & strDLLFilePath)
				Return False
			Else

				Dim oFileVersionInfo As FileVersionInfo
				oFileVersionInfo = FileVersionInfo.GetVersionInfo(strDLLFilePath)

				Dim strName As String
				Dim strVersion As String

				strName = oFileVersionInfo.FileDescription
				If String.IsNullOrEmpty(strName) Then
					strName = oFileVersionInfo.InternalName
				End If

				If String.IsNullOrEmpty(strName) Then
					strName = oFileVersionInfo.FileName
				End If

				If String.IsNullOrEmpty(strName) Then
					strName = ioFileInfo.Name
				End If

				strVersion = oFileVersionInfo.FileVersion
				If String.IsNullOrEmpty(strVersion) Then
					strVersion = oFileVersionInfo.ProductVersion
				End If

				If String.IsNullOrEmpty(strVersion) Then
					strVersion = "??"
				End If

				Dim strNameAndVersion As String
				strNameAndVersion = strName & ", Version=" & strVersion
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

				blnSuccess = True
			End If

		Catch ex As Exception
			m_message = "Exception determining File Version for " & Path.GetFileName(strDLLFilePath)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
	''' </summary>
	''' <param name="strToolVersionInfo"></param>
	''' <param name="strDLLFilePath"></param>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfoOneFile64Bit(ByRef strToolVersionInfo As String, ByVal strDLLFilePath As String) As Boolean

		Dim strNameAndVersion As String = String.Empty
		Dim strAppPath As String
		Dim strVersionInfoFilePath As String
		Dim strArgs As String

		Dim ioFileInfo As FileInfo

		Try
			strAppPath = Path.Combine(clsGlobal.GetAppFolderPath(), "DLLVersionInspector.exe")

			ioFileInfo = New FileInfo(strDLLFilePath)
			strNameAndVersion = Path.GetFileNameWithoutExtension(ioFileInfo.Name) & ", Version="

			If Not ioFileInfo.Exists Then
				m_message = "File not found by StoreToolVersionInfoOneFile64Bit"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strDLLFilePath)
				Return False
			ElseIf Not File.Exists(strAppPath) Then
				m_message = "DLLVersionInspector not found by StoreToolVersionInfoOneFile64Bit"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strAppPath)
				Return False
			Else
				' Call DLLVersionInspector.exe to determine the tool version

				strVersionInfoFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_VersionInfo.txt")

				Dim objProgRunner As clsRunDosProgram
				Dim blnSuccess As Boolean
				Dim strVersion As String = String.Empty

				objProgRunner = New clsRunDosProgram(clsGlobal.GetAppFolderPath())

				strArgs = ioFileInfo.FullName & " /O:" & PossiblyQuotePath(strVersionInfoFilePath)

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strAppPath & " " & strArgs)
				End If

				With objProgRunner
					.CacheStandardOutput = False
					.CreateNoWindow = True
					.EchoOutputToConsole = True
					.WriteConsoleOutputToFile = False

					.DebugLevel = 1
					.MonitorInterval = 250
				End With

				blnSuccess = objProgRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", False)

				If Not blnSuccess Then
					Return False
				End If

				Thread.Sleep(100)

				blnSuccess = ReadVersionInfoFile(strDLLFilePath, strVersionInfoFilePath, strVersion)

				' Delete the version info file
				Try
					Thread.Sleep(100)
					File.Delete(strVersionInfoFilePath)
				Catch ex As Exception
					' Ignore errors here
				End Try


				If Not blnSuccess OrElse String.IsNullOrWhiteSpace(strVersion) Then
					Return False
				Else
					strNameAndVersion = String.Copy(strVersion)
				End If

			End If

			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

			Return True

		Catch ex As Exception
			m_message = "Exception determining Version info for " & Path.GetFileName(strDLLFilePath)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, Path.GetFileNameWithoutExtension(strDLLFilePath))
		End Try

		Return False

	End Function

	''' <summary>
	''' Copies new/changed files from the source folder to the target folder
	''' </summary>
	''' <param name="sourceFolderPath"></param>
	''' <param name="targetFolderPath"></param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function SynchronizeFolders(ByVal sourceFolderPath As String, ByVal targetFolderPath As String) As Boolean
		Return SynchronizeFolders(sourceFolderPath, targetFolderPath, "*")
	End Function

	''' <summary>
	''' Copies new/changed files from the source folder to the target folder
	''' </summary>
	''' <param name="sourceFolderPath"></param>
	''' <param name="targetFolderPath"></param>
	''' <param name="fileNameFilterSpec">Filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>Will retry failed copies up to 3 times</remarks>
	Protected Function SynchronizeFolders(
	  ByVal sourceFolderPath As String,
	  ByVal targetFolderPath As String,
	  ByVal fileNameFilterSpec As String) As Boolean

		Dim lstFileNameFilterSpec = New List(Of String) From {fileNameFilterSpec}		
		Dim lstFileNameExclusionSpec = New List(Of String)
		Const maxRetryCount = 3

		Return SynchronizeFolders(sourceFolderPath, targetFolderPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount)
	End Function

	''' <summary>
	''' Copies new/changed files from the source folder to the target folder
	''' </summary>
	''' <param name="sourceFolderPath"></param>
	''' <param name="targetFolderPath"></param>
	''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>Will retry failed copies up to 3 times</remarks>
	Protected Function SynchronizeFolders(
	  ByVal sourceFolderPath As String,
	  ByVal targetFolderPath As String,
	  ByVal lstFileNameFilterSpec As List(Of String)) As Boolean

		Dim lstFileNameExclusionSpec = New List(Of String)
		Const maxRetryCount = 3

		Return SynchronizeFolders(sourceFolderPath, targetFolderPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount)
	End Function

	''' <summary>
	''' Copies new/changed files from the source folder to the target folder
	''' </summary>
	''' <param name="sourceFolderPath"></param>
	''' <param name="targetFolderPath"></param>
	''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
	''' <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>Will retry failed copies up to 3 times</remarks>
	Protected Function SynchronizeFolders(
	  ByVal sourceFolderPath As String,
	  ByVal targetFolderPath As String,
	  ByVal lstFileNameFilterSpec As List(Of String),
	  ByVal lstFileNameExclusionSpec As List(Of String)) As Boolean

		Const maxRetryCount = 3

		Return SynchronizeFolders(sourceFolderPath, targetFolderPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount)
	End Function

	''' <summary>
	''' Copies new/changed files from the source folder to the target folder
	''' </summary>
	''' <param name="sourceFolderPath"></param>
	''' <param name="targetFolderPath"></param>
	''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
	''' <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
	''' <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function SynchronizeFolders(
	  ByVal sourceFolderPath As String,
	  ByVal targetFolderPath As String,
	  ByVal lstFileNameFilterSpec As List(Of String),
	  ByVal lstFileNameExclusionSpec As List(Of String),
	  ByVal maxRetryCount As Integer) As Boolean

		Try
			Dim diSourceFolder = New DirectoryInfo(sourceFolderPath)
			Dim diTargetFolder = New DirectoryInfo(targetFolderPath)

			If Not diTargetFolder.Exists Then
				diTargetFolder.Create()
			End If

			If lstFileNameFilterSpec Is Nothing Then
				lstFileNameFilterSpec = New List(Of String)
			End If

			If lstFileNameFilterSpec.Count = 0 Then lstFileNameFilterSpec.Add("*")

			Dim lstFilesToCopy = New SortedSet(Of String)

			For Each filterSpec In lstFileNameFilterSpec
				If String.IsNullOrWhiteSpace(filterSpec) Then
					filterSpec = "*"
				End If

				For Each fiFile In diSourceFolder.GetFiles(filterSpec)
					If Not lstFilesToCopy.Contains(fiFile.Name) Then
						lstFilesToCopy.Add(fiFile.Name)
					End If
				Next
			Next

			If Not lstFileNameExclusionSpec Is Nothing AndAlso lstFileNameExclusionSpec.Count > 0 Then
				' Remove any files from lstFilesToCopy that would get matched by items in lstFileNameExclusionSpec

				For Each filterSpec In lstFileNameExclusionSpec
					If Not String.IsNullOrWhiteSpace(filterSpec) Then
						For Each fiFile In diSourceFolder.GetFiles(filterSpec)
							If lstFilesToCopy.Contains(fiFile.Name) Then
								lstFilesToCopy.Remove(fiFile.Name)
							End If
						Next
					End If
				Next
			End If


			For Each fileName In lstFilesToCopy
				Dim fiSourceFile = New FileInfo(Path.Combine(diSourceFolder.FullName, fileName))
				Dim fiTargetFile = New FileInfo(Path.Combine(diTargetFolder.FullName, fileName))
				Dim copyFile = False

				If Not fiTargetFile.Exists Then
					copyFile = True
				ElseIf fiTargetFile.Length <> fiSourceFile.Length Then
					copyFile = True
				ElseIf fiTargetFile.LastWriteTimeUtc < fiSourceFile.LastWriteTimeUtc Then
					copyFile = True
				End If

				If copyFile Then
					Dim retriesRemaining = maxRetryCount

					Dim success = False
					While Not success
						success = m_FileTools.CopyFileUsingLocks(fiSourceFile, fiTargetFile.FullName, m_MachName, True)
						If Not success Then
							retriesRemaining -= 1
							If retriesRemaining < 0 Then
								m_message = "Error copying " & fiSourceFile.FullName & " to " & fiTargetFile.Directory.FullName
								Return False
							End If

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error copying " & fiSourceFile.FullName & " to " & fiTargetFile.Directory.FullName & "; RetriesRemaining: " & retriesRemaining)

							' Wait 2 seconds then try again
							Thread.Sleep(2000)
						End If
					End While

				End If
			Next

		Catch ex As Exception
			m_message = "Error in SynchronizeFolders"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Updates the analysis summary file
	''' </summary>
	''' <returns>TRUE for success, FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function UpdateSummaryFile() As Boolean
		Dim strTool As String
		Dim strToolAndStepTool As String
		Try
			'Add a separator
			m_SummaryFile.Add(Environment.NewLine)
			m_SummaryFile.Add("=====================================================================================")
			m_SummaryFile.Add(Environment.NewLine)

			' Construct the Tool description (combination of Tool name and Step Tool name)
			strTool = m_jobParams.GetParam("ToolName")

			strToolAndStepTool = m_jobParams.GetParam("StepTool")
			If strToolAndStepTool Is Nothing Then strToolAndStepTool = String.Empty

			If strToolAndStepTool <> strTool Then
				If strToolAndStepTool.Length > 0 Then
					strToolAndStepTool &= " (" & strTool & ")"
				Else
					strToolAndStepTool &= strTool
				End If
			End If

			'Add the data
			m_SummaryFile.Add("Job Number" & ControlChars.Tab & m_JobNum)
			m_SummaryFile.Add("Job Step" & ControlChars.Tab & m_jobParams.GetParam("StepParameters", "Step"))
			m_SummaryFile.Add("Date" & ControlChars.Tab & DateTime.Now().ToString)
			m_SummaryFile.Add("Processor" & ControlChars.Tab & m_MachName)
			m_SummaryFile.Add("Tool" & ControlChars.Tab & strToolAndStepTool)
			m_SummaryFile.Add("Dataset Name" & ControlChars.Tab & m_Dataset)
			m_SummaryFile.Add("Xfer Folder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
			m_SummaryFile.Add("Param File Name" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
			m_SummaryFile.Add("Settings File Name" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
			m_SummaryFile.Add("Legacy Organism Db Name" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
			m_SummaryFile.Add("Protein Collection List" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
			m_SummaryFile.Add("Protein Options List" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
			m_SummaryFile.Add("Fasta File Name" & ControlChars.Tab & m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
			m_SummaryFile.Add("Analysis Time (hh:mm:ss)" & ControlChars.Tab & CalcElapsedTime(m_StartTime, m_StopTime))

			'Add another separator
			m_SummaryFile.Add(Environment.NewLine)
			m_SummaryFile.Add("=====================================================================================")
			m_SummaryFile.Add(Environment.NewLine)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("StepParameters", "Step") _
			 & " - " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Unzips all files in the specified Zip file
	''' Output folder is m_WorkDir
	''' </summary>
	''' <param name="ZipFilePath">File to unzip</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function UnzipFile(ByVal ZipFilePath As String) As Boolean
		Return UnzipFile(ZipFilePath, m_WorkDir, String.Empty)
	End Function

	''' <summary>
	''' Unzips all files in the specified Zip file
	''' Output folder is TargetDirectory
	''' </summary>
	''' <param name="ZipFilePath">File to unzip</param>
	''' <param name="TargetDirectory">Target directory for the extracted files</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String) As Boolean
		Return UnzipFile(ZipFilePath, TargetDirectory, String.Empty)
	End Function

	''' <summary>
	''' Unzips files in the specified Zip file that match the FileFilter spec
	''' Output folder is TargetDirectory
	''' </summary>
	''' <param name="ZipFilePath">File to unzip</param>
	''' <param name="TargetDirectory">Target directory for the extracted files</param>
	''' <param name="FileFilter">FilterSpec to apply, for example *.txt</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String, ByVal FileFilter As String) As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel
		Return m_IonicZipTools.UnzipFile(ZipFilePath, TargetDirectory, FileFilter)
	End Function

	''' <summary>
	''' Make sure the _DTA.txt file exists and has at least one spectrum in it
	''' </summary>
	''' <returns>True if success; false if failure</returns>
	''' <remarks></remarks>
	Protected Function ValidateCDTAFile() As Boolean
		Dim strDTAFilePath As String

		strDTAFilePath = Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

		Return ValidateCDTAFile(strDTAFilePath)

	End Function

	Protected Function ValidateCDTAFile(ByVal strDTAFilePath As String) As Boolean

		Dim strLineIn As String
		Dim blnDataFound As Boolean = False

		Try
			If Not File.Exists(strDTAFilePath) Then
				m_message = "_DTA.txt file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strDTAFilePath)
				Return False
			End If

			Using srReader As StreamReader = New StreamReader(New FileStream(strDTAFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srReader.Peek > -1
					strLineIn = srReader.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						blnDataFound = True
						Exit Do
					End If
				Loop

			End Using

			If Not blnDataFound Then
				m_message = "The _DTA.txt file is empty"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

		Catch ex As Exception
			m_message = "Exception in ValidateCDTAFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return blnDataFound

	End Function

	''' <summary>
	''' Stores SourceFilePath in a zip file with the same name, but extension .zip
	''' </summary>
	''' <param name="SourceFilePath">Full path to the file to be zipped</param>
	''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
	''' <returns>True if success; false if an error</returns>
	Public Function ZipFile(ByVal SourceFilePath As String, ByVal DeleteSourceAfterZip As Boolean) As Boolean

		Dim blnSuccess As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel

		blnSuccess = m_IonicZipTools.ZipFile(SourceFilePath, DeleteSourceAfterZip)

		If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
			m_NeedToAbortProcessing = True
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Stores SourceFilePath in a zip file named ZipfilePath
	''' </summary>
	''' <param name="SourceFilePath">Full path to the file to be zipped</param>
	''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
	''' <param name="ZipfilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
	''' <returns>True if success; false if an error</returns>
	Public Function ZipFile(ByVal SourceFilePath As String, ByVal DeleteSourceAfterZip As Boolean, ByVal ZipFilePath As String) As Boolean
		Dim blnSuccess As Boolean
		m_IonicZipTools.DebugLevel = m_DebugLevel
		blnSuccess = m_IonicZipTools.ZipFile(SourceFilePath, DeleteSourceAfterZip, ZipFilePath)

		If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
			m_NeedToAbortProcessing = True
		End If

		Return blnSuccess

	End Function

#Region "Event Handlers"
	Private Sub m_FileTools_WaitingForLockQueue(SourceFilePath As String, TargetFilePath As String, MBBacklogSource As Integer, MBBacklogTarget As Integer) Handles m_FileTools.WaitingForLockQueue
		If DateTime.UtcNow.Subtract(m_LastLockQueueWaitTimeLog).TotalSeconds >= 30 Then
			m_LastLockQueueWaitTimeLog = DateTime.UtcNow
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Waiting for lockfile queue to fall below threshold (clsAnalysisResources); SourceBacklog=" & MBBacklogSource & " MB, TargetBacklog=" & MBBacklogTarget & " MB, Source=" & SourceFilePath & ", Target=" & TargetFilePath)
			End If
		End If
	End Sub

#End Region


#End Region

End Class


