Imports PRISM.Logging
Imports System.Xml
Imports System.IO
Imports AnalysisManagerBase.clsGlobal
'Imports AnalysisManagerTools

Public Class clsAnalysisToolRunnerBase
	Implements IToolRunner


#Region "Member Variables"
	'status tools
	Protected m_StatusTools As IStatusFile

	' access to the job parameters
	Protected m_jobParams As IJobParams

	' access to the logger
	Protected m_logger As ILogger

	' access to mgr parameters
	Protected m_mgrParams As IMgrParams

	' access to settings file parameters
    Protected m_settingsFileParams As New PRISM.Files.XmlSettingsFileAccessor

	' progress of run (in percent)
	Protected m_progress As Single = 0

	'	status code
	Protected m_status As IStatusFile.JobStatus

	'DTA count for status report
	Protected m_DtaCount As Integer = 0

	' for posting a general explanation for external consumption
	Protected m_message As String

	'Debug level
	Protected m_DebugLevel As Short

	'Working directory, machine name, & job number (used frequently by subclasses)
	Protected m_WorkDir As String
	Protected m_MachName As String
	Protected m_JobNum As String

	'Elapsed time information
	Protected m_StartTime As Date
	Protected m_StopTime As Date

	'Results folder name
	Protected m_ResFolderName As String

	'DLL file info
	Protected m_FileVersion As String
	Protected m_FileDate As String

#End Region

	Public Sub New()
	End Sub

	Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
	 ByVal logger As ILogger, ByVal StatusTools As IStatusFile) Implements IToolRunner.Setup

		m_mgrParams = mgrParams
		m_logger = logger
		m_jobParams = jobParams
		m_StatusTools = StatusTools
		m_WorkDir = m_mgrParams.GetParam("commonfileandfolderlocations", "workdir")
		m_MachName = m_mgrParams.GetParam("programcontrol", "machname")
		m_JobNum = m_jobParams.GetParam("jobNum")
		m_DebugLevel = CShort(m_mgrParams.GetParam("programcontrol", "debuglevel"))

		m_StatusTools.Tool = m_jobParams.GetParam("tool")

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerBase.Setup()", ILogger.logMsgType.logDebug, True)
		End If

	End Sub

	'Publicly accessible results folder name and path
	Public ReadOnly Property ResFolderName() As String Implements IToolRunner.ResFolderName
		Get
			Return m_ResFolderName
		End Get
	End Property

	' explanation of what happened to last operation this class performed
	Public ReadOnly Property Message() As String Implements IToolRunner.Message
		Get
			Return m_message
		End Get
	End Property

	' the state of completion of the job (as a percentage)
	Public ReadOnly Property Progress() As Single Implements IToolRunner.Progress
		Get
			Return m_progress
		End Get
	End Property

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

	Public Overridable Function RunTool() As IJobParams.CloseOutType Implements IToolRunner.RunTool

		'Runs the job. Major work is performed by overrides

		'Make log entry
		m_logger.PostEntry(m_MachName & ": Starting analysis, job " & m_JobNum, _
		 ILogger.logMsgType.logNormal, LOG_DATABASE)
		'Load the settings file
		Try
			If Not LoadSettingsFile() Then
				m_logger.PostEntry("Unable to load settings file, job " & m_JobNum, _
				 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				m_message = AppendToComment(m_message, "Unable to load settings file")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Catch Err As Exception
			m_logger.PostEntry("Unable to load settings file, job " & m_JobNum, _
			 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
			m_message = AppendToComment(m_message, "Unable to load settings file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Start the job timer
		m_StartTime = Now

		'Remainder of method is supplied by subclasses

	End Function

	Protected Overridable Function MakeResultsFolder(ByVal AnalysisType As String) As IJobParams.CloseOutType

		'Makes results folder and moves files into it
		Dim ResFolderNamePath As String
		Dim Files() As String
		Dim TmpFile As String

		'Log status
		m_logger.PostEntry(m_MachName & ": Creating results folder, Job " & m_JobNum, _
		 ILogger.logMsgType.logNormal, LOG_DATABASE)
		m_ResFolderName = AnalysisType & Now().ToString("yyyyMMddHHmm") & "_Auto" & m_JobNum
		ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)

		'make the results folder
		Try
			Directory.CreateDirectory(ResFolderNamePath)
		Catch Err As Exception
			m_logger.PostError("Error making results folder, job " & m_JobNum, Err, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error making results folder")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Move files into results folder
		Try
			Files = Directory.GetFiles(m_WorkDir, "*.*")
			For Each TmpFile In Files
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("clsAnalysisToolRunnerBase.MakeResultFolder(); Moving files to results folder", _
					 ILogger.logMsgType.logDebug, True)
					m_logger.PostEntry("TmpFile = " & TmpFile, ILogger.logMsgType.logDebug, True)
					m_logger.PostEntry("ResFolderNamePath = " & ResFolderNamePath, ILogger.logMsgType.logDebug, True)
					m_logger.PostEntry("GetFileName(TmpFile) = " & Path.GetFileName(TmpFile), ILogger.logMsgType.logDebug, True)
				End If
				'If valid file name, then copy file to results folder
				'	(Required because Sequest sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in 
				'	working directory)
				If (Asc(Path.GetFileName(TmpFile).Chars(0)) > 31) And (Asc(Path.GetFileName(TmpFile).Chars(0)) < 128) Then
					If m_DebugLevel > 0 Then
						m_logger.PostEntry("clsAnalysisToolRunnerBase.MakeResultFolder(); Accepted file " & TmpFile, _
						 ILogger.logMsgType.logDebug, True)
					End If
					File.Move(TmpFile, Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile)))
				Else
					If m_DebugLevel > 0 Then
						m_logger.PostEntry("clsAnalysisToolRunnerBase.MakeResultFolder(); Rejected file " & TmpFile, _
						 ILogger.logMsgType.logDebug, True)
					End If
				End If
			Next
		Catch Err As Exception
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerBase.MakeResultFolder(); Error moving files to results folder", _
				 ILogger.logMsgType.logError, True)
				m_logger.PostEntry("Tmpfile = " & TmpFile, ILogger.logMsgType.logDebug, True)
				m_logger.PostEntry("Results folder name = " & Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile)), _
				 ILogger.logMsgType.logDebug, True)
			End If
			m_logger.PostError("Error moving results files, job " & m_JobNum, Err, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error moving results files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Make the summary file
		OutputSummary(ResFolderNamePath)

	End Function

	Protected Overridable Function UpdateSummaryFile() As Boolean

		'Add a separator
		clsSummaryFile.Add(vbCrLf & "=====================================================================================" & vbCrLf)

		'Add the data
		clsSummaryFile.Add("Job Number" & vbTab & m_JobNum)
		clsSummaryFile.Add("Date" & vbTab & Now())
		clsSummaryFile.Add("Processor" & vbTab & m_MachName)
		clsSummaryFile.Add("Tool" & vbTab & m_jobParams.GetParam("tool"))
		clsSummaryFile.Add("Priority" & vbTab & m_jobParams.GetParam("priority"))
		clsSummaryFile.Add("Dataset Name" & vbTab & m_jobParams.GetParam("datasetNum"))
		clsSummaryFile.Add("Dataset Folder Name" & vbTab & m_jobParams.GetParam("datasetFolderName"))
		clsSummaryFile.Add("Dataset Folder Path" & vbTab & m_jobParams.GetParam("datasetFolderStoragePath"))
		clsSummaryFile.Add("Xfer Folder" & vbTab & m_jobParams.GetParam("transferFolderPath"))
		clsSummaryFile.Add("Param File Name" & vbTab & m_jobParams.GetParam("parmFileName"))
		clsSummaryFile.Add("Param File Path" & vbTab & m_jobParams.GetParam("parmFileStoragePath"))
		clsSummaryFile.Add("Settings File Name" & vbTab & m_jobParams.GetParam("settingsFileName"))
		clsSummaryFile.Add("Settings File Path" & vbTab & m_jobParams.GetParam("settingsFileStoragePath"))
		clsSummaryFile.Add("Legacy Organism Db Name" & vbTab & m_jobParams.GetParam("LegacyFastaFileName"))
		clsSummaryFile.Add("Protein Collection List" & vbTab & m_jobParams.GetParam("ProteinCollectionList"))
		clsSummaryFile.Add("Protein Options List" & vbTab & m_jobParams.GetParam("ProteinOptionsList"))
		clsSummaryFile.Add("Fasta File Name" & vbTab & m_jobParams.GetParam("generatedFastaName"))
		clsSummaryFile.Add("Analysis Time (hh:mm)" & vbTab & CalcElapsedTime(m_StartTime, m_StopTime))

		'Add another separator
		clsSummaryFile.Add(vbCrLf & "=====================================================================================" & vbCrLf)

		Return True

	End Function

	Private Function CalcElapsedTime(ByVal StartTime As Date, ByVal StopTime As Date) As String

		Dim Hours As Long
		Dim Minutes As Long
		Dim Etime As Long

		If StopTime < StartTime Then Return ""

		Etime = DateDiff(DateInterval.Minute, StartTime, StopTime)
		Hours = CLng(Fix(Etime / 60))
		Minutes = Etime - (60 * Hours)

		If m_DebugLevel > 0 Then
			m_logger.PostEntry("CalcElapsedTime, StartTime = " & StartTime.ToString, ILogger.logMsgType.logDebug, True)
			m_logger.PostEntry("CalcElapsedTime, Stoptime = " & StopTime.ToString, ILogger.logMsgType.logDebug, True)
			m_logger.PostEntry("CalcElapsedTime, ETime = " & Etime.ToString, ILogger.logMsgType.logDebug, True)
			m_logger.PostEntry("CalcElapsedTime, Hours = " & Hours.ToString, ILogger.logMsgType.logDebug, True)
			m_logger.PostEntry("CalcElapsedTime, Minutes = " & Minutes.ToString, ILogger.logMsgType.logDebug, True)
		End If
		Return Hours.ToString("###0") & ":" & Minutes.ToString("00")

	End Function

	Protected Overridable Sub CleanupFailedJob(ByVal OopsMessage As String)

		'Sets return message from analysis error and cleans working directory
		m_message = AppendToComment(m_message, OopsMessage)
		CleanWorkDir(m_WorkDir, m_logger)

	End Sub

	Private Sub OutputSummary(ByVal OutputPath As String)

		'Saves the summary file in the results folder

		clsAssemblyTools.GetComponentFileVersionInfo()
		'		clsAssemblyTools.GetLoadedAssemblyInfo()

		clsSummaryFile.SaveSummaryFile(Path.Combine(OutputPath, "AnalysisSummary.txt"))

	End Sub

	Public Overridable Function DeleteFileWithRetries(ByVal FileNamePath As String) As Boolean

		'Makes multiple tries to delete specified file. Returns True for success; Raises exception and 
		'		returns False for error

		Dim RetryCount As Integer = 0
		Dim ErrType As AMFileNotDeletedAfterRetryException.RetryExceptionType

		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method", ILogger.logMsgType.logDebug, True)
		End If

		'Verify specified file exists
		If Not File.Exists(FileNamePath) Then
			'Throw an exception
			Throw New AMFileNotFoundException(FileNamePath, "Specified file not found")
			Return False
		End If

		While RetryCount < 3
			Try
				File.Delete(FileNamePath)
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit", ILogger.logMsgType.logDebug, True)
				End If
				Return True
			Catch Err1 As UnauthorizedAccessException
				'File may be read-only. Clear read-only flag and try again
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("File " & FileNamePath & " exception ERR1: " & Err1.Message, ILogger.logMsgType.logDebug, True)
					If Not Err1.InnerException Is Nothing Then
						m_logger.PostEntry("Inner exception: " & Err1.InnerException.Message, ILogger.logMsgType.logDebug, True)
					End If
					m_logger.PostEntry("File " & FileNamePath & " may be read-only, attribute reset attempt #" & _
					 RetryCount.ToString, ILogger.logMsgType.logDebug, True)
				End If
				File.SetAttributes(FileNamePath, File.GetAttributes(FileNamePath) And (Not FileAttributes.ReadOnly))
				ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception
				RetryCount += 1
			Catch Err2 As IOException
				'If problem is locked file, attempt to fix lock and retry
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("File " & FileNamePath & " exception ERR2: " & Err2.Message, ILogger.logMsgType.logDebug, True)
					If Not Err2.InnerException Is Nothing Then
						m_logger.PostEntry("Inner exception: " & Err2.InnerException.Message, ILogger.logMsgType.logDebug, True)
					End If
					m_logger.PostEntry("Error deleting file " & FileNamePath & ", attempt #" & RetryCount.ToString, ILogger.logMsgType.logDebug, True)
				End If
				ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception
				'Delay 5 seconds
				System.Threading.Thread.Sleep(5000)
				'Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd 
				GC.Collect()
				GC.WaitForPendingFinalizers()
				RetryCount += 1
			Catch Err3 As Exception
				m_logger.PostError("Error deleting file, exception ERR3 " & FileNamePath, Err3, LOG_DATABASE)
				Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
				Return False
			End Try
		End While

		''If we got to here, then we've exceeded the max retry limit; attempt to move the file to garbage can
		'If Not MoveFileToGarbage(FileNamePath) Then
		'	'File can't be deleted or moved
		'	Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")
		'	Return False
		'Else
		'	Return True
		'End If

		'If we got to here, then we've exceeded the max retry limit
		Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")
		Return False

	End Function

	'Public Overridable Function MoveFileToGarbage(ByVal FileNamePath As String) As Boolean

	'	'Moves specified file to C:\GarbageCan folder. Returns True for success, False for error

	'	'Verify garbage can exists
	'	If Not Directory.Exists("C:\GarbageCan") Then
	'		m_logger.PostEntry("Garbage directory not found", ILogger.logMsgType.logError, LOG_DATABASE)
	'		Return False
	'	End If

	'	Dim MyFileName As String = Path.GetFileName(FileNamePath)
	'	Dim DestFile As String = Path.Combine("C:\GarbageCan", MyFileName)

	'	Try
	'		File.Move(FileNamePath, DestFile)
	'		m_logger.PostEntry("Moved file " & MyFileName & " to garbage", ILogger.logMsgType.logWarning, True)
	'		Return True
	'	Catch Err1 As UnauthorizedAccessException
	'		If m_DebugLevel > 0 Then
	'			m_logger.PostEntry("File " & FileNamePath & " to " & DestFile & " move exception ERR1: " & Err1.Message, _
	'					ILogger.logMsgType.logDebug, True)
	'			If Not Err1.InnerException Is Nothing Then
	'				m_logger.PostEntry("Inner exception: " & Err1.InnerException.Message, ILogger.logMsgType.logDebug, True)
	'			End If
	'		End If
	'		Return False
	'	Catch Err2 As IOException
	'		If m_DebugLevel > 0 Then
	'			m_logger.PostEntry("File " & FileNamePath & " move exception ERR2: " & Err2.Message, ILogger.logMsgType.logDebug, True)
	'			If Not Err2.InnerException Is Nothing Then
	'				m_logger.PostEntry("Inner exception: " & Err2.InnerException.Message, ILogger.logMsgType.logDebug, True)
	'			End If
	'		End If
	'		Return False
	'	Catch Err3 As Exception
	'		m_logger.PostError("Error deleting file, exception ERR3 " & FileNamePath, Err3, LOG_DATABASE)
	'		Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
	'		Return False
	'	End Try

	'End Function

End Class
