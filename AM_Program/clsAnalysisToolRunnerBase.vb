'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.Xml
Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerBase

	Public Class clsAnalysisToolRunnerBase
		Implements IToolRunner

		'*********************************************************************************************************
		'Base class for analysis tool runner
		'*********************************************************************************************************

#Region "Constants"
		Protected Const SP_NAME_SET_TASK_TOOL_VERSION As String = "SetStepTaskToolVersion"
		Protected Const DATE_TIME_FORMAT As String = "yyyy-MM-dd hh:mm:ss tt"
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

		' for posting a general explanation for external consumption
		Protected m_message As String = String.Empty

		'Debug level
		Protected m_DebugLevel As Short

		'Working directory, machine name, & job number (used frequently by subclasses)
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
		Protected m_NeedToAbortProcessing As Boolean

#End Region

#Region "Properties"
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
		''' Constructor
		''' </summary>
		''' <remarks>Does nothing at present</remarks>
		Public Sub New()
		End Sub

		''' <summary>
		''' Initializes class
		''' </summary>
		''' <param name="mgrParams">Object holding manager parameters</param>
		''' <param name="jobParams">Object holding job parameters</param>
		''' <param name="StatusTools">Object for status reporting</param>
		''' <remarks></remarks>
		Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
		  ByVal StatusTools As IStatusFile) Implements IToolRunner.Setup

			m_mgrParams = mgrParams
			m_jobParams = jobParams
			m_StatusTools = StatusTools
			m_WorkDir = m_mgrParams.GetParam("workdir")
			m_MachName = m_mgrParams.GetParam("MgrName")
			m_JobNum = m_jobParams.GetParam("StepParameters", "Job")
			m_Dataset = m_jobParams.GetParam("JobParameters", "DatasetNum")
			m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel"))
			m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription()

			m_ResFolderName = m_jobParams.GetParam("OutputFolderName")

			If m_DebugLevel > 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.Setup()")
			End If

			m_IonicZipTools = New clsIonicZipTools(m_DebugLevel, m_WorkDir)
			m_NeedToAbortProcessing = False

		End Sub

		''' <summary>
		''' Loads the job settings file
		''' </summary>
		''' <returns>TRUE for success, FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Function LoadSettingsFile() As Boolean
			Dim fileName As String = m_jobParams.GetParam("settingsFileName")
			If fileName <> "na" Then
				Dim filePath As String = System.IO.Path.Combine(m_WorkDir, fileName)
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
			m_StartTime = System.DateTime.UtcNow

			'Remainder of method is supplied by subclasses

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function

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
				progLoc = System.IO.Path.Combine(progLoc, strStepToolVersion)

				If Not System.IO.Directory.Exists(progLoc) Then
					m_message = "Version-specific folder not found for " & strStepToolName
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
					Return String.Empty
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Using specific version of " & strStepToolName & ": " & progLoc)
				End If
			End If

			' Define the path to the .Exe, then verify that it exists
			progLoc = System.IO.Path.Combine(progLoc, strExeName)

			If Not System.IO.File.Exists(progLoc) Then
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
			GetCurrentMgrSettingsFromDB(0)
		End Function

		''' <summary>
		''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
		''' </summary>
		''' <param name="intUpdateIntervalSeconds">The minimum number of seconds between updates; if fewer than intUpdateIntervalSeconds seconds have elapsed since the last call to this function, then no update will occur</param>
		''' <returns></returns>
		''' <remarks></remarks>
		Protected Function GetCurrentMgrSettingsFromDB(ByVal intUpdateIntervalSeconds As Integer) As Boolean
			GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds, m_mgrParams, m_DebugLevel)
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

			Dim MyConnection As System.Data.SqlClient.SqlConnection
			Dim MyCmd As New System.Data.SqlClient.SqlCommand
			Dim drSqlReader As System.Data.SqlClient.SqlDataReader
			Dim ConnectionString As String

			Dim strParamName As String
			Dim strParamValue As String
			Dim intValueCountRead As Integer = 0

			Dim intNewDebugLevel As Short

			Static dtLastUpdateTime As System.DateTime

			Try

				If intUpdateIntervalSeconds > 0 AndAlso System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds < intUpdateIntervalSeconds Then
					Return True
				End If
				dtLastUpdateTime = System.DateTime.UtcNow

				If DebugLevel >= 5 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings from the Manager Control DB")
				End If

				ConnectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr")
				MyConnection = New System.Data.SqlClient.SqlConnection(ConnectionString)
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

			Catch ex As System.Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception getting current manager settings from the manager control DB" & ex.Message)
			End Try

			If intValueCountRead > 0 Then
				Return True
			Else
				Return False
			End If

		End Function

		''' <summary>
		''' Creates a results folder after analysis complete
		''' </summary>
		''' <returns>CloseOutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function MakeResultsFolder() As IJobParams.CloseOutType

			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)

			'Makes results folder and moves files into it
			Dim ResFolderNamePath As String

			'Log status (both locally and in the DB)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MachName & ": Creating results folder, Job " & m_JobNum)
			ResFolderNamePath = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)

			'make the results folder
			Try
				Directory.CreateDirectory(ResFolderNamePath)
			Catch Err As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error making results folder, job " & m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(Err))
				m_message = AppendToComment(m_message, "Error making results folder")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

		End Function

		''' <summary>
		''' Moves result files after tool has completed
		''' </summary>
		''' <returns>CloseOutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function MoveResultFiles() As IJobParams.CloseOutType
			Const REJECT_LOGGING_THRESHOLD As Integer = 10
			Const ACCEPT_LOGGING_THRESHOLD As Integer = 50
			Const LOG_LEVEL_REPORT_ACCEPT_OR_REJECT As Integer = 5

			'Makes results folder and moves files into it
			Dim ResFolderNamePath As String = String.Empty
			Dim strTargetFilePath As String = String.Empty

			Dim Files() As String
			Dim TmpFile As String = String.Empty
			Dim TmpFileNameLcase As String = String.Empty
			Dim OkToMove As Boolean = False
			Dim ext As String
			Dim excpt As String
			Dim intIndex As Integer
			Dim strLogMessage As String

			Dim strExtension As String
			Dim htRejectStats As System.Collections.Hashtable
			Dim htAcceptStats As System.Collections.Hashtable
			Dim objExtension As System.Collections.IDictionaryEnumerator

			Dim blnErrorEncountered As Boolean = False

			'Move files into results folder
			Try
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)
				ResFolderNamePath = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
				htRejectStats = New System.Collections.Hashtable
				htAcceptStats = New System.Collections.Hashtable

				'Log status
				If m_DebugLevel >= 2 Then
					strLogMessage = "Move Result Files to " & ResFolderNamePath
					If m_DebugLevel >= 3 Then
						strLogMessage &= "; FilesToDelete contains " & FilesToDelete.Count.ToString & " entries" & _
						  "; m_FilesToDeleteExt contains " & m_FilesToDeleteExt.Count.ToString & " entries" & _
						  "; m_ExceptionFiles contains " & m_ExceptionFiles.Count.ToString & " entries"
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strLogMessage)
				End If


				' Make sure the entries in FilesToDelete are all lowercase
				For intIndex = 0 To clsGlobal.FilesToDelete.Count - 1
					clsGlobal.FilesToDelete(intIndex) = clsGlobal.FilesToDelete(intIndex).ToLower
				Next intIndex

				' Make sure FilesToDelete is sorted
				FilesToDelete.Sort()

				' Make sure the entries in clsGlobal.m_FilesToDeleteExt are all lowercase
				For intIndex = 0 To clsGlobal.m_FilesToDeleteExt.Count - 1
					clsGlobal.m_FilesToDeleteExt(intIndex) = clsGlobal.m_FilesToDeleteExt(intIndex).ToLower
				Next intIndex

				' Make sure the entries in  clsGlobal.m_ExceptionFiles are all lowercase
				For intIndex = 0 To clsGlobal.m_ExceptionFiles.Count - 1
					clsGlobal.m_ExceptionFiles(intIndex) = clsGlobal.m_ExceptionFiles(intIndex).ToLower
				Next intIndex


				' Obtain a list of all files in the working directory
				Files = Directory.GetFiles(m_WorkDir, "*.*")

				' Check each file against clsGlobal.m_FilesToDeleteExt and clsGlobal.m_ExceptionFiles
				For Each TmpFile In Files
					OkToMove = True
					TmpFileNameLcase = System.IO.Path.GetFileName(TmpFile).ToLower

					' Check to see if the filename is defined in FilesToDelete
					' Note that entries in FilesToDelete are already lowercase (thanks to a for loop earlier in this function)
					If FilesToDelete.BinarySearch(TmpFileNameLcase) >= 0 Then
						' File found in the FilesToDelete list; do not move it
						OkToMove = False
					End If

					If OkToMove Then
						'Check to see if the file ends with an entry specified in m_FilesToDeleteExt
						' Note that entries in m_FilesToDeleteExt are already lowercase (thanks to a for loop earlier in this function)
						For Each ext In clsGlobal.m_FilesToDeleteExt
							If TmpFileNameLcase.EndsWith(ext) Then
								OkToMove = False
								Exit For
							End If
						Next
					End If

					If Not OkToMove Then
						' Check to see if the file is a result file that got captured as a non result file
						' Note that entries in m_ExceptionFiles are already lowercase (thanks to a for loop earlier in this function)
						For Each excpt In clsGlobal.m_ExceptionFiles
							If TmpFileNameLcase.Contains(excpt) Then
								OkToMove = True
								Exit For
							End If
						Next
					End If

					' Look for invalid characters in the filename
					'	(Required because extract_msn.exe sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in working directory) 
					' Note: now evaluating each character in the filename
					If OkToMove Then
						Dim intAscValue As Integer
						For Each chChar As Char In System.IO.Path.GetFileName(TmpFile).ToCharArray
							intAscValue = System.Convert.ToInt32(chChar)
							If intAscValue <= 31 Or intAscValue >= 128 Then
								' Invalid character found
								OkToMove = False
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
								Exit For
							End If
						Next
					Else
						If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
							strExtension = System.IO.Path.GetExtension(TmpFile)
							If htRejectStats.Contains(strExtension) Then
								htRejectStats(strExtension) = CInt(htRejectStats(strExtension)) + 1
							Else
								htRejectStats.Add(strExtension, 1)
							End If

							' Only log the first 10 times files of a given extension are rejected
							'  However, if a file was rejected due to invalid characters in the name, then we don't track that rejection with htRejectStats
							If CInt(htRejectStats(strExtension)) <= REJECT_LOGGING_THRESHOLD Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected file:  " & TmpFile)
							End If
						End If
					End If

					'If valid file name, then move file to results folder
					If OkToMove Then
						If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
							strExtension = System.IO.Path.GetExtension(TmpFile).ToLower
							If htAcceptStats.Contains(strExtension) Then
								htAcceptStats(strExtension) = CInt(htAcceptStats(strExtension)) + 1
							Else
								htAcceptStats.Add(strExtension, 1)
							End If

							' Only log the first 50 times files of a given extension are accepted
							If CInt(htAcceptStats(strExtension)) <= ACCEPT_LOGGING_THRESHOLD Then
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & TmpFile)
							End If
						End If

						Try
							strTargetFilePath = System.IO.Path.Combine(ResFolderNamePath, System.IO.Path.GetFileName(TmpFile))
							System.IO.File.Move(TmpFile, strTargetFilePath)
						Catch ex As Exception
							Try
								' Move failed
								' Attempt to copy the file instead of moving the file
								System.IO.File.Copy(TmpFile, strTargetFilePath, True)
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
					' Look for any extensions in htAcceptStats that had over 50 accepted files
					objExtension = htAcceptStats.GetEnumerator
					Do While objExtension.MoveNext
						If CInt(objExtension.Value) > ACCEPT_LOGGING_THRESHOLD Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted a total of " & CInt(objExtension.Value) & " files with extension " & CStr(objExtension.Key))
						End If
					Loop

					' Look for any extensions in htRejectStats that had over 10 rejected files
					objExtension = htRejectStats.GetEnumerator
					Do While objExtension.MoveNext
						If CInt(objExtension.Value) > REJECT_LOGGING_THRESHOLD Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected a total of " & CInt(objExtension.Value) & " files with extension " & CStr(objExtension.Key))
						End If
					Loop
				End If

			Catch Err As Exception
				If m_DebugLevel > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results folder")
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Tmpfile = " & TmpFile)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder name = " & System.IO.Path.Combine(ResFolderNamePath, Path.GetFileName(TmpFile)))
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error moving results files, job " & m_JobNum & Err.Message)
				m_message = AppendToComment(m_message, "Error moving results files")

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
				objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

		End Function

		''' <summary>
		''' Adds double quotes around a path if it contains a space
		''' </summary>
		''' <param name="strPath"></param>
		''' <returns>The path (updated if necessary)</returns>
		''' <remarks></remarks>
		Protected Function PossiblyQuotePath(ByVal strPath As String) As String
			If strPath.Contains(" "c) Then
				Return """" & strPath & """"
			Else
				Return strPath
			End If
		End Function

		''' <summary>
		''' Updates the dataset name to the final folder name in the transferFolderPath job parameter
		''' Updates the transfer folder path to remove the final folder
		''' </summary>
		''' <remarks></remarks>
		Protected Sub RedefineAggregationJobDatasetAndTransferFolder()

			Dim strTransferFolderPath As String = m_jobParams.GetParam("transferFolderPath")
			Dim diTransferFolder As New System.IO.DirectoryInfo(strTransferFolderPath)

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
			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim strKey As String
			Dim strValue As String
			Dim intEqualsLoc As Integer

			strVersion = String.Empty
			Dim blnSuccess As Boolean = False

			Try

				If Not System.IO.File.Exists(strVersionInfoFilePath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Version Info File not found: " & strVersionInfoFilePath)
					Return False
				End If

				srInFile = New System.IO.StreamReader(New System.IO.FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

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
										clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Empty version line in Version Info file for " & System.IO.Path.GetFileName(strDLLFilePath))
										blnSuccess = False
									Else
										blnSuccess = True
									End If
								Case "error"
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reported by DLLVersionInspector for " & System.IO.Path.GetFileName(strDLLFilePath) & ": " & strValue)
									blnSuccess = False
								Case Else
									' Ignore the line
							End Select
						End If

					End If
				Loop

				srInFile.Close()

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading Version Info File for " & System.IO.Path.GetFileName(strDLLFilePath), ex)
			End Try

			Return blnSuccess

		End Function

		''' <summary>
		''' Creates a Tool Version Info file
		''' </summary>
		''' <param name="strFolderPath"></param>
		''' <param name="strToolVersionInfo"></param>
		''' <returns></returns>
		''' <remarks></remarks>
		Protected Function SaveToolVersionInfoFile(ByVal strFolderPath As String, ByVal strToolVersionInfo As String) As Boolean
			Dim swToolVersionFile As System.IO.StreamWriter
			Dim strToolVersionFilePath As String

			Try
				strToolVersionFilePath = System.IO.Path.Combine(strFolderPath, "Tool_Version_Info_" & m_jobParams.GetParam("StepTool") & ".txt")

				swToolVersionFile = New System.IO.StreamWriter(New System.IO.FileStream(strToolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

				swToolVersionFile.WriteLine("Date: " & System.DateTime.Now().ToString(DATE_TIME_FORMAT))
				swToolVersionFile.WriteLine("Dataset: " & m_Dataset)
				swToolVersionFile.WriteLine("Job: " & m_JobNum)
				swToolVersionFile.WriteLine("Step: " & m_jobParams.GetParam("StepParameters", "Step"))
				swToolVersionFile.WriteLine("Tool: " & m_jobParams.GetParam("StepTool"))
				swToolVersionFile.WriteLine("ToolVersionInfo:")

				swToolVersionFile.WriteLine(strToolVersionInfo.Replace("; ", ControlChars.NewLine))
				swToolVersionFile.Close()

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception saving tool version info: " & ex.Message)
			End Try

		End Function

		''' <summary>
		''' Communicates with database to record the tool version(s) for the current step task
		''' </summary>
		''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
		''' <returns>True for success, False for failure</returns>
		''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
		Protected Function SetStepTaskToolVersion(ByVal strToolVersionInfo As String) As Boolean
			Return SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of FileInfo))
		End Function

		''' <summary>
		''' Communicates with database to record the tool version(s) for the current step task
		''' </summary>
		''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
		''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
		''' <returns>True for success, False for failure</returns>
		''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
		Protected Function SetStepTaskToolVersion(ByVal strToolVersionInfo As String, _
		   ByVal ioToolFiles As System.Collections.Generic.List(Of FileInfo)) As Boolean

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
		   ByVal ioToolFiles As System.Collections.Generic.List(Of FileInfo), _
		   ByVal blnSaveToolVersionTextFile As Boolean) As Boolean

			Dim strExeInfo As String = String.Empty
			Dim strToolVersionInfoCombined As String

			Dim Outcome As Boolean = False
			Dim ResCode As Integer

			If Not ioToolFiles Is Nothing Then

				For Each ioFileInfo As System.IO.FileInfo In ioToolFiles
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
			Dim MyCmd As New System.Data.SqlClient.SqlCommand
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SP_NAME_SET_TASK_TOOL_VERSION

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

				.Parameters.Add(New SqlClient.SqlParameter("@job", SqlDbType.Int))
				.Parameters.Item("@job").Direction = ParameterDirection.Input
				.Parameters.Item("@job").Value = CInt(m_jobParams.GetParam("StepParameters", "Job"))

				.Parameters.Add(New SqlClient.SqlParameter("@step", SqlDbType.Int))
				.Parameters.Item("@step").Direction = ParameterDirection.Input
				.Parameters.Item("@step").Value = CInt(m_jobParams.GetParam("StepParameters", "Step"))

				.Parameters.Add(New SqlClient.SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900))
				.Parameters.Item("@ToolVersionInfo").Direction = ParameterDirection.Input
				.Parameters.Item("@ToolVersionInfo").Value = strToolVersionInfoCombined
			End With

			Dim objAnalysisTask As clsAnalysisJob
			Dim strBrokerConnStr As String = m_mgrParams.GetParam("brokerconnectionstring")

			objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

			'Execute the SP (retry the call up to 4 times)
			ResCode = objAnalysisTask.ExecuteSP(MyCmd, strBrokerConnStr, 4)

			objAnalysisTask = Nothing

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
		''' Determines the version info for a DLL using reflection
		''' </summary>
		''' <param name="strToolVersionInfo">Version info string to append the veresion info to</param>
		''' <param name="strDLLFilePath">Path to the DLL</param>
		''' 	  ''' <returns>True if success; false if an error</returns>
		''' <remarks></remarks>
		Protected Overridable Function StoreToolVersionInfoOneFile(ByRef strToolVersionInfo As String, ByVal strDLLFilePath As String) As Boolean

			Dim ioFileInfo As System.IO.FileInfo

			Try
				ioFileInfo = New System.IO.FileInfo(strDLLFilePath)

				If Not ioFileInfo.Exists Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "File not found by StoreToolVersionInfoOneFile: " & strDLLFilePath)
					Return False
				Else

					Dim oAssemblyName As System.Reflection.AssemblyName
					oAssemblyName = System.Reflection.Assembly.LoadFrom(ioFileInfo.FullName).GetName

					Dim strNameAndVersion As String
					strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
					strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

					Return True
				End If

			Catch ex As Exception
				' If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
				'  <startup useLegacyV2RuntimeActivationPolicy="true">
				'    <supportedRuntime version="v4.0" />
				'  </startup>
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for " & System.IO.Path.GetFileName(strDLLFilePath) & ": " & ex.Message)
			End Try

			Return False

		End Function

		''' <summary>
		''' Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
		''' </summary>
		''' <param name="strToolVersionInfo"></param>
		''' <param name="strDLLFilePath"></param>
		''' <returns>True if success; false if an error</returns>
		''' <remarks></remarks>
		Protected Overridable Function StoreToolVersionInfoOneFile64Bit(ByRef strToolVersionInfo As String, ByVal strDLLFilePath As String) As Boolean

			Dim strNameAndVersion As String = String.Empty
			Dim strAppPath As String
			Dim strVersionInfoFilePath As String
			Dim strArgs As String

			Dim ioFileInfo As System.IO.FileInfo

			Try
				strAppPath = System.IO.Path.Combine(AppFolderPath, "DLLVersionInspector.exe")

				ioFileInfo = New System.IO.FileInfo(strDLLFilePath)
				strNameAndVersion = System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.Name) & ", Version="

				If Not ioFileInfo.Exists Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "File not found by StoreToolVersionInfoOneFile64Bit: " & strDLLFilePath)
					Return False
				ElseIf Not System.IO.File.Exists(strAppPath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DLLVersionInspector not found by StoreToolVersionInfoOneFile64Bit: " & strAppPath)
					Return False
				Else
					' Call DLLVersionInspector.exe to determine the tool version

					strVersionInfoFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_VersionInfo.txt")

					Dim objProgRunner As clsRunDosProgram
					Dim blnSuccess As Boolean
					Dim strVersion As String = String.Empty

					objProgRunner = New clsRunDosProgram(AppFolderPath)

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

					System.Threading.Thread.Sleep(100)

					blnSuccess = ReadVersionInfoFile(strDLLFilePath, strVersionInfoFilePath, strVersion)

					' Delete the version info file
					Try
						System.Threading.Thread.Sleep(100)
						System.IO.File.Delete(strVersionInfoFilePath)
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Version info for " & System.IO.Path.GetFileName(strDLLFilePath) & ": " & ex.Message)
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, System.IO.Path.GetFileNameWithoutExtension(strDLLFilePath))
			End Try

			Return False

		End Function

		''' <summary>
		''' Updates the analysis summary file
		''' </summary>
		''' <returns>TRUE for success, FALSE for failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function UpdateSummaryFile() As Boolean
			Dim strTool As String
			Dim strToolAndStepTool As String
			Try
				'Add a separator
				clsSummaryFile.Add(System.Environment.NewLine)
				clsSummaryFile.Add("=====================================================================================")
				clsSummaryFile.Add(System.Environment.NewLine)

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
				clsSummaryFile.Add("Job Number" & ControlChars.Tab & m_JobNum)
				clsSummaryFile.Add("Job Step" & ControlChars.Tab & m_jobParams.GetParam("StepParameters", "Step"))
				clsSummaryFile.Add("Date" & ControlChars.Tab & System.DateTime.Now().ToString)
				clsSummaryFile.Add("Processor" & ControlChars.Tab & m_MachName)
				clsSummaryFile.Add("Tool" & ControlChars.Tab & strToolAndStepTool)
				clsSummaryFile.Add("Dataset Name" & ControlChars.Tab & m_Dataset)
				clsSummaryFile.Add("Xfer Folder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
				clsSummaryFile.Add("Param File Name" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
				clsSummaryFile.Add("Settings File Name" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
				clsSummaryFile.Add("Legacy Organism Db Name" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
				clsSummaryFile.Add("Protein Collection List" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
				clsSummaryFile.Add("Protein Options List" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
				clsSummaryFile.Add("Fasta File Name" & ControlChars.Tab & m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
				clsSummaryFile.Add("Analysis Time (hh:mm:ss)" & ControlChars.Tab & CalcElapsedTime(m_StartTime, m_StopTime))

				'Add another separator
				clsSummaryFile.Add(System.Environment.NewLine)
				clsSummaryFile.Add("=====================================================================================")
				clsSummaryFile.Add(System.Environment.NewLine)

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("StepParameters", "Step") _
				 & " - " & ex.Message)
				Return False
			End Try

			Return True

		End Function

		''' <summary>
		''' Calculates total run time for a job
		''' </summary>
		''' <param name="StartTime">Time job started</param>
		''' <param name="StopTime">Time of job completion</param>
		''' <returns>Total job run time (HH:MM)</returns>
		''' <remarks></remarks>
		Protected Function CalcElapsedTime(ByVal StartTime As DateTime, ByVal StopTime As DateTime) As String
			Dim dtElapsedTime As System.TimeSpan

			If StopTime < StartTime Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Stop time is less than StartTime; this is unexpected.  Assuming current time for StopTime")
				StopTime = System.DateTime.UtcNow
			End If

			If StopTime < StartTime OrElse StartTime = System.DateTime.MinValue Then
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
		''' Sets return message from analysis error and cleans working directory
		''' </summary>
		''' <param name="OopsMessage">Message to include in job comment field</param>
		''' <remarks></remarks>
		Protected Overridable Sub CleanupFailedJob(ByVal OopsMessage As String)

			m_message = AppendToComment(m_message, OopsMessage)
			CleanWorkDir(m_WorkDir)

		End Sub

		Protected Function DeleteRawDataFiles() As IJobParams.CloseOutType
			Dim RawDataType As String
			RawDataType = m_jobParams.GetParam("RawDataType")

			Return DeleteRawDataFiles(RawDataType)
		End Function

		Protected Function DeleteRawDataFiles(ByVal RawDataType As String) As IJobParams.CloseOutType

			'Deletes the raw data files/folders from the working directory
			Dim IsFile As Boolean = True
			Dim IsNetworkDir As Boolean = False
			Dim FileOrFolderName As String = String.Empty

			Select Case RawDataType.ToLower
				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
					IsFile = True

				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_WIFF_EXTENSION)
					IsFile = True

				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_UIMF_EXTENSION)
					IsFile = True

				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_MZXML_FILES
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
					IsFile = True

				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
					IsFile = False

				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
					IsFile = False

				Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS

					Dim NewSourceFolder As String = clsAnalysisResources.ResolveSerStoragePath(m_WorkDir)
					'Check for "0.ser" folder
					If String.IsNullOrEmpty(NewSourceFolder) Then
						FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset)
						IsNetworkDir = False
					Else
						IsNetworkDir = True
					End If

					IsFile = False

				Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER
					' Bruker_FT folders are actually .D folders
					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
					IsFile = False

				Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_SPOT
					''''''''''''''''''''''''''''''''''''
					' TODO: Finalize this code
					'       DMS doesn't yet have a BrukerTOF dataset 
					'        so we don't know the official folder structure
					''''''''''''''''''''''''''''''''''''

					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset)
					IsFile = False

				Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_IMAGING

					''''''''''''''''''''''''''''''''''''
					' TODO: Finalize this code
					'       DMS doesn't yet have a BrukerTOF dataset 
					'        so we don't know the official folder structure
					''''''''''''''''''''''''''''''''''''

					FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_Dataset)
					IsFile = False

				Case Else
					'Should never get this value
					m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " & RawDataType
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
					System.IO.Directory.Delete(FileOrFolderName, True)
					Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				Catch ex As System.Exception
					m_message = "Exception deleting raw data folder " & FileOrFolderName & ": " & _
					 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try
			End If

		End Function

		''' <summary>
		''' Adds manager assembly data to job summary file
		''' </summary>
		''' <param name="OutputPath">Path to summary file</param>
		''' <remarks></remarks>
		Protected Sub OutputSummary(ByVal OutputPath As String)

			'Saves the summary file in the results folder

			clsAssemblyTools.GetComponentFileVersionInfo()
			clsSummaryFile.SaveSummaryFile(System.IO.Path.Combine(OutputPath, m_jobParams.GetParam("StepTool") & "_AnalysisSummary.txt"))

		End Sub


		''' <summary>
		''' Makes multiple tries to delete specified file
		''' </summary>
		''' <param name="FileNamePath">Full path to file for deletion</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks>Raises exception if error occurs</remarks>
		Public Overridable Function DeleteFileWithRetries(ByVal FileNamePath As String) As Boolean
			Return DeleteFileWithRetries(FileNamePath, m_DebugLevel)
		End Function

		''' <summary>
		''' Makes multiple tries to delete specified file
		''' </summary>
		''' <param name="FileNamePath">Full path to file for deletion</param>
		''' <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks>Raises exception if error occurs</remarks>
		Public Shared Function DeleteFileWithRetries(ByVal FileNamePath As String, ByVal intDebugLevel As Integer) As Boolean

			Dim RetryCount As Integer = 0
			Dim ErrType As AMFileNotDeletedAfterRetryException.RetryExceptionType

			If intDebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method")
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
					'Delay 5 seconds
					System.Threading.Thread.Sleep(5000)
					'Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd 
					GC.Collect()
					GC.WaitForPendingFinalizers()
					RetryCount += 1
				Catch Err3 As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting file, exception ERR3 " & FileNamePath & Err3.Message)
					Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
					Return False
				End Try
			End While

			'If we got to here, then we've exceeded the max retry limit
			Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")
			Return False

		End Function

		''' <summary>
		''' Copies the files from the results folder to the transfer folder on the server
		''' </summary>
		''' <returns></returns>
		''' <remarks></remarks>
		Protected Overridable Function CopyResultsFolderToServer() As IJobParams.CloseOutType

			Dim SourceFolderPath As String = String.Empty
			Dim TransferFolderPath As String = String.Empty
			Dim TargetFolderPath As String = String.Empty
			Dim ResultsFolderName As String = String.Empty

			Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

			Dim strMessage As String
			Dim blnErrorEncountered As Boolean = False
			Dim intFailedFileCount As Integer = 0


			Dim intRetryCount As Integer = 10
			Dim intRetryHoldoffSeconds As Integer = 15
			Dim blnIncreaseHoldoffOnEachRetry As Boolean = True

			Try
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.DELIVERING_RESULTS, 0)

				ResultsFolderName = m_jobParams.GetParam("OutputFolderName")
				If ResultsFolderName Is Nothing OrElse ResultsFolderName.Length = 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder name is not defined, job " & m_jobParams.GetParam("StepParameters", "Job"))
					m_message = "Results folder not found"
					'TODO: Handle errors
					' Without a source folder; there isn't much we can do
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				SourceFolderPath = System.IO.Path.Combine(m_WorkDir, ResultsFolderName)

				'Verify the source folder exists
				If Not System.IO.Directory.Exists(SourceFolderPath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Results folder not found, job " & m_jobParams.GetParam("StepParameters", "Job") & ", folder " & SourceFolderPath)
					m_message = "Results folder not found"
					'TODO: Handle errors
					' Without a source folder; there isn't much we can do
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				TransferFolderPath = m_jobParams.GetParam("transferFolderPath")

				' Verify transfer directory exists
				' First make sure TransferFolderPath is defined
				If String.IsNullOrEmpty(TransferFolderPath) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Transfer folder path not defined; job param 'transferFolderPath' is empty")
					m_message = AppendToComment(m_message, "Transfer folder path not defined")
					objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Now verify transfer directory exists
				Try
					objAnalysisResults.FolderExistsWithRetry(TransferFolderPath)
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error verifying transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath) & ": " & ex.Message)
					m_message = AppendToComment(m_message, "Error verifying transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath))
					objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try

				'Determine if dataset folder in transfer directory already exists; make directory if it doesn't exist
				' First make sure "DatasetNum" is defined
				If String.IsNullOrEmpty(m_Dataset) Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Dataset name is undefined, job " & m_jobParams.GetParam("StepParameters", "Job"))
					m_message = "Dataset name is undefined"
					objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Now create the folder if it doesn't exist
				TargetFolderPath = System.IO.Path.Combine(TransferFolderPath, m_Dataset)
				Try
					objAnalysisResults.CreateFolderWithRetry(TargetFolderPath)
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating dataset folder in transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath) & ": " & ex.Message)
					m_message = AppendToComment(m_message, "Error creating dataset folder in transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath))
					objAnalysisResults.CopyFailedResultsToArchiveFolder(SourceFolderPath)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try

				' Now append the output folder name to TargetFolderPath
				TargetFolderPath = System.IO.Path.Combine(TargetFolderPath, ResultsFolderName)

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating results folder in transfer directory: " & ex.Message)
				m_message = AppendToComment(m_message, "Error creating dataset folder in transfer directory")
				If SourceFolderPath.Length > 0 Then
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
				eResult = CopyResulsFolderRecursive(SourceFolderPath, SourceFolderPath, TargetFolderPath, _
				   objAnalysisResults, blnErrorEncountered, intFailedFileCount, _
				   intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

				If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying results folder to " & System.IO.Path.GetPathRoot(TargetFolderPath) & " : " & ex.Message)
				m_message = AppendToComment(m_message, "Error copying results folder to " & System.IO.Path.GetPathRoot(TargetFolderPath))
				blnErrorEncountered = True
			End Try

			If blnErrorEncountered Then
				strMessage = "Error copying " & intFailedFileCount.ToString & " file"
				If intFailedFileCount <> 1 Then
					strMessage &= "s"
				End If
				strMessage &= " to transfer folder"
				m_message = AppendToComment(m_message, strMessage)
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
		Private Function CopyResulsFolderRecursive(ByVal RootSourceFolderPath As String, ByVal SourceFolderPath As String, ByVal TargetFolderPath As String, _
		 ByRef objAnalysisResults As clsAnalysisResults, _
		 ByRef blnErrorEncountered As Boolean, _
		 ByRef intFailedFileCount As Integer, _
		 ByVal intRetryCount As Integer, _
		 ByVal intRetryHoldoffSeconds As Integer, _
		 ByVal blnIncreaseHoldoffOnEachRetry As Boolean) As IJobParams.CloseOutType

			Dim objSourceFolderInfo As System.IO.DirectoryInfo
			Dim objSourceFile As System.IO.FileInfo
			Dim objTargetFile As System.IO.FileInfo

			Dim htFilesToOverwrite As System.Collections.Hashtable

			Dim ResultFiles() As String
			Dim strSourceFileName As String
			Dim strTargetPath As String

			Dim strMessage As String

			Try
				htFilesToOverwrite = New System.Collections.Hashtable
				htFilesToOverwrite.Clear()

				If objAnalysisResults.FolderExistsWithRetry(TargetFolderPath) Then
					' The target folder already exists

					' Examine the files in the results folder to see if any of the files already exist in the xfer folder
					' If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)

					objSourceFolderInfo = New System.IO.DirectoryInfo(SourceFolderPath)
					For Each objSourceFile In objSourceFolderInfo.GetFiles()
						If System.IO.File.Exists(System.IO.Path.Combine(TargetFolderPath, objSourceFile.Name)) Then
							objTargetFile = New System.IO.FileInfo(System.IO.Path.Combine(TargetFolderPath, objSourceFile.Name))

							If objSourceFile.LastWriteTimeUtc > objTargetFile.LastWriteTimeUtc Then
								strMessage = "File in transfer folder on server will be overwritten by newer file in results folder: " & objSourceFile.Name & "; new file date (UTC): " & objSourceFile.LastWriteTimeUtc.ToString() & "; old file date (UTC): " & objTargetFile.LastWriteTimeUtc.ToString()
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)

								htFilesToOverwrite.Add(objSourceFile.Name.ToLower, 1)
							End If
						End If
					Next
				Else
					' Need to create the target folder
					Try
						objAnalysisResults.CreateFolderWithRetry(TargetFolderPath)
					Catch ex As Exception
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating results folder in transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath) & ": " & ex.Message)
						m_message = AppendToComment(m_message, "Error creating results folder in transfer directory, " & System.IO.Path.GetPathRoot(TargetFolderPath))
						objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End Try
				End If

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error comparing files in source folder to " & TargetFolderPath & ": " & ex.Message)
				m_message = AppendToComment(m_message, "Error comparing files in source folder to transfer directory")
				objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			' Note: Entries in ResultFiles will have full file paths, not just file names
			ResultFiles = System.IO.Directory.GetFiles(SourceFolderPath, "*.*")

			For Each FileToCopy As String In ResultFiles
				strSourceFileName = System.IO.Path.GetFileName(FileToCopy)
				strTargetPath = System.IO.Path.Combine(TargetFolderPath, strSourceFileName)

				Try
					If htFilesToOverwrite.Count > 0 AndAlso htFilesToOverwrite.Contains(strSourceFileName.ToLower) Then
						' Copy file and overwrite existing
						objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True, _
						 intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
					Else
						' Copy file only if it doesn't currently exist
						If Not System.IO.File.Exists(strTargetPath) Then
							objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True, _
							 intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
						End If
					End If
				Catch ex As Exception
					' Continue copying files; we'll fail the results at the end of this function
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, " CopyResultsFolderToServer: error copying " & System.IO.Path.GetFileName(FileToCopy) & " to " & strTargetPath & ": " & ex.Message)
					blnErrorEncountered = True
					intFailedFileCount += 1
				End Try
			Next


			' Recursively call this function for each subfolder
			' If any of the subfolders have an error, we'll continue copying, but will set blnErrorEncountered to True
			Dim eResult As IJobParams.CloseOutType
			eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

			Dim diSourceFolder As System.IO.DirectoryInfo
			Dim strTargetFolderPathCurrent As String
			diSourceFolder = New System.IO.DirectoryInfo(SourceFolderPath)

			For Each objSubFolder As System.IO.DirectoryInfo In diSourceFolder.GetDirectories()
				strTargetFolderPathCurrent = System.IO.Path.Combine(TargetFolderPath, objSubFolder.Name)

				eResult = CopyResulsFolderRecursive(RootSourceFolderPath, objSubFolder.FullName, strTargetFolderPathCurrent, _
				   objAnalysisResults, blnErrorEncountered, intFailedFileCount, _
				   intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

				If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

			Next

			Return eResult

		End Function

		''' <summary>
		''' Unzips all files in the specified Zip file
		''' Output folder is m_WorkDir
		''' </summary>
		''' <param name="ZipFilePath">File to unzip</param>
		''' <returns></returns>
		''' <remarks></remarks>
		Protected Function UnzipFile(ByVal ZipFilePath As String) As Boolean
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
		Protected Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String) As Boolean
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
		Protected Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String, ByVal FileFilter As String) As Boolean
			m_IonicZipTools.DebugLevel = m_DebugLevel
			Return m_IonicZipTools.UnzipFile(ZipFilePath, TargetDirectory, FileFilter)
		End Function

		''' <summary>
		''' Stores SourceFilePath in a zip file with the same name, but extension .zip
		''' </summary>
		''' <param name="SourceFilePath">Full path to the file to be zipped</param>
		''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
		''' <returns>True if success; false if an error</returns>
		Protected Function ZipFile(ByVal SourceFilePath As String, _
		   ByVal DeleteSourceAfterZip As Boolean) As Boolean
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
		Protected Function ZipFile(ByVal SourceFilePath As String, _
		   ByVal DeleteSourceAfterZip As Boolean, _
		   ByVal ZipFilePath As String) As Boolean
			Dim blnSuccess As Boolean
			m_IonicZipTools.DebugLevel = m_DebugLevel
			blnSuccess = m_IonicZipTools.ZipFile(SourceFilePath, DeleteSourceAfterZip, ZipFilePath)

			If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
				m_NeedToAbortProcessing = True
			End If

			Return blnSuccess

		End Function

#End Region

	End Class

End Namespace
