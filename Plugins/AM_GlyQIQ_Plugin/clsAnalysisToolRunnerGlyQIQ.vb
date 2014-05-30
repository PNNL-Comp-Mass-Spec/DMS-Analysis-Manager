' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerGlyQIQ
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running the GlyQ-IQ
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const GLYQ_IQ_CONSOLE_OUTPUT As String = "GlyQ-IQ_ConsoleOutput.txt"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

#End Region

#Region "Module Variables"

	Protected mConsoleOutputErrorMsg As String

	Protected mGlyQIQApplicationFilesFolderPath As String
	Protected mBatchFileLauncher As FileInfo

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs GlyQ-IQ
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim result As IJobParams.CloseOutType

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerGlyQIQ.RunTool(): Enter")
			End If

			' Lookup the HPC options
			Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

			mGlyQIQApplicationFilesFolderPath = String.Empty

			' Determine the path to the GlyQIQ application files folder
			mGlyQIQApplicationFilesFolderPath = Path.Combine(udtHPCOptions.SharePath, "GlyQ_ApplicationFiles")

			' Make sure the GlyQ-IQ application files are up-to-date
			If Not SynchronizeGlyQIQApplicationFiles() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the GlyQ-IQ version info in the database
			m_message = String.Empty
			If Not StoreToolVersionInfo() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error determining DeconPeakDetector version"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Setup the parameter files
			If Not CreateParameterFiles(udtHPCOptions) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Run GlyQ-IQ
			Dim blnSuccess = RunGlyQIQ()

			If blnSuccess Then
				' Copy the results to the local computer
				blnSuccess = RetrieveGlyQIQResults(udtHPCOptions)
			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		'2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If Not blnSuccess Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MakeResultsFolder()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'MakeResultsFolder handles posting to local log, so set database error message and exit
				m_message = "Error making results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = MoveResultFiles()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				m_message = "Error moving files into results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			result = CopyResultsFolderToServer()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			m_message = "Error in GlyQIQ->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function CreateParameterFiles(ByVal udtHpcOptionsType As clsAnalysisResources.udtHPCOptionsType) As Boolean

		Try
			Dim strDatasetNameTruncated = String.Copy(m_Dataset)
			If strDatasetNameTruncated.Length > 25 Then
				' Shorten the dataset name to prevent paths from getting too long
				strDatasetNameTruncated = strDatasetNameTruncated.Substring(0, 25)

				' Possibly shorten a bit more if an underscore or dash is present between char index 15 and 25
				Dim chSepChars = New Char() {"_"c, "-"c}

				Dim charIndex = strDatasetNameTruncated.IndexOfAny(chSepChars, 15)
				If charIndex > 1 Then
					strDatasetNameTruncated = strDatasetNameTruncated.Substring(0, charIndex)
				End If

			End If

			' Construct the launcher file path, e.g.
			' \\winhpcfs\Projects\DMS\DMS_Work_Dir\Pub-61-3\0x_Launch_DatasetName_201405291749.bat

			Dim timeStamp As String = DateTime.Now.ToString("yyyyMMddhhmm")
			Dim batchFileLauncherPath As String = Path.Combine(udtHpcOptionsType.WorkDirPath, "0x_Launch_" & strDatasetNameTruncated & "_" & timeStamp & ".bat")
			mBatchFileLauncher = New FileInfo(batchFileLauncherPath)

			Return True

		Catch ex As Exception
			m_message = "Exception retrieving the results files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

	End Function

	Private Function RetrieveGlyQIQResults(ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

		Try
			Dim resultsFolderSource As String = Path.Combine(udtHPCOptions.WorkDirPath, "Results")
			Dim resultsFolderTarget = Path.Combine(m_WorkDir, "Results")

			Throw New NotImplementedException("Need to copy the important results files locally")

			Dim success = SynchronizeFolders(resultsFolderSource, resultsFolderTarget)

			If Not success AndAlso String.IsNullOrEmpty(m_message) Then
				m_message = "SynchronizeFolders returned false"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

			Return success

		Catch ex As Exception
			m_message = "Exception retrieving the results files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

	End Function


	''' <summary>
	''' Parse the DeconPeakDetector console output file to determine the DeconPeakDetector version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'

		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow
		
		Try
			If Not File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If

			Dim strLineIn As String

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srInFile.Peek() >= 0
					strLineIn = srInFile.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then


					End If
				Loop

			End Using

			'Dim sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, peakDetectProgress, 100)

			'If m_progress < sngActualProgress Then
			'	m_progress = sngActualProgress

			'	If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
			'		dtLastProgressWriteTime = DateTime.UtcNow
			'		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
			'	End If
			'End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function RunGlyQIQ() As Boolean

		Dim blnSuccess As Boolean

		Dim paramFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"))

		mConsoleOutputErrorMsg = String.Empty

		Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
		Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

		If eRawDataType <> clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
			m_message = "The DeconPeakDetector presently only supports Thermo .Raw files"
			Return False
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running GlyQ-IQ")

		' Set up and execute a program runner to run the batch file that launches GlyQ-IQ
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Launching GlyQ-IQ using " & mBatchFileLauncher.FullName)

		CmdRunner = New clsRunDosProgram(mBatchFileLauncher.Directory.FullName)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, GLYQ_IQ_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		Dim CmdStr As String = String.Empty
		blnSuccess = CmdRunner.RunProgram(mBatchFileLauncher.FullName, CmdStr, "GlyQ-IQ", True)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running GlyQ-IQ"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "GlyQ-IQ returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to GlyQ-IQ failed (but exit code is 0)")
			End If

			Return False
		End If

		m_progress = PROGRESS_PCT_COMPLETE
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GlyQ-IQ Analysis Complete")
		End If

		Return True

	End Function

	Protected Function SynchronizeGlyQIQApplicationFiles() As Boolean

		Try
			Const appFolderSource As String = "\\pnl\projects\OmicsSW\DMS_Programs\GlyQ-IQ\ApplicationFiles"
			Dim appFolderTarget = mGlyQIQApplicationFilesFolderPath

			Dim success = SynchronizeFolders(appFolderSource, appFolderTarget)

			If Not success AndAlso String.IsNullOrEmpty(m_message) Then
				m_message = "SynchronizeFolders returned false"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			End If

			Return success

		Catch ex As Exception
			m_message = "Exception synchronizing the GlyQ-IQ application files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

	End Function
	
	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		Dim applicationFolderPath = Path.Combine(mGlyQIQApplicationFilesFolderPath, "GlyQ-IQ_Application\Release")
		Dim consoleAppPath = Path.Combine(applicationFolderPath, "IQGlyQ_Console.exe")

		' Lookup the version of the GlyQ application
		Dim blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, consoleAppPath)
		If Not blnSuccess Then Return False

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(consoleAppPath))

		Dim lstDLLs = New List(Of String)
		lstDLLs.Add("IQGlyQ.dll")
		lstDLLs.Add("IQ2.dll")
		lstDLLs.Add("Run32.dll")

		For Each dllName In lstDLLs
			ioToolFiles.Add(New FileInfo(Path.Combine(applicationFolderPath, dllName)))
		Next

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
		m_progress = sngPercentComplete
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
	End Sub

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, GLYQ_IQ_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region

End Class
