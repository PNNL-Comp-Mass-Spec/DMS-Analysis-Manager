'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/23/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerDeconPeakDetector
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running the Decon Peak Detector
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const DECON_PEAK_DETECTOR_EXE_NAME As String = "HammerOrDeconSimplePeakDetector.exe"

	Protected Const DECON_PEAK_DETECTOR_CONSOLE_OUTPUT As String = "DeconPeakDetector_ConsoleOutput.txt"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

#End Region

#Region "Module Variables"

	Protected mConsoleOutputErrorMsg As String

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs DeconPeakDetector tool
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter")
			End If

			' Verify that program files exist

			' Determine the path to the PeakDetector program
			Dim progLoc As String
			progLoc = DetermineProgramLocation("DeconPeakDetector", "DeconPeakDetectorProgLoc", DECON_PEAK_DETECTOR_EXE_NAME)

			If String.IsNullOrWhiteSpace(progLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the PeakDetector version info in the database
			m_message = String.Empty
			If Not StoreToolVersionInfo(progLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error determining DeconPeakDetector version"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Run DeconPeakDetector
			Dim blnSuccess = RunDeconPeakDetector(progLoc)

			If blnSuccess Then
				' Look for the DeconPeakDetector results file
				Dim peakDetectorResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & "_peaks.txt")

				Dim fiResultsFile = New FileInfo(peakDetectorResultsFilePath)

				If Not fiResultsFile.Exists Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "DeconPeakDetector results file not found: " & Path.GetFileName(peakDetectorResultsFilePath)
					End If
					blnSuccess = False
				End If
			End If

			If blnSuccess Then
				m_jobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt")
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
			m_message = "Error in DeconPeakDetectorPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Parse the DeconPeakDetector console output file to determine the DeconPeakDetector version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'

		' Started Peak Detector
		' There are 6695 MS1 scans
		' Using Decon Peak Detector
		' 
		' Peak creation progress: 0%
		' Peak creation progress: 1%
		' Peak creation progress: 2%
		' Peak creation progress: 2%
		' Peak creation progress: 3%
		' Peak creation progress: 4%

		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow
		Static reProgress As Regex = New Regex("Peak creation progress: (?<Progress>\d+)%", RegexOptions.Compiled)

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
			Dim peakDetectProgress As Integer

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srInFile.Peek() >= 0
					strLineIn = srInFile.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						Dim reMatch As Match = reProgress.match(strLineIn)

						If reMatch.Success Then
							peakDetectProgress = Int32.Parse(reMatch.Groups("Progress").Value)
						End If

					End If
				Loop

			End Using

			Dim sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, peakDetectProgress, 100)

			If m_progress < sngActualProgress Then
				m_progress = sngActualProgress

				If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = DateTime.UtcNow
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function RunDeconPeakDetector(ByVal strPeakDetectorProgLoc As String) As Boolean

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		Dim peakDetectorParamFileName = m_jobParams.GetJobParameter("PeakDetectorParamFile", "")
		Dim paramFilePath = Path.Combine(m_WorkDir, peakDetectorParamFileName)

		mConsoleOutputErrorMsg = String.Empty

		Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
		Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

		If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
			m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
		Else
			m_message = "The DeconPeakDetector presently only supports Thermo .Raw files"
			Return False
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running DeconPeakDetector")

		'Set up and execute a program runner to run the Peak Detector
		CmdStr = m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION
		CmdStr &= " /P:" & PossiblyQuotePath(paramFilePath)
		CmdStr &= " /O:" & PossiblyQuotePath(m_WorkDir)

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strPeakDetectorProgLoc & " " & CmdStr)

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		blnSuccess = CmdRunner.RunProgram(strPeakDetectorProgLoc, CmdStr, "PeakDetector", True)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running DeconPeakDetector"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PeakDetector returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to PeakDetector failed (but exit code is 0)")
			End If

			Return False
		End If

		m_progress = PROGRESS_PCT_COMPLETE
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconPeakDetector Search Complete")
		End If

		Return True

	End Function
	
	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strPeakDetectorPath As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the DeconConsole application
		Dim fiPeakDetector As FileInfo = New FileInfo(strPeakDetectorPath)

		Dim blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiPeakDetector.FullName)
		If Not blnSuccess Then Return False

		Dim dllPath = Path.Combine(fiPeakDetector.Directory.FullName, "SimplePeakDetectorEngine.dll")
		MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, dllPath)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(strPeakDetectorPath))
		ioToolFiles.Add(New FileInfo(dllPath))

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

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region

End Class
