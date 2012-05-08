Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerIDPicker
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running IDPicker
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const PEPXML_CONSOLE_OUTPUT As String = "PepXML_ConsoleOutput.txt"

	Protected Const IPD_Qonvert_CONSOLE_OUTPUT As String = "IDPicker_Qonvert_ConsoleOutput.txt"
	Protected Const IPD_Assemble_CONSOLE_OUTPUT As String = "IDPicker_Assemble_ConsoleOutput.txt"
	Protected Const IPD_Report_CONSOLE_OUTPUT As String = "IDPicker_Report_ConsoleOutput.txt"

	Protected Const IDPicker_Qonvert As String = "idpQonvert.exe"
	Protected Const IDPicker_Assemble As String = "idpAssemble.exe"
	Protected Const IDPicker_Report As String = "idpReport.exe"
	Protected Const IDPicker_GUI As String = "IdPickerGui.exe"

	Protected Const PEPTIDE_LIST_TO_XML_EXE As String = "PeptideListToXML.exe"

	Protected Const PROGRESS_PCT_IDPicker_STARTING As Single = 1
	Protected Const PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES As Single = 5
	Protected Const PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE As Single = 10
	Protected Const PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert As Single = 20
	Protected Const PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble As Single = 80
	Protected Const PROGRESS_PCT_IDPicker_RUNNING_IDPReport As Single = 90
	Protected Const PROGRESS_PCT_IDPicker_COMPLETE As Single = 95
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected mConsoleOutputErrorMsg As String
	Protected mDatasetID As Integer = 0

	Protected mPeptideListToXMLExePath As String = String.Empty
	Protected mIDPickerProgramFolder As String = String.Empty
	Protected mPepXMLFilePath As String = String.Empty

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"
	
#End Region

#Region "Methods"

	''' <summary>
	''' Runs PepXML converter and IDPicker tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim OrgDbDir As String
		Dim strFASTAFilePath As String

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False

		Dim blnSuccess As Boolean
		
		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIDPicker.RunTool(): Enter")
			End If

			m_progress = PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES

			' Determine the path to the IDPicker program (idpQonvert); folder will also contain idpAssemble.exe and idpReport.exe
			Dim progLocQonvert As String
			progLocQonvert = DetermineProgramLocation("IDPicker", "IDPickerProgLoc", IDPicker_Qonvert)

			If String.IsNullOrWhiteSpace(progLocQonvert) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the IDPicker version info in the database
			' This function updates mPeptideListToXMLExePath and mIDPickerProgramFolder

			If Not StoreToolVersionInfo(progLocQonvert) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining IDPicker version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			mConsoleOutputErrorMsg = String.Empty

			' Define the path to the fasta file
			OrgDbDir = m_mgrParams.GetParam("orgdbdir")
			strFASTAFilePath = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

			Dim fiFastaFile As System.IO.FileInfo
			fiFastaFile = New System.IO.FileInfo(strFASTAFilePath)

			If Not fiFastaFile.Exists Then
				' Fasta file not found
				m_message = "Fasta file not found: " & fiFastaFile.Name
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If

			blnSuccess = CreatePepXMLFile(fiFastaFile.FullName)
			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then m_message = "Error creating PepXML file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating PepXML file for job " & m_JobNum)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			blnSuccess = RunQonvert()
			If Not blnSuccess Then blnProcessingError = True

			If blnSuccess Then
				blnSuccess = RunAssemble()
				If Not blnSuccess Then blnProcessingError = True
			End If

			If blnSuccess Then
				blnSuccess = RunReport()
				If Not blnSuccess Then blnProcessingError = True
			End If
	
			If Not blnProcessingError Then
				' Zip the PepXML file
				ZipPepXMLFile()
			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = System.DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			'Make sure objects are released
			System.Threading.Thread.Sleep(2000)		   '2 second delay
			GC.Collect()
			GC.WaitForPendingFinalizers()

			If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				CopyFailedResultsToArchiveFolder()
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
				Return result
			End If

		Catch ex As Exception
			m_message = "Exception in IDPickerPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		' Make the results folder
		result = MakeResultsFolder()
		If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Move the result files into the result folder
			result = MoveResultFiles()
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Move was a success; update strFolderPathToArchive
				strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	Protected Function CreatePepXMLFile(ByVal strFastaFilePath As String) As Boolean

		Dim eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType

		Dim strParamFileName As String
		Dim strResultType As String
		Dim strSynFilePath As String

		Dim iHitsPerSpectrum As Integer

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		'Set up and execute a program runner to run PeptideListToXML

		strResultType = m_jobParams.GetParam("ResultType")

		eResultType = PHRPReader.clsPHRPReader.GetPeptideHitResultType(strResultType)
		strParamFileName = m_jobParams.GetParam("ParmFileName")

		strSynFilePath = PHRPReader.clsPHRPReader.GetPHRPSynopsisFileName(eResultType, m_Dataset)

		mPepXMLFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".pepXML")
		iHitsPerSpectrum = m_jobParams.GetJobParameter("PepXMLHitsPerSpectrum", 3)

		CmdStr = strSynFilePath & " /E:" & strParamFileName & " /F:" & strFastaFilePath & " /H:" & iHitsPerSpectrum
		m_progress = PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE

		blnSuccess = RunProgramWork("PeptideListToXML", mPeptideListToXMLExePath, CmdStr, PEPXML_CONSOLE_OUTPUT)

		If blnSuccess Then
			' Optional: Parse the console output file to check for errors
			' System.Threading.Thread.Sleep(250)
			' ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)
			'If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			'	clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			'End If

			' Make sure a .pepXML file was created
			If Not System.IO.File.Exists(mPepXMLFilePath) Then
				m_message = "Error creating .pepXML file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
				blnSuccess = False
			End If
		End If
	
		Return blnSuccess

	End Function

	'' <summary>
	'' Parse the IDPicker console output file to track progress
	'' </summary>
	'' <param name="strConsoleOutputFilePath"></param>
	'' <remarks></remarks>
	''Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

	''	' Example Console output:
	''	'
	''	' ....

	''	Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow

	''	' This RegEx matches lines in the form:
	''	' 2/13/2012 07:15:42 PM - Searching for Text Files!...
	''	Static reMatchTimeStamp As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("^\d+/\d+/\d+ \d+:\d+:\d+ [AP]M - ", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

	''	Dim reMatch As System.Text.RegularExpressions.Match

	''	Try

	''		If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
	''			If m_DebugLevel >= 4 Then
	''				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
	''			End If

	''			Exit Sub
	''		End If

	''		If m_DebugLevel >= 4 Then
	''			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
	''		End If


	''		Dim srInFile As System.IO.StreamReader
	''		Dim strLineIn As String
	''		Dim intLinesRead As Integer

	''		Dim sngEffectiveProgress As Single
	''		sngEffectiveProgress = PROGRESS_PCT_IDPicker_STARTING

	''		srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

	''		intLinesRead = 0
	''		Do While srInFile.Peek() >= 0
	''			strLineIn = srInFile.ReadLine()
	''			intLinesRead += 1

	''			If Not String.IsNullOrWhiteSpace(strLineIn) Then

	''				' Remove the timestamp from the start of the line (if present)
	''				reMatch = reMatchTimeStamp.Match(strLineIn)
	''				If reMatch.Success Then
	''					strLineIn = strLineIn.Substring(reMatch.Length)
	''				End If

	''				' Update progress if the line starts with one of the expected phrases
	''				If strLineIn.StartsWith("Searching for Text Files") Then
	''					If sngEffectiveProgress < PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES Then
	''						sngEffectiveProgress = PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES
	''					End If

	''				ElseIf Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
	''					If strLineIn.ToLower.Contains("error") Then
	''						mConsoleOutputErrorMsg &= "; " & strLineIn
	''					End If
	''				End If
	''			End If
	''		Loop

	''		srInFile.Close()

	''		If m_progress < sngEffectiveProgress Then
	''			m_progress = sngEffectiveProgress

	''			If m_DebugLevel >= 3 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
	''				dtLastProgressWriteTime = System.DateTime.UtcNow
	''				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
	''			End If
	''		End If

	''	Catch ex As Exception
	''		' Ignore errors here
	''		If m_DebugLevel >= 2 Then
	''			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
	''		End If
	''	End Try

	''End Sub

	Protected Function RunAssemble() As Boolean
		Dim progLoc As String
		Dim CmdStr As String
		Dim blnSuccess As Boolean

		progLoc = System.IO.Path.Combine(mIDPickerProgramFolder, IDPicker_Assemble)
		CmdStr = "???"

		m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble

		blnSuccess = RunProgramWork("IDPAssemble", progLoc, CmdStr, IPD_Assemble_CONSOLE_OUTPUT)

		If blnSuccess Then
			' Optional: Parse the console output file to check for errors			

			' Make sure the output file was created
			If Not System.IO.File.Exists("???") Then
				m_message = "Error creating ??? file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
				blnSuccess = False
			End If
		End If

		Return blnSuccess

	End Function

	Protected Function RunQonvert() As Boolean
		Dim progLoc As String
		Dim CmdStr As String
		Dim blnSuccess As Boolean

		progLoc = System.IO.Path.Combine(mIDPickerProgramFolder, IDPicker_Qonvert)
		CmdStr = "???"

		m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert

		blnSuccess = RunProgramWork("IDPQonvert", progLoc, CmdStr, IPD_Qonvert_CONSOLE_OUTPUT)

		If blnSuccess Then
			' Optional: Parse the console output file to check for errors			

			' Make sure the output file was created
			If Not System.IO.File.Exists("???") Then
				m_message = "Error creating ??? file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
				blnSuccess = False
			End If
		End If

		Return blnSuccess

	End Function

	Protected Function RunReport() As Boolean
		Dim progLoc As String
		Dim CmdStr As String
		Dim blnSuccess As Boolean

		progLoc = System.IO.Path.Combine(mIDPickerProgramFolder, IDPicker_Report)
		CmdStr = "???"

		m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPReport

		blnSuccess = RunProgramWork("IDPReport", progLoc, CmdStr, IPD_Report_CONSOLE_OUTPUT)

		If blnSuccess Then
			' Optional: Parse the console output file to check for errors			

			' Make sure the output file was created
			If Not System.IO.File.Exists("???") Then
				m_message = "Error creating ??? file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
				blnSuccess = False
			End If
		End If

		Return blnSuccess

	End Function

	Protected Function RunProgramWork(ByVal strProgramDescription As String, ByVal strExePath As String, ByVal CmdStr As String, ByVal strConsoleOutputFileName As String) As Boolean

		Dim blnSuccess As Boolean

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mPeptideListToXMLExePath & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True
			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, strConsoleOutputFileName)
		End With

		blnSuccess = CmdRunner.RunProgram(mPeptideListToXMLExePath, CmdStr, strProgramDescription, True)

		If Not CmdRunner.WriteConsoleOutputToFile Then
			' Write the console output to a text file
			System.Threading.Thread.Sleep(250)

			Dim swConsoleOutputfile As New System.IO.StreamWriter(New System.IO.FileStream(CmdRunner.ConsoleOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
			swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
			swConsoleOutputfile.Close()
		End If

		If Not blnSuccess Then
			m_message = "Error running " & strProgramDescription
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProgramDescription & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strProgramDescription & " failed (but exit code is 0)")
			End If

		Else
			m_StatusTools.UpdateAndWrite(m_progress)
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProgramDescription & " Complete")
			End If
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strIDPickerProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim strExePath As String

		Dim ioIDPicker As System.IO.FileInfo
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioIDPicker = New System.IO.FileInfo(strIDPickerProgLoc)
		If Not ioIDPicker.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		mIDPickerProgramFolder = ioIDPicker.DirectoryName

		' We will store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)

		' Determine the path to the PeptideListToXML.exe, then determine it's version number
		mPeptideListToXMLExePath = DetermineProgramLocation("PeptideListToXML", "PeptideListToXMLProgLoc", PEPTIDE_LIST_TO_XML_EXE)
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, mPeptideListToXMLExePath)
		ioToolFiles.Add(New System.IO.FileInfo(mPeptideListToXMLExePath))

		' Lookup the version of idpAssemble.exe (which is a .NET app; cannot use idpQonvert.exe since it is a C++ app)
		strExePath = System.IO.Path.Combine(ioIDPicker.Directory.Name, IDPicker_Assemble)
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioIDPicker.FullName)
		ioToolFiles.Add(New System.IO.FileInfo(strExePath))
		If Not blnSuccess Then Return False

		' Lookup the version of idpReport.exe
		strExePath = System.IO.Path.Combine(ioIDPicker.Directory.Name, IDPicker_Report)
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioIDPicker.FullName)
		ioToolFiles.Add(New System.IO.FileInfo(strExePath))

		' Also include idpQonvert.exe in ioToolFiles
		strExePath = System.IO.Path.Combine(ioIDPicker.Directory.Name, IDPicker_Qonvert)
		ioToolFiles.Add(New System.IO.FileInfo(strExePath))

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

	''' <summary>
	''' Zips PepXML File
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function ZipPepXMLFile() As IJobParams.CloseOutType

		Try

			If Not MyBase.ZipFile(mPepXMLFilePath, False) Then
				Dim Msg As String = "Error zipping PepXML file, job " & m_JobNum
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				m_message = clsGlobal.AppendToComment(m_message, "Error zipping PepXML file")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Add the .pepXML file to .FilesToDelete since we only want to keep the Zipped version
			m_jobParams.AddResultFileToSkip(System.IO.Path.GetFileName(mPepXMLFilePath))

		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerIDPicker.ZipPepXMLFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Error zipping PepXML file")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow
		Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			UpdateStatusRunning(m_progress)
		End If

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			'' ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, IDPicker_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region

End Class
