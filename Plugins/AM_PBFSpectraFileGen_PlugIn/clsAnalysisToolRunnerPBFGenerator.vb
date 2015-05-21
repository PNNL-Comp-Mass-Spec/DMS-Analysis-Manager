
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/16/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerPBFGenerator
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for creation PBF (PNNL Binary Format) files using PBFGen
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const PBF_GEN_CONSOLE_OUTPUT As String = "PBFGen_ConsoleOutput.txt"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

#End Region

#Region "Module Variables"

	Protected mConsoleOutputErrorMsg As String

	Private mInstrumentFileSizeBytes As Int64
	Protected mResultsFilePath As String
	Protected mPbfFormatVersion As String

	Protected mMSXmlCacheFolder As DirectoryInfo

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"

	''' <summary>
	''' Generates a PBF file for the dataset
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPBFGenerator.RunTool(): Enter")
			End If

			' Determine the path to the PbfGen program
			Dim progLoc As String
			progLoc = DetermineProgramLocation("PBF_Gen", "PbfGenProgLoc", "PbfGen.exe")

			If String.IsNullOrWhiteSpace(progLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the PBFGen version info in the database
			If Not StoreToolVersionInfo(progLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				LogError("Error determining PBFGen version")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
			mMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

			If Not mMSXmlCacheFolder.Exists Then
				LogError("MSXmlCache folder not found: " & msXMLCacheFolderPath)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Create the PBF file
			Dim blnSuccess = StartPBFFileCreation(progLoc)

			If blnSuccess Then
				' Look for the results file

				Dim fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_PBF_EXTENSION))

				If fiResultsFile.Exists Then
					' Success; validate mPbfFormatVersion
					If String.IsNullOrEmpty(mPbfFormatVersion) Then mPbfFormatVersion = String.Empty

					Select Case mPbfFormatVersion
						Case "150601"
							' This version is created by Pbf_Gen.exe v1.0.5311
							' Make sure the output folder starts with PBF_Gen_1_191
							' (which will be the case if the settings file has <item key="PbfFormatVersion" value="110569"/>)
							If Not m_ResFolderName.StartsWith("PBF_Gen_1_191") Then
								blnSuccess = False
							End If
						Case "150604"
							' This version is created by Pbf_Gen.exe v1.0.5367
							' Make sure the output folder starts with PBF_Gen_1_193
							' (which will be the case if the settings file has <item key="PbfFormatVersion" value="150604"/>)
							If Not m_ResFolderName.StartsWith("PBF_Gen_1_193") Then
								blnSuccess = False
							End If
						Case Else
							blnSuccess = False
					End Select

					If Not blnSuccess Then
						LogError(
						  "Unrecognized PbfFormatVersion.  Either create a new Settings file with PbfFormatVersion " & mPbfFormatVersion &
						  " or update the version listed in the current, default settings file;" &
						  " next, delete the job from the DMS_Pipeline database then update the job to use the new settings file (or reset the job)")
					Else

						' Copy the .pbf file to the MSXML cache
						Dim remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiResultsFile.FullName, purgeOldFilesIfNeeded:=True)

						If String.IsNullOrEmpty(remoteCachefilePath) Then
							If String.IsNullOrEmpty(m_message) Then
								LogError("CopyFileToServerCache returned false for " & fiResultsFile.Name)
							End If
							blnSuccess = False
						End If

						' Create the _CacheInfo.txt file
						Dim cacheInfoFilePath = fiResultsFile.FullName & "_CacheInfo.txt"
						Using swOutFile = New StreamWriter(New FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
							swOutFile.WriteLine(remoteCachefilePath)
						End Using

						m_jobParams.AddResultFileToSkip(fiResultsFile.Name)

					End If

				Else
					If String.IsNullOrEmpty(m_message) Then
						LogError("PBF_Gen results file not found: " & fiResultsFile.Name)
						blnSuccess = False
					End If
				End If
			End If

			m_progress = PROGRESS_PCT_COMPLETE

			' Stop the job timer
			m_StopTime = DateTime.UtcNow

			' Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			' Make sure objects are released
			Threading.Thread.Sleep(500)        ' 500 msec delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			If Not blnSuccess Then
				' Move the source files and any results to the Failed Job folder
				' Useful for debugging problems
				CopyFailedResultsToArchiveFolder()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt")

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
			m_message = "Error in clsAnalysisToolRunnerPBFGenerator->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

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

		m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_PBF_EXTENSION)
		m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)

		' Make the results folder
		result = MakeResultsFolder()
		If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Move the result files into the result folder
			result = MoveResultFiles()
			If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				' Move was a success; update strFolderPathToArchive
				strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	''' <summary>
	''' Computes a crude estimate of % complete based on the input dataset file size and the file size of the result file
	''' This will always vastly underestimate the progress since the PBF file is always smaller than the .raw file
	''' Furthermore, it looks like all of the data in the .raw file is cached in memory and the .PBF file is not created until the very end
	'''  and thus this progress estimation is useless
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function EstimatePBFProgress() As Single

		Try

			Dim fiResults = New FileInfo(mResultsFilePath)

			If fiResults.Exists AndAlso mInstrumentFileSizeBytes > 0 Then
				Dim percentComplete As Single = fiResults.Length / CSng(mInstrumentFileSizeBytes) * 100
				Return percentComplete
			End If

		Catch ex As Exception
			' Ignore errors here
		End Try

		Return 0

	End Function

	''' <summary>
	''' Parse the PBFGen console output file to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'
		' Creating E:\DMS_WorkDir\Synocho_L2_1.pbf from E:\DMS_WorkDir\Synocho_L2_1.raw
		' PbfFormatVersion: 150601

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

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Do While srInFile.Peek() >= 0
					Dim strLineIn = srInFile.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						Dim strLineInLCase = strLineIn.ToLower()

						If strLineInLCase.StartsWith("error:") OrElse strLineInLCase.Contains("unhandled exception") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running PBFGen:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
							Continue Do						
						End If

						If strLineIn.StartsWith("PbfFormatVersion:") Then
							' Parse out the version number
							Dim strVersion = strLineIn.Substring("PbfFormatVersion:".Length).Trim()

							mPbfFormatVersion = strVersion
						End If

					End If
				Loop

			End Using

			Dim progressComplete = EstimatePBFProgress()

			If m_progress < progressComplete Then
				m_progress = progressComplete
            End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Protected Function StartPBFFileCreation(ByVal progLoc As String) As Boolean

		Dim CmdStr As String
		Dim blnSuccess As Boolean

		mConsoleOutputErrorMsg = String.Empty

		Dim rawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
		Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

		If eRawDataType <> clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
			LogError("PBF generation presently only supports Thermo .Raw files")
			Return False
		End If

		Dim rawFilePath As String = String.Empty
		rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)

		' Cache the size of the instrument data file
		Dim fiInstrumentFile = New FileInfo(rawFilePath)
		If Not fiInstrumentFile.Exists Then
			LogError("Instrument data not found: " & rawFilePath)
			Return False
		End If

		mInstrumentFileSizeBytes = fiInstrumentFile.Length
		mPbfFormatVersion = String.Empty

		' Cache the full path to the expected output file
		mResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_PBF_EXTENSION)

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PBFGen to create the PBF file")

		'Set up and execute a program runner to run PBFGen
		CmdStr = " -s " & rawFilePath
		' CmdStr &= " -o " & m_WorkDir

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, PBF_GEN_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		blnSuccess = CmdRunner.RunProgram(progLoc, CmdStr, "PbfGen", True)

		If Not CmdRunner.WriteConsoleOutputToFile Then
			' Write the console output to a text file
			Threading.Thread.Sleep(250)

			Dim swConsoleOutputfile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
			swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
			swConsoleOutputfile.Close()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		' Parse the console output file one more time to check for errors and to update mPbfFormatVersion
		Threading.Thread.Sleep(250)
		ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running PBFGen to create a PBF file"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PBFGen returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to PBFGen failed (but exit code is 0)")
			End If

			Return False

		End If

		m_progress = PROGRESS_PCT_COMPLETE
		m_StatusTools.UpdateAndWrite(m_progress)
		If m_DebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PBF Generation Complete")
		End If

		Return True

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		Dim fiProgram = New FileInfo(strProgLoc)
		If Not fiProgram.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), False)
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		' Lookup the version of the .NET application
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiProgram.FullName)
		If Not blnSuccess Then Return False


		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles = New List(Of FileInfo)
		ioToolFiles.Add(fiProgram)

		ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "InformedProteomics.Backend.dll")))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, False)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

		Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

		' Parse the console output file and estimate progress every 15 seconds
		If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = DateTime.UtcNow

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, PBF_GEN_CONSOLE_OUTPUT))

            LogProgress("PBFGenerator")
		End If

	End Sub

#End Region

End Class
