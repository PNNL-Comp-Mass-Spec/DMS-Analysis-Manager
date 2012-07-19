Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerLipidMapSearch
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running LipidMapSearch
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const LIPID_MAPS_DB_FILENAME_PREFIX As String = "LipidMapsDB_"
	Protected Const LIPID_MAPS_STALE_DB_AGE_DAYS As Integer = 5

	Protected Const LIPID_TOOLS_RESULT_FILE_PREFIX As String = "LipidMap_"
	Protected Const LIPID_TOOLS_CONSOLE_OUTPUT As String = "LipidTools_ConsoleOutput.txt"

	Protected Const PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE As Integer = 5
	Protected Const PROGRESS_PCT_LIPID_TOOLS_STARTING As Integer = 10

	Protected Const PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE As Integer = 11
	Protected Const PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA As Integer = 12
	Protected Const PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA As Integer = 13
	Protected Const PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES As Integer = 15
	Protected Const PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES As Integer = 50
	Protected Const PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES As Integer = 90
	Protected Const PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB As Integer = 92
	Protected Const PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS As Integer = 94
	Protected Const PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA As Integer = 96

	Protected Const PROGRESS_PCT_LIPID_TOOLS_COMPLETE As Integer = 98
	Protected Const PROGRESS_PCT_COMPLETE As Integer = 99

	Protected mConsoleOutputErrorMsg As String
	Protected mDatasetID As Integer = 0

	Protected mLipidToolsProgLoc As String
	Protected mConsoleOutputProgressMap As System.Collections.Generic.Dictionary(Of String, Integer)

	Protected mDownloadingLipidMapsDatabase As Boolean
	Protected mLipidMapsDBFilename As String = String.Empty

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"
#End Region

#Region "Methods"
	''' <summary>
	''' Runs LipidMapSearch tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim CmdStr As String

		Dim result As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False

		Dim blnSuccess As Boolean

		Dim strParameterFileName As String = String.Empty
		Dim strParameterFilePath As String = String.Empty

		Dim strDataset2 As String
		Dim strFilePath As String

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerLipidMapSearch.RunTool(): Enter")
			End If

			' Determine the path to the LipidTools program
			mLipidToolsProgLoc = DetermineProgramLocation("LipidTools", "LipidToolsProgLoc", "LipidTools.exe")

			If String.IsNullOrWhiteSpace(mLipidToolsProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the LipidTools version info in the database
			If Not StoreToolVersionInfo(mLipidToolsProgLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining LipidTools version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Obtain the LipidMaps.txt database
			m_progress = PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE

			If Not GetLipidMapsDatabase() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since GetLipidMapsDatabase returned false")
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error obtaining the LipidMaps database"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileToSkip(mLipidMapsDBFilename)				' Don't keep the Lipid Maps Database since we keep the permanent copy on Gigasax

			mConsoleOutputErrorMsg = String.Empty

			' The parameter file name specifies the values to pass to LipidTools.exe at the command line
			strParameterFileName = m_jobParams.GetParam("parmFileName")
			strParameterFilePath = System.IO.Path.Combine(m_WorkDir, strParameterFileName)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running LipidTools")


			'Set up and execute a program runner to run LipidTools
			CmdStr = " -db " & PossiblyQuotePath(System.IO.Path.Combine(m_WorkDir, mLipidMapsDBFilename)) & " -NoDBUpdate"
			CmdStr &= " -rp " & PossiblyQuotePath(System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION))	' Positive-mode .Raw file

			strFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX)
			If System.IO.File.Exists(strFilePath) Then
				CmdStr &= " -pp " & PossiblyQuotePath(strFilePath)					' Positive-mode peaks.txt file
			End If

			strDataset2 = m_jobParams.GetParam("JobParameters", "SourceJob2Dataset")
			If Not String.IsNullOrEmpty(strDataset2) Then
				CmdStr &= " -rn " & PossiblyQuotePath(System.IO.Path.Combine(m_WorkDir, strDataset2 & clsAnalysisResources.DOT_RAW_EXTENSION))	' Negative-mode .Raw file

				strFilePath = System.IO.Path.Combine(m_WorkDir, strDataset2 & clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX)
				If System.IO.File.Exists(strFilePath) Then
					CmdStr &= " -pn " & PossiblyQuotePath(strFilePath)					' Negative-mode peaks.txt file
				End If
			End If

			' Append the remaining parameters
			CmdStr &= ParseLipidMapSearchParameterFile(strParameterFilePath)

			CmdStr &= " -o " & PossiblyQuotePath(System.IO.Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX))			' Folder and prefix text for output files

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mLipidToolsProgLoc & CmdStr)
			End If

			CmdRunner = New clsRunDosProgram(m_WorkDir)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True
				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT)
			End With

			m_progress = PROGRESS_PCT_LIPID_TOOLS_STARTING

			blnSuccess = CmdRunner.RunProgram(mLipidToolsProgLoc, CmdStr, "LipidTools", True)

			If Not CmdRunner.WriteConsoleOutputToFile Then
				' Write the console output to a text file
				System.Threading.Thread.Sleep(250)

				Dim swConsoleOutputfile As New System.IO.StreamWriter(New System.IO.FileStream(CmdRunner.ConsoleOutputFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
				swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
				swConsoleOutputfile.Close()
			End If

			' Parse the console output file one more time to check for errors
			System.Threading.Thread.Sleep(250)
			ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

			If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
			End If

			' Append a line to the console output file listing the name of the LipidMapsDB that we used
			Using swConsoleOutputFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(CmdRunner.ConsoleOutputFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))
				swConsoleOutputFile.WriteLine("LipidMapsDB Name: " & mLipidMapsDBFilename)
				swConsoleOutputFile.WriteLine("LipidMapsDB Hash: " & clsGlobal.ComputeFileHashSha1(System.IO.Path.Combine(m_WorkDir, mLipidMapsDBFilename)))
			End Using

			' Update the evaluation message to include the lipid maps DB filename
			' This message will appear in Evaluation_Message column of T_Job_Steps
			m_EvalMessage = String.Copy(mLipidMapsDBFilename)

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running LipidTools"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "LipidTools returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to LipidTools failed (but exit code is 0)")
				End If

				blnProcessingError = True

			Else
				m_progress = PROGRESS_PCT_LIPID_TOOLS_COMPLETE
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "LipidTools Search Complete")
				End If
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
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			' Zip up the text files that contain the data behind the plots
			' In addition, rename file LipidMap_results.xlsx
			If Not PostProcessLipidToolsResults() Then				
				blnProcessingError = True
			End If

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
			m_message = "Exception in LipidMapSearchPlugin->RunTool"
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

	''' <summary>
	''' Downloads the latest version of the LipidMaps database
	''' </summary>
	''' <param name="diLipidMapsDBFolder">The folder to store the Lipid Maps DB file</param>
	''' <param name="strNewestLipidMapsDBFileName">The name of the newest Lipid Maps DB in the Lipid Maps DB folder</param>
	''' <returns>The filename of the latest version of the database</returns>
	''' <remarks>If the newly downloaded LipidMaps.txt file has a hash that matches the computed hash for strNewestLipidMapsDBFileName, then we update the time stamp on the HashCheckFile instead of copying the downloaded data back to the server</remarks>
	Protected Function DownloadNewLipidMapsDB(ByVal diLipidMapsDBFolder As System.IO.DirectoryInfo, ByVal strNewestLipidMapsDBFileName As String) As String

		Dim blnWaitingForLockFile As Boolean = False
		Dim strLockFilePath As String = String.Empty

		Dim strHashCheckFilePath As String = String.Empty
		Dim strNewestLipidMapsDBFileHash As String = String.Empty

		Dim dtLockFileCreated As System.DateTime
		Dim dtLipidMapsDBFileTime As System.DateTime

		' Look for a recent .lock file

		For Each fiFile As System.IO.FileInfo In diLipidMapsDBFolder.GetFileSystemInfos("*.lock")
			If System.DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours < 2 Then
				blnWaitingForLockFile = True
				strLockFilePath = fiFile.FullName
				dtLockFileCreated = fiFile.LastWriteTimeUtc

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "LipidMapsDB lock file found; will wait for file to be deleted or age; " & fiFile.Name & " created " & fiFile.LastWriteTime.ToString())
				End If
				Exit For
			Else
				' Lock file has aged; delete it
				fiFile.Delete()
			End If
		Next

		If blnWaitingForLockFile Then

			Do While blnWaitingForLockFile
				' Wait 5 seconds
				System.Threading.Thread.Sleep(5000)

				If Not System.IO.File.Exists(strLockFilePath) Then
					blnWaitingForLockFile = False
				ElseIf System.DateTime.UtcNow.Subtract(dtLockFileCreated).TotalHours > 2 Then
					blnWaitingForLockFile = False
				End If
			Loop

			If System.IO.File.Exists(strLockFilePath) Then
				' Lock file is over 2 hours old; delete it
				System.IO.File.Delete(strLockFilePath)
			End If

			strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, dtLipidMapsDBFileTime)

			If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
				If System.DateTime.UtcNow.Subtract(dtLipidMapsDBFileTime).TotalDays < LIPID_MAPS_STALE_DB_AGE_DAYS Then
					' File is now up-to-date
					Return strNewestLipidMapsDBFileName
				End If
			End If

		End If

		If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then

			' Read the hash value stored in the hashcheck file for strNewestLipidMapsDBFileName
			strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strHashCheckFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
				strNewestLipidMapsDBFileHash = srInFile.ReadLine()
			End Using

			If String.IsNullOrEmpty(strNewestLipidMapsDBFileHash) Then strNewestLipidMapsDBFileHash = String.Empty
		End If


		' Call the LipidTools.exe program to obtain the latest database

		Dim strTimeStamp As String = System.DateTime.Now.ToString("yyyy-MM-dd")

		' Create a new lock file
		strLockFilePath = System.IO.Path.Combine(diLipidMapsDBFolder.FullName, LIPID_MAPS_DB_FILENAME_PREFIX & strTimeStamp & ".lock")
		Using swLockFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strLockFilePath, IO.FileMode.CreateNew, IO.FileAccess.Write, IO.FileShare.Read))
			swLockFile.WriteLine("Downloading LipidMaps.txt file on " & m_MachName & " at " & System.DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt"))
		End Using


		' Call the LipidTools program to obtain the latest database from http://www.lipidmaps.org/
		Dim CmdStr As String
		Dim blnSuccess As Boolean
		Dim strLipidMapsDBFileLocal As String = System.IO.Path.Combine(m_WorkDir, LIPID_MAPS_DB_FILENAME_PREFIX & strTimeStamp & ".txt")

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Downloading latest LipidMaps database")

		CmdStr = " -UpdateDBOnly -db " & PossiblyQuotePath(strLipidMapsDBFileLocal)

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mLipidToolsProgLoc & CmdStr)
		End If

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True
			.WriteConsoleOutputToFile = False
		End With


		blnSuccess = CmdRunner.RunProgram(mLipidToolsProgLoc, CmdStr, "LipidTools", True)

		If Not blnSuccess Then
			m_message = "Error downloading the latest LipidMaps DB using LipidTools"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "LipidTools returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to LipidTools failed (but exit code is 0)")
			End If

			Return String.Empty
		End If

		' Compute the MD5 hash value of the newly downloaded file
		Dim strHashCheckNew As String
		strHashCheckNew = clsGlobal.ComputeFileHashSha1(strLipidMapsDBFileLocal)

		If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileHash) AndAlso strHashCheckNew = strNewestLipidMapsDBFileHash Then
			' The hashes match; we'll update the timestamp of the hashcheck file below
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Hash code of the newly downloaded database matches the hash for " & strNewestLipidMapsDBFileName & ": " & strNewestLipidMapsDBFileHash)
			End If

			If System.IO.Path.GetFileName(strLipidMapsDBFileLocal) <> strNewestLipidMapsDBFileName Then
				' Rename the newly downloaded file to strNewestLipidMapsDBFileName
				System.Threading.Thread.Sleep(500)
				System.IO.File.Move(strLipidMapsDBFileLocal, System.IO.Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName))
			End If

		Else
			' Copy the new file up to the server

			strNewestLipidMapsDBFileName = String.Copy(strLipidMapsDBFileLocal)

			Dim intCopyAttempts As Integer = 0

			Do While intCopyAttempts <= 2

				Try
					intCopyAttempts += 1
					System.IO.File.Copy(strLipidMapsDBFileLocal, System.IO.Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName))
					Exit Do
				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying Lipid Maps DB to server; attempt=" & intCopyAttempts)
					' Wait 5 seconds, then try again
					System.Threading.Thread.Sleep(5000)
				End Try

			Loop

			strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)
		End If

		' Update the hash-check file (do this regardless of whether or not the newly downloaded file matched the most recent one)
		Using swOutFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strHashCheckFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))
			swOutFile.WriteLine(strHashCheckNew)
		End Using

		' Delete the lock file
		Try
			System.IO.File.Delete(strLockFilePath)
		Catch ex As Exception
			' Ignore errors here
		End Try

		Return strNewestLipidMapsDBFileName

	End Function

	Protected Function FindNewestLipidMapsDB(ByVal diLipidMapsDBFolder As System.IO.DirectoryInfo, ByRef dtLipidMapsDBFileTime As System.DateTime) As String

		Dim strNewestLipidMapsDBFileName As String
		strNewestLipidMapsDBFileName = String.Empty

		dtLipidMapsDBFileTime = System.DateTime.MinValue

		For Each fiFile As System.IO.FileInfo In diLipidMapsDBFolder.GetFileSystemInfos(LIPID_MAPS_DB_FILENAME_PREFIX & "*.txt")
			If fiFile.LastWriteTimeUtc > dtLipidMapsDBFileTime Then
				dtLipidMapsDBFileTime = fiFile.LastWriteTimeUtc
				strNewestLipidMapsDBFileName = fiFile.Name
			End If
		Next

		If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
			' Now look for a .hashcheck file for this LipidMapsDB.txt file
			Dim fiHashCheckFile As System.IO.FileInfo
			fiHashCheckFile = New System.IO.FileInfo(GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName))

			If fiHashCheckFile.Exists Then
				' Update the Lipid Maps DB file time
				If dtLipidMapsDBFileTime < fiHashCheckFile.LastWriteTimeUtc Then
					dtLipidMapsDBFileTime = fiHashCheckFile.LastWriteTimeUtc
				End If
			End If

		End If

		Return strNewestLipidMapsDBFileName

	End Function

	Protected Function GetHashCheckFilePath(ByVal strLipidMapsDBFolderPath As String, ByVal strNewestLipidMapsDBFileName As String) As String
		Return System.IO.Path.Combine(strLipidMapsDBFolderPath, System.IO.Path.GetFileNameWithoutExtension(strNewestLipidMapsDBFileName) & ".hashcheck")
	End Function

	Protected Function GetLipidMapsDatabase() As Boolean

		Dim strParamFileFolderPath As String
		Dim diLipidMapsDBFolder As System.IO.DirectoryInfo

		Dim strNewestLipidMapsDBFileName As String
		Dim dtLipidMapsDBFileTime As System.DateTime = System.DateTime.MinValue

		Dim strSourceFilePath As String
		Dim strTargetFilePath As String

		Dim blnUpdateDB As Boolean = False

		Try

			strParamFileFolderPath = m_jobParams.GetJobParameter("ParmFileStoragePath", String.Empty)

			If String.IsNullOrEmpty(strParamFileFolderPath) Then
				m_message = "Parameter 'ParmFileStoragePath' is empty"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; unable to get the LipidMaps database")
				Return False
			End If

			diLipidMapsDBFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(strParamFileFolderPath, "LipidMapsDB"))

			If Not diLipidMapsDBFolder.Exists Then
				m_message = "LipidMaps database folder not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & diLipidMapsDBFolder.FullName)
				Return False
			End If

			' Find the newest date-stamped file
			strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, dtLipidMapsDBFileTime)

			If String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
				blnUpdateDB = True
			ElseIf System.DateTime.UtcNow.Subtract(dtLipidMapsDBFileTime).TotalDays > LIPID_MAPS_STALE_DB_AGE_DAYS Then
				blnUpdateDB = True
			End If

			If blnUpdateDB Then
				Dim intDownloadAttempts As Integer = 0

				mDownloadingLipidMapsDatabase = True

				Do While intDownloadAttempts <= 2

					Try
						intDownloadAttempts += 1
						strNewestLipidMapsDBFileName = DownloadNewLipidMapsDB(diLipidMapsDBFolder, strNewestLipidMapsDBFileName)
						Exit Do
					Catch ex As Exception
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception downloading Lipid Maps DB; attempt=" & intDownloadAttempts)
						' Wait 5 seconds, then try again
						System.Threading.Thread.Sleep(5000)
					End Try

				Loop

				mDownloadingLipidMapsDatabase = False

			End If

			If String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Unable to determine the LipidMapsDB file to copy locally"
				End If
				Return False
			End If

			' File is now up-to-date; copy locally (if not already in the work dir)
			mLipidMapsDBFilename = String.Copy(strNewestLipidMapsDBFileName)
			strSourceFilePath = System.IO.Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)
			strTargetFilePath = System.IO.Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName)

			If Not System.IO.File.Exists(strTargetFilePath) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying lipid Maps DB locally: " & strSourceFilePath)
				End If
				System.IO.File.Copy(strSourceFilePath, strTargetFilePath)
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception obtaining lipid Maps DB: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function GetLipidMapsParameterNames() As System.Collections.Generic.Dictionary(Of String, String)
		Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)
		dctParamNames = New System.Collections.Generic.Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

		dctParamNames.Add("AlignmentToleranceNET", "an")
		dctParamNames.Add("AlignmentToleranceMassPPM", "am")
		dctParamNames.Add("DBMatchToleranceMassPPM", "mm")
		dctParamNames.Add("DBMatchToleranceMzPpmCID", "ct")
		dctParamNames.Add("DBMatchToleranceMzPpmHCD", "ht")

		Return dctParamNames
	End Function

	''' <summary>
	''' Parse the LipidTools console output file to track progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output:
		'   Reading local Lipid Maps database...Done.
		'   Reading positive data...Done.
		'   Reading negative data...Done.
		'   Finding features (positive)...200 / 4778
		'   400 / 4778
		'   ...
		'   4600 / 4778
		'   Done (1048 found).
		'   Finding features (negative)...200 / 4558
		'   400 / 4558
		'   ...
		'   4400 / 4558
		'   Done (900 found).
		'   Aligning features...Done (221 alignments).
		'   Matching to Lipid Maps database...Done (2041 matches).
		'   Writing results...Done.
		'   Writing QC data...Done.
		'   Saving QC images...Done.

		Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow
		Static reSubProgress As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("^(\d+) / (\d+)", Text.RegularExpressions.RegexOptions.Compiled)

		Try

			If mConsoleOutputProgressMap Is Nothing OrElse mConsoleOutputProgressMap.Count = 0 Then
				mConsoleOutputProgressMap = New System.Collections.Generic.Dictionary(Of String, Integer)

				mConsoleOutputProgressMap.Add("Reading local Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE)
				mConsoleOutputProgressMap.Add("Reading positive data", PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA)
				mConsoleOutputProgressMap.Add("Reading negative data", PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA)
				mConsoleOutputProgressMap.Add("Finding features (positive)", PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES)
				mConsoleOutputProgressMap.Add("Finding features (negative)", PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
				mConsoleOutputProgressMap.Add("Aligning features", PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES)
				mConsoleOutputProgressMap.Add("Matching to Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB)
				mConsoleOutputProgressMap.Add("Writing results", PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS)
				mConsoleOutputProgressMap.Add("Writing QC data", PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA)
			End If

			If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
			End If


			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer
			Dim oMatch As System.Text.RegularExpressions.Match
			Dim dblSubProgressAddon As Double

			Dim intSubProgressCount As Integer
			Dim intSubProgressCountTotal As Integer

			Dim intEffectiveProgress As Integer
			intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_STARTING

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then

					' Update progress if the line starts with one of the expected phrases
					For Each oItem As System.Collections.Generic.KeyValuePair(Of String, Integer) In mConsoleOutputProgressMap
						If strLineIn.StartsWith(oItem.Key) Then
							If intEffectiveProgress < oItem.Value Then
								intEffectiveProgress = oItem.Value
							End If
						End If
					Next

					If intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES OrElse intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES Then
						oMatch = reSubProgress.Match(strLineIn)
						If oMatch.Success Then
							If Integer.TryParse(oMatch.Groups(1).Value, intSubProgressCount) Then
								If Integer.TryParse(oMatch.Groups(2).Value, intSubProgressCountTotal) Then
									dblSubProgressAddon = intSubProgressCount / CDbl(intSubProgressCountTotal)
								End If
							End If
						End If
					End If

				End If
			Loop

			srInFile.Close()

			Dim sngEffectiveProgress As Single = intEffectiveProgress

			' Bump up the effective progress if finding features in positive or negative data
			If intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES Then
				sngEffectiveProgress += CSng((PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES) * dblSubProgressAddon)
			ElseIf intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES Then
				sngEffectiveProgress += CSng((PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES) * dblSubProgressAddon)
			End If

			If m_progress < sngEffectiveProgress Then
				m_progress = sngEffectiveProgress

				If m_DebugLevel >= 3 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
					dtLastProgressWriteTime = System.DateTime.UtcNow
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

	''' <summary>
	''' Read the LipidMapSearch options file and convert the options to command line switches
	''' </summary>
	''' <param name="strParameterFilePath">Path to the LipidMapSearch Parameter File</param>
	''' <returns>Options string if success; empty string if an error</returns>
	''' <remarks></remarks>
	Private Function ParseLipidMapSearchParameterFile(ByVal strParameterFilePath As String) As String

		Dim sbOptions As System.Text.StringBuilder
		Dim strLineIn As String

		Dim strKey As String
		Dim strValue As String

		Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)

		sbOptions = New System.Text.StringBuilder(500)

		Try

			' Initialize the Param Name dictionary
			dctParamNames = GetLipidMapsParameterNames()

			Using srParamFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strParameterFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				Do While srParamFile.Peek > -1
					strLineIn = srParamFile.ReadLine()
					strKey = String.Empty
					strValue = String.Empty

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						strLineIn = strLineIn.Trim()

						If Not strLineIn.StartsWith("#") AndAlso strLineIn.Contains("="c) Then

							Dim intCharIndex As Integer
							intCharIndex = strLineIn.IndexOf("=")
							If intCharIndex > 0 Then
								strKey = strLineIn.Substring(0, intCharIndex).Trim()
								If intCharIndex < strLineIn.Length - 1 Then
									strValue = strLineIn.Substring(intCharIndex + 1).Trim()
								Else
									strValue = String.Empty
								End If
							End If
						End If

					End If

					If Not String.IsNullOrWhiteSpace(strKey) Then

						Dim strArgumentSwitch As String = String.Empty

						' Check whether strKey is one of the standard keys defined in dctParamNames
						If dctParamNames.TryGetValue(strKey, strArgumentSwitch) Then
							sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)
						ElseIf strKey.ToLower = "adducts" Then
							sbOptions.Append(" -adducts " & """" & strValue & """")
						End If

					End If
				Loop

			End Using

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception reading LipidMaps parameter file: " & ex.Message)
			Return String.Empty
		End Try

		Return sbOptions.ToString()

	End Function


	Protected Function PostProcessLipidToolsResults() As Boolean

		Dim strFolderToZip As String

		Dim lstFilesToMove As System.Collections.Generic.List(Of String)
		Dim oIonicZipper As clsIonicZipTools

		Try
			' Create the PlotData folder and move the plot data text files into that folder
			strFolderToZip = System.IO.Path.Combine(m_WorkDir, "PlotData")
			System.IO.Directory.CreateDirectory(strFolderToZip)

			lstFilesToMove = New System.Collections.Generic.List(Of String)
			lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "AlignMassError.txt")
			lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "AlignNETError.txt")
			lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "MatchMassError.txt")
			lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "Tiers.txt")

			System.Threading.Thread.Sleep(500)

			For Each strFileName As String In lstFilesToMove
				Dim fiSourceFile As System.IO.FileInfo
				fiSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, strFileName))

				If fiSourceFile.Exists Then
					fiSourceFile.MoveTo(System.IO.Path.Combine(strFolderToZip, strFileName))
				End If
			Next

			System.Threading.Thread.Sleep(500)

			' Zip up the files in the PlotData folder
			oIonicZipper = New clsIonicZipTools(m_DebugLevel, m_WorkDir)

			oIonicZipper.ZipDirectory(strFolderToZip, System.IO.Path.Combine(m_WorkDir, "LipidMap_PlotData.zip"))

		Catch ex As Exception
			m_message = "Exception zipping the plot data text files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Try
			Dim fiExcelFile As System.IO.FileInfo

			fiExcelFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX & "results.xlsx"))

			If Not fiExcelFile.Exists Then
				m_message = "Excel results file not found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiExcelFile.Name)
				Return False
			End If

			fiExcelFile.MoveTo(System.IO.Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX & "results_" & m_Dataset & ".xlsx"))

		Catch ex As Exception
			m_message = "Exception renaming Excel results file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strLipidToolsProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim ioLipidTools As System.IO.FileInfo
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioLipidTools = New System.IO.FileInfo(strLipidToolsProgLoc)
		If Not ioLipidTools.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		' Lookup the version of the LipidTools application
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioLipidTools.FullName)
		If Not blnSuccess Then Return False

		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(ioLipidTools)

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

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 AndAlso Not mDownloadingLipidMapsDatabase Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region
End Class
