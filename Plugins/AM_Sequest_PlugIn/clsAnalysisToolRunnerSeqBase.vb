'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/19/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

Public Class clsAnalysisToolRunnerSeqBase
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	' Base class for Sequest analysis 
	' Note that MakeOUTFiles() in this class calls a standalone Sequest.Exe program for groups of DTA files
	' See clsAnalysisToolRunnerSeqCluster for the code used to interface with the Sequest cluster program
	'*********************************************************************************************************

#Region "Constants"
	Public Const CONCATENATED_OUT_TEMP_FILE As String = "_out.txt.tmp"
	Protected Const MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK As Integer = 500
#End Region

#Region "Member variables"
	Protected mDtaCountAddon As Integer = 0
	Protected mTotalOutFileCount As Integer = 0

	Protected mTempConcatenatedOutFilePath As String = String.Empty
	Protected mOutFileNamesAppended As SortedSet(Of String) = New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)

	' Out file search times (in seconds) for recently created .out files
	Protected mRecentOutFileSearchTimes As Queue(Of Single) = New Queue(Of Single)(MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK)

	Protected m_OutFileNameRegEx As System.Text.RegularExpressions.Regex = _
	  New System.Text.RegularExpressions.Regex("^(?<rootname>.+)\.(?<startscan>\d+)\.(?<endscan>\d+)\.(?<cs>\d+)\.(?<extension>\S{3})", _
	  RegexOptions.IgnoreCase Or RegexOptions.CultureInvariant Or RegexOptions.Compiled)

	Protected m_OutFileSearchTimeRegEx As System.Text.RegularExpressions.Regex = _
	  New System.Text.RegularExpressions.Regex("\d+/\d+/\d+, \d+\:\d+ [A-Z]+, (?<time>[0-9.]+) sec", _
	  RegexOptions.IgnoreCase Or RegexOptions.CultureInvariant Or RegexOptions.Compiled)

	Protected mOutFileHandlerInUse As Long

#End Region

#Region "Methods"

	''' <summary>
	''' Runs the analysis tool
	''' </summary>
	''' <returns>IJobParams.CloseOutType value indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim Result As IJobParams.CloseOutType
		Dim eReturnCode As IJobParams.CloseOutType
		Dim blnProcessingError As Boolean = False

		' Do the base class stuff
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Check whether or not we are resuming a job that stopped prematurely
		' Look for a file named Dataset_out.txt.tmp in m_WorkDir
		' This procedure will also de-concatenate the _dta.txt file
		If Not CheckForExistingConcatenatedOutFile() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Make sure at least one .DTA file exists
		If Not ValidateDTAFiles() Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
		End If

		' Count the number of .Dta files and cache in m_DtaCount
		CalculateNewStatus(blnUpdateDTACount:=True)

		'Run Sequest
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)

		'Make the .out files
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Making OUT files, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
		Try
			Result = MakeOUTFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				blnProcessingError = True
			End If
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerSeqBase.RunTool(), Exception making OUT files, " & _
			 Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
			blnProcessingError = True
		End Try

		'Stop the job timer
		m_StopTime = System.DateTime.UtcNow

		If blnProcessingError Then
			' Something went wrong
			' In order to help diagnose things, we will move whatever files were created into the result folder, 
			'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
			eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			eReturnCode = Result
		End If

		'Add the current job data to the summary file
		If Not UpdateSummaryFile() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
		End If

		'Make sure objects are released
		System.Threading.Thread.Sleep(2000)		   ' 2 second delay
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		' Parse the Sequest .Log file to make sure the expected number of nodes was used in the analysis
		Dim strSequestLogFilePath As String
		Dim blnSuccess As Boolean

		If m_mgrParams.GetParam("cluster", True) Then
			' Running on a Sequest cluster
			strSequestLogFilePath = System.IO.Path.Combine(m_WorkDir, "sequest.log")
			blnSuccess = ValidateSequestNodeCount(strSequestLogFilePath)
		Else
			blnSuccess = True
		End If

		If blnProcessingError Then
			' Move the source files and any results to the Failed Job folder
			' Useful for debugging Sequest problems
			CopyFailedResultsToArchiveFolder()
			If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
			Return eReturnCode
		End If

		Result = MakeResultsFolder()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'MakeResultsFolder handles posting to local log, so set database error message and exit
			m_message = "Error making results folder"
			Return Result
		End If

		Result = MoveResultFiles()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
			m_message = "Error moving files into results folder"
			Return Result
		End If

		Result = CopyResultsFolderToServer()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
			Return Result
		End If

		If Not MyBase.RemoveNonResultServerFiles() Then
			' Do not treat this as a fatal error
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error deleting .tmp files in folder " & m_jobParams.GetParam("JobParameters", "transferFolderPath"))
		End If

		Return eReturnCode

	End Function

	''' <summary>
	''' Appends the given .out file to the target file
	''' </summary>
	''' <param name="fiSourceOutFile"></param>
	''' <param name="swTargetFile"></param>
	''' <remarks></remarks>
	Protected Sub AppendOutFile(ByVal fiSourceOutFile As System.IO.FileInfo, ByRef swTargetFile As System.IO.StreamWriter)

		Const hdrLeft As String = "=================================== " & """"
		Const hdrRight As String = """" & " =================================="

		Dim cleanedFileName As String
		Dim strLineIn As String

		Dim reMatch As System.Text.RegularExpressions.Match
		Dim sngOutFileSearchTimeSeconds As Single

		If Not fiSourceOutFile.Exists Then
			Console.WriteLine("Warning, out file not found: " & fiSourceOutFile.FullName)
			Exit Sub
		End If

		If Not mOutFileNamesAppended.Contains(fiSourceOutFile.Name) Then

			' Note: do not put a Try/Catch block here
			' Let the calling function catch any errors

			Using srSrcFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(fiSourceOutFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				reMatch = m_OutFileNameRegEx.Match(fiSourceOutFile.Name)

				If reMatch.Success Then
					cleanedFileName = reMatch.Groups("rootname").Value + "." + CType(reMatch.Groups("startscan").Value, Integer).ToString + "." + _
					   CType(reMatch.Groups("endscan").Value, Integer).ToString + "." + _
					   CType(reMatch.Groups("cs").Value, Integer).ToString + "." + reMatch.Groups("extension").Value
				Else
					cleanedFileName = fiSourceOutFile.Name
				End If

				swTargetFile.WriteLine()
				swTargetFile.WriteLine(hdrLeft & cleanedFileName & hdrRight)

				While srSrcFile.Peek > -1
					strLineIn = srSrcFile.ReadLine()
					swTargetFile.WriteLine(strLineIn)

					reMatch = m_OutFileSearchTimeRegEx.Match(strLineIn)
					If reMatch.Success Then
						If Single.TryParse(reMatch.Groups("time").Value, sngOutFileSearchTimeSeconds) Then

							If mRecentOutFileSearchTimes.Count >= MAX_OUT_FILE_SEARCH_TIMES_TO_TRACK Then
								mRecentOutFileSearchTimes.Dequeue()
							End If

							mRecentOutFileSearchTimes.Enqueue(sngOutFileSearchTimeSeconds)
						End If

						' Append the remainder of the out file (no need to continue reading line-by-line)
						If srSrcFile.Peek > -1 Then
							swTargetFile.Write(srSrcFile.ReadToEnd())
						End If

					End If
				End While

			End Using

			If Not mOutFileNamesAppended.Contains(fiSourceOutFile.Name) Then
				mOutFileNamesAppended.Add(fiSourceOutFile.Name)
			End If

			mTotalOutFileCount += 1
		End If

		Dim strDtaFilePath As String
		strDtaFilePath = System.IO.Path.ChangeExtension(fiSourceOutFile.FullName, "dta")

		Try
			If fiSourceOutFile.Exists Then
				fiSourceOutFile.Delete()
			End If

			If System.IO.File.Exists(strDtaFilePath) Then
				System.IO.File.Delete(strDtaFilePath)
			End If

		Catch ex As Exception
			' Ignore deletion errors; we'll delete these files later
			Console.WriteLine("Warning, exception deleting file: " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Calculates status information for progress file by counting the number of .out files
	''' </summary>
	Protected Sub CalculateNewStatus()
		CalculateNewStatus(blnUpdateDTACount:=False)
	End Sub

	''' <summary>
	''' Calculates status information for progress file by counting the number of .out files
	''' </summary>
	''' <param name="blnUpdateDTACount">Set to True to update m_DtaCount</param>
	Protected Sub CalculateNewStatus(ByVal blnUpdateDTACount As Boolean)

		Dim OutFileCount As Integer

		If blnUpdateDTACount Then
			' Get DTA count
			m_DtaCount = GetDTAFileCountRemaining() + mDtaCountAddon
		End If

		' Get OUT file count
		OutFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount

		' Calculate % complete (value between 0 and 100)
		If m_DtaCount > 0 Then
			m_progress = 100.0! * CSng(OutFileCount / m_DtaCount)
		Else
			m_progress = 0
		End If

	End Sub

	Protected Function CheckForExistingConcatenatedOutFile() As Boolean

		Dim strConcatenatedTempFilePath As String
		Dim lstDTAsToSkip As SortedSet(Of String)

		Try

			mDtaCountAddon = 0
			mTotalOutFileCount = 0

			mTempConcatenatedOutFilePath = String.Empty
			mOutFileNamesAppended.Clear()

			strConcatenatedTempFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & CONCATENATED_OUT_TEMP_FILE)

			If System.IO.File.Exists(strConcatenatedTempFilePath) Then
				' Parse the _out.txt.tmp to determine the .out files that it contains
				lstDTAsToSkip = ConstructDTASkipList(strConcatenatedTempFilePath)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file (skipping " & lstDTAsToSkip.Count.ToString("#,##0") & " existing DTAs with existing .Out files)")
			Else
				lstDTAsToSkip = New SortedSet(Of String)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file")
			End If

			' Now split the DTA file, skipping DTAs corresponding to .Out files that were copied over
			Dim FileSplitter As New clsSplitCattedFiles()
			Dim blnSuccess As Boolean
			blnSuccess = FileSplitter.SplitCattedDTAsOnly(m_Dataset, m_WorkDir, lstDTAsToSkip)

			If Not blnSuccess Then
				m_message = "SplitCattedDTAsOnly returned false"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message & "; aborting")
				Return False
			End If

			mDtaCountAddon = lstDTAsToSkip.Count
			mTotalOutFileCount = mDtaCountAddon

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Completed splitting concatenated DTA file, created " & GetDTAFileCountRemaining().ToString("#,##0") & " DTAs")
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CheckForExistingConcatenatedOutFile: " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	Protected Function ConstructDTASkipList(ByVal strConcatenatedTempFilePath As String) As SortedSet(Of String)

		Dim reFileSeparator As Regex
		Dim objFileSepMatch As Match

		Dim lstDTAsToSkip As SortedSet(Of String)
		Dim strLineIn As String

		lstDTAsToSkip = New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)

		Try

			reFileSeparator = New Regex(clsSplitCattedFiles.REGEX_FILE_SEPARATOR, RegexOptions.CultureInvariant Or RegexOptions.Compiled)

			Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strConcatenatedTempFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				While srInFile.Peek > -1
					strLineIn = srInFile.ReadLine()

					objFileSepMatch = reFileSeparator.Match(strLineIn)

					If objFileSepMatch.Success Then
						lstDTAsToSkip.Add(System.IO.Path.ChangeExtension(objFileSepMatch.Groups("filename").Value, "dta"))
					End If
				End While

			End Using

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ConstructDTASkipList: " & ex.Message)
			Throw New Exception("Error parsing temporary concatenated temp file", ex)
		End Try

		Return lstDTAsToSkip

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
		' We don't need to delete .Dta files since MoveResultFiles() will skip them
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		Try
			System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
			System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
		Catch ex As Exception
			' Ignore errors here
		End Try

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

	Protected Function GetDTAFileCountRemaining() As Integer
		Dim diWorkDir As System.IO.DirectoryInfo
		diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)
		Return diWorkDir.GetFiles("*.dta", System.IO.SearchOption.TopDirectoryOnly).Length()
	End Function

	Protected Function GetOUTFileCountRemaining() As Integer
		Dim diWorkDir As System.IO.DirectoryInfo
		diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)
		Return diWorkDir.GetFiles("*.out", System.IO.SearchOption.TopDirectoryOnly).Length()
	End Function

	''' <summary>
	''' Runs Sequest to make .out files
	''' This function uses the standalone Sequest.exe program; it is not used by the Sequest clusters
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function MakeOUTFiles() As IJobParams.CloseOutType

		'Creates Sequest .out files from DTA files
		Dim CmdStr As String
		Dim DumStr As String
		Dim DtaFiles() As String
		Dim RunProgs() As PRISM.Processes.clsProgRunner
		Dim Textfiles() As System.IO.StreamWriter
		Dim NumFiles As Integer
		Dim ProcIndx As Integer
		Dim StillRunning As Boolean

		'12/19/2008 - The number of processors used to be configurable but now this is done with clustering.
		'This code is left here so we can still debug to make sure everything still works
		'		Dim NumProcessors As Integer = CInt(m_mgrParams.GetParam("numberofprocessors"))
		Dim NumProcessors As Integer = 1

		'Get a list of .dta file names
		DtaFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.dta")
		NumFiles = DtaFiles.GetLength(0)
		If NumFiles = 0 Then
			m_message = clsGlobal.AppendToComment(m_message, "No dta files found for Sequest processing")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Set up a program runner and text file for each processor
		ReDim RunProgs(NumProcessors - 1)
		ReDim Textfiles(NumProcessors - 1)
		CmdStr = "-D" & System.IO.Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName")) & " -P" & m_jobParams.GetParam("parmFileName") & " -R"

		For ProcIndx = 0 To NumProcessors - 1
			DumStr = System.IO.Path.Combine(m_WorkDir, "FileList" & ProcIndx.ToString & ".txt")
			m_jobParams.AddResultFileToSkip(DumStr)

			RunProgs(ProcIndx) = New PRISM.Processes.clsProgRunner
			RunProgs(ProcIndx).Name = "Seq" & ProcIndx.ToString
			RunProgs(ProcIndx).CreateNoWindow = CBool(m_mgrParams.GetParam("createnowindow"))
			RunProgs(ProcIndx).Program = m_mgrParams.GetParam("seqprogloc")
			RunProgs(ProcIndx).Arguments = CmdStr & DumStr
			RunProgs(ProcIndx).WorkDir = m_WorkDir
			Textfiles(ProcIndx) = New System.IO.StreamWriter(DumStr, False)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_mgrParams.GetParam("seqprogloc") & CmdStr & DumStr)
		Next

		'Break up file list into lists for each processor
		ProcIndx = 0
		For Each DumStr In DtaFiles
			Textfiles(ProcIndx).WriteLine(DumStr)
			ProcIndx += 1
			If ProcIndx > (NumProcessors - 1) Then ProcIndx = 0
		Next

		'Close all the file lists
		For ProcIndx = 0 To Textfiles.GetUpperBound(0)
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles: Closing FileList" & ProcIndx)
			End If
			Try
				Textfiles(ProcIndx).Close()
				Textfiles(ProcIndx) = Nothing
			Catch Err As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerSeqBase.MakeOutFiles: " & Err.Message & "; " & _
				 clsGlobal.GetExceptionStackTrace(Err))
			End Try
		Next

		'Run all the programs
		For ProcIndx = 0 To RunProgs.GetUpperBound(0)
			RunProgs(ProcIndx).StartAndMonitorProgram()
			System.Threading.Thread.Sleep(1000)
		Next

		'Wait for completion

		Do
			StillRunning = False

			' Wait 5 seconds
			System.Threading.Thread.Sleep(5000)

			' Synchronize the stored Debug level with the value stored in the database
			Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
			MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

			CalculateNewStatus()
			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)

			For ProcIndx = 0 To RunProgs.GetUpperBound(0)
				If m_DebugLevel > 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(): RunProgs(" & ProcIndx.ToString & ").State = " & _
					 RunProgs(ProcIndx).State.ToString)
				End If
				If (RunProgs(ProcIndx).State <> 0) Then
					If m_DebugLevel > 4 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_2: RunProgs(" & ProcIndx.ToString & ").State = " & _
						 RunProgs(ProcIndx).State.ToString)
					End If
					If (RunProgs(ProcIndx).State <> 10) Then
						If m_DebugLevel > 4 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_3: RunProgs(" & ProcIndx.ToString & ").State = " & _
							 RunProgs(ProcIndx).State.ToString)
						End If
						StillRunning = True
						Exit For
					Else
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_4: RunProgs(" & ProcIndx.ToString & ").State = " & _
							 RunProgs(ProcIndx).State.ToString)
						End If
					End If
				End If
			Next

		Loop While StillRunning

		'Clean up our object references
		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), cleaning up runprog object references")
		End If
		For ProcIndx = 0 To RunProgs.GetUpperBound(0)
			RunProgs(ProcIndx) = Nothing
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Set RunProgs(" & ProcIndx.ToString & ") to Nothing")
			End If
		Next

		'Make sure objects are released
		System.Threading.Thread.Sleep(5000)		' 5 second delay
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		'Verify out file creation
		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), verifying out file creation")
		End If

		If GetOUTFileCountRemaining() < 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "No OUT files created, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			m_message = clsGlobal.AppendToComment(m_message, "No OUT files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		Else
			'Add .out extension to list of file extensions to delete
			m_jobParams.AddResultFileExtensionToSkip(".out")
		End If

		'Package out files into concatenated text files 
		If Not ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Try to ensure there are no open objects with file handles
		System.Threading.Thread.Sleep(2000)		   '2 second delay
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE
		End If

		'If we got here, everything worked
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Concatenates any .out files that still remain in the the working directory
	''' If running on the Sequest Cluster, then the majority of the files should have already been appended to _out.txt.tmp
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="DSName">Dataset name</param>
	''' <param name="JobNum">Job number</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function ConcatOutFiles(ByVal WorkDir As String, ByVal DSName As String, ByVal JobNum As String) As Boolean

		Dim MAX_RETRY_ATTEMPTS As Integer = 5
		Dim MAX_INTERLOCK_WAIT_TIME_MINUTES As Integer = 30
		Dim intRetriesRemaining As Integer
		Dim blnSuccess As Boolean
		Dim oRandom As System.Random = New System.Random()

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenating .out files")
		End If

		intRetriesRemaining = MAX_RETRY_ATTEMPTS
		Do

			Dim dtInterlockWaitStartTime As System.DateTime = System.DateTime.UtcNow
			Dim dtInterlockWaitLastLogtime As System.DateTime = System.DateTime.UtcNow

			Do While System.Threading.Interlocked.Read(mOutFileHandlerInUse) > 0
				' Need to wait for ProcessCandidateOutFiles to exit
				System.Threading.Thread.Sleep(3000)

				If System.DateTime.UtcNow.Subtract(dtInterlockWaitStartTime).TotalMinutes >= MAX_INTERLOCK_WAIT_TIME_MINUTES Then
					m_message = "Unable to verify that all .out files have been appended to the _out.txt.tmp file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message & ": ConcatOutFiles has waited over " & MAX_INTERLOCK_WAIT_TIME_MINUTES & " minutes for mOutFileHandlerInUse to be zero; aborting")
					Return False
				ElseIf System.DateTime.UtcNow.Subtract(dtInterlockWaitStartTime).TotalSeconds >= 30 Then
					dtInterlockWaitStartTime = System.DateTime.UtcNow
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ConcatOutFiles is waiting for mOutFileHandlerInUse to be zero")
					End If
				End If
			Loop

			'Make sure objects are released
			System.Threading.Thread.Sleep(1000)		   ' 1 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Try
				If String.IsNullOrEmpty(mTempConcatenatedOutFilePath) Then
					mTempConcatenatedOutFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_out.txt.tmp")
				End If

				Dim diWorkDir As System.IO.DirectoryInfo
				diWorkDir = New System.IO.DirectoryInfo(WorkDir)

				Using swTargetFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(mTempConcatenatedOutFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))
					For Each fiOutFile As System.IO.FileInfo In diWorkDir.GetFileSystemInfos("*.out")
						AppendOutFile(fiOutFile, swTargetFile)
					Next
				End Using
				blnSuccess = True

			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error appending .out files to the _out.txt.tmp file" & ": " & ex.Message)
				System.Threading.Thread.Sleep(oRandom.Next(15, 30) * 1000)			' Delay for a random length between 15 and 30 seconds
				blnSuccess = False
			End Try

			intRetriesRemaining -= 1
		Loop While Not blnSuccess AndAlso intRetriesRemaining > 0

		If Not blnSuccess Then
			m_message = "Error appending .out files to the _out.txt.tmp file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; aborting after " & MAX_RETRY_ATTEMPTS & " attempts")
			Return False
		End If

		Try
			If String.IsNullOrEmpty(mTempConcatenatedOutFilePath) Then
				' No .out files were created
				m_message = "No out files were created"
				Return False
			End If

			' Now rename the _out.txt.tmp file to _out.txt
			Dim fiConcatenatedOutFile As System.IO.FileInfo
			fiConcatenatedOutFile = New System.IO.FileInfo(mTempConcatenatedOutFilePath)

			Dim strOutFilePathNew As String
			strOutFilePathNew = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_out.txt")

			If System.IO.File.Exists(strOutFilePathNew) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Existing _out.txt file found; overrwriting")
				System.IO.File.Delete(strOutFilePathNew)
			End If

			fiConcatenatedOutFile.MoveTo(strOutFilePathNew)

		Catch ex As Exception
			m_message = "Error renaming _out.txt.tmp file to _out.txt file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Stores the Sequest tool version info in the database
	''' If strOutFilePath is defined, then looks up the specific Sequest version using the given .Out file
	''' Also records the file date of the Sequest Program .exe
	''' </summary>
	''' <param name="strOutFilePath"></param>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strOutFilePath As String) As Boolean

		Dim ioToolFiles As New List(Of System.IO.FileInfo)
		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		' Lookup the version of the Param file generator
		If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "ParamFileGenerator") Then
			Return False
		End If

		' Lookup the version of Sequest using the .Out file
		Try
			If Not String.IsNullOrEmpty(strOutFilePath) Then


				Dim strLineIn As String

				Using srOutFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strOutFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

					Do While srOutFile.Peek > -1
						strLineIn = srOutFile.ReadLine()
						If Not String.IsNullOrEmpty(strLineIn) Then
							strLineIn = strLineIn.Trim()
							If strLineIn.ToLower().StartsWith("TurboSEQUEST".ToLower()) Then
								strToolVersionInfo = strLineIn

								If m_DebugLevel >= 2 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest Version: " & strToolVersionInfo)
								End If

								Exit Do
							End If
						End If
					Loop

				End Using
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception parsing .Out file in StoreToolVersionInfo: " & ex.Message)
		End Try

		' Store the path to the Sequest .Exe in ioToolFiles
		Try
			ioToolFiles.Add(New System.IO.FileInfo(m_mgrParams.GetParam("seqprogloc")))
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception adding Sequest .Exe to ioToolFiles in StoreToolVersionInfo: " & ex.Message)
		End Try

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Make sure at least one .DTA file exists
	''' Also makes sure at least one of the .DTA files has data
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ValidateDTAFiles() As Boolean
		Dim diWorkDir As System.IO.DirectoryInfo
		Dim ioFiles() As System.IO.FileInfo
		Dim ioFile As System.IO.FileInfo

		Dim strLineIn As String
		Dim blnDataFound As Boolean = False
		Dim intFilesChecked As Integer = 0

		Try
			diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

			ioFiles = diWorkDir.GetFiles("*.dta", System.IO.SearchOption.TopDirectoryOnly)

			If ioFiles.Length = 0 Then
				m_message = "No .DTA files are present"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			Else
				For Each ioFile In ioFiles
					Using srReader As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(ioFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

						Do While srReader.Peek > -1
							strLineIn = srReader.ReadLine

							If Not String.IsNullOrWhiteSpace(strLineIn) Then
								blnDataFound = True
								Exit Do
							End If
						Loop
					End Using

					intFilesChecked += 1

					If blnDataFound Then Exit For
				Next

				If Not blnDataFound Then
					If intFilesChecked = 1 Then
						m_message = "One .DTA file is present, but it is empty"
					Else
						m_message = ioFiles.Length.ToString() & " .DTA files are present, but each is empty"
					End If
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return False
				End If

			End If

		Catch ex As Exception
			m_message = "Exception in ValidateDTAFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return False
		End Try

		Return blnDataFound

	End Function

	''' <summary>
	''' Opens the sequest.log file in the working directory
	''' Parses out the number of nodes used and the number of slave processes spawned
	''' Counts the number of DTA files analyzed by each process
	''' </summary>
	''' <remarks></remarks>
	''' <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
	Protected Function ValidateSequestNodeCount(ByVal strLogFilePath As String) As Boolean
		Return ValidateSequestNodeCount(strLogFilePath, blnLogToConsole:=False)
	End Function

	''' <summary>
	''' Opens the sequest.log file in the working directory
	''' Parses out the number of nodes used and the number of slave processes spawned
	''' Counts the number of DTA files analyzed by each process
	''' </summary>
	''' <param name="strLogFilePath">Path to the sequest.log file to parse</param>
	''' <param name="blnLogToConsole">If true, then displays the various status messages at the console</param>
	''' <remarks></remarks>
	''' <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
	Protected Function ValidateSequestNodeCount(ByVal strLogFilePath As String, ByVal blnLogToConsole As Boolean) As Boolean
		Const ERROR_CODE_A As Integer = 2
		Const ERROR_CODE_B As Integer = 4
		Const ERROR_CODE_C As Integer = 8
		Const ERROR_CODE_D As Integer = 16
		Const ERROR_CODE_E As Integer = 32

		Dim reStartingTask As System.Text.RegularExpressions.Regex
		Dim reWaitingForReadyMsg As System.Text.RegularExpressions.Regex
		Dim reReceivedReadyMsg As System.Text.RegularExpressions.Regex
		Dim reSpawnedSlaveProcesses As System.Text.RegularExpressions.Regex
		Dim reSearchedDTAFile As System.Text.RegularExpressions.Regex
		Dim reMatch As System.Text.RegularExpressions.Match

		Dim strLineIn As String
		Dim strHostName As String

		' This dictionary tracks the number of DTAs processed by each node
		Dim dctHostCounts As Dictionary(Of String, Integer)

		' This dictionary tracks the number of distinct nodes on each host
		Dim dctHostNodeCount As Dictionary(Of String, Integer)

		Dim intValue As Integer

		' This dictionary tracks the number of DTAs processed per node on each host
		' Head node rates are ignored when computing medians and reporting warnings since the head nodes typically process far fewer DTAs than the slave nodes
		Dim dctHostProcessingRate As Dictionary(Of String, Single)

		Dim blnShowDetailedRates As Boolean

		Dim intHostCount As Integer
		Dim intNodeCountStarted As Integer
		Dim intNodeCountActive As Integer
		Dim intDTACount As Integer

		Dim intNodeCountExpected As Integer

		Dim strProcessingMsg As String

		Try

			blnShowDetailedRates = False

			If Not System.IO.File.Exists(strLogFilePath) Then
				strProcessingMsg = "Sequest.log file not found; cannot verify the sequest node count"
				If blnLogToConsole Then Console.WriteLine(strProcessingMsg & ": " & strLogFilePath)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
				Return False
			End If

			' Initialize the RegEx objects
			reStartingTask = New System.Text.RegularExpressions.Regex("Starting the SEQUEST task on (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reWaitingForReadyMsg = New System.Text.RegularExpressions.Regex("Waiting for ready messages from (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reReceivedReadyMsg = New System.Text.RegularExpressions.Regex("received ready messsage from (.+)\(", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reSpawnedSlaveProcesses = New System.Text.RegularExpressions.Regex("Spawned (\d+) slave processes", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
			reSearchedDTAFile = New System.Text.RegularExpressions.Regex("Searched dta file .+ on (.+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)

			intHostCount = 0			' Value for reStartingTask
			intNodeCountStarted = 0		' Value for reWaitingForReadyMsg
			intNodeCountActive = 0		' Value for reSpawnedSlaveProcesses
			intDTACount = 0

			' Note: This value is obtained when the manager params are grabbed from the Manager Control DB
			' Use this query to view/update expected node counts'
			'  SELECT M.M_Name, PV.MgrID, PV.Value
			'  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
			'  WHERE (PV.TypeID = 122)

			intNodeCountExpected = m_mgrParams.GetParam("SequestNodeCountExpected", 0)

			' Initialize the dictionary that will track the number of spectra processed by each host
			dctHostCounts = New Dictionary(Of String, Integer)

			' Initialize the dictionary that will track the number of distinct nodes on each host
			dctHostNodeCount = New Dictionary(Of String, Integer)

			' Initialize the dictionary that will track processing rates
			dctHostProcessingRate = New Dictionary(Of String, Single)

			Using srLogFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Read each line from the input file
				Do While srLogFile.Peek > -1
					strLineIn = srLogFile.ReadLine

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						' See if the line matches one of the expected RegEx values
						reMatch = reStartingTask.Match(strLineIn)
						If Not reMatch Is Nothing AndAlso reMatch.Success Then
							If Not Integer.TryParse(reMatch.Groups(1).Value, intHostCount) Then
								strProcessingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the Sequest.log file"
								If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
							End If

						Else
							reMatch = reWaitingForReadyMsg.Match(strLineIn)
							If Not reMatch Is Nothing AndAlso reMatch.Success Then
								If Not Integer.TryParse(reMatch.Groups(1).Value, intNodeCountStarted) Then
									strProcessingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the Sequest.log file"
									If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
								End If

							Else
								reMatch = reReceivedReadyMsg.Match(strLineIn)
								If Not reMatch Is Nothing AndAlso reMatch.Success Then
									strHostName = reMatch.Groups(1).Value

									If dctHostNodeCount.TryGetValue(strHostName, intValue) Then
										dctHostNodeCount(strHostName) = intValue + 1
									Else
										dctHostNodeCount.Add(strHostName, 1)
									End If

								Else
									reMatch = reSpawnedSlaveProcesses.Match(strLineIn)
									If Not reMatch Is Nothing AndAlso reMatch.Success Then
										If Not Integer.TryParse(reMatch.Groups(1).Value, intNodeCountActive) Then
											strProcessingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the Sequest.log file"
											If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
											clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
										End If

									Else
										reMatch = reSearchedDTAFile.Match(strLineIn)
										If Not reMatch Is Nothing AndAlso reMatch.Success Then
											strHostName = reMatch.Groups(1).Value

											If Not strHostName Is Nothing Then
												If dctHostCounts.TryGetValue(strHostName, intValue) Then
													dctHostCounts(strHostName) = intValue + 1
												Else
													dctHostCounts.Add(strHostName, 1)
												End If

												intDTACount += 1
											End If
										Else
											' Ignore this line
										End If
									End If
								End If
							End If
						End If

					End If
				Loop

			End Using


			Try
				' Validate the stats

				strProcessingMsg = "HostCount=" & intHostCount & "; NodeCountActive=" & intNodeCountActive
				If m_DebugLevel >= 1 Then
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
				End If
				m_EvalMessage = String.Copy(strProcessingMsg)

				If intNodeCountActive < intNodeCountExpected OrElse intNodeCountExpected = 0 Then
					strProcessingMsg = "Error: NodeCountActive less than expected value (" & intNodeCountActive & " vs. " & intNodeCountExpected & ")"
					If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)

					' Update the evaluation message and evaluation code
					' These will be used by sub CloseTask in clsAnalysisJob
					'
					' An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
					'  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
					'  "SEQUEST node count is less than the expected value"

					m_EvalMessage &= "; " & strProcessingMsg
					m_EvalCode = m_EvalCode Or ERROR_CODE_A
				Else
					If intNodeCountStarted <> intNodeCountActive Then
						strProcessingMsg = "Warning: NodeCountStarted (" & intNodeCountStarted & ") <> NodeCountActive"
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_B

						' Update the evaluation message and evaluation code
						' These will be used by sub CloseTask in clsAnalysisJob
						' An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
						'  "SEQUEST node count is less than the expected value"

					End If
				End If

				If dctHostCounts.Count < intHostCount Then
					' Only record an error here if the number of DTAs processed was at least 2x the number of nodes
					If intDTACount >= 2 * intNodeCountActive Then
						strProcessingMsg = "Error: only " & dctHostCounts.Count & " host" & CheckForPlurality(dctHostCounts.Count) & " processed DTAs"
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_C
					End If
				End If

				' See if any of the hosts processed far fewer or far more spectra than the other hosts
				' When comparing hosts, we need to scale by the number of active nodes on each host
				' We'll populate intHostProcessingRate() with the number of DTAs processed per node on each host

				Const LOW_THRESHOLD_MULTIPLIER As Single = 0.25
				Const HIGH_THRESHOLD_MULTIPLIER As Single = 4

				Dim intNodeCountThisHost As Integer

				Dim sngProcessingRate As Single
				Dim sngProcessingRateMedian As Single

				Dim sngThresholdRate As Single
				Dim intWarningCount As Integer

				For Each objItem As KeyValuePair(Of String, Integer) In dctHostCounts
					intNodeCountThisHost = 0
					dctHostNodeCount.TryGetValue(objItem.Key, intNodeCountThisHost)
					If intNodeCountThisHost < 1 Then intNodeCountThisHost = 1

					sngProcessingRate = CSng(objItem.Value / intNodeCountThisHost)
					dctHostProcessingRate.Add(objItem.Key, sngProcessingRate)
				Next

				' Determine the median number of spectra processed (ignoring the head nodes)
				Dim lstRatesFiltered As List(Of Single) = (From item In dctHostProcessingRate Where Not item.Key.ToLower().Contains("seqcluster") Select item.Value).ToList()
				sngProcessingRateMedian = ComputeMedian(lstRatesFiltered)

				' Only show warnings if sngProcessingRateMedian is at least 10; otherwise, we don't have enough sampling statistics
				If sngProcessingRateMedian >= 10 Then

					' Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the the median value
					intWarningCount = 0
					sngThresholdRate = CSng(LOW_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

					For Each objItem As KeyValuePair(Of String, Single) In dctHostProcessingRate
						If objItem.Value < sngThresholdRate AndAlso Not objItem.Key.ToLower().Contains("seqcluster") Then
							intWarningCount = +1
						End If
					Next

					If intWarningCount > 0 Then
						strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed fewer than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & LOW_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_D
						blnShowDetailedRates = True
					End If

					' Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value 
					' When comparing hosts, have to scale by the number of active nodes on each host
					intWarningCount = 0
					sngThresholdRate = CSng(HIGH_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

					For Each objItem As KeyValuePair(Of String, Single) In dctHostProcessingRate
						If objItem.Value > sngThresholdRate AndAlso Not objItem.Key.ToLower().Contains("seqcluster") Then
							intWarningCount = +1
						End If
					Next

					If intWarningCount > 0 Then
						strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed more than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & HIGH_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

						m_EvalMessage &= "; " & strProcessingMsg
						m_EvalCode = m_EvalCode Or ERROR_CODE_E
						blnShowDetailedRates = True
					End If
				End If

				If m_DebugLevel >= 2 OrElse blnShowDetailedRates Then
					' Log the number of DTAs processed by each host

					Dim qHosts = From item In dctHostCounts Select item Order By item.Key

					For Each objItem In qHosts
						intNodeCountThisHost = 0
						dctHostNodeCount.TryGetValue(objItem.Key, intNodeCountThisHost)
						If intNodeCountThisHost < 1 Then intNodeCountThisHost = 1

						sngProcessingRate = 0
						dctHostProcessingRate.TryGetValue(objItem.Key, sngProcessingRate)

						strProcessingMsg = "Host " & objItem.Key & " processed " & objItem.Value & " DTA" & CheckForPlurality(objItem.Value) & _
						  " using " & intNodeCountThisHost & " node" & CheckForPlurality(intNodeCountThisHost) & _
						  " (" & sngProcessingRate.ToString("0.0") & " DTAs/node)"

						If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
					Next

				End If

			Catch ex As Exception
				' Error occurred

				strProcessingMsg = "Error in validating the stats in ValidateSequestNodeCount" & ex.Message
				If blnLogToConsole Then
					Console.WriteLine("====================================================================")
					Console.WriteLine(strProcessingMsg)
					Console.WriteLine("====================================================================")
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
				Return False
			End Try

		Catch ex As Exception
			' Error occurred

			strProcessingMsg = "Error parsing Sequest.log file in ValidateSequestNodeCount" & ex.Message
			If blnLogToConsole Then
				Console.WriteLine("====================================================================")
				Console.WriteLine(strProcessingMsg)
				Console.WriteLine("====================================================================")
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
			Return False
		End Try

		Return True

	End Function

	Protected Function CheckForPlurality(ByVal intValue As Integer) As String
		If intValue = 1 Then
			Return String.Empty
		Else
			Return "s"
		End If
	End Function

	Protected Function ComputeMedian(ByVal lstValues As List(Of Single)) As Single

		Dim intMidpoint As Integer

		If lstValues.Count = 0 Then Return 0

		If lstValues.Count <= 2 Then
			intMidpoint = 0
		Else
			intMidpoint = CInt(Math.Floor(lstValues.Count / 2))
		End If

		Return (From item In lstValues Order By item).ToList().Item(intMidpoint)

	End Function

	''' <summary>
	''' Zips the concatenated .out file
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="JobNum">Job number</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function ZipConcatOutFile(ByVal WorkDir As String, ByVal JobNum As String) As Boolean

		Dim OutFileName As String = m_Dataset & "_out.txt"
		Dim OutFilePath As String = System.IO.Path.Combine(WorkDir, OutFileName)

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Zipping concatenated output file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

		'Verify file exists
		If Not System.IO.File.Exists(OutFilePath) Then
			m_message = "Unable to find concatenated .out file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Try
			'Zip the file            
			If Not MyBase.ZipFile(OutFilePath, False) Then
				m_message = "Error zipping concat out file"
				Dim Msg As String = m_message & ", job " & m_JobNum & ", step " & m_jobParams.GetParam("Step")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				Return False
			End If
		Catch ex As Exception
			m_message = "Exception zipping concat out file"
			Dim Msg As String = m_message & ", job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & _
			 ": " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return False
		End Try

		m_jobParams.AddResultFileToSkip(OutFileName)

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... successfully zipped")
		End If

		Return True

	End Function
#End Region

End Class
