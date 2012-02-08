Option Strict On

'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2010, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisToolRunnerMultiAlignAggregator
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    ' Class for running MultiAlignAggregator analysis
    '*********************************************************************************************************

#Region "Module Variables"
	'Protected Const PROGRESS_PCT_MULTIALIGN_RUNNING As Single = 5
	'Protected Const PROGRESS_PCT_MULTI_ALIGN_DONE As Single = 95

	' This dictionary defines the % complete values for each of the progress steps
	' It is populated by sub InitializeProgressStepDictionaries
	Protected mProgressStepPercentComplete As System.Collections.Generic.SortedDictionary(Of eProgressSteps, Int16)

	' This dictionary associates key log text entries with the corresponding progress step for each
	' It is populated by sub InitializeProgressStepDictionaries
	Protected mProgressStepLogText As System.Collections.Generic.SortedDictionary(Of String, eProgressSteps)

	Protected Enum eProgressSteps
		Starting = 0
		LoadingMTDB = 1
		LoadingDatasets = 2
		LinkingMSFeatures = 3
		AligningDatasets = 4
		PerformingClustering = 5
		PerformingPeakMatching = 6
		CreatingFinalPlots = 7
		CreatingReport = 8
		Complete = 9
	End Enum

	Protected m_MultialignErroMessage As String = String.Empty
    Protected WithEvents CmdRunner As clsRunDosProgram  

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MultiAlign Aggregator tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType
        Dim blnSuccess As Boolean

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MultiAlign")

		InitializeProgressStepDictionaries()

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMultiAlignAggregator.RunTool(): Enter")
        End If

        ' Determine the path to the MultiAlign folder
        Dim progLoc As String
        progLoc = DetermineProgramLocation("MultiAlign", "MultiAlignProgLoc", "MultiAlignConsole.exe")

        If String.IsNullOrWhiteSpace(progLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the MultiAlign version info in the database
		If Not StoreToolVersionInfo(progLoc) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining MultiAlign version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Dim MultiAlignResultFilename As String = m_jobParams.GetParam("ResultFilename")

        If String.IsNullOrWhiteSpace(MultiAlignResultFilename) Then
            MultiAlignResultFilename = m_Dataset
        End If

        ' Set up and execute a program runner to run MultiAlign
        CmdStr = " -files " & clsAnalysisResourcesMultiAlignAggregator.MULTIALIGN_INPUT_FILE & " -params " & System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName")) & " -path " & m_WorkDir & " -name " & MultiAlignResultFilename & " -plots"
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = False
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "MultiAlign", True) Then
			m_message = "Error running MultiAlign"
			If Not String.IsNullOrEmpty(m_MultialignErroMessage) Then
				m_message &= ": " & m_MultialignErroMessage
			End If
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
            blnSuccess = False
        Else
            blnSuccess = True
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow
		m_progress = mProgressStepPercentComplete.Item(eProgressSteps.Complete)

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        If Not blnSuccess Then
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging MultiAlign problems
            CopyFailedResultsToArchiveFolder()
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        'Rename the log file so it is consistent with other log files. MultiAlign will add ability to specify log file name
        RenameLogFile(MultiAlignResultFilename)

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        ' Move the Plots folder to the result files folder
        Dim diPlotsFolder As System.IO.DirectoryInfo
        diPlotsFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"))

        Dim strTargetFolderPath As String
        strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots")
        diPlotsFolder.MoveTo(strTargetFolderPath)

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Function RenameLogFile(ByVal LogFilename As String) As IJobParams.CloseOutType

        Dim TmpFile As String = String.Empty
        Dim Files As String()
        Dim LogExtension As String = "-log.txt"
        Dim NewFilename As String = LogFilename & LogExtension
        'This is what MultiAlign is currently naming the log file
        Dim LogNameFilter As String = LogFilename & ".db3-log*.txt"
        Try
            'Get the log file name.  There should only be one log file
            Files = Directory.GetFiles(m_WorkDir, LogNameFilter)
            'go through each log file found.  Again, there should only be one log file
            For Each TmpFile In Files
                'Check to see if the log file exists.  If so, only rename one of them
                If Not File.Exists(NewFilename) Then
                    My.Computer.FileSystem.RenameFile(TmpFile, NewFilename)
                End If
            Next

        Catch ex As Exception
            'Even if the rename failed, go ahead and continue

        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the .UIMF file first, plus also the Decon2LS .csv files)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & ".UIMF"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "*.csv"))
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

	''' <summary>
	''' Populates dictionary mProgressStepPercentComplete(), which is used by ParseMultiAlignLogFile
	''' </summary>
	''' <remarks></remarks>
	Private Sub InitializeProgressStepDictionaries()

		mProgressStepPercentComplete = New System.Collections.Generic.SortedDictionary(Of eProgressSteps, Int16)

		mProgressStepPercentComplete.Add(eProgressSteps.Starting, 5)
		mProgressStepPercentComplete.Add(eProgressSteps.LoadingMTDB, 6)
		mProgressStepPercentComplete.Add(eProgressSteps.LoadingDatasets, 7)
		mProgressStepPercentComplete.Add(eProgressSteps.LinkingMSFeatures, 45)
		mProgressStepPercentComplete.Add(eProgressSteps.AligningDatasets, 50)
		mProgressStepPercentComplete.Add(eProgressSteps.PerformingClustering, 75)
		mProgressStepPercentComplete.Add(eProgressSteps.PerformingPeakMatching, 85)
		mProgressStepPercentComplete.Add(eProgressSteps.CreatingFinalPlots, 90)
		mProgressStepPercentComplete.Add(eProgressSteps.CreatingReport, 95)
		mProgressStepPercentComplete.Add(eProgressSteps.Complete, 97)

		mProgressStepLogText = New System.Collections.Generic.SortedDictionary(Of String, eProgressSteps)
		mProgressStepLogText.Add("[LogStart]", eProgressSteps.Starting)
		mProgressStepLogText.Add(" - Loading Mass Tag database from database", eProgressSteps.LoadingMTDB)
		mProgressStepLogText.Add(" - Loading dataset data files", eProgressSteps.LoadingDatasets)
		mProgressStepLogText.Add(" - Linking MS Features", eProgressSteps.LinkingMSFeatures)
		mProgressStepLogText.Add(" - Aligning datasets", eProgressSteps.AligningDatasets)
		mProgressStepLogText.Add(" - Performing clustering", eProgressSteps.PerformingClustering)
		mProgressStepLogText.Add(" - Performing Peak Matching", eProgressSteps.PerformingPeakMatching)
		mProgressStepLogText.Add(" - Creating Final Plots", eProgressSteps.CreatingFinalPlots)
		mProgressStepLogText.Add(" - Creating report", eProgressSteps.CreatingReport)
		mProgressStepLogText.Add(" - Analysis Complete", eProgressSteps.Complete)

	End Sub

	''' <summary>
	''' Parse the MultiAlign log file to track the search progress
	''' Looks in the work directory to auto-determine the log file name
	''' </summary>
	''' <remarks></remarks>
	Private Sub ParseMultiAlignLogFile()

		Dim diWorkDirectory As System.IO.DirectoryInfo
		Dim fiFiles() As System.IO.FileInfo
		Dim strLogFilePath As String = String.Empty

		Try
			diWorkDirectory = New System.IO.DirectoryInfo(m_WorkDir)
			fiFiles = diWorkDirectory.GetFiles("*-log*.txt")

			If fiFiles.Length >= 1 Then
				strLogFilePath = fiFiles(0).FullName

				If fiFiles.Length > 1 Then
					' Use the newest file in fiFiles
					Dim intBestIndex As Integer = 0

					For intIndex As Integer = 1 To fiFiles.Length - 1
						If fiFiles(intIndex).LastWriteTimeUtc > fiFiles(intBestIndex).LastWriteTimeUtc Then
							intBestIndex = intIndex
						End If
					Next

					strLogFilePath = fiFiles(intBestIndex).FullName
				End If

			End If

			If Not String.IsNullOrWhiteSpace(strLogFilePath) Then
				ParseMultiAlignLogFile(strLogFilePath)
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error finding the MultiAlign log file at " & m_WorkDir & ": " & ex.Message)
			End If

		End Try

	End Sub

	''' <summary>
	''' Parse the MultiAlign log file to track the search progress
	''' </summary>
	''' <param name="strLogFilePath">Full path to the log file</param>
	''' <remarks></remarks>
	Private Sub ParseMultiAlignLogFile(ByVal strLogFilePath As String)

		' The MultiAlign log file is quite big, but we can keep track of progress by looking for known text in the log file lines
		' Dictionary mProgressStepLogText keeps track of the lines of text to match while mProgressStepPercentComplete keeps track of the % complete values to use

		' For certain long-running steps we can compute a more precise version of % complete by keeping track of the number of datasets processed

		'Static reExtractPercentFinished As New System.Text.RegularExpressions.Regex("(\d+)% finished", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow

		'Dim oMatch As System.Text.RegularExpressions.Match

		Try
			If Not System.IO.File.Exists(strLogFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MultiAlign log file not found: " & strLogFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strLogFilePath)
			End If


			Dim srInFile As System.IO.StreamReader
			Dim strLineIn As String
			Dim intLinesRead As Integer

			Dim eProgress As eProgressSteps = eProgressSteps.Starting

			Dim blnMatchFound As Boolean = False
			Dim intTotalDatasets As Integer = 0
			Dim intDatasetsLoaded As Integer = 0
			Dim intDatasetsAligned As Integer = 0
			Dim intChargeStatesClustered As Integer = 0

			' Open the file for read; don't lock it (to thus allow MultiAlign to still write to it)
			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() > -1
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					blnMatchFound = False

					' Update progress if the line contains any of the entries in mProgressStepLogText
					For Each lstItem As System.Collections.Generic.KeyValuePair(Of String, eProgressSteps) In mProgressStepLogText
						If strLineIn.Contains(lstItem.Key) Then
							If eProgress < lstItem.Value Then
								eProgress = lstItem.Value
							End If
							blnMatchFound = True
							Exit For
						End If
					Next

					If Not blnMatchFound Then
						If strLineIn.Contains("Dataset Information: ") Then
							intTotalDatasets += 1
						ElseIf strLineIn.Contains("- Adding features to cache database") Then
							intDatasetsLoaded += 1
						ElseIf strLineIn.Contains("- Features Aligned -") Then
							intDatasetsAligned += 1
						ElseIf strLineIn.Contains("- Clustering Charge State") Then
							intChargeStatesClustered += 1
						ElseIf strLineIn.Contains("No baseline dataset or database was selected") Then
							m_MultialignErroMessage = "No baseline dataset or database was selected"
						End If
					End If

				End If
			Loop

			srInFile.Close()

			' Compute the actual progress
			Dim intActualProgress As Int16
			If mProgressStepPercentComplete.TryGetValue(eProgress, intActualProgress) Then

				Dim sngActualProgress As Single = intActualProgress

				' Possibly bump up dblActualProgress incrementally

				If intTotalDatasets > 0 Then

					' This is a number between 0 and 100
					Dim dblSubProgressPercent As Double = 0

					If eProgress = eProgressSteps.LoadingDatasets Then
						dblSubProgressPercent = intDatasetsLoaded * 100 / intTotalDatasets

					ElseIf eProgress = eProgressSteps.AligningDatasets Then
						dblSubProgressPercent = intDatasetsAligned * 100 / intTotalDatasets

					ElseIf eProgress = eProgressSteps.PerformingClustering Then
						' The majority of the data will be charge 1 through 7
						' Thus, we're dividing by 7 here, which means dblSubProgressPercent might be larger than 100; we'll account for that below
						dblSubProgressPercent = intChargeStatesClustered * 100 / 7
					End If

					If dblSubProgressPercent > 0 Then
						If dblSubProgressPercent > 100 Then dblSubProgressPercent = 100

						' Bump up dblActualProgress based on dblSubProgressPercent
						Dim intProgressNext As Int16

						If mProgressStepPercentComplete.TryGetValue(CType(eProgress + 1, eProgressSteps), intProgressNext) Then
							sngActualProgress += CSng(dblSubProgressPercent * (intProgressNext - intActualProgress) / 100.0)
						End If

					End If

				End If

				If m_progress < sngActualProgress Then
					m_progress = sngActualProgress

					If m_DebugLevel >= 3 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 10 Then
						dtLastProgressWriteTime = System.DateTime.UtcNow
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0.0") & "% complete")
					End If

				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MultiAlign log file (" & strLogFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strMultiAlignProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim ioMultiAlignProg As System.IO.FileInfo
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioMultiAlignProg = New System.IO.FileInfo(strMultiAlignProgLoc)
		If Not ioMultiAlignProg.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		' Lookup the version of MultiAlign 
		blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, ioMultiAlignProg.FullName)
		If Not blnSuccess Then Return False

		' Lookup the version of additional DLLs
		blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignCore.dll"))
		If Not blnSuccess Then Return False

		blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLControls.dll"))
		If Not blnSuccess Then Return False

		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll")))
		ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll")))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow
		Static dtLastMultialignLogFileParse As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress)
		End If

		If System.DateTime.UtcNow.Subtract(dtLastMultialignLogFileParse).TotalSeconds >= 15 Then
			dtLastMultialignLogFileParse = System.DateTime.UtcNow
			ParseMultiAlignLogFile()
		End If

	End Sub


#End Region

End Class
