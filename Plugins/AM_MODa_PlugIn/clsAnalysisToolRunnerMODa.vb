'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/26/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerMODa
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MODa analysis
	'*********************************************************************************************************

#Region "Constants and Enums"
	Protected Const MODa_CONSOLE_OUTPUT As String = "MODa_ConsoleOutput.txt"
	Protected Const MODa_FILTER_CONSOLE_OUTPUT As String = "MODa_Filter_ConsoleOutput.txt"
	Protected Const MODa_JAR_NAME As String = "moda.jar"
	Protected Const MODa_FILTER_JAR_NAME As String = "anal_moda.jar"

	Protected Const REGEX_MODa_PROGRESS As String = "MOD-A \| (\d+)/(\d+)"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_MODA_COMPLETE As Single = 95
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const MODA_RESULTS_FILE_SUFFIX As String = "_moda.txt"
	Protected Const MODA_FILTERED_RESULTS_FILE_SUFFIX As String = "_moda.id.txt"
#End Region

#Region "Module Variables"

	Protected mToolVersionWritten As Boolean
	Protected mMODaVersion As String

	Protected mMODaProgLoc As String
	Protected mConsoleOutputErrorMsg As String

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MODa tool
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMODa.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
			' Note that we need to run MODa with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc As String = m_mgrParams.GetParam("JavaLoc")
			If Not File.Exists(JavaProgLoc) Then
				If JavaProgLoc.Length = 0 Then JavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Java: " & JavaProgLoc)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MODa program
			' Note that 
			mMODaProgLoc = DetermineProgramLocation("MODa", "MODaProgLoc", Path.Combine("jar", MODa_JAR_NAME))

			If String.IsNullOrWhiteSpace(mMODaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Run MODa, then post process the results
			Dim blnSuccess = StartMODa(JavaProgLoc)

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			'Make sure objects are released
			Threading.Thread.Sleep(2000)		'2 second delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			' Trim the console output file to remove the majority of the status messages (since there is currently one per scan)
			TrimConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT))

			If Not blnSuccess Then
				' Move the source files and any results to the Failed Job folder
				' Useful for debugging MODa problems
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
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			m_message = "Error in MODaPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function StartMODa(ByVal JavaProgLoc As String) As Boolean

		Dim intJavaMemorySize As Integer
		Dim CmdStr As String
		Dim blnSuccess As Boolean

		' We will store the MODa version info in the database after the header block is written to file MODa_ConsoleOutput.txt

		mToolVersionWritten = False
		mMODaVersion = String.Empty
		mConsoleOutputErrorMsg = String.Empty

		' Customize the parameter file
		Dim paramFileName = m_jobParams.GetParam("ParmFileName")

		Dim spectrumFileName = m_Dataset & "_dta.txt"

		Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

		' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
		Dim dbFilename As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
		Dim fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename)

		If Not UpdateParameterFile(paramFileName, spectrumFileName, fastaFilePath) Then

		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MODa")

		' Lookup the amount of memory to reserve for Java; default to 2 GB 
		intJavaMemorySize = m_jobParams.GetJobParameter("MODaJavaMemorySize", 2000)
		If intJavaMemorySize < 512 Then intJavaMemorySize = 512

		Dim paramFilePath = Path.Combine(m_WorkDir, paramFileName)
		Dim modaResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & MODA_RESULTS_FILE_SUFFIX)

		'Set up and execute a program runner to run MODa
		CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & mMODaProgLoc
		CmdStr &= " -i " & paramFilePath
		CmdStr &= " -o " & modaResultsFilePath

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MODa", True)

		If Not mToolVersionWritten Then
			If String.IsNullOrWhiteSpace(mMODaVersion) Then
				ParseConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT))
			End If
			mToolVersionWritten = StoreToolVersionInfo()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		If Not blnSuccess Then
			Dim Msg As String
			Msg = "Error running MODa"
			m_message = clsGlobal.AppendToComment(m_message, Msg)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

			If CmdRunner.ExitCode <> 0 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MODa returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MODa failed (but exit code is 0)")
			End If

			Return False

		Else
			m_progress = PROGRESS_PCT_MODA_COMPLETE

			' Post-process the results to create a tab-delimited result file, filtering the identified peptides using the specified FDR value
			If Not PostProcessResults(modaResultsFilePath, modaResultsFilePath, paramFilePath) Then
				Return False
			End If

			m_progress = PROGRESS_PCT_COMPLETE
			m_StatusTools.UpdateAndWrite(m_progress)
			If m_DebugLevel >= 3 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MODa Search Complete")
			End If
		End If

		Return True

	End Function


	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		' Try to save whatever files are in the work directory (however, delete the .mzXML file first)
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		Try
			File.Delete(Path.Combine(m_WorkDir, m_Dataset & ".mzXML"))
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
				strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
			End If
		End If

		' Copy the results folder to the Archive folder
		Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
		objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

	End Sub

	''' <summary>
	''' Parse the MODa console output file to determine the MODa version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output
		'
		' *********************************************************
		' MODa v1.20: Multi-Blind Modification Search
		' Release Date: February 01, 2013
		' Hanyang University, Seoul, Korea
		' *********************************************************
		' 
		' Reading parameter.....
		' Input datasest : E:\DMS_WorkDir\QC_Shew_13_05b_CID_500ng_24Mar14_Tiger_14-03-04.mgf
		' Input datasest : E:\DMS_WorkDir\ID_003456_9B916A8B_Decoy_Scrambled.fasta
		' 
		' Starting MOD-Alignment for multi-blind modification search!
		' Performing mass correction for precursor
		' Reading MS/MS spectra.....  132 scans
		' Reading protein database.....  8632 proteins / 2820896 residues (1)

		' MOD-A | 1/132
		' MOD-A | 2/132
		' MOD-A | 3/132


		Static reExtractScan As New Regex(REGEX_MODa_PROGRESS, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

		Dim oMatch As Match

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
			Dim intLinesRead As Integer

			Dim intValue As Integer
			Dim intScansProcessed As Integer = 0
			Dim intTotalScans As Integer = 0
			Dim strMODaVersionAndDate As String = String.Empty

			Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				intLinesRead = 0
				Do While srInFile.Peek() >= 0
					strLineIn = srInFile.ReadLine()
					intLinesRead += 1

					If Not String.IsNullOrWhiteSpace(strLineIn) Then

						Dim strLineInLCase = strLineIn.ToLower()

						If intLinesRead < 6 AndAlso String.IsNullOrEmpty(strMODaVersionAndDate) AndAlso strLineInLCase.StartsWith("moda") Then
							strMODaVersionAndDate = String.Copy(strLineIn)
							Continue Do
						End If

						If intLinesRead < 6 AndAlso strLineInLCase.StartsWith("release date") Then
							strMODaVersionAndDate &= ", " & strLineIn
							Continue Do
						End If

						If strLineInLCase.StartsWith("abnormal termination") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MODa:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
							Continue Do
						End If

						If strLineInLCase.Contains("failed to read msms spectra file") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MODa:"
							End If
							mConsoleOutputErrorMsg &= "; Fasta file not found"
							Continue Do
						End If

						If strLineInLCase.Contains("exception") AndAlso strLineInLCase.StartsWith("java") Then
							If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
								mConsoleOutputErrorMsg = "Error running MODa:"
							End If
							mConsoleOutputErrorMsg &= "; " & strLineIn
							Continue Do
						End If

						oMatch = reExtractScan.Match(strLineIn)
						If oMatch.Success Then

							If Int32.TryParse(oMatch.Groups(1).Value, intValue) Then
								intScansProcessed = intValue
							End If

							If intTotalScans = 0 Then
								If Int32.TryParse(oMatch.Groups(2).Value, intValue) Then
									intTotalScans = intValue
								End If
							End If
						End If
					End If
				Loop

			End Using

			If intLinesRead >= 5 AndAlso String.IsNullOrEmpty(mMODaVersion) AndAlso Not String.IsNullOrEmpty(strMODaVersionAndDate) Then
				mMODaVersion = strMODaVersionAndDate
			End If

			Dim sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_MODA_COMPLETE, intScansProcessed, intTotalScans)

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

	Protected Function PostProcessResults(ByVal JavaProgLoc As String, ByVal modaResultsFilePath As String, ByVal paramFilePath As String) As Boolean

		Try
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MODa")

			Const intJavaMemorySize = 1000

			Dim fdrThreshold = m_jobParams.GetJobParameter("MODa_FDR_Threshold", 0.01)
			If Math.Abs(fdrThreshold) < Single.Epsilon Then
				fdrThreshold = 0.01
			ElseIf fdrThreshold > 1 Then
				fdrThreshold = 1
			End If

			Const decoyPrefix = "XXX_"
			Dim fiModA = New FileInfo(mMODaProgLoc)

			'Set up and execute a program runner to run MODa
			Dim CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & Path.Combine(fiModA.Directory.FullName, MODa_FILTER_JAR_NAME)
			CmdStr &= " -i " & modaResultsFilePath
			CmdStr &= " -p " & paramFilePath
			CmdStr &= " -fdr " & fdrThreshold
			CmdStr &= " -d " & decoyPrefix

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

			Dim progRunner = New clsRunDosProgram(m_WorkDir)

			With progRunner
				.CreateNoWindow = True
				.CacheStandardOutput = False
				.EchoOutputToConsole = True

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MODa_FILTER_CONSOLE_OUTPUT)
			End With


			Dim blnSuccess = progRunner.RunProgram(JavaProgLoc, CmdStr, "MODa_Filter", True)

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error filtering MODa results based on FDR"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, MODa_FILTER_JAR_NAME & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & MODa_FILTER_JAR_NAME & " failed (but exit code is 0)")
				End If

				Return False

			End If

		Catch ex As Exception
			m_message = "Error in MODaPlugin->PostProcessResults"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try


		Return True

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

		strToolVersionInfo = String.Copy(mMODaVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(mMODaProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Reads the console output file and removes the majority of the percent finished messages
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub TrimConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Look for lines of the form MOD-A | 6947/13253
		Static reExtractScan As New Regex(REGEX_MODa_PROGRESS, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
		Dim oMatch As Match

		Try
			If Not File.Exists(strConsoleOutputFilePath) Then
				If m_DebugLevel >= 4 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
				End If

				Exit Sub
			End If

			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Trimming console output file at " & strConsoleOutputFilePath)
			End If

			Dim intScanNumber As Integer

			Dim intScanNumberOutputThreshold As Integer

			Dim fiConsoleOutputFile = New FileInfo(strConsoleOutputFilePath)
			Dim fiTrimmedFilePath = New FileInfo(strConsoleOutputFilePath & ".trimmed")

			Using srInFile = New StreamReader(New FileStream(fiConsoleOutputFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
				Using swOutFile = New StreamWriter(New FileStream(fiTrimmedFilePath.FullName, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					intScanNumberOutputThreshold = 0
					Do While srInFile.Peek() >= 0
						Dim strLineIn = srInFile.ReadLine()
						Dim blnKeepLine = True

						oMatch = reExtractScan.Match(strLineIn)
						If oMatch.Success Then
							If Integer.TryParse(oMatch.Groups(1).Value, intScanNumber) Then
								If intScanNumber < intScanNumberOutputThreshold Then
									blnKeepLine = False
								Else
									' Write out this line and bump up intScanNumberOutputThreshold by 100
									intScanNumberOutputThreshold += 100
								End If
							End If

						End If

						If blnKeepLine Then
							swOutFile.WriteLine(strLineIn)
						End If
					Loop

				End Using
			End Using
			
			' Replace the original file with the new one
			ReplaceUpdatedFile(fiConsoleOutputFile, fiTrimmedFilePath)

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error trimming console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

	Private Function ReplaceUpdatedFile(ByVal fiOrginalFile As FileInfo, ByVal fiUpdatedFile As FileInfo) As Boolean

		Try
			Dim finalFilePath = fiOrginalFile.FullName

			Threading.Thread.Sleep(250)
			fiOrginalFile.Delete()

			Threading.Thread.Sleep(250)
			fiUpdatedFile.MoveTo(finalFilePath)

		Catch ex As Exception
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ReplaceUpdatedFile: " & ex.Message)
			End If

			Return False
		End Try

		Return True

	End Function


	Protected Function UpdateParameterFile(ByVal paramFileName As String, ByVal spectrumFileName As String, ByVal fastaFilePath As String) As Boolean

		Const SPEC_FILE_PATH As String = "Spectra"
		Const FASTA_FILE_PATH As String = "Fasta"

		Dim specFileDefined = False
		Dim fastaFileDefined = False

		Try

			Dim fiSourceParamFile = New FileInfo(Path.Combine(m_WorkDir, paramFileName))
			Dim fiTempParamFile = New FileInfo(Path.Combine(m_WorkDir, paramFileName & ".temp"))

			Dim fiSpecFile = New FileInfo(Path.Combine(m_WorkDir, spectrumFileName))

			' Open the input file
			Using srInFile = New StreamReader(New FileStream(fiSourceParamFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
				' Create the output file

				Using swOutFile = New StreamWriter(New FileStream(fiTempParamFile.FullName, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srInFile.Peek > -1
						Dim strLineIn = srInFile.ReadLine()

						If strLineIn.TrimStart().StartsWith("#") OrElse String.IsNullOrWhiteSpace(strLineIn) Then
							' Comment line or blank line; write it out as-is
							swOutFile.WriteLine(strLineIn)
							Continue Do
						End If

						' Look for an equals sign
						Dim intEqualsIndex = strLineIn.IndexOf("="c)

						If intEqualsIndex <= 0 Then
							' Unknown line format; skip it
							Continue Do
						End If

						' Split the line on the equals sign
						Dim strKeyName = strLineIn.Substring(0, intEqualsIndex).TrimEnd()

						' Examine the key name to determine what to do
						Select Case strKeyName.ToLower()
							Case SPEC_FILE_PATH.ToLower()
								strLineIn = SPEC_FILE_PATH & "=" & fiSpecFile.FullName
								specFileDefined = True

							Case FASTA_FILE_PATH.ToLower()
								strLineIn = FASTA_FILE_PATH & "=" & fastaFilePath
								fastaFileDefined = True

						End Select

						swOutFile.WriteLine(strLineIn)

					Loop

					If Not specFileDefined Then
						swOutFile.WriteLine()
						swOutFile.WriteLine(SPEC_FILE_PATH & "=" & fiSpecFile.FullName)
					End If

					If Not fastaFileDefined Then
						swOutFile.WriteLine()
						swOutFile.WriteLine(FASTA_FILE_PATH & "=" & fastaFilePath)
					End If

				End Using
			End Using

			' Replace the original parameter file with the updated one
			If Not ReplaceUpdatedFile(fiSourceParamFile, fiTempParamFile) Then
				m_message = "Error replacing the original parameter file with the customized version"
				Return False
			End If

		Catch ex As Exception
			m_message = "Exception in UpdateParameterFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in UpdateParameterFile: " & ex.Message)
			Return False
		End Try

		Return True

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

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT))

			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMODaVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

		End If

	End Sub

#End Region

End Class
