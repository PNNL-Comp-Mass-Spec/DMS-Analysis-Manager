'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.Linq
Imports MSDataFileReader
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerMSDeconv
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSDeconv analysis
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const MSDECONV_CONSOLE_OUTPUT As String = "MSDeconv_ConsoleOutput.txt"
	Protected Const MSDECONV_JAR_NAME As String = "MsDeconvConsole.jar"

	Protected Const PROGRESS_PCT_STARTING As Single = 1
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected mToolVersionWritten As Boolean
	Protected mMSDeconvVersion As String			' Populate this with a tool version reported to the console

	Protected mMSDeconvProgLoc As String
	Protected mConsoleOutputErrorMsg As String

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSDeconv tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSDeconv.RunTool(): Enter")
			End If

			' Verify that program files exist

			' JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
			' Note that we need to run MSDeconv with a 64-bit version of Java since it prefers to use 2 or more GB of ram
			Dim JavaProgLoc = GetJavaProgLoc()
			If String.IsNullOrEmpty(JavaProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Examine the mzXML file to look for large scan gaps (common for data from Agilent IMS TOFs, e.g. AgQTOF05)
			' Possibly generate a new mzXML file with renumbered scans
			Dim blnSuccess = RenumberMzXMLIfRequired()
			If Not blnSuccess Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "RenumberMzXMLIfRequired returned false"
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Determine the path to the MSDeconv program
			mMSDeconvProgLoc = DetermineProgramLocation("MSDeconv", "MSDeconvProgLoc", MSDECONV_JAR_NAME)

			If String.IsNullOrWhiteSpace(mMSDeconvProgLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim strOutputFormat = m_jobParams.GetParam("MSDeconvOutputFormat")
			Dim resultsFileName As String = "unknown"

			If String.IsNullOrEmpty(strOutputFormat) Then
				strOutputFormat = "msalign"
			End If

			Select Case strOutputFormat.ToLower()
				Case "mgf"
					strOutputFormat = "mgf"
					resultsFileName = m_Dataset & "_msdeconv.mgf"
				Case "text"
					strOutputFormat = "text"
					resultsFileName = m_Dataset & "_msdeconv.txt"
				Case "msalign"
					strOutputFormat = "msalign"
					resultsFileName = m_Dataset & "_msdeconv.msalign"
				Case Else
					m_message = "Invalid output format: " & strOutputFormat
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

			blnSuccess = StartMSDeconv(JavaProgLoc, strOutputFormat)

			Dim blnProcessingError = False

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running MSDeconv"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSDeconv returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSDeconv failed (but exit code is 0)")
				End If

				blnProcessingError = True

			Else
				' Make sure the output file was created and is not zero-bytes
				' If the input .mzXML file only has MS spectra and no MS/MS spectra, then the output file will be empty
				Dim ioResultsFile As FileInfo
				ioResultsFile = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))
				If Not ioResultsFile.Exists() Then
					Dim Msg As String
					Msg = "MSDeconv results file not found"
					m_message = clsGlobal.AppendToComment(m_message, Msg)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & " (" & resultsFileName & ")" & ", job " & m_JobNum)

					blnProcessingError = True
				ElseIf ioResultsFile.Length = 0 Then
					Dim Msg As String
					Msg = "MSDeconv results file is empty; assure that the input .mzXML file has MS/MS spectra"
					m_message = clsGlobal.AppendToComment(m_message, Msg)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & " (" & resultsFileName & ")" & ", job " & m_JobNum)

					blnProcessingError = True
				Else
					m_StatusTools.UpdateAndWrite(m_progress)
					If m_DebugLevel >= 3 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSDeconv Search Complete")
					End If
				End If

			End If

			m_progress = PROGRESS_PCT_COMPLETE

			'Stop the job timer
			m_StopTime = DateTime.UtcNow

			'Add the current job data to the summary file
			If Not UpdateSummaryFile() Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
			End If

			CmdRunner = Nothing

			'Make sure objects are released
			Threading.Thread.Sleep(500)        ' 500 msec delay
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			' Trim the console output file to remove the majority of the % finished messages
			TrimConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT))

			If blnProcessingError Then
				' Something went wrong
				' In order to help diagnose things, we will move whatever files were created into the result folder, 
				'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
				CopyFailedResultsToArchiveFolder()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim result = MakeResultsFolder()
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
			m_message = "Error in MSDeconvPlugin->RunTool: " & ex.Message
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

		' Try to save whatever files are in the work directory (however, delete the .mzXML file first)
		Dim strFolderPathToArchive As String
		strFolderPathToArchive = String.Copy(m_WorkDir)

		Try
			File.Delete(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION))
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
	''' Parse the MSDeconv console output file to determine the MSDeconv version and to track the search progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output:
		'
		' ********* parameters begin **********
		' output file format:    msalign
		' data type:             centroided
		' orignal precursor:     false
		' maximum charge:        30
		' maximum mass:          49000.0
		' m/z error tolerance:   0.02
		' sn ratio:              1.0
		' keep unused peak:      false
		' output multiple mass:  false
		' ********* parameters end   **********
		' Processing spectrum Scan_2...           0% finished.
		' Processing spectrum Scan_3...           0% finished.
		' Processing spectrum Scan_4...           0% finished.
		' Deconvolution finished.
		' Result is in Syne_LI_CID_09092011_msdeconv.msalign

		Static reExtractPercentFinished As New Regex("(\d+)% finished", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

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

			Dim intProgress As Int16
			Dim intActualProgress As Int16

			mConsoleOutputErrorMsg = String.Empty
			
            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                intLinesRead = 0
                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()
                    intLinesRead += 1

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then
                        If intLinesRead = 1 Then
                            ' Parse out the MSDeconv version
                            If strLineIn.ToLower.Contains("deconv") Then
                                If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mMSDeconvVersion) Then
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSDeconv version: " & strLineIn)
                                End If

                                mMSDeconvVersion = String.Copy(strLineIn)
                            Else
                                If strLineIn.ToLower.Contains("error") Then
                                    If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                        mConsoleOutputErrorMsg = "Error running MSDeconv:"
                                    End If
                                    mConsoleOutputErrorMsg &= "; " & strLineIn
                                End If
                            End If
                        Else

                            ' Update progress if the line starts with Processing spectrum
                            If strLineIn.StartsWith("Processing spectrum") Then
                                oMatch = reExtractPercentFinished.Match(strLineIn)
                                If oMatch.Success Then
                                    If Int16.TryParse(oMatch.Groups(1).Value, intProgress) Then
                                        intActualProgress = intProgress
                                    End If
                                End If

                            ElseIf String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                If strLineIn.ToLower.StartsWith("error") Then
                                    mConsoleOutputErrorMsg &= "; " & strLineIn
                                End If
                            End If

                        End If
                    End If
                Loop

            End Using

            If m_progress < intActualProgress Then
                m_progress = intActualProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

	End Sub

	Private Function RenumberMzXMLIfRequired() As Boolean
		Try
			Dim mzXmlFileName = m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION
			Dim fiMzXmlFile = New FileInfo(Path.Combine(m_WorkDir, mzXmlFileName))

			If Not fiMzXmlFile.Exists Then
				m_message = "mzXML file not found, " & fiMzXmlFile.FullName
				Return False
			End If

			Dim reader = New clsMzXMLFileReader()
			reader.OpenFile(fiMzXmlFile.FullName)

			' Read the spectra and examine the scan gaps

			Dim lstScanGaps = New List(Of Integer)
			Dim objSpectrumInfo As clsSpectrumInfo = Nothing
			Dim lastScanNumber = 0

			While reader.ReadNextSpectrum(objSpectrumInfo)
				If lastScanNumber > 0 Then
					lstScanGaps.Add(objSpectrumInfo.ScanNumber - lastScanNumber)
				End If

				lastScanNumber = objSpectrumInfo.ScanNumber
			End While

			reader.CloseFile()

			If lstScanGaps.Count > 0 Then
				' Compute the average scan gap
				Dim scanGapSum As Integer = lstScanGaps.Sum()
				Dim scanGapAverage = scanGapSum / CDbl(lstScanGaps.Count())

				If scanGapAverage >= 2 Then
					' Renumber the .mzXML file
					' May need to renumber if the scan gap is every greater than one; not sure

					Threading.Thread.Sleep(200)

					' Rename the file 
					fiMzXmlFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset & "_old" & clsAnalysisResources.DOT_MZXML_EXTENSION))
					fiMzXmlFile.Refresh()
					m_jobParams.AddResultFileToSkip(fiMzXmlFile.Name)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "The mzXML file has an average scan gap of " & scanGapAverage.ToString("0.0") & " scans; will update the file's scan numbers to be 1, 2, 3, etc.")

					Dim converter = New clsRenumberMzXMLScans(fiMzXmlFile.FullName)
					Dim targetFilePath = Path.Combine(m_WorkDir, mzXmlFileName)
					Dim blnSuccess = converter.Process(targetFilePath)

					If Not blnSuccess Then
						m_message = converter.ErrorMessage
						If String.IsNullOrEmpty(m_message) Then
							m_message = "clsRenumberMzXMLScans returned false while renumbering the scans in the .mzXML file"
						End If

						Return False
					End If

					m_jobParams.AddResultFileToSkip(targetFilePath)

				End If
			End If


			Return True

		Catch ex As Exception
			m_message = "Error renumbering the mzXML file: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RenumberMzXMLIfRequired", ex)
			Return False
		End Try

	End Function

	Private Function StartMSDeconv(ByVal JavaProgLoc As String, ByVal strOutputFormat As String) As Boolean

		' Store the MSDeconv version info in the database after the first line is written to file MSDeconv_ConsoleOutput.txt
		mToolVersionWritten = False
		mMSDeconvVersion = String.Empty
		mConsoleOutputErrorMsg = String.Empty

		Dim blnIncludeMS1Spectra = m_jobParams.GetJobParameter("MSDeconvIncludeMS1", False)

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSDeconv")

		' Lookup the amount of memory to reserve for Java; default to 2 GB 
		Dim intJavaMemorySize = m_jobParams.GetJobParameter("MSDeconvJavaMemorySize", 2000)
		If intJavaMemorySize < 512 Then intJavaMemorySize = 512

		'Set up and execute a program runner to run MSDeconv
		Dim CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & mMSDeconvProgLoc


		' Define the input file and processing options
		' Note that capitalization matters for the extension; it must be .mzXML
		CmdStr &= " " & m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION
		CmdStr &= " -o " & strOutputFormat & " -t centroided"

		If blnIncludeMS1Spectra Then
			CmdStr &= " -l"
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

		CmdRunner = New clsRunDosProgram(m_WorkDir)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = False
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT)
		End With

		m_progress = PROGRESS_PCT_STARTING

		Dim blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSDeconv", True)

		If Not mToolVersionWritten Then
			If String.IsNullOrWhiteSpace(mMSDeconvVersion) Then
				ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT))
			End If
			mToolVersionWritten = StoreToolVersionInfo()
		End If

		If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
		End If

		Return blnSuccess

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo() As Boolean

		Dim strToolVersionInfo As String = String.Empty

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		strToolVersionInfo = String.Copy(mMSDeconvVersion)

		' Store paths to key files in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(mMSDeconvProgLoc))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
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

		Static reExtractScan As New Regex("Processing spectrum Scan_(\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
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

			Dim srInFile As StreamReader
			Dim swOutFile As StreamWriter

			Dim strLineIn As String
			Dim blnKeepLine As Boolean

			Dim intScanNumber As Integer
			Dim strMostRecentProgressLine As String = String.Empty
			Dim strMostRecentProgressLineWritten As String = String.Empty

			Dim intScanNumberOutputThreshold As Integer

			Dim strTrimmedFilePath As String
			strTrimmedFilePath = strConsoleOutputFilePath & ".trimmed"

			srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))
			swOutFile = New StreamWriter(New FileStream(strTrimmedFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

			intScanNumberOutputThreshold = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				blnKeepLine = True

				oMatch = reExtractScan.Match(strLineIn)
				If oMatch.Success Then
					If Integer.TryParse(oMatch.Groups(1).Value, intScanNumber) Then
						If intScanNumber < intScanNumberOutputThreshold Then
							blnKeepLine = False
						Else
							' Write out this line and bump up intScanNumberOutputThreshold by 100
							intScanNumberOutputThreshold += 100
							strMostRecentProgressLineWritten = String.Copy(strLineIn)
						End If
					End If
					strMostRecentProgressLine = String.Copy(strLineIn)

				ElseIf strLineIn.StartsWith("Deconvolution finished") Then
					' Possibly write out the most recent progress line
					If String.Compare(strMostRecentProgressLine, strMostRecentProgressLineWritten) <> 0 Then
						swOutFile.WriteLine(strMostRecentProgressLine)
					End If
				End If

				If blnKeepLine Then
					swOutFile.WriteLine(strLineIn)
				End If
			Loop

			srInFile.Close()
			swOutFile.Close()

			' Wait 500 msec, then swap the files
			System.Threading.Thread.Sleep(500)

			Try
				File.Delete(strConsoleOutputFilePath)
				File.Move(strTrimmedFilePath, strConsoleOutputFilePath)
			Catch ex As Exception
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error replacing original console output file (" & strConsoleOutputFilePath & ") with trimmed version: " & ex.Message)
				End If
			End Try

		Catch ex As Exception
			' Ignore errors here
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error trimming console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
			End If
		End Try

	End Sub

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

        UpdateStatusFile()

		If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
			dtLastConsoleOutputParse = System.DateTime.UtcNow

			ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSDECONV_CONSOLE_OUTPUT))

			If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSDeconvVersion) Then
				mToolVersionWritten = StoreToolVersionInfo()
			End If

            LogProgress("MSDeconv")
		End If

	End Sub

#End Region

End Class
