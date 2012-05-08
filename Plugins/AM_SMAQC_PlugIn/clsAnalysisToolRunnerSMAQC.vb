Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerSMAQC
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running SMAQC
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const SMAQC_CONSOLE_OUTPUT As String = "SMAQC_ConsoleOutput.txt"
	Protected Const PROGRESS_PCT_SMAQC_STARTING As Single = 1
	Protected Const PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES As Single = 5
	Protected Const PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES As Single = 10
	Protected Const PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS As Single = 15
	Protected Const PROGRESS_PCT_SMAQC_SAVING_RESULTS As Single = 95
	Protected Const PROGRESS_PCT_SMAQC_COMPLETE As Single = 98
	Protected Const PROGRESS_PCT_COMPLETE As Single = 99

	Protected Const STORE_SMAQC_RESULTS_SP_NAME As String = "StoreSMAQCResults"

	Protected mConsoleOutputErrorMsg As String
	Protected mDatasetID As Integer = 0

	Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"
	Protected Structure udtSMAQCResultType
		Public Name As String
		Public Value As String
		Public Sub Clear()
			Name = String.Empty
			Value = String.Empty
		End Sub
	End Structure
#End Region

#Region "Methods"
	''' <summary>
	''' Runs SMAQC tool
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
		Dim ResultsFilePath As String

		Try
			'Call base class for initial setup
			If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSMAQC.RunTool(): Enter")
			End If

			' Determine the path to the SMAQC program
			Dim progLoc As String
			progLoc = DetermineProgramLocation("SMAQC", "SMAQCProgLoc", "SMAQC.exe")

			If String.IsNullOrWhiteSpace(progLoc) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Store the SMAQC version info in the database
			If Not StoreToolVersionInfo(progLoc) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
				m_message = "Error determining SMAQC version"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			End If

			mConsoleOutputErrorMsg = String.Empty

			' The parameter file name specifies the name of the .XML file listing the Measurements to run
			strParameterFileName = m_jobParams.GetParam("parmFileName")
			strParameterFilePath = System.IO.Path.Combine(m_WorkDir, strParameterFileName)

			' Lookup the InstrumentID for this dataset			
			Dim InstrumentID As Integer = 0
			If Not LookupInstrumentIDFromDB(InstrumentID) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			ResultsFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_SMAQC.txt")

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running SMAQC")


			'Set up and execute a program runner to run SMAQC
			CmdStr = " -i " & InstrumentID.ToString()
			CmdStr &= " -d " & PossiblyQuotePath(m_WorkDir)					' Path to folder containing input files
			CmdStr &= " -m " & PossiblyQuotePath(strParameterFilePath)		' Path to XML file specifying measurements to run	
			CmdStr &= " -o " & PossiblyQuotePath(ResultsFilePath)			' Text file to write the results to
			CmdStr &= " -db " & PossiblyQuotePath(m_WorkDir)				' Folder where SQLite DB will be created

			m_jobParams.AddResultFileToSkip("SMAQC.s3db")				' Don't keep the SQLite DB

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
			End If

			CmdRunner = New clsRunDosProgram(m_WorkDir)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				' Future ToDo: Create a console output file; can't do so at present since SMAQC crashes

				.WriteConsoleOutputToFile = True
				.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, SMAQC_CONSOLE_OUTPUT)
			End With

			' We will delete the console output file later since it has the same content as the log file
			m_jobParams.AddResultFileToSkip(SMAQC_CONSOLE_OUTPUT)

			m_progress = PROGRESS_PCT_SMAQC_STARTING

			blnSuccess = CmdRunner.RunProgram(progLoc, CmdStr, "SMAQC", True)

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

			If Not blnSuccess Then
				Dim Msg As String
				Msg = "Error running SMAQC"
				m_message = clsGlobal.AppendToComment(m_message, Msg)

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

				If CmdRunner.ExitCode <> 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "SMAQC returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to SMAQC failed (but exit code is 0)")
				End If

				blnProcessingError = True

			Else
				m_progress = PROGRESS_PCT_SMAQC_COMPLETE
				m_StatusTools.UpdateAndWrite(m_progress)
				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "SMAQC Search Complete")
				End If
			End If

			If Not blnProcessingError Then
				' Parse the results file and post to the database
				If Not ReadAndStoreSMAQCResults(ResultsFilePath) Then
					If String.IsNullOrEmpty(m_message) Then
						m_message = "Error parsing SMAQC results"
					End If
					blnProcessingError = True
				End If
			End If

			' Rename the SMAQC log file to remove the datestamp
			RenameSMAQCLogFile()

			' Don't move the AnalysisSummary.txt file to the results folder; it doesn't have any useful information
			m_jobParams.AddResultFileToSkip("SMAQC_AnalysisSummary.txt")

			' Don't move the parameter file to the results folder, since it's not very informative
			m_jobParams.AddResultFileToSkip(strParameterFileName)

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
			m_message = "Exception in SMAQCPlugin->RunTool"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS	'No failures so everything must have succeeded

	End Function

	' No longer needed
	''Protected Function CopySMAQCRuntimeFiles(ByVal strSourceFolder As String) As Boolean

	''	Dim lstSourceFileNames As New System.Collections.Generic.List(Of String)

	''	Try
	''		lstSourceFileNames.Add("config.xml")
	''		lstSourceFileNames.Add("SMAQC.s3db")

	''		For Each strSourceFileName In lstSourceFileNames
	''			Dim strSourcePath As String
	''			strSourcePath = System.IO.Path.Combine(strSourceFolder, strSourceFileName)
	''			System.IO.File.Copy(strSourcePath, System.IO.Path.Combine(m_WorkDir, strSourceFileName), True)

	''			m_jobParams.AddResultFileToSkip(strSourceFileName)
	''		Next

	''	Catch ex As Exception
	''		m_message = "Error copying SMAQC runtime files"
	''		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
	''		Return False
	''	End Try	

	''	Return True

	''End Function

	''' <summary>
	''' Looks up the InstrumentID for the dataset associated with this job
	''' </summary>
	''' <param name="InstrumentID">Output parameter</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function LookupInstrumentIDFromDB(ByRef InstrumentID As Integer) As Boolean

		Dim RetryCount As Short = 3

		Dim strDatasetID As String = m_jobParams.GetParam("DatasetID")
		mDatasetID = 0

		If String.IsNullOrWhiteSpace(strDatasetID) Then
			m_message = "DatasetID not defined"
			Return False
		ElseIf Not Int32.TryParse(strDatasetID, mDatasetID) Then
			m_message = "DatasetID is not numeric: " & strDatasetID
			Return False
		End If

		Dim ConnectionString As String = m_mgrParams.GetParam("connectionstring")
		Dim blnSuccess As Boolean

		Dim SqlStr As String = _
		  "SELECT Instrument_ID " & _
		  "FROM V_Dataset_Instrument_List_Report " & _
		  "WHERE ID = " & strDatasetID

		InstrumentID = 0

		'Get a table to hold the results of the query
		While RetryCount > 0
			Try

				Using Cn As System.Data.SqlClient.SqlConnection = New System.Data.SqlClient.SqlConnection(ConnectionString)
					Dim dbCmd As System.Data.SqlClient.SqlCommand
					dbCmd = New System.Data.SqlClient.SqlCommand(SqlStr, Cn)

					Cn.Open()

					Dim objResult As Object
					objResult = dbCmd.ExecuteScalar()

					If Not objResult Is Nothing Then
						InstrumentID = CType(objResult, Int32)
						blnSuccess = True
					End If

				End Using  'Cn
				Exit While

			Catch ex As System.Exception
				RetryCount -= 1S
				m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Exception obtaining InstrumentID from the database: " & ex.Message & "; ConnectionString: " & ConnectionString
				m_message &= ", RetryCount = " & RetryCount.ToString
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				System.Threading.Thread.Sleep(5000)				'Delay for 5 second before trying again
			End Try
		End While

		'If loop exited due to errors, return false
		If RetryCount < 1 Then
			m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Excessive failures obtaining InstrumentID from the database"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		Else
			If Not blnSuccess Then
				m_message = "Error obtaining InstrumentID for dataset " & mDatasetID
			End If
			Return blnSuccess
		End If

	End Function

	Protected Sub CopyFailedResultsToArchiveFolder()

		Dim result As IJobParams.CloseOutType

		Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
		If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

		' Bump up the debug level if less than 2
		If m_DebugLevel < 2 Then m_DebugLevel = 2

		m_jobParams.RemoveResultFileToSkip(SMAQC_CONSOLE_OUTPUT)

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

	Protected Function ConvertResultsToXML(ByRef lstResults As System.Collections.Generic.List(Of udtSMAQCResultType), ByRef strXMLResults As String) As Boolean

		' XML will look like:

		' <?xml version="1.0" encoding="utf-8" standalone="yes"?>
		' <SMAQC_Results>
		'   <Dataset>Shew119-01_17july02_earth_0402-10_4-20</Dataset>
		'   <Job>780000</Job>
		'   <Measurements>
		'     <Measurement Name="C_1A">0.002028</Measurement>
		'     <Measurement Name="C_1B">0.00583</Measurement>
		'     <Measurement Name="C_2A">23.5009</Measurement>
		'     <Measurement Name="C_3B">25.99</Measurement>
		'     <Measurement Name="C_4A">23.28</Measurement>
		'     <Measurement Name="C_4B">26.8</Measurement>
		'     <Measurement Name="C_4C">27.18</Measurement>
		'   </Measurements>
		' </SMAQC_Results>


		Dim sbXML As New System.Text.StringBuilder
		strXMLResults = String.Empty

		Try
			sbXML.Append("<?xml version=""1.0"" encoding=""utf-8"" standalone=""yes""?>")
			sbXML.Append("<SMAQC_Results>")

			sbXML.Append("<Dataset>" & m_Dataset & "</Dataset>")
			sbXML.Append("<Job>" & m_JobNum & "</Job>")

			sbXML.Append("<Measurements>")

			For Each udtResult As udtSMAQCResultType In lstResults
				sbXML.Append("<Measurement Name=""" & udtResult.Name & """>" & udtResult.Value & "</Measurement>")
			Next

			sbXML.Append("</Measurements>")
			sbXML.Append("</SMAQC_Results>")

			strXMLResults = sbXML.ToString()

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error converting SMAQC results to XML: " & ex.Message)
			m_message = "Error converting SMAQC results to XML"
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Extract the results from a SMAQC results file
	''' </summary>
	''' <param name="ResultsFilePath"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function LoadSMAQCResults(ByVal ResultsFilePath As String) As System.Collections.Generic.List(Of udtSMAQCResultType)

		' Typical file contents:

		' Results from Scan ID: 10
		' Instrument ID: 1
		' Scan Date: 2011-12-06 19:03:51
		' [Data]
		' Dataset, Measurement Name, Measurement Value
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_1A, 0.002028
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_1B, 0.00583
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_2A, 23.5009
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_3B, 25.99
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4A, 23.28
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4B, 26.8
		' QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4C, 27.18

		' The measurments are returned via this list
		Dim lstResults As New System.Collections.Generic.List(Of udtSMAQCResultType)

		If Not System.IO.File.Exists(ResultsFilePath) Then
			m_message = "SMAQC Results file not found"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message & ": " & ResultsFilePath)
			Return lstResults
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing SMAQC Results file " & ResultsFilePath)
		End If

		Dim srInFile As System.IO.StreamReader
		Dim strLineIn As String
		Dim strSplitLine() As String

		Dim blnMeasurementsFound As Boolean
		Dim blnHeadersFound As Boolean
		
		srInFile = New System.IO.StreamReader(New System.IO.FileStream(ResultsFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

		Do While srInFile.Peek() >= 0
			strLineIn = srInFile.ReadLine()

			If Not String.IsNullOrWhiteSpace(strLineIn) Then

				If Not blnMeasurementsFound Then
					If strLineIn.StartsWith("[Data]") Then
						blnMeasurementsFound = True
					End If
				ElseIf Not blnHeadersFound Then
					If strLineIn.StartsWith("Dataset") Then
						blnHeadersFound = True
					End If
				Else
					' This is a measurement result line
					strSplitLine = strLineIn.Split(","c)

					If Not strSplitLine Is Nothing AndAlso strSplitLine.Length >= 3 Then
						Dim udtResult As udtSMAQCResultType = New udtSMAQCResultType
						udtResult.Clear()

						udtResult.Name = strSplitLine(1).Trim()
						udtResult.Value = strSplitLine(2).Trim()

						lstResults.Add(udtResult)
					End If
				End If

			End If
		Loop

		srInFile.Close()

		Return lstResults

	End Function

	''' <summary>
	''' Parse the SMAQC console output file to track progress
	''' </summary>
	''' <param name="strConsoleOutputFilePath"></param>
	''' <remarks></remarks>
	Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

		' Example Console output:
		'
		' 2/13/2012 07:15:41 PM - [Version Info]
		' 2/13/2012 07:15:41 PM - Loading Assemblies
		' 2/13/2012 07:15:41 PM - mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089
		' 2/13/2012 07:15:41 PM - SMAQC, Version=1.0.4423.30421, Culture=neutral, PublicKeyToken=null
		' 2/13/2012 07:15:41 PM - [System Information]
		' 2/13/2012 07:15:41 PM - OS Version: Microsoft Windows NT 6.1.7601 Service Pack 1
		' 2/13/2012 07:15:41 PM - Processor Count: 4
		' 2/13/2012 07:15:41 PM - Operating System Type: 64-Bit OS
		' 2/13/2012 07:15:41 PM - Page Size: 4096
		' 2/13/2012 07:15:41 PM - [LogStart]
		' 2/13/2012 07:15:41 PM - -----------------------------------------------------
		' 2/13/2012 07:15:41 PM - SMAQC Version 1.02 [BUILD DATE: Feb 10, 2012]
		' 2/13/2012 07:15:42 PM - Searching for Text Files!...
		' 2/13/2012 07:15:42 PM - Parsing and Inserting Data into DB Temp Tables!...
		' 2/13/2012 07:15:45 PM - Now running Measurements on QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45!
		' 2/13/2012 07:15:47 PM - Saving Scan Results!...
		' 2/13/2012 07:15:47 PM - Scan output has been saved to E:\DMS_WorkDir\QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45_SMAQC.txt
		' 2/13/2012 07:15:47 PM - SMAQC analysis complete

		Static dtLastProgressWriteTime As System.DateTime = System.DateTime.UtcNow

		' This RegEx matches lines in the form:
		' 2/13/2012 07:15:42 PM - Searching for Text Files!...
		Static reMatchTimeStamp As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("^\d+/\d+/\d+ \d+:\d+:\d+ [AP]M - ", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)

		Dim reMatch As System.Text.RegularExpressions.Match

		Try

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

			Dim sngEffectiveProgress As Single
			sngEffectiveProgress = PROGRESS_PCT_SMAQC_STARTING

			srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

			intLinesRead = 0
			Do While srInFile.Peek() >= 0
				strLineIn = srInFile.ReadLine()
				intLinesRead += 1

				If Not String.IsNullOrWhiteSpace(strLineIn) Then

					' Remove the timestamp from the start of the line (if present)
					reMatch = reMatchTimeStamp.Match(strLineIn)
					If reMatch.Success Then
						strLineIn = strLineIn.Substring(reMatch.Length)
					End If

					' Update progress if the line starts with one of the expected phrases
					If strLineIn.StartsWith("Searching for Text Files") Then
						If sngEffectiveProgress < PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES Then
							sngEffectiveProgress = PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES
						End If

					ElseIf strLineIn.StartsWith("Parsing and Inserting Data") Then
						If sngEffectiveProgress < PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES Then
							sngEffectiveProgress = PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES
						End If

					ElseIf strLineIn.StartsWith("Now running Measurements") Then
						If sngEffectiveProgress < PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS Then
							sngEffectiveProgress = PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS
						End If

					ElseIf strLineIn.StartsWith("Saving Scan Results") Then
						If sngEffectiveProgress < PROGRESS_PCT_SMAQC_SAVING_RESULTS Then
							sngEffectiveProgress = PROGRESS_PCT_SMAQC_SAVING_RESULTS
						End If

					ElseIf strLineIn.StartsWith("Scan output has been saved") Then
						' Ignore this line

					ElseIf strLineIn.StartsWith("SMAQC analysis complete") Then
						' Ignore this line

					ElseIf Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
						If strLineIn.ToLower.Contains("error") Then
							mConsoleOutputErrorMsg &= "; " & strLineIn
						End If
					End If
				End If
			Loop

			srInFile.Close()

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

	Private Function ParseSMAQCParameterFile(ByVal strParameterFilePath As String) As String

		' If necessary, could parse a parameter file and convert the options in the parameter file to command line arguments
		' As an example, see function ParseMSGFDBParameterFile in the AnalysisManagerMSGFDBPlugIn project
		Return String.Empty
	End Function

	Protected Function PostSMAQCResultsToDB(ByVal strXMLResults As String) As Boolean

		Dim strConnectionString As String
		strConnectionString = m_mgrParams.GetParam("connectionstring")

		' Note that mDatasetID gets populated by LookupInstrumentIDFromDB

		Return PostSMAQCResultsToDB(mDatasetID, strXMLResults, strConnectionString, STORE_SMAQC_RESULTS_SP_NAME)

	End Function

	Protected Function PostSMAQCResultsToDB(ByVal intDatasetID As Integer, _
	  ByVal strXMLResults As String) As Boolean

		Dim strConnectionString As String
		strConnectionString = m_mgrParams.GetParam("connectionstring")

		Return PostSMAQCResultsToDB(intDatasetID, strXMLResults, strConnectionString, STORE_SMAQC_RESULTS_SP_NAME)

	End Function

	Protected Function PostSMAQCResultsToDB(ByVal intDatasetID As Integer, _
	  ByVal strXMLResults As String, _
	  ByVal strConnectionString As String) As Boolean

		Return PostSMAQCResultsToDB(intDatasetID, strXMLResults, strConnectionString, STORE_SMAQC_RESULTS_SP_NAME)

	End Function

	Protected Function PostSMAQCResultsToDB(ByVal intDatasetID As Integer, _
	  ByVal strXMLResults As String, _
	  ByVal strConnectionString As String, _
	  ByVal strStoredProcedure As String) As Boolean

		Const MAX_RETRY_COUNT As Integer = 3

		Dim intStartIndex As Integer

		Dim strXMLResultsClean As String

		Dim objCommand As System.Data.SqlClient.SqlCommand

		Dim blnSuccess As Boolean

		Try
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Posting SMAQC Results to the database (using Dataset ID " & intDatasetID.ToString & ")")
			End If

			' We need to remove the encoding line from strXMLResults before posting to the DB
			' This line will look like this:
			'   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

			intStartIndex = strXMLResults.IndexOf("?>")
			If intStartIndex > 0 Then
				strXMLResultsClean = strXMLResults.Substring(intStartIndex + 2).Trim()
			Else
				strXMLResultsClean = strXMLResults
			End If

			' Call stored procedure strStoredProcedure using connection string strConnectionString

			If String.IsNullOrWhiteSpace(strConnectionString) Then
				m_message = "Connection string empty in PostSMAQCResultsToDB"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Connection string not defined; unable to post the SMAQC results to the database")
				Return False
			End If

			If String.IsNullOrWhiteSpace(strStoredProcedure) Then
				strStoredProcedure = STORE_SMAQC_RESULTS_SP_NAME
			End If

			objCommand = New System.Data.SqlClient.SqlCommand()

			With objCommand
				.CommandType = CommandType.StoredProcedure
				.CommandText = strStoredProcedure

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

				.Parameters.Add(New SqlClient.SqlParameter("@DatasetID", SqlDbType.Int))
				.Parameters.Item("@DatasetID").Direction = ParameterDirection.Input
				.Parameters.Item("@DatasetID").Value = intDatasetID

				.Parameters.Add(New SqlClient.SqlParameter("@ResultsXML", SqlDbType.Xml))
				.Parameters.Item("@ResultsXML").Direction = ParameterDirection.Input
				.Parameters.Item("@ResultsXML").Value = strXMLResultsClean
			End With


			Dim objAnalysisTask As clsAnalysisJob

			objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

			'Execute the SP (retry the call up to 4 times)
			Dim ResCode As Integer
			ResCode = objAnalysisTask.ExecuteSP(objCommand, strConnectionString, MAX_RETRY_COUNT)

			objAnalysisTask = Nothing

			If ResCode = 0 Then
				blnSuccess = True
			Else
				m_message = "Error storing SMAQC Results in database, " & strStoredProcedure & " returned " & ResCode.ToString
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				blnSuccess = False
			End If

		Catch ex As System.Exception
			m_message = "Exception storing SMAQC Results in database"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Read the SMAQC results files, convert to XML, and post to DMS
	''' </summary>
	''' <param name="ResultsFilePath">Path to the SMAQC results file</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ReadAndStoreSMAQCResults(ByVal ResultsFilePath As String) As Boolean

		Dim blnSuccess As Boolean
		Dim lstResults As System.Collections.Generic.List(Of udtSMAQCResultType)

		Try

			lstResults = LoadSMAQCResults(ResultsFilePath)

			If lstResults.Count = 0 Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "No SMAQC results were found"
				End If

			Else
				' Convert the results to XML format
				Dim strXMLResults As String = String.Empty

				blnSuccess = ConvertResultsToXML(lstResults, strXMLResults)

				If blnSuccess Then
					' Store the results in the database
					blnSuccess = PostSMAQCResultsToDB(strXMLResults)

				End If

			End If

		Catch ex As Exception
			m_message = "Exception parsing SMAQC results"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception parsing SMAQC results and posting to the database", ex)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Renames the SMAQC log file
	''' </summary>
	''' <remarks></remarks>
	Private Sub RenameSMAQCLogFile()
		Dim diWorkDir As System.IO.DirectoryInfo
		Dim fiFiles() As System.IO.FileInfo
		Dim strLogFilePathNew As String

		Try

			diWorkDir = New System.IO.DirectoryInfo(m_WorkDir)

			fiFiles = diWorkDir.GetFiles("SMAQC-log*.txt")

			If Not fiFiles Is Nothing AndAlso fiFiles.Length > 0 Then

				' There should only be one file; just parse fiFiles(0)
				strLogFilePathNew = System.IO.Path.Combine(m_WorkDir, "SMAQC_log.txt")

				If System.IO.File.Exists(strLogFilePathNew) Then
					System.IO.File.Delete(strLogFilePathNew)
				End If

				fiFiles(0).MoveTo(strLogFilePathNew)
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception renaming SMAQC log file", ex)
		End Try

	End Sub

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strSMAQCProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim ioSMAQC As System.IO.FileInfo
		Dim blnSuccess As Boolean

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioSMAQC = New System.IO.FileInfo(strSMAQCProgLoc)
		If Not ioSMAQC.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		' Lookup the version of the SMAQC application
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioSMAQC.FullName)
		If Not blnSuccess Then Return False

		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(ioSMAQC)

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
	''' Zips SMAQC Output File
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function ZipSMAQCResults(ResultsFileName As String) As IJobParams.CloseOutType
		Dim TmpFilePath As String

		Try

			TmpFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)
			If Not MyBase.ZipFile(TmpFilePath, False) Then
				Dim Msg As String = "Error zipping output files, job " & m_JobNum
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Add the _SMAQC.txt file to .FilesToDelete since we only want to keep the Zipped version
			m_jobParams.AddResultFileToSkip(ResultsFileName)

		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerSMAQC.ZipSMAQCResults, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
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

			ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, SMAQC_CONSOLE_OUTPUT))

		End If

	End Sub

#End Region
End Class
