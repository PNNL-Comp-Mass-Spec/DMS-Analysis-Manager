'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 01/24/2013
'
' Uses DeconConsole.exe to create a .MGF file from a .Raw file or .mzXML file or .mzML file
' Next, converts the .MGF file to a _DTA.txt file
'
' Note that DeconConsole is the re-implementation of the legacy DeconMSn program (and is thus DeconMSn v3)
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsDtaGenDeconConsole
	Inherits clsDtaGenThermoRaw

#Region "Constants"
	Protected Const PROGRESS_DECON_CONSOLE_START As Integer = 5
	Protected Const PROGRESS_MGF_TO_CDTA_START As Integer = 85
	Protected Const PROGRESS_CDTA_CREATED As Integer = 95
#End Region

#Region "Structures"

	Protected Structure udtDeconToolsStatusType
		Public CurrentLCScan As Integer		' LC Scan number or IMS Frame Number
		Public PercentComplete As Single
		Public Sub Clear()
			CurrentLCScan = 0
			PercentComplete = 0
		End Sub
	End Structure

#End Region

#Region "Classwide variables"
	Protected mInputFilePath As String
	Protected mDeconConsoleExceptionThrown As Boolean
	Protected mDeconConsoleFinishedDespiteProgRunnerError As Boolean
#End Region

	Protected mDeconConsoleStatus As udtDeconToolsStatusType

	''' <summary>
	''' Returns the default path to the DTA generator tool
	''' </summary>
	''' <returns></returns>
	''' <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
	Protected Overrides Function ConstructDTAToolPath() As String

		Dim strDTAToolPath As String

		Dim DeconToolsDir As String = m_MgrParams.GetParam("DeconToolsProgLoc")			' DeconConsole.exe is stored in the DeconTools folder
		strDTAToolPath = System.IO.Path.Combine(DeconToolsDir, DECON_CONSOLE_FILENAME)

		Return strDTAToolPath

	End Function

	Protected Overrides Sub MakeDTAFilesThreaded()

		m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
		m_ErrMsg = String.Empty

		m_Progress = PROGRESS_DECON_CONSOLE_START

		If Not ConvertRawToMGF(m_RawDataType) Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
			Return
		End If

		m_Progress = PROGRESS_MGF_TO_CDTA_START

		If Not ConvertMGFtoDTA() Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
			Return
		End If

		m_Progress = PROGRESS_CDTA_CREATED

		m_Results = ISpectraFileProcessor.ProcessResults.SF_SUCCESS
		m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE

	End Sub

	''' <summary>
	''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Function ConvertMGFtoDTA() As Boolean

		Dim blnSuccess As Boolean

		Dim strRawDataType As String = m_JobParams.GetJobParameter("RawDataType", "")
		Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants

		Dim oMGFConverter As clsMGFConverter = New clsMGFConverter(m_DebugLevel, m_WorkDir)

		eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)
		blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, m_Dataset)

		If Not blnSuccess Then
			m_ErrMsg = oMGFConverter.ErrorMessage
		End If

		m_SpectraFileCount = oMGFConverter.SpectraCountWritten

		Return blnSuccess

	End Function


	''' <summary>
	''' Create .mgf file using MSConvert
	''' This function is called by MakeDTAFilesThreaded
	''' </summary>
	''' <param name="eRawDataType">Raw data file type</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function ConvertRawToMGF(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As Boolean

		Dim CmdStr As String
		Dim RawFilePath As String

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .MGF file using DeconConsole")
		End If

		m_ErrMsg = String.Empty

		' Construct the path to the .raw file
		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				RawFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
			Case Else
				m_ErrMsg = "Data file type not supported by the DeconMSn workflow in DeconConsole: " & eRawDataType.ToString()
				Return False
		End Select

		m_InstrumentFileName = System.IO.Path.GetFileName(RawFilePath)
		mInputFilePath = RawFilePath
		m_JobParams.AddResultFileToSkip(m_InstrumentFileName)

		If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
			'Get the maximum number of scans in the file
			m_MaxScanInFile = GetMaxScan(RawFilePath)
		Else
			m_MaxScanInFile = DEFAULT_SCAN_STOP
		End If

		'Determine max number of scans to be performed
		m_NumScans = m_MaxScanInFile

		'Setup a program runner tool to make the spectra files
		m_RunProgTool = New clsRunDosProgram(m_WorkDir)

		' Reset the state variables
		mDeconConsoleExceptionThrown = False
		mDeconConsoleFinishedDespiteProgRunnerError = False
		mDeconConsoleStatus.Clear()

		Dim strParamFilePath As String
		strParamFilePath = m_JobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", String.Empty)

		If String.IsNullOrEmpty(strParamFilePath) Then
			m_ErrMsg = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_JobParams, "DeconMSn_ParamFile")
			Return False
		Else
			strParamFilePath = IO.Path.Combine(m_WorkDir, strParamFilePath)
		End If

		'Set up command
		CmdStr = " " & RawFilePath & " " & strParamFilePath

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_DtaToolNameLoc & " " & CmdStr)
		End If

		With m_RunProgTool
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True

			'  We don't need to capture the console output since the DeconConsole log file has very similar information
			.WriteConsoleOutputToFile = False
		End With

		Dim blnSuccess As Boolean
		blnSuccess = m_RunProgTool.RunProgram(m_DtaToolNameLoc, CmdStr, "DeconConsole", True)

		' Parse the DeconTools .Log file to see whether it contains message "Finished file processing"

		Dim dtFinishTime As System.DateTime
		Dim blnFinishedProcessing As Boolean

		ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

		If mDeconConsoleExceptionThrown Then
			blnSuccess = False
		End If

		If blnFinishedProcessing And Not blnSuccess Then
			mDeconConsoleFinishedDespiteProgRunnerError = True
		End If

		' Look for file Dataset*BAD_ERROR_log.txt
		' If it exists, an exception occurred
		Dim diWorkdir As System.IO.DirectoryInfo
		diWorkdir = New System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir))

		For Each fiFile As System.IO.FileInfo In diWorkdir.GetFiles(m_Dataset & "*BAD_ERROR_log.txt")
			m_ErrMsg = "Error running DeconTools; Bad_Error_log file exists"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg & ": " & fiFile.Name)
			blnSuccess = False
			mDeconConsoleFinishedDespiteProgRunnerError = False
			Exit For
		Next

		If mDeconConsoleFinishedDespiteProgRunnerError And Not mDeconConsoleExceptionThrown Then
			' ProgRunner reported an error code
			' However, the log file says things completed successfully
			' We'll trust the log file
			blnSuccess = True
		End If

		If Not blnSuccess Then
			' .RunProgram returned False
			LogDTACreationStats("ConvertRawToMGF", System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False")

			If Not String.IsNullOrEmpty(m_ErrMsg) Then
				m_ErrMsg = "Error running " & System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
			End If

			Return False
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... MGF file created")
		End If

		Return True

	End Function

	Protected Overrides Sub MonitorProgress()

		Dim dtFinishTime As System.DateTime
		Dim blnFinishedProcessing As Boolean

		ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

		If m_DebugLevel >= 2 Then

			Dim strProgressMessage As String
			strProgressMessage = "Scan=" & mDeconConsoleStatus.CurrentLCScan
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "... " & strProgressMessage & ", " & m_Progress.ToString("0.0") & "% complete")

		End If


		Const MAX_LOGFINISHED_WAITTIME_SECONDS As Integer = 120
		If blnFinishedProcessing Then
			' The DeconConsole Log File reports that the task is complete
			' If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, then send an abort to the CmdRunner

			If System.DateTime.Now().Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Note: Log file reports finished over " & MAX_LOGFINISHED_WAITTIME_SECONDS & " seconds ago, but the DeconConsole CmdRunner is still active")

				mDeconConsoleFinishedDespiteProgRunnerError = True

				' Abort processing
				m_RunProgTool.AbortProgramNow()

				System.Threading.Thread.Sleep(3000)
			End If
		End If

	End Sub

	Protected Sub ParseDeconToolsLogFile(ByRef blnFinishedProcessing As Boolean, ByRef dtFinishTime As System.DateTime)

		''Dim TOTAL_WORKFLOW_STEPS As Integer = 2

		Dim fiFileInfo As System.IO.FileInfo

		Dim strLogFilePath As String
		Dim strLineIn As String
		Dim blnDateValid As Boolean

		Dim intCharIndex As Integer

		Dim strScanLine As String = String.Empty
		''Dim intWorkFlowStep As Integer = 1

		blnFinishedProcessing = False

		Try
			Select Case m_RawDataType
				Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
					' As of 11/19/2010, the _Log.txt file is created inside the .D folder
					strLogFilePath = System.IO.Path.Combine(mInputFilePath, m_Dataset) & "_log.txt"
				Case Else
					strLogFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(mInputFilePath) & "_log.txt")
			End Select

			If System.IO.File.Exists(strLogFilePath) Then

				Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

					Do While srInFile.Peek >= 0
						strLineIn = srInFile.ReadLine

						If Not String.IsNullOrWhiteSpace(strLineIn) Then
							intCharIndex = strLineIn.ToLower().IndexOf("finished file processing")
							If intCharIndex >= 0 Then

								blnDateValid = False
								If intCharIndex > 1 Then
									' Parse out the date from strLineIn
									If System.DateTime.TryParse(strLineIn.Substring(0, intCharIndex).Trim, dtFinishTime) Then
										blnDateValid = True
									Else
										' Unable to parse out the date
										clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to parse date from string '" & strLineIn.Substring(0, intCharIndex).Trim & "'; will use file modification date as the processing finish time")
									End If
								End If

								If Not blnDateValid Then
									fiFileInfo = New System.IO.FileInfo(strLogFilePath)
									dtFinishTime = fiFileInfo.LastWriteTime
								End If

								If m_DebugLevel >= 3 Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconConsole log file reports 'finished file processing' at " & dtFinishTime.ToString())
								End If

								''If intWorkFlowStep < TOTAL_WORKFLOW_STEPS Then
								''	intWorkFlowStep += 1
								''End If

								blnFinishedProcessing = True
							End If

							If intCharIndex < 0 Then
								intCharIndex = strLineIn.IndexOf("DeconTools.Backend.dll")
								If intCharIndex > 0 Then
									' DeconConsole reports "Finished file processing" at the end of each step in the workflow
									' Reset blnFinishedProcessing back to false
									blnFinishedProcessing = False
								End If
							End If

							If intCharIndex < 0 Then
								intCharIndex = strLineIn.ToLower.IndexOf("scan/frame")
								If intCharIndex > 0 Then
									strScanLine = strLineIn.Substring(intCharIndex)
								End If
							End If

							If intCharIndex < 0 Then
								intCharIndex = strLineIn.ToLower.IndexOf("scan=")
								If intCharIndex > 0 Then
									strScanLine = strLineIn.Substring(intCharIndex)
								End If
							End If

							If intCharIndex < 0 Then

								intCharIndex = strLineIn.IndexOf("ERROR THROWN")
								If intCharIndex >= 0 Then
									' An exception was reported in the log file; treat this as a fatal error
									m_ErrMsg = "Error thrown by DeconConsole"

									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeconConsole reports " & strLineIn.Substring(intCharIndex))
									mDeconConsoleExceptionThrown = True

								End If
							End If

						End If
					Loop

				End Using
			End If

		Catch ex As System.Exception
			' Ignore errors here		
			If m_DebugLevel >= 4 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Exception in ParseDeconToolsLogFile: " & ex.Message)
			End If

		End Try

		If Not String.IsNullOrWhiteSpace(strScanLine) Then
			' Parse strScanFrameLine
			' It will look like:
			' Scan= 16500; PercentComplete= 89.2

			Dim strProgressStats() As String
			Dim kvStat As System.Collections.Generic.KeyValuePair(Of String, String)

			strProgressStats = strScanLine.Split(";"c)

			For i As Integer = 0 To strProgressStats.Length - 1
				kvStat = ParseKeyValue(strProgressStats(i))
				If Not String.IsNullOrWhiteSpace(kvStat.Key) Then
					Select Case kvStat.Key
						Case "Scan"
							Integer.TryParse(kvStat.Value, mDeconConsoleStatus.CurrentLCScan)
						Case "Scan/Frame"
							Integer.TryParse(kvStat.Value, mDeconConsoleStatus.CurrentLCScan)
						Case "PercentComplete"
							Single.TryParse(kvStat.Value, mDeconConsoleStatus.PercentComplete)

							''mDeconConsoleStatus.PercentComplete = CSng((intWorkFlowStep - 1) / TOTAL_WORKFLOW_STEPS * 100.0 + mDeconConsoleStatus.PercentComplete / TOTAL_WORKFLOW_STEPS)
							mDeconConsoleStatus.PercentComplete = mDeconConsoleStatus.PercentComplete
					End Select
				End If
			Next

			m_Progress = PROGRESS_DECON_CONSOLE_START + mDeconConsoleStatus.PercentComplete * (PROGRESS_MGF_TO_CDTA_START - PROGRESS_DECON_CONSOLE_START) / 100

		End If

	End Sub

	''' <summary>
	''' Looks for an equals sign in strData
	''' Returns a KeyValuePair object with the text before the equals sign and the text after the equals sign
	''' </summary>
	''' <param name="strData"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ParseKeyValue(ByVal strData As String) As System.Collections.Generic.KeyValuePair(Of String, String)
		Dim intCharIndex As Integer
		intCharIndex = strData.IndexOf("="c)

		If intCharIndex > 0 Then
			Try
				Return New System.Collections.Generic.KeyValuePair(Of String, String)(strData.Substring(0, intCharIndex).Trim(), _
				 strData.Substring(intCharIndex + 1).Trim())
			Catch ex As Exception
				' Ignore errors here
			End Try
		End If

		Return New System.Collections.Generic.KeyValuePair(Of String, String)(String.Empty, String.Empty)

	End Function

End Class
