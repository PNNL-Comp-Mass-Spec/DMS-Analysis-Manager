Option Strict On

'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerDecon2ls
    Inherits clsAnalysisToolRunnerBase

#Region "Constants"
    Private Const DECON2LS_SCANS_FILE_SUFFIX As String = "_scans.csv"
    Private Const DECON2LS_ISOS_FILE_SUFFIX As String = "_isos.csv"
    Private Const DECON2LS_PEAKS_FILE_SUFFIX As String = "_peaks.dat"

#End Region

#Region "Module variables"

    Protected mRawDataType As String = String.Empty

    Protected mInputFilePath As String = String.Empty

	Protected mDeconConsoleBuild As Integer = 0

	Protected mDeconToolsFinishedDespiteProgRunnerError As Boolean

	Protected mDeconToolsStatus As udtDeconToolsStatusType

	Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Enums and Structures"
	'Used for result file type
	Enum Decon2LSResultFileType
		DECON2LS_ISOS = 0
		DECON2LS_SCANS = 1
	End Enum

	Enum DeconToolsStateType
		Idle = 0
		Running = 1
		Complete = 2
		ErrorCode = 3
	End Enum

	Enum DeconToolsFileTypeConstants
		Undefined = 0
		Agilent_WIFF = 1
		Agilent_D = 2
		Ascii = 3
		Bruker = 4
		Bruker_Ascii = 5
		Finnigan = 6
		ICR2LS_Rawdata = 7
		Micromass_Rawdata = 8
		MZXML_Rawdata = 9
		PNNL_IMS = 10
		PNNL_UIMF = 11
		SUNEXTREL = 12
	End Enum

	Protected Structure udtDeconToolsStatusType
		Public CurrentLCScan As Integer		' LC Scan number or IMS Frame Number
		Public PercentComplete As Single
		Public IsUIMF As Boolean
		Public Sub Clear()
			CurrentLCScan = 0
			PercentComplete = 0
			IsUIMF = False
		End Sub

	End Structure
#End Region

#Region "Methods"
	Public Sub New()
	End Sub

	''' <summary>
	''' Validate the result files
	''' (legacy code would assemble result files from looping, but that code has been removed)
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function AssembleResults() As IJobParams.CloseOutType

		Dim ScansFilePath As String
		Dim IsosFilePath As String
		Dim PeaksFilePath As String

		Try

			ScansFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX)
			IsosFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX)
			PeaksFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX)

			Select Case mRawDataType
				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS, clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER
					' As of 11/19/2010, the Decon2LS output files are created inside the .D folder
					If Not System.IO.File.Exists(IsosFilePath) And Not System.IO.File.Exists(ScansFilePath) Then
						' Copy the files from the .D folder to the work directory

						Dim fiSrcFilePath As System.IO.FileInfo

						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying Decon2LS result files from the .D folder to the working directory")
						End If

						fiSrcFilePath = New System.IO.FileInfo(System.IO.Path.Combine(mInputFilePath, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX))
						If fiSrcFilePath.Exists Then
							fiSrcFilePath.CopyTo(ScansFilePath)
						End If

						fiSrcFilePath = New System.IO.FileInfo(System.IO.Path.Combine(mInputFilePath, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX))
						If fiSrcFilePath.Exists Then
							fiSrcFilePath.CopyTo(IsosFilePath)
						End If

						fiSrcFilePath = New System.IO.FileInfo(System.IO.Path.Combine(mInputFilePath, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX))
						If fiSrcFilePath.Exists Then
							fiSrcFilePath.CopyTo(PeaksFilePath)
						End If

					End If

			End Select

			clsGlobal.m_ExceptionFiles.Add(ScansFilePath)
			clsGlobal.m_ExceptionFiles.Add(IsosFilePath)
			clsGlobal.m_ExceptionFiles.Add(PeaksFilePath)


			' Make sure the Isos File exists
			If Not System.IO.File.Exists(IsosFilePath) Then
				m_message = "DeconTools Isos file Not Found"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & IsosFilePath)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
			End If

			' Make sure the Isos file contains at least one row of data
			If Not IsosFileHasData(IsosFilePath) Then
				m_message = "No results in DeconTools Isos file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
			End If

		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function CreateNewExportFile(ByVal exportFileName As String) As System.IO.StreamWriter
		Dim ef As System.IO.StreamWriter

		If System.IO.File.Exists(exportFileName) Then
			'post error to log
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->createNewExportFile: Export file already exists (" & exportFileName & "); this is unexpected")
			Return Nothing
		End If

		ef = New System.IO.StreamWriter(exportFileName, False)
		Return ef

	End Function

	''' <summary>
	''' Examines IsosFilePath to look for data lines (does not read the entire file, just the first two lines)
	''' </summary>
	''' <param name="IsosFilePath"></param>
	''' <returns>True if it has one or more lines of data, otherwise, returns False</returns>
	''' <remarks></remarks>
	Protected Function IsosFileHasData(ByVal IsosFilePath As String) As Boolean
		Dim intDataLineCount As Integer
		Return IsosFileHasData(IsosFilePath, intDataLineCount, False)
	End Function

	''' <summary>
	''' Examines IsosFilePath to look for data lines 
	''' </summary>
	''' <param name="IsosFilePath"></param>
	''' <param name="intDataLineCount">Output parameter: total data line count</param>
	''' <param name="blnCountTotalDataLines">True to count all of the data lines; false to just look for the first data line</param>
	''' <returns>True if it has one or more lines of data, otherwise, returns False</returns>
	''' <remarks></remarks>
	Protected Function IsosFileHasData(ByVal IsosFilePath As String, ByRef intDataLineCount As Integer, ByVal blnCountTotalDataLines As Boolean) As Boolean

		Dim srInFile As System.IO.StreamReader

		Dim strLineIn As String
		Dim blnHeaderLineProcessed As Boolean

		intDataLineCount = 0
		blnHeaderLineProcessed = False

		Try

			If System.IO.File.Exists(IsosFilePath) Then
				srInFile = New System.IO.StreamReader(New System.IO.FileStream(IsosFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				Do While srInFile.Peek >= 0
					strLineIn = srInFile.ReadLine

					If Not String.IsNullOrEmpty(strLineIn) Then

						If blnHeaderLineProcessed Then
							' This is a data line
							If blnCountTotalDataLines Then
								intDataLineCount += 1
							Else
								intDataLineCount = 1
								Exit Do
							End If

						Else
							blnHeaderLineProcessed = True
						End If
					End If
				Loop

				srInFile.Close()
			End If

		Catch ex As System.Exception
			' Ignore errors here
		End Try

		If intDataLineCount > 0 Then
			Return True
		Else
			Return False
		End If

	End Function

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		'Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass

		Dim eResult As IJobParams.CloseOutType
		'Dim TcpPort As Integer = CInt(m_mgrParams.GetParam("tcpport"))
		Dim eReturnCode As IJobParams.CloseOutType

		mRawDataType = m_jobParams.GetParam("RawDataType")

		' Set this to success for now
		eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2LSBase.RunTool()")
		End If

		'Get the setup file by running the base class method
		eResult = MyBase.RunTool()
		If Not eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Error message is generated in base class, so just exit with error
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Run Decon2LS
		eResult = RunDecon2Ls()
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Something went wrong
			' In order to help diagnose things, we will move whatever files were created into the eResult folder, 
			'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error running Decon2LS"
			End If

			If eResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
				eReturnCode = eResult
			Else
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		End If

		'Delete the raw data files
		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file")
		End If

		If DeleteRawDataFiles(mRawDataType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " & m_message)
			m_message = "Error deleting raw data files"
			' Don't treat this as a critical error; leave eReturnCode unchanged
		End If

		'Update the job summary file
		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file")
		End If
		UpdateSummaryFile()

		'Make the results folder
		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder")
		End If

		eResult = MakeResultsFolder()
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'MakeResultsFolder handles posting to local log, so set database error message and exit
			m_message = "Error making results folder"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		eResult = MoveResultFiles()
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'MoveResultFiles moves the eResult files to the eResult folder
			m_message = "Error moving files into results folder"
			eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
			' Try to save whatever files were moved into the results folder
			Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
			objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		eResult = CopyResultsFolderToServer()
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: What do we do here?
			Return eResult
		End If

		'If we get to here, return the return code
		Return eReturnCode

	End Function

	Protected Function RunDecon2Ls() As IJobParams.CloseOutType

		Dim blnLoopingEnabled As Boolean = False

		Dim strParamFilePath As String = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
		Dim blnDecon2LSError As Boolean


		' Get file type of the raw data file
		Dim filetype As DeconToolsFileTypeConstants
		filetype = GetInputFileType(mRawDataType)

		If filetype = DeconToolsFileTypeConstants.Undefined Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while getting file type: " & mRawDataType)
			m_message = "Invalid raw data type specified"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Specify Input file or folder
		mInputFilePath = SpecifyInputFilePath(mRawDataType)
		If mInputFilePath = "" Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while input file name: " & mRawDataType)
			m_message = "Invalid raw data type specified"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If


		' Determine the path to the DeconTools folder
		Dim progLoc As String
		progLoc = DetermineProgramLocation("DeconTools", "DeconToolsProgLoc", "DeconConsole.exe")

		If String.IsNullOrWhiteSpace(progLoc) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Store the DeconTools version info in the database
		m_message = String.Empty
		If Not StoreToolVersionInfo(progLoc) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error determining DeconTools version"
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Reset the log file tracking variables
		mDeconToolsFinishedDespiteProgRunnerError = False

		' Reset the state variables
		mDeconToolsStatus.Clear()

		If filetype = DeconToolsFileTypeConstants.PNNL_UIMF Then
			mDeconToolsStatus.IsUIMF = True
		Else
			mDeconToolsStatus.IsUIMF = False
		End If

		'Start Decon2LS and wait for it to finish
		Dim eDeconToolsStatus As DeconToolsStateType
		eDeconToolsStatus = StartDeconTools(progLoc, mInputFilePath, strParamFilePath, filetype)

		' Stop the job timer
		m_StopTime = System.DateTime.UtcNow

		'Make sure objects are released
		System.Threading.Thread.Sleep(2000)		   '2 second delay
		GC.Collect()
		GC.WaitForPendingFinalizers()

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished")
		End If

		' Determine reason for Decon2LS finish
		If mDeconToolsFinishedDespiteProgRunnerError Then
			' ProgRunner reported an error code
			' However, the log file says things completed successfully
			' We'll trust the log file
			blnDecon2LSError = False
		Else
			Select Case eDeconToolsStatus
				Case DeconToolsStateType.Complete
					'This is normal, do nothing else
					blnDecon2LSError = False

				Case DeconToolsStateType.ErrorCode
					m_message = "Decon2LS error"
					blnDecon2LSError = True

				Case DeconToolsStateType.Idle
					' DeconTools never actually started
					m_message = "Decon2LS error"
					blnDecon2LSError = True

				Case DeconToolsStateType.Running
					' We probably shouldn't get here
					' But, we'll assume success
					blnDecon2LSError = False
			End Select
		End If

		If Not blnDecon2LSError Then
			Dim eResult As IJobParams.CloseOutType
			eResult = AssembleResults()

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'Check for no data first. If no data, then exit but still copy results to server
				If eResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
					Return eResult
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), AssembleResults returned " & eResult.ToString)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		If blnDecon2LSError Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Else
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If


	End Function


	Protected Function StartDeconTools(ByVal ProgLoc As String, _
									 ByVal strInputFilePath As String, _
									 ByVal strParamFilePath As String, _
									 ByVal eFileType As DeconToolsFileTypeConstants) As DeconToolsStateType

		Dim blnSuccess As Boolean
		Dim eDeconToolsStatus As DeconToolsStateType = DeconToolsStateType.Idle

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsDeIsotope.StartDeconTools(), Starting deconvolution")
		End If

		Try

			Dim CmdStr As String
			Dim strFileTypeText As String

			If eFileType = DeconToolsFileTypeConstants.Undefined Then
				m_message = "Undefined file type found in StartDeconTools"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return DeconToolsStateType.ErrorCode
			End If

			strFileTypeText = GetDeconFileTypeText(eFileType)

			' Set up and execute a program runner to run DeconTools
			If mDeconConsoleBuild < 4400 Then
				CmdStr = strInputFilePath & " " & strFileTypeText & " " & strParamFilePath
			Else
				CmdStr = strInputFilePath & " " & strParamFilePath
			End If


			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ProgLoc & " " & CmdStr)
			End If

			CmdRunner = New clsRunDosProgram(m_WorkDir)

			With CmdRunner
				.CreateNoWindow = True
				.CacheStandardOutput = True
				.EchoOutputToConsole = True

				' We don't need to capture the console output since the DeconTools log file has very similar information
				.WriteConsoleOutputToFile = False
			End With

			eDeconToolsStatus = DeconToolsStateType.Running

			If Not CmdRunner.RunProgram(ProgLoc, CmdStr, "DeconConsole", True) Then
				m_message = "Error running DeconTools"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
				blnSuccess = False
			Else
				blnSuccess = True
			End If

			' Parse the DeconTools .Log file to see whether it contains message "Finished file processing"

			Dim dtFinishTime As System.DateTime
			Dim blnFinishedProcessing As Boolean

			ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

			If blnSuccess Then
				eDeconToolsStatus = DeconToolsStateType.Complete
			ElseIf blnFinishedProcessing Then
				mDeconToolsFinishedDespiteProgRunnerError = True
				eDeconToolsStatus = DeconToolsStateType.Complete
			Else
				eDeconToolsStatus = DeconToolsStateType.ErrorCode
			End If


		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling DeconConsole: " & ex.Message)
			eDeconToolsStatus = DeconToolsStateType.ErrorCode
		End Try

		Return eDeconToolsStatus

	End Function


	Protected Function GetDeconFileTypeText(eDeconFileType As DeconToolsFileTypeConstants) As String

		Select Case eDeconFileType
			Case DeconToolsFileTypeConstants.Agilent_WIFF : Return "Agilent_WIFF"
			Case DeconToolsFileTypeConstants.Agilent_D : Return "Agilent_D"
			Case DeconToolsFileTypeConstants.Ascii : Return "Ascii"
			Case DeconToolsFileTypeConstants.Bruker : Return "Bruker"
			Case DeconToolsFileTypeConstants.Bruker_Ascii : Return "Bruker_Ascii"
			Case DeconToolsFileTypeConstants.Finnigan : Return "Finnigan"
			Case DeconToolsFileTypeConstants.ICR2LS_Rawdata : Return "ICR2LS_Rawdata"
			Case DeconToolsFileTypeConstants.Micromass_Rawdata : Return "Micromass_Rawdata"
			Case DeconToolsFileTypeConstants.MZXML_Rawdata : Return "MZXML_Rawdata"
			Case DeconToolsFileTypeConstants.PNNL_IMS : Return "PNNL_IMS"
			Case DeconToolsFileTypeConstants.PNNL_UIMF : Return "PNNL_UIMF"
			Case DeconToolsFileTypeConstants.SUNEXTREL : Return "SUNEXTREL"
			Case Else
				Return "Undefined"
		End Select

	End Function

	Protected Function GetInputFileType(ByVal RawDataType As String) As DeconToolsFileTypeConstants

		Dim InstrumentClass As String = m_jobParams.GetParam("instClass")


		'Gets the Decon2LS file type based on the input data type
		Select Case RawDataType.ToLower
			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
				Return DeconToolsFileTypeConstants.Finnigan

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
				Return DeconToolsFileTypeConstants.Agilent_WIFF

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
				Return DeconToolsFileTypeConstants.PNNL_UIMF

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS
				Return DeconToolsFileTypeConstants.Agilent_D

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
				Return DeconToolsFileTypeConstants.Micromass_Rawdata

			Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS
				If InstrumentClass.ToLower = "brukerftms" Then
					'Data off of Bruker FTICR
					Return DeconToolsFileTypeConstants.Bruker

				ElseIf InstrumentClass.ToLower = "finnigan_fticr" Then
					'Data from old Finnigan FTICR
					Return DeconToolsFileTypeConstants.SUNEXTREL
				Else
					'Should never get here
					Return DeconToolsFileTypeConstants.Undefined
				End If

			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER
				Return DeconToolsFileTypeConstants.Bruker

			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_SPOT

				' TODO: Add support for this after Decon2LS is updated
				'Return DeconToolsFileTypeConstants.Bruker_15T

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS_V2 does not yet support Bruker MALDI data (" & RawDataType & ")")
				Return DeconToolsFileTypeConstants.Undefined


			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_IMAGING

				' TODO: Add support for this after Decon2LS is updated
				'Return DeconToolsFileTypeConstants.Bruker_15T

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS_V2 does not yet support Bruker MALDI data (" & RawDataType & ")")
				Return DeconToolsFileTypeConstants.Undefined

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_MZXML_FILES
				Return DeconToolsFileTypeConstants.MZXML_Rawdata

			Case Else
				'Should never get this value
				Return DeconToolsFileTypeConstants.Undefined
		End Select

	End Function


	Protected Sub ParseDeconToolsLogFile(ByRef blnFinishedProcessing As Boolean, ByRef dtFinishTime As System.DateTime)

		Dim fiFileInfo As System.IO.FileInfo
		Dim srInFile As System.IO.StreamReader

		Dim strLogFilePath As String
		Dim strLineIn As String
		Dim blnDateValid As Boolean

		Dim intCharIndex As Integer

		Dim strScanFrameLine As String = String.Empty

		blnFinishedProcessing = False

		Try
			Select Case mRawDataType
				Case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS, clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER
					' As of 11/19/2010, the _Log.txt file is created inside the .D folder
					strLogFilePath = System.IO.Path.Combine(mInputFilePath, m_Dataset) & "_log.txt"
				Case Else
					strLogFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(mInputFilePath) & "_log.txt")
			End Select

			If System.IO.File.Exists(strLogFilePath) Then
				srInFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

				Do While srInFile.Peek >= 0
					strLineIn = srInFile.ReadLine

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						intCharIndex = strLineIn.ToLower.IndexOf("finished file processing")
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
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools log file reports 'finished file processing' at " & dtFinishTime.ToString())
							End If

							blnFinishedProcessing = True
							Exit Do
						End If

						intCharIndex = strLineIn.ToLower.IndexOf("scan/frame")
						If intCharIndex >= 0 Then
							strScanFrameLine = strLineIn.Substring(intCharIndex)
						End If
					End If
				Loop
			End If

		Catch ex As System.Exception
			' Ignore errors here
		Finally
			If Not srInFile Is Nothing Then
				srInFile.Close()
			End If
		End Try

		If Not String.IsNullOrWhiteSpace(strScanFrameLine) Then
			' Parse strScanFrameLine
			' It will look like:
			' Scan/Frame= 347; PercentComplete= 2.7; AccumlatedFeatures= 614

			Dim strProgressStats() As String
			Dim kvStat As System.Collections.Generic.KeyValuePair(Of String, String)

			strProgressStats = strScanFrameLine.Split(";"c)

			For i As Integer = 0 To strProgressStats.Length - 1
				kvStat = ParseKeyValue(strProgressStats(i))
				If Not String.IsNullOrWhiteSpace(kvStat.Key) Then
					Select Case kvStat.Key
						Case "Scan/Frame"
							Integer.TryParse(kvStat.Value, mDeconToolsStatus.CurrentLCScan)
						Case "PercentComplete"
							Single.TryParse(kvStat.Value, mDeconToolsStatus.PercentComplete)
						Case "AccumlatedFeatures"

					End Select
				End If
			Next

			m_progress = mDeconToolsStatus.PercentComplete

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

	Protected Function SpecifyInputFilePath(ByVal RawDataType As String) As String

		'Based on the raw data type, assembles a string telling Decon2LS the name of the input file or folder
		Select Case RawDataType.ToLower
			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_WIFF_EXTENSION)

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_UIMF_EXTENSION)

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION

			Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_RAW_EXTENSION & "/_FUNC001.DAT"

			Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset)

			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER
				' Bruker_FT folders are actually .D folders
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION

			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_SPOT
				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset)

			Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_MALDI_IMAGING
				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''
				Return System.IO.Path.Combine(m_WorkDir, m_Dataset)

			Case Else
				'Should never get this value
				Return ""
		End Select

	End Function

	''' <summary>
	''' Stores the tool version info in the database
	''' </summary>
	''' <remarks></remarks>
	Protected Function StoreToolVersionInfo(ByVal strDeconToolsProgLoc As String) As Boolean

		Dim strToolVersionInfo As String = String.Empty
		Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)
		Dim ioDeconToolsInfo As System.IO.FileInfo
		Dim blnSuccess As Boolean

		Dim reParseVersion As System.Text.RegularExpressions.Regex
		Dim reMatch As System.Text.RegularExpressions.Match

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioDeconToolsInfo = New System.IO.FileInfo(strDeconToolsProgLoc)
		If Not ioDeconToolsInfo.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of System.IO.FileInfo))
			Catch ex As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
				Return False
			End Try

			Return False
		End If

		' Lookup the version of the DeconConsole application
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioDeconToolsInfo.FullName)
		If Not blnSuccess Then Return False

		' Parse out the DeconConsole Build number using a RegEx
		' strToolVersionInfo should look like: DeconConsole, Version=1.0.4400.22961

		mDeconConsoleBuild = 0
		reParseVersion = New System.Text.RegularExpressions.Regex("Version=\d+\.\d+\.(\d+)")
		reMatch = reParseVersion.Match(strToolVersionInfo)
		If reMatch.Success Then
			If Not Integer.TryParse(reMatch.Groups.Item(1).Value, mDeconConsoleBuild) Then
				' Error parsing out the version
				m_message = "Error determining DeconConsole version, cannot convert build to integer"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strToolVersionInfo)
				Return False
			End If
		Else
			m_message = "Error determining DeconConsole version, RegEx did not match"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & strToolVersionInfo)
			Return False
		End If

		' Lookup the version of the DeconTools Backend (in the DeconTools folder)
		Dim strDeconToolsBackendPath As String = System.IO.Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconTools.Backend.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDeconToolsBackendPath)
		If Not blnSuccess Then Return False

		' Lookup the version of the UIMFLibrary (in the DeconTools folder)
		Dim strDLLPath As String = System.IO.Path.Combine(ioDeconToolsInfo.DirectoryName, "UIMFLibrary.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Lookup the version of DeconEngine (in the DeconTools folder)
		strDLLPath = System.IO.Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngine.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Lookup the version of DeconEngineV2 (in the DeconTools folder)
		strDLLPath = System.IO.Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngineV2.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
		ioToolFiles.Add(New System.IO.FileInfo(strDeconToolsProgLoc))
		ioToolFiles.Add(New System.IO.FileInfo(strDeconToolsBackendPath))

		Try
			Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Read the start and end scan values from the DeconTools parameter file
	''' </summary>
	''' <param name="strParamFileCurrent"></param>
	''' <param name="MinScanValueFromParamFile"></param>
	''' <param name="MaxScanValueFromParamFile"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function GetScanValues(ByVal strParamFileCurrent As String, ByRef MinScanValueFromParamFile As Integer, ByRef MaxScanValueFromParamFile As Integer) As Boolean
		Dim objParamFile As System.Xml.XmlDocument
		Dim objNode As System.Xml.XmlNode

		Dim blnMinScanFound As Boolean
		Dim blnMaxScanFound As Boolean

		Try
			MinScanValueFromParamFile = 0
			MaxScanValueFromParamFile = 100000

			If Not System.IO.File.Exists(strParamFileCurrent) Then
				' Parameter file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFileCurrent)

				Return False
			Else
				' Open the file and parse the XML
				objParamFile = New System.Xml.XmlDocument
				objParamFile.Load(New System.IO.FileStream(strParamFileCurrent, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Look for the XML: <MinScan></MinScan>
				objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/MinScan")

				If Not objNode Is Nothing AndAlso objNode.HasChildNodes Then
					' Match found
					' Read the value of MinScan
					If System.Int32.TryParse(objNode.ChildNodes(0).Value, MinScanValueFromParamFile) Then
						blnMinScanFound = True
					End If
				End If

				' Look for the XML: <MinScan></MinScan>
				objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/MaxScan")

				If Not objNode Is Nothing AndAlso objNode.HasChildNodes Then
					' Match found
					' Read the value of MinScan
					If System.Int32.TryParse(objNode.ChildNodes(0).Value, MaxScanValueFromParamFile) Then
						blnMaxScanFound = True
					End If
				End If
			End If
		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GetScanValues: " & ex.Message)
			Return False
		End Try

		If blnMinScanFound Or blnMaxScanFound Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Create a temporary parameter file using the custom scan range to analyze
	''' </summary>
	''' <param name="strParamFile"></param>
	''' <param name="strParamFileTemp"></param>
	''' <param name="NewMinScanValue"></param>
	''' <param name="NewMaxScanValue"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function WriteTempParamFile(ByVal strParamFile As String, ByVal strParamFileTemp As String, ByVal NewMinScanValue As Integer, ByRef NewMaxScanValue As Integer) As Boolean
		Dim objParamFile As System.Xml.XmlDocument
		Dim swTempParamFile As System.IO.StreamWriter
		Dim objTempParamFile As System.Xml.XmlTextWriter

		Try
			If Not System.IO.File.Exists(strParamFile) Then
				' Parameter file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFile)

				Return False
			Else
				' Open the file and parse the XML
				objParamFile = New System.Xml.XmlDocument
				objParamFile.Load(New System.IO.FileStream(strParamFile, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Look for the XML: <UseScanRange></UseScanRange> in the Miscellaneous section
				' Set its value to "True" (this sub will add it if not present)
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "UseScanRange", "True")

				' Now update the MinScan value
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MinScan", NewMinScanValue.ToString)

				' Now update the MaxScan value
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MaxScan", NewMaxScanValue.ToString)

				Try
					' Now write out the XML to strParamFileTemp
					swTempParamFile = New System.IO.StreamWriter(New System.IO.FileStream(strParamFileTemp, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					objTempParamFile = New System.Xml.XmlTextWriter(swTempParamFile)
					objTempParamFile.Indentation = 1
					objTempParamFile.IndentChar = ControlChars.Tab
					objTempParamFile.Formatting = Xml.Formatting.Indented

					objParamFile.WriteContentTo(objTempParamFile)

					swTempParamFile.Close()

				Catch ex As System.Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error writing new param file in WriteTempParamFile: " & ex.Message)
					Return False

				End Try

			End If
		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading existing param file in WriteTempParamFile: " & ex.Message)
			Return False
		End Try

		Return True


	End Function

	''' <summary>
	''' Looks for the section specified by parameter XPathForSection.  If found, updates its value to NewElementValue.  If not found, tries to add a new node with name ElementName
	''' </summary>
	''' <param name="objXMLDocument">XML Document object</param>
	''' <param name="XPathForSection">XPath specifying the section that contains the desired element.  For example: "//parameters/Miscellaneous"</param>
	''' <param name="ElementName">Element name to find (or add)</param>
	''' <param name="NewElementValue">New value for this element</param>
	''' <remarks></remarks>
	Private Sub WriteTempParamFileUpdateElementValue(ByRef objXMLDocument As System.Xml.XmlDocument, ByVal XPathForSection As String, ByVal ElementName As String, ByVal NewElementValue As String)
		Dim objNode As System.Xml.XmlNode
		Dim objNewChild As System.Xml.XmlElement

		objNode = objXMLDocument.SelectSingleNode(XPathForSection & "/" & ElementName)

		If Not objNode Is Nothing Then
			If objNode.HasChildNodes Then
				' Match found; update the value
				objNode.ChildNodes(0).Value = NewElementValue
			End If
		Else
			objNode = objXMLDocument.SelectSingleNode(XPathForSection)

			If Not objNode Is Nothing Then
				objNewChild = CType(objXMLDocument.CreateNode(Xml.XmlNodeType.Element, ElementName, ""), Xml.XmlElement)
				objNewChild.InnerXml = NewElementValue

				objNode.AppendChild(objNewChild)
			End If

		End If

	End Sub

#End Region

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow
        Static dtLastLogCheckTime As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress)
        End If


        ' Parse the log file every 30 seconds to determine the % complete
        If System.DateTime.UtcNow.Subtract(dtLastLogCheckTime).TotalSeconds >= 30 Then
            dtLastLogCheckTime = System.DateTime.UtcNow

            Dim dtFinishTime As System.DateTime
            Dim blnFinishedProcessing As Boolean

            ParseDeconToolsLogFile(blnFinishedProcessing, dtFinishTime)

            Debug.WriteLine("Current Scan: " & mDeconToolsStatus.CurrentLCScan)
            If m_DebugLevel >= 2 Then

                Dim strProgressMessage As String

                If mDeconToolsStatus.IsUIMF Then
                    strProgressMessage = "Frame=" & mDeconToolsStatus.CurrentLCScan
                Else
                    strProgressMessage = "Scan=" & mDeconToolsStatus.CurrentLCScan
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "... " & strProgressMessage & ", " & m_progress.ToString("0.0") & "% complete")

            End If

            Const MAX_LOGFINISHED_WAITTIME_SECONDS As Integer = 120
            If blnFinishedProcessing Then
                ' The Decon2LS Log File reports that the task is complete
                ' If it finished over MAX_LOGFINISHED_WAITTIME_SECONDS seconds ago, then send an abort to the CmdRunner

                If System.DateTime.Now().Subtract(dtFinishTime).TotalSeconds >= MAX_LOGFINISHED_WAITTIME_SECONDS Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Note: Log file reports finished over " & MAX_LOGFINISHED_WAITTIME_SECONDS & " seconds ago, but the DeconTools CmdRunner is still active")

                    mDeconToolsFinishedDespiteProgRunnerError = True

                    ' Abort processing
                    CmdRunner.AbortProgramNow()

                    System.Threading.Thread.Sleep(3000)
                End If
            End If

        End If


    End Sub

End Class
