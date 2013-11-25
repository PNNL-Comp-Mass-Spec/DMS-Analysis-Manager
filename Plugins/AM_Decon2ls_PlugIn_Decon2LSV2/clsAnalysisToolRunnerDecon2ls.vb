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
Imports System.IO

Public Class clsAnalysisToolRunnerDecon2ls
	Inherits clsAnalysisToolRunnerBase

#Region "Constants"
	Private Const DECON2LS_SCANS_FILE_SUFFIX As String = "_scans.csv"
	Private Const DECON2LS_ISOS_FILE_SUFFIX As String = "_isos.csv"
	Private Const DECON2LS_PEAKS_FILE_SUFFIX As String = "_peaks.txt"

#End Region

#Region "Module variables"

	Protected mRawDataType As clsAnalysisResources.eRawDataTypeConstants = clsAnalysisResources.eRawDataTypeConstants.Unknown
	Protected mRawDataTypeName As String = String.Empty

	Protected mInputFilePath As String = String.Empty

	Protected mDeconConsoleBuild As Integer = 0

	Protected mDeconToolsExceptionThrown As Boolean
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
		BadErrorLogFile = 4
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
	Private Function AssembleResults(ByVal oDeconToolsParamFileReader As AnalysisManagerBase.clsXMLParamFileReader) As IJobParams.CloseOutType

		Dim ScansFilePath As String
		Dim IsosFilePath As String
		Dim PeaksFilePath As String
		Dim blnDotDFolder As Boolean = False

		Try

			ScansFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX)
			IsosFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX)
			PeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX)

			Select Case mRawDataType
				Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
					' As of 11/19/2010, the Decon2LS output files are created inside the .D folder
					' Still true as of 5/18/2012
					blnDotDFolder = True
				Case Else
					If Not File.Exists(IsosFilePath) And Not File.Exists(ScansFilePath) Then
						If mInputFilePath.ToLower().EndsWith(".d") Then
							blnDotDFolder = True
						End If
					End If
			End Select

			If blnDotDFolder AndAlso Not File.Exists(IsosFilePath) AndAlso Not File.Exists(ScansFilePath) Then
				' Copy the files from the .D folder to the work directory

				Dim fiSrcFilePath As FileInfo

				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying Decon2LS result files from the .D folder to the working directory")
				End If

				fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_SCANS_FILE_SUFFIX))
				If fiSrcFilePath.Exists Then
					fiSrcFilePath.CopyTo(ScansFilePath)
				End If

				fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX))
				If fiSrcFilePath.Exists Then
					fiSrcFilePath.CopyTo(IsosFilePath)
				End If

				fiSrcFilePath = New FileInfo(Path.Combine(mInputFilePath, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX))
				If fiSrcFilePath.Exists Then
					fiSrcFilePath.CopyTo(PeaksFilePath)
				End If

			End If

			m_jobParams.AddResultFileToKeep(ScansFilePath)
			m_jobParams.AddResultFileToKeep(IsosFilePath)

			Dim blnWritePeaksToTextFile As Boolean = oDeconToolsParamFileReader.GetParameter("WritePeaksToTextFile", False)

			' Examine the Peaks File to check whether it only has a header line, or it has multiple data lines
			If Not ResultsFileHasData(PeaksFilePath) Then
				' The file does not have any data lines
				' Raise an error if it should have had data
				If blnWritePeaksToTextFile Then
					m_EvalMessage = "Warning: no results in DeconTools Peaks.txt file"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
				Else
					' Superfluous file; delete it
					Try
						If File.Exists(PeaksFilePath) Then
							File.Delete(PeaksFilePath)
						End If
					Catch ex As Exception
						' Ignore errors here
					End Try
				End If
			End If

			Dim strDeconvolutionType As String = oDeconToolsParamFileReader.GetParameter("DeconvolutionType", String.Empty)
			Dim blnEmptyIsosFileExpected As Boolean

			If strDeconvolutionType = "None" Then
				blnEmptyIsosFileExpected = True
			End If

			If blnEmptyIsosFileExpected Then
				' The _isos.csv file should be empty; delete it
				If Not ResultsFileHasData(IsosFilePath) Then
					' The file does not have any data lines
					Try
						If File.Exists(IsosFilePath) Then
							File.Delete(IsosFilePath)
						End If
					Catch ex As Exception
						' Ignore errors here
					End Try
				End If

			Else
				' Make sure the Isos File exists
				If Not File.Exists(IsosFilePath) Then
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

			End If


		Catch ex As System.Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function CacheDeconToolsParamFile(ByVal strParamFilePath As String) As AnalysisManagerBase.clsXMLParamFileReader

		Dim oDeconToolsParamFileReader As AnalysisManagerBase.clsXMLParamFileReader

		Try
			oDeconToolsParamFileReader = New AnalysisManagerBase.clsXMLParamFileReader(strParamFilePath)

			If oDeconToolsParamFileReader.ParameterCount = 0 Then
				m_message = "DeconTools parameter file is empty (or could not be parsed)"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return Nothing
			End If

		Catch ex As Exception
			m_message = "Error parsing parameter file: " + ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return Nothing
		End Try

		Return oDeconToolsParamFileReader

	End Function

	''' <summary>
	''' Use MSFileInfoScanner to create QC Plots
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function CreateQCPlots() As IJobParams.CloseOutType

		Dim blnSuccess As Boolean = False

		Try

			Dim strInputFilePath As String

			Dim strMSFileInfoScannerDir As String
			Dim strMSFileInfoScannerDLLPath As String

			strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir")
			If String.IsNullOrEmpty(strMSFileInfoScannerDir) Then
				m_message = "Manager parameter 'MSFileInfoScannerDir' is not defined"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateQCPlots: " + m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll")
			If Not File.Exists(strMSFileInfoScannerDLLPath) Then
				m_message = "File Not Found: " + strMSFileInfoScannerDLLPath
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CreateQCPlots: " + m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			strInputFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_ISOS_FILE_SUFFIX)

			Dim objQCPlotGenerator = New clsDeconToolsQCPlotsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel)

			' Create the QC Plot .png files and associated Index.html file
			blnSuccess = objQCPlotGenerator.CreateQCPlots(strInputFilePath, m_WorkDir)

			If blnSuccess Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generated QC Plots file using " + strInputFilePath)
				End If

			Else
				m_message = "Error generating QC Plots files with clsDeconToolsQCPlotsGenerator"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, objQCPlotGenerator.ErrorMessage)
				If objQCPlotGenerator.MSFileInfoScannerErrorCount > 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner encountered " + objQCPlotGenerator.MSFileInfoScannerErrorCount.ToString() + " errors")
				End If
			End If

		Catch ex As Exception
			m_message = "Error in CreateQCPlots: " + ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		If blnSuccess Then
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
			Return (IJobParams.CloseOutType.CLOSEOUT_FAILED)
		End If


	End Function

	Private Function CreateNewExportFile(ByVal exportFileName As String) As StreamWriter
		Dim ef As StreamWriter

		If File.Exists(exportFileName) Then
			'post error to log
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->createNewExportFile: Export file already exists (" & exportFileName & "); this is unexpected")
			Return Nothing
		End If

		ef = New StreamWriter(exportFileName, False)
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

		Dim srInFile As StreamReader

		Dim strLineIn As String
		Dim blnHeaderLineProcessed As Boolean

		intDataLineCount = 0
		blnHeaderLineProcessed = False

		Try

			If File.Exists(IsosFilePath) Then
				srInFile = New StreamReader(New FileStream(IsosFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

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

	''' <summary>
	''' Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim eResult As IJobParams.CloseOutType
		'Dim TcpPort As Integer = CInt(m_mgrParams.GetParam("tcpport"))
		Dim eReturnCode As IJobParams.CloseOutType

		mRawDataTypeName = m_jobParams.GetParam("RawDataType")
		mRawDataType = clsAnalysisResources.GetRawDataType(mRawDataTypeName)

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

		If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Create the QC plots
			eReturnCode = CreateQCPlots()
		End If
		
		' Zip the _Peaks.txt file (if it exists)
		ZipPeaksFile()

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
			objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

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

		Dim strParamFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName"))
		Dim blnDecon2LSError As Boolean

		' Cache the parameters in the DeconTools parameter file

		Dim oDeconToolsParamFileReader As AnalysisManagerBase.clsXMLParamFileReader
		oDeconToolsParamFileReader = CacheDeconToolsParamFile(strParamFilePath)

		If oDeconToolsParamFileReader Is Nothing Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

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
		mDeconToolsExceptionThrown = False
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
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished")
		End If

		' Determine reason for Decon2LS finish
		If mDeconToolsFinishedDespiteProgRunnerError And Not mDeconToolsExceptionThrown Then
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

				Case DeconToolsStateType.BadErrorLogFile
					blnDecon2LSError = True

					' Sleep for 1 minute
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sleeping for 1 minute")
					System.Threading.Thread.Sleep(60 * 1000)

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
			eResult = AssembleResults(oDeconToolsParamFileReader)

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

			If mDeconToolsExceptionThrown Then
				eDeconToolsStatus = DeconToolsStateType.ErrorCode
			ElseIf blnSuccess Then
				eDeconToolsStatus = DeconToolsStateType.Complete
			ElseIf blnFinishedProcessing Then
				mDeconToolsFinishedDespiteProgRunnerError = True
				eDeconToolsStatus = DeconToolsStateType.Complete
			Else
				eDeconToolsStatus = DeconToolsStateType.ErrorCode
			End If

			' Look for file Dataset*BAD_ERROR_log.txt
			' If it exists, an exception occurred
			Dim diWorkdir As DirectoryInfo
			diWorkdir = New DirectoryInfo(Path.Combine(m_WorkDir))

			For Each fiFile As FileInfo In diWorkdir.GetFiles(m_Dataset & "*BAD_ERROR_log.txt")
				m_message = "Error running DeconTools; Bad_Error_log file exists"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiFile.Name)
				eDeconToolsStatus = DeconToolsStateType.BadErrorLogFile
				Exit For
			Next

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
				' Future: Case DeconToolsFileTypeConstants.MZML_Rawdata : Return "MZML_Rawdata"
			Case DeconToolsFileTypeConstants.PNNL_IMS : Return "PNNL_IMS"
			Case DeconToolsFileTypeConstants.PNNL_UIMF : Return "PNNL_UIMF"
			Case DeconToolsFileTypeConstants.SUNEXTREL : Return "SUNEXTREL"
			Case Else
				Return "Undefined"
		End Select

	End Function

	Protected Function GetInputFileType(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As DeconToolsFileTypeConstants

		Dim InstrumentClass As String = m_jobParams.GetParam("instClass")


		'Gets the Decon2LS file type based on the input data type
		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				Return DeconToolsFileTypeConstants.Finnigan

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
				Return DeconToolsFileTypeConstants.Agilent_WIFF

			Case clsAnalysisResources.eRawDataTypeConstants.UIMF
				Return DeconToolsFileTypeConstants.PNNL_UIMF

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
				Return DeconToolsFileTypeConstants.Agilent_D

			Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
				Return DeconToolsFileTypeConstants.Micromass_Rawdata

			Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders
				If InstrumentClass.ToLower = "brukerftms" Then
					'Data from Bruker FTICR
					Return DeconToolsFileTypeConstants.Bruker

				ElseIf InstrumentClass.ToLower = "finnigan_fticr" Then
					'Data from old Finnigan FTICR
					Return DeconToolsFileTypeConstants.SUNEXTREL
				Else
					'Should never get here
					Return DeconToolsFileTypeConstants.Undefined
				End If

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
				Return DeconToolsFileTypeConstants.Bruker

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot

				' TODO: Add support for this after Decon2LS is updated
				'Return DeconToolsFileTypeConstants.Bruker_15T

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS_V2 does not yet support Bruker MALDI data (" & eRawDataType.ToString() & ")")
				Return DeconToolsFileTypeConstants.Undefined


			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging

				' TODO: Add support for this after Decon2LS is updated
				'Return DeconToolsFileTypeConstants.Bruker_15T

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS_V2 does not yet support Bruker MALDI data (" & eRawDataType.ToString() & ")")
				Return DeconToolsFileTypeConstants.Undefined

			Case clsAnalysisResources.eRawDataTypeConstants.mzXML
				Return DeconToolsFileTypeConstants.MZXML_Rawdata

			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				' TODO: Add support for this after Decon2LS is updated
				'Return DeconToolsFileTypeConstants.MZML_Rawdata

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS_V2 does not yet support mzML data")
				Return DeconToolsFileTypeConstants.Undefined

			Case Else
				'Should never get this value
				Return DeconToolsFileTypeConstants.Undefined
		End Select

	End Function


	Protected Sub ParseDeconToolsLogFile(ByRef blnFinishedProcessing As Boolean, ByRef dtFinishTime As System.DateTime)

		Dim fiFileInfo As FileInfo

		Dim strLogFilePath As String
		Dim strLineIn As String
		Dim blnDateValid As Boolean

		Dim intCharIndex As Integer

		Dim strScanFrameLine As String = String.Empty

		blnFinishedProcessing = False

		Try
			Select Case mRawDataType
				Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
					' As of 11/19/2010, the _Log.txt file is created inside the .D folder
					strLogFilePath = Path.Combine(mInputFilePath, m_Dataset) & "_log.txt"
				Case Else
					strLogFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mInputFilePath) & "_log.txt")
			End Select

			If File.Exists(strLogFilePath) Then

				Using srInFile As StreamReader = New StreamReader(New FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

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
									fiFileInfo = New FileInfo(strLogFilePath)
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

							intCharIndex = strLineIn.IndexOf("ERROR THROWN")
							If intCharIndex > 0 Then
								' An exception was reported in the log file; treat this as a fatal error
								m_message = "Error thrown by DeconTools"

								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DeconTools reports " & strLineIn.Substring(intCharIndex))
								mDeconToolsExceptionThrown = True

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

	''' <summary>
	''' Opens the specified results file from DeconTools and looks for at least two non-blank lines
	''' </summary>
	''' <param name="strFilePath"></param>
	''' <returns>True if two or more non-blank lines; otherwise false</returns>
	''' <remarks></remarks>
	Protected Function ResultsFileHasData(ByVal strFilePath As String) As Boolean

		If Not File.Exists(strFilePath) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file not found: " & strFilePath)
			Return False
		End If

		Dim intDataLineCount As Integer = 0

		' Open the DeconTools results file
		' The first line is the header lines
		' Lines after that are data lines

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Opening the DeconTools results file: " & strFilePath)

		Using srReader As StreamReader = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
			While srReader.Peek > -1 AndAlso intDataLineCount < 2

				Dim strLineIn As String = srReader.ReadLine()
				If Not String.IsNullOrWhiteSpace(strLineIn) Then
					intDataLineCount += 1
				End If
			End While
		End Using

		If intDataLineCount >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file has at least two non-blank lines")
			Return True
		Else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DeconTools results file is empty")
			Return False
		End If

	End Function

	Protected Function SpecifyInputFilePath(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As String

		'Based on the raw data type, assembles a string telling Decon2LS the name of the input file or folder

		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				Return Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
				Return Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_WIFF_EXTENSION)

			Case clsAnalysisResources.eRawDataTypeConstants.UIMF
				Return Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_UIMF_EXTENSION)

			Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
				Return Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION

			Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
				Return Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_RAW_EXTENSION & "/_FUNC001.DAT"

			Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders
				Return Path.Combine(m_WorkDir, m_Dataset)

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder
				' Bruker_FT folders are actually .D folders
				Return Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
				' Bruker_TOFBaf folders are actually .D folders
				Return Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot
				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''
				Return Path.Combine(m_WorkDir, m_Dataset)

			Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging
				''''''''''''''''''''''''''''''''''''
				' TODO: Finalize this code
				'       DMS doesn't yet have a BrukerTOF dataset 
				'        so we don't know the official folder structure
				''''''''''''''''''''''''''''''''''''
				Return Path.Combine(m_WorkDir, m_Dataset)

			Case clsAnalysisResources.eRawDataTypeConstants.mzXML
				Return Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				Return Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

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
		Dim ioDeconToolsInfo As FileInfo
		Dim blnSuccess As Boolean

		Dim reParseVersion As System.Text.RegularExpressions.Regex
		Dim reMatch As System.Text.RegularExpressions.Match

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
		End If

		ioDeconToolsInfo = New FileInfo(strDeconToolsProgLoc)
		If Not ioDeconToolsInfo.Exists Then
			Try
				strToolVersionInfo = "Unknown"
				Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New System.Collections.Generic.List(Of FileInfo))
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
		Dim strDeconToolsBackendPath As String = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconTools.Backend.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDeconToolsBackendPath)
		If Not blnSuccess Then Return False

		' Lookup the version of the UIMFLibrary (in the DeconTools folder)
		Dim strDLLPath As String = Path.Combine(ioDeconToolsInfo.DirectoryName, "UIMFLibrary.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Lookup the version of DeconEngine (in the DeconTools folder)
		strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngine.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Lookup the version of DeconEngineV2 (in the DeconTools folder)
		strDLLPath = Path.Combine(ioDeconToolsInfo.DirectoryName, "DeconEngineV2.dll")
		blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, strDLLPath)
		If Not blnSuccess Then Return False

		' Store paths to key DLLs in ioToolFiles
		Dim ioToolFiles As New System.Collections.Generic.List(Of FileInfo)
		ioToolFiles.Add(New FileInfo(strDeconToolsProgLoc))
		ioToolFiles.Add(New FileInfo(strDeconToolsBackendPath))

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

			If Not File.Exists(strParamFileCurrent) Then
				' Parameter file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFileCurrent)

				Return False
			Else
				' Open the file and parse the XML
				objParamFile = New System.Xml.XmlDocument
				objParamFile.Load(New FileStream(strParamFileCurrent, FileMode.Open, FileAccess.Read, FileShare.Read))

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
		Dim swTempParamFile As StreamWriter
		Dim objTempParamFile As System.Xml.XmlTextWriter

		Try
			If Not File.Exists(strParamFile) Then
				' Parameter file not found
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFile)

				Return False
			Else
				' Open the file and parse the XML
				objParamFile = New System.Xml.XmlDocument
				objParamFile.Load(New FileStream(strParamFile, FileMode.Open, FileAccess.Read, FileShare.Read))

				' Look for the XML: <UseScanRange></UseScanRange> in the Miscellaneous section
				' Set its value to "True" (this sub will add it if not present)
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "UseScanRange", "True")

				' Now update the MinScan value
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MinScan", NewMinScanValue.ToString)

				' Now update the MaxScan value
				WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MaxScan", NewMaxScanValue.ToString)

				Try
					' Now write out the XML to strParamFileTemp
					swTempParamFile = New StreamWriter(New FileStream(strParamFileTemp, FileMode.Create, FileAccess.Write, FileShare.Read))

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

	Private Function ZipPeaksFile() As Boolean

		Dim strPeaksFilePath As String
		Dim strZippedPeaksFilePath As String

		Try
			strPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & DECON2LS_PEAKS_FILE_SUFFIX)
			strZippedPeaksFilePath = Path.Combine(m_WorkDir, m_Dataset & "_peaks.zip")

			If File.Exists(strPeaksFilePath) Then

				If Not MyBase.ZipFile(strPeaksFilePath, False, strZippedPeaksFilePath) Then
					Dim Msg As String = "Error zipping " & DECON2LS_PEAKS_FILE_SUFFIX & " file, job " & m_JobNum
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
					m_message = clsGlobal.AppendToComment(m_message, "Error zipping Peaks.txt file")
					Return False
				End If

				' Add the _peaks.txt file to .FilesToDelete since we only want to keep the Zipped version
				m_jobParams.AddResultFileToSkip(Path.GetFileName(strPeaksFilePath))

			End If

		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerDecon2ls.ZipPeaksFile, Exception zipping Peaks.txt file, job " & m_JobNum & ": " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			m_message = clsGlobal.AppendToComment(m_message, "Error zipping Peaks.txt file")
			Return False
		End Try

		Return True

	End Function


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
