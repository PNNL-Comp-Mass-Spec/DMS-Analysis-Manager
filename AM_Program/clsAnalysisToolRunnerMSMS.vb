'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase
Imports System.Threading
Imports System.Collections.Specialized

Namespace AnalysisManagerMSMSBase

	Public Class clsAnalysisToolRunnerMSMS
		Inherits clsAnalysisToolRunnerBase

		'*********************************************************************************************************
		'Base class for all tool runners using Sequest-type analysis (Sequest & XTandem at present
		'*********************************************************************************************************

#Region "Module variables"
		Dim WithEvents m_CmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
		''' <summary>
		''' Runs the analysis tool
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks>This method is used to meet the interface requirement</remarks>
		Public Overrides Function RunTool() As IJobParams.CloseOutType
			Dim result As IJobParams.CloseOutType

			'do the stuff in the base class
			If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			'implement master MSMS analysis tool sequence

			'Create spectra files
			result = CreateAndFilterMSMSSpectra()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

			'Run the analysis tool
			result = OperateAnalysisTool()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

			'Stop the job timer
			m_StopTime = Now

			'Make results folder and transfer results
			result = DispositionResults()
			If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return result

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function

		''' <summary>
		''' Creates DTA files and filters if necessary
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Public Overridable Function CreateAndFilterMSMSSpectra() As IJobParams.CloseOutType

			'Make the spectra files
			Dim Result As IJobParams.CloseOutType = MakeSpectraFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

			'Apply filters, if appropriate
			Result = FilterSpectraFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

			'Concatenate spectra files
			Result = ConcatSpectraFiles()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

			'Zip concatenated spectra files
			Result = ZipConcDtaFile()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return Result

			'If we got to here, everything's OK
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function

		Public Overridable Function OperateAnalysisTool() As IJobParams.CloseOutType
		End Function

		''' <summary>
		''' Detailed method for running a tool
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Public Overridable Function DispositionResults() As IJobParams.CloseOutType
			Dim StepResult As IJobParams.CloseOutType

			'Make sure all files have released locks
			GC.Collect()
			GC.WaitForPendingFinalizers()
			Thread.Sleep(1000)

			'Delete stray sequest files
			Try
				StepResult = DeleteTempAnalFiles()
				If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					Return StepResult
				End If
			Catch Err As Exception
				m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception while deleting stray Sequest files, " & _
				 Err.Message, ILogger.logMsgType.logError, True)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Get rid of raw data file
			Try
				StepResult = DeleteDataFile()
				If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					Return StepResult
				End If
			Catch Err As Exception
				m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception while deleting data file, " & _
				 Err.Message, ILogger.logMsgType.logError, True)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Add the current job data to the summary file
			Try
				If Not UpdateSummaryFile() Then
					m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
					 ILogger.logMsgType.logWarning, LOG_DATABASE)
				End If
			Catch Err As Exception
				m_logger.PostEntry("Error creating summary file, job " & m_JobNum, _
				 ILogger.logMsgType.logWarning, LOG_DATABASE)
			End Try

			'Delete .dta files
			Dim TmpFile As String
			Dim FileList() As String
			Try
				FileList = Directory.GetFiles(m_WorkDir, "*.dta")
				For Each TmpFile In FileList
					DeleteFileWithRetries(TmpFile)
				Next
			Catch Err As Exception
				m_logger.PostError("Error deleting .dta files, job " & m_JobNum, Err, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error deleting .dta files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Delete unzipped concatenated dta files
			FileList = Directory.GetFiles(m_WorkDir, "*_dta.txt")
			For Each TmpFile In FileList
				Try
					If Path.GetFileName(TmpFile.ToLower) <> "lcq_dta.txt" Then
						DeleteFileWithRetries(TmpFile)
					End If
				Catch ex As Exception
					m_logger.PostEntry("Error: " & ex.Message & " deleting concatenated dta file, job " & m_JobNum, _
					 ILogger.logMsgType.logError, LOG_DATABASE)
					m_message = AppendToComment(m_message, "Error packaging results")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try
			Next

			'make results folder
			Try
				StepResult = MakeResultsFolder("Seq")
				If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					Return StepResult
				End If
			Catch Err As Exception
				m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception making results folder, " & _
				 Err.Message, ILogger.logMsgType.logError, True)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

		End Function

		''' <summary>
		''' Filters DTA files, if necessary
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Public Overridable Function FilterSpectraFiles() As IJobParams.CloseOutType

			'Filters spectra files (ie, dta's) using plugin specified in settings file
			Const SPECTRA_FILTER_SECTION As String = "SpectraFilter"
			Const UNKNOWN_FILTER_TYPE As String = "-UnknownFilterType-"
			Dim FilterType As String
			Dim FilterFound As Boolean = False

			m_logger.PostEntry("Filtering spectra files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

			'Determine if any filtering is specified in the settings file
			FilterFound = m_settingsFileParams.SectionPresent(SPECTRA_FILTER_SECTION)

			If Not FilterFound Then Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No special filtering specified

			'Get the filter type to determine which filter plugin should be loaded
			Dim SpectraFilter As ISpectraFilter

			FilterType = m_settingsFileParams.GetParam(SPECTRA_FILTER_SECTION, "FilterType", UNKNOWN_FILTER_TYPE)

			If FilterType Is Nothing OrElse FilterType.Trim.Length = 0 Then
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS			'Filter type setting is empty; no special filtering specified
			ElseIf FilterType = UNKNOWN_FILTER_TYPE Then
				'Filter type setting not found in spectrafilter
				FilterType = UNKNOWN_FILTER_TYPE
				m_logger.PostEntry("FilterType entry not found in section '" & SPECTRA_FILTER_SECTION & "' of the settings file", _
				 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				m_message = AppendToComment(m_message, "FilterType entry not found in section '" & SPECTRA_FILTER_SECTION & "'")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Load the plugin for the filter type
			SpectraFilter = clsPluginLoader.GetSpectraFilter(FilterType)
			If SpectraFilter Is Nothing Then
				m_logger.PostEntry("Error loading spectra filter '" & FilterType & "': " & clsPluginLoader.Message, _
				 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				m_message = AppendToComment(m_message, "Error loading spectra filter '" & FilterType & "'")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 0 Then
				m_logger.PostEntry("Loaded filter " & clsPluginLoader.Message, ILogger.logMsgType.logDebug, True)
			End If

			'Initialize the plugin
			Dim SetupParams As New ISpectraFilter.InitializationParams
			With SetupParams
				.DebugLevel = m_DebugLevel
				.JobParams = m_jobParams			' Note: This includes the settings file name via m_jobparams.GetParam("settingsFileName")
				.Logger = m_logger
				.MgrParams = m_mgrParams
				.MiscParams = New System.Collections.Specialized.StringDictionary			' Empty, since unused
				.OutputFolderPath = m_WorkDir
				.SourceFolderPath = m_WorkDir
				.StatusTools = m_StatusTools
			End With
			SpectraFilter.Setup(SetupParams)

			'Start the filtering process
			Try
				Dim RetVal As ISpectraFilter.ProcessStatus = SpectraFilter.Start
				If RetVal = ISpectraFilter.ProcessStatus.SFILT_ERROR Then
					m_logger.PostEntry("Error starting spectra filter: " & SpectraFilter.ErrMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				'Loop until filter finishes
				While (SpectraFilter.Status = ISpectraFilter.ProcessStatus.SFILT_STARTING) Or _
				 (SpectraFilter.Status = ISpectraFilter.ProcessStatus.SFILT_RUNNING)
					m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0.1, SpectraFilter.SpectraFileCount)
					Thread.Sleep(2000)				'Pause for 2 seconds
				End While
			Catch ex As Exception
				m_logger.PostEntry("Exception while filtering dta files: " & ex.Message, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Set internal spectra file count to that returned by the spectra filter
			m_DtaCount = SpectraFilter.SpectraFileCount

			m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, m_DtaCount)

			'Check for reason filter exited
			If SpectraFilter.Results = ISpectraFilter.ProcessResults.SFILT_FAILURE Then
				m_logger.PostEntry("Error filtering DTA files: " & SpectraFilter.ErrMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			ElseIf SpectraFilter.Results = ISpectraFilter.ProcessResults.SFILT_ABORTED Then
				m_logger.PostEntry("DTA filtering aborted", ILogger.logMsgType.logError, True)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Return results
			If SpectraFilter.Results = ISpectraFilter.ProcessResults.SFILT_NO_FILES_CREATED Then
				m_message = AppendToComment(m_message, "No spectra files remain after filtering")
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function

		''' <summary>
		''' Creates DTA files
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Public Overridable Function MakeSpectraFiles() As IJobParams.CloseOutType

			'Make individual spectra files from input raw data file, using plugin

			m_logger.PostEntry("Making spectra files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

			'Load the appropriate plugin
			Dim SpectraGen As ISpectraFileProcessor = clsPluginLoader.GetSpectraGenerator(m_jobParams.GetParam("rawdatatype"))
			If SpectraGen Is Nothing Then
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Error loading spectra processor: " & clsPluginLoader.Message, _
				 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				m_message = AppendToComment(m_message, "Error loading spectra processor")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Loaded spectra generator " & clsPluginLoader.Message, _
				 ILogger.logMsgType.logDebug, True)
			End If

			'Initialize the plugin
			Dim SetupParams As ISpectraFileProcessor.InitializationParams
			With SetupParams
				.DebugLevel = m_DebugLevel
				.JobParams = m_jobParams
				.Logger = m_logger
				.MgrParams = m_mgrParams
				.MiscParams = New System.Collections.Specialized.StringDictionary			'Nothing specified for now, so use dummy value
				.OutputFolderPath = m_WorkDir
				.SourceFolderPath = m_WorkDir
				.StatusTools = m_StatusTools
			End With
			SpectraGen.Setup(SetupParams)

			'Start the spectra generation process
			Try
				Dim RetVal As ISpectraFileProcessor.ProcessStatus = SpectraGen.Start
				If RetVal = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
					m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Error starting spectra processor: " & SpectraGen.ErrMsg, _
					 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				If m_DebugLevel > 0 Then
					m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Spectra generation started", _
					  ILogger.logMsgType.logDebug, True)
				End If

				'Loop until the spectra generator finishes
				While (SpectraGen.Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING) Or _
				 (SpectraGen.Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING)
					m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, SpectraGen.SpectraFileCount)
					Thread.Sleep(2000)				'Delay for 2 seconds

				End While
			Catch ex As Exception
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Exception while generating dta files: " & ex.Message, _
				  ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Set internal spectra file count to that returned by the spectra generator
			m_DtaCount = SpectraGen.SpectraFileCount

			m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, m_DtaCount)

			'Check for reason spectra generator exited
			If SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE Then
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Error making DTA files: " & SpectraGen.ErrMsg, _
				  ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			ElseIf SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_ABORTED Then
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: DTA generation aborted", _
				  ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Return results
			If SpectraGen.Results = ISpectraFileProcessor.ProcessResults.SF_NO_FILES_CREATED Then
				m_message = AppendToComment(m_message, "No spectra files created")
				Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			Else
				If m_DebugLevel > 0 Then
					m_logger.PostEntry("clsAnalysisToolRunnerMSMS.MakeSpectraFiles: Spectra generation completed", _
					  ILogger.logMsgType.logDebug, True)
				End If
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

		End Function

		''' <summary>
		''' Concatenates DTA files into a single test file
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function ConcatSpectraFiles() As IJobParams.CloseOutType

			'Packages dta files into concatenated text files

			m_logger.PostEntry("Concatenating spectra files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

			Dim ConcatTools As New clsConcatToolWrapper(m_WorkDir)
			If Not ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, m_jobParams.GetParam("datasetNum")) Then
				m_logger.PostEntry(ConcatTools.ErrMsg & ", job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error packaging results")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			End If

		End Function

		''' <summary>
		''' Deletes unused files after analysis completes
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function DeleteTempAnalFiles() As IJobParams.CloseOutType
		End Function

		''' <summary>
		''' Deletes .raw files from working directory
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks>Overridden for other types of input files</remarks>
		Protected Overridable Function DeleteDataFile() As IJobParams.CloseOutType

			'Deletes the .raw file from the working directory
			Dim FoundFiles() As String
			Dim MyFile As String

			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerMSMS.DeleteDataFile, executing method", ILogger.logMsgType.logDebug, True)
			End If

			'Delete the .raw file
			Try
				FoundFiles = Directory.GetFiles(m_WorkDir, "*.raw")
				For Each MyFile In FoundFiles
					If m_DebugLevel > 0 Then
						m_logger.PostEntry("clsAnalysisToolRunnerMSMS.DeleteDataFile, deleting file " & MyFile, ILogger.logMsgType.logDebug, True)
					End If
					DeleteFileWithRetries(MyFile)
				Next
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Catch Err As Exception
				m_logger.PostError("Error deleting .raw file, job " & m_JobNum, Err, LOG_DATABASE)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

		End Function

		''' <summary>
		''' Zips concatenated DTA file to reduce size
		''' </summary>
		''' <returns>CloseoutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function ZipConcDtaFile() As IJobParams.CloseOutType

			'Zips the concatenated dta file
			Dim DtaFileName As String = m_jobParams.GetParam("datasetNum") & "_dta.txt"
			'		Dim CmdStr As String

			m_logger.PostEntry("Zipping concatenated spectra file, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

			'Verify file exists
			If Not File.Exists(Path.Combine(m_WorkDir, DtaFileName)) Then
				m_logger.PostEntry("Unable to find concatenated dta file", ILogger.logMsgType.logError, True)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Zip the file
			Try
				Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
				Dim ZipFileName As String = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(DtaFileName)) & ".zip"
				If Not Zipper.MakeZipFile("-fast", ZipFileName, DtaFileName) Then
					Dim Msg As String = "Error zipping concat dta file, job " & m_JobNum
					m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Catch ex As Exception
				Dim Msg As String = "Exception zipping concat dta file, job " & m_JobNum & ": " & ex.Message
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'm_CmdRunner = New clsRunDosProgram(m_logger, m_WorkDir)

			''DAC debug
			'Debug.WriteLine("clsAnalysisToolRunnerMSMS.ZipConcDtaFile, calling RunProgram, thread " & Thread.CurrentThread.Name)

			''Set up a program runner to zip the file
			'CmdStr = "-add -fast " & Path.Combine(m_workdir, Path.GetFileNameWithoutExtension(DtaFileName)) & ".zip " & _
			' Path.Combine(m_workdir, DtaFileName)
			'If Not m_CmdRunner.RunProgram(m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"), CmdStr, "Zipper", True) Then
			'	m_logger.PostEntry("Error zipping concat dta file, job " & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
			'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			'End If

			''DAC debug
			'Debug.WriteLine("clsAnalysisToolRunnerMSMS.ZipConcDtaFile, RunProgram complete, thread " & Thread.CurrentThread.Name)

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function
#End Region

	End Class

End Namespace
