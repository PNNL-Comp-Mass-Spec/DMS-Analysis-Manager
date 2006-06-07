Option Strict On

Imports AnalysisManagerBase
Imports PRISM.Logging
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports AnalysisManagerMSMSBase
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerXT
	Inherits clsAnalysisToolRunnerMSMS

#Region "Module Variables"
	Protected Const PROGRESS_PCT_XTANDEM_RUNNING As Single = 5
	Protected Const PROGRESS_PCT_PEPTIDEHIT_START As Single = 95
	Protected Const PROGRESS_PCT_PEPTIDEHIT_COMPLETE As Single = 99

	Protected WithEvents CmdRunner As clsRunDosProgram
	'--------------------------------------------------------------------------------------------
	'Future section to monitor XTandem log file for progress determination
	'--------------------------------------------------------------------------------------------
	'Dim WithEvents m_StatFileWatch As FileSystemWatcher
	'Protected m_XtSetupFile As String = "default_input.xml"
	'--------------------------------------------------------------------------------------------
	'End future section
	'--------------------------------------------------------------------------------------------
#End Region

	Public Overrides Function ExtractDataFromResults() As IJobParams.CloseOutType
		Dim result As IJobParams.CloseOutType

		'NOTE: This function isn't called if the analysis manager is not creating synopsis files. Function use is controlled by 
		'		MSMS base class
		'Reference to PeptideHitsResultsProcessor reference must be restored and comments removed from MakeTextFile to do data extraction
		'		in Analysis Manager (code update may also be required)

		result = MakeTextOutputFile()
		If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return result
		End If

		result = ZipMainOutputFile()
		Return result
	End Function

	Public Overrides Function OperateAnalysisTool() As IJobParams.CloseOutType
		Dim CmdStr As String

		CmdRunner = New clsRunDosProgram(m_logger, m_workdir)

		If m_debuglevel > 4 Then
			m_logger.PostEntry("clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter", ILogger.logMsgType.logDebug, True)
		End If

		' verify that program file exists
		Dim progLoc As String = m_mgrParams.GetParam("commonfileandfolderlocations", "xtprogloc")
		If Not File.Exists(progLoc) Then
			m_logger.PostEntry("Cannot find XTandem program file", ILogger.logMsgType.logError, True)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'--------------------------------------------------------------------------------------------
		'Future section to monitor XTandem log file for progress determination
		'--------------------------------------------------------------------------------------------
		''Get the XTandem log file name for a File Watcher to monitor
		'Dim XtLogFileName As String = GetXTLogFileName(Path.Combine(m_WorkDir, m_XtSetupFile))
		'If XtLogFileName = "" Then
		'	m_logger.PostEntry("Error getting XTandem log file name", ILogger.logMsgType.logError, True)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End If

		''Setup and start a File Watcher to monitor the XTandem log file
		'StartFileWatcher(m_workdir, XtLogFileName)
		'--------------------------------------------------------------------------------------------
		'End future section
		'--------------------------------------------------------------------------------------------

		'Set up and execute a program runner to run X!Tandem
		CmdStr = "input.xml"
		If Not CmdRunner.RunProgram(progLoc, CmdStr, "XTandem", True) Then
			m_logger.PostEntry("Error running XTandem" & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'--------------------------------------------------------------------------------------------
		'Future section to monitor XTandem log file for progress determination
		'--------------------------------------------------------------------------------------------
		''Turn off file watcher
		'm_StatFileWatch.EnableRaisingEvents = False
		'--------------------------------------------------------------------------------------------
		'End future section
		'--------------------------------------------------------------------------------------------

		'Zip the output file
		'NOTE: If data extraction is to be performed by analysis manager, comment out this section and let the
		'	ExtractDataFromResults function handle output file zipping.
		Dim ZipResult As IJobParams.CloseOutType = ZipMainOutputFile()
		Return ZipResult

		'NOTE: Restore statement if analysis manager is handling data extraction
		'Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS		


	End Function

	Protected Overrides Function MakeResultsFolder(ByVal AnalysisType As String) As IJobParams.CloseOutType
		MyBase.MakeResultsFolder("XTM")
	End Function

	Protected Overrides Function DeleteTempAnalFiles() As IJobParams.CloseOutType
		'TODO clean up any stray files (.PRO version of FASTA if we use it)
	End Function

	Private Function ZipMainOutputFile() As IJobParams.CloseOutType
		Dim TmpFile As String
		Dim FileList() As String

		'Zip concatenated XML output files (should only be one)
		Dim CmdStr As String
		FileList = Directory.GetFiles(m_workdir, "*_xt.xml")
		For Each TmpFile In FileList
			'Set up a program runner to zip the file
			CmdStr = "-add -fast " & Path.Combine(m_workdir, Path.GetFileNameWithoutExtension(TmpFile)) & ".zip " & TmpFile
			If Not CmdRunner.RunProgram(m_mgrParams.GetParam("commonfileandfolderlocations", "zipprogram"), CmdStr, "Zipper", True) Then
				m_logger.PostEntry("Error zipping output files, job " & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error zipping output files")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Next

		'Delete the XML output files
		Try
			FileList = Directory.GetFiles(m_workdir, "*_xt.xml")
			For Each TmpFile In FileList
				File.SetAttributes(TmpFile, File.GetAttributes(TmpFile) And (Not FileAttributes.ReadOnly))
				File.Delete(TmpFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error deleting _xt.xml file, job " & m_JobNum, Err, LOG_DATABASE)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function MakeTextOutputFile() As IJobParams.CloseOutType
		'make flat file from output file

		'If this disabled function accidentally gets called, return an error.
		'Remove this line if extraction restored to AM
		Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'******************************************************************************************************************
		'	Restore this section if AM needs to do data extraction
		'******************************************************************************************************************

		'find XTandem output file

		'Dim XMLResultsFileName As String
		'Dim TmpFile As String
		'Dim FileList() As String
		'Try
		'	FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml")
		'	If FileList.Length <> 1 Then
		'		m_logger.PostEntry("More than one output file, job " & m_jobnum, ILogger.logMsgType.logError, True)
		'		Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'	End If
		'	XMLResultsFileName = FileList(0)
		'Catch Err As Exception
		'	m_logger.PostError("Error finding output file, job " & m_jobnum, Err, LOG_DATABASE)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End Try

		''' Old code for applying an XSLT program to the _xt.xml file
		''''TODO verify prog file exists
		''''TODO verify xslt file exists
		'''      'run converter program to make flat file
		''''Set up a program runner to run converter
		'''Dim WorkDir As String = m_mgrParams.GetParam("commonfileandfolderlocations", "workdir")
		'''Dim ConvDir As String = m_mgrParams.GetParam("commonfileandfolderlocations", "xtoutconv-dir")
		'''Dim ConvProg As String = m_mgrParams.GetParam("commonfileandfolderlocations", "xtoutconv-prog")
		'''Dim ConvXSLT As String = m_mgrParams.GetParam("commonfileandfolderlocations", "xtoutconv-xslt")
		'''Dim CmdStr As String
		'''Dim CmdRunner As New clsRunDosProgram(m_logger, m_WorkDir)
		'''CmdStr = "/i:" & OutFileName & " /t:" & ConvDir & ConvXSLT
		'''If Not CmdRunner.RunProgram(ConvDir & ConvProg, CmdStr, "XMLtoTextViaXSL", True) Then
		'''	m_logger.PostEntry("Error running XMLtoTextViaXSL" & m_jobnum, ILogger.logMsgType.logError, LOG_DATABASE)
		'''	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'''End If

		''Example values in m_jobParams
		''m_jobParams("jobNum") = "143080"
		''m_jobParams("datasetNum") = "QC_05_2_a_24Oct05_Doc_0508-08"
		''m_jobParams("datasetFolderName") = "QC_05_2_a_24Oct05_Doc_0508-08"
		''m_jobParams("datasetFolderStoragePath") = "\\Proto-7\LTQ_1_DMS3\"
		''m_jobParams("transferFolderPath") = "\\Proto-7\DMS3_Xfer\"
		''m_jobParams("parmFileName") = "xtandem_Rnd1PartTryp_Rnd2DynMetOxNTermAcet"
		''m_jobParams("parmFileStoragePath") = "\\gigasax\dms_parameter_Files\XTandem\"
		''m_jobParams("settingsFileName") = "IonTrapDefSettings.xml"
		''m_jobParams("settingsFileStoragePath") = "\\gigasax\dms_parameter_Files\XTandem\settings\"
		''m_jobParams("organismDBName") = "PCQ_ETJ_2004-01-21.fasta"
		''m_jobParams("organismDBStoragePath") = "\\gigasax\dms_organism_Files\none\fasta\"
		''m_jobParams("instClass") = "Finnigan_Ion_Trap"
		''m_jobParams("comment") = String.Empty
		''m_jobParams("tool") = "XTandem"
		''m_jobParams("priority") = "5"

		'Dim objPeptideHitResultsProcessor As PeptideHitResultsProcessor.IPeptideHitResultsProcessor
		'Dim ModDefsFileName As String

		'Try
		'	'Bump progress up to PROGRESS_PCT_PEPTIDEHIT_START since XTandem is finished
		'	m_statustools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, PROGRESS_PCT_PEPTIDEHIT_START)

		'	' Define the modification definitions file name
		'	ModDefsFileName = clsAnalysisResourcesXT.ConstructModificationDefinitionsFilename(m_jobParams.GetParam("parmFileName"))

		'	'Initialize the plugin
		'	Dim SetupParams As New PeptideHitResultsProcessor.IPeptideHitResultsProcessor.InitializationParams
		'	With SetupParams
		'		.DebugLevel = m_DebugLevel
		'		.Logger = m_logger
		'		.MiscParams = New System.Collections.Specialized.StringDictionary				' Empty, since unused
		'		.OutputFolderPath = m_WorkDir
		'		.SourceFolderPath = m_WorkDir
		'		.PeptideHitResultsFileName = XMLResultsFileName
		'		.MassCorrectionTagsFileName = clsAnalysisResourcesXT.MASS_CORRECTION_TAGS_FILENAME
		'		.ModificationDefinitionsFileName = ModDefsFileName
		'		.AnalysisToolName = m_JobParams.GetParam("tool")
		'		.DatasetName = m_JobParams.GetParam("datasetNum")
		'		.SettingsFileName = m_jobparams.GetParam("settingsFileName")
		'		.ParameterFileName = m_JobParams.GetParam("parmFileName")
		'	End With

		'	objPeptideHitResultsProcessor = New PeptideHitResultsProcessor.clsAnalysisManagerPeptideHitResultsProcessor
		'	objPeptideHitResultsProcessor.Setup(SetupParams)

		'Catch ex As Exception
		'	m_logger.PostError("Error initializing the peptide hit results processor, job " & m_jobnum, ex, LOG_DATABASE)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End Try


		''Start the peptide hit results processor
		'Try
		'	Dim RetVal As PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus
		'	RetVal = objPeptideHitResultsProcessor.Start()
		'	If RetVal = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_ERROR Then
		'		m_logger.PostEntry("Error starting spectra processor: " & objPeptideHitResultsProcessor.ErrMsg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
		'		Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'	End If

		'	'Loop until the results processor finishes
		'	Do While (objPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_RUNNING) OrElse _
		'		 (objPeptideHitResultsProcessor.Status = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessStatus.PH_STARTING)
		'		m_statustools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, ComputePercentComplete(objPeptideHitResultsProcessor))
		'		System.Threading.Thread.Sleep(2000)				'Delay for 2 seconds
		'	Loop
		'Catch ex As Exception
		'	m_logger.PostEntry("Exception while running the peptide hit results processor: " & ex.Message, _
		'	 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End Try

		''Bump progress up to PROGRESS_PCT_PEPTIDEHIT_COMPLETE since the Peptide Hit Results Processor is finished
		'm_statustools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, PROGRESS_PCT_PEPTIDEHIT_COMPLETE)

		''Check for reason peptide hit results processor exited
		'If objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_FAILURE Then
		'	m_logger.PostEntry("Error calling the peptide hit results processor: " & objPeptideHitResultsProcessor.ErrMsg, _
		'	 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'ElseIf objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_ABORTED Then
		'	m_logger.PostEntry("Peptide Hit Results Processing aborted", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End If

		''Delete the Mass Correction Tags file since no longer needed
		'Try
		'	System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, clsAnalysisResourcesXT.MASS_CORRECTION_TAGS_FILENAME))
		'Catch ex As Exception
		'	' Ignore errors here
		'End Try

		''Return results
		'If objPeptideHitResultsProcessor.Results = PeptideHitResultsProcessor.IPeptideHitResultsProcessor.ProcessResults.PH_SUCCESS Then
		'	objPeptideHitResultsProcessor = Nothing
		'	Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		'Else
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End If
		'******************************************************************************************************************
		'	End of section needing restoration
		'******************************************************************************************************************

	End Function

	'******************************************************************************************************************
	'	Restore this section if AM needs to do data extraction
	'******************************************************************************************************************
	'Private Function ComputePercentComplete(ByVal objPeptideHitResultsProcessor As PeptideHitResultsProcessor.IPeptideHitResultsProcessor) As Single
	'	Return PROGRESS_PCT_PEPTIDEHIT_START + objPeptideHitResultsProcessor.PercentComplete * (PROGRESS_PCT_PEPTIDEHIT_COMPLETE - PROGRESS_PCT_PEPTIDEHIT_START)
	'End Function
	'******************************************************************************************************************
	'	End of section needing restoration
	'******************************************************************************************************************

	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

		'Update the status file
		m_StatusTools.UpdateAndWrite(PROGRESS_PCT_XTANDEM_RUNNING)

	End Sub

	'--------------------------------------------------------------------------------------------
	'Future section to monitor XTandem log file for progress determination
	'--------------------------------------------------------------------------------------------
	'	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

		''Watches the XTandem status file and reports changes

		''Setup
		'm_StatFileWatch = New FileSystemWatcher
		'With m_StatFileWatch
		'	.BeginInit()
		'	.Path = DirToWatch
		'	.IncludeSubdirectories = False
		'	.Filter = FileToWatch
		'	.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
		'	.EndInit()
		'End With

		''Start monitoring
		'm_StatFileWatch.EnableRaisingEvents = True

	'	End Sub
	'--------------------------------------------------------------------------------------------
	'End future section
	'--------------------------------------------------------------------------------------------

End Class
