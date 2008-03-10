'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 02/19/2008
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Logging
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports AnalysisManagerMSMSBase
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerXT
	Inherits clsAnalysisToolRunnerMSMS

	'*********************************************************************************************************
	'Class for running XTandem analysis
	'*********************************************************************************************************

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

#Region "Methods"
	''' <summary>
	''' Runs XTandem tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function OperateAnalysisTool() As IJobParams.CloseOutType

		Dim CmdStr As String

		m_logger.PostEntry("Running XTandem", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

		CmdRunner = New clsRunDosProgram(m_logger, m_WorkDir)

		If m_DebugLevel > 4 Then
			m_logger.PostEntry("clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter", ILogger.logMsgType.logDebug, True)
		End If

		' verify that program file exists
		Dim progLoc As String = m_mgrParams.GetParam("xtprogloc")
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
			m_logger.PostEntry("Error running XTandem" & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
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
		Dim ZipResult As IJobParams.CloseOutType = ZipMainOutputFile()
		Return ZipResult

	End Function

	''' <summary>
	''' Calls base class to make an XTandem results folder
	''' </summary>
	''' <param name="AnalysisType">Analysis type prefix for results folder name</param>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Overrides Function MakeResultsFolder(ByVal AnalysisType As String) As IJobParams.CloseOutType
		MyBase.MakeResultsFolder("XTM")
	End Function

	''' <summary>
	''' Cleans up stray analysis files
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks>Not presently implemented</remarks>
	Protected Overrides Function DeleteTempAnalFiles() As IJobParams.CloseOutType
		'TODO clean up any stray files (.PRO version of FASTA if we use it)
	End Function

	''' <summary>
	''' Zips concatenated XML output file
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function ZipMainOutputFile() As IJobParams.CloseOutType
		Dim TmpFile As String
		Dim FileList() As String
		Dim ZipFileName As String

		Try
			Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
			FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml")
			For Each TmpFile In FileList
				ZipFileName = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(TmpFile)) & ".zip"
				If Not Zipper.MakeZipFile("-fast", ZipFileName, Path.GetFileName(TmpFile)) Then
					Dim Msg As String = "Error zipping output files, job " & m_JobNum
					m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
					m_message = AppendToComment(m_message, "Error zipping output files")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Next
		Catch ex As Exception
			Dim Msg As String = "Exception zipping output files, job " & m_JobNum & ": " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error zipping output files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Delete the XML output files
		Try
			FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml")
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

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
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
#End Region

End Class
