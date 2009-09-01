'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 02/19/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerXT
    Inherits clsAnalysisToolRunnerBase

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
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running XTandem")

        'Start the job timer
        m_StartTime = System.DateTime.Now

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("xtprogloc")
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find XTandem program file")
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running XTandem, job " & m_JobNum)
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

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        'Zip the output file
        result = ZipMainOutputFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = AppendToComment(m_message, "Error zipping output files")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Next
		Catch ex As Exception
			Dim Msg As String = "clsAnalysisToolRunnerXT.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ZipMainOutputFile, Error deleting _xt.xml file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_XTANDEM_RUNNING, 0, "", "", "", False)
        End If

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
