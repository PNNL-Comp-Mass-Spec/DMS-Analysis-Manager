'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 05/23/2009
'
' Last modified 05/23/2009
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerDTAtoDAT
    Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
    'Class for running MSCluster DTAtoDAT analysis
	'*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_TOOL_RUNNING As Single = 5

    Protected WithEvents CmdRunner As clsRunDosProgram
    '--------------------------------------------------------------------------------------------
    'Future section to monitor MSCluster log file for progress determination
    '--------------------------------------------------------------------------------------------
    'Dim WithEvents m_StatFileWatch As FileSystemWatcher
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MSCluster tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim DtaTxtFilename As String
        Dim DATResultFilePath As String

        Dim strQualityScoreThreshold As String
        Dim strPostTranslationalMods As String
        Dim result As IJobParams.CloseOutType

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSCluster")

        'Start the job timer
        m_StartTime = System.DateTime.UtcNow

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDTAtoDAT.RunTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("MSClusterprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error, cannot find MSCluster program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Verify that the "models" folder exists in the MSCluster folder
        Dim ModelsFolderPath As String
        ModelsFolderPath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(progLoc), "Models")
        If Not System.IO.Directory.Exists(ModelsFolderPath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error, the Models folder is missing from the MSCluster folder: " & ModelsFolderPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor MSCluster log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Get the MSCluster log file name for a File Watcher to monitor
        'Dim MSClusterLogFileName As String = GetMSClusterLogFileName(Path.Combine(m_WorkDir, m_MSClusterSetupFile))
        'If MSClusterLogFileName = "" Then
        '	m_logger.PostEntry("Error getting MSCluster log file name", ILogger.logMsgType.logError, True)
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        ''Setup and start a File Watcher to monitor the MSCluster log file
        'StartFileWatcher(m_workdir, MSClusterLogFileName)
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        'Set up and execute a program runner to run MSCluster

        If String.IsNullOrEmpty(m_Dataset) Then
            ' Undefined Dataset; unable to continue
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Dataset name is undefined; unable to continue")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        DtaTxtFilename = System.IO.Path.Combine(m_WorkDir, m_Dataset) & "_dta.txt"

        ' Possibly make these adjustable in the future
        strQualityScoreThreshold = "0.05"
        strPostTranslationalMods = "C+57"

        ' Define the parameter string to be sent to the tool at the command line:
        CmdStr = "--file " & DtaTxtFilename
        CmdStr &= " --PTMs " & strPostTranslationalMods
        CmdStr &= " --sqs " & strQualityScoreThreshold
        CmdStr &= " --convert dat"
        CmdStr &= " --out-dir " & m_WorkDir

        ' Need to make sure CmdStr does not end in a slash (since MSCluster doesn't allow that)
        If CmdStr.EndsWith("\") Then
            CmdStr = CmdStr.TrimEnd("\"c)
        End If

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling MSCluster with command string " & CmdStr)
        End If

        CmdRunner = New clsRunDosProgram(System.IO.Path.GetDirectoryName(progLoc))

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "MSCluster", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running MSCluster" & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor MSCluster log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Turn off file watcher
        'm_StatFileWatch.EnableRaisingEvents = False
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        ' Make sure a .DAT file was created
        DATResultFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(DtaTxtFilename) & ".dat")
        If Not System.IO.File.Exists(DATResultFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSCluster output file not found: " & DATResultFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

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

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_TOOL_RUNNING, 0, "", "", "", False)
        End If

    End Sub

    '--------------------------------------------------------------------------------------------
    'Future section to monitor MSCluster log file for progress determination
    '--------------------------------------------------------------------------------------------
    '	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

    ''Watches the MSCluster status file and reports changes

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
