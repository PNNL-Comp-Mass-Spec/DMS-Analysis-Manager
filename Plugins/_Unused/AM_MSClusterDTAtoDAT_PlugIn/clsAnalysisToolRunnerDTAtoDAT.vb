'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 05/23/2009
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports AnalysisManagerBase.StatusReporting
Imports PRISM.Logging

''' <summary>
''' Class for running MSCluster DTAtoDAT analysis
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerDTAtoDAT
    Inherits AnalysisToolRunnerBase

#Region "Module Variables"
    Protected Const PROGRESS_PCT_TOOL_RUNNING As Single = 5

    Protected WithEvents CmdRunner As RunDosProgram
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
    Public Overrides Function RunTool() As CloseOutType

        Dim CmdStr As String
        Dim DtaTxtFilename As String
        Dim DATResultFilePath As String

        Dim strQualityScoreThreshold As String
        Dim strPostTranslationalMods As String
        Dim result As CloseOutType

        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Running MSCluster")

        'Start the job timer
        mStartTime = System.DateTime.UtcNow

        If mDebugLevel > 4 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDTAtoDAT.RunTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = mMgrParams.GetParam("MSClusterprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error, cannot find MSCluster program file: " & progLoc)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Verify that the "models" folder exists in the MSCluster folder
        Dim ModelsFolderPath As String
        ModelsFolderPath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(progLoc), "Models")
        If Not System.IO.Directory.Exists(ModelsFolderPath) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error, the Models folder is missing from the MSCluster folder: " & ModelsFolderPath)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor MSCluster log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Get the MSCluster log file name for a File Watcher to monitor
        'Dim MSClusterLogFileName As String = GetMSClusterLogFileName(Path.Combine(mWorkDir, m_MSClusterSetupFile))
        'If MSClusterLogFileName = "" Then
        '    m_logger.PostEntry("Error getting MSCluster log file name", ILogger.logMsgType.logError, True)
        '    Return CloseOutType.CLOSEOUT_FAILED
        'End If

        ''Setup and start a File Watcher to monitor the MSCluster log file
        'StartFileWatcher(mWorkDir, MSClusterLogFileName)
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        'Set up and execute a program runner to run MSCluster

        If String.IsNullOrEmpty(mDatasetName) Then
            ' Undefined Dataset; unable to continue
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Dataset name is undefined; unable to continue")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        DtaTxtFilename = System.IO.Path.Combine(mWorkDir, mDatasetName) & "_dta.txt"

        ' Possibly make these adjustable in the future
        strQualityScoreThreshold = "0.05"
        strPostTranslationalMods = "C+57"

        ' Define the parameter string to be sent to the tool at the command line:
        CmdStr = "--file " & DtaTxtFilename
        CmdStr &= " --PTMs " & strPostTranslationalMods
        CmdStr &= " --sqs " & strQualityScoreThreshold
        CmdStr &= " --convert dat"
        CmdStr &= " --out-dir " & mWorkDir

        ' Need to make sure CmdStr does not end in a slash (since MSCluster doesn't allow that)
        If CmdStr.EndsWith("\") Then
            CmdStr = CmdStr.TrimEnd("\"c)
        End If

        If mDebugLevel >= 2 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Calling MSCluster with command string " & CmdStr)
        End If

        CmdRunner = New RunDosProgram(System.IO.Path.GetDirectoryName(progLoc))

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "MSCluster", True) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR, "Error running MSCluster" & mJob)
            Return CloseOutType.CLOSEOUT_FAILED
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
        mStopTime = System.DateTime.UtcNow

        ' Make sure a .DAT file was created
        DATResultFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(DtaTxtFilename) & ".dat")
        If Not System.IO.File.Exists(DATResultFilePath) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "MSCluster output file not found: " & DATResultFilePath)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.WARN, "Error creating summary file, job " & mJob & ", step " & mJobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        PRISM.ProgRunner.GarbageCollectNow()

        Dim success as boolean = MakeResultsDirectory()

        If Not success Then
            'TODO: What do we do here?
            Return result
        End If

        success = MoveResultFiles()
        If Not success Then
            'TODO: What do we do here?
            Return result
        End If

        success = CopyResultsFolderToServer()
        If Not success Then
            'TODO: What do we do here?
            Return result
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        LogProgress("DTAtoDAT")

        ' Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING,TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, PROGRESS_PCT_TOOL_RUNNING, 0, "", "", "", False)
        End If

    End Sub

    '--------------------------------------------------------------------------------------------
    'Future section to monitor MSCluster log file for progress determination
    '--------------------------------------------------------------------------------------------
    '    Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

    ''Watches the MSCluster status file and reports changes

    ''Setup
    'm_StatFileWatch = New FileSystemWatcher
    'With m_StatFileWatch
    '    .BeginInit()
    '    .Path = DirToWatch
    '    .IncludeSubdirectories = False
    '    .Filter = FileToWatch
    '    .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
    '    .EndInit()
    'End With

    ''Start monitoring
    'm_StatFileWatch.EnableRaisingEvents = True

    '    End Sub
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

End Class
