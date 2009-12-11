' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports System.IO
Imports AnalysisManagerBase.clsGlobal

Public MustInherit Class clsAnalysisToolRunnerICRBase
	Inherits clsAnalysisToolRunnerBase

    'ICR-2LS object for use in analysis
    ''Protected m_ICR2LSObj As New clsICR2LSWrapper()

    Public Const ICR2LS_STATE_UNKNOWN As String = "Unknown"
    Public Const ICR2LS_STATE_IDLE As String = "Idle"
    Public Const ICR2LS_STATE_PROCESSING As String = "Processing"
    Public Const ICR2LS_STATE_KILLED As String = "Killed"
    Public Const ICR2LS_STATE_ERROR As String = "Error"
    Public Const ICR2LS_STATE_FINISHED As String = "Finished"
    Public Const ICR2LS_STATE_GENERATING As String = "Generating"
    Public Const ICR2LS_STATE_TICGENERATION As String = "TICGeneration"
    Public Const ICR2LS_STATE_LCQTICGENERATION As String = "LCQTICGeneration"
    Public Const ICR2LS_STATE_QTOFPEKGENERATION As String = "QTOFPEKGeneration"
    Public Const ICR2LS_STATE_MMTOFPEKGENERATION As String = "MMTOFPEKGeneration"
    Public Const ICR2LS_STATE_LTQFTPEKGENERATION As String = "LTQFTPEKGeneration"

    ' ''Enumerated constants
    ''Public Enum ICR_STATUS As Short
    ''    'TODO: This list must be kept current with ICR2LS
    ''    STATE_IDLE = 1
    ''    STATE_PROCESSING = 2
    ''    STATE_KILLED = 3
    ''    STATE_FINISHED = 4
    ''    STATE_GENERATING = 5
    ''    STATE_TICGENERATION = 6
    ''    STATE_LCQTICGENERATION = 7
    ''    STATE_QTOFPEKGENERATION = 8
    ''    STATE_MMTOFPEKGENERATION = 9
    ''    STATE_LTQFTPEKGENERATION = 10
    ''End Enum

    Public Enum ICR2LSProcessingModeConstants
        LTQFTPEK = 0
        LTQFTTIC = 1
        SFoldersPEK = 2
        SFoldersTIC = 3
    End Enum

    Protected Structure udtICR2LSStatusType
        Public StatusDate As DateTime
        Public ScansProcessed As Integer
        Public PercentComplete As Single
        Public ProcessingState As String
        Public ErrorMessage As String

        Public Sub Initialize()
            StatusDate = System.DateTime.Now
            ScansProcessed = 0
            PercentComplete = 0
            ProcessingState = ICR2LS_STATE_UNKNOWN
            ErrorMessage = String.Empty
        End Sub
    End Structure

    'Job running status variable
    Protected m_JobRunning As Boolean
    Protected mStatusFilePath As String = String.Empty
    Protected mMinScanOffset As Integer = 0

    Protected mLastErrorPostingTime As DateTime
    Protected mLastMissingStatusFiletime As DateTime
    Protected mLastInvalidStatusFiletime As DateTime

    Protected mICR2LSStatus As udtICR2LSStatusType

    Protected WithEvents mCmdRunner As AnalysisManagerBase.clsRunDosProgram
    Protected WithEvents mStatusFileWatcher As System.IO.FileSystemWatcher


    Public Sub New()
        ResetStatusLogTimes()

        mICR2LSStatus.Initialize()
    End Sub

    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)
        ''m_ICR2LSObj.DebugLevel = m_DebugLevel

    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        ' Get the settings file info via the base class
        If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Start the job timer
        m_StartTime = System.DateTime.Now

        ResetStatusLogTimes()
        mICR2LSStatus.Initialize()

        ' Remainder of tasks are in subclass (which should call this using MyBase.Runtool)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

    ' Reads the ICR2LS Status file and updates mICR2LSStatus
    Protected Function ParseICR2LSStatusFile(ByVal strStatusFilePath As String, ByVal blnForceParse As Boolean) As Boolean
        Const MINIMUM_PARSING_INTERVAL_SECONDS As Integer = 4
        Static m_LastParseTime As DateTime

        Dim srInFile As System.IO.StreamReader
        Dim strLineIn As String
        Dim intCharIndex As Integer
        Dim strKey As String
        Dim strValue As String

        Dim intResult As Integer
        Dim sngResult As Single
        Dim strProcessingState As String = mICR2LSStatus.ProcessingState

        Dim strStatusDate As String = ""
        Dim strStatusTime As String = ""

        Dim blnSuccess As Boolean

        Try
            blnSuccess = False

            If strStatusFilePath Is Nothing OrElse strStatusFilePath.Length = 0 Then
                Exit Try
            End If

            If Not blnForceParse AndAlso _
               System.DateTime.Now.Subtract(m_LastParseTime).TotalSeconds < MINIMUM_PARSING_INTERVAL_SECONDS Then
                ' Not enough time has elapsed, exit the procedure (returning True)
                blnSuccess = True
                Exit Try
            End If

            m_LastParseTime = System.DateTime.Now

            If System.IO.File.Exists(strStatusFilePath) Then
                ' Read the file
                srInFile = New System.IO.StreamReader(New System.IO.FileStream(strStatusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek() >= 0
                    strLineIn = srInFile.ReadLine()

                    intCharIndex = strLineIn.IndexOf("=")
                    If intCharIndex > 0 Then
                        strKey = strLineIn.Substring(0, intCharIndex).Trim
                        strValue = strLineIn.Substring(intCharIndex + 1).Trim


                        Select Case strKey.ToLower
                            Case "date"
                                strStatusDate = String.Copy(strValue)
                            Case "time"
                                strStatusTime = String.Copy(strValue)
                            Case "scansprocessed"
                                If Integer.TryParse(strValue, intResult) Then
                                    ' When processing a subset of the scans, ICR-2LS reports the "ScansProcessed" starting with the first scan that it is told to process (the /L switch)
                                    ' This can lead to misleading values for ScansProcessed
                                    ' To correct for this, subtract out mMinScanOffset
                                    mICR2LSStatus.ScansProcessed = intResult - mMinScanOffset
                                    If mICR2LSStatus.ScansProcessed < 0 Then
                                        mICR2LSStatus.ScansProcessed = intResult
                                    End If
                                End If

                            Case "percentcomplete"
                                If Single.TryParse(strValue, sngResult) Then
                                    mICR2LSStatus.PercentComplete = sngResult
                                End If

                            Case "processing_state"
                                strProcessingState = String.Copy(strValue)

                            Case "errormessage"
                                mICR2LSStatus.ErrorMessage = String.Copy(strValue)

                            Case Else
                                ' Ignore the line
                        End Select
                    End If
                Loop

                srInFile.Close()

                If strStatusDate.Length > 0 AndAlso strStatusTime.Length > 0 Then
                    strStatusDate &= " " & strStatusTime
                    If Not DateTime.TryParse(strStatusDate, mICR2LSStatus.StatusDate) Then
                        mICR2LSStatus.StatusDate = System.DateTime.Now
                    End If
                End If

                If Not strProcessingState Is Nothing AndAlso strProcessingState.Length > 0 Then
                    mICR2LSStatus.ProcessingState = strProcessingState
                End If

                If Not ValidateICR2LSStatus(strProcessingState) Then
                    If System.DateTime.Now.Subtract(mLastInvalidStatusFiletime).TotalMinutes >= 15 Then
                        mLastInvalidStatusFiletime = System.DateTime.Now
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid processing state reported by ICR2LS: " & strProcessingState)
                    End If
                End If

                m_progress = mICR2LSStatus.PercentComplete

                ' Update the local status file (and post the status to the message queue)
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, mICR2LSStatus.ScansProcessed, "", "", "", False)

                blnSuccess = True
            Else
                ' Status.log file not found; if the job just started, this will be the case
                ' For this reason, ResetStatusLogTimes will set mLastMissingStatusFiletime to the time the job starts, meaning
                '  we won't log an error about a missing Status.log file until 60 minutes into a job
                If System.DateTime.Now.Subtract(mLastMissingStatusFiletime).TotalMinutes >= 60 Then
                    mLastMissingStatusFiletime = System.DateTime.Now
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ICR2LS Status.Log file not found: " & strStatusFilePath)
                End If

                blnSuccess = True
            End If
        Catch ex As Exception
            ' Limit logging of errors to once every 60 minutes

            If System.DateTime.Now.Subtract(mLastErrorPostingTime).TotalMinutes >= 60 Then
                mLastErrorPostingTime = System.DateTime.Now
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error reading the ICR2LS Status.Log file (" & strStatusFilePath & "): " & ex.Message)
            End If
        End Try

        Return blnSuccess

    End Function

    Protected Sub InitializeStatusLogFileWatcher(ByVal strWorkDir As String, ByVal strFilenameToWatch As String)

        mStatusFileWatcher = New System.IO.FileSystemWatcher()
        With mStatusFileWatcher
            .BeginInit()
            .Path = strWorkDir
            .IncludeSubdirectories = False
            .Filter = System.IO.Path.GetFileName(strFilenameToWatch)
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With
    End Sub

    Protected Overridable Function PerfPostAnalysisTasks() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum)
        End If

        'Get rid of raw data file
        result = DeleteDataFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Error deleting raw files; the error will have already been logged
            ' Since the results might still be good, we will not return an error at this point
        End If

        'make results folder
        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
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

    End Function

    Protected Function PossiblyQuotePath(ByVal strPath As String) As String

        If strPath Is Nothing OrElse strPath.Length = 0 Then
            Return ""
        Else

            If strPath.Contains(" ") Then
                If Not strPath.StartsWith("""") Then
                    strPath = """" & strPath
                End If

                If Not strPath.EndsWith("""") Then
                    strPath &= """"
                End If
            End If

            Return strPath
        End If
    End Function

    Private Sub ResetStatusLogTimes()
        ' Initialize the last error posting time to 2 hours before the present
        mLastErrorPostingTime = System.DateTime.Now.Subtract(New System.TimeSpan(2, 0, 0))

        ' Initialize the last MissingStatusFileTime to the time the job starts
        mLastMissingStatusFiletime = System.DateTime.Now

        mLastInvalidStatusFiletime = System.DateTime.Now.Subtract(New System.TimeSpan(2, 0, 0))
    End Sub

    ''' <summary>
    ''' Starts ICR-2LS by running the .Exe at the command line
    ''' </summary>
    ''' <param name="DSNamePath"></param>
    ''' <param name="ParamFilePath"></param>
    ''' <param name="ResultsFileNamePath"></param>
    ''' <param name="eICR2LSMode"></param>
    ''' <returns>True if successfully started; otherwise false</returns>
    ''' <remarks></remarks>
    Protected Function StartICR2LS(ByVal DSNamePath As String, _
                                             ByVal ParamFilePath As String, _
                                             ByVal ResultsFileNamePath As String, _
                                             ByVal eICR2LSMode As ICR2LSProcessingModeConstants) As Boolean
        Return StartICR2LS(DSNamePath, ParamFilePath, ResultsFileNamePath, eICR2LSMode, True, 0, 0)
    End Function

    Protected Function StartICR2LS(ByVal DSNamePath As String, _
                                             ByVal ParamFilePath As String, _
                                             ByVal ResultsFileNamePath As String, _
                                             ByVal eICR2LSMode As ICR2LSProcessingModeConstants, _
                                             ByVal UseAllScans As Boolean, _
                                             ByVal MinScan As Integer, _
                                             ByVal MaxScan As Integer) As Boolean

        Const MONITOR_INTERVAL_SECONDS As Integer = 4

        Dim strExeFilePath As String
        Dim strArguments As String
        Dim blnSuccess As Boolean
        Dim eLogLevel As clsLogTools.LogLevels

        mStatusFilePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(ResultsFileNamePath), "Status.log")

        ' Create a file watcher to monitor the status.log file created by ICR-2L
        ' This file is updated after each scan is processed
        InitializeStatusLogFileWatcher(System.IO.Path.GetDirectoryName(mStatusFilePath), System.IO.Path.GetFileName(mStatusFilePath))

        m_JobRunning = True

        strExeFilePath = m_mgrParams.GetParam("ICR2LSprogloc")

        If strExeFilePath = "" Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Job parameter ICR2LSprogloc is not defined; unable to run ICR-2LS")
            Return False
        ElseIf Not System.IO.File.Exists(strExeFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ICR-2LS path not found: " & strExeFilePath)
            Return False
        End If

        ' Syntax for calling ICR-2LS via the command line:
        ' ICR-2LS.exe /I:InputFilePath /P:ParameterFilePath /O:OutputFilePath /M:[PEK|TIC] /T:[1|2] /F:FirstScan /L:LastScan
        '
        ' /M:PEK means to make a PEK file while /M:TIC means to generate the .TIC file
        ' /T:1 means the input file is a Thermo .Raw file, and /I specifies a file path
        ' /T:2 means the input files are s-files in s-folders (ICR-2LS file format), and thus /I specifies a folder path
        '
        ' /F and /L are optional.  They can be used to limit the range of scan numbers to process
        ' 
        ' See clsAnalysisToolRunnerICR for a description of the expected folder layout when processing S-folders 

        strArguments = " /I:" & PossiblyQuotePath(DSNamePath) & " /P:" & PossiblyQuotePath(ParamFilePath) & " /O:" & PossiblyQuotePath(ResultsFileNamePath)


        Select Case eICR2LSMode
            Case ICR2LSProcessingModeConstants.LTQFTPEK
                strArguments &= " /M:PEK /T:1"
            Case ICR2LSProcessingModeConstants.LTQFTTIC
                strArguments &= " /M:TIC /T:1"
            Case ICR2LSProcessingModeConstants.SFoldersPEK
                strArguments &= " /M:PEK /T:2"
            Case ICR2LSProcessingModeConstants.SFoldersTIC
                strArguments &= " /M:TIC /T:2"
            Case Else
                ' Unknown mode
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unknown mode ICR2LS processing Mode: " & eICR2LSMode.ToString)
                Return False
        End Select


        If UseAllScans Then
            mMinScanOffset = 0
        Else
            mMinScanOffset = MinScan
            strArguments &= " /F:" & MinScan.ToString & " /L:" & MaxScan.ToString
        End If


        ' Initialize the program runner
        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        mCmdRunner.MonitorInterval = MONITOR_INTERVAL_SECONDS

        ' Set up and execute a program runner to run ICR2LS.exe
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strExeFilePath & strArguments)
        End If

        ' Start ICR-2LS.  Note that .Runprogram will not return until after the ICR2LS.exe closes
        ' However, it will raise a Loop Waiting event every MONITOR_INTERVAL_SECONDS seconds
        blnSuccess = mCmdRunner.RunProgram(strExeFilePath, strArguments, "ICR2LS.exe", True)

        ' Pause for another 2 seconds to make sure ICR-2LS closes
        System.Threading.Thread.Sleep(2000)

        ' Make sure the status file is parsed one final time
        ParseICR2LSStatusFile(mStatusFilePath, True)

        If Not mStatusFileWatcher Is Nothing Then
            mStatusFileWatcher.EnableRaisingEvents = False
            mStatusFileWatcher = Nothing
        End If

        'Stop the job timer
        m_StopTime = Now

        If Not blnSuccess Then
            ' ProgRunner returned false, check the Exit Code
            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ICR2LS.exe returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to ICR2LS.exe failed (but exit code is 0)")
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Most recent ICR-2LS State: " & mICR2LSStatus.ProcessingState & " with " & mICR2LSStatus.ScansProcessed & " scans processed (" & mICR2LSStatus.PercentComplete.ToString("0.0") & "% done)")

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS.exe : " & m_JobNum)
        Else

            'Verify ICR-2LS exited due to job completion

            If mICR2LSStatus.ProcessingState <> ICR2LS_STATE_FINISHED Then

                If mICR2LSStatus.ProcessingState = ICR2LS_STATE_ERROR Or _
                   mICR2LSStatus.ProcessingState = ICR2LS_STATE_KILLED Or _
                   m_progress < 100 Then
                    eLogLevel = clsLogTools.LogLevels.ERROR
                Else
                    eLogLevel = clsLogTools.LogLevels.WARN
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogLevel, "ICR-2LS processing state not Finished: " & mICR2LSStatus.ProcessingState & "; Processed " & mICR2LSStatus.ScansProcessed & " scans (" & mICR2LSStatus.PercentComplete.ToString("0.0") & "% complete)")

                If m_progress >= 100 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Progress reported by ICR-2LS is 100%, so will assume the job is complete")
                    blnSuccess = True
                Else
                    blnSuccess = False
                End If

            Else
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Processing state Finished; Processed " & mICR2LSStatus.ScansProcessed & " scans")
                End If
                blnSuccess = True
            End If
        End If

        Return blnSuccess

    End Function

    Protected Function ValidateICR2LSStatus(ByVal strProcessingState As String) As Boolean
        Dim blnValid As Boolean
        blnValid = True

        Select Case strProcessingState.ToLower
            Case ICR2LS_STATE_UNKNOWN.ToLower()
                blnValid = True
            Case ICR2LS_STATE_IDLE.ToLower()
                blnValid = True
            Case ICR2LS_STATE_PROCESSING.ToLower()
                blnValid = True
            Case ICR2LS_STATE_KILLED.ToLower()
                blnValid = True
            Case ICR2LS_STATE_ERROR.ToLower()
                blnValid = True
            Case ICR2LS_STATE_FINISHED.ToLower()
                blnValid = True
            Case ICR2LS_STATE_GENERATING.ToLower()
                blnValid = True
            Case ICR2LS_STATE_TICGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_LCQTICGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_QTOFPEKGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_MMTOFPEKGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_LTQFTPEKGENERATION.ToLower()
                blnValid = True
            Case Else
                blnValid = False
        End Select

        Return blnValid
    End Function

    ''Protected Function WaitForJobToFinish() As Boolean
    ''    Const LOGGER_DETAILED_STATUS_INTERVAL_SECONDS As Integer = 120
    ''    Dim dtLastLogTime As System.DateTime

    ''    ' Waits for ICR2LS job to finish after being started by subclass call
    ''    ' ICR2LS processing status is monitored by reading the Status.Log file, which ICR-2LS updates after processing each scan

    ''    '' Dim StatusResult As ICR_STATUS
    ''    '' Dim Progress As Single

    ''    'Monitor status
    ''    dtLastLogTime = System.DateTime.Now()

    ''    While m_JobRunning
    ''        System.Threading.Thread.Sleep(4000)         'Delay for 4 seconds

    ''        'StatusResult = CType(m_ICR2LSObj.Status, ICR_STATUS)
    ''        ParseICR2LSStatusFile(mStatusFilePath)

    ''        Select Case mICR2LSStatus.ProcessingState
    ''            'TODO: Update this statement when new ICR-2LS states are added
    ''            Case ICR2LS_STATE_UNKNOWN, _
    ''                 ICR2LS_STATE_PROCESSING, _
    ''                 ICR2LS_STATE_GENERATING, _
    ''                 ICR2LS_STATE_TICGENERATION, _
    ''                 ICR2LS_STATE_LCQTICGENERATION, _
    ''                 ICR2LS_STATE_QTOFPEKGENERATION, _
    ''                 ICR2LS_STATE_MMTOFPEKGENERATION, _
    ''                 ICR2LS_STATE_LTQFTPEKGENERATION

    ''                'Report progress
    ''                m_progress = mICR2LSStatus.PercentComplete

    ''                'Update the status report
    ''                m_StatusTools.UpdateAndWrite(m_progress)

    ''                If m_DebugLevel >= 2 Then
    ''                    If System.DateTime.Now.Subtract(dtLastLogTime).TotalSeconds >= LOGGER_DETAILED_STATUS_INTERVAL_SECONDS Then
    ''                        dtLastLogTime = System.DateTime.Now
    ''                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); " & _
    ''                                           "StatusResult=" & mICR2LSStatus.ProcessingState & "; " & _
    ''                                           "Progess=" & m_progress.ToString("0.00") & "; " & _
    ''                                           "ScansProcessed=" & mICR2LSStatus.ScansProcessed.ToString)
    ''                    End If
    ''                End If

    ''            Case Else
    ''                'Analysis is no longer running, exit loop
    ''                m_JobRunning = False
    ''                If m_DebugLevel > 0 Then
    ''                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Ending loop")
    ''                End If
    ''        End Select
    ''    End While

    ''    ' ''Close the ICR-2LS object
    ''    ''If m_DebugLevel > 0 Then
    ''    ''    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Closing ICR2LS object")
    ''    ''End If
    ''    ''m_ICR2LSObj.CloseICR2LS()
    ''    ''m_ICR2LSObj = Nothing
    ''    ' ''Fire off the garbage collector to make sure ICR-2LS dies
    ''    ''GC.Collect()
    ''    ''GC.WaitForPendingFinalizers()

    ''    ''Delay to allow ICR2LS to close everything
    ''    System.Threading.Thread.Sleep(3000)

    ''    'Verify ICR-2LS exited due to job completion
    ''    If mICR2LSStatus.ProcessingState = ICR2LS_STATE_KILLED Or mICR2LSStatus.ProcessingState = ICR2LS_STATE_ERROR Then

    ''    End If

    ''    If mICR2LSStatus.ProcessingState <> ICR2LS_STATE_FINISHED Then
    ''        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Processing state not Finished: " & mICR2LSStatus.ProcessingState)
    ''        If m_progress >= 100 Then
    ''            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Reported progress is 100%, so will assume the job is complete")
    ''            Return True
    ''        Else
    ''            Return False
    ''        End If

    ''    Else
    ''        If m_DebugLevel > 0 Then
    ''            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.WaitForJobToFinish(); Processing state: Finished")
    ''        End If
    ''        Return True
    ''    End If

    ''End Function

    Private Sub CmdRunner_LoopWaiting() Handles mCmdRunner.LoopWaiting
        Const NORMAL_LOG_INTERVAL_MINUTES As Integer = 30
        Const DEBUG_LOG_INTERVAL_MINUTES As Integer = 5

        Static dtLastStatusLogTime As DateTime

        Dim dblMinutesElapsed As Double
        Dim blnLogStatus As Boolean

        dblMinutesElapsed = System.DateTime.Now.Subtract(dtLastStatusLogTime).TotalMinutes
        If m_DebugLevel > 0 Then
            If dblMinutesElapsed >= DEBUG_LOG_INTERVAL_MINUTES AndAlso m_DebugLevel >= 2 Then
                blnLogStatus = True
            ElseIf dblMinutesElapsed >= NORMAL_LOG_INTERVAL_MINUTES Then
                blnLogStatus = True
            End If

            If blnLogStatus Then
                dtLastStatusLogTime = System.DateTime.Now

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.CmdRunner_LoopWaiting(); " & _
                                                           "Processing Time = " & System.DateTime.Now.Subtract(m_StartTime).TotalMinutes.ToString("0.0") & " minutes; " & _
                                                           "Progress = " & m_progress.ToString("0.00") & "; " & _
                                                           "Scans Processed = " & mICR2LSStatus.ScansProcessed.ToString)
            End If
        End If
    End Sub

    Private Sub mStatusFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mStatusFileWatcher.Changed
        ParseICR2LSStatusFile(mStatusFilePath, False)
    End Sub
End Class
