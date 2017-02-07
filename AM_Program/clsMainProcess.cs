'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Text.RegularExpressions
Imports System.Threading
Imports AnalysisManagerBase
Imports PRISM

''' <summary>
''' Master processing class for analysis manager
''' </summary>
''' <remarks></remarks>
Public Class clsMainProcess
    Inherits clsLoggerBase

#Region "Constants"
    ' These constants are used to create the Windows Event log (aka the EmergencyLog) that this program rights to
    '  when the manager is disabled or cannot make an entry in the log file
    Private Const CUSTOM_LOG_SOURCE_NAME As String = "Analysis Manager"
    Public Const CUSTOM_LOG_NAME As String = "DMS_AnalysisMgr"
    Private Const MAX_ERROR_COUNT As Integer = 10

    Private Const DECON2LS_FATAL_REMOTING_ERROR As String = "Fatal remoting error"
    Private Const DECON2LS_CORRUPTED_MEMORY_ERROR As String = "Corrupted memory error"
    Private Const DECON2LS_TCP_ALREADY_REGISTERED_ERROR As String = "channel 'tcp' is already registered"
#End Region

#Region "Module variables"
    Private m_MgrSettings As IMgrParams             ' clsAnalysisMgrSettings
    Private m_MgrErrorCleanup As clsCleanupMgrErrors

    Private ReadOnly m_MgrFolderPath As String
    Private m_WorkDirPath As String
    Private m_MgrName As String = "??"

    Private m_AnalysisTask As IJobParams            ' clsAnalysisJob
    Private m_PluginLoader As clsPluginLoader
    Private m_SummaryFile As clsSummaryFile

    Private WithEvents m_FileWatcher As FileSystemWatcher
    Private m_ConfigChanged As Boolean

    Private m_Resource As IAnalysisResources
    Private m_ToolRunner As IToolRunner
    Private m_StatusTools As clsStatusFile
    Private m_MyEMSLUtilities As clsMyEMSLUtilities

    Private m_NeedToAbortProcessing As Boolean
    Private m_MostRecentJobInfo As String

    Private m_MostRecentErrorMessage As String = String.Empty

#End Region

#Region "Properties"
    Public Property DisableMessageQueue As Boolean
    Public Property DisableMyEMSL As Boolean
    Public Property TraceMode As Boolean
#End Region

#Region "Methods"
    ''' <summary>
    ''' Starts program execution
    ''' </summary>
    ''' <returns>0 if no error; error code if an error</returns>
    ''' <remarks></remarks>
    Public Function Main() As Integer

        Try

            If Me.TraceMode Then ShowTraceMessage("Initializing the manager")

            If Not InitMgr() Then
                If Me.TraceMode Then ShowTraceMessage("InitMgr returned false; aborting")
                Return -1
            End If

            If Me.TraceMode Then ShowTraceMessage("Call DoAnalysis")
            DoAnalysis()

            If Me.TraceMode Then ShowTraceMessage("Exiting clsMainProcess.Main with error code = 0")
            Return 0

        Catch ex As Exception
            'Report any exceptions not handled at a lower level to the system application log
            Dim errMsg = "Critical exception starting application: " & ex.Message
            If Me.TraceMode Then ShowTraceMessage(errMsg & "; " & clsGlobal.GetExceptionStackTrace(ex, True))
            PostToEventLog(errMsg & "; " & clsGlobal.GetExceptionStackTrace(ex, False))
            If Me.TraceMode Then ShowTraceMessage("Exiting clsMainProcess.Main with error code = 1")
            Return 1
        End Try

    End Function

    ''' <summary>
    ''' Constructor
    ''' </summary>	
    Public Sub New(blnTraceModeEnabled As Boolean)
        Me.TraceMode = blnTraceModeEnabled
        m_ConfigChanged = False
        m_DebugLevel = 0
        m_NeedToAbortProcessing = False
        m_MostRecentJobInfo = String.Empty

        Dim fiMgr = New FileInfo(Application.ExecutablePath)
        m_MgrFolderPath = fiMgr.DirectoryName

    End Sub

    ''' <summary>
    ''' Initializes the manager settings
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function InitMgr() As Boolean

        ' Create a database logger connected to DMS5
        ' Once the initial parameters have been successfully read, 
        ' we remove this logger than make a new one using the connection string read from the Manager Control DB
        Dim defaultDmsConnectionString = My.Settings.DefaultDMSConnString
        Dim hostName = Net.Dns.GetHostName()

        clsLogTools.CreateDbLogger(defaultDmsConnectionString, "Analysis Tool Manager: " + hostName, True)

        ' Get settings from config file
        Dim lstMgrSettings As Dictionary(Of String, String)

        Try
            If Me.TraceMode Then ShowTraceMessage("Reading application config file")
            lstMgrSettings = LoadMgrSettingsFromFile()

            ' Get the manager settings
            ' If you get an exception here while debugging in Visual Studio, then be sure 
            '   that "UsingDefaults" is set to False in CaptureTaskManager.exe.config               
            Try
                If Me.TraceMode Then ShowTraceMessage("Instantiating clsAnalysisMgrSettings")
                m_MgrSettings = New clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, m_MgrFolderPath, TraceMode)
            Catch ex As Exception
                ' Failures are logged by clsMgrSettings to application event logs;
                '  this includes MgrActive_Local = False
                '  
                ' If the DMS_AnalysisMgr application log does not exist yet, the SysLogger will create it
                ' However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
                ' Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

                Console.WriteLine()
                Console.WriteLine("===============================================================")
                Console.WriteLine("Exception instantiating clsAnalysisMgrSettings: " & ex.Message)
                Console.WriteLine("===============================================================")
                Console.WriteLine()
                Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " & CUSTOM_LOG_NAME & " application log")
                Console.WriteLine()
                Thread.Sleep(500)

                Return False
            End Try

        Catch ex As Exception
            Console.WriteLine()
            Console.WriteLine("===============================================================")
            Console.WriteLine("Exception loading settings from AnalysisManagerProg.exe.config: " & ex.Message)
            Console.WriteLine("===============================================================")
            Console.WriteLine()
            Thread.Sleep(500)
            Return False
        End Try

        m_MgrName = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME)
        If Me.TraceMode Then ShowTraceMessage("Manager name is " & m_MgrName)

        ' Delete any temporary files that may be left in the app directory
        RemoveTempFiles()

        ' Confirm that the application event log exists
        If Not EventLog.SourceExists(CUSTOM_LOG_SOURCE_NAME) Then
            Dim sourceData = New EventSourceCreationData(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME)
            EventLog.CreateEventSource(sourceData)
        End If

        ' Setup the loggers

        Dim logFileNameBase = m_MgrSettings.GetParam("logfilename")

        clsLogTools.CreateFileLogger(logFileNameBase)

        Dim logCnStr = m_MgrSettings.GetParam("connectionstring")

        clsLogTools.RemoveDefaultDbLogger()
        clsLogTools.CreateDbLogger(logCnStr, "Analysis Tool Manager: " + m_MgrName, False)

        ' Make the initial log entry
        If Me.TraceMode Then ShowTraceMessage("Initializing log file " & clsLogTools.CurrentFileAppenderPath)

        Dim startupMsg As String = "=== Started Analysis Manager V" & Application.ProductVersion & " ===== "
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, startupMsg)

        Dim configFileName = m_MgrSettings.GetParam("configfilename")
        If (String.IsNullOrEmpty(configFileName)) Then
            '  Manager parameter error; log an error and exit
            LogError("Manager parameter 'configfilename' is undefined; this likely indicates a problem retrieving manager parameters.  Shutting down the manager")
            Return False
        End If

        ' Setup a file watcher for the config file
        m_FileWatcher = New FileSystemWatcher With {
            .Path = m_MgrFolderPath,
            .IncludeSubdirectories = False,
            .Filter = configFileName,
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size,
            .EnableRaisingEvents = True
        }

        ' Get the debug level
        m_DebugLevel = CShort(m_MgrSettings.GetParam("debuglevel", 2))

        ' Make sure that the manager name matches the machine name (with a few exceptions)
        If Not hostName.ToLower().StartsWith("emslmq") AndAlso
           Not hostName.ToLower().StartsWith("emslpub") AndAlso
           Not hostName.ToLower().StartsWith("monroe") Then

            If Not m_MgrName.ToLower.StartsWith(hostName.ToLower()) Then
                LogError("Manager name does not match the host name: " & m_MgrName & " vs. " & hostName & "; update AnalysisManagerProg.exe.config")
                Return False
            End If
        End If

        ' Setup the tool for getting tasks
        If Me.TraceMode Then ShowTraceMessage("Instantiate m_AnalysisTask as new clsAnalysisJob")
        m_AnalysisTask = New clsAnalysisJob(m_MgrSettings, m_DebugLevel)

        m_WorkDirPath = m_MgrSettings.GetParam("workdir")

        ' Setup the manager cleanup class
        If Me.TraceMode Then ShowTraceMessage("Setup the manager cleanup class")
        m_MgrErrorCleanup = New clsCleanupMgrErrors(
           m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING),
           m_MgrName,
           m_DebugLevel,
           m_MgrFolderPath,
           m_WorkDirPath)

        If Me.TraceMode Then ShowTraceMessage("Initialize the Summary file")
        m_SummaryFile = New clsSummaryFile()
        m_SummaryFile.Clear()

        If Me.TraceMode Then ShowTraceMessage("Initialize the Plugin Loader")
        m_PluginLoader = New clsPluginLoader(m_SummaryFile, m_MgrFolderPath)

        'Everything worked
        Return True

    End Function

    ''' <summary>
    ''' Loop to perform all analysis jobs
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub DoAnalysis()
        If Me.TraceMode Then ShowTraceMessage("Entering clsMainProcess.DoAnalysis")

        Dim LoopCount = 0
        Dim MaxLoopCount As Integer
        Dim TasksStartedCount = 0
        Dim blnErrorDeletingFilesFlagFile As Boolean

        Dim dtLastConfigDBUpdate As DateTime = Date.UtcNow

        Dim blnRequestJobs As Boolean
        Dim blnOneTaskStarted As Boolean
        Dim blnOneTaskPerformed As Boolean

        ' Used to track critical manager errors (not necessarily failed analysis jobs when the plugin reports "no results" or similar)
        Dim intCriticalMgrErrorCount = 0
        Dim intSuccessiveDeadLockCount = 0

        Try
            If Me.TraceMode Then ShowTraceMessage("Entering clsMainProcess.DoAnalysis Try/Catch block")

            MaxLoopCount = CInt(m_MgrSettings.GetParam("maxrepetitions"))
            blnRequestJobs = True
            blnOneTaskStarted = False
            blnOneTaskPerformed = False

            InitStatusTools()

            While (LoopCount < MaxLoopCount) And blnRequestJobs

                UpdateStatusIdle("No analysis jobs found")

                ' Check for configuration change
                ' This variable will be true if the CaptureTaskManager.exe.config file has been updated
                If m_ConfigChanged Then
                    ' Local config file has changed
                    m_ConfigChanged = False

                    If Me.TraceMode Then ShowTraceMessage("Reloading manager settings since config file has changed")

                    If Not ReloadManagerSettings() Then
                        Exit Sub
                    End If
                    m_FileWatcher.EnableRaisingEvents = True
                Else

                    ' Reload the manager control DB settings in case they have changed
                    ' However, only reload every 2 minutes
                    If Not UpdateManagerSettings(dtLastConfigDBUpdate, 2) Then
                        ' Error retrieving settings from the manager control DB
                        Exit Sub
                    End If

                End If

                ' Check to see if manager is still active
                Dim MgrActive As Boolean = m_MgrSettings.GetParam("mgractive", False)
                Dim MgrActiveLocal As Boolean = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, False)
                Dim strManagerDisableReason As String
                If Not (MgrActive And MgrActiveLocal) Then
                    If Not MgrActiveLocal Then
                        strManagerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config"
                        UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, strManagerDisableReason)
                    Else
                        strManagerDisableReason = "Disabled in Manager Control DB"
                        UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_MC, strManagerDisableReason)
                    End If

                    LogMessage("Manager inactive: " & strManagerDisableReason)
                    Thread.Sleep(750)
                    Exit Sub
                End If

                Dim MgrUpdateRequired As Boolean = m_MgrSettings.GetParam("ManagerUpdateRequired", False)
                If MgrUpdateRequired Then
                    Dim msg = "Manager update is required"
                    LogMessage(msg)
                    m_MgrSettings.AckManagerUpdateRequired()
                    UpdateStatusIdle("Manager update is required")
                    Exit Sub
                End If

                If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
                    ' Delete the Error Deleting status flag file first, so next time through this step is skipped
                    m_MgrErrorCleanup.DeleteErrorDeletingFilesFlagFile()

                    ' There was a problem deleting non result files with the last job.  Attempt to delete files again
                    If Not m_MgrErrorCleanup.CleanWorkDir() Then
                        If blnOneTaskStarted Then
                            LogError("Error cleaning working directory, job " & m_AnalysisTask.GetParam("StepParameters", "Job") & "; see folder " & m_WorkDirPath)
                            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
                        Else
                            LogError("Error cleaning working directory; see folder " & m_WorkDirPath)
                        End If
                        m_MgrErrorCleanup.CreateStatusFlagFile()
                        UpdateStatusFlagFileExists()
                        Exit Sub
                    End If
                    ' Successful delete of files in working directory, so delete the status flag file
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
                End If

                ' Verify that the working directory exists
                If Not VerifyWorkDir() Then
                    Exit Sub
                End If

                ' Verify that an error hasn't left the the system in an odd state
                If StatusFlagFileError() Then
                    LogError("Flag file exists - unable to perform any further analysis jobs")
                    UpdateStatusFlagFileExists()
                    Thread.Sleep(1500)
                    Exit Sub
                End If

                ' Check to see if an excessive number of errors have occurred
                If intCriticalMgrErrorCount > MAX_ERROR_COUNT Then
                    LogError("Excessive task failures; disabling manager via flag file")

                    ' Note: We previously called DisableManagerLocally() to update AnalysisManager.config.exe
                    ' We now create a flag file instead
                    ' This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

                    m_MgrErrorCleanup.CreateStatusFlagFile()
                    UpdateStatusFlagFileExists()

                    Exit While
                End If

                ' Verify working directory properly specified and empty
                If Not ValidateWorkingDir() Then
                    If blnOneTaskStarted Then
                        ' Working directory problem due to the most recently processed job
                        ' Create ErrorDeletingFiles file and exit the program
                        LogError("Working directory problem, creating " & clsCleanupMgrErrors.ERROR_DELETING_FILES_FILENAME & "; see folder " & m_WorkDirPath)
                        m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
                        UpdateStatusIdle("Working directory not empty")
                    Else
                        ' Working directory problem, so create flag file and exit
                        LogError("Working directory problem, disabling manager via flag file; see folder " & m_WorkDirPath)
                        m_MgrErrorCleanup.CreateStatusFlagFile()
                        UpdateStatusFlagFileExists()
                    End If
                    Exit While
                End If

                ' Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                ' Do not request a job between 12 am and 6 am on Thursday in the week with the second Tuesday of the month
                ' Do not request a job between 2 am and 4 am or between 9 am and 11 am on Sunday in the week with the second Tuesday of the month
                Dim pendingWindowsUpdateMessage As String = String.Empty
                If clsWindowsUpdateStatus.UpdatesArePending(pendingWindowsUpdateMessage) Then
                    LogMessage(pendingWindowsUpdateMessage)
                    UpdateStatusIdle(pendingWindowsUpdateMessage)
                    Exit While
                End If

                If Me.TraceMode Then ShowTraceMessage("Requesting a new task from DMS_Pipeline")

                ' Re-initialize these utilies for each analysis job
                m_MyEMSLUtilities = New clsMyEMSLUtilities(m_DebugLevel, m_WorkDirPath)

                ' Get an analysis job, if any are available

                Dim TaskReturn = m_AnalysisTask.RequestTask()

                Select Case TaskReturn
                    Case clsDBTask.RequestTaskResult.NoTaskFound
                        If Me.TraceMode Then ShowTraceMessage("No tasks found")

                        'No tasks found
                        If m_DebugLevel >= 3 Then
                            LogMessage("No analysis jobs found")
                        End If
                        blnRequestJobs = False
                        intCriticalMgrErrorCount = 0
                        UpdateStatusIdle("No analysis jobs found")

                    Case clsDBTask.RequestTaskResult.ResultError
                        If Me.TraceMode Then ShowTraceMessage("Error requesting a task")

                        'There was a problem getting the task; errors were logged by RequestTaskResult
                        intCriticalMgrErrorCount += 1

                    Case clsDBTask.RequestTaskResult.TaskFound

                        If Me.TraceMode Then ShowTraceMessage("Task found")

                        blnRequestJobs = True
                        TasksStartedCount += 1
                        intSuccessiveDeadLockCount = 0

                        Try
                            blnOneTaskStarted = True
                            If DoAnalysisJob() Then
                                ' Task succeeded; reset the sequential job failure counter
                                intCriticalMgrErrorCount = 0
                                blnOneTaskPerformed = True
                            Else
                                'Something went wrong; errors were logged by DoAnalysisJob
                                If m_MostRecentErrorMessage.Contains("None of the spectra are centroided") OrElse
                                   m_MostRecentErrorMessage.Contains("No peaks found") OrElse
                                   m_MostRecentErrorMessage.Contains("No spectra were exported") Then
                                    ' Job failed, but this was not a manager error
                                    ' Do not increment the error count
                                Else
                                    intCriticalMgrErrorCount += 1
                                End If
                            End If

                        Catch ex As Exception
                            ' Something went wrong; errors likely were not logged by DoAnalysisJob

                            LogError("clsMainProcess.DoAnalysis(), Exception thrown by DoAnalysisJob, " & ex.Message, ex)
                            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

                            ' Set the job state to failed
                            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception thrown by DoAnalysisJob")

                            intCriticalMgrErrorCount += 1
                            m_NeedToAbortProcessing = True

                        End Try

                    Case clsDBTask.RequestTaskResult.TooManyRetries
                        If Me.TraceMode Then ShowTraceMessage("Too many retries calling the stored procedure")

                        'There were too many retries calling the stored procedure; errors were logged by RequestTaskResult
                        ' Bump up LoopCount to the maximum to exit the loop
                        UpdateStatusIdle("Excessive retries requesting task")
                        LoopCount = MaxLoopCount

                    Case clsDBTask.RequestTaskResult.Deadlock

                        If Me.TraceMode Then ShowTraceMessage("Deadlock")

                        ' A deadlock error occured
                        ' Query the DB again, but only if we have not had 3 deadlock results in a row
                        intSuccessiveDeadLockCount += 1
                        If intSuccessiveDeadLockCount >= 3 Then
                            Dim msg = "Deadlock encountered " & intSuccessiveDeadLockCount.ToString() & " times in a row when requesting a new task; exiting"
                            LogWarning(msg)
                            blnRequestJobs = False
                        End If

                    Case Else
                        'Shouldn't ever get here
                        LogError("clsMainProcess.DoAnalysis; Invalid request result: " & CInt(TaskReturn).ToString())
                        Exit Sub
                End Select

                If NeedToAbortProcessing() Then
                    If Me.TraceMode Then ShowTraceMessage("Need to abort processing")
                    Exit While
                End If
                LoopCount += 1

                ' If the only problem was deleting non result files, we want to stop the manager
                If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
                    If Me.TraceMode Then ShowTraceMessage("Error deleting files flag file")
                    blnErrorDeletingFilesFlagFile = True
                    LoopCount = MaxLoopCount
                End If

            End While

            If LoopCount >= MaxLoopCount Then
                If blnErrorDeletingFilesFlagFile Then
                    If TasksStartedCount > 0 Then
                        LogWarning("Error deleting file with an open file handle; closing manager. Jobs processed: " & TasksStartedCount.ToString())
                        Thread.Sleep(1500)
                    End If
                Else
                    If TasksStartedCount > 0 Then
                        Dim msg = "Maximum number of jobs to analyze has been reached: " & TasksStartedCount.ToString() & " job"
                        If TasksStartedCount <> 1 Then msg &= "s"
                        msg &= "; closing manager"
                        LogMessage(msg)
                    End If
                End If
            End If

            If blnOneTaskPerformed Then
                LogMessage("Analysis complete for all available jobs")
            End If

            If Me.TraceMode Then ShowTraceMessage("Closing the manager")
            UpdateClose("Closing manager.")
        Catch ex As Exception
            LogError("clsMainProcess.DoAnalysis(), Error encountered, " & ex.Message, ex)
            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

        Finally
            If Not m_StatusTools Is Nothing Then
                If Me.TraceMode Then ShowTraceMessage("Disposing message queue via m_StatusTools.DisposeMessageQueue")
                m_StatusTools.DisposeMessageQueue()
            End If
        End Try

    End Sub

    Private Function DoAnalysisJob() As Boolean

        Dim eToolRunnerResult As IJobParams.CloseOutType
        Dim jobNum As Integer = m_AnalysisTask.GetJobParameter("StepParameters", "Job", 0)
        Dim stepNum As Integer = m_AnalysisTask.GetJobParameter("StepParameters", "Step", 0)
        Dim cpuLoadExpected As Integer = m_AnalysisTask.GetJobParameter("StepParameters", "CPU_Load", 1)

        Dim datasetName As String = m_AnalysisTask.GetParam("JobParameters", "DatasetNum")
        Dim jobToolDescription As String = m_AnalysisTask.GetCurrentJobToolDescription()

        Dim blnRunToolError = False

        If Me.TraceMode Then ShowTraceMessage("Processing job " & jobNum & ", " & jobToolDescription)

        'Initialize summary and status files
        m_SummaryFile.Clear()

        If m_StatusTools Is Nothing Then
            InitStatusTools()
        End If

        ' Update the cached most recent job info
        m_MostRecentJobInfo = ConstructMostRecentJobInfoText(Date.Now.ToString(), jobNum, datasetName, jobToolDescription)

        With m_StatusTools
            .TaskStartTime = Date.UtcNow
            .Dataset = datasetName
            .JobNumber = jobNum
            .JobStep = stepNum
            .Tool = jobToolDescription
            .MgrName = m_MgrName
            .ProgRunnerProcessID = 0
            .ProgRunnerCoreUsage = cpuLoadExpected
            .UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RETRIEVING_RESOURCES, 0, 0, "", "", m_MostRecentJobInfo, True)
        End With

        Dim processID = Process.GetCurrentProcess().Id

        ' Note: The format of the following text is important; be careful about changing it
        ' In particular, function DetermineRecentErrorMessages in clsMainProcess looks for log entries
        '   matching RegEx: "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool ([^,]+)"

        ' Example log entries
        ' 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
        ' 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,

        LogMessage(m_MgrName & ": Started analysis job " & jobNum & ", Dataset " & datasetName &
                     ", Tool " & jobToolDescription & ", Process ID " & processID)

        If m_DebugLevel >= 2 Then
            ' Log the debug level value whenever the debug level is 2 or higher
            LogMessage("Debug level is " & m_DebugLevel.ToString)
        End If

        ' Create an object to manage the job resources
        If Not SetResourceObject() Then
            LogError(m_MgrName & ": Unable to set the Resource object, job " & jobNum & ", Dataset " & datasetName, True)
            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object")
            m_MgrErrorCleanup.CleanWorkDir()
            UpdateStatusIdle("Error encountered: Unable to set resource object")
            Return False
        End If

        ' Create an object to run the analysis tool
        If Not SetToolRunnerObject() Then
            LogError(m_MgrName & ": Unable to set the toolRunner object, job " & jobNum & ", Dataset " & datasetName, True)
            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set tool runner object")
            m_MgrErrorCleanup.CleanWorkDir()
            UpdateStatusIdle("Error encountered: Unable to set tool runner object")
            Return False
        End If

        If NeedToAbortProcessing() Then
            If Me.TraceMode Then ShowTraceMessage("NeedToAbortProcessing; closing job step task")
            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Processing aborted")
            m_MgrErrorCleanup.CleanWorkDir()
            UpdateStatusIdle("Processing aborted")
            Return False
        End If

        ' Make sure we have enough free space on the drive with the working directory and on the drive with the transfer folder
        If Not ValidateFreeDiskSpace(m_MostRecentErrorMessage) Then
            If Me.TraceMode Then ShowTraceMessage("Insufficient free space; closing job step task")
            If String.IsNullOrEmpty(m_MostRecentErrorMessage) Then
                m_MostRecentErrorMessage = "Insufficient free space (location undefined)"
            End If
            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage)
            m_MgrErrorCleanup.CleanWorkDir()
            UpdateStatusIdle("Processing aborted")
            Return False
        End If

        ' Possibly disable MyEMSL
        If DisableMyEMSL Then
            m_Resource.SetOption(clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled, True)
        End If

        ' Retrieve files required for the job
        m_MgrErrorCleanup.CreateStatusFlagFile()
        Try
            If Me.TraceMode Then ShowTraceMessage("Getting job resources")

            eToolRunnerResult = m_Resource.GetResources()
            If Not eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                m_MostRecentErrorMessage = "GetResources returned result: " & eToolRunnerResult.ToString
                If Me.TraceMode Then ShowTraceMessage(m_MostRecentErrorMessage & "; closing job step task")
                If Not m_Resource.Message Is Nothing Then
                    m_MostRecentErrorMessage &= "; " & m_Resource.Message
                End If

                LogError(m_MgrName & ": " & m_MostRecentErrorMessage & ", Job " & jobNum & ", Dataset " & datasetName)
                m_AnalysisTask.CloseTask(eToolRunnerResult, m_Resource.Message)

                m_MgrErrorCleanup.CleanWorkDir()
                UpdateStatusIdle("Error encountered: " & m_MostRecentErrorMessage)
                m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
                Return False
            End If
        Catch ex As Exception
            LogError("clsMainProcess.DoAnalysisJob(), Getting resources, " & ex.Message, ex)

            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception getting resources")

            If m_MgrErrorCleanup.CleanWorkDir() Then
                m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
            Else
                m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
            End If

            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
            Return False
        End Try

        ' Run the job
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 0)
        Try
            If Me.TraceMode Then ShowTraceMessage("Running the step tool")

            eToolRunnerResult = m_ToolRunner.RunTool()
            If eToolRunnerResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                m_MostRecentErrorMessage = m_ToolRunner.Message

                If String.IsNullOrEmpty(m_MostRecentErrorMessage) Then
                    m_MostRecentErrorMessage = "Unknown ToolRunner Error"
                End If

                If Me.TraceMode Then ShowTraceMessage("Error running the tool; closing job step task")

                LogError(m_MgrName & ": " & m_MostRecentErrorMessage & ", Job " & jobNum & ", Dataset " & datasetName)
                m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)

                Try
                    If m_MostRecentErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) OrElse
                       m_MostRecentErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR) Then
                        m_NeedToAbortProcessing = True
                    End If

                Catch ex As Exception
                    LogError("clsMainProcess.DoAnalysisJob(), Exception examining MostRecentErrorMessage", ex)
                End Try

                If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE Then
                    m_NeedToAbortProcessing = True
                End If

                If m_NeedToAbortProcessing AndAlso m_MostRecentErrorMessage.StartsWith(clsAnalysisToolRunnerBase.PVM_RESET_ERROR_MESSAGE) Then
                    DisableManagerLocally()
                End If

                blnRunToolError = True
            End If

            If m_ToolRunner.NeedToAbortProcessing Then
                m_NeedToAbortProcessing = True
                If Me.TraceMode Then ShowTraceMessage("ToolRunner.NeedToAbortProcessing = True; closing job step task")
                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)
            End If

        Catch ex As Exception
            LogError("clsMainProcess.DoAnalysisJob(), running tool, " & ex.Message, ex)

            If ex.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR) Then
                m_NeedToAbortProcessing = True
            End If

            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception running tool", m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)

            blnRunToolError = True
        End Try

        If blnRunToolError Then
            ' Note: the above code should have already called m_AnalysisTask.CloseTask()

            If Me.TraceMode Then ShowTraceMessage("Tool run error; cleaning up")

            Try
                If m_MgrErrorCleanup.CleanWorkDir() Then
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
                Else
                    m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
                End If

                If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES AndAlso
                   m_AnalysisTask.GetParam("StepTool").ToLower() = "sequest" Then
                    ' This was a Sequest job, but no .DTA files were found
                    ' Return True; do not count this as a manager failure
                    Return True
                ElseIf eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                    ' Return True; do not count this as a manager failure
                    Return True
                Else
                    Return False
                End If

            Catch ex As Exception
                LogError("clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," & ex.Message, ex)
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
                Return False
            End Try

        End If

        ' Close out the job
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.CLOSING, IStatusFile.EnumTaskStatusDetail.CLOSING, 100)
        Try
            If Me.TraceMode Then ShowTraceMessage("Task completed successfully; closing the job step task")

            'Close out the job as a success
            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_SUCCESS, String.Empty, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)
            LogMessage(m_MgrName & ": Completed job " & jobNum)

            UpdateStatusIdle("Completed job " & jobNum & ", step " & stepNum)

        Catch ex As Exception
            LogError("clsMainProcess.DoAnalysisJob(), Close task after normal run," & ex.Message, ex)
            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
            Return False
        End Try

        Try
            ' If success was reported check to see if there was an error deleting non result files
            If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
                'If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next start up
                ' However, wait another 5 seconds before continuing
                Processes.clsProgRunner.GarbageCollectNow()
                Thread.Sleep(5000)

                Return True
            Else
                ' Clean the working directory
                Try
                    If Not m_MgrErrorCleanup.CleanWorkDir(1) Then
                        LogError("Error cleaning working directory, job " & m_AnalysisTask.GetParam("StepParameters", "Job"))
                        m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
                        m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
                        Return False
                    End If
                Catch ex As Exception
                    LogError("clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," & ex.Message, ex)
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
                    Return False
                End Try

                'Delete the status flag file
                m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)

                ' Note that we do not need to call m_StatusTools.UpdateIdle() here since 
                ' we called UpdateStatusIdle() just after m_AnalysisTask.CloseTask above

                Return True
            End If

        Catch ex As Exception
            LogError("clsMainProcess.DoAnalysisJob(), " & ex.Message, ex)
            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Constructs a description of the given job using the job number, step tool name, and dataset name
    ''' </summary>
    ''' <param name="JobStartTimeStamp">Time job started</param>
    ''' <param name="Job">Job name</param>
    ''' <param name="Dataset">Dataset name</param>
    ''' <param name="ToolName">Tool name (or step tool name)</param>
    ''' <returns>Info string, similar to: Job 375797; DataExtractor (XTandem), Step 4; QC_Shew_09_01_b_pt5_25Mar09_Griffin_09-02-03; 3/26/2009 3:17:57 AM</returns>
    ''' <remarks></remarks>
    Private Function ConstructMostRecentJobInfoText(JobStartTimeStamp As String, Job As Integer, Dataset As String, ToolName As String) As String

        Try
            If JobStartTimeStamp Is Nothing Then JobStartTimeStamp = String.Empty
            If ToolName Is Nothing Then ToolName = "??"
            If Dataset Is Nothing Then Dataset = "??"

            Return "Job " & Job.ToString & "; " & ToolName & "; " & Dataset & "; " & JobStartTimeStamp
        Catch ex As Exception
            ' Error combining the terms; return an empty string
            Return String.Empty
        End Try

    End Function

    Public Shared Sub CreateAnalysisManagerEventLog()
        Dim blnSuccess As Boolean
        blnSuccess = CreateAnalysisManagerEventLog(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME)

        If blnSuccess Then
            Console.WriteLine()
            Console.WriteLine("Windows Event Log '" & CUSTOM_LOG_NAME & "' has been validated for source '" & CUSTOM_LOG_SOURCE_NAME & "'")
            Console.WriteLine()
        End If
    End Sub

    Private Shared Function CreateAnalysisManagerEventLog(SourceName As String, LogName As String) As Boolean

        Try
            If String.IsNullOrEmpty(SourceName) Then
                Console.WriteLine("Error creating the Windows Event Log: SourceName cannot be blank")
                Return False
            End If

            If String.IsNullOrEmpty(LogName) Then
                Console.WriteLine("Error creating the Windows Event Log: LogName cannot be blank")
                Return False
            End If

            If Not EventLog.SourceExists(SourceName) Then
                Console.WriteLine("Creating Windows Event Log " & LogName & " for source " & SourceName)
                Dim SourceData = New EventSourceCreationData(SourceName, LogName)
                EventLog.CreateEventSource(SourceData)
            End If

            ' Create custom event logging object and update it's configuration
            Dim ELog As New EventLog
            ELog.Log = LogName
            ELog.Source = SourceName

            Try
                ELog.MaximumKilobytes = 1024
            Catch ex As Exception
                Console.WriteLine("Warning: unable to update the maximum log size to 1024 KB: " & ControlChars.NewLine & "  " & ex.Message)
            End Try

            Try
                ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90)
            Catch ex As Exception
                Console.WriteLine("Warning: unable to update the overflow policy to keep events for 90 days and overwrite as needed: " & ControlChars.NewLine & "  " & ex.Message)
            End Try

        Catch ex As Exception
            Console.WriteLine("Exception creating the Windows Event Log named '" & LogName & "' for source '" & SourceName & "': " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Private Function DataPackageIdMissing() As Boolean

        Dim stepToolName = m_AnalysisTask.GetParam("JobParameters", "StepTool")

        Dim multiJobStepTools = New SortedSet(Of String) From {
                    "APE",
                    "AScore",
                    "Cyclops",
                    "IDM",
                    "Mage",
                    "MultiAlign_Aggregator",
                    "mzXML_Aggregator",
                    "Phospho_FDR_Aggregator",
                    "PRIDE_Converter",
                    "RepoPkgr"}

        Dim dataPkgRequired As Boolean
        If multiJobStepTools.Any(Function(multiJobTool) String.Equals(stepToolName, multiJobTool, StringComparison.InvariantCultureIgnoreCase)) Then
            dataPkgRequired = True
        End If

        If dataPkgRequired Then
            Dim dataPkgID = m_AnalysisTask.GetJobParameter("JobParameters", "DataPackageID", 0)
            If dataPkgID <= 0 Then
                ' The data package ID is 0 or missing
                Return True
            End If
        End If

        Return False

    End Function

    ''' <summary>
    ''' Given a log file with a name like AnalysisMgr_03-25-2009.txt, returns the log file name for the previous day
    ''' </summary>
    ''' <param name="strLogFilePath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function DecrementLogFilePath(strLogFilePath As String) As String

        Dim reLogFileName As Regex
        Dim objMatch As Match

        Dim intYear As Integer
        Dim intMonth As Integer
        Dim intDay As Integer
        Dim strPreviousLogFilePath As String = String.Empty

        Try
            reLogFileName = New Regex("(.+_)(\d+)-(\d+)-(\d+).\S+", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

            objMatch = reLogFileName.Match(strLogFilePath)

            If objMatch.Success AndAlso objMatch.Groups.Count >= 4 Then
                intMonth = CInt(objMatch.Groups(2).Value)
                intDay = CInt(objMatch.Groups(3).Value)
                intYear = CInt(objMatch.Groups(4).Value)

                Dim dtCurrentDate As DateTime
                Dim dtNewDate As DateTime

                dtCurrentDate = DateTime.Parse(intYear & "-" & intMonth & "-" & intDay)
                dtNewDate = dtCurrentDate.Subtract(New TimeSpan(1, 0, 0, 0))

                strPreviousLogFilePath = objMatch.Groups(1).Value & dtNewDate.ToString("MM-dd-yyyy") & Path.GetExtension(strLogFilePath)
            End If

        Catch ex As Exception
            Console.WriteLine("Error in DecrementLogFilePath: " & ex.Message)
        End Try

        Return strPreviousLogFilePath

    End Function

    ''' <summary>
    ''' Parses the log files for this manager to determine the recent error messages, returning up to intErrorMessageCountToReturn of them
    ''' Will use objLogger to determine the most recent log file
    ''' Also examines the message info stored in objLogger
    ''' Lastly, if strMostRecentJobInfo is empty, then will update it with info on the most recent job started
    ''' </summary>
    ''' <param name="intErrorMessageCountToReturn">Maximum number of error messages to return</param>
    ''' <param name="strMostRecentJobInfo">Info on the most recent job started by this manager</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function DetermineRecentErrorMessages(intErrorMessageCountToReturn As Integer, ByRef strMostRecentJobInfo As String) As String()

        ' This regex will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
        Const ERROR_MATCH_REGEX = "^([^,]+),(.+), Error, *$"

        ' This regex looks for information on a job starting
        ' Note: do not try to match "Step \d+" with this regex due to variations on how the log message appears
        Const JOB_START_REGEX = "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool ([^,]+)"

        ' Examples matching log entries
        ' 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
        ' 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,

        ' The following effectively defines the number of days in the past to search when finding recent errors
        Const MAX_LOG_FILES_TO_SEARCH = 5

        Dim blnLoggerReportsError As Boolean
        Dim strLogFilePath As String
        Dim intLogFileCountProcessed As Integer

        Dim srInFile As StreamReader

        Dim reErrorLine As Regex
        Dim reJobStartLine As Regex

        Dim objMatch As Match

        Dim qErrorMsgQueue As Queue
        Dim htUniqueErrorMessages As Hashtable

        ' Note that strRecentErrorMessages() and dtRecentErrorMessageDates() are parallel arrays
        Dim intRecentErrorMessageCount As Integer
        Dim strRecentErrorMessages = New String() {}
        Dim dtRecentErrorMessageDates() As DateTime

        Dim strLineIn As String

        Dim blnCheckForMostRecentJob As Boolean
        Dim strMostRecentJobInfoFromLogs As String

        Dim strTimestamp As String
        Dim strErrorMessageClean As String

        If strMostRecentJobInfo Is Nothing Then strMostRecentJobInfo = String.Empty

        Try

            strMostRecentJobInfoFromLogs = String.Empty

            'If objLogger Is Nothing Then
            '    intRecentErrorMessageCount = 0
            '    ReDim strRecentErrorMessages(-1)
            'Else
            If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

            intRecentErrorMessageCount = 0
            ReDim strRecentErrorMessages(intErrorMessageCountToReturn - 1)
            ReDim dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)

            ' Initialize the RegEx that splits out the timestamp from the error message
            reErrorLine = New Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
            reJobStartLine = New Regex(JOB_START_REGEX, RegexOptions.Compiled Or RegexOptions.IgnoreCase)

            ' Initialize the queue that holds recent error messages
            qErrorMsgQueue = New Queue(intErrorMessageCountToReturn)

            ' Initialize the hashtable to hold the error messages, but without date stamps
            htUniqueErrorMessages = New Hashtable

            ' Examine the most recent error reported by objLogger
            strLineIn = clsLogTools.MostRecentErrorMessage
            If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then
                blnLoggerReportsError = True
            Else
                blnLoggerReportsError = False
            End If


            strLogFilePath = GetRecentLogFilename()
            If intErrorMessageCountToReturn > 1 OrElse Not blnLoggerReportsError Then

                ' Recent error message reported by objLogger is empty or intErrorMessageCountToReturn is greater than one
                ' Open log file strLogFilePath to find the most recent error messages
                ' If not enough error messages are found, we will look through previous log files

                intLogFileCountProcessed = 0
                blnCheckForMostRecentJob = True

                Do While qErrorMsgQueue.Count < intErrorMessageCountToReturn AndAlso intLogFileCountProcessed < MAX_LOG_FILES_TO_SEARCH

                    If File.Exists(strLogFilePath) Then
                        srInFile = New StreamReader(New FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                        If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

                        Do While Not srInFile.EndOfStream
                            strLineIn = srInFile.ReadLine

                            If Not strLineIn Is Nothing Then
                                objMatch = reErrorLine.Match(strLineIn)

                                If objMatch.Success Then
                                    DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
                                End If

                                If blnCheckForMostRecentJob Then
                                    objMatch = reJobStartLine.Match(strLineIn)
                                    If objMatch.Success Then
                                        Try
                                            strMostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(
                                              objMatch.Groups(1).Value, CInt(objMatch.Groups(2).Value),
                                              objMatch.Groups(3).Value, objMatch.Groups(4).Value)
                                        Catch ex As Exception
                                            ' Ignore errors here
                                        End Try
                                    End If
                                End If
                            End If
                        Loop

                        srInFile.Close()

                        If blnCheckForMostRecentJob AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
                            ' We determine the most recent job; no need to check other log files
                            blnCheckForMostRecentJob = False
                        End If

                    Else
                        ' Log file not found; that's OK, we'll decrement the name by one day and keep checking
                    End If

                    ' Increment the log file counter, regardless of whether or not the log file was found
                    intLogFileCountProcessed += 1

                    If qErrorMsgQueue.Count < intErrorMessageCountToReturn Then
                        ' We still haven't found intErrorMessageCountToReturn error messages
                        ' Keep checking older log files as long as qErrorMsgQueue.Count < intErrorMessageCountToReturn

                        ' Decrement the log file path by one day
                        strLogFilePath = DecrementLogFilePath(strLogFilePath)
                        If String.IsNullOrEmpty(strLogFilePath) Then
                            Exit Do
                        End If
                    End If
                Loop

            End If

            If blnLoggerReportsError Then
                ' Append the error message reported by the Logger to the error message queue (treating it as the newest error)
                strLineIn = clsLogTools.MostRecentErrorMessage
                objMatch = reErrorLine.Match(strLineIn)

                If objMatch.Success Then
                    DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
                End If
            End If


            ' Populate strRecentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
            Do While qErrorMsgQueue.Count > 0
                strErrorMessageClean = CStr(qErrorMsgQueue.Dequeue())

                ' Find the newest timestamp for this message
                If htUniqueErrorMessages.ContainsKey(strErrorMessageClean) Then
                    strTimestamp = CStr(htUniqueErrorMessages(strErrorMessageClean))
                Else
                    ' This code should not be reached
                    strTimestamp = ""
                End If

                If intRecentErrorMessageCount >= strRecentErrorMessages.Length Then
                    ' Need to reserve more memory; this is unexpected
                    ReDim Preserve strRecentErrorMessages(strRecentErrorMessages.Length * 2 - 1)
                    ReDim Preserve dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)
                End If

                strRecentErrorMessages(intRecentErrorMessageCount) = strTimestamp & ", " & strErrorMessageClean.TrimStart(" "c)

                Try
                    dtRecentErrorMessageDates(intRecentErrorMessageCount) = CDate(strTimestamp)
                Catch ex As Exception
                    ' Error converting date;
                    dtRecentErrorMessageDates(intRecentErrorMessageCount) = DateTime.MinValue
                End Try

                intRecentErrorMessageCount += 1
            Loop

            If intRecentErrorMessageCount < strRecentErrorMessages.Length Then
                ' Shrink the arrays
                ReDim Preserve strRecentErrorMessages(intRecentErrorMessageCount - 1)
                ReDim Preserve dtRecentErrorMessageDates(intRecentErrorMessageCount - 1)
            End If

            If intRecentErrorMessageCount > 1 Then
                ' Sort the arrays by descending date
                Array.Sort(dtRecentErrorMessageDates, strRecentErrorMessages)
                Array.Reverse(dtRecentErrorMessageDates)
                Array.Reverse(strRecentErrorMessages)
            End If

            If String.IsNullOrEmpty(strMostRecentJobInfo) Then
                If Not strMostRecentJobInfoFromLogs Is Nothing AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
                    ' Update strMostRecentJobInfo
                    strMostRecentJobInfo = strMostRecentJobInfoFromLogs
                End If
            End If

        Catch ex As Exception
            ' Ignore errors here
            Try
                LogError("Error in DetermineRecentErrorMessages", ex)
            Catch ex2 As Exception
                ' Ignore errors logging the error
            End Try
        End Try

        Return strRecentErrorMessages

    End Function

    Private Sub DetermineRecentErrorCacheError(
     objMatch As Match,
     strErrorMessage As String,
     htUniqueErrorMessages As Hashtable,
     qErrorMsgQueue As Queue,
     intMaxErrorMessageCountToReturn As Integer)

        Dim strTimestamp As String
        Dim strErrorMessageClean As String
        Dim strQueuedError As String

        Dim blnAddItemToQueue As Boolean

        ' See if this error is present in htUniqueErrorMessages yet
        ' If it is present, update the timestamp in htUniqueErrorMessages
        ' If not present, queue it

        If objMatch.Groups.Count >= 2 Then
            strTimestamp = objMatch.Groups(1).Value
            strErrorMessageClean = objMatch.Groups(2).Value
        Else
            ' Regex didn't match; this is unexpected
            strTimestamp = DateTime.MinValue.ToString()
            strErrorMessageClean = strErrorMessage
        End If

        ' Check whether strErrorMessageClean is in the hash table
        Dim objItem = htUniqueErrorMessages.Item(strErrorMessageClean)
        If Not objItem Is Nothing Then
            ' The error message is present
            ' Update the timestamp associated with strErrorMessageClean if the time stamp is newer than the stored one
            Try
                If DateTime.Parse(strTimestamp) > DateTime.Parse(CStr(objItem)) Then
                    htUniqueErrorMessages(strErrorMessageClean) = strTimestamp
                End If
            Catch ex As Exception
                ' Date comparison failed; leave the existing timestamp unchanged
            End Try

        Else
            ' The error message is not present
            htUniqueErrorMessages.Add(strErrorMessageClean, strTimestamp)
        End If

        If Not qErrorMsgQueue.Contains(strErrorMessageClean) Then
            ' Queue this message
            ' However, if we already have intErrorMessageCountToReturn messages queued, then dequeue the oldest one

            If qErrorMsgQueue.Count < intMaxErrorMessageCountToReturn Then
                qErrorMsgQueue.Enqueue(strErrorMessageClean)
            Else
                ' Too many queued messages, so remove oldest one
                ' However, only do this if the new error message has a timestamp newer than the oldest queued message
                '  (this is a consideration when processing multiple log files)

                blnAddItemToQueue = True

                strQueuedError = CStr(qErrorMsgQueue.Peek())

                ' Get the timestamp associated with strQueuedError, as tracked by the hashtable
                objItem = htUniqueErrorMessages.Item(strQueuedError)
                If objItem Is Nothing Then
                    ' The error message is not in the hashtable; this is unexpected
                Else
                    ' Compare the queued error's timestamp with the timestamp of the new error message
                    Try
                        If DateTime.Parse(CStr(objItem)) >= DateTime.Parse(strTimestamp) Then
                            ' The queued error message's timestamp is equal to or newer than the new message's timestamp
                            ' Do not add the new item to the queue
                            blnAddItemToQueue = False
                        End If
                    Catch ex As Exception
                        ' Date comparison failed; Do not add the new item to the queue
                        blnAddItemToQueue = False
                    End Try
                End If

                If blnAddItemToQueue Then
                    qErrorMsgQueue.Dequeue()
                    qErrorMsgQueue.Enqueue(strErrorMessageClean)
                End If

            End If
        End If

    End Sub

    ''' <summary>
    ''' Sets the local mgr_active flag to False for serious problems
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub DisableManagerLocally()

        ' Note: We previously called m_MgrSettings.DisableManagerLocally() to update AnalysisManager.config.exe
        ' We now create a flag file instead
        ' This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

        m_MgrErrorCleanup.CreateStatusFlagFile()
        UpdateStatusFlagFileExists()

    End Sub

    Private Function GetRecentLogFilename() As String
        Dim lastFilename As String
        Dim x As Integer
        Dim Files() As String

        Try
            ' Obtain a list of log files
            Files = Directory.GetFiles(m_MgrFolderPath, m_MgrSettings.GetParam("logfilename") & "*.txt")

            ' Change the file names to lowercase (to assure that the sorting works)
            For x = 0 To Files.Length - 1
                Files(x) = Files(x).ToLower
            Next

            ' Sort the files by filename
            Array.Sort(Files)

            ' Return the last filename in the list
            lastFilename = Files(Files.Length - 1)

        Catch ex As Exception
            Return String.Empty
        End Try

        Return lastFilename
    End Function

    Private Function GetManagerErrorCleanupMode() As clsCleanupMgrErrors.eCleanupModeConstants
        Dim strManagerErrorCleanupMode As String
        Dim eManagerErrorCleanupMode As clsCleanupMgrErrors.eCleanupModeConstants

        strManagerErrorCleanupMode = m_MgrSettings.GetParam("ManagerErrorCleanupMode")

        Select Case strManagerErrorCleanupMode.Trim
            Case "0"
                eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled
            Case "1"
                eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupOnce
            Case "2"
                eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways
            Case Else
                eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled
        End Select

        Return eManagerErrorCleanupMode

    End Function

    ''' <summary>
    ''' Initializes the status file writing tool
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub InitStatusTools()

        If m_StatusTools Is Nothing Then
            Dim statusFileLoc As String = Path.Combine(m_MgrFolderPath, m_MgrSettings.GetParam("statusfilelocation"))

            If Me.TraceMode Then ShowTraceMessage("Initialize m_StatusTools using " & statusFileLoc)
            m_StatusTools = New clsStatusFile(statusFileLoc, m_DebugLevel)

            With m_StatusTools
                .TaskStartTime = Date.UtcNow
                .Dataset = ""
                .JobNumber = 0
                .JobStep = 0
                .Tool = ""
                .MgrName = m_MgrName
                .MgrStatus = IStatusFile.EnumMgrStatus.RUNNING
                .TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
                .TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
            End With

            UpdateStatusToolLoggingSettings(m_StatusTools)

        End If

    End Sub

    ''' <summary>
    ''' Loads the initial settings from application config file
    ''' </summary>
    ''' <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
    ''' <remarks></remarks>
    Friend Shared Function LoadMgrSettingsFromFile() As Dictionary(Of String, String)

        'Load initial settings into string dictionary for return
        Dim lstMgrSettings As New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

        ' Note: When you are editing this project using the Visual Studio IDE, if you edit the values
        '  ->My Project>Settings.settings, then when you run the program (from within the IDE), it
        '  will update file AnalysisManagerProg.exe.config with your settings
        ' The manager will exit if the "UsingDefaults" value is "True", thus you need to have 
        '  "UsingDefaults" be "False" to run (and/or debug) the application

        My.Settings.Reload()

        'Manager config db connection string
        lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, My.Settings.MgrCnfgDbConnectStr)

        'Manager active flag
        lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, My.Settings.MgrActive_Local.ToString())

        ' Manager name
        ' If the MgrName setting in the AnalysisManagerProg.exe.config file contains the text $ComputerName$
        ' that text is replaced with this computer's domain name
        ' This is a case-sensitive comparison

        lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, My.Settings.MgrName.Replace("$ComputerName$", Environment.MachineName))

        'Default settings in use flag
        Dim usingDefaults = My.Settings.UsingDefaults.ToString()
        lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS, usingDefaults)

        ' Default connection string for logging errors to the databsae
        ' Will get updated later when manager settings are loaded from the manager control database
        Dim defaultDMSConnectionString = My.Settings.DefaultDMSConnString
        lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, defaultDMSConnectionString)

        Return lstMgrSettings

    End Function

    Private Function NeedToAbortProcessing() As Boolean

        If m_NeedToAbortProcessing Then
            LogError("Analysis manager has encountered a fatal error - aborting processing (m_NeedToAbortProcessing = True)")
            Return True
        End If

        If Not m_StatusTools Is Nothing Then
            If m_StatusTools.AbortProcessingNow Then
                LogError("Found file " & clsStatusFile.ABORT_PROCESSING_NOW_FILENAME & " - aborting processing")
                Return True
            End If
        End If

        Return False
    End Function

    Private Function LoadCachedLogMessages(messageCacheFile As FileInfo) As Dictionary(Of String, DateTime)
        Dim cachedMessages = New Dictionary(Of String, DateTime)()

        Dim sepChars As Char() = {ControlChars.Tab}

        Using reader = New StreamReader(New FileStream(messageCacheFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            Dim lineCount = 0
            While Not reader.EndOfStream
                Dim dataLine = reader.ReadLine()
                lineCount += 1

                ' Assume that the first line is the header line, which we'll skip
                If lineCount = 1 OrElse String.IsNullOrWhiteSpace(dataLine) Then
                    Continue While
                End If

                Dim lineParts = dataLine.Split(sepChars, 2)

                Dim timeStampText = lineParts(0)
                Dim message = lineParts(1)

                Dim timeStamp As DateTime
                If DateTime.TryParse(timeStampText, timeStamp) Then
                    ' Valid message; store it

                    Dim cachedTimeStamp As DateTime
                    If cachedMessages.TryGetValue(message, cachedTimeStamp) Then
                        If timeStamp > cachedTimeStamp Then
                            cachedMessages(message) = timeStamp
                        End If
                    Else
                        cachedMessages.Add(message, timeStamp)
                    End If

                End If
            End While
        End Using

        Return cachedMessages
    End Function

    Private Sub LogErrorToDatabasePeriodically(errorMessage As String, logIntervalHours As Integer)
        Const PERIODIC_LOG_FILE As String = "Periodic_ErrorMessages.txt"

        Try
            Dim cachedMessages As Dictionary(Of String, DateTime)

            Dim messageCacheFile = New FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), PERIODIC_LOG_FILE))

            If messageCacheFile.Exists Then
                cachedMessages = LoadCachedLogMessages(messageCacheFile)
                Thread.Sleep(150)
            Else
                cachedMessages = New Dictionary(Of String, DateTime)()
            End If

            Dim timeStamp As DateTime
            If cachedMessages.TryGetValue(errorMessage, timeStamp) Then
                If DateTime.UtcNow.Subtract(timeStamp).TotalHours < logIntervalHours Then
                    ' Do not log to the database
                    Return
                End If
                cachedMessages(errorMessage) = DateTime.UtcNow
            Else
                cachedMessages.Add(errorMessage, DateTime.UtcNow)
            End If

            LogError(errorMessage, True)

            ' Update the message cache file
            Using writer = New StreamWriter(New FileStream(messageCacheFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                writer.WriteLine("{0}" & vbTab & "{1}", "TimeStamp", "Message")
                For Each message In cachedMessages
                    writer.WriteLine("{0}" & vbTab & "{1}", message.Value.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT), message.Key)
                Next
            End Using
        Catch ex As Exception
            LogError("Exception in LogErrorToDatabasePeriodically", ex)
        End Try
    End Sub

    Private Sub PostToEventLog(ErrMsg As String)
        Const EVENT_LOG_NAME = "DMSAnalysisManager"

        Try
            Console.WriteLine()
            Console.WriteLine("===============================================================")
            Console.WriteLine(ErrMsg)
            Console.WriteLine("===============================================================")
            Console.WriteLine()
            Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " & EVENT_LOG_NAME & " application log")
            Console.WriteLine()

            Dim Ev As New EventLog("Application", ".", EVENT_LOG_NAME)
            Trace.Listeners.Add(New EventLogTraceListener(EVENT_LOG_NAME))
            Trace.WriteLine(ErrMsg)
            Ev.Close()

        Catch ex As Exception
            Console.WriteLine()
            Console.WriteLine("Exception logging to the event log: " & ex.Message)
        End Try

        Thread.Sleep(500)

    End Sub

    ''' <summary>
    ''' Reload the settings from AnalysisManagerProg.exe.config
    ''' </summary>
    ''' <returns>True if success, false if now disabled locally or if an error</returns>
    ''' <remarks></remarks>
    Private Function ReloadManagerSettings() As Boolean

        Try
            If Me.TraceMode Then ShowTraceMessage("Reading application config file")

            'Get settings from config file
            Dim lstMgrSettings As Dictionary(Of String, String)
            lstMgrSettings = LoadMgrSettingsFromFile()

            If Me.TraceMode Then ShowTraceMessage("Storing manager settings in m_MgrSettings")
            If Not m_MgrSettings.LoadSettings(lstMgrSettings) Then
                If Not String.IsNullOrWhiteSpace(m_MgrSettings.ErrMsg) Then
                    'Manager has been deactivated, so report this
                    LogMessage(m_MgrSettings.ErrMsg)
                    UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, "Disabled Locally")
                Else
                    'Unknown problem reading config file
                    LogError("Error re-reading config file in ReloadManagerSettings")
                End If
                Return False
            End If

        Catch ex As Exception
            LogError("Error re-loading manager settings: " & ex.Message)
            Return False
        End Try

        Return True


    End Function

    Private Sub RemoveTempFiles()

        Dim diMgrFolder = New DirectoryInfo(m_MgrFolderPath)

        ' Files starting with the name IgnoreMe are created by log4NET when it is first instantiated 
        ' This name is defined in the RollingFileAppender section of the Logging.config file via this XML:
        ' <file value="IgnoreMe" />

        For Each fiFile As FileInfo In diMgrFolder.GetFiles("IgnoreMe*.txt")
            Try
                fiFile.Delete()
            Catch ex As Exception
                LogError("Error deleting IgnoreMe file: " & fiFile.Name, ex)
            End Try
        Next

        ' Files named tmp.iso.#### and tmp.peak.#### (where #### are integers) are files created by Decon2LS
        ' These files indicate a previous, failed Decon2LS task and can be safely deleted
        ' For safety, we will not delete files less than 24 hours old

        Dim lstFilesToDelete As List(Of FileInfo) = diMgrFolder.GetFiles("tmp.iso.*").ToList()

        lstFilesToDelete.AddRange(diMgrFolder.GetFiles("tmp.peak.*"))

        For Each fiFile As FileInfo In lstFilesToDelete
            Try
                If Date.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > 24 Then
                    If Me.TraceMode Then ShowTraceMessage("Deleting temp file " & fiFile.FullName)
                    fiFile.Delete()
                End If
            Catch ex As Exception
                LogError("Error deleting file: " & fiFile.Name)
            End Try
        Next


    End Sub

    Private Function SetResourceObject() As Boolean

        Dim strMessage As String
        Dim stepToolName As String = m_AnalysisTask.GetParam("StepTool")

        m_PluginLoader.ClearMessageList()
        m_Resource = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower)
        If m_Resource Is Nothing Then
            LogError("Unable to load resource object, " & m_PluginLoader.Message)
            Return False
        End If

        If m_DebugLevel > 0 Then
            strMessage = "Loaded resourcer for StepTool " & stepToolName
            If m_PluginLoader.Message.Length > 0 Then strMessage &= ": " & m_PluginLoader.Message
            LogMessage(strMessage)
        End If

        Try
            m_Resource.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools, m_MyEMSLUtilities)
        Catch ex As Exception
            LogError("Unable to load resource object, " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Look for flagFile.txt in the .exe folder
    ''' Auto clean errors if AutoCleanupManagerErrors is enabled
    ''' </summary>
    ''' <returns>True if a flag file exists, false if safe to proceed</returns>
    Private Function StatusFlagFileError() As Boolean

        Dim blnMgrCleanupSuccess As Boolean

        If Not m_MgrErrorCleanup.DetectStatusFlagFile() Then
            ' No error; return false
            Return False
        End If

        Try
            blnMgrCleanupSuccess = m_MgrErrorCleanup.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), m_DebugLevel)

        Catch ex As Exception

            LogError("Error calling AutoCleanupManagerErrors, " & ex.Message, ex)
            m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

            blnMgrCleanupSuccess = False
        End Try

        If blnMgrCleanupSuccess Then
            LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)")

            ' No error; return false
            Return False
        End If

        ' Error removing flag file (or manager not set to auto-remove flag files)

        ' Periodically log errors to the database
        Dim flagFile = New FileInfo(m_MgrErrorCleanup.FlagFilePath)
        Dim errorMessage As String
        If (flagFile.Directory Is Nothing) Then
            errorMessage = "Flag file exists in the manager folder"
        Else
            errorMessage = "Flag file exists in folder " + flagFile.Directory.Name
        End If

        ' Post a log entry to the database every 4 hours
        LogErrorToDatabasePeriodically(errorMessage, 4)

        ' Return true (indicating a flag file exists)
        Return True

    End Function

    Private Function SetToolRunnerObject() As Boolean
        Dim stepToolName As String = m_AnalysisTask.GetParam("StepTool")

        m_PluginLoader.ClearMessageList()
        m_ToolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower())
        If m_ToolRunner Is Nothing Then
            LogError("Unable to load tool runner for StepTool " & stepToolName & ": " & m_PluginLoader.Message)
            Return False
        End If

        If m_DebugLevel > 0 Then
            Dim msg = "Loaded tool runner for StepTool " & m_AnalysisTask.GetCurrentJobToolDescription()
            If m_PluginLoader.Message.Length > 0 Then msg &= ": " & m_PluginLoader.Message
            LogMessage(msg)
        End If

        Try
            ' Setup the new tool runner
            m_ToolRunner.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools, m_SummaryFile, m_MyEMSLUtilities)
        Catch ex As Exception
            LogError("Exception calling ToolRunner.Setup(): " + ex.Message)
            Return False
        End Try

        Return True

    End Function

    Public Shared Sub ShowTraceMessage(strMessage As String)
        Console.ForegroundColor = ConsoleColor.DarkGray
        Console.WriteLine(Date.Now.ToString("hh:mm:ss.fff tt") & ": " & strMessage)
        Console.ResetColor()
    End Sub

    Private Sub UpdateClose(ManagerCloseMessage As String)
        Dim strErrorMessages() As String
        strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)

        m_StatusTools.UpdateClose(ManagerCloseMessage, strErrorMessages, m_MostRecentJobInfo, True)
    End Sub

    ''' <summary>
    ''' Reloads the manager settings from the manager control database 
    ''' if at least MinutesBetweenUpdates minutes have elapsed since the last update
    ''' </summary>
    ''' <param name="dtLastConfigDBUpdate"></param>
    ''' <param name="MinutesBetweenUpdates"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function UpdateManagerSettings(ByRef dtLastConfigDBUpdate As DateTime, MinutesBetweenUpdates As Double) As Boolean

        Dim blnSuccess = True

        If (Date.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates) Then

            dtLastConfigDBUpdate = Date.UtcNow

            If Me.TraceMode Then ShowTraceMessage("Loading manager settings from the manager control DB")

            If Not m_MgrSettings.LoadDBSettings() Then
                Dim msg As String

                If (String.IsNullOrEmpty(m_MgrSettings.ErrMsg)) Then
                    msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings"
                Else
                    msg = m_MgrSettings.ErrMsg
                End If

                LogError(msg)

                blnSuccess = False
            Else
                ' Need to synchronize some of the settings
                UpdateStatusToolLoggingSettings(m_StatusTools)
            End If

        End If

        Return blnSuccess

    End Function

    Private Sub UpdateStatusDisabled(managerStatus As IStatusFile.EnumMgrStatus, managerDisableMessage As String)
        Dim strErrorMessages() As String
        strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)
        m_StatusTools.UpdateDisabled(managerStatus, managerDisableMessage, strErrorMessages, m_MostRecentJobInfo)
        Console.WriteLine(managerDisableMessage)
    End Sub

    Private Sub UpdateStatusFlagFileExists()
        Dim strErrorMessages() As String
        strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)
        m_StatusTools.UpdateFlagFileExists(strErrorMessages, m_MostRecentJobInfo)
        Console.WriteLine("Flag file exists")
    End Sub

    Private Sub UpdateStatusIdle(ManagerIdleMessage As String)
        Dim strErrorMessages() As String
        strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)

        m_StatusTools.UpdateIdle(ManagerIdleMessage, strErrorMessages, m_MostRecentJobInfo, True)
    End Sub

    Private Sub UpdateStatusToolLoggingSettings(objStatusFile As clsStatusFile)


        Dim logMemoryUsage As Boolean = m_MgrSettings.GetParam("LogMemoryUsage", False)
        Dim minimumMemoryUsageLogInterval As Single = m_MgrSettings.GetParam("MinimumMemoryUsageLogInterval", 1)

        ' Most managers have logStatusToBrokerDb=False and logStatusToMessageQueue=True
        Dim logStatusToBrokerDb As Boolean = m_MgrSettings.GetParam("LogStatusToBrokerDB", False)
        Dim brokerDbConnectionString As String = m_MgrSettings.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline
        Dim brokerDbStatusUpdateIntervalMinutes As Single = m_MgrSettings.GetParam("BrokerDBStatusUpdateIntervalMinutes", 60)

        Dim logStatusToMessageQueue As Boolean = m_MgrSettings.GetParam("LogStatusToMessageQueue", False)
        If DisableMessageQueue Then
            ' Command line has switch /NQ
            ' Disable message queue logging
            logStatusToMessageQueue = False
        End If

        Dim messageQueueUri As String = m_MgrSettings.GetParam("MessageQueueURI")
        Dim messageQueueTopicMgrStatus As String = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus")

        With objStatusFile
            .ConfigureMemoryLogging(logMemoryUsage, minimumMemoryUsageLogInterval, m_MgrFolderPath)
            .ConfigureBrokerDBLogging(logStatusToBrokerDb, brokerDbConnectionString, brokerDbStatusUpdateIntervalMinutes)
            .ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus, m_MgrName)
        End With

    End Sub

    ''' <summary>
    ''' Confirms that the drive with the working directory has sufficient free space
    ''' Confirms that the remote share for storing results is accessible and has sufficient free space
    ''' </summary>
    ''' <param name="ErrorMessage"></param>
    ''' <returns></returns>
    ''' <remarks>Disables the manager if the working directory drive does not have enough space</remarks>
    Private Function ValidateFreeDiskSpace(<Out> ByRef errorMessage As String) As Boolean

        Const DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 10
        Const DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB = 10

        Const DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB = 750
        Const DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750

        errorMessage = String.Empty

        Try
            Dim stepToolNameLCase = m_AnalysisTask.GetParam("JobParameters", "StepTool").ToLower()

            If stepToolNameLCase = "results_transfer" Then
                ' We only need to evaluate the dataset storage folder for free space

                Dim datasetStoragePath = m_AnalysisTask.GetParam("DatasetStoragePath")
                Dim datasetStorageMinFreeSpaceGB = m_MgrSettings.GetParam("DatasetStorageMinFreeSpaceGB", DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB)

                If String.IsNullOrEmpty(datasetStoragePath) Then
                    errorMessage = "DatasetStoragePath job parameter is empty"
                    LogError(errorMessage)
                    Return False
                End If

                Dim diDatasetStoragePath = New DirectoryInfo(datasetStoragePath)
                If Not diDatasetStoragePath.Exists Then
                    ' Dataset folder not found; that's OK, since the Results Transfer plugin will auto-create it
                    ' Try to use the parent folder (or the parent of the parent)
                    Do While Not diDatasetStoragePath.Exists AndAlso Not diDatasetStoragePath.Parent Is Nothing
                        diDatasetStoragePath = diDatasetStoragePath.Parent
                    Loop

                    datasetStoragePath = diDatasetStoragePath.FullName
                End If

                If Not ValidateFreeDiskSpaceWork("Dataset directory", datasetStoragePath, datasetStorageMinFreeSpaceGB * 1024, errorMessage, clsLogTools.LoggerTypes.LogFile) Then
                    Return False
                Else
                    Return True
                End If
            End If

            Dim workingDirMinFreeSpaceMB = m_MgrSettings.GetParam("WorkDirMinFreeSpaceMB", DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB)

            Dim transferDir = m_AnalysisTask.GetParam("JobParameters", "transferFolderPath")
            Dim transferDirMinFreeSpaceGB = m_MgrSettings.GetParam("TransferDirMinFreeSpaceGB", DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB)

            Dim orgDbDir = m_MgrSettings.GetParam("orgdbdir")
            Dim orgDbDirMinFreeSpaceMB = m_MgrSettings.GetParam("OrgDBDirMinFreeSpaceMB", DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB)

            ' Verify that the working directory exists and that its drive has sufficient free space
            If Not ValidateFreeDiskSpaceWork("Working directory", m_WorkDirPath, workingDirMinFreeSpaceMB, errorMessage, clsLogTools.LoggerTypes.LogDb) Then
                LogError("Disabling manager since working directory problem")
                DisableManagerLocally()
                Return False
            End If

            If String.IsNullOrEmpty(transferDir) Then
                errorMessage = "Transfer directory for the job is empty; cannot continue"

                If DataPackageIdMissing() Then
                    errorMessage &= ". Data package ID cannot be 0 for this job type"
                End If

                LogError(errorMessage)
                Return False
            End If

            ' Verify that the remote transfer directory exists and that its drive has sufficient free space
            If Not ValidateFreeDiskSpaceWork("Transfer directory", transferDir, transferDirMinFreeSpaceGB * 1024, errorMessage, clsLogTools.LoggerTypes.LogFile) Then
                Return False
            End If

            Dim orgDbRequired As Boolean = m_Resource.GetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired)

            If orgDbRequired Then
                ' Verify that the local fasta file cache directory has sufficient free space

                If Not ValidateFreeDiskSpaceWork("Organism DB directory", orgDbDir, orgDbDirMinFreeSpaceMB, errorMessage, clsLogTools.LoggerTypes.LogFile) Then
                    DisableManagerLocally()
                    Return False
                End If

            End If

        Catch ex As Exception
            LogError("Exception validating free space: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Private Function ValidateFreeDiskSpaceWork(
      directoryDescription As String,
      directoryPath As String,
      minFreeSpaceMB As Integer,
     <Out()> ByRef errorMessage As String,
      eLogLocationIfNotFound As clsLogTools.LoggerTypes) As Boolean

        Return clsGlobal.ValidateFreeDiskSpace(directoryDescription, directoryPath, minFreeSpaceMB, eLogLocationIfNotFound, errorMessage)

    End Function

    Private Function VerifyWorkDir() As Boolean

        ' Verify working directory is valid
        If Not Directory.Exists(m_WorkDirPath) Then
            Dim msg = "Invalid working directory: " & m_WorkDirPath
            LogError(msg)
            Console.WriteLine(msg)
            Thread.Sleep(1500)
            Return False
        End If

        Return True

    End Function

    ''' <summary>
    ''' Verifies working directory is properly specified and is empty
    ''' </summary>
    ''' <returns></returns>
    Private Function ValidateWorkingDir() As Boolean

        ' Verify working directory is valid
        If Not VerifyWorkDir() Then
            Return False
        End If

        ' Verify the working directory is empty
        Dim workDir = New DirectoryInfo(m_WorkDirPath)
        Dim workDirFiles = workDir.GetFiles()
        Dim workDirFolders = workDir.GetDirectories()

        If (workDirFolders.Count = 0) And (workDirFiles.Count = 1) Then
            ' If the only file in the working directory is a JobParameters xml file,
            '  then try to delete it, since it's likely left over from a previous job that never actually started
            Dim firstFile = workDirFiles.First

            If firstFile.Name.StartsWith(clsGlobal.XML_FILENAME_PREFIX) AndAlso
               firstFile.Name.EndsWith(clsGlobal.XML_FILENAME_EXTENSION) Then
                Try
                    LogWarning("Working directory contains a stray JobParameters file, deleting it: " & firstFile.FullName)

                    firstFile.Delete()

                    ' Wait 0.5 second and then refresh tmpFilArray
                    Thread.Sleep(500)

                    ' Now obtain a new listing of files
                    If workDir.GetFiles(m_WorkDirPath).Count = 0 Then
                        ' The directory is now empty
                        Return True
                    End If
                Catch ex As Exception
                    ' Deletion failed
                End Try
            End If
        End If

        Dim errorCount = workDirFiles.Count(Function(item) Not Files.clsFileTools.IsVimSwapFile(item.FullName))

        If errorCount = 0 Then
            ' No problems found
            Return True
        End If

        LogError("Working directory not empty: " & m_WorkDirPath)
        Return False

    End Function

#End Region

    ''' <summary>
    ''' Event handler for file watcher
    ''' </summary>
    ''' <param name="sender"></param>
    ''' <param name="e"></param>
    ''' <remarks></remarks>
    Private Sub m_FileWatcher_Changed(sender As Object, e As FileSystemEventArgs) Handles m_FileWatcher.Changed

        m_FileWatcher.EnableRaisingEvents = False
        m_ConfigChanged = True

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Config file changed")
        End If

    End Sub

End Class
