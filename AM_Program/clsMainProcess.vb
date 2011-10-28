'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerProg

	Public Class clsMainProcess

		'*********************************************************************************************************
		'Master processing class for analysis manager
		'*********************************************************************************************************

#Region "Constants"
        Private Const CUSTOM_LOG_SOURCE_NAME As String = "Analysis Manager"
		Private Const CUSTOM_LOG_NAME As String = "DMS_AnalysisMgr"
        Private Const MAX_ERROR_COUNT As Integer = 6

        Private Const DECON2LS_FATAL_REMOTING_ERROR As String = "Fatal remoting error"
        Private Const DECON2LS_CORRUPTED_MEMORY_ERROR As String = "Corrupted memory error"
        Private Const DECON2LS_TCP_ALREADY_REGISTERED_ERROR As String = "channel 'tcp' is already registered"
#End Region

#Region "Module variables"
        Private m_MainProcess As clsMainProcess
		Private m_MgrSettings As clsAnalysisMgrSettings
		Private m_AnalysisTask As clsAnalysisJob
        Private WithEvents m_FileWatcher As FileSystemWatcher
		Private m_ConfigChanged As Boolean = False
        Private m_DebugLevel As Integer = 0
        Private m_Resource As IAnalysisResources
        Private m_ToolRunner As IToolRunner
        Private m_StatusTools As clsStatusFile
        Private m_NeedToAbortProcessing As Boolean = False
        Private m_MostRecentJobInfo As String = String.Empty
#End Region

#Region "Properties"
#End Region

#Region "Methods"
        ''' <summary>
        ''' Starts program execution
        ''' </summary>
        ''' <returns>0 if no error; error code if an error</returns>
        ''' <remarks></remarks>
        Public Function Main() As Integer

            Dim ErrMsg As String

            Try
                If IsNothing(m_MainProcess) Then
                    m_MainProcess = New clsMainProcess
                    If Not m_MainProcess.InitMgr Then Exit Function
                End If
                clsGlobal.AppFilePath = Application.ExecutablePath

                Dim fiExecutable As New FileInfo(AppFilePath)
                clsGlobal.AppFolderPath = fiExecutable.DirectoryName

                m_MainProcess.DoAnalysis()

                Return 0

            Catch Err As System.Exception
                'Report any exceptions not handled at a lower level to the system application log
                ErrMsg = "Critical exception starting application: " & Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err)
                Dim Ev As New EventLog("Application", ".", "DMSAnalysisManager")
                Trace.Listeners.Add(New EventLogTraceListener("DMSAnalysisManager"))
                Trace.WriteLine(ErrMsg)
                Ev.Close()
                Return 1
            End Try

            Return 0

        End Function


        ''' <summary>
        ''' Constructor
        ''' </summary>
        ''' <remarks>Doesn't do anything at present</remarks>
        Public Sub New()

        End Sub

        ''' <summary>
        ''' Initializes the manager settings
        ''' </summary>
        ''' <returns>TRUE for success, FALSE for failure</returns>
        ''' <remarks></remarks>
        Private Function InitMgr() As Boolean

            ' Get the manager settings
            ' If you get an exception here while debugging in Visual Studio, then be sure 
            '   that "UsingDefaults" is set to False in CaptureTaskManager.exe.config               
            Try
                m_MgrSettings = New clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME)
            Catch ex As System.Exception
                ' Failures are logged by clsMgrSettings to application event logs;
                '  this includes MgrActive_Local = False
                '  
                ' If the DMS_AnalysisMgr application log does not exist yet, the SysLogger will create it
                ' However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
                ' Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

                Console.WriteLine("Exception instantiating clsAnalysisMgrSettings: " & ex.Message)
                Return False
            End Try

            ' Delete any temporary files that may be left in the app directory
            RemoveTempFiles()

            'Setup the logger
            Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
            Dim LogFileName As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))

            'Make the initial log entry
            clsLogTools.ChangeLogFileName(m_MgrSettings.GetParam("logfilename"))
            Dim MyMsg As String = "=== Started Analysis Manager V" & Application.ProductVersion & " ===== "
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, MyMsg)

            'Setup a file watcher for the config file
            m_FileWatcher = New FileSystemWatcher
            With m_FileWatcher
                .BeginInit()
                .Path = FInfo.DirectoryName
                .IncludeSubdirectories = False
                .Filter = m_MgrSettings.GetParam("configfilename")
                .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
                .EndInit()
                .EnableRaisingEvents = True
            End With

            'Get the debug level
            m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

            'Setup the tool for getting tasks
            m_AnalysisTask = New clsAnalysisJob(m_MgrSettings, m_DebugLevel)

            'Everything worked
            Return True

        End Function

        ''' <summary>
        ''' Loop to perform all analysis jobs
        ''' </summary>
        ''' <remarks></remarks>
        Public Sub DoAnalysis()

            Dim LoopCount As Integer = 0
            Dim MaxLoopCount As Integer = CInt(m_MgrSettings.GetParam("maxrepetitions"))
            Dim TasksStartedCount As Integer = 0
            Dim blnErrorDeletingFilesFlagFile As Boolean

            Dim strWorkingDir As String = m_MgrSettings.GetParam("workdir")
            Dim strMessage As String
            Dim dtLastConfigDBUpdate As System.DateTime = System.DateTime.UtcNow

            Dim blnRequestJobs As Boolean = False
            Dim blnOneTaskStarted As Boolean = False
            Dim blnOneTaskPerformed As Boolean = False
            Dim intErrorCount As Integer = 0

            Try

                blnRequestJobs = True
                blnOneTaskStarted = False
                blnOneTaskPerformed = False

                InitStatusTools()

                While (LoopCount < MaxLoopCount) And blnRequestJobs

                    UpdateStatusIdle("No analysis jobs found")
                    If DetectErrorDeletingFilesFlagFile() Then
                        'Delete the Error Deleting status flag file first, so next time through this step is skipped
                        DeleteErrorDeletingFilesFlagFile()

                        'There was a problem deleting non result files with the last job.  Attempt to delete files again
                        If Not CleanWorkDir(strWorkingDir) Then
                            If blnOneTaskStarted Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error cleaning working directory, job " & m_AnalysisTask.GetParam("Job") & "; see folder " & strWorkingDir)
                                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
                            Else
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error cleaning working directory; see folder " & strWorkingDir)
                            End If
                            CreateStatusFlagFile()
                            UpdateStatusFlagFileExists()
                            Exit Sub
                        End If
                        'successful delete of files in working directory, so delete the status flag file
                        DeleteStatusFlagFile(m_DebugLevel)
                    End If

                    'Verify that an error hasn't left the the system in an odd state
                    If StatusFlagFileError(strWorkingDir) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Flag file exists - unable to perform any further analysis jobs")
                        UpdateStatusFlagFileExists()
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                        Exit Sub
                    End If

                    ' Check for configuration change
                    ' This variable will be true if the CaptureTaskManager.exe.config file has been updated
                    If m_ConfigChanged Then
                        'Local config file has changed
                        m_ConfigChanged = False
                        If Not m_MgrSettings.LoadSettings() Then
                            If m_MgrSettings.ErrMsg <> "" Then
                                'Manager has been deactivated, so report this
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_MgrSettings.ErrMsg)
                                UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, "Disabled Locally")
                            Else
                                'Unknown problem reading config file
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Error re-reading config file")
                            End If
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                            Exit Sub
                        End If
                        m_FileWatcher.EnableRaisingEvents = True
                    Else

                        ' Reload the manager control DB settings in case they have changed
                        ' However, only reload every 2 minutes
                        If Not UpdateManagerSettings(dtLastConfigDBUpdate, 2) Then
                            ' Error retrieving settings from the manager control DB
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                            Exit Sub
                        End If

                    End If

                    'Check to see if manager is still active
                    Dim MgrActive As Boolean = CBoolSafe(m_MgrSettings.GetParam("mgractive"))
                    Dim MgrActiveLocal As Boolean = CBoolSafe(m_MgrSettings.GetParam("mgractive_local"))
                    Dim strManagerDisableReason As String
                    If Not (MgrActive And MgrActiveLocal) Then
                        If Not MgrActiveLocal Then
                            strManagerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config"
                            UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, strManagerDisableReason)
                        Else
                            strManagerDisableReason = "Disabled in Manager Control DB"
                            UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_MC, strManagerDisableReason)
                        End If

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manager inactive: " & strManagerDisableReason)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                        Exit Sub
                    End If

                    Dim MgrUpdateRequired As Boolean = CBoolSafe(m_MgrSettings.GetParam("ManagerUpdateRequired"))
                    If MgrUpdateRequired Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manager update is required")
                        m_MgrSettings.AckManagerUpdateRequired()
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                        UpdateStatusIdle("Manager update is required")
                        Exit Sub
                    End If

                    'Check to see if an excessive number of errors have occurred
                    If intErrorCount > MAX_ERROR_COUNT Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Excessive task failures; disabling manager via flag file")

                        ' Note: We previously called DisableManagerLocally() to update AnalysisManager.config.exe
                        ' We now create a flag file instead
                        ' This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

                        CreateStatusFlagFile()
                        UpdateStatusFlagFileExists()

                        Exit While
                    End If

                    'Verify working directory properly specified and empty
                    If Not ValidateWorkingDir() Then
                        If blnOneTaskStarted Then
                            ' Working directory problem due to the most recently processed job
                            ' Create ErrorDeletingFiles file and exit the program
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Working directory problem, creating " & ERROR_DELETING_FILES_FILENAME & "; see folder " & strWorkingDir)
                            CreateErrorDeletingFilesFlagFile()
                            UpdateStatusIdle("Working directory not empty")
                        Else
                            ' Working directory problem, so create flag file and exit
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Working directory problem, disabling manager via flag file; see folder " & strWorkingDir)
                            CreateStatusFlagFile()
                            UpdateStatusFlagFileExists()
                        End If
                        Exit While
                    End If

                    'Get an analysis job, if any are available
                    Dim TaskReturn As clsAnalysisJob.RequestTaskResult = m_AnalysisTask.RequestTask
                    Select Case TaskReturn
                        Case clsDBTask.RequestTaskResult.NoTaskFound
                            'No tasks found
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "No analysis jobs found")
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.INFO, "No analysis jobs found")
                            blnRequestJobs = False
                            intErrorCount = 0
                            UpdateStatusIdle("No analysis jobs found")
                        Case clsDBTask.RequestTaskResult.ResultError
                            'There was a problem getting the task; errors were logged by RequestTaskResult
                            intErrorCount += 1
                        Case clsDBTask.RequestTaskResult.TaskFound
                            blnRequestJobs = True
                            TasksStartedCount += 1

                            Try
                                blnOneTaskStarted = True
                                If DoAnalysisJob() Then
                                    ' Task succeeded; reset the sequential job failure counter
                                    intErrorCount = 0
                                    blnOneTaskPerformed = True
                                Else
                                    'Something went wrong; errors were logged by DoAnalysisJob
                                    intErrorCount += 1
                                End If

                            Catch ex As Exception
                                ' Something went wrong; errors likely were not logged by DoAnalysisJob

                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysis(), Exception thrown by DoAnalysisJob, " & _
                                    ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

                                ' Set the job state to failed
                                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception thrown by DoAnalysisJob")

                                intErrorCount += 1
                                m_NeedToAbortProcessing = True

                            End Try

                        Case clsDBTask.RequestTaskResult.TooManyRetries
                            'There were too many retries calling the stored procedure; errors were logged by RequestTaskResult
                            ' Bump up LoopCount to the maximum to exit the loop
                            UpdateStatusIdle("Excessive retries requesting task")
                            LoopCount = MaxLoopCount
                        Case clsDBTask.RequestTaskResult.Deadlock
                            ' A deadlock error occured
                            ' Query the DB again
                        Case Else
                            'Shouldn't ever get here
                            Dim MyErr As String = "clsMainProcess.DoAnalysis; Invalid request result: "
                            MyErr &= CInt(TaskReturn).ToString
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, MyErr)
                            Exit Sub
                    End Select

                    If NeedToAbortProcessing() Then Exit While
                    LoopCount += 1

                    'if the only problem was deleting non result files, we want to stop the manager
                    If DetectErrorDeletingFilesFlagFile() Then
                        blnErrorDeletingFilesFlagFile = True
                        LoopCount = MaxLoopCount
                    End If

                End While

                If LoopCount >= MaxLoopCount Then
                    If blnErrorDeletingFilesFlagFile Then
                        If TasksStartedCount > 0 Then
                            strMessage = "Error deleting file with an open file handle; closing manager.  Jobs processed: " & TasksStartedCount.ToString
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                        End If
                    Else
                        If TasksStartedCount > 0 Then
                            strMessage = "Maximum number of jobs to analyze has been reached: " & TasksStartedCount.ToString & " job"
                            If TasksStartedCount <> 1 Then strMessage &= "s"
                            strMessage &= "; closing manager"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                        End If
                    End If
                End If

                If blnOneTaskPerformed Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "Analysis complete for all available jobs")
                End If

                UpdateClose("Closing manager.")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysis(), Error encountered, " & _
                 Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & Err.Message, m_MostRecentJobInfo, True)

            Finally
                If Not m_StatusTools Is Nothing Then
                    m_StatusTools.DisposeMessageQueue()
                End If
            End Try

        End Sub

        ''' <summary>
        ''' Sets the local mgr_active flag to False for serious problems
        ''' </summary>
        ''' <remarks></remarks>
        Private Sub DisableManagerLocally()

            If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error while disabling manager: " & m_MgrSettings.ErrMsg)
            End If

        End Sub

        ''' <summary>
        ''' Event handler for file watcher
        ''' </summary>
        ''' <param name="sender"></param>
        ''' <param name="e"></param>
        ''' <remarks></remarks>
        Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed

            m_FileWatcher.EnableRaisingEvents = False
            m_ConfigChanged = True

            If m_DebugLevel > 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Config file changed")
            End If

        End Sub

        Private Function DoAnalysisJob() As Boolean

            Dim eToolRunnerResult As IJobParams.CloseOutType
            Dim MgrName As String = m_MgrSettings.GetParam("MgrName")
            Dim JobNum As Integer = CInt(m_AnalysisTask.GetParam("Job"))
            Dim StepNum As Integer = CIntSafe(m_AnalysisTask.GetParam("Step"), 0)
			Dim Dataset As String = m_AnalysisTask.GetParam("DatasetNum")
            Dim WorkDirPath As String = m_MgrSettings.GetParam("workdir")
            Dim JobToolDescription As String = m_AnalysisTask.GetCurrentJobToolDescription
            Dim ErrorMessage As String = String.Empty

            Dim blnRunToolError As Boolean = False

            'Initialize summary and status files
            InitSummary()
            If m_StatusTools Is Nothing Then
                InitStatusTools()
            End If

            ' Reset the completion message, the evaluation code, and the evaluation message
            clsGlobal.m_Completions_Msg = String.Empty
            clsGlobal.m_EvalCode = 0
            clsGlobal.m_EvalMessage = String.Empty

            ' Update the cached most recent job info
            m_MostRecentJobInfo = ConstructMostRecentJobInfoText(System.DateTime.Now.ToString(), JobNum, Dataset, JobToolDescription)

            With m_StatusTools
                .TaskStartTime = System.DateTime.UtcNow
                .Dataset = Dataset
                .JobNumber = JobNum
                .JobStep = StepNum
                .Tool = JobToolDescription
                .MgrName = m_MgrSettings.GetParam("MgrName")
                .UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RETRIEVING_RESOURCES, 0, 0, "", "", m_MostRecentJobInfo, True)
            End With

            ' Note: The format of the following text is important; be careful about changing it
            ' In particular, function DetermineRecentErrorMessages in clsGlobal looks for log entries
            '   matching RegEx: "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool (.+), Normal"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, MgrName & ": Started analysis job " & JobNum & ", Dataset " & Dataset & ", Tool " & JobToolDescription)

            If m_DebugLevel >= 2 Then
                ' Log the debug level value whenever the debug level is 2 or higher
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Debug level is " & m_DebugLevel.ToString)
            End If

            'Create an object to manage the job resources
            If Not SetResourceObject() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MgrName & ": Unable to SetResourceObject, job " & JobNum & ", Dataset " & Dataset)
                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object")
                CleanWorkDir(WorkDirPath)
                UpdateStatusIdle("Error encountered: Unable to set resource object")
                Return False
            End If

            'Create an object to run the analysis tool
            If Not SetToolRunnerObject() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MgrName & ": Unable to SetToolRunnerObject, job " & JobNum & ", Dataset " & Dataset)
                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set tool runner object")
                CleanWorkDir(WorkDirPath)
                UpdateStatusIdle("Error encountered: Unable to set tool runner object")
                Return False
            End If

            If NeedToAbortProcessing() Then
                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Processing aborted")
                CleanWorkDir(WorkDirPath)
                UpdateStatusIdle("Processing aborted")
                Return False
            End If

            'Create the object that handles analysis results
            Dim myResults As New clsAnalysisResults(m_MgrSettings, m_AnalysisTask)

            'Retrieve files required for the job
            CreateStatusFlagFile()
            Try
                eToolRunnerResult = m_Resource.GetResources()
                If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    '						m_ToolRunner.SetResourcerDataFileList(m_Resource.DataFileList)
                Else
                    ErrorMessage = "GetResources returned result: " & eToolRunnerResult.ToString
                    If Not m_Resource.Message Is Nothing Then
                        ErrorMessage &= "; " & m_Resource.Message
                    End If

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MgrName & ": " & ErrorMessage & ", Job " & JobNum & ", Dataset " & Dataset)
                    m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_Resource.Message)

                    clsGlobal.CleanWorkDir(WorkDirPath)
                    UpdateStatusIdle("Error encountered: " & ErrorMessage)
                    clsGlobal.DeleteStatusFlagFile(m_DebugLevel)
                    Return False
                End If
            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Getting resources, " & _
                 Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))

                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception getting resources")

                If CleanWorkDir(WorkDirPath) Then
                    DeleteStatusFlagFile(m_DebugLevel)
                Else
                    CreateErrorDeletingFilesFlagFile()
                End If

                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
                Return False
            End Try

            'Run the job
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 0)
            Try
                eToolRunnerResult = m_ToolRunner.RunTool()
                If eToolRunnerResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    ErrorMessage = m_ToolRunner.Message

                    If String.IsNullOrEmpty(ErrorMessage) Then
                        If Not String.IsNullOrEmpty(m_Completions_Msg) Then
                            ErrorMessage = String.Copy(m_Completions_Msg)
                        Else
                            ErrorMessage = "Unknown ToolRunner Error"
                        End If
                    End If

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MgrName & ": " & ErrorMessage & ", Job " & JobNum & ", Dataset " & Dataset)
                    m_AnalysisTask.CloseTask(eToolRunnerResult, ErrorMessage)

                    Try
                        If ErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) OrElse _
                           ErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR) Then
                            m_NeedToAbortProcessing = True
                        End If

                    Catch ex As Exception
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Exception examining ErrorMessage", ex)
                    End Try

                    If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE Then
                        m_NeedToAbortProcessing = True
                    End If

                    blnRunToolError = True
                End If

                If m_ToolRunner.NeedToAbortProcessing Then
                    m_NeedToAbortProcessing = True
                End If

            Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), running tool, " & Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))

                If Err.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR) Then
                    m_NeedToAbortProcessing = True
                End If

                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception running tool")

                blnRunToolError = True
            End Try

            If blnRunToolError Then
                ' Note: the above code should have already called m_AnalysisTask.CloseTask()

                Try
                    If CleanWorkDir(WorkDirPath) Then
                        DeleteStatusFlagFile(m_DebugLevel)
                    Else
                        CreateErrorDeletingFilesFlagFile()
                    End If

                    If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES AndAlso _
                       m_AnalysisTask.GetParam("StepTool").ToLower = "sequest" Then
                        ' This was a Sequest job, but no .DTA files were found
                        ' We return True here because we don't want this problem to be counted as a manager failure
                        Return True
                    Else
                        Return False
                    End If

                Catch Err As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," & _
                      Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
                    Return False
                End Try

            End If

            'Close out the job
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.CLOSING, IStatusFile.EnumTaskStatusDetail.CLOSING, 0)
            Try
                'Close out the job as a success
                m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_SUCCESS, m_Completions_Msg, m_EvalCode, m_EvalMessage)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, MgrName & ": Completed job " & JobNum)

                UpdateStatusIdle("Completed job " & JobNum & ", step " & StepNum)

            Catch err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Close task after normal run," & _
                 err.Message & "; " & clsGlobal.GetExceptionStackTrace(err))
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & err.Message, m_MostRecentJobInfo, True)
                Return False
            End Try

            Try
                'If success was reported check to see if there was an error deleting non result files
                If DetectErrorDeletingFilesFlagFile() Then
                    'If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next start up
                    ' However, wait another 5 seconds before continuing
                    GC.Collect()
                    GC.WaitForPendingFinalizers()
                    System.Threading.Thread.Sleep(5000)

                    Return True
                Else
                    'Clean the working directory
                    Try
                        If Not CleanWorkDir(WorkDirPath) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error cleaning working directory, job " & m_AnalysisTask.GetParam("Job"))
                            m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
                            CreateErrorDeletingFilesFlagFile()
                            Return False
                        End If
                    Catch Err As Exception
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," & _
                                                                           Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
                        m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
                        Return False
                    End Try

                    'Delete the status flag file
                    DeleteStatusFlagFile(m_DebugLevel)

                    ' Note that we do not need to call m_StatusTools.UpdateIdle() here since 
                    ' we called UpdateStatusIdle() just after m_AnalysisTask.CloseTask above

                    Return True
                End If

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), " & _
                                 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
                Return False
            End Try

        End Function

        Protected Function GetManagerErrorCleanupMode() As clsCleanupMgrErrors.eCleanupModeConstants
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

        Private Sub InitSummary()
            clsSummaryFile.Clear()
        End Sub

        Private Function NeedToAbortProcessing() As Boolean

            If m_NeedToAbortProcessing Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Analysis manager has encountered a fatal error - aborting processing")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
                Return True
            End If

            If Not m_StatusTools Is Nothing Then
                If m_StatusTools.AbortProcessingNow Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Found file " & clsStatusFile.ABORT_PROCESSING_NOW_FILENAME & " - aborting processing")
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "===== Closing Analysis Manager =====")
                    Return True
                End If
            End If

            Return False
        End Function

        Private Function SetResourceObject() As Boolean

            Dim strMessage As String
            Dim StepToolName As String = m_AnalysisTask.GetParam("StepTool")

            m_Resource = clsPluginLoader.GetAnalysisResources(StepToolName.ToLower)
            If m_Resource Is Nothing Then
                Dim Msg As String = clsPluginLoader.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to load resource object, " & Msg)
                Return False
            End If
            If m_DebugLevel > 0 Then
                strMessage = "Loaded resourcer for StepTool " & StepToolName
                If clsPluginLoader.Message.Length > 0 Then strMessage &= ": " & clsPluginLoader.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
            End If
            m_Resource.Setup(m_MgrSettings, m_AnalysisTask)
            Return True

        End Function

        Private Function StatusFlagFileError(ByVal strWorkingDir As String) As Boolean

            Dim blnMgrCleanupSuccess As Boolean = False

            If DetectStatusFlagFile() Then

                Try
                    Dim objCleanupMgrErrors As New clsCleanupMgrErrors( _
                                        m_MgrSettings.GetParam("MgrCnfgDbConnectStr"), _
                                        m_MgrSettings.GetParam("MgrName"), _
                                        clsGlobal.AppFolderPath, _
                                        strWorkingDir)

                    blnMgrCleanupSuccess = objCleanupMgrErrors.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), m_DebugLevel)

                Catch ex As Exception

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error calling AutoCleanupManagerErrors, " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

                    blnMgrCleanupSuccess = False
                End Try

                If blnMgrCleanupSuccess Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Flag file found; automatically cleaned the work directory and deleted the flag file(s)")
                    ' No error; return false
                    Return False
                Else
                    ' Error removing flag file; return true
                    Return True
                End If

            End If

            ' No error; return false
            Return False

        End Function

        Private Function SetToolRunnerObject() As Boolean
            Dim strMessage As String
            Dim StepToolName As String = m_AnalysisTask.GetParam("StepTool")

            'TODO: May be able to get rid of cluster parameter
            Dim clustered As Boolean = CBoolSafe(m_MgrSettings.GetParam("cluster"))

            m_ToolRunner = clsPluginLoader.GetToolRunner(StepToolName.ToLower, clustered)
            If m_ToolRunner Is Nothing Then
                strMessage = "Unable to load tool runner for StepTool " & StepToolName & ": " & clsPluginLoader.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
                Return False
            End If
            If m_DebugLevel > 0 Then
                strMessage = "Loaded tool runner for StepTool " & m_AnalysisTask.GetCurrentJobToolDescription()
                If clsPluginLoader.Message.Length > 0 Then strMessage &= ": " & clsPluginLoader.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
            End If
            m_ToolRunner.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools)
            Return True

        End Function

        ''' <summary>
        ''' Reloads the manager settings from the manager control database 
        ''' if at least MinutesBetweenUpdates minutes have elapsed since the last update
        ''' </summary>
        ''' <param name="dtLastConfigDBUpdate"></param>
        ''' <param name="MinutesBetweenUpdates"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Function UpdateManagerSettings(ByRef dtLastConfigDBUpdate As System.DateTime, ByVal MinutesBetweenUpdates As Double) As Boolean

            Dim blnSuccess As Boolean = True

            If (System.DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates) Then

                dtLastConfigDBUpdate = System.DateTime.UtcNow

                If Not m_MgrSettings.LoadDBSettings() Then
                    Dim msg As String

                    If (String.IsNullOrEmpty(m_MgrSettings.ErrMsg)) Then
                        msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings"
                    Else
                        msg = m_MgrSettings.ErrMsg
                    End If

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)

                    blnSuccess = False
                Else
                    ' Need to synchronize some of the settings
                    UpdateStatusToolLoggingSettings(m_StatusTools)
                End If

            End If

            Return blnSuccess
           
        End Function

        Protected Sub UpdateStatusDisabled(ByVal ManagerStatus As IStatusFile.EnumMgrStatus, ByVal ManagerDisableMessage As String)
            Dim strErrorMessages() As String
            strErrorMessages = clsGlobal.DetermineRecentErrorMessages(m_MgrSettings, 5, m_MostRecentJobInfo)
            m_StatusTools.UpdateDisabled(ManagerStatus, ManagerDisableMessage, strErrorMessages, m_MostRecentJobInfo)
        End Sub

        Protected Sub UpdateStatusFlagFileExists()
            Dim strErrorMessages() As String
            strErrorMessages = clsGlobal.DetermineRecentErrorMessages(m_MgrSettings, 5, m_MostRecentJobInfo)
            m_StatusTools.UpdateFlagFileExists(strErrorMessages, m_MostRecentJobInfo)
        End Sub

        Protected Sub UpdateStatusIdle(ByVal ManagerIdleMessage As String)
            Dim strErrorMessages() As String
            strErrorMessages = clsGlobal.DetermineRecentErrorMessages(m_MgrSettings, 5, m_MostRecentJobInfo)

            m_StatusTools.UpdateIdle(ManagerIdleMessage, strErrorMessages, m_MostRecentJobInfo, True)
        End Sub

        Protected Sub UpdateClose(ByVal ManagerCloseMessage As String)
            Dim strErrorMessages() As String
            strErrorMessages = clsGlobal.DetermineRecentErrorMessages(m_MgrSettings, 5, m_MostRecentJobInfo)

            m_StatusTools.UpdateClose(ManagerCloseMessage, strErrorMessages, m_MostRecentJobInfo, True)
        End Sub

        Private Function ValidateWorkingDir() As Boolean

            'Verifies working directory is properly specified and is empty
            Dim WorkingDir As String = m_MgrSettings.GetParam("WorkDir")
            Dim MsgStr As String

            'Verify working directory is valid
            If Not Directory.Exists(WorkingDir) Then
                MsgStr = "Invalid working directory: " & WorkingDir
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MsgStr)
                Return False
            End If

            'Verify the working directory is empty
            Dim TmpFilArray() As String = Directory.GetFiles(WorkingDir)
            Dim TmpDirArray() As String = Directory.GetDirectories(WorkingDir)

            If (TmpDirArray.Length = 0) And (TmpFilArray.Length = 1) Then
                ' If the only file in the working directory is a JobParameters xml file,
                '  then try to delete it, since it's likely left over from a previous job that never actually started
                Dim strFileToCheck As String
                strFileToCheck = System.IO.Path.GetFileName(TmpFilArray(0))

                If strFileToCheck.StartsWith(clsGlobal.XML_FILENAME_PREFIX) AndAlso _
                   strFileToCheck.EndsWith(clsGlobal.XML_FILENAME_EXTENSION) Then
                    Try
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Working directory contains a stray JobParameters file, deleting it: " & TmpFilArray(0))

                        System.IO.File.Delete(TmpFilArray(0))

                        ' Wait 0.5 second and then refresh TmpFilArray
                        System.Threading.Thread.Sleep(500)

                        ' Now obtain a new listing of files
                        TmpFilArray = Directory.GetFiles(WorkingDir)
                    Catch ex As Exception
                        ' Deletion failed
                    End Try
                End If
            End If

            If (TmpDirArray.Length > 0) Or (TmpFilArray.Length > 0) Then
                MsgStr = "Working directory not empty: " & WorkingDir
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MsgStr)
                Return False
            End If

            'No problems found
            Return True

        End Function

        ''' <summary>
        ''' Initializes the status file writing tool
        ''' </summary>
        ''' <remarks></remarks>
        Private Sub InitStatusTools()

            If m_StatusTools Is Nothing Then
                Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)

                Dim StatusFileLoc As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("statusfilelocation"))

                m_StatusTools = New clsStatusFile(StatusFileLoc, m_DebugLevel)

                With m_StatusTools
                    .TaskStartTime = System.DateTime.UtcNow
                    .Dataset = ""
                    .JobNumber = 0
                    .JobStep = 0
                    .Tool = ""
                    .MgrName = m_MgrSettings.GetParam("MgrName")
                    .MgrStatus = IStatusFile.EnumMgrStatus.RUNNING
                    .TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
                    .TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
                End With

                UpdateStatusToolLoggingSettings(m_StatusTools)

            End If

        End Sub

        Private Sub UpdateStatusToolLoggingSettings(ByRef objStatusFile As clsStatusFile)

            Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)

            Dim LogMemoryUsage As Boolean = CBoolSafe(m_MgrSettings.GetParam("LogMemoryUsage"))
            Dim MinimumMemoryUsageLogInterval As Single = CSngSafe(m_MgrSettings.GetParam("MinimumMemoryUsageLogInterval"), 1)

            Dim LogStatusToBrokerDB As Boolean = CBoolSafe(m_MgrSettings.GetParam("LogStatusToBrokerDB"))
            Dim BrokerDBConnectionString As String = m_MgrSettings.GetParam("brokerconnectionstring")
            Dim BrokerDBStatusUpdateIntervalMinutes As Single = CSngSafe(m_MgrSettings.GetParam("BrokerDBStatusUpdateIntervalMinutes"), 60)

            Dim LogStatusToMessageQueue As Boolean = CBoolSafe(m_MgrSettings.GetParam("LogStatusToMessageQueue"))
            Dim MessageQueueURI As String = m_MgrSettings.GetParam("MessageQueueURI")
            Dim MessageQueueTopicMgrStatus As String = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus")

            Dim MgrName As String = m_MgrSettings.GetParam("MgrName")

            With objStatusFile
                .ConfigureMemoryLogging(LogMemoryUsage, MinimumMemoryUsageLogInterval, FInfo.DirectoryName)
                .ConfigureBrokerDBLogging(LogStatusToBrokerDB, BrokerDBConnectionString, BrokerDBStatusUpdateIntervalMinutes)
                .ConfigureMessageQueueLogging(LogStatusToMessageQueue, MessageQueueURI, MessageQueueTopicMgrStatus, MgrName)
            End With

        End Sub

        Private Sub RemoveTempFiles()
            Dim fiFilesToDelete() As System.IO.FileInfo
            Dim fiFilesToDeleteAddnl() As System.IO.FileInfo
            Dim intTargetIndex As Integer

            Dim fiFileInfo As System.IO.FileInfo

            Dim fiExeFilePath As FileInfo = New FileInfo(Application.ExecutablePath)
            Dim msg As String

            ' Files starting with the name IgnoreMe are created by log4NET when it is first instantiated 
            ' This name is defined in the RollingFileAppender section of the Logging.config file via this XML:
            ' <file value="IgnoreMe" />

            fiFilesToDelete = fiExeFilePath.Directory.GetFiles("IgnoreMe*.txt")
            If fiFilesToDelete.Length > 0 Then
                For Each fiFileInfo In fiFilesToDelete
                    Try
                        fiFileInfo.Delete()
                    Catch ex As Exception
                        msg = "Error deleting IgnoreMe file: " & fiFileInfo.Name
                        Console.WriteLine(msg & " : " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                    End Try
                Next
            End If

            ' Files named tmp.iso.#### and tmp.peak.#### (where #### are integers) are files created by Decon2LS
            ' These files indicate a previous, failed Decon2LS task and can be safely deleted
            ' For safety, we will not delete files less than 24 hours old

            fiFilesToDelete = fiExeFilePath.Directory.GetFiles("tmp.iso.*")
            fiFilesToDeleteAddnl = fiExeFilePath.Directory.GetFiles("tmp.peak.*")

            If fiFilesToDeleteAddnl.Length > 0 Then
                intTargetIndex = fiFilesToDelete.Length
                ReDim Preserve fiFilesToDelete(fiFilesToDelete.Length + fiFilesToDeleteAddnl.Length - 1)
                fiFilesToDeleteAddnl.CopyTo(fiFilesToDelete, intTargetIndex)
            End If

            If fiFilesToDelete.Length > 0 Then
                For Each fiFileInfo In fiFilesToDelete
                    Try
                        If System.DateTime.UtcNow.Subtract(fiFileInfo.LastWriteTimeUtc).TotalHours > 24 Then
                            fiFileInfo.Delete()
                        End If
                    Catch ex As Exception
                        msg = "Error deleting file: " & fiFileInfo.Name
                        Console.WriteLine(msg & " : " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                    End Try
                Next
            End If

        End Sub
#End Region

    End Class

End Namespace
