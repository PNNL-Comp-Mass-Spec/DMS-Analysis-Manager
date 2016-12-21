'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading
Imports System.Runtime.InteropServices
Imports System.Text.RegularExpressions

''' <summary>
''' Base class for analysis tool runner
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerBase
    Inherits clsAnalysisMgrBase
    Implements IToolRunner

#Region "Constants"
    Protected Const SP_NAME_SET_TASK_TOOL_VERSION As String = "SetStepTaskToolVersion"

    Public Const DATE_TIME_FORMAT As String = "yyyy-MM-dd hh:mm:ss tt"
    Public Const PVM_RESET_ERROR_MESSAGE As String = "Error resetting PVM"
#End Region

#Region "Module variables"
    'status tools
    Protected m_StatusTools As IStatusFile

    ' access to the job parameters
    Protected m_jobParams As IJobParams

    ' access to mgr parameters
    Protected m_mgrParams As IMgrParams

    ' access to settings file parameters
    Protected m_settingsFileParams As New PRISM.Files.XmlSettingsFileAccessor

    ' progress of run (in percent); This is a value between 0 and 100
    Protected m_progress As Single = 0

    '	status code
    Protected m_status As IStatusFile.EnumMgrStatus

    'DTA count for status report
    Protected m_DtaCount As Integer = 0

    Protected m_EvalCode As Integer = 0                         ' Can be used to pass codes regarding the results of this analysis back to the DMS_Pipeline DB
    Protected m_EvalMessage As String = String.Empty            ' Can be used to pass information regarding the results of this analysis back to the DMS_Pipeline DB        

    'Working directory, machine name (aka manager name), & job number (used frequently by subclasses)
    Protected m_WorkDir As String
    Protected m_MachName As String
    Protected m_JobNum As String
    Protected m_Dataset As String

    'Elapsed time information
    Protected m_StartTime As Date
    Protected m_StopTime As Date

    'Results folder name
    Protected m_ResFolderName As String

    'DLL file info
    Protected m_FileVersion As String
    Protected m_FileDate As String

    Protected m_IonicZipTools As clsIonicZipTools

    Protected m_NeedToAbortProcessing As Boolean

    Protected m_SummaryFile As clsSummaryFile

    Protected m_MyEMSLUtilities As clsMyEMSLUtilities

    Private m_LastProgressWriteTime As DateTime = Date.UtcNow
    Private m_LastProgressConsoleTime As DateTime = Date.UtcNow

    Private m_LastStatusFileUpdate As DateTime = Date.UtcNow

    Private mLastSortUtilityProgress As DateTime
    Private mSortUtilityErrorMessage As String

    Protected mProgRunnerStartTime As DateTime

    ''' <summary>
    ''' Queue tracking recent CPU values from an externally spawned process
    ''' </summary>
    ''' <remarks>Keys are the sampling date, value is the CPU usage (number of cores in use)</remarks>
    Protected ReadOnly mCoreUsageHistory As Queue(Of KeyValuePair(Of DateTime, Single))

#End Region

#Region "Properties"

    ''' <summary>
    ''' Evaluation code to be reported to the DMS_Pipeline DB
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property EvalCode As Integer Implements IToolRunner.EvalCode
        Get
            Return m_EvalCode
        End Get
    End Property

    ''' <summary>
    ''' Evaluation message to be reported to the DMS_Pipeline DB
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property EvalMessage As String Implements IToolRunner.EvalMessage
        Get
            Return m_EvalMessage
        End Get
    End Property

    ''' <summary>
    ''' Publicly accessible results folder name and path
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property ResFolderName() As String Implements IToolRunner.ResFolderName
        Get
            Return m_ResFolderName
        End Get
    End Property

    ''' <summary>
    ''' Explanation of what happened to last operation this class performed
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property Message() As String Implements IToolRunner.Message
        Get
            Return m_message
        End Get
    End Property

    Public ReadOnly Property NeedToAbortProcessing() As Boolean Implements IToolRunner.NeedToAbortProcessing
        Get
            Return m_NeedToAbortProcessing
        End Get
    End Property

    ' the state of completion of the job (as a percentage)
    Public ReadOnly Property Progress() As Single Implements IToolRunner.Progress
        Get
            Return m_progress
        End Get
    End Property
#End Region

#Region "Methods"

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        MyBase.New("clsAnalysisToolRunnerBase")
        mProgRunnerStartTime = Date.UtcNow
        mCoreUsageHistory = New Queue(Of KeyValuePair(Of DateTime, Single))
    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object holding manager parameters</param>
    ''' <param name="jobParams">Object holding job parameters</param>
    ''' <param name="statusTools">Object for status reporting</param>
    ''' <param name="summaryFile">Object for creating an analysis job summary file</param>
    ''' <param name="myEMSLUtilities">MyEMSL download Utilities</param>
    ''' <remarks></remarks>
    Public Overridable Sub Setup(
       mgrParams As IMgrParams,
       jobParams As IJobParams,
       statusTools As IStatusFile,
       summaryFile As clsSummaryFile,
       myEMSLUtilities As clsMyEMSLUtilities) Implements IToolRunner.Setup

        m_mgrParams = mgrParams
        m_jobParams = jobParams
        m_StatusTools = statusTools
        m_WorkDir = m_mgrParams.GetParam("workdir")
        m_MachName = m_mgrParams.GetParam("MgrName")
        m_JobNum = m_jobParams.GetParam("StepParameters", "Job")
        m_Dataset = m_jobParams.GetParam("JobParameters", "DatasetNum")

        If myEMSLUtilities Is Nothing Then
            m_MyEMSLUtilities = New clsMyEMSLUtilities(m_DebugLevel, m_WorkDir)
        Else
            m_MyEMSLUtilities = myEMSLUtilities
        End If

        AddHandler m_MyEMSLUtilities.ErrorEvent, AddressOf m_MyEMSLUtilities_ErrorEvent
        AddHandler m_MyEMSLUtilities.WarningEvent, AddressOf m_MyEMSLUtilities_WarningEvent

        m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel", 1))
        m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription()

        m_SummaryFile = summaryFile

        m_ResFolderName = m_jobParams.GetParam("OutputFolderName")

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.Setup()")
        End If

        m_IonicZipTools = New clsIonicZipTools(m_DebugLevel, m_WorkDir)

        MyBase.InitFileTools(m_MachName, m_DebugLevel)

        m_NeedToAbortProcessing = False

        m_message = String.Empty
        m_EvalCode = 0
        m_EvalMessage = String.Empty

    End Sub

    ''' <summary>
    ''' Calculates total run time for a job
    ''' </summary>
    ''' <param name="StartTime">Time job started</param>
    ''' <param name="StopTime">Time of job completion</param>
    ''' <returns>Total job run time (HH:MM)</returns>
    ''' <remarks></remarks>
    Protected Function CalcElapsedTime(startTime As DateTime, stopTime As DateTime) As String
        Dim dtElapsedTime As TimeSpan

        If stopTime < startTime Then
            ReportStatus("CalcElapsedTime: Stop time is less than StartTime; this is unexpected.  Assuming current time for StopTime")
            stopTime = Date.UtcNow
        End If

        If stopTime < startTime OrElse startTime = DateTime.MinValue Then
            Return String.Empty
        End If

        dtElapsedTime = stopTime.Subtract(startTime)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 "CalcElapsedTime: StartTime = " & startTime.ToString & "; Stoptime = " & stopTime.ToString)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 String.Format("CalcElapsedTime: {0} Hours, {1} Minutes, {2} Seconds",
                                 dtElapsedTime.Hours, dtElapsedTime.Minutes, dtElapsedTime.Seconds))
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 "CalcElapsedTime: TotalMinutes = " & dtElapsedTime.TotalMinutes.ToString("0.00"))
        End If

        Return dtElapsedTime.Hours.ToString("###0") & ":" & dtElapsedTime.Minutes.ToString("00") & ":" & dtElapsedTime.Seconds.ToString("00")

    End Function

    ''' <summary>
    ''' Computes the incremental progress that has been made beyond currentTaskProgressAtStart, based on the number of items processed and the next overall progress level
    ''' </summary>
    ''' <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
    ''' <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
    ''' <param name="subTaskProgress">Progress of the current subtask (value between 0 and 100)</param>
    ''' <returns>Overall progress (value between 0 and 100)</returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeIncrementalProgress(currentTaskProgressAtStart As Single, currentTaskProgressAtEnd As Single, subTaskProgress As Single) As Single
        If subTaskProgress < 0 Then
            Return currentTaskProgressAtStart
        ElseIf subTaskProgress >= 100 Then
            Return currentTaskProgressAtEnd
        Else
            Return CSng(currentTaskProgressAtStart + (subTaskProgress / 100.0) * (currentTaskProgressAtEnd - currentTaskProgressAtStart))
        End If
    End Function

    ''' <summary>
    ''' Computes the incremental progress that has been made beyond currentTaskProgressAtStart, based on the number of items processed and the next overall progress level
    ''' </summary>
    ''' <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
    ''' <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
    ''' <param name="currentTaskItemsProcessed">Number of items processed so far during this subtask</param>
    ''' <param name="currentTaskTotalItems">Total number of items to process during this subtask</param>
    ''' <returns>Overall progress (value between 0 and 100)</returns>
    ''' <remarks></remarks>
    Public Shared Function ComputeIncrementalProgress(currentTaskProgressAtStart As Single, currentTaskProgressAtEnd As Single, currentTaskItemsProcessed As Integer, currentTaskTotalItems As Integer) As Single
        If currentTaskTotalItems < 1 Then
            Return currentTaskProgressAtStart
        ElseIf currentTaskItemsProcessed > currentTaskTotalItems Then
            Return currentTaskProgressAtEnd
        Else
            Return CSng(currentTaskProgressAtStart + (currentTaskItemsProcessed / currentTaskTotalItems) * (currentTaskProgressAtEnd - currentTaskProgressAtStart))
        End If
    End Function

    ''' <summary>
    ''' Computes the maximum threads to allow given the number of cores on the machine and 
    ''' the the amount of memory that each thread is allowed to reserve
    ''' </summary>
    ''' <param name="memorySizeMBPerThread">Amount of memory allocated to each thread</param>
    ''' <returns>Maximum number of cores to use</returns>
    ''' <remarks></remarks>
    Protected Function ComputeMaxThreadsGivenMemoryPerThread(memorySizeMBPerThread As Single) As Integer

        If memorySizeMBPerThread < 512 Then memorySizeMBPerThread = 512

        Dim maxThreadsToAllow = PRISM.Processes.clsProgRunner.GetCoreCount()

        Dim freeMemoryMB = m_StatusTools.GetFreeMemoryMB()

        Dim maxThreadsBasedOnMemory = freeMemoryMB / memorySizeMBPerThread

        ' Round up maxThreadsBasedOnMemory only if it is within 0.2 of the next highest integer
        Dim maxThreadsRoundedUp = CInt(Math.Ceiling(maxThreadsBasedOnMemory))
        If maxThreadsRoundedUp - maxThreadsBasedOnMemory <= 0.2 Then
            maxThreadsBasedOnMemory = maxThreadsRoundedUp
        Else
            maxThreadsBasedOnMemory = maxThreadsRoundedUp - 1
        End If

        If maxThreadsBasedOnMemory < maxThreadsToAllow Then
            maxThreadsToAllow = CInt(Math.Round(maxThreadsBasedOnMemory))
        End If

        Return maxThreadsToAllow

    End Function

    ''' <summary>
    ''' Copies a file (typically a mzXML or mzML file) to a server cache folder
    ''' Will store the file in a subfolder based on job parameter OutputFolderName, and below that, in a folder with a name like 2013_2
    ''' </summary>
    ''' <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
    ''' <param name="sourceFilePath">Path to the data file</param>
    ''' <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
    ''' <returns>Path to the remotely cached file; empty path if an error</returns>
    Protected Function CopyFileToServerCache(cacheFolderPath As String, sourceFilePath As String, purgeOldFilesIfNeeded As Boolean) As String

        Try
            ' m_ResFolderName should contain the output folder; e.g. MSXML_Gen_1_120_275966			
            If String.IsNullOrEmpty(m_ResFolderName) Then
                LogError("m_ResFolderName (from job parameter OutputFolderName) is empty; cannot construct MSXmlCache path")
                Return String.Empty
            End If

            ' Remove the dataset ID portion from the output folder
            Dim toolNameVersionFolder As String
            Try
                toolNameVersionFolder = clsAnalysisResources.GetMSXmlToolNameVersionFolder(m_ResFolderName)
            Catch ex As Exception
                LogError("OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" & m_ResFolderName & "); cannot construct MSXmlCache path")
                Return String.Empty
            End Try

            ' Determine the year_quarter text for this dataset
            Dim strDatasetStoragePath As String = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
            If String.IsNullOrEmpty(strDatasetStoragePath) Then strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")

            Dim strDatasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(strDatasetStoragePath)
            If String.IsNullOrEmpty(strDatasetYearQuarter) Then
                LogError("Unable to determine DatasetYearQuarter using the DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path")
                Return String.Empty
            End If

            Dim remoteCacheFilePath = String.Empty

            Dim success = CopyFileToServerCache(
              cacheFolderPath,
              toolNameVersionFolder,
              sourceFilePath,
              strDatasetYearQuarter,
              purgeOldFilesIfNeeded:=purgeOldFilesIfNeeded,
              remoteCacheFilePath:=remoteCacheFilePath)

            If Not success Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("CopyFileToServerCache returned false copying the " & Path.GetExtension(sourceFilePath) & " file to " & Path.Combine(cacheFolderPath, toolNameVersionFolder))
                    Return String.Empty
                End If
            End If

            Return remoteCacheFilePath

        Catch ex As Exception
            LogError("Exception in CopyFileToServerCache", ex)
            Return String.Empty
        End Try

    End Function

    ''' <summary>
    ''' Copies a file (typically a mzXML or mzML file) to a server cache folder
    ''' Will store the file in the subfolder strSubfolderInTarget and, below that, in a folder with a name like 2013_2
    ''' </summary>
    ''' <param name="strCacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
    ''' <param name="strSubfolderInTarget">Subfolder name to create below strCacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
    ''' <param name="strsourceFilePath">Path to the data file</param>
    ''' <param name="strDatasetYearQuarter">Dataset year quarter text (optional); example value is 2013_2; if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
    ''' <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>
    ''' Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
    ''' If those parameters are not defined, then copies the file anyway
    ''' </remarks>
    Protected Function CopyFileToServerCache(
      strCacheFolderPath As String,
      strSubfolderInTarget As String,
      strsourceFilePath As String,
      strDatasetYearQuarter As String,
      blnPurgeOldFilesIfNeeded As Boolean) As Boolean

        Return CopyFileToServerCache(strCacheFolderPath, strSubfolderInTarget, strsourceFilePath, strDatasetYearQuarter, blnPurgeOldFilesIfNeeded, String.Empty)

    End Function

    ''' <summary>
    ''' Copies a file (typically a mzXML or mzML file) to a server cache folder
    ''' Will store the file in the subfolder strSubfolderInTarget and, below that, in a folder with a name like 2013_2
    ''' </summary>
    ''' <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
    ''' <param name="subfolderInTarget">Subfolder name to create below strCacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
    ''' <param name="sourceFilePath">Path to the data file</param>
    ''' <param name="datasetYearQuarter">Dataset year quarter text (optional); example value is 2013_2; if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
    ''' <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
    ''' <param name="remoteCacheFilePath">Output parameter: the target file path (determined by this function)</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>
    ''' Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
    ''' If those parameters are not defined, then copies the file anyway
    ''' </remarks>
    Protected Function CopyFileToServerCache(
      cacheFolderPath As String,
      subfolderInTarget As String,
      sourceFilePath As String,
      datasetYearQuarter As String,
      purgeOldFilesIfNeeded As Boolean,
      <Out()> ByRef remoteCacheFilePath As String) As Boolean

        Dim blnSuccess As Boolean
        remoteCacheFilePath = String.Empty

        Try

            Dim diCacheFolder = New DirectoryInfo(cacheFolderPath)

            If Not diCacheFolder.Exists Then
                LogWarning("Cache folder not found: " & cacheFolderPath)
                Return False
            End If

            Dim ditargetDirectory As DirectoryInfo

            ' Define the target folder
            If String.IsNullOrEmpty(subfolderInTarget) Then
                ditargetDirectory = diCacheFolder
            Else
                ditargetDirectory = New DirectoryInfo(Path.Combine(diCacheFolder.FullName, subfolderInTarget))
                If Not ditargetDirectory.Exists Then ditargetDirectory.Create()
            End If

            If String.IsNullOrEmpty(datasetYearQuarter) Then
                ' Determine the year_quarter text for this dataset
                Dim strDatasetStoragePath As String = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
                If String.IsNullOrEmpty(strDatasetStoragePath) Then strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")

                datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(strDatasetStoragePath)
            End If

            If Not String.IsNullOrEmpty(datasetYearQuarter) Then
                ditargetDirectory = New DirectoryInfo(Path.Combine(ditargetDirectory.FullName, datasetYearQuarter))
                If Not ditargetDirectory.Exists Then ditargetDirectory.Create()
            End If

            m_jobParams.AddResultFileExtensionToSkip(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX)

            ' Create the .hashcheck file
            Dim strHashcheckFilePath As String
            strHashcheckFilePath = clsGlobal.CreateHashcheckFile(sourceFilePath, blnComputeMD5Hash:=True)

            If String.IsNullOrEmpty(strHashcheckFilePath) Then
                LogError("Error in CopyFileToServerCache: Hashcheck file was not created")
                Return False
            End If

            Dim fiTargetFile = New FileInfo(Path.Combine(ditargetDirectory.FullName, Path.GetFileName(sourceFilePath)))

            ResetTimestampForQueueWaitTimeLogging()
            blnSuccess = m_FileTools.CopyFileUsingLocks(sourceFilePath, fiTargetFile.FullName, m_MachName, True)

            If Not blnSuccess Then
                m_message = "CopyFileUsingLocks returned false copying " & Path.GetFileName(sourceFilePath) & " to " & fiTargetFile.FullName
                Return False
            End If

            remoteCacheFilePath = fiTargetFile.FullName

            ' Copy over the .Hashcheck file
            m_FileTools.CopyFile(strHashcheckFilePath, Path.Combine(fiTargetFile.DirectoryName, Path.GetFileName(strHashcheckFilePath)), True)

            If purgeOldFilesIfNeeded Then
                PurgeOldServerCacheFiles(cacheFolderPath)
            End If

        Catch ex As Exception
            LogError("Error in CopyFileToServerCache", ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Copies the .mzXML file to the generic MSXML_Cache folder, e.g. \\proto-6\MSXML_Cache\MSConvert
    ''' </summary>
    ''' <param name="strsourceFilePath"></param>
    ''' <param name="strDatasetYearQuarter">Dataset year quarter text, e.g. 2013_2;  if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
    ''' <param name="strMSXmlGeneratorName">Name of the MzXML generator, e.g. MSConvert</param>
    ''' <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>
    ''' Contrast with CopyMSXmlToCache in clsAnalysisToolRunnerMSXMLGen, where the target folder is 
    ''' of the form \\proto-6\MSXML_Cache\MSConvert\MSXML_Gen_1_93
    ''' </remarks>
    Protected Function CopyMzXMLFileToServerCache(
      strsourceFilePath As String,
      strDatasetYearQuarter As String,
      strMSXmlGeneratorName As String,
      blnPurgeOldFilesIfNeeded As Boolean) As Boolean

        Dim blnSuccess As Boolean

        Try

            Dim strMSXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)

            If String.IsNullOrEmpty(strMSXmlGeneratorName) Then
                strMSXmlGeneratorName = m_jobParams.GetJobParameter("MSXMLGenerator", String.Empty)

                If Not String.IsNullOrEmpty(strMSXmlGeneratorName) Then
                    strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(strMSXmlGeneratorName)
                End If
            End If

            blnSuccess = CopyFileToServerCache(strMSXMLCacheFolderPath, strMSXmlGeneratorName, strsourceFilePath, strDatasetYearQuarter, blnPurgeOldFilesIfNeeded)

        Catch ex As Exception
            LogError("Error in CopyMzXMLFileToServerCache", ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Copies the files from the results folder to the transfer folder on the server
    ''' </summary>
    ''' <returns>CloseOutType.CLOSEOUT_SUCCESS on success</returns>
    ''' <remarks></remarks>
    Protected Function CopyResultsFolderToServer() As IJobParams.CloseOutType

        Dim transferFolderPath = GetTransferFolderPath()

        If String.IsNullOrEmpty(transferFolderPath) Then
            ' Error has already geen logged and m_message has been updated
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return CopyResultsFolderToServer(transferFolderPath)
    End Function

    ''' <summary>
    ''' Copies the files from the results folder to the transfer folder on the server
    ''' </summary>
    ''' <param name="transferFolderPath">Base transfer folder path to use
    ''' e.g. \\proto-6\DMS3_Xfer\ or 
    ''' \\protoapps\PeptideAtlas_Staging\1000_DataPackageName</param>
    ''' <returns>CloseOutType.CLOSEOUT_SUCCESS on success</returns>
    ''' <remarks></remarks>
    Protected Function CopyResultsFolderToServer(transferFolderPath As String) As IJobParams.CloseOutType

        Dim sourceFolderPath As String = String.Empty
        Dim targetDirectoryPath As String

        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)

        Dim strMessage As String
        Dim blnErrorEncountered = False
        Dim intFailedFileCount = 0


        Const intRetryCount = 10
        Const intRetryHoldoffSeconds = 15
        Const blnIncreaseHoldoffOnEachRetry = True

        Try

            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.DELIVERING_RESULTS, 0)

            If String.IsNullOrEmpty(m_ResFolderName) Then
                ' Log this error to the database (the logger will also update the local log file)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR,
                                     "Results folder name is not defined, job " & m_jobParams.GetParam("StepParameters", "Job"))

                ' Also display at console
                ReportStatus("Results folder not defined (job parameter OutputFolderName)", 10, True)

                ' Without a source folder; there isn't much we can do
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            sourceFolderPath = Path.Combine(m_WorkDir, m_ResFolderName)

            ' Verify the source folder exists
            If Not Directory.Exists(sourceFolderPath) Then
                ' Log this error to the database
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR,
                                     "Results folder not found, job " & m_jobParams.GetParam("StepParameters", "Job") & ", folder " & sourceFolderPath)

                ' Also display at console
                ReportStatus("Results folder not found", 10, True)

                ' Without a source folder; there isn't much we can do
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the remote transfer folder path (create it if missing)
            targetDirectoryPath = CreateRemoteTransferFolder(objAnalysisResults, transferFolderPath)
            If String.IsNullOrEmpty(targetDirectoryPath) Then
                objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            LogError("Error creating results folder in transfer directory", ex)
            m_message = clsGlobal.AppendToComment(m_message, "Error creating dataset folder in transfer directory")
            If Not String.IsNullOrEmpty(sourceFolderPath) Then
                objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath)
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Copy results folder to xfer folder
        ' Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)
        Try

            ' Copy all of the files and subdirectories in the local result folder to the target folder
            Dim eResult As IJobParams.CloseOutType

            ' Copy the files and subfolders
            eResult = CopyResultsFolderRecursive(sourceFolderPath, sourceFolderPath, targetDirectoryPath,
              objAnalysisResults, blnErrorEncountered, intFailedFileCount,
              intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

        Catch ex As Exception
            LogError("Error copying results folder to " & Path.GetPathRoot(targetDirectoryPath), ex)
            m_message = clsGlobal.AppendToComment(m_message, "Error copying results folder to " & Path.GetPathRoot(targetDirectoryPath))
            blnErrorEncountered = True
        End Try

        If blnErrorEncountered Then
            strMessage = "Error copying " & intFailedFileCount.ToString & " file"
            If intFailedFileCount <> 1 Then
                strMessage &= "s"
            End If
            strMessage &= " to transfer folder"
            m_message = clsGlobal.AppendToComment(m_message, strMessage)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

    End Function

    ''' <summary>
    ''' Copies each of the files in the source folder to the target folder
    ''' Uses CopyFileWithRetry to retry the copy up to intRetryCount times
    ''' </summary>
    ''' <param name="SourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <remarks></remarks>
    Private Function CopyResultsFolderRecursive(
      RootSourceFolderPath As String, SourceFolderPath As String, targetDirectoryPath As String,
      objAnalysisResults As clsAnalysisResults,
      ByRef blnErrorEncountered As Boolean,
      ByRef intFailedFileCount As Integer,
      intRetryCount As Integer,
      intRetryHoldoffSeconds As Integer,
      blnIncreaseHoldoffOnEachRetry As Boolean) As IJobParams.CloseOutType

        Dim objSourceFolderInfo As DirectoryInfo
        Dim objSourceFile As FileInfo
        Dim objTargetFile As FileInfo

        Dim htFilesToOverwrite As Hashtable

        Dim ResultFiles() As String
        Dim strSourceFileName As String
        Dim strTargetPath As String

        Dim strMessage As String

        Try
            htFilesToOverwrite = New Hashtable
            htFilesToOverwrite.Clear()

            If objAnalysisResults.FolderExistsWithRetry(targetDirectoryPath) Then
                ' The target folder already exists

                ' Examine the files in the results folder to see if any of the files already exist in the transfer folder
                ' If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)
                ' However, if file sizes differ, then replace the file

                objSourceFolderInfo = New DirectoryInfo(SourceFolderPath)
                For Each objSourceFile In objSourceFolderInfo.GetFiles()
                    If File.Exists(Path.Combine(targetDirectoryPath, objSourceFile.Name)) Then
                        objTargetFile = New FileInfo(Path.Combine(targetDirectoryPath, objSourceFile.Name))

                        If objSourceFile.Length <> objTargetFile.Length OrElse objSourceFile.LastWriteTimeUtc > objTargetFile.LastWriteTimeUtc Then
                            strMessage = "File in transfer folder on server will be overwritten by newer file in results folder: " & objSourceFile.Name & "; new file date (UTC): " & objSourceFile.LastWriteTimeUtc.ToString() & "; old file date (UTC): " & objTargetFile.LastWriteTimeUtc.ToString()

                            If objSourceFile.Name <> clsAnalysisJob.JobParametersFilename(m_JobNum) Then
                                LogWarning(strMessage)
                            End If

                            htFilesToOverwrite.Add(objSourceFile.Name.ToLower, 1)
                        End If
                    End If
                Next
            Else
                ' Need to create the target folder
                Try
                    objAnalysisResults.CreateFolderWithRetry(targetDirectoryPath)
                Catch ex As Exception
                    LogError("Error creating results folder in transfer directory, " & Path.GetPathRoot(targetDirectoryPath), ex)
                    m_message = clsGlobal.AppendToComment(m_message, "Error creating results folder in transfer directory, " & Path.GetPathRoot(targetDirectoryPath))
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End Try
            End If

        Catch ex As Exception
            LogError("Error comparing files in source folder to " & targetDirectoryPath, ex)
            m_message = clsGlobal.AppendToComment(m_message, "Error comparing files in source folder to transfer directory")
            objAnalysisResults.CopyFailedResultsToArchiveFolder(RootSourceFolderPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Note: Entries in ResultFiles will have full file paths, not just file names
        ResultFiles = Directory.GetFiles(SourceFolderPath, "*")

        For Each FileToCopy As String In ResultFiles
            strSourceFileName = Path.GetFileName(FileToCopy)
            strTargetPath = Path.Combine(targetDirectoryPath, strSourceFileName)

            Try
                If htFilesToOverwrite.Count > 0 AndAlso htFilesToOverwrite.Contains(strSourceFileName.ToLower) Then
                    ' Copy file and overwrite existing
                    objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True,
                     intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
                Else
                    ' Copy file only if it doesn't currently exist
                    If Not File.Exists(strTargetPath) Then
                        objAnalysisResults.CopyFileWithRetry(FileToCopy, strTargetPath, True,
                         intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
                    End If
                End If
            Catch ex As Exception
                ' Continue copying files; we'll fail the results at the end of this function
                LogError(" CopyResultsFolderToServer: error copying " & Path.GetFileName(FileToCopy) & " to " & strTargetPath, ex)
                blnErrorEncountered = True
                intFailedFileCount += 1
            End Try
        Next

        ' Recursively call this function for each subfolder
        ' If any of the subfolders have an error, we'll continue copying, but will set blnErrorEncountered to True
        Dim eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Dim diSourceFolder As DirectoryInfo
        Dim strtargetDirectoryPathCurrent As String
        diSourceFolder = New DirectoryInfo(SourceFolderPath)

        For Each objSubFolder As DirectoryInfo In diSourceFolder.GetDirectories()
            strtargetDirectoryPathCurrent = Path.Combine(targetDirectoryPath, objSubFolder.Name)

            eResult = CopyResultsFolderRecursive(RootSourceFolderPath, objSubFolder.FullName, strtargetDirectoryPathCurrent,
             objAnalysisResults, blnErrorEncountered, intFailedFileCount,
             intRetryCount, intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then blnErrorEncountered = True

        Next

        Return eResult

    End Function

    ''' <summary>
    ''' Determines the path to the remote transfer folder
    ''' Creates the folder if it does not exist
    ''' </summary>
    ''' <returns>The full path to the remote transfer folder; an empty string if an error</returns>
    ''' <remarks></remarks>
    Protected Function CreateRemoteTransferFolder(objAnalysisResults As clsAnalysisResults) As String

        Dim transferFolderPath = m_jobParams.GetParam("transferFolderPath")

        ' Verify transfer directory exists
        ' First make sure TransferFolderPath is defined
        If String.IsNullOrEmpty(transferFolderPath) Then
            ReportStatus("Transfer folder path not defined; job param 'transferFolderPath' is empty", 0, True)
            m_message = clsGlobal.AppendToComment(m_message, "Transfer folder path not defined")
            Return String.Empty
        End If

        Return CreateRemoteTransferFolder(objAnalysisResults, transferFolderPath)

    End Function

    ''' <summary>
    ''' Determines the path to the remote transfer folder
    ''' Creates the folder if it does not exist
    ''' </summary>
    ''' <param name="objAnalysisResults">Analysis results object</param>
    ''' <param name="transferFolderPath">Base transfer folder path, e.g. \\proto-11\DMS3_Xfer\</param>
    ''' <returns>The full path to the remote transfer folder; an empty string if an error</returns>
    Protected Function CreateRemoteTransferFolder(objAnalysisResults As clsAnalysisResults, transferFolderPath As String) As String

        If String.IsNullOrEmpty(m_ResFolderName) Then
            LogError("Results folder name is not defined, job " & m_jobParams.GetParam("StepParameters", "Job"))
            m_message = "Results folder job parameter not defined (OutputFolderName)"
            Return String.Empty
        End If

        ' Now verify transfer directory exists
        Try
            objAnalysisResults.FolderExistsWithRetry(transferFolderPath)
        Catch ex As Exception
            LogError("Error verifying transfer directory, " & Path.GetPathRoot(transferFolderPath), ex)
            Return String.Empty
        End Try

        ' Determine if dataset folder in transfer directory already exists; make directory if it doesn't exist
        ' First make sure "DatasetFolderName" or "DatasetNum" is defined
        If String.IsNullOrEmpty(m_Dataset) Then
            LogError("Dataset name is undefined, job " & m_jobParams.GetParam("StepParameters", "Job"))
            m_message = "Dataset name is undefined"
            Return String.Empty
        End If

        Dim strRemoteTransferFolderPath As String

        If clsGlobal.IsMatch(m_Dataset, "Aggregation") Then
            ' Do not append "Aggregation" to the path since this is a generic dataset name applied to jobs that use Data Packages
            strRemoteTransferFolderPath = String.Copy(transferFolderPath)
        Else
            ' Append the dataset folder name to the transfer folder path
            Dim datasetFolderName = m_jobParams.GetParam("StepParameters", "DatasetFolderName")
            If String.IsNullOrWhiteSpace(datasetFolderName) Then datasetFolderName = m_Dataset
            strRemoteTransferFolderPath = Path.Combine(transferFolderPath, datasetFolderName)
        End If

        ' Create the target folder if it doesn't exist
        Try
            objAnalysisResults.CreateFolderWithRetry(strRemoteTransferFolderPath, MaxRetryCount:=5, RetryHoldoffSeconds:=20, blnIncreaseHoldoffOnEachRetry:=True)
        Catch ex As Exception
            LogError("Error creating dataset folder in transfer directory, " & Path.GetPathRoot(strRemoteTransferFolderPath), ex)
            Return String.Empty
        End Try

        ' Now append the output folder name to strRemoteTransferFolderPath
        strRemoteTransferFolderPath = Path.Combine(strRemoteTransferFolderPath, m_ResFolderName)

        Return strRemoteTransferFolderPath

    End Function

    ''' <summary>
    ''' Makes up to 3 attempts to delete specified file
    ''' </summary>
    ''' <param name="FileNamePath">Full path to file for deletion</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Raises exception if error occurs</remarks>
    Public Function DeleteFileWithRetries(FileNamePath As String) As Boolean
        Return DeleteFileWithRetries(FileNamePath, m_DebugLevel, 3)
    End Function

    ''' <summary>
    ''' Makes up to 3 attempts to delete specified file
    ''' </summary>
    ''' <param name="FileNamePath">Full path to file for deletion</param>
    ''' <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Raises exception if error occurs</remarks>
    Public Shared Function DeleteFileWithRetries(FileNamePath As String, intDebugLevel As Integer) As Boolean
        Return DeleteFileWithRetries(FileNamePath, intDebugLevel, 3)
    End Function

    ''' <summary>
    ''' Makes multiple tries to delete specified file
    ''' </summary>
    ''' <param name="FileNamePath">Full path to file for deletion</param>
    ''' <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
    ''' <param name="MaxRetryCount">Maximum number of deletion attempts</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Raises exception if error occurs</remarks>
    Public Shared Function DeleteFileWithRetries(FileNamePath As String, intDebugLevel As Integer, MaxRetryCount As Integer) As Boolean

        Dim RetryCount = 0
        Dim ErrType As AMFileNotDeletedAfterRetryException.RetryExceptionType

        If intDebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method")
        End If

        'Verify specified file exists
        If Not File.Exists(FileNamePath) Then
            'Throw an exception
            Throw New AMFileNotFoundException(FileNamePath, "Specified file not found")
        End If

        While RetryCount < MaxRetryCount
            Try
                File.Delete(FileNamePath)
                If intDebugLevel > 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit")
                End If
                Return True

            Catch Err1 As UnauthorizedAccessException
                'File may be read-only. Clear read-only flag and try again
                If intDebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR1: " & Err1.Message)
                    If Not Err1.InnerException Is Nothing Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err1.InnerException.Message)
                    End If
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " may be read-only, attribute reset attempt #" & RetryCount.ToString)
                End If
                File.SetAttributes(FileNamePath, File.GetAttributes(FileNamePath) And (Not FileAttributes.ReadOnly))
                ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception
                RetryCount += 1

            Catch Err2 As IOException
                'If problem is locked file, attempt to fix lock and retry
                If intDebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "File " & FileNamePath & " exception ERR2: " & Err2.Message)
                    If Not Err2.InnerException Is Nothing Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inner exception: " & Err2.InnerException.Message)
                    End If
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Error deleting file " & FileNamePath & ", attempt #" & RetryCount.ToString)
                End If
                ErrType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception

                'Delay 2 seconds
                Thread.Sleep(2000)

                'Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd 
                PRISM.Processes.clsProgRunner.GarbageCollectNow()
                RetryCount += 1

            Catch Err3 As Exception
                Dim msg = "Error deleting file, exception ERR3 " & FileNamePath & Err3.Message
                Console.WriteLine(msg)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg)
                Throw New AMFileNotDeletedException(FileNamePath, Err3.Message)
            End Try
        End While

        'If we got to here, then we've exceeded the max retry limit
        Throw New AMFileNotDeletedAfterRetryException(FileNamePath, ErrType, "Unable to delete or move file after multiple retries")

    End Function

    Protected Function DeleteRawDataFiles() As IJobParams.CloseOutType
        Dim RawDataType As String
        RawDataType = m_jobParams.GetParam("RawDataType")

        Return DeleteRawDataFiles(RawDataType)
    End Function

    Protected Function DeleteRawDataFiles(RawDataType As String) As IJobParams.CloseOutType
        Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants
        eRawDataType = clsAnalysisResources.GetRawDataType(RawDataType)

        Return DeleteRawDataFiles(eRawDataType)
    End Function

    Protected Function DeleteRawDataFiles(eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As IJobParams.CloseOutType

        'Deletes the raw data files/folders from the working directory
        Dim IsFile As Boolean
        Dim IsNetworkDir = False
        Dim FileOrFolderName As String = String.Empty

        Select Case eRawDataType
            Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
                IsFile = True

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_WIFF_EXTENSION)
                IsFile = True

            Case clsAnalysisResources.eRawDataTypeConstants.UIMF
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_UIMF_EXTENSION)
                IsFile = True

            Case clsAnalysisResources.eRawDataTypeConstants.mzXML
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
                IsFile = True

            Case clsAnalysisResources.eRawDataTypeConstants.mzML
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)
                IsFile = True

            Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders

                Dim NewSourceFolder As String = clsAnalysisResources.ResolveSerStoragePath(m_WorkDir)
                'Check for "0.ser" folder
                If String.IsNullOrEmpty(NewSourceFolder) Then
                    FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
                    IsNetworkDir = False
                Else
                    IsNetworkDir = True
                End If

                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder
                ' Bruker_FT folders are actually .D folders
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot
                ''''''''''''''''''''''''''''''''''''
                ' TODO: Finalize this code
                '       DMS doesn't yet have a BrukerTOF dataset 
                '        so we don't know the official folder structure
                ''''''''''''''''''''''''''''''''''''

                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging

                ''''''''''''''''''''''''''''''''''''
                ' TODO: Finalize this code
                '       DMS doesn't yet have a BrukerTOF dataset 
                '        so we don't know the official folder structure
                ''''''''''''''''''''''''''''''''''''

                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset)
                IsFile = False

            Case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf

                ' BrukerTOFBaf folders are actually .D folders
                FileOrFolderName = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_D_EXTENSION)
                IsFile = False

            Case Else
                'Should never get this value
                m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " & eRawDataType.ToString()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Select


        If IsFile Then
            'Data is a file, so use file deletion tools
            Try
                If Not File.Exists(FileOrFolderName) Then
                    ' File not found; treat this as a success
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                End If

                ' DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
                ' Thus, need to wrap it with an Exception handler

                If DeleteFileWithRetries(FileOrFolderName) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                Else
                    LogError("Error deleting raw data file " & FileOrFolderName)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            Catch ex As Exception
                LogError("Exception deleting raw data file " & FileOrFolderName, ex)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
        ElseIf IsNetworkDir Then
            'The files were on the network and do not need to be deleted

        Else
            'Use folder deletion tools
            Try
                If Directory.Exists(FileOrFolderName) Then
                    Directory.Delete(FileOrFolderName, True)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Catch ex As Exception
                LogError("Exception deleting raw data folder " & FileOrFolderName, ex)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Sub DeleteTemporaryfile(strFilePath As String)

        Try
            If File.Exists(strFilePath) Then
                File.Delete(strFilePath)
            End If
        Catch ex As Exception
            ReportStatus("Exception deleting temporary file " & strFilePath, 0, True)
        End Try

    End Sub

    ''' <summary>
    ''' Determine the path to the correct version of the step tool
    ''' </summary>
    ''' <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
    ''' <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
    ''' <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
    ''' <returns>The path to the program, or an empty string if there is a problem</returns>
    ''' <remarks></remarks>
    Protected Function DetermineProgramLocation(
      strStepToolName As String,
      strProgLocManagerParamName As String,
      strExeName As String) As String

        ' Check whether the settings file specifies that a specific version of the step tool be used
        Dim strStepToolVersion As String = m_jobParams.GetParam(strStepToolName & "_Version")

        Return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strStepToolVersion)

    End Function

    ''' <summary>
    ''' Determine the path to the correct version of the step tool
    ''' </summary>
    ''' <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
    ''' <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
    ''' <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
    ''' <param name="strStepToolVersion">Specific step tool version to use (will be the name of a subfolder located below the primary ProgLoc location)</param>
    ''' <returns>The path to the program, or an empty string if there is a problem</returns>
    ''' <remarks></remarks>
    Protected Function DetermineProgramLocation(
       strStepToolName As String,
       strProgLocManagerParamName As String,
       strExeName As String,
       strStepToolVersion As String) As String

        Return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strStepToolVersion, m_mgrParams, m_message)
    End Function

    ''' <summary>
    ''' Determine the path to the correct version of the step tool
    ''' </summary>
    ''' <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
    ''' <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
    ''' <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
    ''' <param name="strStepToolVersion">Specific step tool version to use (will be the name of a subfolder located below the primary ProgLoc location)</param>
    ''' <param name="mgrParams">Manager parameters</param>
    ''' <param name="errorMessage">Output: error message</param>
    ''' <returns>The path to the program, or an empty string if there is a problem</returns>
    ''' <remarks></remarks>
    Public Shared Function DetermineProgramLocation(
      strStepToolName As String,
      strProgLocManagerParamName As String,
      strExeName As String,
      strStepToolVersion As String,
      mgrParams As IMgrParams,
      <Out> ByRef errorMessage As String) As String

        errorMessage = String.Empty

        ' Lookup the path to the folder that contains the Step tool
        Dim progLoc As String = mgrParams.GetParam(strProgLocManagerParamName)

        If String.IsNullOrWhiteSpace(progLoc) Then
            errorMessage = "Manager parameter " & strProgLocManagerParamName & " is not defined in the Manager Control DB"
            Console.WriteLine(errorMessage)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Return String.Empty
        End If

        ' Check whether the settings file specifies that a specific version of the step tool be used
        If Not String.IsNullOrWhiteSpace(strStepToolVersion) Then

            ' Specific version is defined; verify that the folder exists
            progLoc = Path.Combine(progLoc, strStepToolVersion)

            If Not Directory.Exists(progLoc) Then
                errorMessage = "Version-specific folder not found for " & strStepToolName
                Console.WriteLine(errorMessage)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage & ": " & progLoc)
                Return String.Empty
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Using specific version of " & strStepToolName & ": " & progLoc)
            End If
        End If

        ' Define the path to the .Exe, then verify that it exists
        progLoc = Path.Combine(progLoc, strExeName)

        If Not File.Exists(progLoc) Then
            errorMessage = "Cannot find " & strStepToolName & " program file " & strExeName
            Console.WriteLine(errorMessage)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage & " at " & progLoc)
            Return String.Empty
        End If

        Return progLoc

    End Function

    ''' <summary>
    ''' Gets the dictionary for the packed job parameter
    ''' </summary>
    ''' <param name="strPackedJobParameterName">Packaged job parameter name</param>
    ''' <returns>List of strings</returns>
    ''' <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
    Protected Function ExtractPackedJobParameterDictionary(strPackedJobParameterName As String) As Dictionary(Of String, String)

        Dim lstData As List(Of String)
        Dim dctData = New Dictionary(Of String, String)

        lstData = ExtractPackedJobParameterList(strPackedJobParameterName)

        For Each strItem In lstData
            Dim intEqualsIndex = strItem.LastIndexOf("="c)
            If intEqualsIndex > 0 Then
                Dim strKey As String = strItem.Substring(0, intEqualsIndex)
                Dim strValue As String = strItem.Substring(intEqualsIndex + 1)

                If Not dctData.ContainsKey(strKey) Then
                    dctData.Add(strKey, strValue)
                End If
            Else
                LogError("Packed dictionary item does not contain an equals sign: " & strItem)
            End If
        Next

        Return dctData

    End Function

    ''' <summary>
    ''' Gets the list of values for the packed job parameter
    ''' </summary>
    ''' <param name="strPackedJobParameterName">Packaged job parameter name</param>
    ''' <returns>List of strings</returns>
    ''' <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
    Protected Function ExtractPackedJobParameterList(strPackedJobParameterName As String) As List(Of String)

        Dim strList As String

        strList = m_jobParams.GetJobParameter(strPackedJobParameterName, String.Empty)

        If String.IsNullOrEmpty(strList) Then
            Return New List(Of String)
        Else
            ' Split the list on tab characters
            Return strList.Split(ControlChars.Tab).ToList()
        End If

    End Function

    ''' <summary>
    ''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function GetCurrentMgrSettingsFromDB() As Boolean
        Return GetCurrentMgrSettingsFromDB(0)
    End Function

    ''' <summary>
    ''' Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
    ''' </summary>
    ''' <param name="intUpdateIntervalSeconds">The minimum number of seconds between updates; if fewer than intUpdateIntervalSeconds seconds have elapsed since the last call to this function, then no update will occur</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds As Integer) As Boolean
        Return GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds, m_mgrParams, m_DebugLevel)
    End Function

    ''' <summary>
    ''' Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
    ''' </summary>
    ''' <param name="DebugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
    ''' <returns>True for success; False for error</returns>
    ''' <remarks></remarks>
    Public Shared Function GetCurrentMgrSettingsFromDB(
       intUpdateIntervalSeconds As Integer,
       objMgrParams As IMgrParams,
       ByRef debugLevel As Short) As Boolean

        Static dtLastUpdateTime As DateTime = Date.UtcNow.Subtract(New TimeSpan(1, 0, 0))

        Try

            If intUpdateIntervalSeconds > 0 AndAlso Date.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds < intUpdateIntervalSeconds Then
                Return True
            End If
            dtLastUpdateTime = Date.UtcNow

            If debugLevel >= 5 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating manager settings from the Manager Control DB")
            End If

            ' Data Source=proteinseqs;Initial Catalog=manager_control
            Dim connectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr")
            Dim managerName = objMgrParams.GetParam("MgrName")

            Dim newDebugLevel = GetManagerDebugLevel(connectionString, managerName, debugLevel, 0)

            If debugLevel > 0 AndAlso newDebugLevel <> debugLevel Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Debug level changed from " & debugLevel.ToString & " to " & newDebugLevel.ToString)
                debugLevel = newDebugLevel
            End If

            Return True

        Catch ex As Exception
            Dim errorMessage = "Exception getting current manager settings from the manager control DB"
            Console.WriteLine(errorMessage)
            Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex))
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage & ": " & ex.Message)
        End Try

        Return False

    End Function

    Protected Shared Function GetManagerDebugLevel(connectionString As String, managerName As String, currentDebugLevel As Short, recursionLevel As Integer) As Short

        If recursionLevel > 3 Then
            Return currentDebugLevel
        End If

        Dim sqlQuery = "SELECT ParameterName, ParameterValue " &
                       "FROM V_MgrParams " &
                       "WHERE ManagerName = '" & managerName & "' AND " &
                       " ParameterName IN ('debuglevel', 'MgrSettingGroupName')"

        Dim lstResults As List(Of List(Of String)) = Nothing
        Dim success = clsGlobal.GetQueryResults(sqlQuery, connectionString, lstResults, "GetCurrentMgrSettingsFromDB")

        If success AndAlso lstResults.Count > 0 Then

            For Each resultRow In lstResults
                Dim paramName = resultRow(0)
                Dim paramValue = resultRow(1)

                If clsGlobal.IsMatch(paramName, "debuglevel") Then
                    Dim debugLevel = Short.Parse(paramValue)
                    Return debugLevel
                End If

                If clsGlobal.IsMatch(paramName, "MgrSettingGroupName") Then
                    ' DebugLevel is defined by a manager settings group; repeat the query to V_MgrParams

                    Dim debugLevel = GetManagerDebugLevel(connectionString, paramValue, currentDebugLevel, recursionLevel + 1)
                    Return debugLevel
                End If
            Next
        End If

        Return currentDebugLevel

    End Function


    ''' <summary>
    ''' Deterime the path to java.exe
    ''' </summary>
    ''' <returns>The path to the java.exe, or an empty string if the manager parameter is not defined or if java.exe does not exist</returns>
    ''' <remarks></remarks>
    Protected Function GetJavaProgLoc() As String

        ' JavaLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
        Dim javaProgLoc As String = m_mgrParams.GetParam("JavaLoc")

        If String.IsNullOrEmpty(javaProgLoc) Then
            LogError("Parameter 'JavaLoc' not defined for this manager")
            Return String.Empty
        End If

        If Not File.Exists(javaProgLoc) Then
            LogError("Cannot find Java: " & javaProgLoc)
            Return String.Empty
        End If

        Return javaProgLoc

    End Function

    ''' <summary>
    ''' Returns the full path to the program to use for converting a dataset to a .mzXML file
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function GetMSXmlGeneratorAppPath() As String

        Dim strMSXmlGeneratorExe As String = GetMSXmlGeneratorExeName()
        Dim strMSXmlGeneratorAppPath As String

        strMSXmlGeneratorAppPath = String.Empty
        If strMSXmlGeneratorExe.ToLower().Contains("readw") Then
            ' ReadW
            ' Note that msXmlGenerator will likely be ReAdW.exe
            strMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdW", "ReAdWProgLoc", strMSXmlGeneratorExe)

        ElseIf strMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
            ' MSConvert
            Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")         ' MSConvert.exe is stored in the ProteoWizard folder
            strMSXmlGeneratorAppPath = Path.Combine(ProteoWizardDir, strMSXmlGeneratorExe)

        Else
            LogError("Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'")
        End If

        Return strMSXmlGeneratorAppPath

    End Function

    ''' <summary>
    ''' Returns the name of the .Exe to use to convert a dataset to a .mzXML file
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function GetMSXmlGeneratorExeName() As String
        ' Determine the path to the XML Generator
        Dim strMSXmlGeneratorExe As String = m_jobParams.GetParam("MSXMLGenerator")         ' ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)

        If String.IsNullOrEmpty(strMSXmlGeneratorExe) Then
            ' Assume we're using MSConvert
            strMSXmlGeneratorExe = "MSConvert.exe"
        End If

        Return strMSXmlGeneratorExe
    End Function

    ''' <summary>
    ''' Determines the folder that contains R.exe and Rcmd.exe (queries the registry)
    ''' </summary>
    ''' <returns>Folder path, e.g. C:\Program Files\R\R-3.2.2\bin\x64</returns>
    ''' <remarks>This function is public because it is used by the Cyclops test harness program</remarks>
    Public Function GetRPathFromWindowsRegistry() As String

        Const RCORE_SUBKEY = "SOFTWARE\R-core"

        Try

            Dim regRCore = Microsoft.Win32.Registry.LocalMachine.OpenSubKey("SOFTWARE\R-core")
            If regRCore Is Nothing Then
                ' Local machine SOFTWARE\R-core not found; try current user
                regRCore = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("SOFTWARE\R-core")
                If regRCore Is Nothing Then
                    LogError("Windows Registry key'" + RCORE_SUBKEY + " not found in HKEY_LOCAL_MACHINE nor HKEY_CURRENT_USER")
                    Return String.Empty
                End If
            End If

            Dim is64Bit As Boolean = Environment.Is64BitProcess
            Dim sRSubKey As String = If(is64Bit, "R64", "R")
            Dim regR As Microsoft.Win32.RegistryKey = regRCore.OpenSubKey(sRSubKey)
            If regR Is Nothing Then
                LogError("Registry key is not found: " & RCORE_SUBKEY + "\" & sRSubKey)
                Return String.Empty
            End If

            Dim currentVersionText = DirectCast(regR.GetValue("Current Version"), String)

            Dim installPath As String
            Dim bin As String

            If String.IsNullOrEmpty(currentVersionText) Then
                If regR.SubKeyCount = 0 Then
                    LogError("Unable to determine the R Path: " & RCORE_SUBKEY & "\" & sRSubKey & " has no subkeys")
                    Return String.Empty
                End If

                ' Find the newest subkey
                Dim subKeys = regR.GetSubKeyNames().ToList()
                subKeys.Sort()
                subKeys.Reverse()

                Dim newestSubkey = subKeys.FirstOrDefault()

                Dim regRNewest As Microsoft.Win32.RegistryKey = regR.OpenSubKey(newestSubkey)
                installPath = DirectCast(regRNewest.GetValue("InstallPath"), String)
                If String.IsNullOrEmpty(installPath) Then
                    LogError("Unable to determine the R Path: " & newestSubkey & " does not have key InstallPath")
                    Return String.Empty
                End If

                bin = Path.Combine(installPath, "bin")
            Else

                installPath = DirectCast(regR.GetValue("InstallPath"), String)
                If String.IsNullOrEmpty(installPath) Then
                    LogError("Unable to determine the R Path: " & RCORE_SUBKEY + "\" & sRSubKey & " does not have key InstallPath")
                    Return String.Empty
                End If

                bin = Path.Combine(installPath, "bin")

                ' If version is of the form "3.2.3" (for Major.Minor.Build)
                ' we can directly instantiate a new Version object from the string

                ' However, in 2016 R version "3.2.4 Revised" was released, and that
                ' string cannot be directly used to instantiate a new Version object

                ' The following checks for this and removes any non-numeric characters
                ' (though it requires that the Major version be an integer)

                Dim versionParts = currentVersionText.Split("."c)
                Dim reconstructVersion = False

                Dim currentVersion As Version

                If currentVersionText.Length <= 1 Then
                    currentVersion = New Version(currentVersionText)
                Else
                    Dim nonNumericChars = New Regex("[^0-9]+", RegexOptions.Compiled)

                    For i = 1 To versionParts.Length - 1
                        If nonNumericChars.IsMatch(versionParts(i)) Then
                            versionParts(i) = nonNumericChars.Replace(versionParts(i), String.Empty)
                            reconstructVersion = True
                        End If
                    Next

                    If reconstructVersion Then
                        currentVersion = New Version(String.Join(".", versionParts))
                    Else
                        currentVersion = New Version(currentVersionText)
                    End If

                End If

                ' Up to 2.11.x, DLLs are installed in R_HOME\bin
                ' From 2.12.0, DLLs are installed in either i386 or x64 (or both) below the bin folder
                ' The bin folder has an R.exe file but it does not have Rcmd.exe or R.dll
                If currentVersion < New Version(2, 12) Then
                    Return bin
                End If
            End If

            If is64Bit Then
                Return Path.Combine(bin, "x64")
            Else
                Return Path.Combine(bin, "i386")
            End If

        Catch ex As Exception
            LogError("Exception in GetRPathFromWindowsRegistry", ex)
            Return String.Empty
        End Try

    End Function

    ''' <summary>
    ''' Lookup the base transfer folder path
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks>For example, \\proto-7\DMS3_XFER\</remarks>
    Protected Function GetTransferFolderPath() As String

        Dim transferFolderPath = m_jobParams.GetParam("transferFolderPath")

        If String.IsNullOrEmpty(transferFolderPath) Then
            LogError("Transfer folder path not defined; job param 'transferFolderPath' is empty")
            m_message = clsGlobal.AppendToComment(m_message, "Transfer folder path not defined")
            Return String.Empty
        End If

        Return transferFolderPath

    End Function

    ''' <summary>
    ''' Gets the .zip file path to create when zipping a single file
    ''' </summary>
    ''' <param name="sourceFilePath"></param>
    ''' <returns></returns>
    Public Function GetZipFilePathForFile(sourceFilePath As String) As String
        Return clsIonicZipTools.GetZipFilePathForFile(sourceFilePath)
    End Function

    ''' <summary>
    ''' Decompresses the specified gzipped file
    ''' Output folder is m_WorkDir
    ''' </summary>
    ''' <param name="gzipFilePath">File to decompress</param>
    ''' <returns></returns>
    Public Function GUnzipFile(gzipFilePath As String) As Boolean
        Return GUnzipFile(gzipFilePath, m_WorkDir)
    End Function

    ''' <summary>
    ''' Decompresses the specified gzipped file
    ''' </summary>
    ''' <param name="gzipFilePath">File to unzip</param>
    ''' <param name="targetDirectory">Target directory for the extracted files</param>
    ''' <returns></returns>
    Public Function GUnzipFile(gzipFilePath As String, targetDirectory As String) As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        Return m_IonicZipTools.GUnzipFile(gzipFilePath, targetDirectory)
    End Function

    ''' <summary>
    ''' Gzips sourceFilePath, creating a new file in the same folder, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
    ''' </summary>
    ''' <param name="sourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GZipFile(sourceFilePath As String, deleteSourceAfterZip As Boolean) As Boolean
        Dim blnSuccess As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        blnSuccess = m_IonicZipTools.GZipFile(sourceFilePath, deleteSourceAfterZip)

        If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
            m_NeedToAbortProcessing = True
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Gzips sourceFilePath, creating a new file in targetDirectoryPath; the file extension will be the original extension plus .gz
    ''' </summary>
    ''' <param name="sourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GZipFile(sourceFilePath As String, targetDirectoryPath As String, deleteSourceAfterZip As Boolean) As Boolean

        Dim blnSuccess As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        blnSuccess = m_IonicZipTools.GZipFile(sourceFilePath, targetDirectoryPath, deleteSourceAfterZip)

        If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
            m_NeedToAbortProcessing = True
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' GZip the given file
    ''' </summary>
    ''' <param name="fiResultFile"></param>
    ''' <returns>Fileinfo object of the new .gz file or null if an error</returns>
    ''' <remarks>Deletes the original file after creating the .gz file</remarks>
    Public Function GZipFile(fiResultFile As FileInfo) As FileInfo
        Return GZipFile(fiResultFile, True)
    End Function

    ''' <summary>
    ''' GZip the given file
    ''' </summary>
    ''' <param name="fiResultFile"></param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <returns>Fileinfo object of the new .gz file or null if an error</returns>
    Public Function GZipFile(fiResultFile As FileInfo, deleteSourceAfterZip As Boolean) As FileInfo

        Try
            Dim success = GZipFile(fiResultFile.FullName, True)

            If Not success Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("GZipFile returned false for " & fiResultFile.Name)
                End If
                Return Nothing
            End If

            Dim fiGZippedFile = New FileInfo(fiResultFile.FullName & clsAnalysisResources.DOT_GZ_EXTENSION)
            If Not fiGZippedFile.Exists Then
                LogError("GZip file was not created: " & fiGZippedFile.Name)
                Return Nothing
            End If

            Return fiGZippedFile

        Catch ex As Exception
            LogError("Exception in GZipFile(fiResultFile As FileInfo)", ex)
            Return Nothing
        End Try

    End Function

    ''' <summary>
    ''' Parse a thread count text value to determine the number of threads (cores) to use
    ''' </summary>
    ''' <param name="threadCountText">Can be "0" or "all" for all threads, or a number of threads, or "90%"</param>
    ''' <param name="maxThreadsToAllow">Maximum number of cores to use (0 for all)</param>
    ''' <returns>The core count to use</returns>
    ''' <remarks>Core count will be a minimum of 1 and a maximum of Environment.ProcessorCount</remarks>
    Public Shared Function ParseThreadCount(threadCountText As String, maxThreadsToAllow As Integer) As Integer

        Dim rePercentage = New Regex("([0-9.]+)%")

        If String.IsNullOrWhiteSpace(threadCountText) Then
            threadCountText = "all"
        Else
            threadCountText = threadCountText.Trim()
        End If

        Dim coresOnMachine = PRISM.Processes.clsProgRunner.GetCoreCount()
        Dim coreCount = 0

        If threadCountText.ToLower().StartsWith("all") Then
            coreCount = coresOnMachine
        Else
            Dim reMatch As Match = rePercentage.Match(threadCountText)
            If reMatch.Success Then
                ' Value is similar to 90%
                ' Convert to a double, then compute the number of cores to use
                Dim coreCountPct = CDbl(reMatch.Groups(1).Value)
                coreCount = CInt(Math.Round(coreCountPct / 100 * coresOnMachine))
                If coreCount < 1 Then coreCount = 1
            Else
                If Integer.TryParse(threadCountText, coreCount) Then
                    coreCount = 0
                End If
            End If
        End If

        If coreCount = 0 Then coreCount = coresOnMachine
        If coreCount > coresOnMachine Then coreCount = coresOnMachine

        If maxThreadsToAllow > 0 AndAlso coreCount > maxThreadsToAllow Then
            coreCount = maxThreadsToAllow
        End If

        If coreCount < 1 Then coreCount = 1

        Return coreCount

    End Function

    ''' <summary>
    ''' Looks up dataset information for the data package associated with this analysis job
    ''' </summary>
    ''' <param name="dctDataPackageDatasets"></param>
    ''' <returns>True if a data package is defined and it has datasets associated with it</returns>
    ''' <remarks></remarks>
    Protected Function LoadDataPackageDatasetInfo(<Out()> ByRef dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo)) As Boolean

        Dim connectionString As String = m_mgrParams.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline
        Dim dataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

        If dataPackageID < 0 Then
            dctDataPackageDatasets = New Dictionary(Of Integer, clsDataPackageDatasetInfo)
            Return False
        Else
            Return clsAnalysisResources.LoadDataPackageDatasetInfo(connectionString, dataPackageID, dctDataPackageDatasets)
        End If

    End Function

    ''' <summary>
    ''' Looks up job information for the data package associated with this analysis job
    ''' </summary>
    ''' <param name="dctDataPackageJobs"></param>
    ''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
    ''' <remarks></remarks>
    Protected Function LoadDataPackageJobInfo(<Out()> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        Dim connectionString As String = m_mgrParams.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline
        Dim dataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

        If dataPackageID < 0 Then
            dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)
            Return False
        Else
            Return clsAnalysisResources.LoadDataPackageJobInfo(connectionString, dataPackageID, dctDataPackageJobs)
        End If

    End Function

    ''' <summary>
    ''' Loads the job settings file
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function LoadSettingsFile() As Boolean
        Dim fileName As String = m_jobParams.GetParam("settingsFileName")
        If fileName <> "na" Then
            Dim filePath As String = Path.Combine(m_WorkDir, fileName)
            If File.Exists(filePath) Then            'XML tool Loadsettings returns True even if file is not found, so separate check reqd
                Return m_settingsFileParams.LoadSettings(filePath)
            Else
                Return False            'Settings file wasn't found
            End If
        Else
            Return True       'Settings file wasn't required
        End If

    End Function

    ''' <summary>
    ''' Logs current progress to the log file at a given interval
    ''' </summary>
    ''' <param name="toolName"></param>
    ''' <remarks>Longer log intervals when m_debuglevel is 0 or 1; shorter intervals for 5</remarks>
    Protected Sub LogProgress(toolName As String)
        Dim logIntervalMinutes As Integer

        If m_DebugLevel >= 5 Then
            logIntervalMinutes = 1
        ElseIf m_DebugLevel >= 4 Then
            logIntervalMinutes = 5
        ElseIf m_DebugLevel >= 3 Then
            logIntervalMinutes = 15
        ElseIf m_DebugLevel >= 2 Then
            logIntervalMinutes = 30
        Else
            logIntervalMinutes = 60
        End If

        LogProgress(toolName, logIntervalMinutes)
    End Sub

    ''' <summary>
    ''' Logs m_progress to the log file at interval logIntervalMinutes
    ''' </summary>
    ''' <param name="toolName"></param>
    ''' <param name="logIntervalMinutes"></param>
    ''' <remarks>Calls GetCurrentMgrSettingsFromDB every 300 seconds</remarks>
    Protected Sub LogProgress(toolName As String, logIntervalMinutes As Integer)

        Const CONSOLE_PROGRESS_INTERVAL_MINUTES = 1

        Try

            If logIntervalMinutes < 1 Then logIntervalMinutes = 1

            Dim progressMessage = " ... " & m_progress.ToString("0.0") & "% complete for " & toolName & ", job " & m_JobNum

            If Date.UtcNow.Subtract(m_LastProgressConsoleTime).TotalMinutes >= CONSOLE_PROGRESS_INTERVAL_MINUTES Then
                m_LastProgressConsoleTime = Date.UtcNow
                Console.WriteLine(progressMessage)
            End If

            If Date.UtcNow.Subtract(m_LastProgressWriteTime).TotalMinutes >= logIntervalMinutes Then
                m_LastProgressWriteTime = Date.UtcNow
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progressMessage)
            End If

            ' Synchronize the stored Debug level with the value stored in the database
            Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS = 300
            GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        Catch ex As Exception
            ' Ignore errors here
        End Try

    End Sub

    ''' <summary>
    ''' Log a warning message in the manager's log file
    ''' Also display it at console
    ''' Optionally update m_EvalMessage
    ''' </summary>
    ''' <param name="warningMessage">Warning message</param>
    ''' <param name="updateEvalMessage">When true, update m_EvalMessage</param>
    Protected Overloads Sub LogWarning(warningMessage As String, Optional updateEvalMessage As Boolean = False)
        If updateEvalMessage Then
            m_EvalMessage = warningMessage
        End If
        MyBase.LogWarning(warningMessage)
    End Sub

    ''' <summary>
    ''' Creates a results folder after analysis complete
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Protected Function MakeResultsFolder() As IJobParams.CloseOutType

        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)

        ' Makes results folder and moves files into it

        ' Log status
        ReportStatus(m_MachName & ": Creating results folder, Job " & m_JobNum)
        Dim ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)

        ' Make the results folder
        Try
            Directory.CreateDirectory(ResFolderNamePath)
        Catch ex As Exception
            ' Log this error to the database
            ReportStatus("Error making results folder, job " & m_JobNum, ex)
            m_message = clsGlobal.AppendToComment(m_message, "Error making results folder")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Moves result files to the local results folder after tool has completed
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Protected Function MoveResultFiles() As IJobParams.CloseOutType
        Const REJECT_LOGGING_THRESHOLD = 10
        Const ACCEPT_LOGGING_THRESHOLD = 50
        Const LOG_LEVEL_REPORT_ACCEPT_OR_REJECT = 5

        'Makes results folder and moves files into it
        Dim ResFolderNamePath As String = String.Empty
        Dim strTargetFilePath As String = String.Empty

        Dim Files() As String
        Dim tmpFileName As String = String.Empty
        Dim tmpFileNameLcase As String
        Dim OkToMove As Boolean
        Dim strLogMessage As String

        Dim strExtension As String
        Dim dctRejectStats As Dictionary(Of String, Integer)
        Dim dctAcceptStats As Dictionary(Of String, Integer)
        Dim intCount As Integer

        Dim objExtension As Dictionary(Of String, Integer).Enumerator

        Dim blnErrorEncountered = False

        ' Move files into results folder
        Try
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS, 0)
            ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)
            dctRejectStats = New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)
            dctAcceptStats = New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)

            'Log status
            If m_DebugLevel >= 2 Then
                strLogMessage = "Move Result Files to " & ResFolderNamePath
                If m_DebugLevel >= 3 Then
                    strLogMessage &= "; ResultFilesToSkip contains " & m_jobParams.ResultFilesToSkip.Count.ToString & " entries" &
                      "; ResultFileExtensionsToSkip contains " & m_jobParams.ResultFileExtensionsToSkip.Count.ToString & " entries" &
                      "; ResultFilesToKeep contains " & m_jobParams.ResultFilesToKeep.Count.ToString & " entries"
                End If
                ReportStatus(strLogMessage, m_DebugLevel)
            End If

            ' Obtain a list of all files in the working directory
            ' Ignore subdirectories
            Files = Directory.GetFiles(m_WorkDir, "*")

            ' Check each file against m_jobParams.m_ResultFileExtensionsToSkip and m_jobParams.m_ResultFilesToKeep
            For Each tmpFileName In Files

                OkToMove = True
                tmpFileNameLcase = Path.GetFileName(tmpFileName).ToLower()

                ' Check to see if the filename is defined in ResultFilesToSkip
                ' Note that entries in ResultFilesToSkip are not case sensitive since they were instantiated using SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)
                If m_jobParams.ResultFilesToSkip.Contains(tmpFileNameLcase) Then
                    ' File found in the ResultFilesToSkip list; do not move it
                    OkToMove = False
                End If

                If OkToMove Then
                    ' Check to see if the file ends with an entry specified in m_ResultFileExtensionsToSkip
                    ' Note that entries in m_ResultFileExtensionsToSkip can be extensions, or can even be partial file names, e.g. _peaks.txt
                    For Each ext As String In m_jobParams.ResultFileExtensionsToSkip
                        If tmpFileNameLcase.EndsWith(ext.ToLower()) Then
                            OkToMove = False
                            Exit For
                        End If
                    Next
                End If

                If Not OkToMove Then
                    ' Check to see if the file is a result file that got captured as a non result file
                    If m_jobParams.ResultFilesToKeep.Contains(tmpFileNameLcase) Then
                        OkToMove = True
                    End If
                End If

                If OkToMove AndAlso PRISM.Files.clsFileTools.IsVimSwapFile(tmpFileName) Then
                    ' VIM swap file; skip it
                    OkToMove = False
                End If

                ' Look for invalid characters in the filename
                '	(Required because extract_msn.exe sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in working directory) 
                ' Note: now evaluating each character in the filename
                If OkToMove Then
                    Dim intAscValue As Integer
                    For Each chChar As Char In Path.GetFileName(tmpFileName).ToCharArray
                        intAscValue = Convert.ToInt32(chChar)
                        If intAscValue <= 31 Or intAscValue >= 128 Then
                            ' Invalid character found
                            OkToMove = False
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & tmpFileName)
                            Exit For
                        End If
                    Next
                Else
                    If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                        strExtension = Path.GetExtension(tmpFileName)
                        If dctRejectStats.TryGetValue(strExtension, intCount) Then
                            dctRejectStats(strExtension) = intCount + 1
                        Else
                            dctRejectStats.Add(strExtension, 1)
                        End If

                        ' Only log the first 10 times files of a given extension are rejected
                        '  However, if a file was rejected due to invalid characters in the name, then we don't track that rejection with dctRejectStats
                        If dctRejectStats(strExtension) <= REJECT_LOGGING_THRESHOLD Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Rejected file:  " & tmpFileName)
                        End If
                    End If
                End If

                If Not OkToMove Then Continue For

                'If valid file name, then move file to results folder
                If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                    strExtension = Path.GetExtension(tmpFileName).ToLower
                    If dctAcceptStats.TryGetValue(strExtension, intCount) Then
                        dctAcceptStats(strExtension) = intCount + 1
                    Else
                        dctAcceptStats.Add(strExtension, 1)
                    End If

                    ' Only log the first 50 times files of a given extension are accepted
                    If dctAcceptStats(strExtension) <= ACCEPT_LOGGING_THRESHOLD Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " MoveResultFiles: Accepted file:  " & tmpFileName)
                    End If
                End If

                Try
                    strTargetFilePath = Path.Combine(ResFolderNamePath, Path.GetFileName(tmpFileName))
                    File.Move(tmpFileName, strTargetFilePath)
                Catch ex As Exception
                    Try
                        ' Move failed
                        ' Attempt to copy the file instead of moving the file
                        File.Copy(tmpFileName, strTargetFilePath, True)

                        ' If we get here, then the copy succeeded; the original file (in the work folder) will get deleted when the work folder is "cleaned" after the job finishes

                    Catch ex2 As Exception
                        ' Copy also failed
                        ' Continue moving files; we'll fail the results at the end of this function
                        ReportStatus(" MoveResultFiles: error moving/copying file: " & tmpFileName, ex)
                        blnErrorEncountered = True
                    End Try
                End Try

            Next

            If m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT Then
                ' Look for any extensions in dctAcceptStats that had over 50 accepted files
                objExtension = dctAcceptStats.GetEnumerator
                Do While objExtension.MoveNext
                    If objExtension.Current.Value > ACCEPT_LOGGING_THRESHOLD Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                             " MoveResultFiles: Accepted a total of " & objExtension.Current.Value & " files with extension " & objExtension.Current.Key)
                    End If
                Loop

                ' Look for any extensions in dctRejectStats that had over 10 rejected files
                objExtension = dctRejectStats.GetEnumerator
                Do While objExtension.MoveNext
                    If objExtension.Current.Value > REJECT_LOGGING_THRESHOLD Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                             " MoveResultFiles: Rejected a total of " & objExtension.Current.Value & " files with extension " & objExtension.Current.Key)
                    End If
                Loop
            End If

        Catch ex As Exception
            If m_DebugLevel > 0 Then
                ReportStatus("clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results folder", 0, True)
                ReportStatus("Tmpfile = " & tmpFileName)
                ReportStatus("Results folder name = " & Path.Combine(ResFolderNamePath, Path.GetFileName(tmpFileName)))
            End If

            ' Log this error to the database
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error moving results files, job " & m_JobNum & ex.Message)
            m_message = clsGlobal.AppendToComment(m_message, "Error moving results files")

            blnErrorEncountered = True
        End Try

        Try
            'Make the summary file
            OutputSummary(ResFolderNamePath)
        Catch ex As Exception
            ' Ignore errors here
        End Try

        If blnErrorEncountered Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

    End Function

    Public Shared Function NotifyMissingParameter(oJobParams As IJobParams, strParameterName As String) As String

        Dim strSettingsFile As String = oJobParams.GetJobParameter("SettingsFileName", "?UnknownSettingsFile?")
        Dim strToolName As String = oJobParams.GetJobParameter("ToolName", "?UnknownToolName?")

        Return "Settings file " & strSettingsFile & " for tool " & strToolName & " does not have parameter " & strParameterName & " defined"

    End Function

    ''' <summary>
    ''' Adds manager assembly data to job summary file
    ''' </summary>
    ''' <param name="OutputPath">Path to summary file</param>
    ''' <remarks>Skipped if the debug level is less than 4</remarks>
    Protected Sub OutputSummary(OutputPath As String)

        If m_DebugLevel < 4 Then
            ' Do not create the AnalysisSummary file
            Exit Sub
        End If

        ' Saves the summary file in the results folder
        Dim objAssemblyTools = New clsAssemblyTools

        objAssemblyTools.GetComponentFileVersionInfo(m_SummaryFile)

        Dim summaryFileName = m_jobParams.GetParam("StepTool") & "_AnalysisSummary.txt"

        If Not m_jobParams.ResultFilesToSkip.Contains(summaryFileName) Then
            m_SummaryFile.SaveSummaryFile(Path.Combine(OutputPath, summaryFileName))
        End If

    End Sub

    ''' <summary>
    ''' Adds double quotes around a path if it contains a space
    ''' </summary>
    ''' <param name="strPath"></param>
    ''' <returns>The path (updated if necessary)</returns>
    ''' <remarks></remarks>
    Public Shared Function PossiblyQuotePath(strPath As String) As String
        Return clsGlobal.PossiblyQuotePath(strPath)
    End Function

    Public Sub PurgeOldServerCacheFiles(cacheFolderPath As String)
        ' Value prior to December 2014: 3 TB
        ' Value effective December 2014: 20 TB
        Const spaceUsageThresholdGB = 20000
        PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB)
    End Sub

    Public Sub PurgeOldServerCacheFilesTest(cacheFolderPath As String, spaceUsageThresholdGB As Integer)
        If cacheFolderPath.ToLower().StartsWith("\\proto") Then
            If Not String.Equals(cacheFolderPath, "\\proto-2\past\PurgeTest", StringComparison.CurrentCultureIgnoreCase) Then
                Console.WriteLine("This function cannot be used with a \\Proto-x\ server")
                Return
            End If
        End If
        PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB)
    End Sub


    ''' <summary>
    ''' Determines the space usage of data files in the cache folder
    ''' If usage is over intSpaceUsageThresholdGB, then deletes the oldest files until usage falls below intSpaceUsageThresholdGB
    ''' </summary>
    ''' <param name="strCacheFolderPath">Path to the file cache</param>
    ''' <param name="spaceUsageThresholdGB">Maximum space usage, in GB (cannot be less than 1000 on Proto-x servers; 10 otherwise)</param>
    ''' <remarks></remarks>
    Private Sub PurgeOldServerCacheFiles(strCacheFolderPath As String, spaceUsageThresholdGB As Integer)

        Const PURGE_INTERVAL_MINUTES = 90
        Static dtLastCheck As DateTime = Date.UtcNow.AddMinutes(-PURGE_INTERVAL_MINUTES * 2)

        Dim diCacheFolder As DirectoryInfo
        Dim lstDataFiles = New List(Of KeyValuePair(Of DateTime, FileInfo))

        Dim dblTotalSizeMB As Double = 0

        Dim dblSizeDeletedMB As Double = 0
        Dim intFileDeleteCount = 0
        Dim intFileDeleteErrorCount = 0

        Dim dctErrorSummary = New Dictionary(Of String, Integer)

        If String.IsNullOrWhiteSpace(strCacheFolderPath) Then
            Throw New ArgumentOutOfRangeException(NameOf(strCacheFolderPath), "Cache folder path cannot be empty")
        End If

        If strCacheFolderPath.ToLower().StartsWith("\\proto-") Then
            If spaceUsageThresholdGB < 1000 Then spaceUsageThresholdGB = 1000
        Else
            If spaceUsageThresholdGB < 10 Then spaceUsageThresholdGB = 10
        End If

        Try
            If Date.UtcNow.Subtract(dtLastCheck).TotalMinutes < PURGE_INTERVAL_MINUTES Then
                Exit Sub
            End If
            diCacheFolder = New DirectoryInfo(strCacheFolderPath)

            If Not diCacheFolder.Exists Then
                Exit Sub
            End If

            ' Look for a purge check file
            Dim fiPurgeCheckFile = New FileInfo(Path.Combine(diCacheFolder.FullName, "PurgeCheckFile.txt"))
            If fiPurgeCheckFile.Exists Then
                If Date.UtcNow.Subtract(fiPurgeCheckFile.LastWriteTimeUtc).TotalMinutes < PURGE_INTERVAL_MINUTES Then
                    Exit Sub
                End If
            End If

            ' Create / update the purge check file
            Try
                Using swPurgeCheckFile = New StreamWriter(New FileStream(fiPurgeCheckFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read))
                    swPurgeCheckFile.WriteLine(Date.Now.ToString(DATE_TIME_FORMAT) & " - " & m_MachName)
                End Using

            Catch ex As Exception
                ' Likely another manager tried to update the file at the same time
                ' Ignore the error and proceed to look for files to purge
            End Try

            dtLastCheck = Date.UtcNow

            Dim dtLastProgress = Date.UtcNow
            ReportStatus("Examining hashcheck files in folder " & diCacheFolder.FullName, 1)

            ' Make a list of all of the hashcheck files in diCacheFolder

            For Each fiItem As FileInfo In diCacheFolder.GetFiles("*.hashcheck", SearchOption.AllDirectories)

                If fiItem.FullName.ToLower().EndsWith(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.ToLower()) Then
                    Dim strDataFilePath As String
                    strDataFilePath = fiItem.FullName.Substring(0, fiItem.FullName.Length - clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.Length)

                    Dim fiDataFile = New FileInfo(strDataFilePath)

                    If fiDataFile.Exists Then
                        Try
                            lstDataFiles.Add(New KeyValuePair(Of DateTime, FileInfo)(fiDataFile.LastWriteTimeUtc, fiDataFile))

                            dblTotalSizeMB += clsGlobal.BytesToMB(fiDataFile.Length)
                        Catch ex As Exception
                            ReportStatus("Exception adding to file list " + fiDataFile.Name + "; " + ex.Message, 0, True)
                        End Try

                        If Date.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 5 Then
                            dtLastProgress = Date.UtcNow
                            ReportStatus(String.Format(" ... {0:#,##0} files processed", lstDataFiles.Count))
                        End If

                    End If

                End If
            Next

            If dblTotalSizeMB / 1024 <= spaceUsageThresholdGB Then
                Return
            End If

            ' Purge files until the space usage falls below the threshold
            ' Start with the earliest file then work our way forward

            ' Keep track of the deleted file info using this list
            Dim purgedFileLogEntries = New List(Of String)

            Dim fiPurgeLogFile = New FileInfo(Path.Combine(diCacheFolder.FullName, "PurgeLog_" & Date.Now.Year & ".txt"))
            If Not fiPurgeLogFile.Exists Then
                ' Create the purge log file and write the header line
                Try
                    Using swPurgeLogFile = New StreamWriter(New FileStream(fiPurgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read))
                        swPurgeLogFile.WriteLine(String.Join(vbTab, New String() {"Date",
                                                                                    "Manager",
                                                                                    "Size (MB)",
                                                                                    "Modification_Date",
                                                                                    "Path"}))
                    End Using
                Catch ex As Exception
                    ' Likely another manager tried to create the file at the same time
                    ' Ignore the error
                End Try
            End If

            Dim lstSortedFiles = (From item In lstDataFiles Select item Order By item.Key)
            Dim managerName As String = m_mgrParams.GetParam("MgrName", m_MachName)

            For Each kvItem As KeyValuePair(Of DateTime, FileInfo) In lstSortedFiles

                Try
                    Dim fileSizeMB As Double = clsGlobal.BytesToMB(kvItem.Value.Length)

                    Dim hashcheckPath = kvItem.Value.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX
                    Dim fiHashCheckFile = New FileInfo(hashcheckPath)

                    dblTotalSizeMB -= fileSizeMB

                    kvItem.Value.Delete()

                    ' Keep track of the deleted file's details
                    purgedFileLogEntries.Add(String.Join(vbTab, New String() {Date.Now.ToString(DATE_TIME_FORMAT),
                                                                                managerName,
                                                                                fileSizeMB.ToString("0.00"),
                                                                                kvItem.Value.LastWriteTime.ToString(DATE_TIME_FORMAT),
                                                                                kvItem.Value.FullName}))

                    dblSizeDeletedMB += fileSizeMB
                    intFileDeleteCount += 1

                    If fiHashCheckFile.Exists Then
                        fiHashCheckFile.Delete()
                    End If

                Catch ex As Exception
                    ' Keep track of the number of times we have an exception
                    intFileDeleteErrorCount += 1

                    Dim intOccurrences = 1
                    Dim strExceptionName As String = ex.GetType.ToString()
                    If dctErrorSummary.TryGetValue(strExceptionName, intOccurrences) Then
                        dctErrorSummary(strExceptionName) = intOccurrences + 1
                    Else
                        dctErrorSummary.Add(strExceptionName, 1)
                    End If

                End Try

                If dblTotalSizeMB / 1024.0 < spaceUsageThresholdGB * 0.95 Then
                    Exit For
                End If
            Next

            ReportStatus("Deleted " & intFileDeleteCount & " file(s) from " & strCacheFolderPath &
                         ", recovering " & dblSizeDeletedMB.ToString("0.0") & " MB in disk space")

            If intFileDeleteErrorCount > 0 Then
                ReportStatus("Unable to delete " & intFileDeleteErrorCount & " file(s) from " & strCacheFolderPath, 0, True)
                Console.WriteLine("See the log file for details")
                For Each kvItem As KeyValuePair(Of String, Integer) In dctErrorSummary
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  " & kvItem.Key & ": " & kvItem.Value)
                Next
            End If

            If purgedFileLogEntries.Count > 0 Then
                ' Log the info for each of the deleted files
                Try
                    Using swPurgeLogFile = New StreamWriter(New FileStream(fiPurgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                        For Each purgedFileLogEntry In purgedFileLogEntries
                            swPurgeLogFile.WriteLine(purgedFileLogEntry)
                        Next
                    End Using
                Catch ex As Exception
                    ' Likely another manager tried to create the file at the same time
                    ' Ignore the error
                End Try
            End If

        Catch ex As Exception
            ReportStatus("Error in PurgeOldServerCacheFiles: " & clsGlobal.GetExceptionStackTrace(ex), 0, True)
        End Try
    End Sub

    ''' <summary>
    ''' Updates the dataset name to the final folder name in the transferFolderPath job parameter
    ''' Updates the transfer folder path to remove the final folder
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub RedefineAggregationJobDatasetAndTransferFolder()

        Dim strTransferFolderPath As String = m_jobParams.GetParam("transferFolderPath")
        Dim diTransferFolder As New DirectoryInfo(strTransferFolderPath)

        m_Dataset = diTransferFolder.Name
        strTransferFolderPath = diTransferFolder.Parent.FullName
        m_jobParams.SetParam("JobParameters", "transferFolderPath", strTransferFolderPath)

    End Sub

    ''' <summary>
    ''' Extracts the contents of the Version= line in a Tool Version Info file
    ''' </summary>
    ''' <param name="strDLLFilePath"></param>
    ''' <param name="strVersionInfoFilePath"></param>
    ''' <param name="strVersion"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ReadVersionInfoFile(strDLLFilePath As String, strVersionInfoFilePath As String, <Out()> ByRef strVersion As String) As Boolean

        ' Open strVersionInfoFilePath and read the Version= line
        Dim strLineIn As String
        Dim strKey As String
        Dim strValue As String
        Dim intEqualsLoc As Integer

        strVersion = String.Empty
        Dim blnSuccess = False

        Try

            If Not File.Exists(strVersionInfoFilePath) Then
                ReportStatus("Version Info File not found: " & strVersionInfoFilePath, 0, True)
                Return False
            End If

            Using srInFile = New StreamReader(New FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    If String.IsNullOrWhiteSpace(strLineIn) Then
                        Continue While
                    End If

                    intEqualsLoc = strLineIn.IndexOf("="c)

                    If intEqualsLoc > 0 Then
                        strKey = strLineIn.Substring(0, intEqualsLoc)

                        If intEqualsLoc < strLineIn.Length Then
                            strValue = strLineIn.Substring(intEqualsLoc + 1)
                        Else
                            strValue = String.Empty
                        End If

                        Select Case strKey.ToLower()
                            Case "filename"
                            Case "path"
                            Case "version"
                                strVersion = String.Copy(strValue)
                                If String.IsNullOrWhiteSpace(strVersion) Then
                                    ReportStatus("Empty version line in Version Info file for " & Path.GetFileName(strDLLFilePath), 0, True)
                                    blnSuccess = False
                                Else
                                    blnSuccess = True
                                End If
                            Case "error"
                                ReportStatus("Error reported by DLLVersionInspector for " & Path.GetFileName(strDLLFilePath) & ": " & strValue, 0, True)
                                blnSuccess = False
                            Case Else
                                ' Ignore the line
                        End Select
                    End If

                End While

            End Using

        Catch ex As Exception
            ReportStatus("Error reading Version Info File for " & Path.GetFileName(strDLLFilePath) & ": " & clsGlobal.GetExceptionStackTrace(ex), 0, True)
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Deletes files in specified directory that have been previously flagged as not wanted in results folder
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>List of files to delete is tracked via m_jobParams.ServerFilesToDelete; must store full file paths in ServerFilesToDelete</remarks>
    Public Function RemoveNonResultServerFiles() As Boolean

        Dim FileToDelete = ""

        Try
            ' Log status
            ReportStatus("Remove Files from the storage server; " &
                         "ServerFilesToDelete contains " & m_jobParams.ServerFilesToDelete.Count.ToString & " entries", 2)

            For Each FileToDelete In m_jobParams.ServerFilesToDelete

                ' Log file to be deleted
                ReportStatus("Deleting " & FileToDelete, 4)

                If File.Exists(FileToDelete) Then
                    'Verify file is not set to readonly, then delete it
                    File.SetAttributes(FileToDelete, File.GetAttributes(FileToDelete) And (Not FileAttributes.ReadOnly))
                    File.Delete(FileToDelete)
                End If
            Next
        Catch ex As Exception
            ReportStatus("clsGlobal.RemoveNonResultServerFiles(), Error deleting file " & FileToDelete, ex)
            'Even if an exception occurred, return true since the results were already copied back to the server
            Return True
        End Try

        Return True

    End Function

    Protected Function ReplaceUpdatedFile(fiOrginalFile As FileInfo, fiUpdatedFile As FileInfo) As Boolean

        Try
            Dim finalFilePath = fiOrginalFile.FullName

            Thread.Sleep(250)
            fiOrginalFile.Delete()

            Thread.Sleep(250)
            fiUpdatedFile.MoveTo(finalFilePath)

        Catch ex As Exception
            If m_DebugLevel >= 1 Then
                ReportStatus("Error in ReplaceUpdatedFile", ex)
            End If

            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Runs the analysis tool
    ''' Major work is performed by overrides
    ''' </summary>
    ''' <returns>CloseoutType enum representing completion status</returns>
    ''' <remarks></remarks>
    Public Overridable Function RunTool() As IJobParams.CloseOutType Implements IToolRunner.RunTool

        ' Synchronize the stored Debug level with the value stored in the database
        GetCurrentMgrSettingsFromDB()

        'Make log entry
        ReportStatus(m_MachName & ": Starting analysis, job " & m_JobNum)

        'Start the job timer
        m_StartTime = Date.UtcNow

        'Remainder of method is supplied by subclasses

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Creates a Tool Version Info file
    ''' </summary>
    ''' <param name="strFolderPath"></param>
    ''' <param name="strToolVersionInfo"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function SaveToolVersionInfoFile(strFolderPath As String, strToolVersionInfo As String) As Boolean

        Try
            Dim strStepToolName = m_jobParams.GetParam("StepTool")
            Dim strToolVersionFilePath = Path.Combine(strFolderPath, "Tool_Version_Info_" & strStepToolName & ".txt")

            Using swToolVersionFile = New StreamWriter(New FileStream(strToolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

                swToolVersionFile.WriteLine("Date: " & Date.Now().ToString(DATE_TIME_FORMAT))
                swToolVersionFile.WriteLine("Dataset: " & m_Dataset)
                swToolVersionFile.WriteLine("Job: " & m_JobNum)
                swToolVersionFile.WriteLine("Step: " & m_jobParams.GetParam("StepParameters", "Step"))
                swToolVersionFile.WriteLine("Tool: " & m_jobParams.GetParam("StepTool"))
                swToolVersionFile.WriteLine("ToolVersionInfo:")

                swToolVersionFile.WriteLine(strToolVersionInfo.Replace("; ", Environment.NewLine))

            End Using

        Catch ex As Exception
            ReportStatus("Exception saving tool version info", ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Communicates with database to record the tool version(s) for the current step task
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
    ''' <returns>True for success, False for failure</returns>
    ''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
    Protected Function SetStepTaskToolVersion(strToolVersionInfo As String) As Boolean
        Return SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
    End Function

    ''' <summary>
    ''' Communicates with database to record the tool version(s) for the current step task
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
    ''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
    ''' <returns>True for success, False for failure</returns>
    ''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
    Protected Function SetStepTaskToolVersion(strToolVersionInfo As String, ioToolFiles As List(Of FileInfo)) As Boolean

        Return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, True)
    End Function

    ''' <summary>
    ''' Communicates with database to record the tool version(s) for the current step task
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
    ''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
    ''' <returns>True for success, False for failure</returns>
    ''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
    Protected Function SetStepTaskToolVersion(strToolVersionInfo As String, ioToolFiles As IEnumerable(Of FileInfo)) As Boolean

        Return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, True)
    End Function

    ''' <summary>
    ''' Communicates with database to record the tool version(s) for the current step task
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
    ''' <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
    ''' <param name="blnSaveToolVersionTextFile">if true, then creates a text file with the tool version information</param>
    ''' <returns>True for success, False for failure</returns>
    ''' <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
    Protected Function SetStepTaskToolVersion(
      strToolVersionInfo As String,
      ioToolFiles As IEnumerable(Of FileInfo),
      blnSaveToolVersionTextFile As Boolean) As Boolean

        Dim strExeInfo As String = String.Empty
        Dim strToolVersionInfoCombined As String

        Dim Outcome As Boolean
        Dim ResCode As Integer

        If Not ioToolFiles Is Nothing Then

            For Each ioFileInfo As FileInfo In ioToolFiles
                Try
                    If ioFileInfo.Exists Then
                        strExeInfo = clsGlobal.AppendToComment(strExeInfo, ioFileInfo.Name & ": " & ioFileInfo.LastWriteTime.ToString(DATE_TIME_FORMAT))
                        ReportStatus("EXE Info: " & strExeInfo, 2)
                    Else
                        ReportStatus("Warning: Tool file not found: " & ioFileInfo.FullName)
                    End If

                Catch ex As Exception
                    ReportStatus("Exception looking up tool version file info", ex)
                End Try
            Next
        End If

        ' Append the .Exe info to strToolVersionInfo
        If String.IsNullOrEmpty(strExeInfo) Then
            strToolVersionInfoCombined = String.Copy(strToolVersionInfo)
        Else
            strToolVersionInfoCombined = clsGlobal.AppendToComment(strToolVersionInfo, strExeInfo)
        End If

        If blnSaveToolVersionTextFile Then
            SaveToolVersionInfoFile(m_WorkDir, strToolVersionInfoCombined)
        End If

        'Setup for execution of the stored procedure
        Dim myCmd As New SqlClient.SqlCommand() With {
            .CommandType = CommandType.StoredProcedure,
            .CommandText = SP_NAME_SET_TASK_TOOL_VERSION
        }

        myCmd.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue
        myCmd.Parameters.Add(New SqlClient.SqlParameter("@job", SqlDbType.Int)).Value = m_jobParams.GetJobParameter("StepParameters", "Job", 0)
        myCmd.Parameters.Add(New SqlClient.SqlParameter("@step", SqlDbType.Int)).Value = m_jobParams.GetJobParameter("StepParameters", "Step", 0)
        myCmd.Parameters.Add(New SqlClient.SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900)).Value = strToolVersionInfoCombined

        Dim objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

        ' Execute the stored procedure (retry the call up to 4 times)
        ResCode = objAnalysisTask.PipelineDBProcedureExecutor.ExecuteSP(myCmd, 4)

        If ResCode = 0 Then
            Outcome = True
        Else
            ReportStatus("Error " & ResCode.ToString & " storing tool version for current processing step", 0, True)
            Outcome = False
        End If

        Return Outcome

    End Function

    Protected Function SortTextFile(textFilePath As String, mergedFilePath As String, hasHeaderLine As Boolean) As Boolean
        Try
            Dim sortUtility = New FlexibleFileSortUtility.TextFileSorter

            mLastSortUtilityProgress = Date.UtcNow
            mSortUtilityErrorMessage = String.Empty

            sortUtility.WorkingDirectoryPath = m_WorkDir
            sortUtility.HasHeaderLine = hasHeaderLine
            sortUtility.ColumnDelimiter = ControlChars.Tab
            sortUtility.MaxFileSizeMBForInMemorySort = FlexibleFileSortUtility.TextFileSorter.DEFAULT_IN_MEMORY_SORT_MAX_FILE_SIZE_MB
            sortUtility.ChunkSizeMB = FlexibleFileSortUtility.TextFileSorter.DEFAULT_CHUNK_SIZE_MB

            AddHandler sortUtility.ProgressChanged, AddressOf mSortUtility_ProgressChanged
            AddHandler sortUtility.ErrorEvent, AddressOf mSortUtility_ErrorEvent
            AddHandler sortUtility.WarningEvent, AddressOf mSortUtility_WarningEvent
            AddHandler sortUtility.MessageEvent, AddressOf mSortUtility_MessageEvent

            Dim success = sortUtility.SortFile(textFilePath, mergedFilePath)

            If Not success Then
                If String.IsNullOrWhiteSpace(mSortUtilityErrorMessage) Then
                    m_message = "Unknown error sorting " & Path.GetFileName(textFilePath)
                Else
                    m_message = mSortUtilityErrorMessage
                End If
                Return False
            End If

            Return True

        Catch ex As Exception
            LogError("Exception in SortTextFile", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Uses Reflection to determine the version info for an assembly already loaded in memory
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
    ''' <param name="strAssemblyName">Assembly Name</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
    Protected Function StoreToolVersionInfoForLoadedAssembly(ByRef strToolVersionInfo As String, strAssemblyName As String) As Boolean
        Return StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, strAssemblyName, blnIncludeRevision:=True)
    End Function

    ''' <summary>
    ''' Uses Reflection to determine the version info for an assembly already loaded in memory
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
    ''' <param name="strAssemblyName">Assembly Name</param>
    ''' <param name="blnIncludeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
    Protected Function StoreToolVersionInfoForLoadedAssembly(ByRef strToolVersionInfo As String, strAssemblyName As String, blnIncludeRevision As Boolean) As Boolean

        Try
            Dim oAssemblyName As Reflection.AssemblyName
            oAssemblyName = Reflection.Assembly.Load(strAssemblyName).GetName

            Dim strNameAndVersion As String
            If blnIncludeRevision Then
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
            Else
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.Major & "." & oAssemblyName.Version.Minor & "." & oAssemblyName.Version.Build
            End If

            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

        Catch ex As Exception
            ReportStatus("Exception determining Assembly info for " & strAssemblyName, ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Determines the version info for a .NET DLL using reflection
    ''' If reflection fails, then uses System.Diagnostics.FileVersionInfo
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
    ''' <param name="strDLLFilePath">Path to the DLL</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Public Function StoreToolVersionInfoOneFile(ByRef strToolVersionInfo As String, strDLLFilePath As String) As Boolean

        Dim ioFileInfo As FileInfo
        Dim blnSuccess As Boolean

        Try
            ioFileInfo = New FileInfo(strDLLFilePath)

            If Not ioFileInfo.Exists Then
                ReportStatus("Warning: File not found by StoreToolVersionInfoOneFile: " & strDLLFilePath)
                Return False
            Else

                Dim oAssembly = Reflection.Assembly.LoadFrom(ioFileInfo.FullName)
                Dim oAssemblyName = oAssembly.GetName()

                Dim strNameAndVersion As String
                strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

                blnSuccess = True
            End If

        Catch ex As BadImageFormatException
            ' Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
            ' Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

            ' Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

            ' Use this when compiled as AnyCPU
            blnSuccess = StoreToolVersionInfoOneFile32Bit(strToolVersionInfo, strDLLFilePath)

            ' Use this when compiled as 32-bit
            'blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, strDLLFilePath)


        Catch ex As Exception
            ' If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
            '  <startup useLegacyV2RuntimeActivationPolicy="true">
            '    <supportedRuntime version="v4.0" />
            '  </startup>
            ReportStatus("Exception determining Assembly info for " & Path.GetFileName(strDLLFilePath), ex)
            blnSuccess = False
        End Try

        If Not blnSuccess Then
            blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strDLLFilePath)
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
    ''' </summary>
    ''' <param name="strToolVersionInfo">Version info string to append the version info to</param>
    ''' <param name="strDLLFilePath">Path to the DLL</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfoViaSystemDiagnostics(ByRef strToolVersionInfo As String, strDLLFilePath As String) As Boolean

        Try
            Dim ioFileInfo = New FileInfo(strDLLFilePath)

            If Not ioFileInfo.Exists Then
                m_message = "File not found by StoreToolVersionInfoViaSystemDiagnostics"
                ReportStatus(m_message & ": " & strDLLFilePath)
                Return False
            End If

            Dim oFileVersionInfo = FileVersionInfo.GetVersionInfo(strDLLFilePath)

            Dim strName As String
            Dim strVersion As String

            strName = oFileVersionInfo.FileDescription
            If String.IsNullOrEmpty(strName) Then
                strName = oFileVersionInfo.InternalName
            End If

            If String.IsNullOrEmpty(strName) Then
                strName = oFileVersionInfo.FileName
            End If

            If String.IsNullOrEmpty(strName) Then
                strName = ioFileInfo.Name
            End If

            strVersion = oFileVersionInfo.FileVersion
            If String.IsNullOrEmpty(strVersion) Then
                strVersion = oFileVersionInfo.ProductVersion
            End If

            If String.IsNullOrEmpty(strVersion) Then
                strVersion = "??"
            End If

            Dim strNameAndVersion As String
            strNameAndVersion = strName & ", Version=" & strVersion
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

            Return True

        Catch ex As Exception
            LogError("Exception determining File Version for " & Path.GetFileName(strDLLFilePath), ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
    ''' </summary>
    ''' <param name="strToolVersionInfo"></param>
    ''' <param name="strDLLFilePath"></param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfoOneFile32Bit(ByRef strToolVersionInfo As String, strDLLFilePath As String) As Boolean
        Return StoreToolVersionInfoOneFileUseExe(strToolVersionInfo, strDLLFilePath, "DLLVersionInspector_x86.exe")
    End Function

    ''' <summary>
    ''' Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
    ''' </summary>
    ''' <param name="strToolVersionInfo"></param>
    ''' <param name="strDLLFilePath"></param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfoOneFile64Bit(ByRef strToolVersionInfo As String, strDLLFilePath As String) As Boolean
        Return StoreToolVersionInfoOneFileUseExe(strToolVersionInfo, strDLLFilePath, "DLLVersionInspector_x64.exe")
    End Function

    ''' <summary>
    ''' Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
    ''' </summary>
    ''' <param name="strToolVersionInfo"></param>
    ''' <param name="strDLLFilePath"></param>
    ''' <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfoOneFileUseExe(
      ByRef strToolVersionInfo As String,
      strDLLFilePath As String,
      versionInspectorExeName As String) As Boolean

        Dim strAppPath As String
        Dim strVersionInfoFilePath As String
        Dim strArgs As String

        Dim ioFileInfo As FileInfo

        Try
            strAppPath = Path.Combine(clsGlobal.GetAppFolderPath(), versionInspectorExeName)

            ioFileInfo = New FileInfo(strDLLFilePath)

            If Not ioFileInfo.Exists Then
                m_message = "File not found by StoreToolVersionInfoOneFileUseExe"
                ReportStatus(m_message & ": " & strDLLFilePath, 0, True)
                Return False
            ElseIf Not File.Exists(strAppPath) Then
                m_message = "DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe"
                ReportStatus(m_message & ": " & strAppPath, 0, True)
                Return False
            End If

            ' Call DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe to determine the tool version

            strVersionInfoFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.Name) & "_VersionInfo.txt")

            Dim blnSuccess As Boolean
            Dim strVersion As String = String.Empty

            strArgs = PossiblyQuotePath(ioFileInfo.FullName) & " /O:" & PossiblyQuotePath(strVersionInfoFilePath)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strAppPath & " " & strArgs)
            End If

            Dim objProgRunner = New clsRunDosProgram(clsGlobal.GetAppFolderPath()) With {
                .CacheStandardOutput = False,
                .CreateNoWindow = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = False,
                .DebugLevel = 1,
                .MonitorInterval = 250
            }

            blnSuccess = objProgRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", False)

            If Not blnSuccess Then
                Return False
            End If

            Thread.Sleep(100)

            blnSuccess = ReadVersionInfoFile(strDLLFilePath, strVersionInfoFilePath, strVersion)

            ' Delete the version info file
            Try
                If File.Exists(strVersionInfoFilePath) Then
                    Thread.Sleep(100)
                    File.Delete(strVersionInfoFilePath)
                End If
            Catch ex As Exception
                ' Ignore errors here
            End Try

            If Not blnSuccess OrElse String.IsNullOrWhiteSpace(strVersion) Then
                Return False
            End If

            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strVersion)

            Return True

        Catch ex As Exception
            m_message = "Exception determining Version info for " & Path.GetFileName(strDLLFilePath) & " using " & versionInspectorExeName
            ReportStatus(m_message, ex)
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, Path.GetFileNameWithoutExtension(strDLLFilePath))
        End Try

        Return False

    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function SynchronizeFolders(sourceFolderPath As String, targetDirectoryPath As String) As Boolean
        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, "*")
    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="copySubfolders">If true, then recursively copies subfolders</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function SynchronizeFolders(sourceFolderPath As String, targetDirectoryPath As String, copySubfolders As Boolean) As Boolean

        Dim lstFileNameFilterSpec = New List(Of String) From {"*"}
        Dim lstFileNameExclusionSpec = New List(Of String)
        Const maxRetryCount = 3

        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)
    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="fileNameFilterSpec">Filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Will retry failed copies up to 3 times</remarks>
    Protected Function SynchronizeFolders(
      sourceFolderPath As String,
      targetDirectoryPath As String,
      fileNameFilterSpec As String) As Boolean

        Dim lstFileNameFilterSpec = New List(Of String) From {fileNameFilterSpec}
        Dim lstFileNameExclusionSpec = New List(Of String)
        Const maxRetryCount = 3
        Const copySubfolders = False

        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)
    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Will retry failed copies up to 3 times</remarks>
    Protected Function SynchronizeFolders(
      sourceFolderPath As String,
      targetDirectoryPath As String,
      lstFileNameFilterSpec As List(Of String)) As Boolean

        Dim lstFileNameExclusionSpec = New List(Of String)
        Const maxRetryCount = 3
        Const copySubfolders = False

        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)
    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
    ''' <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Will retry failed copies up to 3 times</remarks>
    Protected Function SynchronizeFolders(
      sourceFolderPath As String,
      targetDirectoryPath As String,
      lstFileNameFilterSpec As List(Of String),
      lstFileNameExclusionSpec As List(Of String)) As Boolean

        Const maxRetryCount = 3
        Const copySubfolders = False

        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)
    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
    ''' <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
    ''' <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function SynchronizeFolders(
      sourceFolderPath As String,
      targetDirectoryPath As String,
      lstFileNameFilterSpec As List(Of String),
      lstFileNameExclusionSpec As List(Of String),
      maxRetryCount As Integer) As Boolean

        Const copySubfolders = False
        Return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)

    End Function

    ''' <summary>
    ''' Copies new/changed files from the source folder to the target folder
    ''' </summary>
    ''' <param name="sourceFolderPath"></param>
    ''' <param name="targetDirectoryPath"></param>
    ''' <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
    ''' <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
    ''' <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
    ''' <param name="copySubfolders">If true, then recursively copies subfolders</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function SynchronizeFolders(
      sourceFolderPath As String,
      targetDirectoryPath As String,
      lstFileNameFilterSpec As List(Of String),
      lstFileNameExclusionSpec As List(Of String),
      maxRetryCount As Integer,
      copySubfolders As Boolean) As Boolean

        Try
            Dim diSourceFolder = New DirectoryInfo(sourceFolderPath)
            Dim ditargetDirectory = New DirectoryInfo(targetDirectoryPath)

            If Not ditargetDirectory.Exists Then
                ditargetDirectory.Create()
            End If

            If lstFileNameFilterSpec Is Nothing Then
                lstFileNameFilterSpec = New List(Of String)
            End If

            If lstFileNameFilterSpec.Count = 0 Then lstFileNameFilterSpec.Add("*")

            Dim lstFilesToCopy = New SortedSet(Of String)

            For Each filterSpec In lstFileNameFilterSpec
                If String.IsNullOrWhiteSpace(filterSpec) Then
                    filterSpec = "*"
                End If

                For Each fiFile In diSourceFolder.GetFiles(filterSpec)
                    If Not lstFilesToCopy.Contains(fiFile.Name) Then
                        lstFilesToCopy.Add(fiFile.Name)
                    End If
                Next
            Next

            If Not lstFileNameExclusionSpec Is Nothing AndAlso lstFileNameExclusionSpec.Count > 0 Then
                ' Remove any files from lstFilesToCopy that would get matched by items in lstFileNameExclusionSpec

                For Each filterSpec In lstFileNameExclusionSpec
                    If Not String.IsNullOrWhiteSpace(filterSpec) Then
                        For Each fiFile In diSourceFolder.GetFiles(filterSpec)
                            If lstFilesToCopy.Contains(fiFile.Name) Then
                                lstFilesToCopy.Remove(fiFile.Name)
                            End If
                        Next
                    End If
                Next
            End If

            For Each fileName In lstFilesToCopy
                Dim fiSourceFile = New FileInfo(Path.Combine(diSourceFolder.FullName, fileName))
                Dim fiTargetFile = New FileInfo(Path.Combine(ditargetDirectory.FullName, fileName))
                Dim copyFile = False

                If Not fiTargetFile.Exists Then
                    copyFile = True
                ElseIf fiTargetFile.Length <> fiSourceFile.Length Then
                    copyFile = True
                ElseIf fiTargetFile.LastWriteTimeUtc < fiSourceFile.LastWriteTimeUtc Then
                    copyFile = True
                End If

                If copyFile Then
                    Dim retriesRemaining = maxRetryCount

                    Dim success = False
                    While Not success
                        success = m_FileTools.CopyFileUsingLocks(fiSourceFile, fiTargetFile.FullName, m_MachName, True)
                        If Not success Then
                            retriesRemaining -= 1
                            If retriesRemaining < 0 Then
                                m_message = "Error copying " & fiSourceFile.FullName & " to " & fiTargetFile.Directory.FullName
                                Return False
                            End If

                            ReportStatus("Error copying " & fiSourceFile.FullName & " to " & fiTargetFile.Directory.FullName &
                                         "; RetriesRemaining: " & retriesRemaining, 0, True)

                            ' Wait 2 seconds then try again
                            Thread.Sleep(2000)
                        End If
                    End While

                End If
            Next

            If copySubfolders Then
                Dim lstSubFolders = diSourceFolder.GetDirectories()

                For Each diSubFolder In lstSubFolders
                    Dim subfolderTargetPath = Path.Combine(targetDirectoryPath, diSubFolder.Name)
                    Dim success = SynchronizeFolders(diSubFolder.FullName, subfolderTargetPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders)

                    If Not success Then
                        LogError("Error copying subfolder " & diSubFolder.FullName & " to " & targetDirectoryPath)
                        Exit For
                    End If

                Next
            End If

        Catch ex As Exception
            LogError("Error in SynchronizeFolders", ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Updates the analysis summary file
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function UpdateSummaryFile() As Boolean
        Dim strTool As String
        Dim strToolAndStepTool As String
        Try
            'Add a separator
            m_SummaryFile.Add(Environment.NewLine)
            m_SummaryFile.Add("=====================================================================================")
            m_SummaryFile.Add(Environment.NewLine)

            ' Construct the Tool description (combination of Tool name and Step Tool name)
            strTool = m_jobParams.GetParam("ToolName")

            strToolAndStepTool = m_jobParams.GetParam("StepTool")
            If strToolAndStepTool Is Nothing Then strToolAndStepTool = String.Empty

            If strToolAndStepTool <> strTool Then
                If strToolAndStepTool.Length > 0 Then
                    strToolAndStepTool &= " (" & strTool & ")"
                Else
                    strToolAndStepTool &= strTool
                End If
            End If

            'Add the data
            m_SummaryFile.Add("Job Number" & ControlChars.Tab & m_JobNum)
            m_SummaryFile.Add("Job Step" & ControlChars.Tab & m_jobParams.GetParam("StepParameters", "Step"))
            m_SummaryFile.Add("Date" & ControlChars.Tab & Date.Now().ToString)
            m_SummaryFile.Add("Processor" & ControlChars.Tab & m_MachName)
            m_SummaryFile.Add("Tool" & ControlChars.Tab & strToolAndStepTool)
            m_SummaryFile.Add("Dataset Name" & ControlChars.Tab & m_Dataset)
            m_SummaryFile.Add("Xfer Folder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
            m_SummaryFile.Add("Param File Name" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
            m_SummaryFile.Add("Settings File Name" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
            m_SummaryFile.Add("Legacy Organism Db Name" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
            m_SummaryFile.Add("Protein Collection List" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
            m_SummaryFile.Add("Protein Options List" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
            m_SummaryFile.Add("Fasta File Name" & ControlChars.Tab & m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
            m_SummaryFile.Add("Analysis Time (hh:mm:ss)" & ControlChars.Tab & CalcElapsedTime(m_StartTime, m_StopTime))

            'Add another separator
            m_SummaryFile.Add(Environment.NewLine)
            m_SummaryFile.Add("=====================================================================================")
            m_SummaryFile.Add(Environment.NewLine)

        Catch ex As Exception
            ReportStatus("Error creating summary file, job " & m_JobNum &
                         ", step " & m_jobParams.GetParam("StepParameters", "Step") & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Unzips all files in the specified Zip file
    ''' Output folder is m_WorkDir
    ''' </summary>
    ''' <param name="zipFilePath">File to unzip</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function UnzipFile(zipFilePath As String) As Boolean
        Return UnzipFile(zipFilePath, m_WorkDir, String.Empty)
    End Function

    ''' <summary>
    ''' Unzips all files in the specified Zip file
    ''' Output folder is targetDirectory
    ''' </summary>
    ''' <param name="zipFilePath">File to unzip</param>
    ''' <param name="targetDirectory">Target directory for the extracted files</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function UnzipFile(zipFilePath As String, targetDirectory As String) As Boolean
        Return UnzipFile(zipFilePath, targetDirectory, String.Empty)
    End Function

    ''' <summary>
    ''' Unzips files in the specified Zip file that match the FileFilter spec
    ''' Output folder is targetDirectory
    ''' </summary>
    ''' <param name="zipFilePath">File to unzip</param>
    ''' <param name="targetDirectory">Target directory for the extracted files</param>
    ''' <param name="FileFilter">FilterSpec to apply, for example *.txt</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function UnzipFile(zipFilePath As String, targetDirectory As String, FileFilter As String) As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        Return m_IonicZipTools.UnzipFile(zipFilePath, targetDirectory, FileFilter)

    End Function

    ''' <summary>
    ''' Reset the progrunner start time and the CPU usage queue
    ''' </summary>
    ''' <remarks>Public because used by clsDtaGenThermoRaw</remarks>
    Public Sub ResetProgRunnerCpuUsage()
        mProgRunnerStartTime = Date.UtcNow
        mCoreUsageHistory.Clear()
    End Sub

    ''' <summary>
    ''' Update the CPU usage by monitoring a process by nme
    ''' </summary>
    ''' <param name="processName">Process name, for example chrome (do not include .exe)</param>
    ''' <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
    ''' <param name="defaultProcessID">Process ID to use if not match for processName</param>
    ''' <returns>Actual CPU usage; -1 if an error</returns>
    ''' <remarks>This method is used by clsAnalysisToolRunnerDtaRefinery to monitor X!Tandem and DTA_Refinery</remarks>
    Protected Function UpdateCpuUsageByProcessName(processName As String, secondsBetweenUpdates As Integer, defaultProcessID As Integer) As Single
        Try
            Dim processIDs As List(Of Integer) = Nothing
            Dim processID As Integer = defaultProcessID

            Dim coreUsage = PRISM.Processes.clsProgRunner.GetCoreUsageByProcessName(processName, processIDs)
            If processIDs.Count > 0 Then
                processID = processIDs.First
            End If

            If coreUsage > -1 Then
                UpdateProgRunnerCpuUsage(processID, coreUsage, secondsBetweenUpdates)
            End If

            Return coreUsage

        Catch ex As Exception
            ReportStatus("Exception in UpdateCpuUsageByProcessName determining the processor usage of " & processName, ex)
            Return -1
        End Try

    End Function

    ''' <summary>
    ''' Cache the new core usage value
    ''' Note: call ResetProgRunnerCpuUsage just before calling CmdRunner.RunProgram()
    ''' </summary>
    ''' <param name="processID">ProcessID of the externally running process</param>
    ''' <param name="coreUsage">Number of cores in use by the process; -1 if unknown</param>
    ''' <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
    ''' <remarks>This method is used by this class and by clsAnalysisToolRunnerMODPlus</remarks>
    Protected Sub UpdateProgRunnerCpuUsage(processID As Integer, coreUsage As Single, secondsBetweenUpdates As Integer)

        ' Cache the core usage values for the last 5 minutes
        If coreUsage >= 0 Then
            mCoreUsageHistory.Enqueue(New KeyValuePair(Of DateTime, Single)(Date.Now, coreUsage))

            If secondsBetweenUpdates < 10 Then
                If mCoreUsageHistory.Count > 5 * 60 / 10 Then
                    mCoreUsageHistory.Dequeue()
                End If
            Else
                If mCoreUsageHistory.Count > 5 * 60 / secondsBetweenUpdates Then
                    mCoreUsageHistory.Dequeue()
                End If
            End If

        End If

        If mCoreUsageHistory.Count > 0 Then
            m_StatusTools.ProgRunnerProcessID = processID

            m_StatusTools.StoreCoreUsageHistory(mCoreUsageHistory)

            ' If the Program has been running for at least 3 minutes, store the actual CoreUsage in the database
            If Date.UtcNow.Subtract(mProgRunnerStartTime).TotalMinutes >= 3 Then

                ' Average the data in the history queue

                Dim coreUsageAvg = (From item In mCoreUsageHistory.ToArray() Select item.Value).Average()

                m_StatusTools.ProgRunnerCoreUsage = coreUsageAvg
            End If

        End If

    End Sub

    ''' <summary>
    ''' Update the cached values in mCoreUsageHistory
    ''' Note: call ResetProgRunnerCpuUsage just before calling CmdRunner.RunProgram()
    ''' Then, when handling the LoopWaiting event from the cmdRunner instance
    ''' call this method every secondsBetweenUpdates seconds (typically 30)
    ''' </summary>
    ''' <param name="CmdRunner">clsRunDosProgram instance used to run an external process</param>
    ''' <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
    ''' <remarks>Public because used by clsDtaGenThermoRaw</remarks>
    Public Sub UpdateProgRunnerCpuUsage(cmdRunner As clsRunDosProgram, secondsBetweenUpdates As Integer)

        ' Note that the call to GetCoreUsage() will take at least 1 second
        Dim coreUsage = cmdRunner.GetCoreUsage()

        UpdateProgRunnerCpuUsage(cmdRunner.ProcessID, coreUsage, secondsBetweenUpdates)

    End Sub

    ''' <summary>
    ''' Update Status.xml every 15 seconds using m_progress
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusFile()
        UpdateStatusFile(m_progress)
    End Sub

    ''' <summary>
    ''' Update Status.xml every 15 seconds using sngPercentComplete
    ''' </summary>
    ''' <param name="sngPercentComplete">Percent complete</param>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusFile(sngPercentComplete As Single)
        Dim frequencySeconds = 15
        UpdateStatusFile(sngPercentComplete, frequencySeconds)
    End Sub

    ''' <summary>
    ''' Update Status.xml every frequencySeconds seconds using sngPercentComplete
    ''' </summary>
    ''' <param name="sngPercentComplete">Percent complete</param>
    ''' <param name="frequencySeconds">Minimum time between updates, in seconds (must be at least 5)</param>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusFile(sngPercentComplete As Single, frequencySeconds As Integer)

        If frequencySeconds < 5 Then frequencySeconds = 5

        ' Update the status file (limit the updates to every x seconds)
        If Date.UtcNow.Subtract(m_LastStatusFileUpdate).TotalSeconds >= frequencySeconds Then
            m_LastStatusFileUpdate = Date.UtcNow
            UpdateStatusRunning(sngPercentComplete)
        End If

    End Sub

    ''' <summary>
    ''' Update Status.xml now using m_progress
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusRunning()
        UpdateStatusRunning(m_progress)
    End Sub

    ''' <summary>
    ''' Update Status.xml now using sngPercentComplete
    ''' </summary>
    ''' <param name="sngPercentComplete"></param>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusRunning(sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

    ''' <summary>
    ''' Update Status.xml now using sngPercentComplete and spectrumCountTotal
    ''' </summary>
    ''' <param name="sngPercentComplete"></param>
    ''' <param name="spectrumCountTotal"></param>
    ''' <remarks></remarks>
    Protected Sub UpdateStatusRunning(sngPercentComplete As Single, spectrumCountTotal As Integer)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, spectrumCountTotal, "", "", "", False)
    End Sub

    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at least one spectrum in it
    ''' </summary>
    ''' <returns>True if success; false if failure</returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strDTAFilePath As String

        strDTAFilePath = Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

        Return ValidateCDTAFile(strDTAFilePath)

    End Function

    Protected Function ValidateCDTAFile(strDTAFilePath As String) As Boolean

        Dim strLineIn As String
        Dim blnDataFound = False

        Try
            If Not File.Exists(strDTAFilePath) Then
                LogError("_DTA.txt file not found", strDTAFilePath)
                Return False
            End If

            Using srReader = New StreamReader(New FileStream(strDTAFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srReader.EndOfStream
                    strLineIn = srReader.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then
                        blnDataFound = True
                        Exit Do
                    End If
                Loop

            End Using

            If Not blnDataFound Then
                LogError("The _DTA.txt file is empty")
            End If

        Catch ex As Exception
            LogError("Exception in ValidateCDTAFile", ex)
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Verifies that the zip file exists.  
    ''' If the file size is less than crcCheckThresholdGB, then also performs a full CRC check of the data
    ''' </summary>
    ''' <param name="zipFilePath">Zip file to check</param>
    ''' <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
    ''' <returns>True if a valid zip file, otherwise false</returns>
    Protected Function VerifyZipFile(zipFilePath As String, Optional crcCheckThresholdGB As Single = 4) As Boolean

        Dim success As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        success = m_IonicZipTools.VerifyZipFile(zipFilePath, crcCheckThresholdGB)

        Return success

    End Function

    ''' <summary>
    ''' Stores sourceFilePath in a zip file with the same name, but extension .zip
    ''' </summary>
    ''' <param name="sourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(sourceFilePath As String, deleteSourceAfterZip As Boolean) As Boolean

        Dim success As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        success = m_IonicZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip)

        If Not success AndAlso m_IonicZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower) Then
            m_NeedToAbortProcessing = True
        End If

        Return success

    End Function

    ''' <summary>
    ''' Compress a file using SharpZipLib
    ''' </summary>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>IonicZip is faster, so we typically use function ZipFile</remarks>
    Public Function ZipFileSharpZipLib(sourceFilePath As String) As Boolean

        Try
            Dim fiSourceFile = New FileInfo(sourceFilePath)

            Dim zipFilePath = GetZipFilePathForFile(sourceFilePath)

            Try
                If File.Exists(zipFilePath) Then

                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .zip file: " & zipFilePath)
                    End If

                    File.Delete(zipFilePath)
                    Thread.Sleep(250)

                End If
            Catch ex As Exception
                LogError("Error deleting target .zip file prior to zipping file " & sourceFilePath & " using SharpZipLib", ex)
                Return False
            End Try

            Dim zipper = New ICSharpCode.SharpZipLib.Zip.FastZip()
            zipper.CreateZip(zipFilePath, fiSourceFile.DirectoryName, False, fiSourceFile.Name)

            ' Verify that the zip file is not corrupt
            ' Files less than 4 GB get a full CRC check
            ' Large files get a quick check
            If Not VerifyZipFile(zipFilePath) Then
                Return False
            End If

            Return True

        Catch ex As Exception
            LogError("Exception zipping " & sourceFilePath & " using SharpZipLib", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Stores sourceFilePath in a zip file named zipFilePath
    ''' </summary>
    ''' <param name="sourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <param name="zipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(sourceFilePath As String, deleteSourceAfterZip As Boolean, zipFilePath As String) As Boolean
        Dim success As Boolean
        m_IonicZipTools.DebugLevel = m_DebugLevel

        ' Note that m_IonicZipTools logs error messages using clsLogTools
        success = m_IonicZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath)

        If Not success AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
            m_NeedToAbortProcessing = True
        End If

        Return success

    End Function

    Protected Function ZipOutputFile(fiResultsFile As FileInfo, fileDescription As String) As Boolean

        Try
            If String.IsNullOrWhiteSpace(fileDescription) Then fileDescription = "Unknown_Source"

            If Not ZipFile(fiResultsFile.FullName, False) Then
                LogError("Error zipping " & fileDescription & " results file")
                Return False
            End If

            ' Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
            m_jobParams.AddResultFileToSkip(fiResultsFile.Name)

        Catch ex As Exception
            LogError("Exception zipping " & fileDescription & " results file", ex)
            Return False
        End Try

        Return True

    End Function

#End Region

#Region "Event Handlers"

    Private Sub mSortUtility_ErrorEvent(sender As Object, e As FlexibleFileSortUtility.MessageEventArgs)
        mSortUtilityErrorMessage = e.Message
        ReportStatus("SortUtility: " & e.Message, 0, True)
    End Sub

    Private Sub mSortUtility_MessageEvent(sender As Object, e As FlexibleFileSortUtility.MessageEventArgs)
        If m_DebugLevel >= 1 Then
            ReportStatus(e.Message)
        End If
    End Sub

    Private Sub mSortUtility_ProgressChanged(sender As Object, e As FlexibleFileSortUtility.ProgressChangedEventArgs)
        If m_DebugLevel >= 1 AndAlso Date.UtcNow.Subtract(mLastSortUtilityProgress).TotalSeconds >= 5 Then
            mLastSortUtilityProgress = Date.UtcNow
            ReportStatus(e.taskDescription & ": " & e.percentComplete.ToString("0.0") & "% complete")
        End If
    End Sub

    Private Sub mSortUtility_WarningEvent(sender As Object, e As FlexibleFileSortUtility.MessageEventArgs)
        ReportStatus("SortUtility Warning: " & e.Message)
    End Sub

    Private Sub m_MyEMSLUtilities_ErrorEvent(strMessage As String)
        LogError(strMessage)
    End Sub

    Private Sub m_MyEMSLUtilities_WarningEvent(strMessage As String)
        ReportStatus("MyEMSL Warning: " & strMessage)
    End Sub

#End Region

End Class


