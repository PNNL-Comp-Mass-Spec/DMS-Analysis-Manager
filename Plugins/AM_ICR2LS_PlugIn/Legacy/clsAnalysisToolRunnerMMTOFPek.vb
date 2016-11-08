Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMMTOFPek
    Inherits clsAnalysisToolRunnerICRBase

    Public Sub New()
    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim PekRes As Boolean
        Dim ParamFilePath As String
    
        'Start with base class function to get settings information
        ResCode = MyBase.RunTool()
        If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

        'Verify a param file has been specified
        ParamFilePath = System.IO.Path.Combine(m_WorkDir, m_JobParams.GetParam("parmFileName"))
        If Not System.IO.File.Exists(ParamFilePath) Then
            'Param file wasn't specified, but is required for ICR-2LS analysis
            m_message = "ICR-2LS Param file not found"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ParamFilePath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add handling of settings file info here if it becomes necessary in the future
        '(Settings file is handled by base class)

        'Assemble the dataset folder name for input to ICR2LS
        Dim FoundFolders() As String = Directory.GetDirectories(m_WorkDir, "*.raw")
        If FoundFolders.GetLength(0) <> 1 Then
            m_message = "Unable to find data files in working directory"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        Dim DSNamePath As String = Path.Combine(m_workdir, FoundFolders(0))

        'Assemble other input parameters
        Dim ParmFileNamePath As String = Path.Combine(m_WorkDir, m_JobParams.GetParam("parmFileName"))
        Dim OutFileNamePath As String = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".pek")
        Dim FilterNum As Integer = m_settingsFileParams.GetParam("mmtofsettings", "filternum", 1)
        Dim SGFilter As Boolean = m_settingsFileParams.GetParam("mmtofsettings", "sdfilter", True)

        'Make the PEK file
        m_JobRunning = True
        PekRes = m_ICR2LSObj.MakeMMTOFPEKFile(DSNamePath, ParmFileNamePath, OutFileNamePath, FilterNum, SGFilter)
        If Not PekRes Then
            m_message = "Error creating PEK file"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Wait for the job to complete
        If Not WaitForJobToFinish() Then
            m_message = "Error waiting for PEK job to finish"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks("ICR") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            m_message = AppendToComment(m_message, "Error performing post analysis tasks")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

        'Deletes the .raw dataset folder
        Dim RetryCount As Integer = 0
        Dim ErrMsg As String = ""

        While RetryCount < 3
            Try
                System.Threading.Thread.Sleep(5000)				'Allow time for ICR2LS to release file locks
                Dim FoundFolders() As String = Directory.GetDirectories(m_workdir, "*.raw")
                If FoundFolders.GetLength(0) < 1 Then Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                For Each WorkFldr As String In FoundFolders
                    Directory.Delete(WorkFldr, True)
                Next
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Catch Err As IOException
                'If problem is locked file, retry
                If m_DebugLevel > 0 Then
                    m_logger.PostEntry("Error deleting data folder, attempt #" & RetryCount.ToString, ILogger.logMsgType.logError, True)
                End If
                ErrMsg = err.Message
                RetryCount += 1
            Catch Err As Exception
                m_logger.PostError("Error deleting raw data folders, job " & m_JobNum, Err, LOG_DATABASE)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
        End While

        'If we got to here, then we've exceeded the max retry limit
        m_logger.PostEntry("Unable to delete raw data folder after multiple tries, job " & m_jobnum & _
             ", Error " & ErrMsg, ILogger.logMsgType.logError, LOG_DATABASE)
        Return IJobParams.CloseOutType.CLOSEOUT_FAILED

    End Function

End Class
