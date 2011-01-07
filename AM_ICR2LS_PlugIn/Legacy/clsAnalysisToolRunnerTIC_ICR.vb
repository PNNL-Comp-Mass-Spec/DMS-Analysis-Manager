Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerTIC_ICR
    Inherits clsAnalysisToolRunnerICRBase

    Public Sub New()
    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim DSNamePath As String
        Dim BaseTIC As Boolean
        Dim TICResult As Boolean

        'Start with base class function to get settings information
        ResCode = MyBase.RunTool()
        If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

        If m_JobParams.GetParam("settingsFileName") <> "na" Then
            BaseTIC = m_settingsFileParams.GetParam("TIC_Analysis", "BasePeak", False)
        Else
            BaseTIC = False
        End If

        'Assemble the raw file name
        DSNamePath = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum"))
        If Not Directory.Exists(DSNamePath) Then
            CleanupFailedJob("Unable to find data files in working directory")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Make the TIC file
        m_JobRunning = True
        m_ICR2LSObj.BrukerFlag = clsICR2LSWrapper.BRUKERCHK.BRUKER_CHK_NONE
        If BaseTIC Then
            TICResult = m_ICR2LSObj.MakeICRTICFile(CheckTerminator(DSNamePath), CheckTerminator(m_workdir), _
             Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName")))
        Else
            TICResult = m_ICR2LSObj.MakeICRTICFile(CheckTerminator(DSNamePath), CheckTerminator(m_workdir), _
             Path.Combine(m_workdir, m_JobParams.GetParam("parmFileName")), True)
        End If
        If Not TICResult Then
            CleanupFailedJob("Error creating TIC file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Wait for the job to complete
        If Not WaitForJobToFinish() Then
            CleanupFailedJob("Error waiting for TIC job to finish")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        If PerfPostAnalysisTasks("TIC") <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            m_message = AppendToComment(m_message, "Error performing post analysis tasks")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

        'Deletes the dataset folder containing s-folders from the working directory

        Try
            Directory.Delete(Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum")), True)
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Catch Err As Exception
            m_logger.PostError("Error deleting raw data files, job " & m_JobNum, Err, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

End Class

