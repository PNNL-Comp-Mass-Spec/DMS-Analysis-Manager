Imports PRISM.Logging
Imports System.IO
Imports AnalysisManagerBase.clsGlobal


Public Class clsAnalysisToolRunnerTIC_LCQ
    Inherits clsAnalysisToolRunnerICRBase

    Public Sub New()
    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim ResCode As IJobParams.CloseOutType
        Dim RawFileName As String

        'Start with base class function to get settings information
        ResCode = MyBase.RunTool()
        If ResCode <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return ResCode

        'Assemble the raw file name
        RawFileName = Path.Combine(m_workdir, m_JobParams.GetParam("datasetNum") & ".raw")
        If Not File.Exists(RawFileName) Then
            m_message = "Unable to find data file in working directory"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Make the TIC file
        m_JobRunning = True
        If Not MakeLCQTICFile(RawFileName, RawFileName & ".tic") Then
            m_message = "Error creating TIC file"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Wait for the job to complete
        If Not WaitForJobToFinish() Then
            m_message = "Error waiting for TIC job to finish"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Run the cleanup routine from the base class
        PerfPostAnalysisTasks("TIC")

    End Function

    Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

        'Deletes the .raw file from the working directory
        Dim FoundFiles() As String
        Dim MyFile As String

        'Delete the .raw file
        Try
            FoundFiles = Directory.GetFiles(m_workdir, "*.raw")
            For Each MyFile In FoundFiles
                DeleteFileWithRetries(MyFile)
            Next
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Catch Err As Exception
            m_logger.PostError("Error deleting .raw file, job " & m_JobNum, Err, LOG_DATABASE)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    Private Function MakeLCQTICFile(ByVal Dataset As String, ByVal OutPath As String) As Boolean

        'Makes a TIC file for an LCQ dataset
        Return m_ICR2LSObj.MakeLCQTICFIle(Dataset, OutPath)

    End Function
End Class
