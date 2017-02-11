Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerAgilentSeq
    Inherits clsAnalysisToolRunnerSeqBase

    Protected Function DeleteDataFile() As CloseOutType

        'Deletes the data files (.mgf and .cdf) from the working directory
        Dim FoundFiles() As String
        Dim MyFile As String

        Try
            'Delete the .mgf file
            FoundFiles = Directory.GetFiles(m_WorkDir, "*.mgf")
            For Each MyFile In FoundFiles
                DeleteFileWithRetries(MyFile)
            Next
            'Delete the .cdf file, if present
            FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf")
            For Each MyFile In FoundFiles
                DeleteFileWithRetries(MyFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting raw data file(s), job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & Err.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

End Class
