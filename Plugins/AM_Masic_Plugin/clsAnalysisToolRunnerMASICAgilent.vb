'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

''' <summary>
''' Derived class for performing MASIC analysis on Agilent datasets
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerMASICAgilent
    Inherits clsAnalysisToolRunnerMASICBase

    Public Sub New()
    End Sub

    Protected Overrides Function RunMASIC() As CloseOutType

        Dim strParameterFileName As String
        Dim strParameterFilePath As String

        Dim strMgfFileName As String
        Dim strInputFilePath As String

        strParameterFileName = m_JobParams.GetParam("parmFileName")

        If Not strParameterFileName Is Nothing AndAlso strParameterFileName.Trim.ToLower <> "na" Then
            strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        Else
            strParameterFilePath = String.Empty
        End If

        ' Determine the path to the .Raw file
        strMgfFileName = m_Dataset & ".mgf"
        strInputFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strMgfFileName)

        If strInputFilePath Is Nothing OrElse strInputFilePath.Length = 0 Then
            ' Unable to resolve the file path
            m_ErrorMessage = "Could not find " & strMgfFileName & " or " & strMgfFileName & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to run MASIC"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return MyBase.StartMASICAndWait(strInputFilePath, m_workdir, strParameterFilePath)

    End Function

    Protected Overrides Function DeleteDataFile() As CloseOutType

        'Deletes the .cdf and .mgf files from the working directory
        Dim FoundFiles() As String
        Dim MyFile As String

        'Delete the .cdf file
        Try
            FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf")
            For Each MyFile In FoundFiles
                DeleteFileWithRetries(MyFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .cdf file, job " & m_JobNum & Err.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        'Delete the .mgf file
        Try
            FoundFiles = Directory.GetFiles(m_WorkDir, "*.mgf")
            For Each MyFile In FoundFiles
                DeleteFileWithRetries(MyFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting .mgf file, job " & m_JobNum)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
