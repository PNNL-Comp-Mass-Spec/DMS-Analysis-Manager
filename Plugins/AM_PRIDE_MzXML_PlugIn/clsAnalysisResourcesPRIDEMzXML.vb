Option Strict On

Imports AnalysisManagerBase
Imports System.Collections.Generic
Imports System.IO

Public Class clsAnalysisResourcesPRIDEMzXML
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        Dim SplitString As String()
        Dim FileNameExt As String()

        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt(2) = "nocopy" Then
                clsGlobal.m_FilesToDeleteExt.Add(FileNameExt(1))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting PRIDE MzXML Input file")

        If Not RetrieveFile(m_jobParams.GetParam("PRIDEMzXMLInputFile"), _
         m_jobParams.GetParam("transferFolderPath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("PRIDEMzXMLInputFile"))

        If Not RetrieveAggregateFiles(SplitString) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
