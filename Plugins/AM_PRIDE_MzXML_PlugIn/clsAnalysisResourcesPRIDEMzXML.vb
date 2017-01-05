Option Strict On

Imports AnalysisManagerBase
Imports System.Linq
Imports System.Collections.Generic

Public Class clsAnalysisResourcesPRIDEMzXML
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Dim fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(","c).ToList()

        For Each fileSpec As String In fileSpecList.ToList()
            Dim fileSpecTerms = fileSpec.Split(":"c).ToList()
            If fileSpecTerms.Count <= 2 OrElse Not fileSpecTerms(2).ToLower = "copy" Then
                m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms(1))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting PRIDE MzXML Input file")

        If Not RetrieveFile(m_jobParams.GetParam("PRIDEMzXMLInputFile"), _
         m_jobParams.GetParam("transferFolderPath")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        m_jobParams.AddResultFileToSkip(m_jobParams.GetParam("PRIDEMzXMLInputFile"))

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files")

        Dim dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo) = Nothing

        If Not RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Undefined, dctDataPackageJobs) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
