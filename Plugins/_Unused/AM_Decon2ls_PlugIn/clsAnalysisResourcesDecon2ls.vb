' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase
Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports PRISM.Logging

Public Class clsAnalysisResourcesDecon2ls
    Inherits AnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Decon2ls analysis
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As CloseOutType

        'Retrieve param file
        If Not FileSearchTool.RetrieveFile( _
          mJobParams.GetParam("ParmFileName"), _
          mJobParams.GetParam("ParmFileStoragePath")) _
        Then Return CloseOutType.CLOSEOUT_FAILED

        'Get input data file
        Dim rawDataType As String = mJobParams.GetParam("RawDataType")
        If Not FileSearchTool.RetrieveSpectra(rawDataType) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region


End Class
