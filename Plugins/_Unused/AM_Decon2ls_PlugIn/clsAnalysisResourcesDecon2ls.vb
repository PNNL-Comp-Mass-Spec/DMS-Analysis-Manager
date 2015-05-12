' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDecon2ls
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Decon2ls analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        'Retrieve param file
		If Not RetrieveFile( _
		  m_jobParams.GetParam("ParmFileName"), _
		  m_jobParams.GetParam("ParmFileStoragePath")) _
		Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		'Get input data file
		Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
		If Not RetrieveSpectra(rawDataType) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region


End Class
