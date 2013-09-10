Option Strict On

' Last modified 06/11/2009 JDS - Added logging using log4net
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

		Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

        'Get input data file
		If Not RetrieveSpectra(strRawDataType) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_UIMF_EXTENSION)
        m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
        m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_WIFF_EXTENSION)
		m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION)
		m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZML_EXTENSION)

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        'Retrieve param file
        If Not RetrieveFile( _
           m_jobParams.GetParam("ParmFileName"), _
           m_jobParams.GetParam("ParmFileStoragePath")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED


        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region


End Class
