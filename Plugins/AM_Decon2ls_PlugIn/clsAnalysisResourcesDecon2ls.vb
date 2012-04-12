' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDecon2ls
    Inherits clsAnalysisResources

    ' Setting this to True will skip the step of copying the 0.ser folder locally
    ' In that case, if a network burp occurs while ICR-2LS is running, processing will fail
    ' Thus, for safety, we are setting this to False
    Public Const PROCESS_SER_FOLDER_OVER_NETWORK As Boolean = False

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
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Get input data file
        If Not RetrieveSpectra(m_jobParams.GetParam("RawDataType"), m_mgrParams.GetParam("workdir"), PROCESS_SER_FOLDER_OVER_NETWORK) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If PROCESS_SER_FOLDER_OVER_NETWORK Then
            Dim NewSourceFolder As String = clsAnalysisResources.ResolveSerStoragePath(m_mgrParams.GetParam("workdir"))
            'Check for "0.ser" folder
			If Not String.IsNullOrEmpty(NewSourceFolder) Then
				m_jobParams.AddResultFileToSkip(STORAGE_PATH_INFO_FILE_SUFFIX)
			End If
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region


End Class
