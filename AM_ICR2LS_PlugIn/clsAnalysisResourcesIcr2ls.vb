' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports System.IO
Imports PRISM.Files
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesIcr2ls
    Inherits clsAnalysisResources

    ' Setting this to True will skip the step of copying the 0.ser folder locally
    ' In that case, if a network burp occurs while ICR-2LS is running, processing will fail
    ' Thus, for safety, we are setting this to False
    Public Const PROCESS_SER_FOLDER_OVER_NETWORK As Boolean = False

#Region "Methods"
    Public Overrides Function GetResources() As IJobParams.CloseOutType
        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve param file
        If Not RetrieveFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Get input data file
        If Not RetrieveSpectra(m_jobParams.GetParam("RawDataType"), m_mgrParams.GetParam("workdir"), PROCESS_SER_FOLDER_OVER_NETWORK) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If PROCESS_SER_FOLDER_OVER_NETWORK Then
            Dim NewSourceFolder As String = AnalysisManagerBase.clsAnalysisResources.ResolveSerStoragePath(m_mgrParams.GetParam("workdir"))
            'Check for "0.ser" folder
            If Not String.IsNullOrEmpty(NewSourceFolder) Then
                clsGlobal.FilesToDelete.Add(STORAGE_PATH_INFO_FILE_SUFFIX)
            End If
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
