'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/23/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesDeconPeakDetector
    Inherits clsAnalysisResources
    
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Dim strRawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
        
        ' Retrieve the peak detector parameter file

        Dim peakDetectorParamFileName = m_jobParams.GetJobParameter("PeakDetectorParamFile", "")
        Dim paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")

        paramFileStoragePath = Path.Combine(paramFileStoragePath, "PeakDetection")

        If Not FileSearch.RetrieveFile(peakDetectorParamFileName, paramFileStoragePath) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the instrument data file
        If Not FileSearch.RetrieveSpectra(strRawDataType) Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error retrieving instrument data file"
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
