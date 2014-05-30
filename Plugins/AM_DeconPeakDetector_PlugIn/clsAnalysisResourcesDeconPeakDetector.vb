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
	
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strRawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
		
		' Retrieve the peak detector parameter file

		Dim peakDetectorParamFileName = m_jobParams.GetJobParameter("PeakDetectorParamFile", "")
		Dim paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")

		paramFileStoragePath = Path.Combine(paramFileStoragePath, "PeakDetection")

		If Not RetrieveFile(peakDetectorParamFileName, paramFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Retrieve the instrument data file
		If Not RetrieveSpectra(strRawDataType) Then
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error retrieving instrument data file"
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
