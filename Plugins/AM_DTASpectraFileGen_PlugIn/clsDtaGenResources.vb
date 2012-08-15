'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/08/2008
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports AnalysisManagerBase

Public Class clsDtaGenResources
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Gets resources necessary for DTA creation
	'*********************************************************************************************************

#Region "Methods"
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strRawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
		Dim blnMGFInstrumentData As Boolean = m_jobParams.GetJobParameter("MGFInstrumentData", False)

		If blnMGFInstrumentData Then
			Dim strFileToFind As String = m_DatasetName & DOT_MGF_EXTENSION
			If Not FindAndRetrieveMiscFiles(strFileToFind, False) Then
				m_message = "Instrument data not found: " & strFileToFind
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION)
			End If
		Else
			'Get input data file
			If Not RetrieveSpectra(strRawDataType, m_WorkingDir) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error retrieving instrument data file"
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function
#End Region

End Class

