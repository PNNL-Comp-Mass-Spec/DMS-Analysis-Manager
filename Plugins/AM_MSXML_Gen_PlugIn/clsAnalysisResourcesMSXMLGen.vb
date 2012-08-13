' Last modified 06/15/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSXMLGen
    Inherits clsAnalysisResources


#Region "Methods"
    ''' <summary>
	''' Retrieves files necessary for creating the .mzXML file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        'Get input data file
        Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

        Select Case strRawDataType.ToLower
			Case RAW_DATA_TYPE_DOT_RAW_FILES
				If RetrieveSpectra(strRawDataType, m_mgrParams.GetParam("workdir")) Then
					m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)  'Raw file
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Case Else
				m_message = "Dataset type " & strRawDataType & " is not supported"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select


        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
