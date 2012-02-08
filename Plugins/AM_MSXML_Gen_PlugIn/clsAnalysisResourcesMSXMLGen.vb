' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase
Imports System.IO
Imports System

Public Class clsAnalysisResourcesMSXMLGen
    Inherits clsAnalysisResources


#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Get input data file
        Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

        Select Case strRawDataType.ToLower
            Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_BRUKER_FT_FOLDER
                If RetrieveSpectra(strRawDataType, m_mgrParams.GetParam("workdir")) Then
                    clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_RAW_EXTENSION)  'Raw file
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Case Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Dataset type " & strRawDataType & " is not supported; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Select


        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
