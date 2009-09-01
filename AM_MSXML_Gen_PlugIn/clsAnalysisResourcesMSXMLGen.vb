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
        If RetrieveSpectra(m_jobParams.GetParam("RawDataType"), m_mgrParams.GetParam("workdir")) Then
            Dim rawFilename As String = Path.Combine(m_mgrParams.GetParam("workdir"), m_jobParams.GetParam("datasetNum") & ".raw")
            clsGlobal.m_FilesToDeleteExt.Add(".raw")  'Raw file
            clsGlobal.FilesToDelete.Add(rawFilename)
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


    End Function
#End Region

End Class
