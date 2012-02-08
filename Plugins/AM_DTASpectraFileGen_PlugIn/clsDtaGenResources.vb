'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/08/2008
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files
Imports AnalysisManagerBase

Public Class clsDtaGenResources
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Gets resources necessary for DTA creation
	'*********************************************************************************************************

#Region "Methods"
	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

		'Get input data file
        If RetrieveSpectra(m_jobParams.GetParam("RawDataType"), m_WorkingDir) Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

    End Function
#End Region

End Class

