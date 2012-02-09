'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerDecon2lsDeIsotope
	Inherits clsAnalysisToolRunnerDecon2lsBase

    '*********************************************************************************************************
    'Subclass for using Decon2LS to deisotope FTICR and LTQ-FT data
    '
    'Establishes tool type for results folder and calls deisotoping method of Decon2LS
    '*********************************************************************************************************

#Region "Methods"
	Sub New()

		MyBase.New()
		m_AnalysisType = "DLS"

	End Sub

    Protected Overrides Sub StartDecon2LS()

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsDeIsotope.StartDecon2LS(), Starting deconvolution")
        End If

        Try
            m_ToolObj.DeConvolute()
        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception from m_ToolObj.DeConvolute in clsAnalysisToolRunnerDecon2lsDeIsotope.StartDecon2LS(): " & ex.Message)
        End Try

    End Sub
#End Region

End Class
