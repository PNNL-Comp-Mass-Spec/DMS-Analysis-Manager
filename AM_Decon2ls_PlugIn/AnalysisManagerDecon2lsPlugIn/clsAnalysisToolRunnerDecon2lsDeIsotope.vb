'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 10/06/2006
'*********************************************************************************************************
Imports PRISM.Logging

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
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsDeIsotope.StartDecon2LS(), Starting deconvolution", ILogger.logMsgType.logDebug, True)
		End If
		m_ToolObj.DeConvolute()

	End Sub
#End Region

End Class
