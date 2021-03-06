'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerDecon2lsTIC
	Inherits clsAnalysisToolRunnerDecon2lsBase

	'*********************************************************************************************************
	'Subclass for using Decon2LS to generate TIC files
	'
	'Establishes tool type for results folder and calls TIC creation method of Decon2LS
	'*********************************************************************************************************

#Region "Methods"
	Sub New()

		MyBase.New()
		m_AnalysisType = "TDL"

	End Sub

	Protected Overrides Sub StartDecon2LS()

		'TIC generation requires its own file extension creation
		m_ToolObj.OutFile = m_ToolObj.OutFile & "_scans.csv"

		'Start Decon2LS
		If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsTIC.StartDecon2LS(), Starting TIC processing")
        End If
		m_ToolObj.CreateTIC()

	End Sub
#End Region

End Class
