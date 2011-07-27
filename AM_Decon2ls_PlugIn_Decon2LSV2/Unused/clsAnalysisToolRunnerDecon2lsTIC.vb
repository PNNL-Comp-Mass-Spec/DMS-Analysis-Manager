Option Strict On

'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

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

    Protected Overrides Sub StartDecon2LS(ByRef bw As System.ComponentModel.BackgroundWorker, _
                                          ByVal udtCurrentLoopParams As udtCurrentLoopParamsType)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsDeIsotope.StartDecon2LS(), Starting TIC processing")
        End If

        Try

            ' TIC generation requires its own file extension creation
            ' We may need to add it
            'udtCurrentLoopParams.OutputFilePath &= "_scans.csv"

            Dim objDeconTools As DeconTools.Backend.OldSchoolProcRunner

            ' ToDo: Specify the output file path
            objDeconTools = New DeconTools.Backend.OldSchoolProcRunner(udtCurrentLoopParams.InputFilePath, _
                                                                       udtCurrentLoopParams.DeconFileType, _
                                                                       udtCurrentLoopParams.ParamFilePath, _
                                                                       bw)

            ''objDeconTools = New DeconTools.Backend.OldSchoolProcRunner(udtCurrentLoopParams.InputFilePath, _
            ''                                               udtCurrentLoopParams.OutputFilePath, _
            ''                                               udtCurrentLoopParams.DeconFileType, _
            ''                                               udtCurrentLoopParams.ParamFilePath, _
            ''                                               bw)

            mDeconToolsStatus.CurrentState = DeconToolsStateType.Running

            objDeconTools.IsosResultThreshold = 25000
            objDeconTools.Execute()

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling DeconTools.Backend.OldSchoolProcRunner in StartDecon2LS(): " & ex.Message)
        End Try

    End Sub

#End Region

End Class
