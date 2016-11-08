Option Strict On

'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

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

    Protected Overrides Sub StartDecon2LS(ByRef bw As System.ComponentModel.BackgroundWorker, _
                                          ByVal udtCurrentLoopParams As udtCurrentLoopParamsType)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsDeIsotope.StartDecon2LS(), Starting deconvolution")
        End If

        ''If mPickPeaksOnly Then
        ''    Try

        ''        Dim objDeconTools As DeconTools.Backend.ProjectControllers.UIMFMSOnlyProjectController

        ''        objDeconTools = New DeconTools.Backend.ProjectControllers.UIMFMSOnlyProjectController( _
        ''                                                                   udtCurrentLoopParams.InputFilePath, _
        ''                                                                   udtCurrentLoopParams.DeconFileType, _
        ''                                                                   udtCurrentLoopParams.ParamFilePath, _
        ''                                                                   bw)

        ''        mDeconToolsStatus.CurrentState = DeconToolsStateType.Running

        ''        objDeconTools.IsosResultThreshold = 25000
        ''        objDeconTools.Execute()

        ''    Catch ex As System.Exception
        ''        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling DeconTools.Backend.OldSchoolProcRunner in StartDecon2LS(): " & ex.Message)
        ''    End Try

        ''Else
        ''End If

        Try

            Dim objDeconTools As DeconTools.Backend.OldSchoolProcRunner

            ' ToDo: Specify the output file path
            objDeconTools = New DeconTools.Backend.OldSchoolProcRunner(udtCurrentLoopParams.InputFilePath, _
                                                                       udtCurrentLoopParams.DeconFileType, _
                                                                       udtCurrentLoopParams.ParamFilePath, _
                                                                       bw)

            ''objDeconTools = New DeconTools.Backend.OldSchoolProcRunner(udtCurrentLoopParams.InputFilePath, _
            ''                                               udtCurrentLoopParams.OutputFolderPath, _
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
