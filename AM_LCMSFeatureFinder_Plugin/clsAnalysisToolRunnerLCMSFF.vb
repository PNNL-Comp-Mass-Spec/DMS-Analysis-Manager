Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
'Imports PRISM.Files
'Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerLCMSFF
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    ' Class for running the LCMS Feature Finder
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_FEATURE_FINDER_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_FEATURE_FINDER_DONE As Single = 95

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs LCMS Feature Finder tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running LCMSFeatureFinder")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerLCMSFF.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("LCMSFeatureFinderProgLoc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Manager parameter LCMSFeatureFinderProgLoc is not defined in the Manager Control DB")
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find LCMSFeatureFinder program file: " & progLoc)
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Set up and execute a program runner to run the LCMS Feature Finder
        CmdStr = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("LCMSFeatureFinderIniFile"))
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "LCMSFeatureFinder", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running LCMSFeatureFinder, job " & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now
        m_progress = PROGRESS_PCT_FEATURE_FINDER_DONE

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_WorkDir, m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function


    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_FEATURE_FINDER_RUNNING, 0, "", "", "", False)
        End If

    End Sub


#End Region

End Class
