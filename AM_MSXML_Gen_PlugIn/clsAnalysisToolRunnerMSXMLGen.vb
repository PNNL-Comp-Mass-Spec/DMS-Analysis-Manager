'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/06/2009
'
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

Public Class clsAnalysisToolRunnerMSXMLGen
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MS XML generator
    'Currently used to generate MZXML or MZML files
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Presently not used</remarks>
    Public Sub New()

    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.Setup()")
        End If
    End Sub
    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType

        'Start the job timer
        m_StartTime = System.DateTime.Now

        If CreateMZXMLFile() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

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

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            m_message = AppendToComment(m_message, "Error deleting non-result files")
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    ''' <summary>
    ''' Generate the MsXML file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreateMZXMLFile() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim rawFilename As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & ".raw")
        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")
        Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")
        Dim centroidOption As Boolean = CBool(m_jobParams.GetParam("CentroidMSXML"))

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.CreateMZXMLFile(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = Path.Combine(InspectDir, msXmlGenerator)
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find MSXmlGenerator exe program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run MS XML executable
        If centroidOption Then
            CmdStr = " --" & msXmlFormat & " " & " -c " & rawFilename
        Else
            CmdStr = " --" & msXmlFormat & " " & rawFilename
        End If
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        If Not CmdRunner.RunProgram(progLoc, CmdStr, msXmlGenerator, True) Then
            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msXmlGenerator & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & msXmlGenerator & " failed (but exit code is 0)")
            End If
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

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
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_MSXML_GEN_RUNNING, 0, "", "", "", False)
        End If

    End Sub
#End Region

End Class
