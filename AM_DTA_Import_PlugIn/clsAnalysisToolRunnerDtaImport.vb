'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 04/10/2009
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

Public Class clsAnalysisToolRunnerDtaImport
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running DTA Importer
    '*********************************************************************************************************

#Region "Module Variables"
#End Region

#Region "Methods"

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDtaImport.Setup()")
        End If
    End Sub

    ''' <summary>
    ''' Runs DTA Import tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType

        Try
            'Start the job timer
            m_StartTime = System.DateTime.Now

            result = CopyManualDTAs()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
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

        Catch ex As Exception
            m_message = "Error in DtaImportPlugin->RunTool: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    Private Function CopyManualDTAs() As IJobParams.CloseOutType

        Dim SourceFolderNamePath As String = String.Empty
        Dim TargetFolderNamePath As String = String.Empty
        Dim CompleteFolderNamePath As String = String.Empty

        Try
            SourceFolderNamePath = System.IO.Path.Combine(m_mgrParams.GetParam("DTAFolderLocation"), m_jobParams.GetParam("OutputFolderName"))
            CompleteFolderNamePath = System.IO.Path.Combine(m_mgrParams.GetParam("DTAProcessedFolderLocation"), m_jobParams.GetParam("OutputFolderName"))

            'Determine if Dta folder in transfer directory already exists; Make directory if it doesn't exist
            TargetFolderNamePath = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("DatasetNum"))
            If Not System.IO.Directory.Exists(TargetFolderNamePath) Then
                'Make the DTA folder
                Try
                    System.IO.Directory.CreateDirectory(TargetFolderNamePath)
                Catch ex As Exception
                    m_message = AppendToComment(m_message, "Error creating results folder on " & System.IO.Path.GetPathRoot(TargetFolderNamePath)) & ": " & ex.Message
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    'TODO: Handle errors
                End Try
            End If

            ' Now append the output folder name to TargetFolderNamePath
            TargetFolderNamePath = System.IO.Path.Combine(TargetFolderNamePath, m_jobParams.GetParam("OutputFolderName"))

        Catch ex As Exception
            m_message = AppendToComment(m_message, "Error creating results folder: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            'TODO: Handle errors
        End Try

        Try

            'Copy the DTA folder to the transfer folder
            My.Computer.FileSystem.CopyDirectory(SourceFolderNamePath, TargetFolderNamePath, False)

            'Now move the DTA folder to succeeded folder
            My.Computer.FileSystem.MoveDirectory(SourceFolderNamePath, CompleteFolderNamePath, False)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = AppendToComment(m_message, "Error copying results folder to " & System.IO.Path.GetPathRoot(TargetFolderNamePath) & " : " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


    End Function

#End Region

End Class
