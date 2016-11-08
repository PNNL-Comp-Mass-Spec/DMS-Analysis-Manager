'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2010, Battelle Memorial Institute
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerPRIDEMzXML
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running PRIDEMzXML analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_PRIDEMZXML_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_START As Single = 95
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99

    Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MSDataFileTrimmer tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the MSDataFileTrimmer version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining MSDataFileTrimmer version"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSDataFileTrimmer")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPRIDEMzXML.RunTool(): Enter")
        End If

        ' verify that program file exists
        ' progLoc will be something like this: "C:\DMS_Programs\MSDataFileTrimmer\MSDataFileTrimmer.exe"
        Dim progLoc As String = m_mgrParams.GetParam("MSDataFileTrimmerprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'MSDataFileTrimmerprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find MSDataFileTrimmer program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim CmdStr As String
        CmdStr = "/M:" & System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("PRIDEMzXMLInputFile"))
        CmdStr &= " /G /O:" & m_WorkDir
        CmdStr &= " /L:" & System.IO.Path.Combine(m_WorkDir, "MSDataFileTrimmer_Log.txt")

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, "MSDataFileTrimmer_ConsoleOutput.txt")
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "MSDataFileTrimmer", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running MSDataFileTrimmer, job " & m_JobNum)

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(500)        ' 500 msec delay
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

        ' Override the dataset name and transfer folder path so that the results get copied to the correct location
        MyBase.RedefineAggregationJobDatasetAndTransferFolder()

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        Dim DumFiles() As String

        'update list of files to be deleted after run
        DumFiles = System.IO.Directory.GetFiles(m_WorkDir, "*_grouped*")
        For Each FileToSave As String In DumFiles
            m_jobParams.AddResultFileToKeep(System.IO.Path.GetFileName(FileToSave))
        Next

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

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(m_mgrParams.GetParam("MSDataFileTrimmerprogloc")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        UpdateStatusFile(PROGRESS_PCT_PRIDEMZXML_RUNNING)

        LogProgress("PrideMzXML")

    End Sub

#End Region

End Class
