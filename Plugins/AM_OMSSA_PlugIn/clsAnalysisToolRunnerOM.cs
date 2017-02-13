'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerOM
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running OMSSA analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_OMSSA_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_PEPTIDEHIT_START As Single = 95
    Protected Const PROGRESS_PCT_PEPTIDEHIT_COMPLETE As Single = 99

    '--------------------------------------------------------------------------------------------
    'Future section to monitor OMSSA log file for progress determination
    '--------------------------------------------------------------------------------------------
    'Dim WithEvents m_StatFileWatch As FileSystemWatcher
    'Protected m_XtSetupFile As String = "default_input.xml"
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs OMSSA tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As CloseOutType

        Dim CmdStr As String
        Dim result As CloseOutType

        Dim blnProcessingError As Boolean
        Dim eReturnCode As CloseOutType

        ' Set this to success for now
        eReturnCode = CloseOutType.CLOSEOUT_SUCCESS

        'Do the base class stuff
        If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the OMSSA version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining OMSSA version"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running OMSSA")

        Dim cmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(cmdRunner)
        AddHandler cmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("OMSSAprogloc")
        If Not File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'OMSSAprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find OMSSA program file: " & progLoc)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor OMSSA log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Get the OMSSA log file name for a File Watcher to monitor
        'Dim OMSSALogFileName As String = GetOMSSALogFileName(Path.Combine(m_WorkDir, m_OMSSASetupFile))
        'If OMSSALogFileName = "" Then
        '	m_logger.PostEntry("Error getting OMSSA log file name", ILogger.logMsgType.logError, True)
        '	Return CloseOutType.CLOSEOUT_FAILED
        'End If

        ''Setup and start a File Watcher to monitor the OMSSA log file
        'StartFileWatcher(m_workdir, OMSSALogFileName)
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        Dim inputFilename As String = Path.Combine(m_WorkDir, "OMSSA_Input.xml")
        'Set up and execute a program runner to run OMSSA
        CmdStr = " -pm " & inputFilename

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting OMSSA: " & progLoc & " " & CmdStr)
        End If

        With cmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) & "_ConsoleOutput.txt")
        End With

        If Not cmdRunner.RunProgram(progLoc, CmdStr, "OMSSA", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running OMSSA, job " & m_JobNum)
            blnProcessingError = True
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor OMSSA log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Turn off file watcher
        'm_StatFileWatch.EnableRaisingEvents = False
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        'Stop the job timer
        m_StopTime = DateTime.UtcNow


        If blnProcessingError Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the result folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
            eReturnCode = CloseOutType.CLOSEOUT_FAILED
        End If


        If Not blnProcessingError Then
            If Not ConvertOMSSA2PepXmlFile() Then
                blnProcessingError = True
            End If
        End If

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        Thread.Sleep(500)        ' 500 msec delay
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

        If Not blnProcessingError Then
            'Zip the output file
            result = ZipMainOutputFile()
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                blnProcessingError = True
            End If
        End If

        result = MakeResultsFolder()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        result = MoveResultFiles()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the result files to the result folder
            m_message = "Error moving files into results folder"
            eReturnCode = CloseOutType.CLOSEOUT_FAILED
        End If

        If blnProcessingError Or eReturnCode = CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Return CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyResultsFolderToServer()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        'If we get to here, everything worked so exit happily
        Return CloseOutType.CLOSEOUT_SUCCESS


    End Function

    ''' <summary>
    ''' Zips OMSSA XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As CloseOutType

        'Zip the output file
        Dim strOMSSAResultsFilePath As String
        Dim blnSuccess As Boolean

        strOMSSAResultsFilePath = Path.Combine(m_WorkDir, m_Dataset & "_om.omx")

        blnSuccess = MyBase.ZipFile(strOMSSAResultsFilePath, True)
        If Not blnSuccess Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        UpdateStatusFile(PROGRESS_PCT_OMSSA_RUNNING)

        LogProgress("OMSSA")

    End Sub

    Protected Function ConvertOMSSA2PepXmlFile() As Boolean
        Dim CmdStr As String
        Dim result = True

        Try
            ' set up formatdb.exe to reference the organsim DB file (fasta)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running OMSSA2PepXml")

            Dim cmdRunner = New clsRunDosProgram(m_WorkDir)
            RegisterEvents(cmdRunner)
            AddHandler cmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile(): Enter")
            End If

            ' verify that program formatdb.exe file exists
            Dim progLoc As String = m_mgrParams.GetParam("omssa2pepprogloc")
            If Not File.Exists(progLoc) Then
                If progLoc.Length = 0 Then progLoc = "Parameter 'omssa2pepprogloc' not defined for this manager"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find OMSSA2PepXml program file: " & progLoc)
                Return False
            End If

            Dim outputFilename As String = Path.Combine(m_WorkDir, m_Dataset & "_pepxml.xml")
            Dim inputFilename As String = Path.Combine(m_WorkDir, m_Dataset & "_om_large.omx")

            'Set up and execute a program runner to run Omssa2PepXml.exe
            'omssa2pepxml.exe -xml -o C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_pepxml.xml C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_omx_large.omx
            CmdStr = "-xml -o " & outputFilename & " " & inputFilename

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting OMSSA2PepXml: " & progLoc & " " & CmdStr)
            End If

            With cmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(progLoc) & "_ConsoleOutput.txt")
            End With

            If Not cmdRunner.RunProgram(progLoc, CmdStr, "OMSSA2PepXml", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running OMSSA2PepXml, job " & m_JobNum)
                Return False
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile, exception, " & ex.Message)
        End Try

        Return result
    End Function


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
        Dim ioToolFiles As New Generic.List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(m_mgrParams.GetParam("OMSSAprogloc")))
        ioToolFiles.Add(New FileInfo(m_mgrParams.GetParam("omssa2pepprogloc")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    '--------------------------------------------------------------------------------------------
    'Future section to monitor OMSSA log file for progress determination
    '--------------------------------------------------------------------------------------------
    '	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

    ''Watches the OMSSA status file and reports changes

    ''Setup
    'm_StatFileWatch = New FileSystemWatcher
    'With m_StatFileWatch
    '	.BeginInit()
    '	.Path = DirToWatch
    '	.IncludeSubdirectories = False
    '	.Filter = FileToWatch
    '	.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
    '	.EndInit()
    'End With

    ''Start monitoring
    'm_StatFileWatch.EnableRaisingEvents = True

    '	End Sub
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

End Class
