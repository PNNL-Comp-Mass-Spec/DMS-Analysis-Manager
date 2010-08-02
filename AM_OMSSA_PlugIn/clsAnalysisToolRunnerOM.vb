Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerOM
    Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running OMSSA analysis
	'*********************************************************************************************************

#Region "Module Variables"
	Protected Const PROGRESS_PCT_OMSSA_RUNNING As Single = 5
	Protected Const PROGRESS_PCT_PEPTIDEHIT_START As Single = 95
	Protected Const PROGRESS_PCT_PEPTIDEHIT_COMPLETE As Single = 99

    Protected m_DatasetName As String = String.Empty

	Protected WithEvents CmdRunner As clsRunDosProgram
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
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType

        Dim blnProcessingError As Boolean
        Dim eReturnCode As IJobParams.CloseOutType

        ' Set this to success for now
        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        ' Cache the dataset name
        m_DatasetName = m_jobParams.GetParam("datasetNum")

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running OMSSA")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerOM.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("OMSSAprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'OMSSAprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find OMSSA program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor OMSSA log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Get the OMSSA log file name for a File Watcher to monitor
        'Dim OMSSALogFileName As String = GetOMSSALogFileName(System.IO.Path.Combine(m_WorkDir, m_OMSSASetupFile))
        'If OMSSALogFileName = "" Then
        '	m_logger.PostEntry("Error getting OMSSA log file name", ILogger.logMsgType.logError, True)
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        ''Setup and start a File Watcher to monitor the OMSSA log file
        'StartFileWatcher(m_workdir, OMSSALogFileName)
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

        Dim inputFilename As String = System.IO.Path.Combine(m_WorkDir, "OMSSA_Input.xml")
        'Set up and execute a program runner to run OMSSA
        CmdStr = " -pm " & inputFilename

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting OMSSA: " & progLoc & " " & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(progLoc) & "_ConsoleOutput.txt")
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "OMSSA", True) Then
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
        m_StopTime = System.DateTime.Now


        If blnProcessingError Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the result folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
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
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        If Not blnProcessingError Then
            'Zip the output file
            result = ZipMainOutputFile()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                blnProcessingError = True
            End If
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MoveResultFiles moves the result files to the result folder
            m_message = "Error moving files into results folder"
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        'If we get to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS


    End Function

    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at lease one spectrum in it
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strInputFilePath As String
        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False

        Try
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_DatasetName & "_dta.txt")

            If Not System.IO.File.Exists(strInputFilePath) Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If
            
            srReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

            Do While srReader.Peek >= 0
                If srReader.ReadLine.Trim.Length > 0 Then
                    blnDataFound = True
                    Exit Do
                End If
            Loop

            srReader.Close()

            If Not blnDataFound Then
                m_message = "The _DTA.txt file is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

        Catch ex As Exception
            m_message = "Exception in ValidateCDTAFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnDataFound

    End Function

	''' <summary>
    ''' Zips OMSSA XML output file
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Private Function ZipMainOutputFile() As IJobParams.CloseOutType

        'Zip the output file
        Dim strOMSSAResultsFilePath As String
        Dim blnSuccess As Boolean

        strOMSSAResultsFilePath = System.IO.Path.Combine(m_WorkDir, m_DatasetName & "_om.omx")

        blnSuccess = MyBase.ZipFile(strOMSSAResultsFilePath, True)
        If Not blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

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
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_OMSSA_RUNNING, 0, "", "", "", False)
        End If

	End Sub

    Protected Function ConvertOMSSA2PepXmlFile() As Boolean
        Dim CmdStr As String
        Dim result As Boolean = True

        Try
            ' set up formatdb.exe to reference the organsim DB file (fasta)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running OMSSA2PepXml")

            CmdRunner = New clsRunDosProgram(m_WorkDir)

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile(): Enter")
            End If

            ' verify that program formatdb.exe file exists
            Dim progLoc As String = m_mgrParams.GetParam("omssa2pepprogloc")
            If Not System.IO.File.Exists(progLoc) Then
                If progLoc.Length = 0 Then progLoc = "Parameter 'omssa2pepprogloc' not defined for this manager"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find OMSSA2PepXml program file: " & progLoc)
                Return False
            End If

            Dim outputFilename As String = System.IO.Path.Combine(m_WorkDir, m_DatasetName & "_pepxml.xml")
            Dim inputFilename As String = System.IO.Path.Combine(m_WorkDir, m_DatasetName & "_om_large.omx")

            'Set up and execute a program runner to run Omssa2PepXml.exe
            'omssa2pepxml.exe -xml -o C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_pepxml.xml C:\DMS_WorkDir\QC_Shew_09_02_pt5_a_20May09_Earth_09-04-20_omx_large.omx
            CmdStr = "-xml -o " & outputFilename & " " & inputFilename

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting OMSSA2PepXml: " & progLoc & " " & CmdStr)
            End If

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(progLoc) & "_ConsoleOutput.txt")
            End With

            If Not CmdRunner.RunProgram(progLoc, CmdStr, "OMSSA2PepXml", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running OMSSA2PepXml, job " & m_JobNum)
                Return False
            End If

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerOM.ConvertOMSSA2PepXmlFile, exception, " & ex.Message)
        End Try

        Return result
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
